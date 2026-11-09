package metastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3util"
)

// Objects is the small part of an object store the metadata store needs:
// whole-object reads and conditional whole-object writes.
type Objects interface {
	// Get returns an object's body and ETag, or ErrNotFound.
	Get(ctx context.Context, key string) (body []byte, etag string, err error)
	// Put writes an object if cond holds, and returns its new ETag. When
	// cond doesn't hold, it returns ErrPreconditionFailed and writes
	// nothing.
	Put(ctx context.Context, key string, body []byte, cond Condition) (etag string, err error)
	// Delete removes an object. Deleting a missing object is not an error.
	Delete(ctx context.Context, key string) error
}

// Condition is when a Put may write. The zero Condition always writes.
type Condition struct {
	// IfNoneMatch writes only when the key doesn't exist (If-None-Match: *).
	IfNoneMatch bool
	// IfMatch writes only when the object's ETag is this one (If-Match).
	IfMatch string
}

var (
	// ErrNotFound is an object that doesn't exist.
	ErrNotFound = errors.New("object not found")
	// ErrPreconditionFailed is a conditional write that lost: the object
	// exists (IfNoneMatch), or has changed (IfMatch).
	ErrPreconditionFailed = errors.New("precondition failed")
)

// S3Objects is Objects in an S3 bucket. Conditional writes need a store that
// honors If-Match and If-None-Match on PutObject: AWS S3 (since 2024), MinIO,
// DigitalOcean Spaces and others. VerifyConditionalWrites checks one.
type S3Objects struct {
	Client *s3.Client
	Bucket string
	// FilterAllUploads lists the bucket's uploads and keeps those under the
	// prefix, when a listing by prefix finds none: for stores that list
	// multipart uploads only by exact key (MinIO). See gc.S3Bucket.
	FilterAllUploads bool
}

var _ Objects = (*S3Objects)(nil)

// Get reads an object with GetObject.
func (o *S3Objects) Get(ctx context.Context, key string) ([]byte, string, error) {
	out, err := o.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &o.Bucket, Key: &key})
	if err != nil {
		if s3util.HTTPStatus(err) == http.StatusNotFound {
			return nil, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
		}
		return nil, "", fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("get %s: read body: %w", key, err)
	}
	return body, aws.ToString(out.ETag), nil
}

// Put writes an object with PutObject, sending cond as If-None-Match or
// If-Match.
//
// S3 answers a lost condition with 412. AWS S3 may also answer 409 when
// another conditional write to the key is in flight; that write may still
// win, so 409 is a lost condition too, and the caller reads again.
func (o *S3Objects) Put(ctx context.Context, key string, body []byte, cond Condition) (string, error) {
	in := &s3.PutObjectInput{
		Bucket:        &o.Bucket,
		Key:           &key,
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   aws.String("application/json"),
	}
	if cond.IfNoneMatch {
		in.IfNoneMatch = aws.String("*")
	}
	if cond.IfMatch != "" {
		// Unquoted: Spaces answers 412 to the quoted ETag it returned itself,
		// even when it is current, and MinIO takes either.
		in.IfMatch = aws.String(strings.Trim(cond.IfMatch, `"`))
	}
	out, err := o.Client.PutObject(ctx, in)
	if err != nil {
		switch s3util.HTTPStatus(err) {
		case http.StatusPreconditionFailed, http.StatusConflict:
			return "", fmt.Errorf("put %s: %w", key, ErrPreconditionFailed)
		}
		return "", fmt.Errorf("put %s: %w", key, err)
	}
	return aws.ToString(out.ETag), nil
}

// Delete removes an object with DeleteObject, which succeeds for a missing
// key too.
func (o *S3Objects) Delete(ctx context.Context, key string) error {
	if _, err := o.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &o.Bucket, Key: &key}); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// ListKeys returns up to max keys under prefix, in key order, with one
// ListObjectsV2.
func (o *S3Objects) ListKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	out, err := o.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &o.Bucket,
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(max)),
	})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", prefix, err)
	}
	keys := make([]string, 0, len(out.Contents))
	for _, c := range out.Contents {
		keys = append(keys, aws.ToString(c.Key))
	}
	// A store may return more than asked.
	return keys[:min(len(keys), max)], nil
}

// ListUploadKeys returns the keys of up to max incomplete multipart uploads
// under prefix: one ListMultipartUploads, and with FilterAllUploads, when it
// finds none, one more of the bucket's, filtered.
func (o *S3Objects) ListUploadKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	list := func(p string) ([]string, error) {
		in := &s3.ListMultipartUploadsInput{Bucket: &o.Bucket}
		if p != "" {
			in.Prefix = aws.String(p)
		}
		var keys []string
		for {
			out, err := o.Client.ListMultipartUploads(ctx, in)
			if err != nil {
				return nil, fmt.Errorf("list uploads %s: %w", p, err)
			}
			for _, u := range out.Uploads {
				if k := aws.ToString(u.Key); strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
			if len(keys) >= max || !aws.ToBool(out.IsTruncated) || (out.NextKeyMarker == nil && out.NextUploadIdMarker == nil) {
				return keys[:min(len(keys), max)], nil
			}
			in.KeyMarker, in.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
		}
	}
	keys, err := list(prefix)
	if err != nil || len(keys) > 0 || prefix == "" || !o.FilterAllUploads {
		return keys, err
	}
	return list("")
}

// MemObjects is Objects in memory, with the same conditional writes as S3.
// It is for tests: the service under test, and a restarted one, can share
// one.
type MemObjects struct {
	mu      sync.Mutex
	objects map[string]memObject
	uploads []string
	version uint64
	now     func() time.Time
}

type memObject struct {
	body     []byte
	etag     string
	modified time.Time
}

var _ Objects = (*MemObjects)(nil)

// NewMemObjects returns an empty MemObjects.
func NewMemObjects() *MemObjects {
	return &MemObjects{objects: map[string]memObject{}, now: time.Now}
}

// SetClock sets the clock that stamps when objects are written, as List
// reports it. Nil is time.Now.
func (m *MemObjects) SetClock(now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if now == nil {
		now = time.Now
	}
	m.now = now
}

// ObjectInfo is an object as a bucket listing shows it.
type ObjectInfo struct {
	Key      string
	Size     int64
	Modified time.Time
}

// List returns the objects whose keys start with prefix, in key order.
func (m *MemObjects) List(prefix string) []ObjectInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ObjectInfo
	for k, o := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(o.body)), Modified: o.modified})
		}
	}
	slices.SortFunc(out, func(a, b ObjectInfo) int { return strings.Compare(a.Key, b.Key) })
	return out
}

// ListKeys returns up to max keys under prefix, in key order.
func (m *MemObjects) ListKeys(_ context.Context, prefix string, max int) ([]string, error) {
	var keys []string
	for _, o := range m.List(prefix) {
		if len(keys) == max {
			break
		}
		keys = append(keys, o.Key)
	}
	return keys, nil
}

// AddUpload records an incomplete multipart upload at key, which
// ListUploadKeys reports: MemObjects holds no upload's data.
func (m *MemObjects) AddUpload(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads = append(m.uploads, key)
}

// ListUploadKeys returns up to max of the uploads AddUpload recorded under
// prefix, sorted.
func (m *MemObjects) ListUploadKeys(_ context.Context, prefix string, max int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for _, k := range m.uploads {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys[:min(len(keys), max)], nil
}

// Get returns a copy of the object.
func (m *MemObjects) Get(_ context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
	}
	return bytes.Clone(obj.body), obj.etag, nil
}

// Put writes a copy of body if cond holds.
func (m *MemObjects) Put(_ context.Context, key string, body []byte, cond Condition) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, exists := m.objects[key]
	if cond.IfNoneMatch && exists {
		return "", fmt.Errorf("put %s: %w", key, ErrPreconditionFailed)
	}
	if cond.IfMatch != "" && (!exists || strings.Trim(obj.etag, `"`) != strings.Trim(cond.IfMatch, `"`)) {
		return "", fmt.Errorf("put %s: %w", key, ErrPreconditionFailed)
	}
	m.version++
	etag := strconv.Quote(strconv.FormatUint(m.version, 10))
	m.objects[key] = memObject{body: bytes.Clone(body), etag: etag, modified: m.now()}
	return etag, nil
}

// Delete removes an object, if it exists.
func (m *MemObjects) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

// Keys returns the stored keys, in no order.
func (m *MemObjects) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	return keys
}

// NewS3Objects returns S3Objects for cfg's bucket, reaching S3 the way the
// blob store does. It ignores cfg's prefix and expiries, and makes no
// requests.
func NewS3Objects(ctx context.Context, cfg blobstore.S3Config) (*S3Objects, error) {
	return newS3Objects(ctx, cfg, nil)
}

func newS3Objects(ctx context.Context, cfg blobstore.S3Config, client *s3.Client) (*S3Objects, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 metadata store: no bucket")
	}
	if client == nil {
		var err error
		if client, err = blobstore.NewS3Client(ctx, cfg); err != nil {
			return nil, fmt.Errorf("s3 metadata store: %w", err)
		}
	}
	// A custom endpoint is an S3-compatible store, which may list multipart
	// uploads only by exact key (MinIO), as for GC (server.EnableS3).
	return &S3Objects{Client: client, Bucket: cfg.Bucket, FilterAllUploads: cfg.Endpoint != ""}, nil
}

// NewS3 returns a Store in cfg's bucket, under cfg's prefix: the same bucket
// and prefix as the blob store's, so a scope's metadata sits next to its
// blobs, under <prefix>/ns/<scope>/ (see package layout).
func NewS3(ctx context.Context, cfg blobstore.S3Config) (*Store, error) {
	return NewS3WithClient(ctx, cfg, nil)
}

// NewS3WithClient is NewS3 with client, which blobstore.NewS3Client built for
// cfg, instead of a client of its own. A nil client builds one.
func NewS3WithClient(ctx context.Context, cfg blobstore.S3Config, client *s3.Client) (*Store, error) {
	objects, err := newS3Objects(ctx, cfg, client)
	if err != nil {
		return nil, err
	}
	return New(Config{Objects: objects, Prefix: cfg.Prefix})
}
