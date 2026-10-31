package metastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
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
// honors If-Match and If-None-Match on PutObject: AWS S3 (since 2024), MinIO
// and others. VerifyConditionalWrites checks one.
type S3Objects struct {
	Client *s3.Client
	Bucket string
}

var _ Objects = (*S3Objects)(nil)

// Get reads an object with GetObject.
func (o *S3Objects) Get(ctx context.Context, key string) ([]byte, string, error) {
	out, err := o.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &o.Bucket, Key: &key})
	if err != nil {
		if httpStatus(err) == http.StatusNotFound {
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
		in.IfMatch = aws.String(cond.IfMatch)
	}
	out, err := o.Client.PutObject(ctx, in)
	if err != nil {
		switch httpStatus(err) {
		case http.StatusPreconditionFailed, http.StatusConflict:
			return "", fmt.Errorf("put %s: %w", key, ErrPreconditionFailed)
		}
		return "", fmt.Errorf("put %s: %w", key, err)
	}
	return aws.ToString(out.ETag), nil
}

func httpStatus(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// MemObjects is Objects in memory, with the same conditional writes as S3.
// It is for tests: the service under test, and a restarted one, can share
// one.
type MemObjects struct {
	mu      sync.Mutex
	objects map[string]memObject
	version uint64
}

type memObject struct {
	body []byte
	etag string
}

var _ Objects = (*MemObjects)(nil)

// NewMemObjects returns an empty MemObjects.
func NewMemObjects() *MemObjects {
	return &MemObjects{objects: map[string]memObject{}}
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
	if cond.IfMatch != "" && (!exists || obj.etag != cond.IfMatch) {
		return "", fmt.Errorf("put %s: %w", key, ErrPreconditionFailed)
	}
	m.version++
	etag := strconv.Quote(strconv.FormatUint(m.version, 10))
	m.objects[key] = memObject{body: bytes.Clone(body), etag: etag}
	return etag, nil
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
	if cfg.Bucket == "" {
		return nil, errors.New("s3 metadata store: no bucket")
	}
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("s3 metadata store: %w", err)
	}
	return &S3Objects{Client: client, Bucket: cfg.Bucket}, nil
}

// NewS3 returns a Store in cfg's bucket, under cfg's prefix: the same bucket
// and prefix as the blob store's, so a scope's metadata sits next to the
// blobs (<prefix>/blobs/...), and a scope can't be named "blobs".
func NewS3(ctx context.Context, cfg blobstore.S3Config) (*Store, error) {
	objects, err := NewS3Objects(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return New(Config{Objects: objects, Prefix: cfg.Prefix})
}
