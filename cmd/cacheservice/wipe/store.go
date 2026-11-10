package wipe

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3util"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// CallTimeout bounds each request to the store.
const CallTimeout = 30 * time.Second

// PageSize is the most keys one listing returns and one delete takes:
// S3's limit for both.
const PageSize = 1000

// Object is an object as a listing shows it.
type Object struct {
	Key  string
	Size int64
}

// Upload is an incomplete multipart upload.
type Upload struct {
	Key, ID string
}

// Failure is one key a delete couldn't remove, with the store's reason.
type Failure struct {
	Key, Reason string
}

// Store is what the command needs of the bucket. Every method is one
// request.
type Store interface {
	// List returns up to PageSize objects under prefix whose keys sort
	// after startAfter, in key order, and whether there are more.
	List(ctx context.Context, prefix, startAfter string) (objs []Object, more bool, err error)
	// ListUploads returns a page of the incomplete multipart uploads the
	// store lists for prefix ("": the bucket's), after the markers (both
	// "" for the first), and the markers of the next page (both "" when
	// there is none).
	ListUploads(ctx context.Context, prefix, keyMarker, idMarker string) (ups []Upload, nextKey, nextID string, err error)
	// UploadsByExactKey reports a store that may list multipart uploads
	// only by exact key (MinIO): when a listing by prefix finds none, the
	// command lists the bucket's and keeps those under the prefix.
	UploadsByExactKey() bool
	// Get reads up to MaxMarkerBytes+1 bytes of an object:
	// metastore.ErrNotFound when it is missing, metastore.ErrDenied when
	// the key may not read it.
	Get(ctx context.Context, key string) ([]byte, error)
	// Delete deletes up to PageSize keys, and returns those the store
	// reported failing. When a request fails, it returns the error and the
	// failures it had collected before, which the caller still records.
	Delete(ctx context.Context, keys []string) ([]Failure, error)
	// Versioning is the bucket's versioning status: "" when it was never
	// enabled, else Enabled or Suspended. metastore.ErrDenied when the key
	// may not read it.
	Versioning(ctx context.Context) (string, error)
	// Abort aborts an upload; one already gone is not an error.
	Abort(ctx context.Context, u Upload) error
}

// Opener makes the Store for an S3 configuration.
type Opener func(ctx context.Context, cfg blobstore.S3Config) (Store, error)

// OpenS3 is the Opener of the real command: the service's own S3 client.
func OpenS3(ctx context.Context, cfg blobstore.S3Config) (Store, error) {
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3Store{Client: client, Bucket: cfg.Bucket, FilterAllUploads: cfg.Endpoint != ""}, nil
}

// S3Store is a Store in S3. Every request is bounded by CallTimeout.
type S3Store struct {
	Client *s3.Client
	Bucket string
	// FilterAllUploads is UploadsByExactKey: set for S3-compatible stores
	// (a custom endpoint), some of which list multipart uploads only by
	// exact key, such as MinIO (gc.S3Bucket).
	FilterAllUploads bool
}

func (s *S3Store) UploadsByExactKey() bool { return s.FilterAllUploads }

var _ Store = (*S3Store)(nil)

func (s *S3Store) List(ctx context.Context, prefix, startAfter string) ([]Object, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	// URL-encoded keys: a key holding a character XML can't carry (such as
	// U+0001, which MinIO stores) otherwise fails the whole page's parse.
	in := &s3.ListObjectsV2Input{Bucket: &s.Bucket, Prefix: aws.String(prefix), MaxKeys: aws.Int32(PageSize), EncodingType: types.EncodingTypeUrl}
	if startAfter != "" {
		in.StartAfter = aws.String(startAfter)
	}
	out, err := s.Client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, false, fmt.Errorf("list %s: %w", prefix, err)
	}
	objs := make([]Object, 0, len(out.Contents))
	for _, o := range out.Contents {
		k, err := decodeKey(aws.ToString(o.Key))
		if err != nil {
			return nil, false, fmt.Errorf("list %s: %w", prefix, err)
		}
		objs = append(objs, Object{Key: k, Size: aws.ToInt64(o.Size)})
	}
	return objs, aws.ToBool(out.IsTruncated), nil
}

func (s *S3Store) ListUploads(ctx context.Context, prefix, keyMarker, idMarker string) ([]Upload, string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	in := &s3.ListMultipartUploadsInput{Bucket: &s.Bucket, MaxUploads: aws.Int32(PageSize), EncodingType: types.EncodingTypeUrl}
	if prefix != "" {
		in.Prefix = aws.String(prefix)
	}
	// Each marker only when the last page gave it.
	if keyMarker != "" {
		in.KeyMarker = aws.String(keyMarker)
	}
	if idMarker != "" {
		in.UploadIdMarker = aws.String(idMarker)
	}
	out, err := s.Client.ListMultipartUploads(ctx, in)
	if err != nil {
		return nil, "", "", fmt.Errorf("list uploads under %s: %w", prefix, err)
	}
	ups := make([]Upload, 0, len(out.Uploads))
	for _, u := range out.Uploads {
		k, err := decodeKey(aws.ToString(u.Key))
		if err != nil {
			return nil, "", "", fmt.Errorf("list uploads under %s: %w", prefix, err)
		}
		ups = append(ups, Upload{Key: k, ID: aws.ToString(u.UploadId)})
	}
	if !aws.ToBool(out.IsTruncated) {
		return ups, "", "", nil
	}
	nextKey, err := decodeKey(aws.ToString(out.NextKeyMarker))
	if err != nil {
		return nil, "", "", fmt.Errorf("list uploads under %s: %w", prefix, err)
	}
	return ups, nextKey, aws.ToString(out.NextUploadIdMarker), nil
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.Bucket, Key: &key})
	if err != nil {
		if s3util.HTTPStatus(err) == http.StatusNotFound {
			return nil, fmt.Errorf("get %s: %w", key, metastore.ErrNotFound)
		}
		if s3util.Denied(err) {
			return nil, fmt.Errorf("get %s: %w: %w", key, metastore.ErrDenied, err)
		}
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	// The marker is a few hundred bytes: read at most MaxMarkerBytes+1.
	body, err := io.ReadAll(io.LimitReader(out.Body, MaxMarkerBytes+1))
	if err != nil {
		return nil, fmt.Errorf("get %s: read body: %w", key, err)
	}
	return body, nil
}

func (s *S3Store) Delete(ctx context.Context, keys []string) ([]Failure, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) > PageSize {
		return nil, fmt.Errorf("delete: %d keys, over %d", len(keys), PageSize)
	}
	// A key XML can't carry goes in a request of its own: MinIO answers a
	// DeleteObjects of one without an error and keeps it, and deletes it
	// with a DeleteObject (both seen on the dev MinIO).
	var batch []string
	var failed []Failure
	for _, k := range keys {
		if xmlSafe(k) {
			batch = append(batch, k)
			continue
		}
		if err := s.deleteOne(ctx, k); err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			failed = append(failed, Failure{Key: k, Reason: err.Error()})
		}
	}
	if len(batch) == 0 {
		return failed, nil
	}
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	ids := make([]types.ObjectIdentifier, len(batch))
	for i := range batch {
		ids[i] = types.ObjectIdentifier{Key: aws.String(batch[i])}
	}
	out, err := s.Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &s.Bucket,
		Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return failed, fmt.Errorf("delete %d objects: %w", len(batch), err)
	}
	for _, e := range out.Errors {
		failed = append(failed, Failure{Key: aws.ToString(e.Key), Reason: aws.ToString(e.Code) + ": " + aws.ToString(e.Message)})
	}
	return failed, nil
}

func (s *S3Store) deleteOne(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	if _, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.Bucket, Key: &key}); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

func (s *S3Store) Versioning(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	out, err := s.Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: &s.Bucket})
	if err != nil {
		if s3util.Denied(err) {
			return "", fmt.Errorf("get the bucket's versioning: %w: %w", metastore.ErrDenied, err)
		}
		return "", fmt.Errorf("get the bucket's versioning: %w", err)
	}
	return string(out.Status), nil
}

// xmlSafe reports whether XML 1.0 can carry k: valid UTF-8, of the
// characters XML allows.
func xmlSafe(k string) bool {
	if !utf8.ValidString(k) {
		return false
	}
	for _, r := range k {
		switch {
		case r == 0x9 || r == 0xA || r == 0xD,
			r >= 0x20 && r <= 0xD7FF,
			r >= 0xE000 && r <= 0xFFFD,
			r >= 0x10000 && r <= 0x10FFFF:
		default:
			return false
		}
	}
	return true
}

func (s *S3Store) Abort(ctx context.Context, u Upload) error {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	_, err := s.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &s.Bucket, Key: &u.Key, UploadId: &u.ID})
	if err != nil && s3util.HTTPStatus(err) != http.StatusNotFound {
		return fmt.Errorf("abort upload of %q: %w", u.Key, err)
	}
	return nil
}

// decodeKey is a key of a listing asked for with EncodingType url.
func decodeKey(k string) (string, error) {
	d, err := url.QueryUnescape(k)
	if err != nil {
		return "", fmt.Errorf("the store listed a key it didn't URL-encode, %q: %w", k, err)
	}
	return d, nil
}
