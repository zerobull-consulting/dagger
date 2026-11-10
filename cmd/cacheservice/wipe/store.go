package wipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3util"
)

// CallTimeout bounds each request to the store.
const CallTimeout = 30 * time.Second

// PageSize is the most keys one listing returns and one delete takes:
// S3's limit for both.
const PageSize = 1000

// ErrNotFound is a missing object.
var ErrNotFound = errors.New("not found")

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
// request (ListUploads, with FilterAllUploads, a page of the bucket's).
type Store interface {
	// List returns up to PageSize objects under prefix whose keys sort
	// after startAfter, in key order, and whether there are more.
	List(ctx context.Context, prefix, startAfter string) (objs []Object, more bool, err error)
	// ListUploads returns a page of the incomplete multipart uploads under
	// prefix, after the markers (both "" for the first), and the markers
	// of the next page (both "" when there is none).
	ListUploads(ctx context.Context, prefix, keyMarker, idMarker string) (ups []Upload, nextKey, nextID string, err error)
	// Get reads an object: ErrNotFound when it is missing.
	Get(ctx context.Context, key string) ([]byte, error)
	// Delete deletes up to PageSize keys, and returns those the store
	// reported failing.
	Delete(ctx context.Context, keys []string) ([]Failure, error)
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
	// FilterAllUploads lists the bucket's uploads and keeps those under the
	// prefix: for S3-compatible stores (a custom endpoint) that list
	// multipart uploads only by exact key, such as MinIO (gc.S3Bucket).
	FilterAllUploads bool
}

var _ Store = (*S3Store)(nil)

func (s *S3Store) List(ctx context.Context, prefix, startAfter string) ([]Object, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	in := &s3.ListObjectsV2Input{Bucket: &s.Bucket, Prefix: aws.String(prefix), MaxKeys: aws.Int32(PageSize)}
	if startAfter != "" {
		in.StartAfter = aws.String(startAfter)
	}
	out, err := s.Client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, false, fmt.Errorf("list %s: %w", prefix, err)
	}
	objs := make([]Object, 0, len(out.Contents))
	for _, o := range out.Contents {
		objs = append(objs, Object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size)})
	}
	return objs, aws.ToBool(out.IsTruncated), nil
}

func (s *S3Store) ListUploads(ctx context.Context, prefix, keyMarker, idMarker string) ([]Upload, string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	in := &s3.ListMultipartUploadsInput{Bucket: &s.Bucket, MaxUploads: aws.Int32(PageSize)}
	if !s.FilterAllUploads {
		in.Prefix = aws.String(prefix)
	}
	if keyMarker != "" || idMarker != "" {
		in.KeyMarker, in.UploadIdMarker = aws.String(keyMarker), aws.String(idMarker)
	}
	out, err := s.Client.ListMultipartUploads(ctx, in)
	if err != nil {
		return nil, "", "", fmt.Errorf("list uploads under %s: %w", prefix, err)
	}
	var ups []Upload
	for _, u := range out.Uploads {
		if k := aws.ToString(u.Key); strings.HasPrefix(k, prefix) {
			ups = append(ups, Upload{Key: k, ID: aws.ToString(u.UploadId)})
		}
	}
	if !aws.ToBool(out.IsTruncated) {
		return ups, "", "", nil
	}
	return ups, aws.ToString(out.NextKeyMarker), aws.ToString(out.NextUploadIdMarker), nil
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.Bucket, Key: &key})
	if err != nil {
		if s3util.HTTPStatus(err) == http.StatusNotFound {
			return nil, fmt.Errorf("get %s: %w", key, ErrNotFound)
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
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	ids := make([]types.ObjectIdentifier, len(keys))
	for i := range keys {
		ids[i] = types.ObjectIdentifier{Key: aws.String(keys[i])}
	}
	out, err := s.Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &s.Bucket,
		Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return nil, fmt.Errorf("delete %d objects: %w", len(keys), err)
	}
	failed := make([]Failure, 0, len(out.Errors))
	for _, e := range out.Errors {
		failed = append(failed, Failure{Key: aws.ToString(e.Key), Reason: aws.ToString(e.Code) + ": " + aws.ToString(e.Message)})
	}
	return failed, nil
}

func (s *S3Store) Abort(ctx context.Context, u Upload) error {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	_, err := s.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &s.Bucket, Key: &u.Key, UploadId: &u.ID})
	if err != nil && s3util.HTTPStatus(err) != http.StatusNotFound {
		return fmt.Errorf("abort upload of %s: %w", u.Key, err)
	}
	return nil
}
