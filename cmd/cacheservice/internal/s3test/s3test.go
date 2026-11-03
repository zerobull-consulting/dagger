// Package s3test keeps tests that run against a real S3-compatible bucket to
// their own keys, so the bucket doesn't have to be disposable:
//
//   - a run refuses an empty or "/" prefix (CACHESERVICE_S3_PREFIX);
//   - everything it writes, deletes or aborts is under
//     <prefix>/<run-unique>/, which Guard enforces on every request;
//   - bucket configuration can't change, except the lifecycle configuration
//     of a bucket marked disposable (CACHESERVICE_TEST_DISPOSABLE_BUCKET=1);
//   - each test deletes its objects and aborts its uploads when it ends,
//     failed or not. A process killed before then (go test's -timeout)
//     leaves them under its run's prefix.
package s3test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// EnvPrefix is blobstore.EnvPrefix, which this package can't import: the
	// blob store's own tests import this package.
	EnvPrefix = "CACHESERVICE_S3_PREFIX"
	// EnvDisposableBucket, set to 1, marks the bucket as one whose lifecycle
	// configuration a test may replace. Leave it unset on a shared bucket.
	EnvDisposableBucket = "CACHESERVICE_TEST_DISPOSABLE_BUCKET"
)

// EnvStore names the store under test, for what tests expect of it: minio
// (the default) or spaces.
const EnvStore = "CACHESERVICE_TEST_STORE"

// Behavior is what the store under test does where stores differ. The tests
// pin it, so a store changing what it does fails them: update the README's
// table of results along with it.
type Behavior struct {
	// HashesCopies: a copy with ChecksumAlgorithm SHA256 returns the SHA-256.
	HashesCopies bool
	// CopyConditions: CopySourceIfMatch is honored on copies.
	CopyConditions bool
	// CompleteConditions: If-None-Match is honored on
	// CompleteMultipartUpload.
	CompleteConditions bool
	// QuotedIfMatch: If-Match on PutObject takes the ETag quoted, as the store
	// returns it.
	QuotedIfMatch bool
	// LifecycleAbortRule: a lifecycle rule with
	// AbortIncompleteMultipartUpload is accepted.
	LifecycleAbortRule bool
}

// StoreBehavior is the Behavior of the store EnvStore names.
func StoreBehavior(t testing.TB) Behavior {
	t.Helper()
	switch v := os.Getenv(EnvStore); v {
	case "", "minio":
		return Behavior{HashesCopies: true, CopyConditions: true, CompleteConditions: true, QuotedIfMatch: true}
	case "spaces":
		return Behavior{LifecycleAbortRule: true}
	default:
		t.Fatalf("%s=%q: want minio or spaces", EnvStore, v)
		return Behavior{}
	}
}

// Disposable reports whether the bucket is marked disposable.
func Disposable() bool { return os.Getenv(EnvDisposableBucket) == "1" }

// CleanPrefix validates a configured prefix, and returns it without the
// slashes around it.
func CleanPrefix(prefix string) (string, error) {
	p := strings.Trim(strings.TrimSpace(prefix), "/")
	if p == "" {
		return "", fmt.Errorf("%s is %q: tests against a real bucket need a prefix of their own, such as test (hack/remote-cache/dev.sh test sets one)", EnvPrefix, prefix)
	}
	if slices.Contains(strings.Split(p, "/"), "..") {
		return "", fmt.Errorf("%s is %q: it can't contain ..", EnvPrefix, prefix)
	}
	return p, nil
}

// runID is unique to this process: go test runs one per package.
var runID = sync.OnceValue(func() string {
	return time.Now().UTC().Format("20060102-150405") + "-" + randomHex(4)
})

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// RunRoot is <prefix>/<run-unique>, from the configured prefix: every key
// this process's tests touch is under it. It fails t on a prefix CleanPrefix
// refuses.
func RunRoot(t testing.TB) string {
	t.Helper()
	p, err := CleanPrefix(os.Getenv(EnvPrefix))
	if err != nil {
		t.Fatal(err)
	}
	return p + "/" + runID()
}

// Scope is one test's keys in the bucket, and the guard on them.
type Scope struct {
	// Prefix is the test's own prefix, without a trailing slash, under the
	// run's root.
	Prefix string
	// Guard is the HTTP client for every S3 client the test makes
	// (blobstore.S3Config.HTTPClient, s3.Options.HTTPClient).
	Guard *Guard

	bucket string
}

// NewScope returns a scope for t in bucket. When t ends, it fails t if the
// guard refused a request.
func NewScope(t testing.TB, bucket string) *Scope {
	t.Helper()
	root := RunRoot(t)
	g := NewGuard(bucket, root, Disposable())
	t.Cleanup(func() {
		for _, r := range g.Refused() {
			t.Errorf("s3test: a request was refused: %s", r)
		}
	})
	return &Scope{
		Prefix: root + "/" + t.Name() + "-" + randomHex(6),
		Guard:  g,
		bucket: bucket,
	}
}

// DeleteOnCleanup deletes the scope's objects and aborts its uploads when t
// ends, failed or not, with client, and fails t if any are left. Register it
// before anything the test starts that writes: cleanups run last to first.
func (s *Scope) DeleteOnCleanup(t testing.TB, client *s3.Client) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := DeletePrefix(ctx, client, s.bucket, s.Prefix); err != nil {
			t.Errorf("s3test: cleaning up %s: %v", s.Prefix, err)
		}
	})
}

// CheckURL fails t if a presigned URL's request would touch a key outside the
// run's root: the clients of presigned URLs don't go through a Guard.
func CheckURL(t testing.TB, method, rawURL, bucket string) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGuard(bucket, RunRoot(t), Disposable())
	if err := g.Check(&http.Request{Method: method, URL: u, Host: u.Host}); err != nil {
		t.Fatal(err)
	}
}

// Guard is an aws.HTTPClient that refuses requests that would change anything
// outside its root, or the bucket's configuration, so they never reach the
// store. Reads go through: they change nothing, and the code under test lists
// the bucket's uploads to filter them itself.
type Guard struct {
	bucket string
	root   string
	// lifecycle allows replacing the bucket's lifecycle configuration.
	lifecycle bool
	inner     aws.HTTPClient

	mu      sync.Mutex
	touched []string
	refused []string
}

// NewGuard returns a guard for keys under root in bucket, which sends what it
// allows with the SDK's default client.
func NewGuard(bucket, root string, lifecycle bool) *Guard {
	return &Guard{
		bucket:    bucket,
		root:      strings.Trim(root, "/"),
		lifecycle: lifecycle,
		inner:     awshttp.NewBuildableClient(),
	}
}

// WithClient makes the guard send what it allows with inner.
func (g *Guard) WithClient(inner aws.HTTPClient) *Guard {
	g.inner = inner
	return g
}

// Do implements aws.HTTPClient.
func (g *Guard) Do(r *http.Request) (*http.Response, error) {
	if err := g.CheckRecord(r); err != nil {
		return nil, refusal{err}
	}
	return g.inner.Do(r)
}

// CheckRecord is Check, and records a refusal for Refused.
func (g *Guard) CheckRecord(r *http.Request) error {
	err := g.Check(r)
	if err != nil {
		g.mu.Lock()
		g.refused = append(g.refused, err.Error())
		g.mu.Unlock()
	}
	return err
}

// refusal tells the SDK's retryer not to try again.
type refusal struct{ error }

func (refusal) RetryableError() bool { return false }
func (r refusal) Unwrap() error      { return r.error }

// Touched are the keys of the requests that changed something, as they were
// allowed: what a test asserts the guard saw.
func (g *Guard) Touched() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.touched)
}

// Refused describes the requests the guard refused.
func (g *Guard) Refused() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.refused)
}

// Check returns an error when r would change anything outside the guard's
// root, or the bucket's configuration. It reads a bulk delete's body to see
// its keys, and leaves it for the request.
func (g *Guard) Check(r *http.Request) error {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return nil
	}
	what := r.Method + " " + r.URL.Path
	key, ok := g.key(r)
	if !ok {
		return fmt.Errorf("s3test: refused %s: not in bucket %s", what, g.bucket)
	}
	q := r.URL.Query()
	if key != "" {
		return g.allow(what, key)
	}
	switch {
	case r.Method == http.MethodPost && q.Has("delete"):
		keys, err := bulkDeleteKeys(r)
		if err != nil {
			return fmt.Errorf("s3test: refused %s: %w", what, err)
		}
		return g.allow(what, keys...)
	case q.Has("lifecycle") && g.lifecycle:
		return nil
	case q.Has("lifecycle"):
		return fmt.Errorf("s3test: refused %s?lifecycle: it replaces the whole bucket's lifecycle configuration; set %s=1 only on a disposable bucket", what, EnvDisposableBucket)
	}
	return fmt.Errorf("s3test: refused %s?%s: it changes the bucket, not a key", what, r.URL.RawQuery)
}

func (g *Guard) allow(what string, keys ...string) error {
	for _, key := range keys {
		if !strings.HasPrefix(key, g.root+"/") || slices.Contains(strings.Split(key, "/"), "..") {
			return fmt.Errorf("s3test: refused %s: key %q is outside %s/", what, key, g.root)
		}
	}
	g.mu.Lock()
	g.touched = append(g.touched, keys...)
	g.mu.Unlock()
	return nil
}

// key is r's object key ("" for the bucket itself), for path-style and
// virtual-hosted addressing; ok is false when r is for another bucket.
func (g *Guard) key(r *http.Request) (key string, ok bool) {
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if strings.HasPrefix(host, g.bucket+".") {
		return path, true
	}
	if path == g.bucket {
		return "", true
	}
	rest, found := strings.CutPrefix(path, g.bucket+"/")
	return rest, found
}

// bulkDeleteKeys are the keys of a DeleteObjects request. The body is read
// and put back; one that can't be read refuses the request.
func bulkDeleteKeys(r *http.Request) ([]string, error) {
	if r.Body == nil {
		return nil, errors.New("a bulk delete without a body")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read the bulk delete: %w", err)
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	var del struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	if err := xml.Unmarshal(body, &del); err != nil {
		return nil, fmt.Errorf("parse the bulk delete: %w", err)
	}
	keys := make([]string, len(del.Objects))
	for i, o := range del.Objects {
		keys[i] = o.Key
	}
	return keys, nil
}

// DeletePrefix deletes every object under prefix and aborts every multipart
// upload under it, and returns an error if it couldn't or any are left.
// Uploads are listed for the whole bucket and filtered, since some stores
// list them only by exact key (MinIO).
func DeletePrefix(ctx context.Context, client *s3.Client, bucket, prefix string) error {
	if strings.Trim(prefix, "/") == "" {
		return errors.New("refusing to delete the whole bucket")
	}
	under := strings.TrimSuffix(prefix, "/") + "/"
	var errs []error
	pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &under})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("list: %w", err))
			break
		}
		for _, o := range page.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: o.Key}); err != nil {
				errs = append(errs, fmt.Errorf("delete %s: %w", aws.ToString(o.Key), err))
			}
		}
	}
	var keyMarker, uploadMarker *string
	for {
		out, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket, KeyMarker: keyMarker, UploadIdMarker: uploadMarker})
		if err != nil {
			errs = append(errs, fmt.Errorf("list uploads: %w", err))
			break
		}
		for _, u := range out.Uploads {
			if !strings.HasPrefix(aws.ToString(u.Key), under) {
				continue
			}
			if _, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: u.Key, UploadId: u.UploadId}); err != nil {
				errs = append(errs, fmt.Errorf("abort %s: %w", aws.ToString(u.Key), err))
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker, uploadMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	left, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &under, MaxKeys: aws.Int32(1)})
	if err != nil {
		return fmt.Errorf("list what is left: %w", err)
	}
	if len(left.Contents) > 0 {
		return fmt.Errorf("objects left, such as %s", aws.ToString(left.Contents[0].Key))
	}
	return nil
}
