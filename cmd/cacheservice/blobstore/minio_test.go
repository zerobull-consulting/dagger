package blobstore

// Tests of S3Store against a real S3-compatible store: the MinIO dev
// environment in hack/remote-cache, or any bucket (s3test keeps them to their
// own keys). They skip unless CACHESERVICE_S3_ENDPOINT is set;
// `hack/remote-cache/dev.sh test` starts MinIO and runs them.

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
)

// minioBucket is the bucket under test.
func minioBucket() string {
	if b := os.Getenv(EnvBucket); b != "" {
		return b
	}
	return "dagger-cache"
}

// newMinIOStore returns a store under a prefix of its own in the run's root,
// emptied when the test ends, and kept to the root by s3test's guard.
func newMinIOStore(t *testing.T, modify func(*S3Config)) *S3Store {
	t.Helper()
	if os.Getenv(EnvEndpoint) == "" {
		t.Skipf("%s is not set; start MinIO with hack/remote-cache/dev.sh up", EnvEndpoint)
	}
	cfg, err := S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	cfg.Bucket = minioBucket()
	scope := s3test.NewScope(t, cfg.Bucket)
	cfg.Prefix = scope.Prefix
	cfg.HTTPClient = scope.Guard
	// Cleaning up uses the real credentials, whatever modify does to the
	// store's.
	cleanup, err := NewS3Client(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope.DeleteOnCleanup(t, cleanup)
	if modify != nil {
		modify(&cfg)
	}
	s, err := NewS3(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// enginePut uploads body the way the engine does (engine/remotecache's
// httpPut): a bare PUT with Content-Length and no other headers.
func enginePut(t *testing.T, url string, body []byte) int {
	t.Helper()
	s3test.CheckURL(t, http.MethodPut, url, minioBucket())
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func httpGet(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}

func randomBlob(t *testing.T, size int) ([]byte, digest.Digest) {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return data, digest.FromBytes(data)
}

// stageAndPromote uploads body as the engine would to a fresh stage's PUT
// for d, and promotes it.
func stageAndPromote(t *testing.T, s *S3Store, d digest.Digest, size int64, body []byte) Promotion {
	t.Helper()
	ctx := context.Background()
	stage := NewStage()
	put, err := s.PresignPut(ctx, stage, d, size)
	if err != nil {
		t.Fatal(err)
	}
	if status := enginePut(t, put.URL, body); status != http.StatusOK {
		t.Fatalf("presigned PUT: status %d", status)
	}
	p, err := s.Promote(ctx, stage, d, size)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// objectExists reports whether key is in the bucket.
func objectExists(t *testing.T, s *S3Store, key string) bool {
	t.Helper()
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil && !isNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// listKeys lists the keys under prefix.
func listKeys(t *testing.T, s *S3Store, prefix string) []string {
	t.Helper()
	var keys []string
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys
}

func mustHas(t *testing.T, s *S3Store, d digest.Digest) bool {
	t.Helper()
	ok, err := s.Has(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// A blob uploaded with a presigned PUT is staged, not stored, until it is
// promoted; then it can be fetched with a presigned GET, and the staged
// object is gone.
func TestMinIOStorePutGet(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	blob, d := randomBlob(t, 1<<20+17)

	if mustHas(t, s, d) {
		t.Fatalf("%s is stored before its upload", d)
	}
	stage := NewStage()
	put, err := s.PresignPut(ctx, stage, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if status := enginePut(t, put.URL, blob); status != http.StatusOK {
		t.Fatalf("presigned PUT: status %d", status)
	}
	staged, _ := s.StagingKey(stage, d)
	if mustHas(t, s, d) || !objectExists(t, s, staged) {
		t.Fatalf("after the upload: stored %v, staged %v; want staged only", mustHas(t, s, d), objectExists(t, s, staged))
	}
	p, err := s.Promote(ctx, stage, d, int64(len(blob)))
	if err != nil || p.Outcome != Promoted || p.HashedBy != wantHashedBy(t, HashedByStore) || p.Bytes != int64(len(blob)) {
		t.Fatalf("promote: %+v, %v", p, err)
	}
	if !mustHas(t, s, d) || objectExists(t, s, staged) {
		t.Fatalf("after promotion: stored %v, staged %v; want stored only", mustHas(t, s, d), objectExists(t, s, staged))
	}

	key, _ := s.Key(d)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToInt64(head.ContentLength) != int64(len(blob)) {
		t.Fatalf("stored %d bytes, want %d", aws.ToInt64(head.ContentLength), len(blob))
	}

	get, err := s.PresignGet(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	status, data := httpGet(t, get.URL)
	if status != http.StatusOK {
		t.Fatalf("presigned GET: status %d", status)
	}
	if got := digest.FromBytes(data); got != d {
		t.Fatalf("presigned GET returned %d bytes with digest %s, want %s", len(data), got, d)
	}
}

// A blob PUT through one namespace's store is at <prefix>/<ns>/blobs/..., is
// stored for that namespace only, and another namespace's GET of its digest
// finds nothing: UploadURLs asks the other namespace for its own copy.
func TestMinIOStoreNamespaces(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	nsA, err := s.Namespace("team-a")
	if err != nil {
		t.Fatal(err)
	}
	nsB, err := s.Namespace("team-b")
	if err != nil {
		t.Fatal(err)
	}
	a, b := nsA.(*S3Store), nsB.(*S3Store)
	blob, d := randomBlob(t, 4096)
	stage := NewStage()
	put, err := a.PresignPut(ctx, stage, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if status := enginePut(t, put.URL, blob); status != http.StatusOK {
		t.Fatalf("presigned PUT: status %d", status)
	}
	staged, _ := a.StagingKey(stage, d)
	if want := s.prefix + "/team-a/staging/" + string(stage) + "/sha256/" + d.Encoded(); staged != want {
		t.Fatalf("staging key %s, want %s", staged, want)
	}
	// Promotion never crosses namespaces: team-b finds nothing staged.
	if p, err := b.Promote(ctx, stage, d, int64(len(blob))); err != nil || p.Outcome != Missing {
		t.Fatalf("team-b's promotion of team-a's upload: %+v, %v", p, err)
	}
	if p, err := a.Promote(ctx, stage, d, int64(len(blob))); err != nil || p.Outcome != Promoted {
		t.Fatalf("promote: %+v, %v", p, err)
	}
	key, _ := a.Key(d)
	if want := s.prefix + "/team-a/blobs/sha256/" + d.Encoded(); key != want {
		t.Fatalf("key %s, want %s", key, want)
	}
	if !mustHas(t, a, d) || mustHas(t, b, d) || mustHas(t, s, d) {
		t.Fatalf("stored in team-a %v, team-b %v, at the top %v", mustHas(t, a, d), mustHas(t, b, d), mustHas(t, s, d))
	}
	get, err := b.PresignGet(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := httpGet(t, get.URL); status != http.StatusNotFound {
		t.Fatalf("team-b's GET: status %d", status)
	}
	urls, err := UploadURLs(ctx, b, NewStage(), []Blob{{Digest: d, Size: int64(len(blob))}})
	if err != nil || len(urls) != 1 {
		t.Fatalf("team-b's upload URLs %v, %v", urls, err)
	}
}

// An existing blob is not requested again: once uploaded and promoted,
// UploadURLs leaves it out of the next upload request. An upload not yet
// promoted is asked for again.
func TestMinIOStoreUploadURLsSkipsExisting(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	first, d1 := randomBlob(t, 4096)
	second, d2 := randomBlob(t, 8192)
	blobs := []Blob{{Digest: d1, Size: int64(len(first))}, {Digest: d2, Size: int64(len(second))}}

	stage := NewStage()
	urls, err := UploadURLs(ctx, s, stage, blobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 {
		t.Fatalf("first request: %d URLs, want 2", len(urls))
	}
	if status := enginePut(t, urls[d1], first); status != http.StatusOK {
		t.Fatalf("PUT %s: status %d", d1, status)
	}
	if urls, _ := UploadURLs(ctx, s, NewStage(), blobs); len(urls) != 2 {
		t.Fatalf("before promotion: %d URLs, want 2", len(urls))
	}
	if p, err := s.Promote(ctx, stage, d1, int64(len(first))); err != nil || p.Outcome != Promoted {
		t.Fatalf("promote: %+v, %v", p, err)
	}

	stage = NewStage()
	urls, err = UploadURLs(ctx, s, stage, blobs)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := urls[d1]; ok || len(urls) != 1 || urls[d2] == "" {
		t.Fatalf("second request: URLs for %v, want only %s", keys(urls), d2)
	}
	if status := enginePut(t, urls[d2], second); status != http.StatusOK {
		t.Fatalf("PUT %s: status %d", d2, status)
	}
	if p, err := s.Promote(ctx, stage, d2, int64(len(second))); err != nil || p.Outcome != Promoted {
		t.Fatalf("promote: %+v, %v", p, err)
	}

	urls, err = UploadURLs(ctx, s, NewStage(), blobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 0 {
		t.Fatalf("third request: URLs for %v, want none", keys(urls))
	}
}

func keys(m map[digest.Digest]string) []digest.Digest {
	var out []digest.Digest
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The presigned PUT is bound to the size it was signed for: a longer or
// shorter body is refused and nothing is staged or stored.
func TestMinIOStorePutSizeBound(t *testing.T) {
	s := newMinIOStore(t, nil)
	blob, d := randomBlob(t, 4096)
	stage := NewStage()
	put, err := s.PresignPut(context.Background(), stage, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	staged, _ := s.StagingKey(stage, d)
	for _, body := range [][]byte{append(append([]byte{}, blob...), 0), blob[:len(blob)-1]} {
		if status := enginePut(t, put.URL, body); status/100 == 2 {
			t.Fatalf("PUT of %d bytes, signed for %d: status %d", len(body), len(blob), status)
		}
		if mustHas(t, s, d) || objectExists(t, s, staged) {
			t.Fatalf("PUT of %d bytes stored an object", len(body))
		}
	}
}

// Presigned URLs stop working at their expiry.
func TestMinIOStoreExpiry(t *testing.T) {
	s := newMinIOStore(t, func(c *S3Config) {
		c.PutExpiry = time.Second
		c.GetExpiry = time.Second
	})
	ctx := context.Background()
	blob, d := randomBlob(t, 128)
	stage := NewStage()
	put, err := s.PresignPut(ctx, stage, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	get, err := s.PresignGet(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if status := enginePut(t, put.URL, blob); status != http.StatusOK {
		t.Fatalf("fresh PUT: status %d", status)
	}
	// The GET is of the digest key: promote what the PUT staged.
	if p, err := s.Promote(ctx, stage, d, int64(len(blob))); err != nil || p.Outcome != Promoted {
		t.Fatalf("promote: %+v, %v", p, err)
	}
	time.Sleep(time.Until(get.ExpiresAt) + 1500*time.Millisecond)
	if status, _ := httpGet(t, get.URL); status != http.StatusForbidden {
		t.Fatalf("expired GET: status %d, want 403", status)
	}
	if status := enginePut(t, put.URL, blob); status != http.StatusForbidden {
		t.Fatalf("expired PUT: status %d, want 403", status)
	}
}

// A store with the wrong credentials reports an error from Has, not a
// missing blob, so the service doesn't hand out PUTs that will fail.
func TestMinIOStoreHasBadCredentials(t *testing.T) {
	s := newMinIOStore(t, func(c *S3Config) {
		c.Credentials = aws.NewCredentialsCache(aws.CredentialsProviderFunc(
			func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "nobody", SecretAccessKey: "wrong"}, nil
			}))
	})
	if ok, err := s.Has(context.Background(), digest.FromString("x")); err == nil {
		t.Fatalf("Has with bad credentials = %v, nil; want an error", ok)
	}
}
