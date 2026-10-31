package blobstore

// Tests of S3Store against a real S3-compatible store: the MinIO dev
// environment in hack/remote-cache. They skip unless CACHESERVICE_S3_ENDPOINT
// is set; `hack/remote-cache/dev.sh test` starts MinIO and runs them.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencontainers/go-digest"
)

// newMinIOStore returns a store under a prefix of its own, emptied when the
// test ends.
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
	if cfg.Bucket == "" {
		cfg.Bucket = "dagger-cache"
	}
	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	cfg.Prefix = fmt.Sprintf("test/%s-%s", t.Name(), hex.EncodeToString(suffix))
	if modify != nil {
		modify(&cfg)
	}
	s, err := NewS3(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { deletePrefix(s) })
	return s
}

func deletePrefix(s *S3Store) {
	ctx := context.Background()
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: aws.String(s.prefix + "/"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return
		}
		for _, obj := range page.Contents {
			_, _ = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: obj.Key})
		}
	}
}

// enginePut uploads body the way the engine does (engine/remotecache's
// httpPut): a bare PUT with Content-Length and no other headers.
func enginePut(t *testing.T, url string, body []byte) int {
	t.Helper()
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

func mustHas(t *testing.T, s *S3Store, d digest.Digest) bool {
	t.Helper()
	ok, err := s.Has(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// A blob uploaded with a presigned PUT can be fetched with a presigned GET.
func TestMinIOStorePutGet(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	blob, d := randomBlob(t, 1<<20+17)

	if mustHas(t, s, d) {
		t.Fatalf("%s is stored before its upload", d)
	}
	put, err := s.PresignPut(ctx, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if status := enginePut(t, put.URL, blob); status != http.StatusOK {
		t.Fatalf("presigned PUT: status %d", status)
	}
	if !mustHas(t, s, d) {
		t.Fatalf("%s is not stored after its upload", d)
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

// An existing blob is not requested again: once uploaded, UploadURLs leaves
// it out of the next upload request.
func TestMinIOStoreUploadURLsSkipsExisting(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	first, d1 := randomBlob(t, 4096)
	second, d2 := randomBlob(t, 8192)
	blobs := []Blob{{Digest: d1, Size: int64(len(first))}, {Digest: d2, Size: int64(len(second))}}

	urls, err := UploadURLs(ctx, s, blobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 {
		t.Fatalf("first request: %d URLs, want 2", len(urls))
	}
	if status := enginePut(t, urls[d1], first); status != http.StatusOK {
		t.Fatalf("PUT %s: status %d", d1, status)
	}

	urls, err = UploadURLs(ctx, s, blobs)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := urls[d1]; ok || len(urls) != 1 || urls[d2] == "" {
		t.Fatalf("second request: URLs for %v, want only %s", keys(urls), d2)
	}
	if status := enginePut(t, urls[d2], second); status != http.StatusOK {
		t.Fatalf("PUT %s: status %d", d2, status)
	}

	urls, err = UploadURLs(ctx, s, blobs)
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
// shorter body is refused and nothing is stored.
func TestMinIOStorePutSizeBound(t *testing.T) {
	s := newMinIOStore(t, nil)
	blob, d := randomBlob(t, 4096)
	put, err := s.PresignPut(context.Background(), d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range [][]byte{append(append([]byte{}, blob...), 0), blob[:len(blob)-1]} {
		if status := enginePut(t, put.URL, body); status/100 == 2 {
			t.Fatalf("PUT of %d bytes, signed for %d: status %d", len(body), len(blob), status)
		}
		if mustHas(t, s, d) {
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
	put, err := s.PresignPut(ctx, d, int64(len(blob)))
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

// Known gap, pinned on purpose: S3 checks a presigned PUT's size, not its
// content. A same-size body that doesn't hash to the digest is stored under
// that digest's key, Has then reports it, and UploadURLs never asks for the
// right blob again. Downloading engines refuse the blob (containerd checks the
// digest), so builds stay correct, but the cache entry fails until the object
// is deleted. When uploads are verified before they reach the digest key,
// this test should fail: replace it with one that the bad upload is refused.
func TestMinIOStoreKnownGapMisdeclaredDigest(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	blob, d := randomBlob(t, 4096)
	wrong := append([]byte{}, blob...)
	wrong[0] ^= 0xff

	put, err := s.PresignPut(ctx, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if status := enginePut(t, put.URL, wrong); status != http.StatusOK {
		t.Fatalf("PUT of the wrong content: status %d; the store now refuses it, so replace this test", status)
	}
	if !mustHas(t, s, d) {
		t.Fatal("the wrong content isn't reported under the digest; replace this test")
	}
	urls, err := UploadURLs(ctx, s, []Blob{{Digest: d, Size: int64(len(blob))}})
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 0 {
		t.Fatal("UploadURLs asks for the blob again; replace this test")
	}
	get, err := s.PresignGet(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	status, data := httpGet(t, get.URL)
	if status != http.StatusOK || digest.FromBytes(data) == d {
		t.Fatalf("GET: status %d, digest matches %v; want the wrong content served", status, digest.FromBytes(data) == d)
	}
}
