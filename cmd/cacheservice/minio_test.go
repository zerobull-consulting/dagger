package main

// Tests against a real S3-compatible store: the MinIO dev environment in
// hack/remote-cache. They check, through the raw SDK, what the blob store
// (blobstore, #5) and the metadata store (metastore, #6) rely on. They skip
// unless CACHESERVICE_S3_ENDPOINT is set;
// `hack/remote-cache/dev.sh test` starts MinIO and runs them.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type minioTestEnv struct {
	client *s3.Client
	bucket string
	// prefix holds this run's objects, so runs don't see each other's.
	prefix string
}

func newMinIOTestEnv(t *testing.T) *minioTestEnv {
	t.Helper()
	endpoint := os.Getenv("CACHESERVICE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("CACHESERVICE_S3_ENDPOINT is not set; start MinIO with hack/remote-cache/dev.sh up")
	}
	env := func(name, def string) string {
		if v := os.Getenv(name); v != "" {
			return v
		}
		return def
	}
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       env("CACHESERVICE_S3_REGION", "us-east-1"),
		Credentials: credentials.NewStaticCredentialsProvider(
			env("CACHESERVICE_S3_ACCESS_KEY_ID", "dagger-dev"),
			env("CACHESERVICE_S3_SECRET_ACCESS_KEY", "dagger-dev-secret"),
			"",
		),
		UsePathStyle: env("CACHESERVICE_S3_PATH_STYLE", "true") == "true",
		// Without this, the SDK adds checksum headers to presigned PUTs, and
		// the engine's upload sends none of them.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	})
	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	e := &minioTestEnv{
		client: client,
		bucket: env("CACHESERVICE_S3_BUCKET", "dagger-cache"),
		prefix: fmt.Sprintf("test/%s-%s", t.Name(), hex.EncodeToString(suffix)),
	}
	t.Cleanup(e.deleteAll)
	return e
}

func (e *minioTestEnv) key(rest string) string { return e.prefix + "/" + rest }

func (e *minioTestEnv) deleteAll() {
	ctx := context.Background()
	pages := s3.NewListObjectsV2Paginator(e.client, &s3.ListObjectsV2Input{
		Bucket: &e.bucket,
		Prefix: aws.String(e.prefix + "/"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return
		}
		for _, obj := range page.Contents {
			_, _ = e.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &e.bucket, Key: obj.Key})
		}
	}
}

// exists reports whether key is in the bucket, the way the blob store's Has
// will: HeadObject, with 404 as false.
func (e *minioTestEnv) exists(ctx context.Context, t *testing.T, key string) (bool, int64) {
	t.Helper()
	out, err := e.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &e.bucket, Key: &key})
	if httpStatus(err) == http.StatusNotFound {
		return false, 0
	}
	if err != nil {
		t.Fatalf("HeadObject %s: %v", key, err)
	}
	return true, aws.ToInt64(out.ContentLength)
}

func httpStatus(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// enginePut uploads body the way the engine does (engine/remotecache's
// httpPut): a bare PUT with Content-Length and no other headers.
func enginePut(ctx context.Context, url string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func httpGet(ctx context.Context, url string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func randomBlob(t *testing.T, size int) ([]byte, string) {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:])
}

// A blob goes up through a presigned PUT and comes back through a presigned
// GET, keyed by its digest, the way #5 will store layers.
func TestMinIOPresignedPutGet(t *testing.T) {
	e := newMinIOTestEnv(t)
	ctx := context.Background()
	presign := s3.NewPresignClient(e.client)

	blob, hexDigest := randomBlob(t, 1<<20+17)
	key := e.key("blobs/sha256/" + hexDigest)

	if ok, _ := e.exists(ctx, t, key); ok {
		t.Fatalf("%s exists before upload", key)
	}

	put, err := presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:        &e.bucket,
		Key:           &key,
		ContentLength: aws.Int64(int64(len(blob))),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	// The engine's PUT sends no headers beyond these, so the signature must
	// need no others.
	for name := range put.SignedHeader {
		switch strings.ToLower(name) {
		case "host", "content-length":
		default:
			t.Errorf("presigned PUT signs header %q, which the engine won't send", name)
		}
	}
	status, err := enginePut(ctx, put.URL, blob)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("presigned PUT: status %d", status)
	}

	ok, size := e.exists(ctx, t, key)
	if !ok || size != int64(len(blob)) {
		t.Fatalf("after upload: exists %v, size %d; want true, %d", ok, size, len(blob))
	}

	get, err := presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &e.bucket, Key: &key},
		s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	status, data, err := httpGet(ctx, get.URL)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("presigned GET: status %d", status)
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != hexDigest {
		t.Fatalf("presigned GET returned %d bytes with the wrong digest", len(data))
	}
}

// A presigned PUT is bound to the size it was signed for, so an engine can't
// store more (or less) than the service agreed to.
func TestMinIOPresignedPutSizeBound(t *testing.T) {
	e := newMinIOTestEnv(t)
	ctx := context.Background()
	presign := s3.NewPresignClient(e.client)

	blob, hexDigest := randomBlob(t, 4096)
	key := e.key("blobs/sha256/" + hexDigest)
	put, err := presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:        &e.bucket,
		Key:           &key,
		ContentLength: aws.Int64(int64(len(blob))),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"one byte longer", append(append([]byte{}, blob...), 0)},
		{"one byte shorter", blob[:len(blob)-1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, err := enginePut(ctx, put.URL, tc.body)
			if err != nil {
				t.Fatal(err)
			}
			if status/100 == 2 {
				t.Fatalf("PUT of %d bytes, signed for %d, succeeded: status %d", len(tc.body), len(blob), status)
			}
			if ok, _ := e.exists(ctx, t, key); ok {
				t.Fatalf("the PUT of %d bytes stored an object", len(tc.body))
			}
		})
	}
}

// An expired presigned GET is refused, so long builds will need fresh URLs:
// renew -> renewed, still to do in #9. The service answers renew with
// Unavailable today.
func TestMinIOPresignedGetExpires(t *testing.T) {
	e := newMinIOTestEnv(t)
	ctx := context.Background()
	presign := s3.NewPresignClient(e.client)

	key := e.key("blobs/sha256/expiry")
	if _, err := e.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &e.bucket, Key: &key, Body: strings.NewReader("x"),
	}); err != nil {
		t.Fatal(err)
	}
	get, err := presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &e.bucket, Key: &key},
		s3.WithPresignExpires(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	status, _, err := httpGet(ctx, get.URL)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusForbidden {
		t.Fatalf("expired presigned GET: status %d, want 403", status)
	}
}

// MinIO honors conditional writes, which #6 needs for atomic index updates:
// If-None-Match: * creates only, and If-Match replaces only the version read.
func TestMinIOConditionalWrites(t *testing.T) {
	e := newMinIOTestEnv(t)
	ctx := context.Background()
	key := e.key("index.json")

	first, err := e.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &e.bucket, Key: &key, Body: strings.NewReader(`{"v":1}`),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		t.Fatalf("create with If-None-Match: %v", err)
	}
	_, err = e.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &e.bucket, Key: &key, Body: strings.NewReader(`{"v":"other"}`),
		IfNoneMatch: aws.String("*"),
	})
	if httpStatus(err) != http.StatusPreconditionFailed {
		t.Fatalf("second create with If-None-Match: %v, want 412", err)
	}

	second, err := e.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &e.bucket, Key: &key, Body: strings.NewReader(`{"v":2}`),
		IfMatch: first.ETag,
	})
	if err != nil {
		t.Fatalf("update with current If-Match: %v", err)
	}
	_, err = e.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &e.bucket, Key: &key, Body: strings.NewReader(`{"v":"stale"}`),
		IfMatch: first.ETag,
	})
	if httpStatus(err) != http.StatusPreconditionFailed {
		t.Fatalf("update with stale If-Match: %v, want 412", err)
	}

	out, err := e.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &e.bucket, Key: &key})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, _ := io.ReadAll(out.Body)
	if string(body) != `{"v":2}` || aws.ToString(out.ETag) != aws.ToString(second.ETag) {
		t.Fatalf("index holds %s (ETag %s), want the If-Match update", body, aws.ToString(out.ETag))
	}
}
