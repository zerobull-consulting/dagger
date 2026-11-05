package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
)

// bareRequest sends a request the way the engine does: only Content-Length.
func bareRequest(t *testing.T, method, url string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

// The S3 proxy against a real store, which is what checks the signatures: the
// SDK's requests and presigned URLs a client signed for the proxy's address
// still work, in both ways of addressing the bucket, and what the store should
// refuse is still refused. The proxy's guard refuses what is outside the
// prefix, and the fault injection works through it. It runs with the store
// suites (hack/remote-cache/dev.sh test), against MinIO and any other bucket.
func TestMinIOProxySignsAgain(t *testing.T) {
	if os.Getenv(blobstore.EnvEndpoint) == "" {
		t.Skipf("%s is not set; start MinIO with hack/remote-cache/dev.sh up", blobstore.EnvEndpoint)
	}
	ctx := context.Background()
	base, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if base.Region == "" {
		base.Region = "us-east-1"
	}
	if base.Bucket == "" {
		base.Bucket = "dagger-cache"
	}
	if base.Credentials == nil {
		t.Skip("the proxy signs with static credentials; none are set")
	}
	direct := func(t *testing.T, scope *s3test.Scope, pathStyle bool) *s3.Client {
		cfg := base
		cfg.PathStyle, cfg.HTTPClient = pathStyle, scope.Guard
		c, err := blobstore.NewS3Client(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	modes := []struct {
		name      string
		pathStyle bool
	}{{"path-style", true}}
	// Only where the bucket is configured for it: a local endpoint has no
	// bucket subdomains, and HTTPS doesn't promise wildcard DNS or certificates.
	if strings.HasPrefix(base.Endpoint, "https://") && !base.PathStyle {
		modes = append(modes, struct {
			name      string
			pathStyle bool
		}{"virtual-hosted", false})
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			scope := s3test.NewScope(t, base.Bucket)
			scope.DeleteOnCleanup(t, direct(t, scope, mode.pathStyle))
			// Not bound to the endpoint: requests arrive at the proxy's own address.
			guard := s3test.NewGuard(base.Bucket, s3test.RunRoot(t), false)
			p := StartS3ProxyWith(t, "127.0.0.1", base.Endpoint, ProxyOptions{
				Guard:    guard,
				Upstream: &Upstream{Endpoint: base.Endpoint, Region: base.Region, Bucket: base.Bucket, PathStyle: mode.pathStyle, Credentials: base.Credentials},
			})
			cfg := base
			cfg.Endpoint, cfg.PathStyle, cfg.Prefix = p.URL, true, scope.Prefix
			c, err := blobstore.NewS3Client(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			bucket := aws.String(base.Bucket)

			// The SDK's own requests, as the service makes them.
			key := aws.String(scope.Prefix + "/sdk/with space+plus")
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("sdk")}); err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key}); err != nil {
				t.Fatalf("HeadObject: %v", err)
			}
			list, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket, Prefix: aws.String(scope.Prefix + "/")})
			if err != nil || len(list.Contents) != 1 {
				t.Fatalf("ListObjectsV2: %+v, %v", list, err)
			}
			src := aws.String(base.Bucket + "/" + scope.Prefix + "/sdk/src")
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String(scope.Prefix + "/sdk/src"), Body: strings.NewReader("copied")}); err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String(scope.Prefix + "/sdk/dst")})
			if err != nil {
				t.Fatalf("CreateMultipartUpload: %v", err)
			}
			part, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: bucket, Key: aws.String(scope.Prefix + "/sdk/dst"), UploadId: up.UploadId, PartNumber: aws.Int32(1), CopySource: src})
			if err != nil {
				t.Fatalf("UploadPartCopy: %v", err)
			}
			if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucket, Key: aws.String(scope.Prefix + "/sdk/dst"), UploadId: up.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.CopyPartResult.ETag}}}}); err != nil {
				t.Fatalf("CompleteMultipartUpload: %v", err)
			}
			if _, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String(scope.Prefix + "/sdk/dst")}}}}); err != nil {
				t.Fatalf("DeleteObjects: %v", err)
			}

			// Presigned URLs, as the engine uses them.
			store, err := nsStore(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			blob := bytes.Repeat([]byte("blob"), 1024)
			d := digest.FromBytes(blob)
			stage := blobstore.NewStage()
			put, err := store.PresignPut(ctx, stage, d, int64(len(blob)))
			if err != nil {
				t.Fatal(err)
			}
			if status, _ := bareRequest(t, http.MethodPut, put.URL, blob[:len(blob)-1]); status/100 == 2 {
				t.Fatalf("PUT of another size than signed: status %d", status)
			}
			if status, body := bareRequest(t, http.MethodPut, put.URL, blob); status != http.StatusOK {
				t.Fatalf("presigned PUT: status %d: %s", status, body)
			}
			if pr, err := store.Promote(ctx, stage, d, int64(len(blob))); err != nil || pr.Outcome != blobstore.Promoted {
				t.Fatalf("promote: %+v, %v", pr, err)
			}
			get, err := store.PresignGet(ctx, d)
			if err != nil {
				t.Fatal(err)
			}
			if status, data := bareRequest(t, http.MethodGet, get.URL, nil); status != http.StatusOK || !bytes.Equal(data, blob) {
				t.Fatalf("presigned GET: status %d, %d bytes", status, len(data))
			}

			// A tampered URL, and one past its expiry, are refused.
			tampered := strings.Replace(get.URL, "X-Amz-Expires=", "X-Amz-Expires=1", 1)
			if status, _ := bareRequest(t, http.MethodGet, tampered, nil); status != http.StatusForbidden {
				t.Fatalf("tampered GET: status %d, want 403", status)
			}
			short := cfg
			short.GetExpiry = time.Second
			shortStore, err := nsStore(ctx, short)
			if err != nil {
				t.Fatal(err)
			}
			expired, err := shortStore.PresignGet(ctx, d)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Until(expired.ExpiresAt) + 1500*time.Millisecond)
			if status, _ := bareRequest(t, http.MethodGet, expired.URL, nil); status != http.StatusForbidden {
				t.Fatalf("expired GET: status %d, want 403", status)
			}

			// Faults still apply.
			fault := p.Inject(&Fault{Name: "slow", Match: Methods("/fault", http.MethodPut), Status: http.StatusServiceUnavailable, Code: "SlowDown", Count: 1})
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String(scope.Prefix + "/fault"), Body: strings.NewReader("x")}); err != nil {
				t.Fatalf("PutObject after a retried 503: %v", err)
			}
			if p.Hits(fault) != 1 {
				t.Fatalf("the fault took %d requests", p.Hits(fault))
			}

			touched := guard.Touched()
			if len(touched) == 0 {
				t.Fatal("the proxy's guard saw nothing")
			}
			for _, key := range touched {
				if !strings.HasPrefix(key, scope.Prefix+"/") {
					t.Fatalf("touched %s, outside %s/", key, scope.Prefix)
				}
			}
			if refused := guard.Refused(); len(refused) != 0 {
				t.Fatalf("refused %v", refused)
			}
		})
	}
}

// What's outside the run's prefix is refused by the proxy, and doesn't reach
// the store, however it is signed.
func TestS3ProxyGuard(t *testing.T) {
	reached := 0
	target := startFakeStore(t, func() { reached++ })
	root := "test/run"
	g := s3test.NewGuard("bkt", root, false)
	p := StartS3ProxyWith(t, "127.0.0.1", target, ProxyOptions{Guard: g})
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodPut, "/bkt/elsewhere/k", http.StatusForbidden},
		{http.MethodDelete, "/bkt/test/other/k", http.StatusForbidden},
		{http.MethodPut, "/bkt?lifecycle", http.StatusForbidden},
		{http.MethodPut, "/other/test/run/k", http.StatusForbidden},
		{http.MethodPut, "/bkt/test/run/k", http.StatusOK},
		{http.MethodGet, "/bkt/elsewhere/k", http.StatusOK},
	} {
		if status, _ := bareRequest(t, tc.method, p.URL+tc.path, nil); status != tc.want {
			t.Errorf("%s %s: status %d, want %d", tc.method, tc.path, status, tc.want)
		}
	}
	if reached != 2 {
		t.Fatalf("%d requests reached the store, want the 2 allowed", reached)
	}
	if len(g.Refused()) != 4 {
		t.Fatalf("refused %v", g.Refused())
	}
}

// startFakeStore is a store that answers 200 to everything, and calls reached.
func startFakeStore(t *testing.T, reached func()) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached() }))
	t.Cleanup(srv.Close)
	return srv.URL
}

// With no static credentials, the SDK's default chain signs for the store, as
// it does for the service: the proxy has to use what it resolved, not the
// configuration's nil, and forward rather than panic.
func TestUpstreamUsesTheDefaultCredentialChain(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "from-the-chain")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	ctx := context.Background()
	cfg := blobstore.S3Config{Endpoint: "https://store.test", Region: "r", Bucket: "bkt", PathStyle: true}
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	u := newUpstream(cfg, client)
	if u.Credentials == nil {
		t.Fatal("no credentials")
	}
	creds, err := u.Credentials.Retrieve(ctx)
	if err != nil || creds.AccessKeyID != "from-the-chain" {
		t.Fatalf("credentials %q, %v", creds.AccessKeyID, err)
	}
}

// roundTripFunc records what the proxy sends the store, without a network.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// The proxy forwards in the addressing it is told, to the store's own host,
// with a signature for that host: the store's host name is what a real store
// checks it against, and either addressing passes against a store that takes
// both.
func TestS3ProxyForwardsToTheStore(t *testing.T) {
	ctx := context.Background()
	creds := credentials.NewStaticCredentialsProvider("id", "secret", "")
	for _, tc := range []struct {
		name               string
		pathStyle          bool
		wantHost, wantPath string
	}{
		{"path-style", true, "store.test", "/bkt/pre/k"},
		{"virtual-hosted", false, "bkt.store.test", "/pre/k"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []*http.Request
			p := StartS3ProxyWith(t, "127.0.0.1", "https://store.test", ProxyOptions{Upstream: &Upstream{
				Endpoint: "https://store.test", Region: "r", Bucket: "bkt", PathStyle: tc.pathStyle, Credentials: creds,
				Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					got = append(got, r)
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: r}, nil
				}),
			}})
			store, err := nsStore(ctx, blobstore.S3Config{Endpoint: p.URL, Region: "r", Bucket: "bkt", Prefix: "pre", PathStyle: true, Credentials: creds})
			if err != nil {
				t.Fatal(err)
			}
			client, err := blobstore.NewS3Client(ctx, blobstore.S3Config{Endpoint: p.URL, Region: "r", PathStyle: true, Credentials: creds})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("bkt"), Key: aws.String("pre/k")}); err != nil {
				t.Fatal(err)
			}
			d := digest.FromString("x")
			get, err := store.PresignGet(ctx, d)
			if err != nil {
				t.Fatal(err)
			}
			if status, body := bareRequest(t, http.MethodGet, get.URL, nil); status != http.StatusOK {
				t.Fatalf("presigned GET: status %d: %s", status, body)
			}
			if len(got) != 2 {
				t.Fatalf("%d requests reached the store", len(got))
			}
			head, presigned := got[0], got[1]
			for _, r := range got {
				if r.URL.Scheme != "https" || r.URL.Host != tc.wantHost || r.Host != tc.wantHost {
					t.Errorf("sent to %s://%s, Host %s; want https://%s", r.URL.Scheme, r.URL.Host, r.Host, tc.wantHost)
				}
			}
			if head.URL.Path != tc.wantPath {
				t.Errorf("path %s, want %s", head.URL.Path, tc.wantPath)
			}
			wantBlob := strings.Replace(tc.wantPath, "/pre/k", "/pre/ns/default/blobs/sha256/"+d.Encoded(), 1)
			if presigned.URL.Path != wantBlob {
				t.Errorf("presigned path %s, want %s", presigned.URL.Path, wantBlob)
			}

			// Signed for the store's host: not what the client sent.
			u, err := url.Parse(get.URL)
			if err != nil {
				t.Fatal(err)
			}
			if sent, sentByProxy := u.Query().Get("X-Amz-Signature"), presigned.URL.Query().Get("X-Amz-Signature"); sentByProxy == "" || sentByProxy == sent {
				t.Errorf("the proxy's signature %q, the client's %q: want a new one", sentByProxy, sent)
			}
			if presigned.URL.Query().Get("X-Amz-Expires") != u.Query().Get("X-Amz-Expires") || presigned.URL.Query().Get("X-Amz-Date") != u.Query().Get("X-Amz-Date") {
				t.Errorf("the proxy changed the expiry or date: %s", presigned.URL.RawQuery)
			}
		})
	}
}

// nsStore is the blob store of namespace "default" for cfg: blobs are kept
// only per namespace.
func nsStore(ctx context.Context, cfg blobstore.S3Config) (blobstore.Store, error) {
	s, err := blobstore.NewS3(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return s.Namespace("default")
}
