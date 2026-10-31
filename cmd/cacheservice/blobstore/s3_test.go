package blobstore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/opencontainers/go-digest"
)

var testDigest = digest.FromString("layer")

func testConfig() S3Config {
	return S3Config{
		Endpoint:    "http://s3.test:9000",
		Region:      "us-east-1",
		Bucket:      "cache",
		Prefix:      "team",
		PathStyle:   true,
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	}
}

func newTestStore(t *testing.T, cfg S3Config) *S3Store {
	t.Helper()
	s, err := NewS3(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBlobKey(t *testing.T) {
	hex := testDigest.Encoded()
	for _, tc := range []struct {
		prefix string
		want   string
	}{
		{"", "blobs/sha256/" + hex},
		{"team", "team/blobs/sha256/" + hex},
		{"/team/a/", "team/a/blobs/sha256/" + hex},
	} {
		cfg := testConfig()
		cfg.Prefix = tc.prefix
		got, err := newTestStore(t, cfg).Key(testDigest)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("prefix %q: key %q, want %q", tc.prefix, got, tc.want)
		}
	}

	sha512 := digest.SHA512.FromString("layer")
	got, err := blobKey("", sha512)
	if err != nil {
		t.Fatal(err)
	}
	if want := "blobs/sha512/" + sha512.Encoded(); got != want {
		t.Errorf("sha512 key %q, want %q", got, want)
	}
}

func TestBlobKeyRejectsBadDigests(t *testing.T) {
	for _, d := range []digest.Digest{
		"",
		"sha256:short",
		"sha256:../../../../etc/passwd",
		"md5:d41d8cd98f00b204e9800998ecf8427e",
		digest.Digest("sha256:" + strings.Repeat("A", 64)),
	} {
		if key, err := blobKey("p", d); err == nil {
			t.Errorf("digest %q: key %q, want an error", d, key)
		}
	}
}

func TestNewS3Validates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*S3Config)
		want   string
	}{
		{"no bucket", func(c *S3Config) { c.Bucket = "" }, "no bucket"},
		{"no region", func(c *S3Config) { c.Region = "" }, "no region"},
		{"PUT expiry too short", func(c *S3Config) { c.PutExpiry = time.Millisecond }, "PUT expiry"},
		{"GET expiry too long", func(c *S3Config) { c.GetExpiry = 8 * 24 * time.Hour }, "GET expiry"},
		{"negative expiry", func(c *S3Config) { c.PutExpiry = -time.Minute }, "PUT expiry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.modify(&cfg)
			_, err := NewS3(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

func TestNewS3DefaultCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "ENVID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ENVSECRET")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	cfg := testConfig()
	cfg.Credentials = nil
	s := newTestStore(t, cfg)
	addr, err := s.PresignGet(context.Background(), testDigest)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(addr.URL)
	if cred := u.Query().Get("X-Amz-Credential"); !strings.HasPrefix(cred, "ENVID/") {
		t.Fatalf("X-Amz-Credential = %q, want the environment's key", cred)
	}
}

func parseURL(t *testing.T, raw string) (*url.URL, url.Values) {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u, u.Query()
}

func TestPresignPut(t *testing.T) {
	s := newTestStore(t, testConfig())
	before := time.Now()
	addr, err := s.PresignPut(context.Background(), testDigest, 12345)
	if err != nil {
		t.Fatal(err)
	}
	u, q := parseURL(t, addr.URL)
	if u.Host != "s3.test:9000" {
		t.Errorf("host %q, want the endpoint's", u.Host)
	}
	if want := "/cache/team/blobs/sha256/" + testDigest.Encoded(); u.Path != want {
		t.Errorf("path %q, want %q", u.Path, want)
	}
	if got := q.Get("X-Amz-Expires"); got != strconv.Itoa(int(DefaultPutExpiry.Seconds())) {
		t.Errorf("X-Amz-Expires = %q, want %v", got, DefaultPutExpiry)
	}
	// The engine sends Host and Content-Length only, so only they may be
	// signed; Content-Length being signed is what bounds the size.
	signed := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	if strings.Join(signed, ";") != "content-length;host" {
		t.Errorf("X-Amz-SignedHeaders = %q, want content-length;host", q.Get("X-Amz-SignedHeaders"))
	}
	for name := range q {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-checksum") || strings.ToLower(name) == "x-amz-sdk-checksum-algorithm" {
			t.Errorf("URL carries %s, which the engine's PUT can't satisfy", name)
		}
	}
	if addr.ExpiresAt.Before(before.Add(DefaultPutExpiry)) || addr.ExpiresAt.After(time.Now().Add(DefaultPutExpiry)) {
		t.Errorf("ExpiresAt %v is not %v after signing", addr.ExpiresAt, DefaultPutExpiry)
	}
}

// SigV4 doesn't sign a Content-Length of 0, so an empty blob's PUT could
// not be bounded: it is refused.
func TestPresignPutZeroSize(t *testing.T) {
	s := newTestStore(t, testConfig())
	_, err := s.PresignPut(context.Background(), digest.FromString(""), 0)
	if !errors.Is(err, ErrNotPositiveSize) {
		t.Fatalf("err = %v, want ErrNotPositiveSize", err)
	}
}

func TestPresignPutRejects(t *testing.T) {
	s := newTestStore(t, testConfig())
	if _, err := s.PresignPut(context.Background(), testDigest, -1); err == nil {
		t.Error("negative size: no error")
	}
	if _, err := s.PresignPut(context.Background(), "sha256:bad", 1); err == nil {
		t.Error("bad digest: no error")
	}
	if _, err := s.PresignGet(context.Background(), "sha256:bad"); err == nil {
		t.Error("bad digest GET: no error")
	}
}

func TestPresignGet(t *testing.T) {
	cfg := testConfig()
	cfg.GetExpiry = 90 * time.Minute
	s := newTestStore(t, cfg)
	addr, err := s.PresignGet(context.Background(), testDigest)
	if err != nil {
		t.Fatal(err)
	}
	u, q := parseURL(t, addr.URL)
	if want := "/cache/team/blobs/sha256/" + testDigest.Encoded(); u.Path != want {
		t.Errorf("path %q, want %q", u.Path, want)
	}
	if got := q.Get("X-Amz-Expires"); got != "5400" {
		t.Errorf("X-Amz-Expires = %q, want 5400", got)
	}
	if got := q.Get("X-Amz-SignedHeaders"); got != "host" {
		t.Errorf("X-Amz-SignedHeaders = %q, want host", got)
	}
	if time.Until(addr.ExpiresAt) > 90*time.Minute || time.Until(addr.ExpiresAt) < 89*time.Minute {
		t.Errorf("ExpiresAt %v is not 90m away", addr.ExpiresAt)
	}
}

func TestPresignVirtualHostedStyle(t *testing.T) {
	cfg := testConfig()
	cfg.Endpoint = ""
	cfg.PathStyle = false
	cfg.Region = "eu-west-1"
	s := newTestStore(t, cfg)
	addr, err := s.PresignGet(context.Background(), testDigest)
	if err != nil {
		t.Fatal(err)
	}
	u, q := parseURL(t, addr.URL)
	if u.Host != "cache.s3.eu-west-1.amazonaws.com" {
		t.Errorf("host %q, want the bucket's virtual host", u.Host)
	}
	if want := "/team/blobs/sha256/" + testDigest.Encoded(); u.Path != want {
		t.Errorf("path %q, want %q", u.Path, want)
	}
	if cred := q.Get("X-Amz-Credential"); !strings.Contains(cred, "/eu-west-1/s3/") {
		t.Errorf("X-Amz-Credential %q isn't scoped to the region", cred)
	}
}

// fakeS3 answers HeadObject from a set of keys, and records the requests.
type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string]bool
	status   int // when set, answers every request with it
	requests []*http.Request
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r)
	switch {
	case f.status != 0:
		w.WriteHeader(f.status)
	case r.Method != http.MethodHead:
		w.WriteHeader(http.StatusMethodNotAllowed)
	case f.objects[r.URL.Path]:
		w.Header().Set("Content-Length", "5")
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newFakeS3Store(t *testing.T, f *fakeS3) *S3Store {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	cfg := testConfig()
	cfg.Endpoint = srv.URL
	return newTestStore(t, cfg)
}

func TestHas(t *testing.T) {
	f := &fakeS3{objects: map[string]bool{"/cache/team/blobs/sha256/" + testDigest.Encoded(): true}}
	s := newFakeS3Store(t, f)
	ctx := context.Background()

	ok, err := s.Has(ctx, testDigest)
	if err != nil || !ok {
		t.Fatalf("Has(stored) = %v, %v; want true", ok, err)
	}
	ok, err = s.Has(ctx, digest.FromString("other"))
	if err != nil || ok {
		t.Fatalf("Has(missing) = %v, %v; want false", ok, err)
	}
	for _, r := range f.requests {
		if r.Method != http.MethodHead {
			t.Errorf("Has sent %s %s, want HEAD", r.Method, r.URL.Path)
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AKID/") {
			t.Errorf("Has sent no SigV4 authorization: %q", r.Header.Get("Authorization"))
		}
	}
}

func TestHasErrors(t *testing.T) {
	f := &fakeS3{status: http.StatusForbidden}
	s := newFakeS3Store(t, f)
	ok, err := s.Has(context.Background(), testDigest)
	if err == nil || ok {
		t.Fatalf("Has on 403 = %v, %v; want an error", ok, err)
	}
	if !strings.Contains(err.Error(), "blobs/sha256/"+testDigest.Encoded()) {
		t.Errorf("err = %v, want it to name the key", err)
	}
	if _, err := s.Has(context.Background(), "sha256:bad"); err == nil {
		t.Error("Has(bad digest): no error")
	}
}

func TestS3ConfigFromEnv(t *testing.T) {
	for _, name := range []string{EnvEndpoint, EnvRegion, EnvBucket, EnvPrefix, EnvPathStyle,
		EnvAccessKeyID, EnvSecretAccessKey, EnvPutExpiry, EnvGetExpiry} {
		t.Setenv(name, "")
	}
	t.Setenv(EnvEndpoint, "http://localhost:9000")
	t.Setenv(EnvRegion, "us-east-1")
	t.Setenv(EnvBucket, "b")
	t.Setenv(EnvPrefix, "p")
	t.Setenv(EnvPathStyle, "true")
	t.Setenv(EnvPutExpiry, "5m")
	t.Setenv(EnvGetExpiry, "2h")
	t.Setenv(EnvAccessKeyID, "id")
	t.Setenv(EnvSecretAccessKey, "secret")

	cfg, err := S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint != "http://localhost:9000" || cfg.Region != "us-east-1" || cfg.Bucket != "b" ||
		cfg.Prefix != "p" || !cfg.PathStyle || cfg.PutExpiry != 5*time.Minute || cfg.GetExpiry != 2*time.Hour {
		t.Errorf("cfg = %+v", cfg)
	}
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil || creds.AccessKeyID != "id" || creds.SecretAccessKey != "secret" {
		t.Errorf("credentials = %+v, %v", creds, err)
	}

	for _, tc := range []struct{ name, value, want string }{
		{EnvPathStyle, "maybe", EnvPathStyle},
		{EnvPutExpiry, "soon", EnvPutExpiry},
		{EnvGetExpiry, "later", EnvGetExpiry},
		{EnvSecretAccessKey, "", "set both"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			_, err := S3ConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

func TestS3ConfigFromEnvNoCredentials(t *testing.T) {
	t.Setenv(EnvAccessKeyID, "")
	t.Setenv(EnvSecretAccessKey, "")
	t.Setenv(EnvPathStyle, "")
	cfg, err := S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Credentials != nil {
		t.Error("Credentials set with no keys in the environment")
	}
	if cfg.PathStyle {
		t.Error("PathStyle on by default")
	}
}
