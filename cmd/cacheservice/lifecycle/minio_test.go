package lifecycle

// The lifecycle command against a real S3-compatible store: the MinIO dev
// environment in hack/remote-cache, or any bucket marked disposable. These
// skip unless CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh
// test` starts MinIO and runs them.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// countingClient counts the requests that change the lifecycle
// configuration, past the guard.
type countingClient struct {
	inner  aws.HTTPClient
	writes atomic.Int32
}

func (c *countingClient) Do(r *http.Request) (*http.Response, error) {
	if r.URL.Query().Has("lifecycle") && r.Method != http.MethodGet {
		c.writes.Add(1)
	}
	return c.inner.Do(r)
}

type minioEnv struct {
	t       *testing.T
	cfg     blobstore.S3Config
	client  *s3.Client
	counter *countingClient
	tokens  string
}

func newMinIOEnv(t *testing.T) *minioEnv {
	t.Helper()
	if os.Getenv(blobstore.EnvEndpoint) == "" {
		t.Skipf("%s is not set; start MinIO with hack/remote-cache/dev.sh up", blobstore.EnvEndpoint)
	}
	if !s3test.Disposable() {
		t.Skipf("%s is not 1: apply changes the bucket's lifecycle configuration", s3test.EnvDisposableBucket)
	}
	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Bucket == "" {
		cfg.Bucket = "dagger-cache"
	}
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		cfg.PathStyle = true
	}
	scope := s3test.NewScope(t, cfg.Bucket)
	cfg.Prefix = scope.Prefix
	counter := &countingClient{inner: scope.Guard}
	cfg.HTTPClient = counter
	ctx := context.Background()
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope.DeleteOnCleanup(t, client)
	s3test.LockLifecycle(t, cfg.Bucket)
	e := &minioEnv{t: t, cfg: cfg, client: client, counter: counter}
	// Put back the bucket's configuration as it was, whatever the test did.
	orig, err := (&S3Store{Client: client, Bucket: cfg.Bucket}).GetLifecycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := (&S3Store{Client: client, Bucket: cfg.Bucket}).PutLifecycle(context.Background(), orig); err != nil {
			t.Errorf("restoring the bucket's lifecycle configuration: %v", err)
		}
	})
	t.Setenv(blobstore.EnvPrefix, cfg.Prefix)
	t.Setenv(blobstore.EnvBucket, cfg.Bucket)
	return e
}

// claim creates the prefix's ownership marker, as a writer does.
func (e *minioEnv) claim() {
	e.t.Helper()
	objs, err := metastore.NewS3Objects(context.Background(), e.cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, _, err := ownership.Check(context.Background(), objs, e.cfg.Prefix, true, time.Now()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *minioEnv) writeTokens(namespaces ...string) string {
	e.t.Helper()
	var b strings.Builder
	for _, ns := range namespaces {
		tok := make([]byte, 8)
		_, _ = rand.Read(tok)
		b.WriteString(hex.EncodeToString(tok) + " " + ns + "\n")
	}
	path := e.t.TempDir() + "/tokens"
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		e.t.Fatal(err)
	}
	return path
}

// main runs the command as the binary does, with the test's client.
func (e *minioEnv) main(args ...string) (int, string) {
	e.t.Helper()
	var out bytes.Buffer
	open := func(ctx context.Context, cfg blobstore.S3Config) (Store, error) {
		cfg.HTTPClient = e.cfg.HTTPClient
		cfg.Region, cfg.PathStyle = e.cfg.Region, e.cfg.PathStyle
		return OpenS3(ctx, cfg)
	}
	code := Main(context.Background(), args, &out, &out, open)
	e.t.Logf("lifecycle %s: exit %d\n%s", strings.Join(args, " "), code, out.String())
	return code, out.String()
}

func (e *minioEnv) rules() []types.LifecycleRule {
	e.t.Helper()
	cfg, err := (&S3Store{Client: e.client, Bucket: e.cfg.Bucket}).GetLifecycle(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return cfg.Rules
}

// plan, then apply, against the store: the rules match the tokens file, a
// foreign rule is kept, a second apply changes nothing, a removed namespace
// loses its rule, check passes after apply, and the store's refusal of the
// abort rule (MinIO) is explained.
func TestMinIOLifecycle(t *testing.T) {
	e := newMinIOEnv(t)
	ctx := context.Background()
	e.claim()
	confirm := "-confirm=" + ConfirmValue(e.cfg.Bucket, e.cfg.Prefix)
	behavior := s3test.StoreBehavior(t)
	abort := []string{}
	if !behavior.LifecycleAbortRule {
		abort = []string{"-abort-rule=false"}
	}

	// A rule of another tool's, under the test's prefix so it never
	// expires anything else, kept with whatever the bucket had.
	foreign := types.LifecycleRule{
		ID: aws.String("other-tool-" + hex.EncodeToString([]byte(t.Name()))[:12]), Status: types.ExpirationStatusEnabled,
		Filter:     &types.LifecycleRuleFilter{Prefix: aws.String(e.cfg.Prefix + "/other-tool/")},
		Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)},
	}
	before := append(e.rules(), foreign)
	if err := (&S3Store{Client: e.client, Bucket: e.cfg.Bucket}).PutLifecycle(ctx, Config{Rules: before}); err != nil {
		t.Fatal(err)
	}
	e.counter.writes.Store(0)

	tokens := e.writeTokens("team-a", "team-b", "team-a")
	if code, out := e.main(append([]string{"plan", "-tokens-file", tokens}, abort...)...); code != ExitOK || !strings.Contains(out, "2 to add") || e.counter.writes.Load() != 0 {
		t.Fatalf("plan: %d, %d writes", code, e.counter.writes.Load())
	}
	if code, _ := e.main(append([]string{"check", "-tokens-file", tokens}, abort...)...); code != ExitFindings {
		t.Fatalf("check before apply: %d", code)
	}
	if code, _ := e.main("apply", "-tokens-file", tokens, "-confirm="+e.cfg.Bucket+"/other"); code != ExitUsage || e.counter.writes.Load() != 0 {
		t.Fatalf("a -confirm mismatch: %d", code)
	}

	// The abort rule, on a store that refuses it: explained, and nothing
	// changed.
	if !behavior.LifecycleAbortRule {
		code, out := e.main("apply", "-tokens-file", tokens, confirm)
		if code != ExitError || !strings.Contains(out, "-abort-rule=false") {
			t.Fatalf("apply with the abort rule on a store that refuses it: %d", code)
		}
		if got := ids(e.rules()); !slices.Equal(got, ids(before)) {
			t.Fatalf("a refused apply changed the rules: %v", got)
		}
		e.counter.writes.Store(0)
	}

	if code, _ := e.main(append([]string{"apply", "-tokens-file", tokens, confirm}, abort...)...); code != ExitOK || e.counter.writes.Load() != 1 {
		t.Fatalf("apply: %d, %d writes", code, e.counter.writes.Load())
	}
	after := e.rules()
	wanted, _ := Rules(e.cfg.Prefix, []string{"team-a", "team-b"}, behavior.LifecycleAbortRule)
	for _, w := range append(slices.Clone(before), wanted...) {
		i := slices.IndexFunc(after, func(r types.LifecycleRule) bool { return aws.ToString(r.ID) == aws.ToString(w.ID) })
		if i < 0 || describe(after[i]) != describe(w) {
			t.Fatalf("rule %s: not as it should be in %v", aws.ToString(w.ID), ids(after))
		}
	}
	if len(after) != len(before)+len(wanted) {
		t.Fatalf("rules %v", ids(after))
	}
	// GC reads them as covering both namespaces.
	prefixes, err := (&gc.S3Bucket{Client: e.client, Bucket: e.cfg.Bucket}).ExpirationPrefixes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"team-a", "team-b"} {
		if !gc.StagingCovered(e.cfg.Prefix+"/ns/"+ns+"/", prefixes) {
			t.Fatalf("GC doesn't see %s's staging rule in %v", ns, prefixes)
		}
	}
	if code, _ := e.main(append([]string{"check", "-tokens-file", tokens}, abort...)...); code != ExitOK {
		t.Fatalf("check after apply: %d", code)
	}

	// A second apply writes nothing.
	if code, out := e.main(append([]string{"apply", "-tokens-file", tokens, confirm}, abort...)...); code != ExitOK || e.counter.writes.Load() != 1 || !strings.Contains(out, "nothing written") {
		t.Fatalf("second apply: %d, %d writes", code, e.counter.writes.Load())
	}

	// team-b removed from the tokens file: its rule goes, with
	// -allow-removals only; the foreign rule and team-a's stay.
	tokens = e.writeTokens("team-a")
	if code, _ := e.main(append([]string{"apply", "-tokens-file", tokens, confirm}, abort...)...); code != ExitUsage || e.counter.writes.Load() != 1 {
		t.Fatalf("a removal without -allow-removals: %d", code)
	}
	if code, _ := e.main(append([]string{"apply", "-tokens-file", tokens, confirm, "-allow-removals"}, abort...)...); code != ExitOK || e.counter.writes.Load() != 2 {
		t.Fatalf("apply with -allow-removals: %d", code)
	}
	got := ids(e.rules())
	if slices.Contains(got, StagingID(e.cfg.Prefix, "team-b")) || !slices.Contains(got, StagingID(e.cfg.Prefix, "team-a")) || !slices.Contains(got, aws.ToString(foreign.ID)) {
		t.Fatalf("after removing team-b: %v", got)
	}
	for _, r := range before {
		if !slices.Contains(got, aws.ToString(r.ID)) {
			t.Fatalf("the bucket's rule %s is gone: %v", aws.ToString(r.ID), got)
		}
	}
	tokens = e.writeTokens("team-a", "team-b")
	if code, _ := e.main(append([]string{"check", "-tokens-file", tokens}, abort...)...); code != ExitFindings {
		t.Fatalf("check with team-b's rule removed: %d", code)
	}
}

// apply refuses a prefix without the service's marker, and changes nothing.
func TestMinIOLifecycleRefusesUnownedPrefix(t *testing.T) {
	e := newMinIOEnv(t)
	code, _ := e.main("apply", "-abort-rule=false", "-confirm="+ConfirmValue(e.cfg.Bucket, e.cfg.Prefix))
	if code != ExitError || e.counter.writes.Load() != 0 {
		t.Fatalf("apply on a prefix without a marker: %d, %d writes", code, e.counter.writes.Load())
	}
}

// check with a key that can only read objects (the read-only user of
// hack/remote-cache, which has no s3:GetLifecycleConfiguration): the
// configuration is unreadable, which check reports as an error (exit 1), not
// as rules missing.
func TestMinIOLifecycleCheckUnreadable(t *testing.T) {
	e := newMinIOEnv(t)
	id, secret := os.Getenv("CACHESERVICE_TEST_READONLY_ACCESS_KEY_ID"), os.Getenv("CACHESERVICE_TEST_READONLY_SECRET_ACCESS_KEY")
	if id == "" || secret == "" {
		t.Skip("no read-only key: CACHESERVICE_TEST_READONLY_ACCESS_KEY_ID (hack/remote-cache/dev.sh test sets it)")
	}
	cfg := e.cfg
	cfg.Credentials = credentials.NewStaticCredentialsProvider(id, secret, "")
	client, err := blobstore.NewS3Client(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	code := Run(context.Background(), Options{Action: "check", Bucket: cfg.Bucket, Prefix: cfg.Prefix, Namespaces: []string{"a"}, Source: "test", AbortRule: true, MaxRules: MaxRules},
		&S3Store{Client: client, Bucket: cfg.Bucket}, &out, &out)
	t.Log(out.String())
	if code != ExitError || !strings.Contains(out.String(), "s3:GetLifecycleConfiguration") {
		t.Fatalf("check with the read-only key: %d", code)
	}
}

// claim on an empty prefix creates the marker with nothing else, apply and
// check work right after, and a second claim writes nothing (the marker
// object is the same, by its ETag and body).
func TestMinIOLifecycleClaim(t *testing.T) {
	e := newMinIOEnv(t)
	ctx := context.Background()
	confirm := "-confirm=" + ConfirmValue(e.cfg.Bucket, e.cfg.Prefix)
	abort := []string{}
	if !s3test.StoreBehavior(t).LifecycleAbortRule {
		abort = []string{"-abort-rule=false"}
	}
	objs, err := metastore.NewS3Objects(ctx, e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := e.main(append([]string{"apply", confirm}, abort...)...); code != ExitError {
		t.Fatalf("apply before claim: %d", code)
	}

	if code, out := e.main("claim", confirm); code != ExitOK || !strings.Contains(out, "created") {
		t.Fatalf("claim: %d", code)
	}
	keys, err := objs.ListKeys(ctx, e.cfg.Prefix+"/", 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{e.cfg.Prefix + "/cacheservice.json"}; !slices.Equal(keys, want) {
		t.Fatalf("keys after claim: %v, want only the marker %v", keys, want)
	}
	body, etag, err := objs.Get(ctx, e.cfg.Prefix+"/cacheservice.json")
	if err != nil {
		t.Fatal(err)
	}
	if m, err := ownership.ReadMarker(ctx, objs, e.cfg.Prefix); err != nil {
		t.Fatalf("the marker claim wrote: %v", err)
	} else if m.ServiceVersion == "" {
		t.Fatalf("marker %+v", m)
	}

	if code, _ := e.main(append([]string{"apply", confirm}, abort...)...); code != ExitOK {
		t.Fatalf("apply after claim: %d", code)
	}
	if code, _ := e.main(append([]string{"check"}, abort...)...); code != ExitOK {
		t.Fatalf("check after apply: %d", code)
	}

	time.Sleep(1100 * time.Millisecond) // a rewritten marker would carry another creation time
	if code, out := e.main("claim", confirm); code != ExitOK || !strings.Contains(out, "already claimed") {
		t.Fatalf("second claim: %d", code)
	}
	body2, etag2, err := objs.Get(ctx, e.cfg.Prefix+"/cacheservice.json")
	if err != nil {
		t.Fatal(err)
	}
	if etag2 != etag || !bytes.Equal(body, body2) {
		t.Fatalf("a second claim rewrote the marker: %s then %s", body, body2)
	}
}

// claim refuses a prefix that holds a key and no marker, and writes nothing;
// without -confirm it exits 2 before any request.
func TestMinIOLifecycleClaimRefusesKeys(t *testing.T) {
	e := newMinIOEnv(t)
	ctx := context.Background()
	confirm := "-confirm=" + ConfirmValue(e.cfg.Bucket, e.cfg.Prefix)
	objs, err := metastore.NewS3Objects(ctx, e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	stray := e.cfg.Prefix + "/stray"
	if _, err := objs.Put(ctx, stray, []byte("not the service's"), metastore.Condition{}); err != nil {
		t.Fatal(err)
	}
	if code, _ := e.main("claim"); code != ExitUsage {
		t.Fatalf("claim without -confirm: %d", code)
	}
	if code, _ := e.main("claim", "-confirm="+e.cfg.Bucket+"/other"); code != ExitUsage {
		t.Fatalf("claim with a wrong -confirm: %d", code)
	}
	if code, out := e.main("claim", confirm); code != ExitError || !strings.Contains(out, stray) {
		t.Fatalf("claim on a prefix with a key: %d", code)
	}
	keys, err := objs.ListKeys(ctx, e.cfg.Prefix+"/", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{stray}) {
		t.Fatalf("keys after a refused claim: %v, want only %s", keys, stray)
	}
}

// claim refuses a prefix with an upload in progress under a key that isn't
// the prefix itself: MinIO lists uploads only by exact key, so the claim
// must list the bucket's and filter, as the service's startup does
// (S3Store.FilterAllUploads, set by OpenS3 for a custom endpoint).
func TestMinIOLifecycleClaimRefusesUploadInProgress(t *testing.T) {
	e := newMinIOEnv(t)
	ctx := context.Background()
	key := e.cfg.Prefix + "/ns/default/staging/in-progress"
	up, err := e.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &e.cfg.Bucket, Key: &key})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = e.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: &e.cfg.Bucket, Key: &key, UploadId: up.UploadId})
	})
	code, out := e.main("claim", "-confirm="+ConfirmValue(e.cfg.Bucket, e.cfg.Prefix))
	if code != ExitError || !strings.Contains(out, "upload:"+key) {
		t.Fatalf("claim with an upload in progress: %d", code)
	}
	if _, err := (&S3Store{Client: e.client, Bucket: e.cfg.Bucket}).ReadMarker(ctx, e.cfg.Prefix); !errors.Is(err, ownership.ErrNoMarker) {
		t.Fatalf("a refused claim left a marker: %v", err)
	}
}
