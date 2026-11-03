package metastore

// Tests of the metadata store against a real S3-compatible store: the MinIO
// dev environment in hack/remote-cache. They skip unless
// CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh test` starts
// MinIO and runs them.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
)

// minioConfig returns the MinIO settings with a prefix of this test's own,
// emptied when the test ends.
func minioConfig(t *testing.T) blobstore.S3Config {
	t.Helper()
	if os.Getenv(blobstore.EnvEndpoint) == "" {
		t.Skipf("%s is not set; start MinIO with hack/remote-cache/dev.sh up", blobstore.EnvEndpoint)
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
	scope := s3test.NewScope(t, cfg.Bucket)
	cfg.Prefix = scope.Prefix
	cfg.HTTPClient = scope.Guard
	client, err := blobstore.NewS3Client(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope.DeleteOnCleanup(t, client)
	return cfg
}

// newMinIOStore returns a Store with its own S3 client, as a separate
// service process would have.
func newMinIOStore(t *testing.T, cfg blobstore.S3Config) *Store {
	t.Helper()
	s, err := NewS3(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// MinIO honors the conditional writes the index relies on, and the check
// cleans up after itself.
func TestMinIOMetastoreConditionalWrites(t *testing.T) {
	ctx := context.Background()
	cfg := minioConfig(t)
	s := newMinIOStore(t, cfg)
	if err := s.VerifyConditionalWrites(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &cfg.Bucket, Prefix: aws.String(cfg.Prefix + "/")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Contents) != 0 {
		t.Fatalf("the check left %d objects behind", len(out.Contents))
	}
}

// Bundles and the index are stored where the layout says, and read back.
func TestMinIOMetastoreRecord(t *testing.T) {
	ctx := context.Background()
	cfg := minioConfig(t)
	s := newMinIOStore(t, cfg)

	r, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBundle(ctx, "s1", r.BundleID)
	if err != nil {
		t.Fatal(err)
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(chain(10))
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("bundle %s, want %s", gotJSON, wantJSON)
	}
	// The raw objects are at the documented keys.
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		cfg.Prefix + "/s1/index.json",
		cfg.Prefix + "/s1/bundles/" + r.BundleID + ".json",
		cfg.Prefix + "/s1/resolutions/" + r.ResolutionID + ".json",
	} {
		if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &cfg.Bucket, Key: aws.String(key)}); err != nil {
			t.Errorf("%s: %v", key, err)
		}
	}
	// Recording the same bundle again changes nothing but the generation.
	r2, err := s.Record(ctx, "s1", chain(10), "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if r2.BundleID != r.BundleID || r2.ResolutionID != r.ResolutionID || fmt.Sprint(r2.Numbers) != fmt.Sprint(r.Numbers) || r2.Index.NextNumber != 4 {
		t.Fatalf("re-record: %+v", r2)
	}
	checkResolution(t, s, "s1", r2, "cache-a")
	if _, err := s.GetBundle(ctx, "s1", "0000000000000000000000000000000000000000000000000000000000000000"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing bundle: %v, want ErrNotFound", err)
	}
}

// Several stores, each with its own client, as separate service processes,
// record into one scope at once. No update is lost and no number is handed
// out twice.
func TestMinIOMetastoreConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	cfg := minioConfig(t)
	const writers, perWriter = 6, 5

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for w := range writers {
		objects, err := NewS3Objects(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		s, err := New(Config{Objects: objects, Prefix: cfg.Prefix, MaxAttempts: 1000})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				if _, err := s.Record(ctx, "s1", chain(uint64(100*i+1)), fmt.Sprintf("cache-%d", w), 1); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	idx, err := newMinIOStore(t, cfg).Index(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	const total = writers * perWriter * 3
	if len(idx.Entries) != total || idx.NextNumber != total+1 || len(idx.Roots) != writers*perWriter {
		t.Fatalf("%d entries, %d roots, next number %d; want %d, %d, %d",
			len(idx.Entries), len(idx.Roots), idx.NextNumber, total, writers*perWriter, total+1)
	}
	for w := range writers {
		if c := idx.Caches[fmt.Sprintf("cache-%d", w)]; c == nil || len(c.Numbers) != perWriter*3 {
			t.Fatalf("cache-%d: %+v", w, c)
		}
	}
	// Every record names a stored resolution that gives it its number,
	// however many times its writer lost the index to another.
	reader := newMinIOStore(t, cfg)
	for n, e := range idx.Entries {
		res, err := reader.GetResolution(ctx, "s1", e.Resolution)
		if err != nil {
			t.Fatalf("entry %d: %v", n, err)
		}
		if got, _ := res.Number(e.Ordinal); got != n || res.CacheID != e.Source.CacheID || res.Bundle != e.Bundle {
			t.Fatalf("entry %d: resolution %+v gives ordinal %d %d", n, res, e.Ordinal, got)
		}
	}
}

// #47 against MinIO: a bundle holding imports keeps its resolution after
// they move and are forgotten.
func TestMinIOMetastoreResolutionOfImports(t *testing.T) {
	ctx := context.Background()
	cfg := minioConfig(t)
	s := newMinIOStore(t, cfg)
	a, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordImports(ctx, "s1", "cache-b", 1, []Import{{a.Numbers[1], 20}, {a.Numbers[2], 21}}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Record(ctx, "s1", testBundle([]val{{sender: 20}, {sender: 21, deps: []uint64{1}}, {sender: 22, deps: []uint64{2}}}, root(3, 0)), "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordImports(ctx, "s1", "cache-b", 2, []Import{{a.Numbers[1], 30}, {a.Numbers[2], 31}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ForgetImports(ctx, "s1", "cache-b"); err != nil {
		t.Fatal(err)
	}
	res, err := newMinIOStore(t, cfg).GetResolution(ctx, "s1", b.ResolutionID)
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint64{a.Numbers[1], a.Numbers[2], b.Numbers[3]}; fmt.Sprint(res.Numbers) != fmt.Sprint(want) {
		t.Fatalf("resolution %v, want %v", res.Numbers, want)
	}
}

// A new store, as after a service restart, reads what the old one wrote, and
// numbers new entries after the old ones.
func TestMinIOMetastoreRestart(t *testing.T) {
	ctx := context.Background()
	cfg := minioConfig(t)

	before := newMinIOStore(t, cfg)
	r1, err := before.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}

	after := newMinIOStore(t, cfg)
	idx, err := after.Index(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	gotJSON, _ := json.Marshal(idx)
	wantJSON, _ := json.Marshal(r1.Index)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("after restart: %s, want %s", gotJSON, wantJSON)
	}
	r2, err := after.Record(ctx, "s1", chain(10), "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(r2.Numbers) != fmt.Sprint(r1.Numbers) {
		t.Fatalf("same entries after restart: %v, want %v", r2.Numbers, r1.Numbers)
	}
	r3, err := after.Record(ctx, "s1", chain(50), "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Numbers[1] != 4 {
		t.Fatalf("new entry after restart: number %d, want 4", r3.Numbers[1])
	}
}
