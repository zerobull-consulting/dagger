package gc

// GC against a real S3-compatible store: the MinIO dev environment in
// hack/remote-cache. These skip unless CACHESERVICE_S3_ENDPOINT is set;
// `hack/remote-cache/dev.sh test` starts MinIO and runs them.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// minioEnv is a prefix of its own in the dev bucket, with GC's S3 bucket,
// the metadata store and a raw client on it.
type minioEnv struct {
	t      *testing.T
	client *s3.Client
	bucket string
	prefix string
	meta   *metastore.Store
	gcb    *S3Bucket
}

func newMinIOEnv(t *testing.T) *minioEnv {
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
	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	cfg.Prefix = fmt.Sprintf("test/%s-%s", t.Name(), hex.EncodeToString(suffix))
	ctx := context.Background()
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metastore.NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := &minioEnv{t: t, client: client, bucket: cfg.Bucket, prefix: cfg.Prefix, meta: meta, gcb: &S3Bucket{Client: client, Bucket: cfg.Bucket}}
	t.Cleanup(func() {
		_ = e.gcb.List(ctx, e.prefix+"/", func(objs []Object) error {
			keys := make([]string, len(objs))
			for i, o := range objs {
				keys[i] = o.Key
			}
			return e.gcb.Delete(ctx, keys)
		})
		ups, _ := e.gcb.ListUploads(ctx, e.prefix+"/")
		for _, u := range ups {
			_ = e.gcb.AbortUpload(ctx, u)
		}
	})
	return e
}

func (e *minioEnv) put(key string, body []byte) {
	e.t.Helper()
	if _, err := e.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: &e.bucket, Key: aws.String(e.prefix + "/" + key), Body: bytes.NewReader(body)}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *minioEnv) keys(ns string) []string {
	e.t.Helper()
	var out []string
	if err := e.gcb.List(context.Background(), e.prefix+"/"+ns+"/", func(objs []Object) error {
		for _, o := range objs {
			out = append(out, strings.TrimPrefix(o.Key, e.prefix+"/"+ns+"/"))
		}
		return nil
	}); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// export stores entries as an export would: blobs at their digest keys,
// then the bundle, resolution and index.
func (e *minioEnv) export(ns, cacheID string, entries []fakeengine.Entry, roots ...uint64) metastore.Recorded {
	e.t.Helper()
	c := fakeengine.New(cacheID, 1)
	req := protocol.Export{Roots: roots}
	for _, en := range entries {
		c.Add(en)
		req.Outputs = append(req.Outputs, protocol.ExportOutput{Number: en.Number, Address: fakeengine.FSPart})
	}
	capt, err := c.Capture(req)
	if err != nil {
		e.t.Fatal(err)
	}
	for d, data := range capt.Blobs {
		e.put(ns+"/"+metastore.BlobRelKey(d), data)
	}
	rec, err := e.meta.Record(context.Background(), ns, capt.Bundle, cacheID, 1)
	if err != nil {
		e.t.Fatal(err)
	}
	return rec
}

// GC against MinIO: what nothing names is deleted once past the grace
// period, what a live entry names stays, staged and multipart leftovers go,
// a namespace with a lifecycle rule for its staging prefix is left to it,
// and an expired root's data goes with it. The clock is moved ahead,
// since MinIO's modification times are its own.
func TestMinIOGC(t *testing.T) {
	e := newMinIOEnv(t)
	ctx := context.Background()
	live := e.export("team-a", "a", buildX("live"), 2)
	e.export("team-b", "b", buildX("b"), 2)
	orphan := digest.FromBytes([]byte("orphan"))
	e.put("team-a/"+metastore.BlobRelKey(orphan), []byte("orphan"))
	stage := blobstore.NewStage()
	e.put("team-a/staging/"+string(stage)+"/sha256/"+orphan.Encoded(), []byte("orphan"))
	e.put("team-a/notes.txt", []byte("not GC's"))
	e.put("blobs/sha256/"+orphan.Encoded(), []byte("pre-namespace"))
	up, err := e.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &e.bucket, Key: aws.String(e.prefix + "/team-a/blobs/sha256/" + strings.Repeat("0", 64))})
	if err != nil {
		t.Fatal(err)
	}

	// A lifecycle rule for team-b's staging. It replaces the bucket's
	// configuration, so it is removed again; the dev bucket has none.
	rule := e.prefix + "/team-b/staging/"
	if _, err := e.client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: &e.bucket,
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
			ID: aws.String("gc-test"), Status: types.ExpirationStatusEnabled,
			Filter:     &types.LifecycleRuleFilter{Prefix: aws.String(rule)},
			Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = e.client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: &e.bucket})
	}()
	prefixes, err := e.gcb.ExpirationPrefixes(ctx)
	if err != nil || !slices.Contains(prefixes, rule) {
		t.Fatalf("lifecycle prefixes %v, %v", prefixes, err)
	}

	clock := NewNow(time.Now().Add(3 * time.Hour))
	limits := map[string]Limits{}
	var mu sync.Mutex
	c, err := New(Config{Bucket: e.gcb, Meta: e.meta, Now: clock.Now, Limits: func(ns string) Limits {
		mu.Lock()
		defer mu.Unlock()
		return limits[ns]
	}})
	if err != nil {
		t.Fatal(err)
	}
	reps, err := c.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := reportOf(reps, "team-a")
	if a.Deleted["blob"] != 1 || a.Deleted["staging"] != 1 || a.Deleted["upload"] != 1 || a.StagingRule != "missing" {
		t.Fatalf("team-a: %+v", a)
	}
	if b := reportOf(reps, "team-b"); b.StagingRule != "present" || len(b.Deleted) != 0 {
		t.Fatalf("team-b: %+v", b)
	}
	keys := e.keys("team-a")
	want := []string{
		metastore.BlobRelKey(digest.FromBytes(layer("livebase", 1000))),
		metastore.BlobRelKey(digest.FromBytes(layer("liveexec", 2000))),
		metastore.BundleRelKey(live.BundleID),
		"gc-epoch.json", "index.json", "notes.txt",
		metastore.ResolutionRelKey(live.ResolutionID),
	}
	slices.Sort(want)
	if !slices.Equal(keys, want) {
		t.Fatalf("team-a keys %v, want %v", keys, want)
	}
	if _, err := e.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &e.bucket, Key: aws.String(e.prefix + "/blobs/sha256/" + orphan.Encoded())}); err != nil {
		t.Fatalf("the pre-namespace blob: %v", err)
	}
	if ups, _ := e.gcb.ListUploads(ctx, e.prefix+"/"); slices.ContainsFunc(ups, func(u Upload) bool { return u.ID == aws.ToString(up.UploadId) }) {
		t.Fatal("the stale multipart upload wasn't aborted")
	}
	if ep, _ := e.meta.ReadEpoch(ctx, "team-a"); ep != 1 {
		t.Fatalf("epoch %d", ep)
	}

	// team-a's max age: its root expires, and its data with it.
	mu.Lock()
	limits["team-a"] = Limits{MaxAge: time.Hour}
	mu.Unlock()
	if _, err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := e.keys("team-a"); !slices.Equal(keys, []string{"gc-epoch.json", "index.json", "notes.txt"}) {
		t.Fatalf("team-a after expiry: %v", keys)
	}
	if n := len(e.keys("team-b")); n != 5 {
		t.Fatalf("team-b: %d keys", n)
	}
}

// countingBucket counts the requests GC makes.
type countingBucket struct {
	Bucket
	lists, deletes atomic.Int64
}

func (b *countingBucket) List(ctx context.Context, p string, fn func([]Object) error) error {
	return b.Bucket.List(ctx, p, func(objs []Object) error {
		b.lists.Add(1)
		return fn(objs)
	})
}

func (b *countingBucket) Delete(ctx context.Context, keys []string) error {
	b.deletes.Add(1)
	return b.Bucket.Delete(ctx, keys)
}

// GC at scale against MinIO: listing and deleting many objects, across
// pages of 1,000 keys and delete batches of 1,000. CI runs 2,500 objects
// (about 5s); CACHESERVICE_TEST_GC_OBJECTS sets another count.
func TestMinIOGCScale(t *testing.T) {
	e := newMinIOEnv(t)
	ctx := context.Background()
	n := 2500
	if v := os.Getenv("CACHESERVICE_TEST_GC_OBJECTS"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil || n < 1 {
			t.Fatalf("CACHESERVICE_TEST_GC_OBJECTS=%q", v)
		}
	}
	e.export("team", "a", buildX("live"), 2)
	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			body := []byte("garbage-" + strconv.Itoa(i))
			e.put("team/"+metastore.BlobRelKey(digest.FromBytes(body)), body)
		}()
	}
	wg.Wait()
	t.Logf("stored %d objects in %s", n, time.Since(start).Round(time.Millisecond))

	b := &countingBucket{Bucket: e.gcb}
	c, err := New(Config{Bucket: b, Meta: e.meta, Now: NewNow(time.Now().Add(3 * time.Hour)).Now, MaxDeletes: n})
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	rep, err := c.RunNamespace(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	if rep.Deleted["blob"] != n {
		t.Fatalf("deleted %v, want %d blobs", rep.Deleted, n)
	}
	// The index, the epoch, the live bundle, resolution and 2 blobs.
	if got := len(e.keys("team")); got != 6 {
		t.Fatalf("%d keys left, want 6", got)
	}
	wantLists := int64((n + 5 + 999) / 1000)
	if b.lists.Load() != wantLists || b.deletes.Load() != int64((n+999)/1000) {
		t.Fatalf("%d list pages (want %d), %d delete requests", b.lists.Load(), wantLists, b.deletes.Load())
	}
	t.Logf("GC of %d objects: %s, %d list requests, %d delete requests", n+5, took.Round(time.Millisecond), b.lists.Load(), b.deletes.Load())
}
