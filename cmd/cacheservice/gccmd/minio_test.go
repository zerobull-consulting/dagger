package gccmd

// The gc command against a real S3-compatible store: the MinIO dev
// environment in hack/remote-cache. These skip unless
// CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh test` starts
// MinIO and runs them. Everything happens under the test's own prefix
// (s3test.NewScope), whose guard refuses any change outside it.

import (
	"bytes"
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

type minioEnv struct {
	t      *testing.T
	cfg    blobstore.S3Config
	client *s3.Client
	meta   *metastore.Store
	gcb    *gc.S3Bucket
	guard  *s3test.Guard
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
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		cfg.PathStyle = true
	}
	scope := s3test.NewScope(t, cfg.Bucket)
	cfg.Prefix = scope.Prefix
	cfg.HTTPClient = scope.Guard
	ctx := context.Background()
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metastore.NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope.DeleteOnCleanup(t, client)
	// A writer claimed the prefix: GC runs only under the marker.
	if _, _, err := ownership.Check(ctx, meta.Objects().(ownership.Store), cfg.Prefix, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	t.Setenv(blobstore.EnvBucket, cfg.Bucket)
	t.Setenv(blobstore.EnvPrefix, cfg.Prefix)
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		t.Setenv(blobstore.EnvPathStyle, "true")
	}
	return &minioEnv{t: t, cfg: cfg, client: client, meta: meta, guard: scope.Guard,
		gcb: &gc.S3Bucket{Client: client, Bucket: cfg.Bucket, FilterAllUploads: true}}
}

// open is OpenS3, the command's own opener, through the test's guard.
func (e *minioEnv) open(ctx context.Context, cfg blobstore.S3Config) (Stores, error) {
	cfg.HTTPClient = e.guard
	return OpenS3(ctx, cfg)
}

func (e *minioEnv) put(key string, body []byte) {
	e.t.Helper()
	if _, err := e.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: &e.cfg.Bucket, Key: aws.String(e.cfg.Prefix + "/" + key), Body: bytes.NewReader(body)}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *minioEnv) export(ns, cacheID string, entries []fakeengine.Entry, roots ...uint64) {
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
		e.put("ns/"+ns+"/"+metastore.BlobRelKey(d), data)
	}
	if _, err := e.meta.Record(context.Background(), ns, capt.Bundle, cacheID, 1); err != nil {
		e.t.Fatal(err)
	}
}

// listing is every key under the prefix with its size, every incomplete
// multipart upload, and every index's body: what a dry run must leave.
func (e *minioEnv) listing() []string {
	e.t.Helper()
	ctx := context.Background()
	var out []string
	err := e.gcb.List(ctx, e.cfg.Prefix+"/", func(objs []gc.Object) error {
		for _, o := range objs {
			out = append(out, o.Key)
			if strings.HasSuffix(o.Key, "/index.json") {
				b, err := e.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &e.cfg.Bucket, Key: aws.String(o.Key)})
				if err != nil {
					return err
				}
				var buf bytes.Buffer
				_, err = buf.ReadFrom(b.Body)
				b.Body.Close()
				if err != nil {
					return err
				}
				out = append(out, "body "+o.Key+" "+buf.String())
			}
		}
		return nil
	})
	if err != nil {
		e.t.Fatal(err)
	}
	ups, err := e.gcb.ListUploads(ctx, e.cfg.Prefix+"/")
	if err != nil {
		e.t.Fatal(err)
	}
	for _, u := range ups {
		out = append(out, "upload "+u.Key+" "+u.ID)
	}
	slices.Sort(out)
	return out
}

func (e *minioEnv) count(substr string) int {
	n := 0
	for _, l := range e.listing() {
		if !strings.HasPrefix(l, "body ") && !strings.HasPrefix(l, "upload ") && strings.Contains(l, substr) {
			n++
		}
	}
	return n
}

// The command against MinIO: a dry run reports and changes nothing; a
// confirmed run expires what the limits expire, then deletes what nothing
// names (a second run, as with the service), the staged upload, the
// incomplete multipart upload and the orphan; a wrong -confirm refuses; and
// the guard saw no change outside the test's prefix.
func TestMinIOGCCommand(t *testing.T) {
	e := newMinIOEnv(t)
	ctx := context.Background()
	e.export("team-a", "a", build("a"), 2)
	e.export("team-b", "b", build("b"), 2)
	orphan := digest.FromBytes([]byte("orphan"))
	e.put("ns/team-a/"+metastore.BlobRelKey(orphan), []byte("orphan"))
	stage := blobstore.NewStage()
	e.put("ns/team-a/staging/"+string(stage)+"/sha256/"+orphan.Encoded(), []byte("staged"))
	if _, err := e.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &e.cfg.Bucket, Key: aws.String(e.cfg.Prefix + "/ns/team-a/blobs/sha256/" + strings.Repeat("0", 64)),
	}); err != nil {
		t.Fatal(err)
	}
	// Past the limits' minimum (1s) and the grace periods, which this run
	// sets to it: MinIO stamps objects itself, so the clock can't be moved.
	time.Sleep(3 * time.Second)
	tune := []string{"-max-age=1s", "-max-bytes=0", "-gc-grace=1s", "-gc-staging-age=1s"}

	before := e.listing()
	code, out, errOut := runAsIs(t, e.open, tune...)
	if code != ExitOK {
		t.Fatalf("dry run: exit %d: %s", code, errOut)
	}
	dry := reports(t, out)
	if len(dry) != 2 || report(dry, "team-a").Expired != 2 || !report(dry, "team-a").DryRun {
		t.Fatalf("dry run reports: %+v", dry)
	}
	if after := e.listing(); !slices.Equal(before, after) {
		t.Fatalf("the dry run changed the bucket:\nbefore %v\nafter %v", before, after)
	}

	// A confirmation for another prefix refuses, and touches nothing.
	if code, out, _ := runAsIs(t, e.open, slices.Concat([]string{"-confirm=" + e.cfg.Bucket + "/" + e.cfg.Prefix + "x"}, tune)...); code != ExitUsage || out != "" {
		t.Fatalf("a wrong -confirm: exit %d, stdout %q", code, out)
	}
	if after := e.listing(); !slices.Equal(before, after) {
		t.Fatal("a refused run changed the bucket")
	}

	args := slices.Concat([]string{"-confirm=" + e.cfg.Bucket + "/" + e.cfg.Prefix}, tune)
	for round := 1; round <= 2; round++ {
		code, out, errOut := runAsIs(t, e.open, args...)
		if code != ExitOK {
			t.Fatalf("round %d: exit %d: %s", round, code, errOut)
		}
		for _, r := range reports(t, out) {
			if r.DryRun || len(r.Errors) != 0 {
				t.Fatalf("round %d: %+v", round, r)
			}
		}
	}
	for _, kind := range []string{"/blobs/", "/bundles/", "/resolutions/", "/staging/"} {
		if n := e.count(kind); n != 0 {
			t.Errorf("%d keys of %s are left: %v", n, kind, e.listing())
		}
	}
	ups, err := e.gcb.ListUploads(ctx, e.cfg.Prefix+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(ups) != 0 {
		t.Errorf("incomplete multipart uploads left: %+v", ups)
	}
	if e.count("/index.json") != 2 || e.count("cacheservice.json") != 1 {
		t.Errorf("the indexes and the marker stay: %v", e.listing())
	}
	if refused := e.guard.Refused(); len(refused) > 0 {
		t.Errorf("the guard refused requests: %v", refused)
	}
}

// An ownership marker of another layout: exit 3 against the real store too,
// and nothing deleted.
func TestMinIOGCCommandRefusesAnotherLayout(t *testing.T) {
	e := newMinIOEnv(t)
	e.export("team-a", "a", build("a"), 2)
	e.put("cacheservice.json", []byte(`{"layout":3}`))
	before := e.listing()
	code, out, errOut := runAsIs(t, e.open, "-confirm="+e.cfg.Bucket+"/"+e.cfg.Prefix, "-max-age=1s", "-max-bytes=0")
	if code != ExitNotOwned {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	reports(t, out)
	if after := e.listing(); !slices.Equal(before, after) {
		t.Fatal("a refused run changed the bucket")
	}
}
