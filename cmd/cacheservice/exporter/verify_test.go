package exporter

// Tests of the verification of uploads (#23): a blob reaches its digest key
// only once its content is checked, and a bundle is published only once
// every blob it needs is.

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
)

// An engine that uploads a same-size body with the wrong content, once, has
// it rejected: nothing reaches the digest key and nothing is published on
// that try; the retry asks for that blob alone again, under a new stage, and
// stores the right one.
func TestExportMismatchedUploadRetried(t *testing.T) {
	reg := prometheus.NewRegistry()
	e := newTestEnv(t, func(c *Config) { c.Metrics = metrics.New(reg) })
	e.addBuild()
	exec := digest.FromBytes(layer("exec"))
	e.cache.CorruptUploads(exec, 1)

	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{11}) || len(res.Failed) != 0 || !slices.Equal(res.Rejected, []digest.Digest{exec}) {
		t.Fatalf("unexpected result: %+v", res)
	}
	uploads := e.eng.uploadMaps()
	if len(uploads) != 2 || len(uploads[1]) != 1 || uploads[1][exec] == "" {
		t.Fatalf("uploads %v: want the retry to ask for %s alone", uploads, exec)
	}
	if stageOf(t, uploads[0][exec]) == stageOf(t, uploads[1][exec]) {
		t.Fatal("the retry reused the first try's stage")
	}
	if got, ok := e.blobs.Get(exec); !ok || digest.FromBytes(got) != exec {
		t.Fatal("the right content isn't stored")
	}
	if left := e.blobs.Staged(); len(left) != 0 {
		t.Fatalf("staged blobs left: %v", left)
	}
	for outcome, want := range map[string]float64{"promoted": 2, "mismatch": 1} {
		if got := metricstest.Value(t, reg, "cacheservice_blob_promotions_total", map[string]string{"scope": "team", "outcome": outcome}); got != want {
			t.Errorf("%s promotions: %v, want %v", outcome, got, want)
		}
	}
	if got := metricstest.Value(t, reg, "cacheservice_blob_hashed_bytes_total", map[string]string{"scope": "team", "by": "service"}); got != float64(3*len(layer("exec"))) {
		t.Errorf("bytes hashed: %v", got)
	}
}

// stageOf is the stage of a MemStore PUT address.
func stageOf(t *testing.T, address string) string {
	t.Helper()
	u, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(u.Path, "/staging/")
	if !ok {
		t.Fatalf("address %s isn't a staging key", address)
	}
	stage, _, _ := strings.Cut(rest, "/")
	return stage
}

// An engine that keeps lying never gets the bundle published: the root
// fails after UploadAttempts tries, the index stays empty, and the wrong
// content never reaches the digest key. The blob it uploaded honestly is
// stored (verified), so it isn't asked for again.
func TestExportLyingEngineNothingPublished(t *testing.T) {
	e := newTestEnv(t, func(c *Config) { c.UploadAttempts = 2 })
	e.addBuild()
	base, exec := digest.FromBytes(layer("base")), digest.FromBytes(layer("exec"))
	e.cache.CorruptUploads(exec, -1)
	ctx := context.Background()

	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stored) != 0 || len(res.Bundles) != 0 || len(res.Failed) != 1 || len(res.Rejected) != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if _, ok := e.blobs.Get(exec); ok {
		t.Fatal("the wrong content reached the digest key")
	}
	if _, ok := e.blobs.Get(base); !ok {
		t.Fatal("the honest upload wasn't stored")
	}
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 0 {
		t.Fatalf("index has %d entries", len(idx.Entries))
	}
	for _, k := range e.objects.Keys() {
		if strings.Contains(k, "/bundles/") {
			t.Fatalf("bundle %s published", k)
		}
	}
	if uploads := e.eng.uploadMaps(); len(uploads) != 2 || len(uploads[1]) != 1 {
		t.Fatalf("uploads %v: want the retry to ask for the rejected blob alone", uploads)
	}
}

// An engine that reports a blob done without uploading it gets nothing
// stored for it (Missing), and is asked for it again.
func TestExportReportedButNotUploaded(t *testing.T) {
	reg := prometheus.NewRegistry()
	e := newTestEnv(t, func(c *Config) { c.Metrics = metrics.New(reg) })
	e.addBuild()
	exec := digest.FromBytes(layer("exec"))
	e.eng.noPut = map[digest.Digest]bool{exec: true}

	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stored) != 0 || len(res.Failed) != 1 || len(res.Rejected) != DefaultUploadAttempts {
		t.Fatalf("unexpected result: %+v", res)
	}
	if got := metricstest.Value(t, reg, "cacheservice_blob_promotions_total", map[string]string{"scope": "team", "outcome": "missing"}); got != DefaultUploadAttempts {
		t.Fatalf("missing promotions: %v", got)
	}
}

// promoteFailStore fails every promotion, as a store that is down would.
type promoteFailStore struct{ *blobstore.MemStore }

func (promoteFailStore) Promote(context.Context, blobstore.Stage, digest.Digest, int64) (blobstore.Promotion, error) {
	return blobstore.Promotion{}, errors.New("s3 down")
}

// A promotion the store fails leaves the blob unstored: the export is tried
// again, and nothing is published while it fails. The staged upload is left
// for the lifecycle rule.
func TestExportPromotionFailureNotPublished(t *testing.T) {
	reg := prometheus.NewRegistry()
	e := newTestEnv(t, func(c *Config) { c.Metrics = metrics.New(reg) })
	e.addBuild()
	e.x.cfg.Blobs = promoteFailStore{e.blobs}
	ctx := context.Background()

	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stored) != 0 || len(res.Failed) != 1 || len(res.Uploaded) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if e.blobs.Len() != 0 {
		t.Fatalf("%d blobs stored", e.blobs.Len())
	}
	if got := len(e.blobs.Staged()); got != 2*DefaultUploadAttempts {
		t.Fatalf("%d staged uploads, want each try's left", got)
	}
	if idx, _ := e.meta.Index(ctx, "team"); len(idx.Entries) != 0 {
		t.Fatalf("index has %d entries", len(idx.Entries))
	}
	if got := metricstest.Value(t, reg, "cacheservice_blob_promotions_total", map[string]string{"scope": "team", "outcome": metrics.PromotionError}); got != 2*DefaultUploadAttempts {
		t.Fatalf("failed promotions: %v", got)
	}
}

// An export cut short while it promotes (the service stopping) fails as a
// whole, publishes nothing, and leaves the staged uploads to the lifecycle
// rule: the next export asks for them again.
func TestExportCanceledDuringPromotion(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	ctx, cancel := context.WithCancel(context.Background())
	e.blobs.BeforePromote(func(context.Context, string, digest.Digest) error {
		cancel()
		return context.Canceled
	})
	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want canceled", err)
	}
	if len(res.Stored) != 0 || len(res.Bundles) != 0 || e.blobs.Len() != 0 || len(e.blobs.Staged()) != 2 {
		t.Fatalf("result %+v; %d stored, %d staged", res, e.blobs.Len(), len(e.blobs.Staged()))
	}
	e.blobs.BeforePromote(nil)
	res, err = e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil || !slices.Equal(res.Stored, []uint64{11}) || len(res.Uploaded) != 2 {
		t.Fatalf("next export: %+v, %v", res, err)
	}
}
