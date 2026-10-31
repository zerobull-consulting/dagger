package main

// The export path against MinIO (#7): an engine, through its own
// remotecache.Run, uploads a build's layers straight to S3 through presigned
// PUTs, and the service stores the bundle and index next to them. Skips
// unless CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh test`
// starts MinIO and runs it.

import (
	"context"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// newMinIOExportHarness runs the export path on MinIO, under this test's own
// prefix.
func newMinIOExportHarness(t *testing.T) (*exportHarness, *minioTestEnv) {
	t.Helper()
	env := newMinIOTestEnv(t)
	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	cfg.Bucket = env.bucket
	cfg.Prefix = env.prefix
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		cfg.PathStyle = true
	}
	ctx := context.Background()
	blobs, err := blobstore.NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metastore.NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.VerifyConditionalWrites(ctx, testScope); err != nil {
		t.Fatal(err)
	}
	return newExportHarness(t, blobs, meta, nil), env
}

// After a build on engine A, its layers and bundle exist in S3. A repeated
// export uploads nothing new.
func TestMinIOExportBuild(t *testing.T) {
	h, env := newMinIOExportHarness(t)
	ctx := context.Background()
	layers := h.build()

	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	idx := h.waitIndex(10, 11)

	// The layers, at their content-addressed keys, with their sizes.
	for i, d := range layers {
		ok, size := env.exists(ctx, t, env.key("blobs/sha256/"+d.Encoded()))
		want := int64(len(testLayer([]string{"base", "exec"}[i])))
		if !ok || size != want {
			t.Fatalf("layer %s: exists %v, size %d, want %d", d, ok, size, want)
		}
	}
	// The bundle and the index.
	entry := idx.Entries[idx.Caches[h.cache.ID].Numbers[11].Number]
	if ok, _ := env.exists(ctx, t, env.key(testScope+"/bundles/"+entry.Bundle+".json")); !ok {
		t.Fatal("bundle not in S3")
	}
	if ok, _ := env.exists(ctx, t, env.key(testScope+"/index.json")); !ok {
		t.Fatal("index not in S3")
	}
	bundle, err := h.meta.GetBundle(ctx, testScope, entry.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Values) != 2 || len(bundle.Outputs) != 2 {
		t.Fatalf("bundle: %d values, %d outputs", len(bundle.Values), len(bundle.Outputs))
	}
	exports := h.cache.Exports()

	// A repeated export uploads nothing new, and stores the same bundle.
	code, res := h.debugExport(`{"roots":[11],"outputs":[{"number":10,"address":{"part":"fs"}},{"number":11,"address":{"part":"fs"}}]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if !slices.Equal(res.Stored, []uint64{11}) || len(res.Uploaded) != 0 || len(res.Present) != 2 {
		t.Fatalf("repeat export: %+v", res)
	}
	if !slices.Equal(res.Bundles, []string{entry.Bundle}) {
		t.Fatalf("repeat export stored bundle %v, want %s", res.Bundles, entry.Bundle)
	}
	if h.cache.Exports() != exports+1 {
		t.Fatalf("%d exports", h.cache.Exports()-exports)
	}
	again, err := h.meta.Index(ctx, testScope)
	if err != nil {
		t.Fatal(err)
	}
	if again.NextNumber != idx.NextNumber || len(again.Entries) != len(idx.Entries) {
		t.Fatalf("index changed: next %d -> %d, entries %d -> %d", idx.NextNumber, again.NextNumber, len(idx.Entries), len(again.Entries))
	}
}

// A second build sharing the base layer uploads only its own.
func TestMinIOExportSharedLayer(t *testing.T) {
	h, _ := newMinIOExportHarness(t)
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	h.waitIndex(10, 11)

	h.cache.Add(fakeengine.Entry{Number: 12, Deps: []uint64{10}, Layers: [][]byte{testLayer("base"), testLayer("other")}})
	code, res := h.debugExport(`{"roots":[12],"outputs":[{"number":12,"address":{"part":"fs"}}]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if len(res.Uploaded) != 1 || len(res.Present) != 1 {
		t.Fatalf("uploaded %v, present %v", res.Uploaded, res.Present)
	}
	if !strings.HasPrefix(string(res.Uploaded[0]), "sha256:") {
		t.Fatalf("uploaded %v", res.Uploaded)
	}
}
