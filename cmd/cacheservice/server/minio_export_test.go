package server

// The export path against MinIO (#7): an engine, through its own
// remotecache.Run, uploads a build's layers straight to S3 through presigned
// PUTs, and the service stores the bundle and index next to them. Skips
// unless CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh test`
// starts MinIO and runs it.

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// newMinIOExportHarness runs the export path on MinIO, under this test's own
// prefix.
func newMinIOExportHarness(t *testing.T) (*exportHarness, *minioTestEnv) {
	t.Helper()
	blobs, meta, env := newMinIOStores(t)
	return newExportHarness(t, blobs, meta, nil), env
}

// newMinIOStores are the blob and metadata stores on MinIO, under the test's
// own prefix.
func newMinIOStores(t *testing.T) (blobstore.Store, *metastore.Store, *minioTestEnv) {
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
	cfg.HTTPClient = env.guard
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
	return blobs, meta, env
}

// After a build on engine A, its layers and bundle exist in S3. A repeated
// export uploads nothing new, and stores the same bundle even though the
// engine annotates the layers with buildkit/createdat only from the second
// export on, as a real engine does (#30).
func TestMinIOExportBuild(t *testing.T) {
	h, env := newMinIOExportHarness(t)
	ctx := context.Background()
	h.cache.CreatedAtAnnotation = true
	layers := h.build()

	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	idx := h.waitIndex(10, 11)

	// The layers, at their content-addressed keys under the scope (the
	// namespace of a service without tokens), with their sizes.
	for i, d := range layers {
		ok, size := env.exists(ctx, t, env.key(testScope+"/blobs/sha256/"+d.Encoded()))
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

	// A repeated export of the same roots and outputs uploads nothing new,
	// and stores the same bundle.
	code, res := h.debugExport(`{"roots":[10,11],"outputs":[{"number":10,"address":{"part":"fs"}},{"number":11,"address":{"part":"fs"}}]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if !slices.Equal(res.Stored, []uint64{10, 11}) || len(res.Uploaded) != 0 || len(res.Present) != 2 {
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

// EnableS3 turns on the export and import paths against MinIO, or the export
// path alone with NoImport, and refuses a bucket it can't write to.
func TestMinIOEnableS3(t *testing.T) {
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
	cfg.HTTPClient = env.guard
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		cfg.PathStyle = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, noImport := range []bool{false, true} {
		svc := New(slog.New(slog.DiscardHandler), "")
		if err := svc.EnableS3(ctx, cfg, S3Options{Scope: testScope, NoImport: noImport}); err != nil {
			t.Fatalf("NoImport %v: %v", noImport, err)
		}
		if svc.export == nil {
			t.Fatalf("NoImport %v: export path off", noImport)
		}
		if (svc.imports == nil) != noImport {
			t.Fatalf("NoImport %v: import path on: %v", noImport, svc.imports != nil)
		}
	}

	// Past the guard, which keeps requests to the test's bucket.
	bad := cfg
	bad.HTTPClient = nil
	bad.Bucket = "no-such-bucket-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	if err := New(slog.New(slog.DiscardHandler), "").EnableS3(ctx, bad, S3Options{Scope: testScope}); err == nil {
		t.Fatal("EnableS3 accepted a missing bucket")
	}
}

// On MinIO, two namespaces exporting the same build store everything under
// their own prefix: <prefix>/<ns>/blobs/sha256/<hex> for every blob, the
// shared base layer once in each, and their bundles and indexes under
// <prefix>/<ns>/; nothing lands at <prefix>/blobs. Team B's engine can't
// renew team A's layers, and an address of team B's for A's exec layer
// finds nothing.
func TestMinIONamespaceKeys(t *testing.T) {
	blobs, meta, env := newMinIOStores(t)
	h := newAuthHarnessOn(t, blobs, meta)
	a, b := fakeengine.New("cache-a", 1), fakeengine.New("cache-b", 1)
	layersA, layersB := authBuild(a, "a"), authBuild(b, "b")
	h.connect(a, "tok-a")
	h.connect(b, "tok-b")
	h.export(a, "tok-a")
	h.export(b, "tok-b")

	ctx := context.Background()
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"team-a/blobs/sha256/" + digest.FromBytes(layersA[0]).Encoded(), true},
		{"team-b/blobs/sha256/" + digest.FromBytes(layersB[0]).Encoded(), true},
		{"team-a/blobs/sha256/" + digest.FromBytes(layersA[1]).Encoded(), true},
		{"team-b/blobs/sha256/" + digest.FromBytes(layersB[1]).Encoded(), true},
		{"team-b/blobs/sha256/" + digest.FromBytes(layersA[1]).Encoded(), false},
		{"team-a/blobs/sha256/" + digest.FromBytes(layersB[1]).Encoded(), false},
		{"blobs/sha256/" + digest.FromBytes(layersA[0]).Encoded(), false},
		{"team-a/index.json", true},
		{"team-b/index.json", true},
	} {
		if ok, _ := env.exists(ctx, t, env.key(tc.key)); ok != tc.want {
			t.Errorf("%s: exists %v, want %v", tc.key, ok, tc.want)
		}
	}
	if addrs, ok, err := b.RenewLayers(ctx, "team-a", exportLayers(layersA...)); err != nil || ok || len(addrs) != 0 {
		t.Fatalf("B renewed A's layers: %v, %v, %v", addrs, ok, err)
	}
	inB, err := namespaceBlobs(blobs, "team-b")
	if err != nil {
		t.Fatal(err)
	}
	get, err := inB.PresignGet(ctx, digest.FromBytes(layersA[1]))
	if err != nil {
		t.Fatal(err)
	}
	if code, _, err := httpGet(ctx, get.URL); err != nil || code != http.StatusNotFound {
		t.Fatalf("team B's GET of A's exec layer: %d, %v", code, err)
	}
}
