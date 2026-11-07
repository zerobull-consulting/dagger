package exporter

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/snapshots"
)

// claimForever marks keys as being deleted by GC in the scope's index, so
// every export naming one is refused.
func claimForever(t *testing.T, meta *metastore.Store, keys ...string) {
	t.Helper()
	if _, err := meta.UpdateIndex(context.Background(), "team", func(idx *metastore.Index) error {
		idx.GC = &metastore.Collection{Deletions: map[string]metastore.Deletion{}}
		for _, k := range keys {
			idx.GC.Deletions[k] = metastore.Deletion{Run: "r", StartedUnix: 1}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// An export GC's fence keeps refusing is tried again apart from failed
// uploads: the refusals don't use up its upload attempts, and when they
// run out, the roots fail for what happened, not as failed uploads.
func TestExportCollectedRetriesApart(t *testing.T) {
	e := newTestEnv(t, func(c *Config) { c.UploadAttempts = 1 })
	e.addBuild()
	claimForever(t, e.meta, metastore.BlobRelKey(digest.FromBytes(layer("exec"))))
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.cache.Exports(); got != maxCollectedTries {
		t.Fatalf("tried %d times, want %d (UploadAttempts is 1, and refusals don't use it)", got, maxCollectedTries)
	}
	if len(res.Stored) != 0 || len(res.Failed) != 1 {
		t.Fatalf("result %+v", res)
	}
	for _, f := range res.Failed {
		if !strings.Contains(f.Error, "garbage collection") {
			t.Fatalf("failed as %q, want it to name garbage collection", f.Error)
		}
	}
}

// countEpochReads counts the GETs of the scope's epoch object.
type countEpochReads struct {
	metastore.Objects
	n atomic.Int64
}

func (o *countEpochReads) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, "/gc-epoch.json") {
		o.n.Add(1)
	}
	return o.Objects.Get(ctx, key)
}

// The fence is read at most once per epochTTL, not once per export try;
// a try GC's fence refused reads a fresh one.
func TestExportFenceReadOncePerTTL(t *testing.T) {
	e := newTestEnv(t, nil)
	counted := &countEpochReads{Objects: e.objects}
	now := time.Unix(1_800_000_000, 0)
	meta, err := metastore.New(metastore.Config{Objects: counted, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	x, err := New(Config{Blobs: e.blobs, Meta: meta, Scope: "team"})
	if err != nil {
		t.Fatal(err)
	}
	x.sleep = func(context.Context, time.Duration) error { return nil }
	e.addBuild()
	ctx := context.Background()
	for _, roots := range [][]uint64{{10}, {11}} {
		if res, err := x.Export(ctx, e.eng, Request{Roots: roots, Outputs: fsOutputs(10, 11)}); err != nil || len(res.Stored) != 1 {
			t.Fatalf("export %v: %+v, %v", roots, res, err)
		}
	}
	if got := counted.n.Load(); got != 1 {
		t.Fatalf("%d epoch reads for two exports within the TTL, want 1", got)
	}
	now = now.Add(epochTTL)
	claimForever(t, meta, metastore.BlobRelKey(digest.FromBytes(layer("exec"))))
	counted.n.Store(0)
	// A later state of the exec, so it is exported again.
	e.cache.Add(fakeengine.Entry{Number: 11, Replacements: 1, Deps: []uint64{10}, Layers: [][]byte{layer("base"), layer("exec")}})
	if _, err := x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)}); err != nil {
		t.Fatal(err)
	}
	// Past the TTL: read again; then once more after each refusal.
	if got := counted.n.Load(); got != maxCollectedTries {
		t.Fatalf("%d epoch reads over %d refused tries, want one per try", got, maxCollectedTries)
	}
}

// An export whose bundle names a pending offer's layer that isn't stored
// (GC deleted it before the export started) fails: the engine can't
// upload an offered layer, and the fence protects only what the export
// checked. Once the layer is stored, the export is.
func TestExportChecksOfferedLayers(t *testing.T) {
	e := newTestEnv(t, nil)
	offered := layer("offered")
	d := digest.FromBytes(offered)
	e.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{layer("base")}})
	e.cache.Add(fakeengine.Entry{Number: 12, Deps: []uint64{10}, Offer: &dagql.PersistedPartOffer{
		Address: fakeengine.FSPart,
		Value:   dagql.SnapshotValue{Kind: "directory", Path: "/"},
		Chain:   dagql.OfferedChain{Layers: []snapshots.ExportLayer{{Descriptor: ocispecs.Descriptor{MediaType: ocispecs.MediaTypeImageLayerGzip, Digest: d, Size: int64(len(offered))}}}},
	}})
	ctx := context.Background()
	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{12}, Outputs: fsOutputs(10)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stored) != 0 || len(res.Failed) != 1 || !strings.Contains(res.Failed[0].Error, "pending offer") {
		t.Fatalf("with the offered layer missing: %+v", res)
	}
	if idx, _ := e.meta.Index(ctx, "team"); len(idx.Entries) != 0 {
		t.Fatalf("recorded %d entries", len(idx.Entries))
	}
	e.blobs.Put(offered)
	res, err = e.x.Export(ctx, e.eng, Request{Roots: []uint64{12}, Outputs: fsOutputs(10)})
	if err != nil || len(res.Stored) != 1 {
		t.Fatalf("with it stored: %+v, %v", res, err)
	}
}
