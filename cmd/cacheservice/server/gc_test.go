package server

// Garbage collection (#11) with fake engines connected over the engine's own
// remotecache.Run: expiry and the max age reach merges, collected data
// leaves the store, a fresh build is stored and served again, an export
// raced by a sweep never leaves the index naming a missing object, and
// namespaces are collected whatever their tokens.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// gcStores are in-memory stores on one settable clock, which starts now
// (the fake engine checks merged expiries against the real clock).
type gcStores struct {
	clock  *gc.Now
	blobs  *blobstore.MemStore
	objs   *metastore.MemObjects
	meta   *metastore.Store
	bucket *gc.MemBucket
}

func newGCStores(t *testing.T, wrap func(metastore.Objects) metastore.Objects) *gcStores {
	t.Helper()
	clock := gc.NewNow(time.Now())
	blobs := blobstore.NewMemStore("")
	blobSrv := httptestServer(t, blobs)
	blobs.SetBaseURL(blobSrv)
	blobs.SetClock(clock.Now)
	objs := metastore.NewMemObjects()
	objs.SetClock(clock.Now)
	var objects metastore.Objects = objs
	if wrap != nil {
		objects = wrap(objs)
	}
	meta, err := metastore.New(metastore.Config{Objects: objects, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	return &gcStores{clock: clock, blobs: blobs, objs: objs, meta: meta, bucket: &gc.MemBucket{Blobs: blobs, Objects: objs}}
}

func httptestServer(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// enableGC turns GC on, run only when asked, on the stores' clock.
func enableGC(t *testing.T, svc *Service, st *gcStores, modify func(*gc.Config)) {
	t.Helper()
	cfg := gc.Config{Bucket: st.bucket, Meta: st.meta, Now: st.clock.Now}
	if modify != nil {
		modify(&cfg)
	}
	if err := svc.EnableGC(context.Background(), GCConfig{Collector: cfg}); err != nil {
		t.Fatal(err)
	}
}

func collect(t *testing.T, svc *Service) []gc.Report {
	t.Helper()
	reps, err := svc.CollectGarbage(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	return reps
}

func reportIn(reps []gc.Report, ns string) gc.Report {
	for _, r := range reps {
		if r.Namespace == ns {
			return r
		}
	}
	return gc.Report{}
}

// keysOf lists a namespace's objects, relative to it.
func (st *gcStores) keysOf(t *testing.T, ns string) []string {
	t.Helper()
	var out []string
	if err := st.bucket.List(context.Background(), ns+"/", func(objs []gc.Object) error {
		for _, o := range objs {
			out = append(out, strings.TrimPrefix(o.Key, ns+"/"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// checkConsistent fails when the index names a missing bundle or
// resolution, or a stored bundle a missing blob.
func (st *gcStores) checkConsistent(t *testing.T, ns string) {
	t.Helper()
	ctx := context.Background()
	idx, err := st.meta.Index(ctx, ns)
	if err != nil {
		t.Fatal(err)
	}
	view := memIn(t, st.blobs, ns)
	for n, e := range idx.Entries {
		b, err := st.meta.GetBundle(ctx, ns, e.Bundle)
		if err != nil {
			t.Fatalf("entry %d: bundle %s: %v", n, e.Bundle, err)
		}
		if e.Resolution != "" {
			if _, err := st.meta.GetResolution(ctx, ns, e.Resolution); err != nil {
				t.Fatalf("entry %d: resolution: %v", n, err)
			}
		}
		for _, d := range metastore.BundleBlobs(b) {
			if _, ok := view.Get(d); !ok {
				t.Fatalf("entry %d: blob %s is not stored", n, d)
			}
		}
	}
}

// A namespace's max age expires what an export recorded, measured from
// then: the merge sends roots capped at it, so engines release them on
// their own; past it, they are no longer merged (before GC runs, by the
// importer's guard); GC drops them from the index, then deletes their
// bundles and blobs once past the grace period. A fresh build after that
// is exported, uploading the layers again, and served again.
func TestGCMaxAgeEndToEnd(t *testing.T) {
	st := newGCStores(t, nil)
	h := newExportHarnessWith(t, st.blobs, st.meta, nil, func(c *ImportConfig) { c.Importer.Now = st.clock.Now })
	if err := h.svc.UseLimits("", gc.Limits{MaxAge: time.Hour}); err != nil {
		t.Fatal(err)
	}
	enableGC(t, h.svc, st, nil)
	layers := h.buildRecipes()
	h.exportBuild()
	idx := h.waitIndex(10, 11)
	root := idx.Caches[h.cache.ID].Numbers[11].Number
	recordedAt := idx.Roots[root].RecordedAtUnix
	if recordedAt != st.clock.Now().Unix() {
		t.Fatalf("recorded at %d, want %d", recordedAt, st.clock.Now().Unix())
	}

	// B gets it merged, with the root's expiry capped at the max age,
	// though the export recorded it with none.
	b := fakeengine.New("cache-b", 1)
	engB := h.connectEngine(b)
	checkPull(t, b, "withExec(make)", layers)
	waitImports(t, h.meta, "cache-b", 2)
	stB := h.debugImports(engB)
	if got := stB.Merged[0].Retained[root]; got != recordedAt+3600 {
		t.Fatalf("merged root expiry %d, want %d (recorded + max age)", got, recordedAt+3600)
	}
	if idx.Roots[root].ExpiresAtUnix != 0 {
		t.Fatalf("the index's own expiry changed: %d", idx.Roots[root].ExpiresAtUnix)
	}

	// Past the max age, before any GC: not merged into a new engine.
	st.clock.Add(61 * time.Minute)
	c := fakeengine.New("cache-c", 1)
	engC := h.connectEngine(c)
	if code, res := h.debugImport(engC); code != http.StatusOK || len(res.Merged) != 0 {
		t.Fatalf("import past max age: %d %+v", code, res)
	}

	// GC drops the entries; the objects are 61 minutes old, under the
	// grace period: kept. A dry run first changes nothing.
	resp, err := http.Post(h.debug.URL+"/debug/gc?dry-run=1", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var dry struct{ Reports []gc.Report }
	_ = json.NewDecoder(resp.Body).Decode(&dry)
	resp.Body.Close()
	if r := reportIn(dry.Reports, testScope); !r.DryRun || r.Expired != 2 {
		t.Fatalf("dry run: %+v", r)
	}
	if n := len(h.waitIndex().Entries); n != 2 {
		t.Fatalf("a dry run expired entries: %d left", n)
	}
	before := st.keysOf(t, testScope)
	rep := reportIn(collect(t, h.svc), testScope)
	if rep.Expired != 2 || len(rep.Deleted) != 0 {
		t.Fatalf("gc: %+v", rep)
	}
	if after := st.keysOf(t, testScope); !slices.Equal(before, after) {
		t.Fatalf("deleted within the grace period: %v, then %v", before, after)
	}
	if code, res := h.debugImport(engC); code != http.StatusOK || len(res.Merged) != 0 {
		t.Fatalf("import after expiry: %d %+v", code, res)
	}

	// Past the grace period: the bundle, resolution and both blobs go.
	st.clock.Add(time.Hour)
	rep = reportIn(collect(t, h.svc), testScope)
	if rep.Deleted["blob"] != 2 || rep.Deleted["bundle"] != 1 || rep.Deleted["resolution"] != 1 {
		t.Fatalf("gc: %+v", rep.Deleted)
	}
	if n := memIn(t, st.blobs, testScope).Len(); n != 0 {
		t.Fatalf("%d blobs left", n)
	}
	if got := metricstest.Value(t, h.svc.registry, "cacheservice_gc_deleted_objects_total", map[string]string{"scope": testScope, "kind": "blob"}); got != 2 {
		t.Fatalf("gc_deleted_objects_total{kind=blob} %v", got)
	}
	if got := metricstest.Value(t, h.svc.registry, "cacheservice_gc_entries_expired_total", map[string]string{"scope": testScope, "reason": "expired"}); got != 2 {
		t.Fatalf("gc_entries_expired_total %v", got)
	}
	st.checkConsistent(t, testScope)

	// A fresh build on the same base: its layers are uploaded again, and
	// it is merged into C.
	puts := st.blobs.Puts()
	h.cache.Add(fakeengine.Entry{Number: 12, Recipe: "withExec(test)", Deps: []uint64{10}, Layers: [][]byte{layers[0], testLayer("test")}})
	h.postSpans("session-2", cacheSpan(10), cacheSpan(12, "10"), carrierSpan(2))
	h.waitIndex(10, 12)
	if got := st.blobs.Puts() - puts; got != 2 {
		t.Fatalf("uploaded %d blobs again, want 2", got)
	}
	waitLookup(t, c, "withExec(test)", hasOffer)
	checkPull(t, c, "withExec(test)", [][]byte{layers[0], testLayer("test")})
	st.checkConsistent(t, testScope)
}

// holdIndexWrites holds the export path's index writes while armed, until
// released: an export stays between its blob promotions and its index
// write.
type holdIndexWrites struct {
	metastore.Objects
	mu       sync.Mutex
	armed    bool
	held     chan struct{}
	released chan struct{}
}

func (o *holdIndexWrites) arm() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.armed, o.held, o.released = true, make(chan struct{}), make(chan struct{})
}

func (o *holdIndexWrites) Put(ctx context.Context, key string, body []byte, cond metastore.Condition) (string, error) {
	o.mu.Lock()
	hold := o.armed && strings.HasSuffix(key, "/index.json") && metrics.Caller(ctx) == metrics.CallerExport
	if hold {
		o.armed = false
	}
	held, released := o.held, o.released
	o.mu.Unlock()
	if hold {
		close(held)
		<-released
	}
	return o.Objects.Put(ctx, key, body, cond)
}

// An export held between its blob promotions and its index write, while GC
// runs. With the default grace period, GC collects nothing of it: the
// promoted blobs are younger. With a grace period too short to cover it
// (or for an export that found its blobs stored and unreferenced), GC
// deletes them; the export's index write is then refused, since it checked
// them before they were deleted, and it is tried again from the start: it
// uploads them again and is stored. Either way the index never names a
// missing object.
func TestGCRacesExport(t *testing.T) {
	for _, tc := range []struct {
		name  string
		grace time.Duration
	}{
		{"default grace", 0},
		{"grace too short", time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hold *holdIndexWrites
			st := newGCStores(t, func(o metastore.Objects) metastore.Objects {
				hold = &holdIndexWrites{Objects: o}
				return hold
			})
			h := newExportHarness(t, st.blobs, st.meta, nil)
			enableGC(t, h.svc, st, func(c *gc.Config) { c.Grace = tc.grace })
			// Something stored first, so the namespace has an index.
			h.cache.Add(fakeengine.Entry{Number: 20, Recipe: "other", Layers: [][]byte{testLayer("other")}})
			h.postSpans("session-0", cacheSpan(20), carrierSpan(1))
			h.waitIndex(20)

			hold.arm()
			layers := h.buildRecipes()
			h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
			select {
			case <-hold.held:
			case <-time.After(20 * time.Second):
				t.Fatal("the export didn't reach its index write")
			}
			view := memIn(t, st.blobs, testScope)
			for _, l := range layers {
				if _, ok := view.Get(digest.FromBytes(l)); !ok {
					t.Fatal("not promoted")
				}
			}
			puts := st.blobs.Puts()
			st.clock.Add(time.Second)
			rep := reportIn(collect(t, h.svc), testScope)
			close(hold.released)
			h.waitIndex(10, 11)
			st.checkConsistent(t, testScope)
			refused := metricstest.Value(t, h.svc.registry, "cacheservice_export_collected_total", map[string]string{"scope": testScope})
			if tc.grace == 0 {
				if len(rep.Deleted) != 0 || refused != 0 || st.blobs.Puts() != puts {
					t.Fatalf("deleted %v, refused %v, uploads %d", rep.Deleted, refused, st.blobs.Puts()-puts)
				}
				return
			}
			if rep.Deleted["blob"] != 2 {
				t.Fatalf("deleted %v", rep.Deleted)
			}
			if refused < 1 || st.blobs.Puts()-puts != 2 {
				t.Fatalf("refused %v, uploaded again %d", refused, st.blobs.Puts()-puts)
			}
		})
	}
}

// A namespace whose tokens were all removed keeps its data (revocation
// deletes nothing), and is still collected by GC, with the limits of the
// namespaces file's defaults; GC of it leaves the other namespace alone.
func TestGCNamespaceWithoutTokens(t *testing.T) {
	st := newGCStores(t, nil)
	h := newAuthHarnessOn(t, st.blobs, st.meta)
	enableGC(t, h.svc, st, nil)
	a := fakeengine.New("cache-a", 1)
	authBuild(a, "a")
	h.connect(a, "tok-a")
	h.export(a, "tok-a")
	bc := fakeengine.New("cache-b", 1)
	authBuild(bc, "b")
	h.connect(bc, "tok-b")
	h.export(bc, "tok-b")
	keysA, keysB := st.keysOf(t, "team-a"), st.keysOf(t, "team-b")

	// Revoke team-a's tokens: nothing is deleted, by the revocation or by
	// GC (no limits).
	h.reload("tok-b team-b\n")
	st.clock.Add(1000 * time.Hour)
	collect(t, h.svc)
	if got := st.keysOf(t, "team-a"); !slices.Equal(got, keysA) {
		t.Fatalf("team-a after revocation: %v, was %v", got, keysA)
	}

	// A namespaces file with a max age for team-a, though it has no
	// tokens; team-b gets a line with no limit.
	file := filepath.Join(t.TempDir(), "namespaces")
	if err := os.WriteFile(file, []byte("team-a max-age=1h\nteam-b max-age=none\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.svc.UseLimits(file, gc.Limits{}); err != nil {
		t.Fatal(err)
	}
	reps := collect(t, h.svc)
	if r := reportIn(reps, "team-a"); r.Expired != 2 {
		t.Fatalf("team-a: %+v", r)
	}
	// The run that expires entries leaves their objects to the next.
	reps = collect(t, h.svc)
	if r := reportIn(reps, "team-a"); r.Deleted["blob"] != 2 || r.Deleted["bundle"] != 1 {
		t.Fatalf("team-a: %+v", r)
	}
	if r := reportIn(reps, "team-b"); r.Expired != 0 || len(r.Deleted) != 0 {
		t.Fatalf("team-b: %+v", r)
	}
	if got := st.keysOf(t, "team-b"); !slices.Equal(got, keysB) {
		t.Fatalf("team-b: %v, was %v", got, keysB)
	}

	// A file that doesn't parse keeps the limits.
	if err := os.WriteFile(file, []byte("team-a max-age=soon\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.svc.ReloadLimits(); err == nil {
		t.Fatal("a bad file reloaded")
	}
	if got := h.svc.Limits("team-a").MaxAge; got != time.Hour {
		t.Fatalf("max age after a bad reload: %s", got)
	}
}

// Engine signals never delete: a prune and a wipe of the exporting engine
// leave the stored data, which GC keeps while it is live.
func TestGCIgnoresEngineSignals(t *testing.T) {
	st := newGCStores(t, nil)
	h := newExportHarness(t, st.blobs, st.meta, nil)
	enableGC(t, h.svc, st, nil)
	h.buildRecipes()
	h.exportBuild()
	before := st.keysOf(t, testScope)
	prune, _ := json.Marshal(pruneOf(10, 11))
	wipe, _ := json.Marshal(telemetryattrs.EngineStartEvent{WipedCache: h.cache.ID})
	h.postEvent(telemetryattrs.EngineEventPrune, string(prune))
	h.postEvent(telemetryattrs.EngineEventStart, string(wipe))
	st.clock.Add(1000 * time.Hour)
	if rep := reportIn(collect(t, h.svc), testScope); rep.Expired != 0 || len(rep.Deleted) != 0 {
		t.Fatalf("gc after engine signals: %+v", rep)
	}
	if after := st.keysOf(t, testScope); !slices.Equal(before, after) {
		t.Fatalf("keys %v, then %v", before, after)
	}
}

// POST /debug/gc refuses a dry-run value it can't read, rather than run for
// real, and answers 500 when a run failed.
func TestDebugGC(t *testing.T) {
	st := newGCStores(t, nil)
	h := newExportHarness(t, st.blobs, st.meta, nil)
	enableGC(t, h.svc, st, nil)
	h.buildRecipes()
	h.exportBuild()
	post := func(query string) int {
		t.Helper()
		resp, err := http.Post(h.debug.URL+"/debug/gc"+query, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("?dry-run=yes"); code != http.StatusBadRequest {
		t.Fatalf("dry-run=yes: %d", code)
	}
	if code := post("?dry-run=1"); code != http.StatusOK {
		t.Fatalf("dry-run=1: %d", code)
	}
	// A bundle the index names, gone: the run fails.
	idx := h.waitIndex(10, 11)
	for _, e := range idx.Entries {
		if err := st.objs.Delete(context.Background(), testScope+"/bundles/"+e.Bundle+".json"); err != nil {
			t.Fatal(err)
		}
	}
	if code := post(""); code != http.StatusInternalServerError {
		t.Fatalf("failed run: %d", code)
	}
}

// A negative GC interval is refused: only 0 turns the schedule off, and a
// typo must not leave retention off without a word.
func TestEnableGCRefusesNegativeInterval(t *testing.T) {
	st := newGCStores(t, nil)
	svc := New(slog.New(slog.DiscardHandler), "")
	err := svc.EnableGC(context.Background(), GCConfig{Interval: -time.Second, Collector: gc.Config{Bucket: st.bucket, Meta: st.meta}})
	if err == nil || !strings.Contains(err.Error(), "negative interval") {
		t.Fatalf("EnableGC: %v", err)
	}
	if _, err := svc.CollectGarbage(context.Background(), true); !errors.Is(err, ErrGCOff) {
		t.Fatalf("GC on after a refused config: %v", err)
	}
}
