package main

// The import path (#8): a second engine with an empty cache gets the entries
// a first engine exported merged into it, and reads their layers from the
// blob store through presigned GETs. Both engines run the engine's own
// remotecache.Run over a real WebSocket, with the fake engine cache behind it.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// buildRecipes adds the build of exportHarness.build, with recipes, so
// another engine's merge can land on matching entries.
func (h *exportHarness) buildRecipes() [][]byte {
	base, exec := testLayer("base"), testLayer("exec")
	h.cache.Add(fakeengine.Entry{Number: 10, Recipe: "from(alpine)", Layers: [][]byte{base}})
	h.cache.Add(fakeengine.Entry{Number: 11, Recipe: "withExec(make)", Deps: []uint64{10}, Layers: [][]byte{base, exec}})
	return [][]byte{base, exec}
}

// exportBuild runs the build's session and waits until it is stored.
func (h *exportHarness) exportBuild() {
	h.t.Helper()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	h.waitIndex(10, 11)
}

// connectEngine connects another engine with cache, until the test ends,
// and returns its connection.
func (h *exportHarness) connectEngine(cache *fakeengine.Cache) *engineConn {
	h.t.Helper()
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: cache}, remotecache.Config{URL: h.srv.URL, Token: "engine-token", EngineName: cache.ID})
	}()
	h.t.Cleanup(func() {
		cancel()
		<-done
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c := h.svc.engineFor(cache.ID); c != nil {
			return c
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("engine %s did not connect", cache.ID)
	return nil
}

// waitLookup waits until cache has an entry of recipe, and returns it.
func waitLookup(t *testing.T, cache *fakeengine.Cache, recipe string, ready func(fakeengine.Entry) bool) fakeengine.Entry {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := cache.Lookup(recipe); ok && (ready == nil || ready(e)) {
			return e
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in cache %s", recipe, cache.ID)
	return fakeengine.Entry{}
}

func hasOffer(e fakeengine.Entry) bool { return e.Offer != nil }

func (h *exportHarness) debugImports(c *engineConn) importer.State {
	h.t.Helper()
	resp, err := http.Get(h.debug.URL + "/debug/engines/" + itoa(c.id) + "/imports")
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("status %d", resp.StatusCode)
	}
	var st importer.State
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		h.t.Fatal(err)
	}
	return st
}

func (h *exportHarness) debugImport(c *engineConn) (int, importer.Result) {
	h.t.Helper()
	resp, err := http.Post(h.debug.URL+"/debug/engines/"+itoa(c.id)+"/import", "application/json", nil)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var res importer.Result
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			h.t.Fatal(err)
		}
	}
	return resp.StatusCode, res
}

func itoa(n uint64) string { return strconv.FormatUint(n, 10) }

// checkPull pulls recipe's entry on cache and checks it downloaded want.
func checkPull(t *testing.T, cache *fakeengine.Cache, recipe string, want [][]byte) {
	t.Helper()
	e := waitLookup(t, cache, recipe, hasOffer)
	layers, downloaded, err := cache.Pull(context.Background(), nil, e.Number)
	if err != nil {
		t.Fatal(err)
	}
	if !downloaded {
		t.Fatalf("%s: not downloaded", recipe)
	}
	if len(layers) != len(want) {
		t.Fatalf("%s: %d layers, want %d", recipe, len(layers), len(want))
	}
	for i := range want {
		if !bytes.Equal(layers[i], want[i]) {
			t.Fatalf("%s: layer %d differs", recipe, i)
		}
	}
	// A second read is a local hit.
	if _, downloaded, err := cache.Pull(context.Background(), nil, e.Number); err != nil || downloaded {
		t.Fatalf("second pull: downloaded %v, %v", downloaded, err)
	}
}

// The acceptance test of #8: engine A builds and exports; engine B, with an
// empty cache, connects, has the build merged on hello, and its "run" of the
// same pipeline hits both entries, with layers pulled from the blob store.
func TestImportFreshEngine(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	h.exportBuild()

	b := fakeengine.New("cache-b", 1)
	eng := h.connectEngine(b)
	exec := waitLookup(t, b, "withExec(make)", hasOffer)
	base := waitLookup(t, b, "from(alpine)", hasOffer)
	if !slices.Equal(exec.Deps, []uint64{base.Number}) {
		t.Fatalf("exec deps %v, want [%d]", exec.Deps, base.Number)
	}
	// The engine keeps the service's numbers as its Cloud holdings.
	idx := h.waitIndex(10, 11)
	if want := idx.Caches[h.cache.ID].Numbers[11].Number; exec.CloudNumber != want {
		t.Fatalf("exec cloud number %d, want %d", exec.CloudNumber, want)
	}
	checkPull(t, b, "withExec(make)", layers)
	checkPull(t, b, "from(alpine)", layers[:1])

	// What the service knows B kept.
	st := h.debugImports(eng)
	if st.CacheID != "cache-b" || len(st.Merged) != 1 || len(st.Values) != 2 {
		t.Fatalf("imports %+v", st)
	}
	root := idx.Caches[h.cache.ID].Numbers[11].Number
	if _, ok := st.Merged[0].Retained[root]; !ok || st.Values[root].Number != exec.Number {
		t.Fatalf("imports %+v", st)
	}
	// Engine A was not merged its own export.
	if h.cache.Merges() != 0 {
		t.Fatalf("engine A merged %d times", h.cache.Merges())
	}
	// A sync now has nothing to do.
	code, res := h.debugImport(eng)
	if code != http.StatusOK || len(res.Merged) != 0 || len(res.Failed) != 0 {
		t.Fatalf("import: %d %+v", code, res)
	}
	if b.Merges() != 1 {
		t.Fatalf("engine B merged %d times", b.Merges())
	}
}

// An engine connected before the export gets it merged once it is stored.
func TestImportAfterExport(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	b := fakeengine.New("cache-b", 1)
	h.connectEngine(b)
	layers := h.buildRecipes()
	h.exportBuild()
	checkPull(t, b, "withExec(make)", layers)
}

// A merged entry B already computed keeps its own layers; B is told so, and
// pulls nothing.
func TestImportKeepsLocalEntries(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	h.exportBuild()
	b := fakeengine.New("cache-b", 1)
	b.Add(fakeengine.Entry{Number: 7, Recipe: "from(alpine)", Layers: [][]byte{layers[0]}})
	h.connectEngine(b)
	waitLookup(t, b, "withExec(make)", hasOffer)
	got, downloaded, err := b.Pull(context.Background(), nil, 7)
	if err != nil || downloaded || !bytes.Equal(got[0], layers[0]) {
		t.Fatalf("pull: %v, downloaded %v", err, downloaded)
	}
	exec, _ := b.Lookup("withExec(make)")
	if !slices.Equal(exec.Deps, []uint64{7}) {
		t.Fatalf("exec deps %v, want [7]", exec.Deps)
	}
}

// Parts the merge left unoffered are offered; an executionStarted is tried
// again at the next sync.
func TestImportOffersParts(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	h.exportBuild()
	idx := h.waitIndex(10, 11)
	execNumber := idx.Caches[h.cache.ID].Numbers[11].Number
	b := fakeengine.New("cache-b", 1)
	b.NoMergeOffers = true
	started := 0
	b.OnOffer(func(_ uint64, offer protocol.CloudPart) protocol.OfferOutcome {
		// Called with b's lock held.
		if offer.CloudNumber == execNumber && started == 0 {
			started++
			return protocol.OfferExecutionStarted
		}
		return ""
	})
	eng := h.connectEngine(b)
	// The first sync, on hello, merges and offers: exec is started, base
	// accepted.
	waitLookup(t, b, "from(alpine)", hasOffer)
	exec, _ := b.Lookup("withExec(make)")
	if exec.Offer != nil {
		t.Fatal("exec offered while its execution started")
	}
	code, res := h.debugImport(eng)
	if code != http.StatusOK || res.Offers.Accepted != 1 {
		t.Fatalf("import: %d %+v", code, res)
	}
	checkPull(t, b, "withExec(make)", layers)
	st := h.debugImports(eng)
	if st.Offers.Accepted != 2 || st.Offers.ExecutionStarted != 1 || st.Pending != 0 {
		t.Fatalf("imports %+v", st.Offers)
	}
}

// After the merged addresses expire, the engine asks for fresh ones, which
// work; a layer gone from the store is unavailable.
func TestImportRenew(t *testing.T) {
	h, blobs := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	h.exportBuild()
	b := fakeengine.New("cache-b", 1)
	h.connectEngine(b)
	e := waitLookup(t, b, "withExec(make)", hasOffer)

	ok, err := b.Renew(context.Background(), e.Number)
	if err != nil || !ok {
		t.Fatalf("renew: %v, %v", ok, err)
	}
	checkPull(t, b, "withExec(make)", layers)

	base := waitLookup(t, b, "from(alpine)", hasOffer)
	blobs.Delete(digest.FromBytes(layers[0]))
	ok, err = b.Renew(context.Background(), base.Number)
	if err != nil || ok {
		t.Fatalf("renew of a missing layer: %v, %v", ok, err)
	}
}

// Without a store, renew is unavailable, and the import hooks are off.
func TestImportOff(t *testing.T) {
	svc := newService(slog.New(slog.DiscardHandler), "")
	if got := svc.renew(context.Background(), protocol.Renew{RenewalKey: testScope}); !got.Unavailable {
		t.Fatalf("renewed %+v", got)
	}
	svc.triggerImports("")
	svc.waitImports()
}

// Roots that have expired in the index are never merged: an engine with an
// empty cache connects, syncs, and gets nothing.
func TestImportSkipsExpiredRoots(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.buildRecipes()
	h.exportBuild()
	past := time.Now().Add(-time.Minute).Unix()
	if _, err := h.meta.UpdateIndex(context.Background(), testScope, func(idx *metastore.Index) error {
		for n := range idx.Roots {
			idx.Roots[n] = metastore.Root{ExpiresAtUnix: past}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	b := fakeengine.New("cache-b", 1)
	eng := h.connectEngine(b)
	code, res := h.debugImport(eng)
	if code != http.StatusOK || len(res.Merged) != 0 || len(res.Failed) != 0 {
		t.Fatalf("import: %d %+v", code, res)
	}
	if b.Merges() != 0 {
		t.Fatalf("engine B merged %d times", b.Merges())
	}
	for _, recipe := range []string{"from(alpine)", "withExec(make)"} {
		if _, ok := b.Lookup(recipe); ok {
			t.Fatalf("%s was imported", recipe)
		}
	}
}
