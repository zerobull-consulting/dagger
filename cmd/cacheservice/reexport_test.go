package main

// #35: an engine that had entries merged into it, and hits them, must not
// export them back as new entries, nor record them again through the
// closure of a new entry over them.

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

func TestImportsNotReexported(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	testImportsNotReexported(t, h)
}

func testImportsNotReexported(t *testing.T, h *exportHarness) {
	t.Helper()
	ctx := context.Background()
	layers := h.buildRecipes()
	h.exportBuild()
	idx := h.waitIndex(10, 11)
	baseN := idx.Caches[h.cache.ID].Numbers[10].Number
	execN := idx.Caches[h.cache.ID].Numbers[11].Number
	if len(idx.Entries) != 2 {
		t.Fatalf("after A's export: %d entries", len(idx.Entries))
	}

	// B has A's build merged, and its imports recorded in the index.
	b := fakeengine.New("cache-b", 1)
	engB := h.connectEngine(b)
	base := waitLookup(t, b, "from(alpine)", hasOffer)
	exec := waitLookup(t, b, "withExec(make)", hasOffer)
	idx = waitImports(t, h.meta, b.ID, 2)
	want := map[uint64]metastore.Mapping{base.Number: {Number: baseN, Generation: 1}, exec.Number: {Number: execN, Generation: 1}}
	if fmt.Sprint(idx.Caches[b.ID].Imported) != fmt.Sprint(want) {
		t.Fatalf("B's imports %v, want %v", idx.Caches[b.ID].Imported, want)
	}
	// B reads the exec's layers, as a run that needs its filesystem does.
	checkPull(t, b, "withExec(make)", layers)

	// B runs the same pipeline: its spans name the imported entries, hit
	// and retained. They are the stored values, so B exports nothing.
	h.postSpansOf(b, "b-1", cacheSpan(base.Number), cacheSpan(exec.Number, itoa(base.Number)), carrierSpan(2))
	waitTracked(t, h, b.ID, base.Number, exec.Number)
	time.Sleep(200 * time.Millisecond)
	if b.Exports() != 0 {
		t.Fatalf("B exported its imports %d times", b.Exports())
	}

	// Exported anyway, as the explicit hook does without asking the
	// tracker: the bundle is stored, but its values keep A's numbers.
	code, res := h.debugExportOf(engB, fmt.Sprintf(`{"roots":[%d,%d]}`, base.Number, exec.Number))
	if code != 200 || len(res.Stored) != 2 {
		t.Fatalf("export hook: %d %+v", code, res)
	}
	idx, err := h.meta.Index(ctx, testScope)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 2 || idx.NextNumber != 3 || len(idx.Caches[b.ID].Numbers) != 0 {
		t.Fatalf("re-exported imports were recorded: %d entries, next %d, B %+v", len(idx.Entries), idx.NextNumber, idx.Caches[b.ID])
	}

	// B builds a new entry over them: it alone is new, and depends on A's
	// exec by its original number.
	test := testLayer("test")
	b.Add(fakeengine.Entry{Number: 2000, Recipe: "withExec(test)", Deps: []uint64{exec.Number}, Layers: [][]byte{layers[0], layers[1], test}})
	h.postSpansOf(b, "b-2", cacheSpan(base.Number), cacheSpan(exec.Number, itoa(base.Number)), cacheSpan(2000, itoa(exec.Number)), carrierSpan(3))
	idx = waitMapped(t, h.meta, b.ID, 2000)
	if len(idx.Entries) != 3 || idx.NextNumber != 4 {
		t.Fatalf("after B's new entry: %d entries, next %d; want 3 and 4", len(idx.Entries), idx.NextNumber)
	}
	newN := idx.Caches[b.ID].Numbers[2000].Number
	if e := idx.Entries[newN]; !slices.Equal(e.Deps, []uint64{execN}) {
		t.Fatalf("new entry deps %v, want [%d]", e.Deps, execN)
	}
	if len(idx.Caches[b.ID].Numbers) != 1 || len(idx.Caches[b.ID].Imported) != 2 {
		t.Fatalf("B's mappings %+v", idx.Caches[b.ID])
	}
	for _, n := range []uint64{baseN, execN} {
		if src := idx.Entries[n].Source.CacheID; src != h.cache.ID {
			t.Fatalf("entry %d now from %s", n, src)
		}
	}

	// A gets only the new entry merged: no echo of its own build.
	waitLookup(t, h.cache, "withExec(test)", hasOffer)
	st := h.debugImports(h.engine)
	if len(st.Merged) != 1 || !slices.Equal(st.Merged[0].Roots, []uint64{newN}) {
		t.Fatalf("A's merges %+v", st.Merged)
	}
	if h.cache.Merges() != 1 {
		t.Fatalf("A merged %d times", h.cache.Merges())
	}
	if e, _ := h.cache.Lookup("withExec(make)"); e.Number != 11 {
		t.Fatalf("A's exec landed on %d", e.Number)
	}
}

// waitImports waits until the index records n imports into cacheID.
func waitImports(t *testing.T, meta *metastore.Store, cacheID string, n int) *metastore.Index {
	t.Helper()
	return waitIndexFor(t, meta, func(idx *metastore.Index) bool {
		c := idx.Caches[cacheID]
		return c != nil && len(c.Imported) == n
	})
}

// waitMapped waits until the index maps an exported engine entry of cacheID.
func waitMapped(t *testing.T, meta *metastore.Store, cacheID string, number uint64) *metastore.Index {
	t.Helper()
	return waitIndexFor(t, meta, func(idx *metastore.Index) bool {
		c := idx.Caches[cacheID]
		_, ok := c.Lookup(number)
		return ok && c.Numbers[number].Number != 0
	})
}

func waitIndexFor(t *testing.T, meta *metastore.Store, ok func(*metastore.Index) bool) *metastore.Index {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		idx, err := meta.Index(context.Background(), testScope)
		if err != nil {
			t.Fatal(err)
		}
		if ok(idx) {
			return idx
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the index; it has %+v", idx.Caches)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitTracked waits until the tracker has seen cacheID's entries, each
// retained and stored.
func waitTracked(t *testing.T, h *exportHarness, cacheID string, numbers ...uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		seen := 0
		for _, e := range h.svc.export.tracker.Entries(cacheID) {
			if slices.Contains(numbers, e.Number) && e.Retained && e.Stored {
				seen++
			}
		}
		if seen == len(numbers) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %v tracked as stored: %+v", numbers, h.svc.export.tracker.Entries(cacheID))
}
