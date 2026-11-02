package server

// #47: a bundle that holds an engine's imports stays mergeable after those
// imports move to other engine entries, or are dropped by a wipe.

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

func TestBundleOfMovedImportsMerges(t *testing.T) {
	for _, how := range []string{"moved", "wiped"} {
		t.Run(how, func(t *testing.T) {
			h, _ := newMemExportHarness(t, nil)
			testBundleOfMovedImports(t, h, how)
		})
	}
}

// testBundleOfMovedImports: A exports its build; B has it merged and builds
// withExec(test) on it, which it exports; then B's imports move, as when it
// prunes them, restarts and has them merged again, or are dropped, as when
// it is wiped. A fresh engine C still gets B's bundle merged, with B's test
// on A's exec: no merge into C fails.
func testBundleOfMovedImports(t *testing.T, h *exportHarness, how string) {
	t.Helper()
	ctx := context.Background()
	layers := h.buildRecipes()
	h.exportBuild()
	idx := h.waitIndex(10, 11)
	baseN := idx.Caches[h.cache.ID].Numbers[10].Number
	execN := idx.Caches[h.cache.ID].Numbers[11].Number

	b := fakeengine.New("cache-b", 1)
	h.connectEngine(b)
	base := waitLookup(t, b, "from(alpine)", hasOffer)
	exec := waitLookup(t, b, "withExec(make)", hasOffer)
	waitImports(t, h.meta, b.ID, 2)
	b.Add(fakeengine.Entry{Number: 2000, Recipe: "withExec(test)", Deps: []uint64{exec.Number}, Layers: [][]byte{layers[0], layers[1], testLayer("test")}})
	h.postSpansOf(b, "b-1", cacheSpan(base.Number), cacheSpan(exec.Number, itoa(base.Number)), cacheSpan(2000, itoa(exec.Number)), carrierSpan(3))
	idx = waitMapped(t, h.meta, b.ID, 2000)
	testN := idx.Caches[b.ID].Numbers[2000].Number
	bBundle := idx.Entries[testN].Bundle
	if res := idx.Entries[testN].Resolution; res == "" {
		t.Fatalf("B's record of test names no resolution")
	}

	switch how {
	case "moved":
		moved := []metastore.Import{{Number: baseN, Engine: base.Number + 5000}, {Number: execN, Engine: exec.Number + 5000}}
		if _, err := h.meta.RecordImports(ctx, testScope, b.ID, 2, moved); err != nil {
			t.Fatal(err)
		}
	case "wiped":
		if err := h.meta.ForgetImports(ctx, testScope, b.ID); err != nil {
			t.Fatal(err)
		}
	}
	idx, err := h.meta.Index(ctx, testScope)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.Caches[b.ID].Lookup(exec.Number); ok {
		t.Fatalf("B's imports didn't %s: %+v", how, idx.Caches[b.ID])
	}

	c := fakeengine.New("cache-c", 1)
	engC := h.connectEngine(c)
	waitLookup(t, c, "withExec(test)", hasOffer)
	// The engine has the merge before the importer commits it: wait for
	// the importer's state.
	var st importer.State
	deadline := time.Now().Add(10 * time.Second)
	for {
		st = h.debugImports(engC)
		if slices.ContainsFunc(st.Merged, func(m importer.MergedBundle) bool { return m.Bundle == bBundle }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected B's bundle %s merged into C after B's imports %s; C's merges: %+v, failures: %v", bBundle, how, st.Merged, st.Failures)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(st.Failures) != 0 {
		t.Fatalf("expected no failed merges into C; saw %v", st.Failures)
	}
	for _, n := range []uint64{baseN, execN, testN} {
		if _, ok := st.Values[n]; !ok {
			t.Fatalf("expected C to hold stored entry %d; it holds %v", n, keys(st.Values))
		}
	}
	// C's test sits on A's exec: the engine resolved test's dependency to
	// the entry it holds for A's exec.
	cTest, _ := c.Lookup("withExec(test)")
	cExec, _ := c.Lookup("withExec(make)")
	if !slices.Equal(cTest.Deps, []uint64{cExec.Number}) {
		t.Fatalf("expected C's test to depend on its exec %d; it depends on %v", cExec.Number, cTest.Deps)
	}
}

func keys[V any](m map[uint64]V) []string {
	var out []string
	for k := range m {
		out = append(out, fmt.Sprint(k))
	}
	slices.Sort(out)
	return out
}
