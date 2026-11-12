package exporter

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
)

// Check 3 of zerobull-consulting/dagger#133, through the path the service
// runs: the tracker's batches, exported from the engine and recorded. V is
// retained and exported in one session, then W, retained, over V in
// another: the tracker asks for W's parts only (its batches carry outputs
// for their roots), so the engine's bundle holds V's record without them.
// V's entry keeps its first record, with its fs part, and W depends on it.
func TestExportDependencyKeepsStoredParts(t *testing.T) {
	e := newTestEnv(t, nil)
	ctx := context.Background()
	e.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{layer("base"), layer("v")}})
	e.cache.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Layers: [][]byte{layer("base"), layer("v"), layer("w")}})
	tr := newTestTracker()
	export := func(n uint64) Result {
		t.Helper()
		o := state(n, 10, fsPart)
		o.CacheID, o.Generation, o.Session = e.cache.ID, e.cache.Generation, fmt.Sprintf("session-%d", n)
		tr.Observe(o, t0)
		batches := tr.All(e.cache.ID, t0)
		if len(batches) != 1 || !slices.Equal(batches[0].Request.Roots, []uint64{n}) {
			t.Fatalf("batches %+v", batches)
		}
		for _, out := range batches[0].Request.Outputs {
			if out.Number != n {
				t.Fatalf("the batch asks for an output of %d, not a root", out.Number)
			}
		}
		res, err := e.x.Export(ctx, e.eng, batches[0].Request)
		if err != nil {
			t.Fatal(err)
		}
		tr.Done(batches[0], res, t0)
		if !slices.Equal(res.Stored, []uint64{n}) {
			t.Fatalf("result %+v", res)
		}
		return res
	}

	first := export(10)
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	v := idx.Caches[e.cache.ID].Numbers[10].Number
	before := *idx.Entries[v]
	if before.Bundle != first.Bundles[0] || !slices.Equal(before.Parts, []string{fsPart}) {
		t.Fatalf("V's entry %+v", before)
	}

	second := export(11)
	bundle, err := e.meta.GetBundle(ctx, "team", second.Bundles[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range bundle.Outputs {
		for _, val := range bundle.Values {
			if val.Ordinal == o.Ordinal && val.SenderNumber == 10 {
				t.Fatalf("W's bundle holds an output of V: the test no longer covers a dependency without outputs")
			}
		}
	}
	idx, err = e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if after := idx.Entries[v]; after.Bundle != before.Bundle || after.Resolution != before.Resolution || !slices.Equal(after.Parts, before.Parts) {
		t.Fatalf("V's entry lost its record to W's bundle: before %+v, after %+v", before, *after)
	}
	w := idx.Caches[e.cache.ID].Numbers[11].Number
	if e := idx.Entries[w]; e.Bundle != second.Bundles[0] || !slices.Equal(e.Deps, []uint64{v}) || !slices.Equal(e.Parts, []string{fsPart}) {
		t.Fatalf("W's entry %+v", e)
	}
}
