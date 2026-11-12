package gc

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// exportRoots stores, as the service's export path does, an export of
// roots with their own fs parts only, as the tracker asks for them: a
// dependency in the closure has no outputs in the bundle.
func (e *env) exportRoots(ns, cacheID string, entries []fakeengine.Entry, roots ...uint64) metastore.Recorded {
	e.t.Helper()
	c := fakeengine.New(cacheID, 1)
	for _, en := range entries {
		c.Add(en)
	}
	req := protocol.Export{Roots: roots}
	for _, r := range roots {
		req.Outputs = append(req.Outputs, protocol.ExportOutput{Number: r, Address: fakeengine.FSPart})
	}
	capt, err := c.Capture(req)
	if err != nil {
		e.t.Fatal(err)
	}
	v := e.view(ns)
	for _, data := range capt.Blobs {
		if _, ok := v.Get(digest.FromBytes(data)); !ok {
			v.Put(data)
		}
	}
	rec, err := e.meta.Record(context.Background(), ns, capt.Bundle, cacheID, 1)
	if err != nil {
		e.t.Fatal(err)
	}
	return rec
}

// zerobull-consulting/dagger#133, check 3, at garbage collection: a stored
// value carried as a dependency of a later root keeps its first record, so
// the bundle that holds its outputs stays named, and collection keeps it.
// Before the fix, the later bundle took the value's record, with no parts,
// and the first bundle, named by no entry, was deleted with its outputs.
func TestDependencyKeepsItsBundle(t *testing.T) {
	e := newEnv(t)
	entries := buildX("x")
	first := e.exportRoots("team", "a", entries, 1)
	second := e.exportRoots("team", "a", entries, 2)
	if first.BundleID == second.BundleID {
		t.Fatal("same bundle")
	}
	c := e.collector(nil)
	e.run(c)
	e.clock.Add(100 * 24 * time.Hour)
	rep := reportOf(e.run(c), "team")
	if len(rep.Deleted) != 0 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	keys := e.keys("team")
	if !slices.Contains(keys, metastore.BundleRelKey(first.BundleID)) {
		t.Fatalf("the base's bundle, which holds its fs output, was deleted: %v", keys)
	}
	idx := e.index("team")
	base := idx.Entries[first.Numbers[1]]
	if base.Bundle != first.BundleID || !slices.Equal(base.Parts, []string{`{"part":"fs"}`}) {
		t.Fatalf("base entry %+v", base)
	}
	e.checkConsistent("team")
}
