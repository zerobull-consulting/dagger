package metastore

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/dagger/dagger/dagql"
)

// The checks of zerobull-consulting/dagger#133 at the index: a value of an
// engine's own cache recorded again with more, or fewer, parts. Each bundle
// is shaped as the engine exports one: outputs for the roots only.

var (
	fsKey   = `{"part":"fs"}`
	metaKey = `{"part":"execMeta"}`
)

// recordAt records b from cache "c" at generation, at nowUnix.
func recordAt(t *testing.T, idx *Index, id string, b *dagql.ValueBundle, generation uint64, nowUnix int64) map[dagql.TransferOrdinal]uint64 {
	t.Helper()
	nums, _, err := idx.RecordResolvedAt(strings.Repeat(id, 64), b, "c", generation, nowUnix)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
	return nums
}

// Check 1: a record of the same value with every stored part and more
// replaces the stored one.
func TestRecordSameValueMoreParts(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	if _, err := s.Record(ctx, "s", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs"), "c", 1); err != nil {
		t.Fatal(err)
	}
	r2, err := s.Record(ctx, "s", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs", "execMeta"), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e := r2.Index.Entries[1]; e.Bundle != r2.BundleID || e.Resolution != r2.ResolutionID || !slices.Equal(e.Parts, []string{metaKey, fsKey}) {
		t.Fatalf("the record with more parts didn't replace the stored one: %+v", e)
	}
}

// Check 2: a record of the same value with fewer parts, as from an earlier,
// slower export of the cache finishing late, keeps the stored record. Its
// root counts as recorded: its recording time moves.
func TestRecordSameValueFewerPartsKeepsStored(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	r1, err := s.Record(ctx, "s", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs", "execMeta"), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Record(ctx, "s", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs"), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e := r2.Index.Entries[1]; e.Bundle != r1.BundleID || e.Resolution != r1.ResolutionID || !slices.Equal(e.Parts, []string{metaKey, fsKey}) {
		t.Fatalf("a record with fewer parts replaced the stored one: %+v", e)
	}

	idx := NewIndex()
	recordAt(t, idx, "a", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs", "execMeta"), 1, 100)
	recordAt(t, idx, "b", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs"), 1, 200)
	if e := idx.Entries[1]; e.Bundle != strings.Repeat("a", 64) || idx.Roots[1].RecordedAtUnix != 200 {
		t.Fatalf("entry %+v, root %+v", e, idx.Roots[1])
	}
}

// Check 3: a stored value carried in a later bundle as a dependency of
// another root has no outputs there (the engine exports the roots' outputs
// only). Its stored record and parts stay, at the same generation and at a
// later one (a clean restart, which keeps replacement counts). The new root
// depends on it by its service number.
func TestRecordDependencyKeepsStoredParts(t *testing.T) {
	for _, generation := range []uint64{1, 2} {
		idx := NewIndex()
		recordAt(t, idx, "a", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs", "execMeta"), 1, 100)
		w := withParts(testBundle([]val{{sender: 5}, {sender: 6, deps: []uint64{1}}}, root(2, 0)), 2, "fs", "execMeta")
		nums := recordAt(t, idx, "b", w, generation, 200)
		if nums[1] != 1 || nums[2] != 2 {
			t.Fatalf("generation %d: numbers %v", generation, nums)
		}
		v := idx.Entries[1]
		if v.Bundle != strings.Repeat("a", 64) || !slices.Equal(v.Parts, []string{metaKey, fsKey}) || v.Source.Generation != 1 {
			t.Fatalf("generation %d: the dependency's record was replaced: %+v", generation, v)
		}
		// The dependency's root keeps its time: this bundle doesn't root it.
		if idx.Roots[1].RecordedAtUnix != 100 {
			t.Fatalf("generation %d: root %+v", generation, idx.Roots[1])
		}
		if m := idx.Caches["c"].Numbers[5]; m.Generation != generation {
			t.Fatalf("generation %d: mapping %+v", generation, m)
		}
		if w := idx.Entries[2]; w.Bundle != strings.Repeat("b", 64) || !slices.Equal(w.Deps, []uint64{1}) {
			t.Fatalf("generation %d: new root %+v", generation, w)
		}
	}
}

// A record of a new value (more replacements) replaces the stored one
// whatever its parts: the stored parts are the old value's.
func TestRecordNewValueFewerPartsReplaces(t *testing.T) {
	idx := NewIndex()
	recordAt(t, idx, "a", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs", "execMeta"), 1, 100)
	w := withParts(testBundle([]val{{sender: 5, replacements: 1}, {sender: 6, deps: []uint64{1}}}, root(2, 0)), 2, "fs")
	recordAt(t, idx, "b", w, 1, 200)
	if v := idx.Entries[1]; v.Bundle != strings.Repeat("b", 64) || v.Replacements != 1 || len(v.Parts) != 0 {
		t.Fatalf("the new value's record didn't replace the old one: %+v", v)
	}
}

// Intentional: a record of the same value with other parts, neither more
// nor fewer, keeps the stored record, as an imported value's does
// (TestRecordKnownGapImportWithOtherParts): an entry names one record, and
// records of two bundles can't be merged. The new part reaches the index
// once a record holds both. The tracker keeps every part it has seen of a
// value, so its next export of the value holds them all.
func TestRecordSameValueOtherPartsKeepsStored(t *testing.T) {
	idx := NewIndex()
	recordAt(t, idx, "a", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs"), 1, 100)
	recordAt(t, idx, "b", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "execMeta"), 1, 200)
	if v := idx.Entries[1]; v.Bundle != strings.Repeat("a", 64) || !slices.Equal(v.Parts, []string{fsKey}) {
		t.Fatalf("entry %+v", v)
	}
	recordAt(t, idx, "c", withParts(testBundle([]val{{sender: 5}}, root(1, 0)), 1, "fs", "execMeta"), 1, 300)
	if v := idx.Entries[1]; v.Bundle != strings.Repeat("c", 64) || !slices.Equal(v.Parts, []string{metaKey, fsKey}) {
		t.Fatalf("entry %+v", v)
	}
}
