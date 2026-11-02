package metastore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dagger/dagger/dagql"
)

// withParts adds an output of each part to ordinal o of b.
func withParts(b *dagql.ValueBundle, o dagql.TransferOrdinal, parts ...string) *dagql.ValueBundle {
	for _, p := range parts {
		b.Outputs = append(b.Outputs, dagql.TransferredOutput{Ordinal: o, Address: dagql.PersistedPartAddress{Part: dagql.PartKey(p)}, State: "completed"})
	}
	return b
}

// importChain records cache-a's chain(10), service numbers 1, 2, 3 (each with
// an fs part), and imports it into cache-b on engine entries 100, 101, 102.
func importChain(t *testing.T) *Index {
	t.Helper()
	idx := NewIndex()
	b := chain(10)
	for o := dagql.TransferOrdinal(1); o <= 3; o++ {
		withParts(b, o, "fs")
	}
	if _, err := idx.Record(strings.Repeat("a", 64), b, "cache-a", 1); err != nil {
		t.Fatal(err)
	}
	n := idx.RecordImports("cache-b", 1, []Import{{Number: 1, Engine: 100}, {Number: 2, Engine: 101}, {Number: 3, Engine: 102}})
	if n != 3 {
		t.Fatalf("recorded %d imports, want 3", n)
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
	return idx
}

// An importing engine's export of what it imported, as roots and in a new
// entry's closure, reuses the stored entries: no new numbers, no entry
// changes source, and the new entry depends on the original numbers.
func TestRecordReusesImports(t *testing.T) {
	idx := importChain(t)
	before := idx.Entries[3].Bundle

	// The imports themselves, re-exported as roots.
	b := testBundle([]val{{sender: 100}, {sender: 101, deps: []uint64{1}}, {sender: 102, deps: []uint64{2, 1}}}, root(3, 0))
	for o := dagql.TransferOrdinal(1); o <= 3; o++ {
		withParts(b, o, "fs")
	}
	nums, err := idx.Record(strings.Repeat("b", 64), b, "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(nums) != fmt.Sprint(map[dagql.TransferOrdinal]uint64{1: 1, 2: 2, 3: 3}) {
		t.Fatalf("numbers %v", nums)
	}
	if idx.NextNumber != 4 || len(idx.Entries) != 3 || idx.Entries[3].Bundle != before || idx.Entries[3].Source.CacheID != "cache-a" {
		t.Fatalf("re-export of imports changed the index: next %d, entries %d, entry 3 %+v", idx.NextNumber, len(idx.Entries), idx.Entries[3])
	}
	if len(idx.Caches["cache-b"].Numbers) != 0 || len(idx.Caches["cache-b"].Imported) != 3 {
		t.Fatalf("cache-b %+v", idx.Caches["cache-b"])
	}

	// A new entry over them: one new number, depending on the originals.
	b2 := testBundle([]val{{sender: 100}, {sender: 101, deps: []uint64{1}}, {sender: 102, deps: []uint64{2, 1}}, {sender: 103, deps: []uint64{3}}}, root(4, 0))
	nums, err = idx.Record(strings.Repeat("c", 64), b2, "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if nums[4] != 4 || idx.NextNumber != 5 || len(idx.Entries) != 4 {
		t.Fatalf("numbers %v, next %d, entries %d", nums, idx.NextNumber, len(idx.Entries))
	}
	if e := idx.Entries[4]; !slices.Equal(e.Deps, []uint64{3}) || e.Source != (Source{CacheID: "cache-b", Generation: 1, Number: 103}) {
		t.Fatalf("new entry %+v", e)
	}
	// The closure's records stay cache-a's: they hold no more parts.
	for n := uint64(1); n <= 3; n++ {
		if idx.Entries[n].Source.CacheID != "cache-a" {
			t.Fatalf("entry %d changed source: %+v", n, idx.Entries[n])
		}
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
}

// An importing engine's record that holds more parts than the stored one
// replaces it: the entry keeps its number, its source becomes the importing
// cache, and the old source's mapping becomes an import.
func TestRecordImportWithMoreParts(t *testing.T) {
	idx := importChain(t)
	b := withParts(testBundle([]val{{sender: 100}}, root(1, 0)), 1, "fs", "execMeta")
	nums, err := idx.Record(strings.Repeat("b", 64), b, "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if nums[1] != 1 || idx.NextNumber != 4 {
		t.Fatalf("numbers %v, next %d", nums, idx.NextNumber)
	}
	e := idx.Entries[1]
	if e.Source != (Source{CacheID: "cache-b", Generation: 1, Number: 100}) || !slices.Equal(e.Parts, []string{`{"part":"execMeta"}`, `{"part":"fs"}`}) {
		t.Fatalf("entry 1 %+v", e)
	}
	if m, ok := idx.Caches["cache-b"].Numbers[100]; !ok || m.Number != 1 {
		t.Fatalf("cache-b %+v", idx.Caches["cache-b"])
	}
	if _, ok := idx.Caches["cache-b"].Imported[100]; ok {
		t.Fatal("cache-b still imports 100")
	}
	if m, ok := idx.Caches["cache-a"].Imported[10]; !ok || m.Number != 1 {
		t.Fatalf("cache-a %+v", idx.Caches["cache-a"])
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
	// cache-a's later export of the same value, with fewer parts, keeps
	// cache-b's record, even from a later generation.
	if _, err := idx.Record(strings.Repeat("c", 64), withParts(testBundle([]val{{sender: 10}}, root(1, 0)), 1, "fs"), "cache-a", 5); err != nil {
		t.Fatal(err)
	}
	if idx.Entries[1].Source.CacheID != "cache-b" || idx.NextNumber != 4 {
		t.Fatalf("entry 1 %+v, next %d", idx.Entries[1], idx.NextNumber)
	}
	// A record with other parts, not more, doesn't replace it either.
	if _, err := idx.Record(strings.Repeat("d", 64), withParts(testBundle([]val{{sender: 10}}, root(1, 0)), 1, "fs", "other"), "cache-a", 5); err != nil {
		t.Fatal(err)
	}
	if idx.Entries[1].Source.CacheID != "cache-b" {
		t.Fatalf("entry 1 %+v", idx.Entries[1])
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRecordImportsSkips(t *testing.T) {
	idx := importChain(t)
	// Not stored, the cache's own export, and onto an exported engine entry.
	if _, err := idx.Record(strings.Repeat("b", 64), testBundle([]val{{sender: 200}}, root(1, 0)), "cache-b", 1); err != nil {
		t.Fatal(err)
	}
	n := idx.RecordImports("cache-b", 1, []Import{{Number: 99, Engine: 300}, {Number: 4, Engine: 301}, {Number: 1, Engine: 200}})
	if n != 0 {
		t.Fatalf("recorded %d", n)
	}
	if n := idx.RecordImports("cache-a", 1, []Import{{Number: 1, Engine: 10}}); n != 0 {
		t.Fatalf("recorded an import of cache-a's own entry")
	}
	// An import that landed elsewhere moves.
	if n := idx.RecordImports("cache-b", 2, []Import{{Number: 1, Engine: 150}}); n != 1 {
		t.Fatalf("recorded %d", n)
	}
	c := idx.Caches["cache-b"]
	if _, ok := c.Imported[100]; ok || c.Imported[150] != (Mapping{Number: 1, Generation: 2}) {
		t.Fatalf("cache-b imports %+v", c.Imported)
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateImports(t *testing.T) {
	for name, change := range map[string]func(*Index){
		"not stored":     func(idx *Index) { idx.Caches["cache-b"].Imported[100] = Mapping{Number: 99} },
		"own entry":      func(idx *Index) { idx.Caches["cache-a"].Imported = map[uint64]Mapping{50: {Number: 1}} },
		"also exported":  func(idx *Index) { idx.Caches["cache-a"].Imported = map[uint64]Mapping{10: {Number: 1}} },
		"imported twice": func(idx *Index) { idx.Caches["cache-b"].Imported[105] = Mapping{Number: 1} },
	} {
		idx := importChain(t)
		change(idx)
		if err := idx.Validate(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// Dropped entries' imports go with them; a wiped cache's imports all go.
func TestDropExpiredAndForgetImports(t *testing.T) {
	idx := importChain(t)
	idx.Roots[3] = Root{ExpiresAtUnix: 10}
	if dropped := idx.DropExpired(20); !slices.Equal(dropped, []uint64{1, 2, 3}) {
		t.Fatalf("dropped %v", dropped)
	}
	if len(idx.Caches["cache-b"].Imported) != 0 {
		t.Fatalf("imports left %+v", idx.Caches["cache-b"].Imported)
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}

	idx = importChain(t)
	idx.ForgetImports("cache-b")
	if _, ok := idx.Caches["cache-b"]; ok {
		t.Fatalf("cache-b kept: %+v", idx.Caches["cache-b"])
	}
	if len(idx.Entries) != 3 || len(idx.Caches["cache-a"].Numbers) != 3 {
		t.Fatalf("forgetting imports dropped entries")
	}
	// A wiped cache that exported too keeps its exports.
	idx = importChain(t)
	if _, err := idx.Record(strings.Repeat("b", 64), testBundle([]val{{sender: 200}}, root(1, 0)), "cache-b", 1); err != nil {
		t.Fatal(err)
	}
	idx.ForgetImports("cache-b")
	if c := idx.Caches["cache-b"]; c == nil || len(c.Imported) != 0 || len(c.Numbers) != 1 {
		t.Fatalf("cache-b %+v", c)
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRecordImports(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	if _, err := s.Record(ctx, "s1", chain(10), "cache-a", 1); err != nil {
		t.Fatal(err)
	}
	n, err := s.RecordImports(ctx, "s1", "cache-b", 1, []Import{{Number: 3, Engine: 7}})
	if err != nil || n != 1 {
		t.Fatalf("%d, %v", n, err)
	}
	if m := mustIndex(t, s, "s1").Caches["cache-b"].Imported[7]; m.Number != 3 {
		t.Fatalf("mapping %+v", m)
	}
	if err := s.ForgetImports(ctx, "s1", "cache-b"); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustIndex(t, s, "s1").Caches["cache-b"]; ok {
		t.Fatal("imports not forgotten")
	}
	if _, err := s.RecordImports(ctx, "s1", "", 1, []Import{{Number: 3, Engine: 7}}); err == nil {
		t.Fatal("no cacheID: no error")
	}
}
