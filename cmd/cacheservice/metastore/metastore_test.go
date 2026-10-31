package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dagger/dagger/dagql"
)

// val is a test value: an engine entry number, the ordinals it depends on,
// and its replacements and expiry.
type val struct {
	sender       uint64
	deps         []uint64
	replacements uint64
	expires      int64
}

// testBundle returns a bundle of vals, ordinals from 1, rooted at the given
// ordinals.
func testBundle(vals []val, roots ...dagql.TransferredRoot) *dagql.ValueBundle {
	b := &dagql.ValueBundle{Version: 3, Roots: roots}
	for i, v := range vals {
		b.Values = append(b.Values, dagql.TransferredValue{
			Ordinal:            dagql.TransferOrdinal(i + 1),
			SenderNumber:       v.sender,
			SenderReplacements: v.replacements,
			DependencyIDs:      v.deps,
			ExpiresAtUnix:      v.expires,
			Record:             dagql.PersistedRecord{ResultID: uint64(i + 1)},
		})
	}
	return b
}

func root(ordinal uint64, expires int64) dagql.TransferredRoot {
	return dagql.TransferredRoot{Ordinal: dagql.TransferOrdinal(ordinal), ExpiresAtUnix: expires}
}

// chain is a bundle of three entries, 10 <- 11 <- 12, rooted at 12.
func chain(base uint64) *dagql.ValueBundle {
	return testBundle([]val{
		{sender: base},
		{sender: base + 1, deps: []uint64{1}},
		{sender: base + 2, deps: []uint64{2, 1}},
	}, root(3, 0))
}

func newTestStore(t *testing.T, objects Objects) *Store {
	t.Helper()
	s, err := New(Config{Objects: objects, Prefix: "/team/"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustIndex(t *testing.T, s *Store, scope string) *Index {
	t.Helper()
	idx, err := s.Index(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("New with no object store: no error")
	}
	if _, err := New(Config{Objects: NewMemObjects(), MaxAttempts: -1}); err == nil {
		t.Error("New with negative max attempts: no error")
	}
}

func TestKeys(t *testing.T) {
	id := strings.Repeat("ab", 32)
	for _, tc := range []struct {
		prefix, wantIndex, wantBundle string
	}{
		{"", "s1/index.json", "s1/bundles/" + id + ".json"},
		{"team", "team/s1/index.json", "team/s1/bundles/" + id + ".json"},
		{"/a/b/", "a/b/s1/index.json", "a/b/s1/bundles/" + id + ".json"},
	} {
		s, err := New(Config{Objects: NewMemObjects(), Prefix: tc.prefix})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := s.IndexKey("s1"); err != nil || got != tc.wantIndex {
			t.Errorf("prefix %q: index key %q, %v; want %q", tc.prefix, got, err, tc.wantIndex)
		}
		if got, err := s.BundleKey("s1", id); err != nil || got != tc.wantBundle {
			t.Errorf("prefix %q: bundle key %q, %v; want %q", tc.prefix, got, err, tc.wantBundle)
		}
	}
}

func TestKeysRejectBadInput(t *testing.T) {
	s := newTestStore(t, NewMemObjects())
	for _, scope := range []string{"", ".", "..", "a/b", "a..b", "-a", "blobs", "a b", strings.Repeat("a", 129), "é"} {
		if key, err := s.IndexKey(scope); err == nil {
			t.Errorf("scope %q: index key %q, want an error", scope, key)
		}
	}
	for _, id := range []string{"", "abc", strings.Repeat("AB", 32), "../" + strings.Repeat("a", 61)} {
		if key, err := s.BundleKey("s1", id); err == nil {
			t.Errorf("bundle ID %q: key %q, want an error", id, key)
		}
	}
	for _, scope := range []string{"a", "team-1", "Org_2.prod", strings.Repeat("a", 128)} {
		if err := ValidateScope(scope); err != nil {
			t.Errorf("scope %q: %v", scope, err)
		}
	}
}

func TestBundleRoundTrip(t *testing.T) {
	ctx := context.Background()
	mem := NewMemObjects()
	s := newTestStore(t, mem)
	b := chain(10)

	id, err := s.PutBundle(ctx, "s1", b)
	if err != nil {
		t.Fatal(err)
	}
	wantID, _, err := BundleID(b)
	if err != nil {
		t.Fatal(err)
	}
	if id != wantID {
		t.Fatalf("ID %s, want %s", id, wantID)
	}
	got, err := s.GetBundle(ctx, "s1", id)
	if err != nil {
		t.Fatal(err)
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(b)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("bundle came back as %s, want %s", gotJSON, wantJSON)
	}

	// Storing it again is a no-op with the same ID.
	again, err := s.PutBundle(ctx, "s1", b)
	if err != nil || again != id {
		t.Fatalf("second put: %s, %v; want %s", again, err, id)
	}
	if keys := mem.Keys(); len(keys) != 1 || keys[0] != "team/s1/bundles/"+id+".json" {
		t.Fatalf("keys %v", keys)
	}

	// Another bundle has another ID.
	other, err := s.PutBundle(ctx, "s1", chain(20))
	if err != nil || other == id {
		t.Fatalf("other bundle: %s, %v", other, err)
	}

	// A bundle whose content doesn't hash to its key is refused.
	otherKey, _ := s.BundleKey("s1", other)
	if _, err := mem.Put(ctx, otherKey, []byte(`{"version":3,"roots":[]}`), Condition{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetBundle(ctx, "s1", other); !errors.Is(err, ErrCorruptBundle) {
		t.Fatalf("corrupt bundle: %v, want ErrCorruptBundle", err)
	}

	if _, err := s.GetBundle(ctx, "s1", strings.Repeat("0", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing bundle: %v, want ErrNotFound", err)
	}
	if _, err := s.PutBundle(ctx, "s1", nil); err == nil {
		t.Fatal("nil bundle: no error")
	}
}

func TestIndexOfEmptyScope(t *testing.T) {
	s := newTestStore(t, NewMemObjects())
	idx := mustIndex(t, s, "s1")
	if idx.NextNumber != 1 || len(idx.Entries) != 0 || len(idx.Roots) != 0 || len(idx.Caches) != 0 {
		t.Fatalf("empty scope: %+v", idx)
	}
}

func TestRecordNumbersAndDeps(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())

	rec, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	want := map[dagql.TransferOrdinal]uint64{1: 1, 2: 2, 3: 3}
	if fmt.Sprint(rec.Numbers) != fmt.Sprint(want) {
		t.Fatalf("numbers %v, want %v", rec.Numbers, want)
	}
	idx := mustIndex(t, s, "s1")
	if idx.NextNumber != 4 {
		t.Fatalf("next number %d, want 4", idx.NextNumber)
	}
	if e := idx.Entries[3]; !slices.Equal(e.Deps, []uint64{1, 2}) || e.Bundle != rec.BundleID || e.Ordinal != 3 ||
		e.Source != (Source{CacheID: "cache-a", Generation: 1, Number: 12}) {
		t.Fatalf("entry 3: %+v", e)
	}
	if !slices.Equal(idx.Entries[2].Deps, []uint64{1}) || len(idx.Entries[1].Deps) != 0 {
		t.Fatalf("deps: %v, %v", idx.Entries[2].Deps, idx.Entries[1].Deps)
	}
	if _, ok := idx.Roots[3]; !ok || len(idx.Roots) != 1 {
		t.Fatalf("roots %v, want only 3", idx.Roots)
	}
	if m := idx.Caches["cache-a"].Numbers[11]; m != (Mapping{Number: 2, Generation: 1}) {
		t.Fatalf("mapping of 11: %+v", m)
	}

	// The same engine entries, exported again at a later generation, keep
	// their service numbers.
	rec2, err := s.Record(ctx, "s1", chain(10), "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rec2.Numbers) != fmt.Sprint(want) {
		t.Fatalf("re-export numbers %v, want %v", rec2.Numbers, want)
	}
	idx = mustIndex(t, s, "s1")
	if idx.NextNumber != 4 || idx.Caches["cache-a"].Generation != 2 || idx.Caches["cache-a"].Numbers[10].Generation != 2 {
		t.Fatalf("after re-export: next %d, cache %+v", idx.NextNumber, idx.Caches["cache-a"])
	}

	// The same engine numbers from another cache are other entries.
	rec3, err := s.Record(ctx, "s1", chain(10), "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	want3 := map[dagql.TransferOrdinal]uint64{1: 4, 2: 5, 3: 6}
	if fmt.Sprint(rec3.Numbers) != fmt.Sprint(want3) {
		t.Fatalf("cache-b numbers %v, want %v", rec3.Numbers, want3)
	}
	if !slices.Equal(rec3.Index.Entries[6].Deps, []uint64{4, 5}) {
		t.Fatalf("cache-b entry 6 deps %v", rec3.Index.Entries[6].Deps)
	}

	// A bundle sharing an entry with an earlier one reuses its number.
	shared := testBundle([]val{{sender: 11}, {sender: 30, deps: []uint64{1}}}, root(2, 0))
	rec4, err := s.Record(ctx, "s1", shared, "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if rec4.Numbers[1] != 2 || rec4.Numbers[2] != 7 {
		t.Fatalf("shared bundle numbers %v, want 1:2 2:7", rec4.Numbers)
	}
	// Entry 2 now comes from the new bundle, and 3 still depends on it.
	idx = rec4.Index
	if idx.Entries[2].Bundle != rec4.BundleID || len(idx.Entries[2].Deps) != 0 {
		t.Fatalf("entry 2 after the shared bundle: %+v", idx.Entries[2])
	}
	if !slices.Equal(idx.Entries[3].Deps, []uint64{1, 2}) {
		t.Fatalf("entry 3 deps %v", idx.Entries[3].Deps)
	}
}

func TestRecordKeepsNewerRecord(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	newer := testBundle([]val{{sender: 5, replacements: 3, expires: 100}}, root(1, 0))
	older := testBundle([]val{{sender: 5, replacements: 2, expires: 200}}, root(1, 0))
	same := testBundle([]val{{sender: 5, replacements: 3, expires: 300}}, root(1, 0))

	r1, err := s.Record(ctx, "s1", newer, "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Record(ctx, "s1", older, "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e := r2.Index.Entries[1]; e.Bundle != r1.BundleID || e.Replacements != 3 || e.ExpiresAtUnix != 100 {
		t.Fatalf("an older record replaced a newer one: %+v", e)
	}
	r3, err := s.Record(ctx, "s1", same, "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e := r3.Index.Entries[1]; e.Bundle != r3.BundleID || e.ExpiresAtUnix != 300 {
		t.Fatalf("a record with as many replacements didn't replace: %+v", e)
	}
}

// A record from an earlier generation that arrives late doesn't replace one
// from a later generation, whatever its replacement count.
func TestRecordFencesOlderGenerations(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	gen2 := testBundle([]val{{sender: 5, replacements: 0, expires: 100}}, root(1, 0))
	gen1 := testBundle([]val{{sender: 5, replacements: 9, expires: 200}}, root(1, 0))
	gen3 := testBundle([]val{{sender: 5, replacements: 0, expires: 300}}, root(1, 0))

	r2, err := s.Record(ctx, "s1", gen2, "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := s.Record(ctx, "s1", gen1, "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e := r1.Index.Entries[1]; e.Bundle != r2.BundleID || e.Source.Generation != 2 || e.ExpiresAtUnix != 100 {
		t.Fatalf("a late generation 1 record replaced generation 2: %+v", e)
	}
	if m := r1.Index.Caches["c"].Numbers[5]; m.Generation != 2 {
		t.Fatalf("mapping generation went back to %d", m.Generation)
	}
	r3, err := s.Record(ctx, "s1", gen3, "c", 3)
	if err != nil {
		t.Fatal(err)
	}
	if e := r3.Index.Entries[1]; e.Bundle != r3.BundleID || e.Source.Generation != 3 {
		t.Fatalf("a generation 3 record didn't replace generation 2: %+v", e)
	}
}

// An entry keeps alive the owners of its pending offers and of its outputs,
// not only its direct dependencies, so DropExpired keeps them too.
func TestRecordRetainsOfferAndOutputOwners(t *testing.T) {
	b := testBundle([]val{{sender: 1}, {sender: 2}, {sender: 3}}, root(3, 0))
	b.Values[2].Record.Envelope.PendingOffers = []dagql.PersistedPartOffer{
		{Owner: dagql.PersistedOfferOwner{DependencyIDs: []uint64{1}}},
	}
	b.Outputs = []dagql.TransferredOutput{{Ordinal: 3, Owner: &dagql.PersistedOfferOwner{DependencyIDs: []uint64{2, 1}}}}
	idx := NewIndex()
	if _, err := idx.Record(strings.Repeat("a", 64), b, "c", 1); err != nil {
		t.Fatal(err)
	}
	if deps := idx.Entries[3].Deps; !slices.Equal(deps, []uint64{1, 2}) {
		t.Fatalf("deps %v, want [1 2]", deps)
	}
	if dropped := idx.DropExpired(1000); len(dropped) != 0 {
		t.Fatalf("dropped %v, owners of a retained root's offers and outputs", dropped)
	}
}

func TestRecordRootExpiry(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	record := func(expires int64) int64 {
		t.Helper()
		r, err := s.Record(ctx, "s1", testBundle([]val{{sender: 1}}, root(1, expires)), "c", 1)
		if err != nil {
			t.Fatal(err)
		}
		return r.Index.Roots[1].ExpiresAtUnix
	}
	for _, tc := range []struct{ expires, want int64 }{
		{100, 100},
		{50, 100}, // earlier: kept
		{200, 200},
		{0, 0},   // never: wins
		{300, 0}, // a date after never: kept
	} {
		if got := record(tc.expires); got != tc.want {
			t.Fatalf("after a root expiring at %d: %d, want %d", tc.expires, got, tc.want)
		}
	}
}

func TestRecordRejectsBadInput(t *testing.T) {
	ctx := context.Background()
	mem := NewMemObjects()
	s := newTestStore(t, mem)
	if _, err := s.Record(ctx, "s1", chain(1), "", 1); err == nil {
		t.Error("no cacheID: no error")
	}
	if _, err := s.Record(ctx, "s1", testBundle([]val{{sender: 1}}), "c", 1); err == nil {
		t.Error("no roots: no error")
	}
	if _, err := s.Record(ctx, "../x", chain(1), "c", 1); err == nil {
		t.Error("bad scope: no error")
	}
	if keys := mem.Keys(); len(keys) != 0 {
		t.Errorf("rejected records wrote %v", keys)
	}
	idx := NewIndex()
	if _, err := idx.Record("nope", chain(1), "c", 1); err == nil {
		t.Error("bad bundle ID: no error")
	}
}

func TestValidateBundle(t *testing.T) {
	ok := chain(1)
	if err := ValidateBundle(ok); err != nil {
		t.Fatalf("valid bundle: %v", err)
	}
	for _, tc := range []struct {
		name   string
		modify func(*dagql.ValueBundle)
	}{
		{"no roots", func(b *dagql.ValueBundle) { b.Roots = nil }},
		{"root out of range", func(b *dagql.ValueBundle) { b.Roots = []dagql.TransferredRoot{root(4, 0)} }},
		{"zero ordinal", func(b *dagql.ValueBundle) { b.Values[0].Ordinal = 0 }},
		{"duplicate ordinal", func(b *dagql.ValueBundle) { b.Values[1].Ordinal = 1 }},
		{"no sender", func(b *dagql.ValueBundle) { b.Values[0].SenderNumber = 0 }},
		{"duplicate sender", func(b *dagql.ValueBundle) { b.Values[1].SenderNumber = b.Values[0].SenderNumber }},
		{"dep out of range", func(b *dagql.ValueBundle) { b.Values[0].DependencyIDs = []uint64{9} }},
		{"self dep", func(b *dagql.ValueBundle) { b.Values[0].DependencyIDs = []uint64{1} }},
		{"output out of range", func(b *dagql.ValueBundle) {
			b.Outputs = []dagql.TransferredOutput{{Ordinal: 7}}
		}},
		{"unsupported version", func(b *dagql.ValueBundle) { b.Version = 2 }},
		{"pending offer owner out of range", func(b *dagql.ValueBundle) {
			b.Values[0].Record.Envelope.PendingOffers = []dagql.PersistedPartOffer{{Owner: dagql.PersistedOfferOwner{DependencyIDs: []uint64{9}}}}
		}},
		{"output owner out of range", func(b *dagql.ValueBundle) {
			b.Outputs = []dagql.TransferredOutput{{Ordinal: 1, Owner: &dagql.PersistedOfferOwner{DependencyIDs: []uint64{9}}}}
		}},
		{"output owner is itself", func(b *dagql.ValueBundle) {
			b.Outputs = []dagql.TransferredOutput{{Ordinal: 2, Owner: &dagql.PersistedOfferOwner{DependencyIDs: []uint64{2}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := chain(1)
			tc.modify(b)
			if err := ValidateBundle(b); err == nil {
				t.Fatal("no error")
			}
		})
	}
	if err := ValidateBundle(nil); err == nil {
		t.Fatal("nil bundle: no error")
	}
}

// A new Store over the same objects, as after a service restart, sees
// everything the old one wrote, and goes on numbering where it stopped.
func TestRestartKeepsState(t *testing.T) {
	ctx := context.Background()
	mem := NewMemObjects()
	before := newTestStore(t, mem)
	r1, err := before.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := before.DropExpired(ctx, "s1", time.Now().Unix()); err != nil {
		t.Fatal(err)
	}

	after := newTestStore(t, mem)
	idx := mustIndex(t, after, "s1")
	wantJSON, _ := json.Marshal(r1.Index)
	gotJSON, _ := json.Marshal(idx)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("after restart: %s, want %s", gotJSON, wantJSON)
	}
	if _, err := after.GetBundle(ctx, "s1", r1.BundleID); err != nil {
		t.Fatalf("bundle after restart: %v", err)
	}
	r2, err := after.Record(ctx, "s1", chain(10), "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(r2.Numbers) != fmt.Sprint(r1.Numbers) {
		t.Fatalf("after restart, the same entries have numbers %v, want %v", r2.Numbers, r1.Numbers)
	}
	r3, err := after.Record(ctx, "s1", chain(50), "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Numbers[1] != 4 {
		t.Fatalf("after restart, a new entry has number %d, want 4", r3.Numbers[1])
	}
}

// Many writers, each with its own Store over the same objects, as several
// service replicas would be, record at once. Every update survives, and no
// two entries share a number.
func TestConcurrentWritersLoseNothing(t *testing.T) {
	ctx := context.Background()
	mem := NewMemObjects()
	const writers, perWriter = 8, 10
	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for w := range writers {
		s, err := New(Config{Objects: mem, MaxAttempts: 10000})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				b := testBundle([]val{{sender: uint64(100*i + 1)}, {sender: uint64(100*i + 2), deps: []uint64{1}}}, root(2, 0))
				if _, err := s.Record(ctx, "s1", b, fmt.Sprintf("cache-%d", w), 1); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	s, err := New(Config{Objects: mem})
	if err != nil {
		t.Fatal(err)
	}
	idx := mustIndex(t, s, "s1")
	const total = writers * perWriter * 2
	if len(idx.Entries) != total || idx.NextNumber != total+1 {
		t.Fatalf("%d entries, next number %d; want %d, %d", len(idx.Entries), idx.NextNumber, total, total+1)
	}
	if len(idx.Roots) != writers*perWriter {
		t.Fatalf("%d roots, want %d", len(idx.Roots), writers*perWriter)
	}
	for w := range writers {
		c := idx.Caches[fmt.Sprintf("cache-%d", w)]
		if c == nil || len(c.Numbers) != perWriter*2 {
			t.Fatalf("cache-%d: %+v", w, c)
		}
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
}

// interfere writes the index behind the Store's back, before the Store's
// next n writes, as a concurrent writer would.
type interfere struct {
	Objects
	key   string
	n     atomic.Int32
	puts  atomic.Int32
	other func(*Index)
}

func (f *interfere) Put(ctx context.Context, key string, body []byte, cond Condition) (string, error) {
	f.puts.Add(1)
	if key == f.key && f.n.Add(-1) >= 0 {
		raw, etag, err := f.Objects.Get(ctx, key)
		idx := NewIndex()
		if err == nil {
			if idx, err = decodeIndex(raw); err != nil {
				return "", err
			}
		}
		f.other(idx)
		otherBody, _ := json.Marshal(idx)
		otherCond := Condition{IfMatch: etag}
		if etag == "" {
			otherCond = Condition{IfNoneMatch: true}
		}
		if _, err := f.Objects.Put(ctx, key, otherBody, otherCond); err != nil {
			return "", err
		}
	}
	return f.Objects.Put(ctx, key, body, cond)
}

func TestUpdateIndexRetriesOnAFreshIndex(t *testing.T) {
	ctx := context.Background()
	f := &interfere{Objects: NewMemObjects(), key: "s1/index.json"}
	f.n.Store(3)
	f.other = func(idx *Index) { idx.allocate() }
	s, err := New(Config{Objects: f})
	if err != nil {
		t.Fatal(err)
	}
	s.sleep = func(context.Context, time.Duration) error { return nil }
	calls := 0
	var seen []uint64
	idx, err := s.UpdateIndex(ctx, "s1", func(idx *Index) error {
		calls++
		seen = append(seen, idx.NextNumber)
		idx.allocate()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Each lost write sees the other writer's number next time.
	if calls != 4 || !slices.Equal(seen, []uint64{1, 2, 3, 4}) || idx.NextNumber != 5 {
		t.Fatalf("calls %d, next numbers seen %v, final %d", calls, seen, idx.NextNumber)
	}
	if got := mustIndex(t, s, "s1").NextNumber; got != 5 {
		t.Fatalf("stored next number %d, want 5", got)
	}
}

func TestUpdateIndexContention(t *testing.T) {
	ctx := context.Background()
	f := &interfere{Objects: NewMemObjects(), key: "s1/index.json", other: func(*Index) {}}
	f.n.Store(1 << 30)
	s, err := New(Config{Objects: f, MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	var waits []time.Duration
	s.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	_, err = s.UpdateIndex(ctx, "s1", func(*Index) error { return nil })
	if !errors.Is(err, ErrContention) {
		t.Fatalf("err %v, want ErrContention", err)
	}
	if len(waits) != 4 {
		t.Fatalf("waited %d times, want 4", len(waits))
	}
	for i, w := range waits {
		if w <= 0 || w > minBackoff<<i {
			t.Errorf("wait %d: %s, want (0, %s]", i, w, minBackoff<<i)
		}
	}
}

func TestBackoffIsBounded(t *testing.T) {
	for attempt := 1; attempt < 100; attempt++ {
		if d := backoff(attempt); d <= 0 || d > maxBackoff {
			t.Fatalf("attempt %d: backoff %s", attempt, d)
		}
	}
}

func TestUpdateIndexCanceled(t *testing.T) {
	f := &interfere{Objects: NewMemObjects(), key: "s1/index.json", other: func(*Index) {}}
	f.n.Store(1 << 30)
	s, err := New(Config{Objects: f})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.UpdateIndex(ctx, "s1", func(*Index) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want context.Canceled", err)
	}
}

func TestUpdateIndexChangeErrorWritesNothing(t *testing.T) {
	mem := NewMemObjects()
	s := newTestStore(t, mem)
	boom := errors.New("boom")
	_, err := s.UpdateIndex(context.Background(), "s1", func(idx *Index) error {
		idx.allocate()
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err %v, want boom", err)
	}
	if keys := mem.Keys(); len(keys) != 0 {
		t.Fatalf("wrote %v", keys)
	}
}

func TestUpdateIndexRefusesAnInvalidIndex(t *testing.T) {
	mem := NewMemObjects()
	s := newTestStore(t, mem)
	_, err := s.UpdateIndex(context.Background(), "s1", func(idx *Index) error {
		idx.Roots[1] = Root{}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("err %v, want an invalid index", err)
	}
	if keys := mem.Keys(); len(keys) != 0 {
		t.Fatalf("wrote %v", keys)
	}
}

func TestReadIndexRefuses(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, body, want string }{
		{"not JSON", "{", "decode"},
		{"a later version", `{"version":2,"nextNumber":1}`, "version 2"},
		{"no next number", `{"version":1}`, "next number"},
		{"a root not stored", `{"version":1,"nextNumber":5,"roots":{"3":{}}}`, "root 3"},
		{"a number out of range", `{"version":1,"nextNumber":2,"entries":{"4":{"bundle":"` + strings.Repeat("a", 64) + `","ordinal":1}}}`, "out of range"},
		{"a dep not stored", `{"version":1,"nextNumber":5,"entries":{"1":{"bundle":"` + strings.Repeat("a", 64) + `","ordinal":1,"deps":[2]}}}`, "dependency 2"},
		{"a mapping not stored", `{"version":1,"nextNumber":5,"caches":{"c":{"numbers":{"7":{"number":3}}}}}`, "not stored"},
		{"a null cache", `{"version":1,"nextNumber":5,"caches":{"c":null}}`, "null"},
		{"a mapping to another source", `{"version":1,"nextNumber":5,"entries":{"1":{"bundle":"` + strings.Repeat("a", 64) + `","ordinal":1,"source":{"cacheID":"a","number":3}}},"caches":{"a":{"numbers":{"3":{"number":1}}},"b":{"numbers":{"7":{"number":1}}}}}`, "came from cache"},
		{"an entry without its mapping", `{"version":1,"nextNumber":5,"entries":{"1":{"bundle":"` + strings.Repeat("a", 64) + `","ordinal":1,"source":{"cacheID":"a","number":3}}}}`, "doesn't map"},
		{"an entry without a source", `{"version":1,"nextNumber":5,"entries":{"1":{"bundle":"` + strings.Repeat("a", 64) + `","ordinal":1}}}`, "no source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := NewMemObjects()
			if _, err := mem.Put(ctx, "team/s1/index.json", []byte(tc.body), Condition{}); err != nil {
				t.Fatal(err)
			}
			s := newTestStore(t, mem)
			if _, err := s.Index(ctx, "s1"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want one containing %q", err, tc.want)
			}
			// And no update overwrites it.
			if _, err := s.UpdateIndex(ctx, "s1", func(*Index) error { return nil }); err == nil {
				t.Fatal("update over a bad index: no error")
			}
		})
	}
}

func TestDropExpired(t *testing.T) {
	idx := NewIndex()
	const now = 1000
	// 1 <- 2 <- 3 (root, never expires): kept.
	// 4 (expired) <- 5 (root): both dropped.
	// 6 (root, expired root): dropped.
	// 7 <- 8 (root, expires later): kept. 9: no root depends on it: dropped.
	b := testBundle([]val{
		{sender: 1},
		{sender: 2, deps: []uint64{1}},
		{sender: 3, deps: []uint64{2}},
		{sender: 4, expires: now},
		{sender: 5, deps: []uint64{4}},
		{sender: 6},
		{sender: 7},
		{sender: 8, deps: []uint64{7}, expires: now + 1},
		{sender: 9},
	}, root(3, 0), root(5, 0), root(6, now-1), root(8, now+5))
	if _, err := idx.Record(strings.Repeat("a", 64), b, "c", 1); err != nil {
		t.Fatal(err)
	}
	// Record's numbers follow the ordinals here.
	dropped := idx.DropExpired(now)
	if !slices.Equal(dropped, []uint64{4, 5, 6, 9}) {
		t.Fatalf("dropped %v, want [4 5 6 9]", dropped)
	}
	var kept []uint64
	for n := range idx.Entries {
		kept = append(kept, n)
	}
	slices.Sort(kept)
	if !slices.Equal(kept, []uint64{1, 2, 3, 7, 8}) {
		t.Fatalf("kept %v", kept)
	}
	if len(idx.Roots) != 2 {
		t.Fatalf("roots %v, want 3 and 8", idx.Roots)
	}
	if len(idx.Caches["c"].Numbers) != 5 {
		t.Fatalf("mappings %v", idx.Caches["c"].Numbers)
	}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
	if idx.NextNumber != 10 {
		t.Fatalf("next number %d: dropping must not free numbers", idx.NextNumber)
	}
	// A dropped engine entry, exported again, gets a new number.
	r, err := idx.Record(strings.Repeat("b", 64), testBundle([]val{{sender: 6}}, root(1, 0)), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if r[1] != 10 {
		t.Fatalf("re-exported dropped entry: number %d, want 10", r[1])
	}
}

func TestVerifyConditionalWrites(t *testing.T) {
	ctx := context.Background()
	mem := NewMemObjects()
	s := newTestStore(t, mem)
	if err := s.VerifyConditionalWrites(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if keys := mem.Keys(); len(keys) != 0 {
		t.Fatalf("the check left %v behind", keys)
	}
	bad := ignoresConditions{NewMemObjects()}
	s = newTestStore(t, bad)
	if err := s.VerifyConditionalWrites(ctx, "s1"); err == nil {
		t.Fatal("a store that ignores conditions passed")
	}
	if keys := bad.Keys(); len(keys) != 0 {
		t.Fatalf("the failed check left %v behind", keys)
	}
}

// ignoresConditions is a store without conditional writes.
type ignoresConditions struct{ *MemObjects }

func (o ignoresConditions) Put(ctx context.Context, key string, body []byte, _ Condition) (string, error) {
	return o.MemObjects.Put(ctx, key, body, Condition{})
}

func TestMemObjects(t *testing.T) {
	ctx := context.Background()
	m := NewMemObjects()
	if _, _, err := m.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := m.Put(ctx, "k", []byte("a"), Condition{IfMatch: `"1"`}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("If-Match on a missing key: %v", err)
	}
	e1, err := m.Put(ctx, "k", []byte("a"), Condition{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	body, etag, err := m.Get(ctx, "k")
	if err != nil || string(body) != "a" || etag != e1 {
		t.Fatalf("get: %q %s %v", body, etag, err)
	}
	body[0] = 'z' // a copy
	if _, err := m.Put(ctx, "k", []byte("b"), Condition{IfNoneMatch: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("If-None-Match on an existing key: %v", err)
	}
	e2, err := m.Put(ctx, "k", []byte("b"), Condition{IfMatch: e1})
	if err != nil || e2 == e1 {
		t.Fatalf("If-Match: %s %v", e2, err)
	}
	if _, err := m.Put(ctx, "k", []byte("c"), Condition{IfMatch: e1}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale If-Match: %v", err)
	}
	if body, _, _ := m.Get(ctx, "k"); string(body) != "b" {
		t.Fatalf("body %q", body)
	}
}
