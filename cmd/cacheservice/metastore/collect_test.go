package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/snapshots"
)

// testClock is a settable clock.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newClockedStore(t *testing.T, objects Objects) (*Store, *testClock) {
	t.Helper()
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	s, err := New(Config{Objects: objects, Prefix: "team", Now: clock.now})
	if err != nil {
		t.Fatal(err)
	}
	return s, clock
}

// An export stamps the roots it records with the time, and a later export
// refreshes that time only when it writes the root's record: a newer state
// of the value. A late export from an older generation doesn't, nor does
// a cache exporting back unchanged a value merged into it: merges never
// keep stored entries alive by themselves.
func TestRecordStampsRoots(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	t0 := clock.now().Unix()

	rec, err := s.Record(ctx, "s1", testBundle([]val{{sender: 5}}, root(1, 0)), "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	n := rec.Numbers[1]
	if got := rec.Index.Roots[n].RecordedAtUnix; got != t0 {
		t.Fatalf("new root recorded at %d, want %d", got, t0)
	}

	// An hour later, a late export from generation 1: not recorded, so not
	// refreshed.
	clock.add(time.Hour)
	rec, err = s.Record(ctx, "s1", testBundle([]val{{sender: 5, replacements: 9}}, root(1, 0)), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Index.Roots[n].RecordedAtUnix; got != t0 {
		t.Fatalf("a late older-generation export refreshed the root: %d, want %d", got, t0)
	}

	// Another cache, which had it merged, exports it back unchanged: no
	// new record, so no refresh.
	if _, err := s.RecordImports(ctx, "s1", "b", 1, []Import{{Number: n, Engine: 70}}); err != nil {
		t.Fatal(err)
	}
	rec, err = s.Record(ctx, "s1", testBundle([]val{{sender: 70}}, root(1, 0)), "b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Numbers[1] != n {
		t.Fatalf("the import was stored again as %d", rec.Numbers[1])
	}
	if got := rec.Index.Roots[n].RecordedAtUnix; got != t0 {
		t.Fatalf("an unchanged import exported back refreshed the root: %d, want %d", got, t0)
	}

	// The same cache's newer state of the value: recorded, refreshed.
	rec, err = s.Record(ctx, "s1", testBundle([]val{{sender: 5, replacements: 1}}, root(1, 0)), "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Index.Roots[n].RecordedAtUnix; got != clock.now().Unix() {
		t.Fatalf("a newer record didn't refresh the root: %d, want %d", got, clock.now().Unix())
	}

	// Index.Record stamps the caller's time only, and the cache's record
	// with it too: no wall clock (#111). It refuses no time at all.
	idx := NewIndex()
	nums, err := idx.Record(rec.BundleID, testBundle([]val{{sender: 5}}, root(1, 0)), "c", 1, testNowUnix)
	if err != nil {
		t.Fatal(err)
	}
	if got := idx.Roots[nums[1]].RecordedAtUnix; got != testNowUnix {
		t.Fatalf("Index.Record stamped %d, want %d", got, testNowUnix)
	}
	if got := idx.Caches["c"].UpdatedAtUnix; got != testNowUnix {
		t.Fatalf("Index.Record stamped the cache's record %d, want %d", got, testNowUnix)
	}
	before := jsonMarshal(t, idx)
	for _, now := range []int64{0, -1} {
		if _, err := idx.Record(rec.BundleID, testBundle([]val{{sender: 6}}, root(1, 0)), "c", 1, now); err == nil {
			t.Fatalf("recorded at %d", now)
		}
		if got := idx.RecordImportsAt("d", 1, []Import{{Number: nums[1], Engine: 7}}, now); len(got) != 0 {
			t.Fatalf("imports recorded at %d: %v", now, got)
		}
	}
	if !bytes.Equal(jsonMarshal(t, idx), before) {
		t.Fatal("a refused record changed the index")
	}
}

// A root expires its max age after it was recorded, with the entries only
// it keeps; entries a younger root keeps stay, whenever they were
// recorded. A root with no recording time, and every root without a max
// age, keeps its own expiry.
func TestExpireMaxAge(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	old, err := s.Record(ctx, "s1", chain(10), "c", 1) // 10 <- 11 <- 12
	if err != nil {
		t.Fatal(err)
	}
	clock.add(2 * time.Hour)
	// A newer root on 11.
	young, err := s.Record(ctx, "s1", testBundle([]val{{sender: 10}, {sender: 11, deps: []uint64{1}}, {sender: 20, deps: []uint64{2}}}, root(3, 0)), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	idx := young.Index
	now := clock.now().Unix()

	if d := idx.Clone().Expire(now, 0); len(d) != 0 {
		t.Fatalf("no max age dropped %v", d)
	}
	if d := idx.Clone().DropExpired(now); len(d) != 0 {
		t.Fatalf("DropExpired dropped %v", d)
	}
	oldRoot, youngRoot := old.Numbers[3], young.Numbers[3]
	if got, want := idx.RootExpiry(oldRoot, 90*time.Minute), idx.Roots[oldRoot].RecordedAtUnix+90*60; got != want {
		t.Fatalf("RootExpiry %d, want %d", got, want)
	}
	x := idx.Clone()
	dropped := x.Expire(now, 90*time.Minute)
	if !slices.Equal(dropped, []uint64{oldRoot}) {
		t.Fatalf("dropped %v, want only the old root %d (11 and 10 are kept by the young root)", dropped, oldRoot)
	}
	if _, ok := x.Roots[youngRoot]; !ok || len(x.Roots) != 1 {
		t.Fatalf("roots %v", x.Roots)
	}
	if len(x.Entries) != 3 {
		t.Fatalf("entries %d, want 3", len(x.Entries))
	}

	// Without a recording time, the max age doesn't apply.
	x = idx.Clone()
	r := x.Roots[oldRoot]
	r.RecordedAtUnix = 0
	x.Roots[oldRoot] = r
	if d := x.Expire(now+1_000_000, 90*time.Minute); slices.Contains(d, oldRoot) {
		t.Fatal("a root with no recording time expired by age")
	}
	if n := x.StampRoots(now); n != 1 || x.Roots[oldRoot].RecordedAtUnix != now {
		t.Fatalf("StampRoots stamped %d", n)
	}

	// ApplyMaxAge caps root expiries in a copy: the earlier of the root's
	// own expiry and its max age.
	x = idx.Clone()
	r = x.Roots[youngRoot]
	r.ExpiresAtUnix = now + 60
	x.Roots[youngRoot] = r
	x.ApplyMaxAge(time.Hour)
	if got := x.Roots[youngRoot].ExpiresAtUnix; got != now+60 {
		t.Fatalf("an earlier own expiry was replaced: %d", got)
	}
	if got, want := x.Roots[oldRoot].ExpiresAtUnix, idx.Roots[oldRoot].RecordedAtUnix+3600; got != want {
		t.Fatalf("capped expiry %d, want %d", got, want)
	}
	if idx.Roots[oldRoot].ExpiresAtUnix != 0 {
		t.Fatal("ApplyMaxAge changed the original")
	}
}

func layered(sender uint64, layers ...digest.Digest) *dagql.ValueBundle {
	b := testBundle([]val{{sender: sender}}, root(1, 0))
	chain := &dagql.OfferedChain{}
	for _, d := range layers {
		chain.Layers = append(chain.Layers, snapshots.ExportLayer{Descriptor: ocispecs.Descriptor{Digest: d, Size: 3}})
	}
	b.Outputs = []dagql.TransferredOutput{{Ordinal: 1, Address: dagql.PersistedPartAddress{Part: "fs"}, Chain: chain}}
	return b
}

// BundleBlobs names the layers of outputs and of pending offers, once each.
func TestBundleBlobs(t *testing.T) {
	a, b, c := digest.FromString("a"), digest.FromString("b"), digest.FromString("c")
	bundle := layered(5, a, b, a)
	bundle.Values[0].Record.Envelope.PendingOffers = []dagql.PersistedPartOffer{{Chain: dagql.OfferedChain{Layers: []snapshots.ExportLayer{{Descriptor: ocispecs.Descriptor{Digest: c}}}}}}
	want := []digest.Digest{a, b, c}
	slices.Sort(want)
	if got := BundleBlobs(bundle); !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// claim marks keys as GC does: being deleted (epoch 0), or deleted in a
// round.
func claim(t *testing.T, s *Store, scope string, epoch uint64, keys ...string) {
	t.Helper()
	if _, err := s.UpdateIndex(context.Background(), scope, func(idx *Index) error {
		if idx.GC == nil {
			idx.GC = &Collection{Deletions: map[string]Deletion{}}
		}
		for _, k := range keys {
			d := Deletion{Run: "r", StartedUnix: 1}
			if epoch > 0 {
				d.Epoch, d.DoneUnix = epoch, 2
			}
			idx.GC.Deletions[k] = d
		}
		idx.GC.Epoch = max(idx.GC.Epoch, epoch)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// An export may not name an object GC is deleting, nor one it deleted after
// the export checked it (a later epoch than its fence). Objects deleted
// before, and a fence of a later epoch, are fine: the export's checks came
// after the deletion. Record (no fence) refuses only objects being
// deleted. Nothing is written when it refuses.
func TestRecordFenced(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	blob := digest.FromString("layer")
	bundle := layered(5, blob)
	id, _, err := BundleID(bundle)
	if err != nil {
		t.Fatal(err)
	}
	fence := func(epoch uint64) Fence { return Fence{Epoch: epoch, Started: clock.now()} }
	isCollected := func(err error) bool { return errors.Is(err, ErrCollected) }

	claim(t, s, "s1", 0, BlobRelKey(blob))
	if _, err := s.RecordFenced(ctx, "s1", bundle, "c", 1, fence(5)); !isCollected(err) {
		t.Fatalf("a blob being deleted: %v", err)
	}
	if _, err := s.Record(ctx, "s1", bundle, "c", 1); !isCollected(err) {
		t.Fatalf("Record, a blob being deleted: %v", err)
	}
	if len(mustIndex(t, s, "s1").Entries) != 0 {
		t.Fatal("a refused export was recorded")
	}

	claim(t, s, "s1", 3, BlobRelKey(blob))
	if _, err := s.RecordFenced(ctx, "s1", bundle, "c", 1, fence(2)); !isCollected(err) {
		t.Fatalf("a blob deleted (round 3) after the fence (round 2): %v", err)
	}
	claim(t, s, "s1", 4, BundleRelKey(id))
	if _, err := s.RecordFenced(ctx, "s1", bundle, "c", 1, fence(3)); !isCollected(err) {
		t.Fatalf("a bundle deleted (round 4) after the fence (round 3): %v", err)
	}
	if _, err := s.RecordFenced(ctx, "s1", bundle, "c", 1, Fence{Epoch: 4, Started: clock.now().Add(-MaxFenceAge - time.Second)}); !isCollected(err) {
		t.Fatalf("a fence over MaxFenceAge: %v", err)
	}
	if _, err := s.RecordFenced(ctx, "s1", bundle, "c", 1, fence(4)); err != nil {
		t.Fatalf("everything deleted before the fence: %v", err)
	}
	// Record has no fence: deletions that finished don't refuse it.
	if _, err := s.Record(ctx, "s1", layered(6, blob), "c", 1); err != nil {
		t.Fatal(err)
	}
}

// The epoch object: 0 before GC published one.
func TestEpochObject(t *testing.T) {
	ctx := context.Background()
	objects := NewMemObjects()
	s := newTestStore(t, objects)
	if e, err := s.ReadEpoch(ctx, "s1"); err != nil || e != 0 {
		t.Fatalf("epoch %d, %v", e, err)
	}
	if err := s.WriteEpoch(ctx, "s1", 7); err != nil {
		t.Fatal(err)
	}
	if e, err := s.ReadEpoch(ctx, "s1"); err != nil || e != 7 {
		t.Fatalf("epoch %d, %v", e, err)
	}
	if _, _, err := objects.Get(ctx, "team/ns/s1/gc-epoch.json"); err != nil {
		t.Fatal(err)
	}
}

// The index refuses deletions of keys GC never claims, and of a later epoch
// than its own.
func TestValidateCollection(t *testing.T) {
	idx := NewIndex()
	idx.GC = &Collection{Epoch: 1, Deletions: map[string]Deletion{"index.json": {}}}
	if err := idx.Validate(); err == nil {
		t.Fatal("a deletion of the index: no error")
	}
	idx.GC.Deletions = map[string]Deletion{BlobRelKey(digest.FromString("x")): {Epoch: 2}}
	if err := idx.Validate(); err == nil {
		t.Fatal("a deletion of a later epoch: no error")
	}
	idx.GC.Deletions = map[string]Deletion{BlobRelKey(digest.FromString("x")): {Epoch: 1}}
	if err := idx.Validate(); err != nil {
		t.Fatal(err)
	}
}

// A version 2 index reads: its roots have no recording time.
func TestIndexVersion2Reads(t *testing.T) {
	ctx := context.Background()
	objects := NewMemObjects()
	s := newTestStore(t, objects)
	if _, err := s.Record(ctx, "s1", chain(10), "c", 1); err != nil {
		t.Fatal(err)
	}
	body, _, _ := objects.Get(ctx, "team/ns/s1/index.json")
	var raw map[string]any
	if err := jsonUnmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	raw["version"] = 2
	for _, r := range raw["roots"].(map[string]any) {
		delete(r.(map[string]any), "recordedAtUnix")
	}
	if _, err := objects.Put(ctx, "team/ns/s1/index.json", jsonMarshal(t, raw), Condition{}); err != nil {
		t.Fatal(err)
	}
	idx := mustIndex(t, s, "s1")
	for n, r := range idx.Roots {
		if r.RecordedAtUnix != 0 {
			t.Fatalf("root %d recorded at %d", n, r.RecordedAtUnix)
		}
	}
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func jsonMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The published epoch only goes up: a replica that publishes late doesn't
// lower it.
func TestWriteEpochMonotonic(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	for _, e := range []uint64{2, 3, 1, 3, 2} {
		if err := s.WriteEpoch(ctx, "s1", e); err != nil {
			t.Fatal(err)
		}
	}
	if e, _ := s.ReadEpoch(ctx, "s1"); e != 3 {
		t.Fatalf("epoch %d, want 3", e)
	}
}

// A stale published epoch is safe once a deletion's marker is pruned: the
// deletion finished more than DeletionRetention ago, and an export that may
// still record started less than MaxFenceAge ago, so its checks came after
// the deletion and saw the object missing. Recording with the stale epoch is
// allowed, which is right: the export uploaded (or found re-uploaded) what
// it names.
func TestStaleEpochAfterPruneIsSafe(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	blob := digest.FromString("layer")
	claim(t, s, "s1", 5, BlobRelKey(blob))
	if _, err := s.RecordFenced(ctx, "s1", layered(5, blob), "c", 1, Fence{Epoch: 1, Started: clock.now()}); !errors.Is(err, ErrCollected) {
		t.Fatalf("with the marker: %v", err)
	}
	// The marker is pruned (GC does so after DeletionRetention); an export
	// started since, fencing at the stale epoch 1, records.
	if _, err := s.UpdateIndex(ctx, "s1", func(idx *Index) error {
		idx.GC.Deletions = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.add(DeletionRetention)
	if _, err := s.RecordFenced(ctx, "s1", layered(5, blob), "c", 1, Fence{Epoch: 1, Started: clock.now()}); err != nil {
		t.Fatal(err)
	}
	// And one that started before the deletion's retention ran out is
	// refused by its age instead.
	if _, err := s.RecordFenced(ctx, "s1", layered(6, blob), "c", 1, Fence{Epoch: 1, Started: clock.now().Add(-MaxFenceAge - time.Minute)}); !errors.Is(err, ErrCollected) {
		t.Fatalf("an export older than MaxFenceAge: %v", err)
	}
}

// A root is added only with its record: an export that doesn't write the
// record (an import exported back unchanged, or a late export from an older
// generation) adds no root. Otherwise a value whose root the max age
// expired, still stored as another root's dependency, would come back as a
// root with a fresh recording time, kept alive by merges alone.
func TestRootAddedOnlyWithItsRecord(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	// X (sender 10) as a root, then Y (20) on it.
	x, err := s.Record(ctx, "s1", testBundle([]val{{sender: 10}}, root(1, 0)), "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	clock.add(time.Hour)
	if _, err := s.Record(ctx, "s1", testBundle([]val{{sender: 10}, {sender: 20, deps: []uint64{1}}}, root(2, 0)), "c", 2); err != nil {
		t.Fatal(err)
	}
	nx := x.Numbers[1]
	// X's root expires by its max age; X stays, as Y's dependency.
	clock.add(time.Hour)
	if _, err := s.UpdateIndex(ctx, "s1", func(idx *Index) error {
		idx.Expire(clock.now().Unix(), 90*time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	idx := mustIndex(t, s, "s1")
	if _, ok := idx.Roots[nx]; ok || idx.Entries[nx] == nil {
		t.Fatalf("setup: root %v, entry %v", idx.Roots[nx], idx.Entries[nx])
	}
	// Cache b had X merged, and exports it back unchanged, as a root.
	if _, err := s.RecordImports(ctx, "s1", "b", 1, []Import{{Number: nx, Engine: 70}}); err != nil {
		t.Fatal(err)
	}
	rec, err := s.Record(ctx, "s1", testBundle([]val{{sender: 70}}, root(1, 0)), "b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.Index.Roots[nx]; ok {
		t.Fatal("an import exported back unchanged made X a root again")
	}
	// A late export of X from c's generation 1: not written, no root.
	rec, err = s.Record(ctx, "s1", testBundle([]val{{sender: 10, replacements: 9}}, root(1, 0)), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.Index.Roots[nx]; ok {
		t.Fatal("a late older-generation export made X a root again")
	}
	// A newer state of X from c: written, a root again.
	rec, err = s.Record(ctx, "s1", testBundle([]val{{sender: 10, replacements: 1}}, root(1, 0)), "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	if r, ok := rec.Index.Roots[nx]; !ok || r.RecordedAtUnix != clock.now().Unix() {
		t.Fatalf("a newer record: root %+v, %v", r, ok)
	}
}

// A root from before version 3 (no recording time) gets one only from an
// export that writes its record, or from GC (StampRoots): an import
// exported back unchanged doesn't stamp it either.
func TestUnstampedRootStampedOnlyWithItsRecord(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	x, err := s.Record(ctx, "s1", testBundle([]val{{sender: 10}}, root(1, 0)), "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	nx := x.Numbers[1]
	if _, err := s.UpdateIndex(ctx, "s1", func(idx *Index) error {
		r := idx.Roots[nx]
		r.RecordedAtUnix = 0 // as in a version 2 index
		idx.Roots[nx] = r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.add(time.Hour)
	if _, err := s.RecordImports(ctx, "s1", "b", 1, []Import{{Number: nx, Engine: 70}}); err != nil {
		t.Fatal(err)
	}
	rec, err := s.Record(ctx, "s1", testBundle([]val{{sender: 70}}, root(1, 0)), "b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Index.Roots[nx].RecordedAtUnix; got != 0 {
		t.Fatalf("an unchanged import exported back stamped the root: %d", got)
	}
	rec, err = s.Record(ctx, "s1", testBundle([]val{{sender: 10, replacements: 1}}, root(1, 0)), "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Index.Roots[nx].RecordedAtUnix; got != clock.now().Unix() {
		t.Fatalf("a newer record: %d", got)
	}
}

// A max age with a fraction of a second counts as the next whole second:
// it never cuts a root short of its max age.
func TestCapExpiryRoundsUp(t *testing.T) {
	r := Root{RecordedAtUnix: 100}
	for age, want := range map[time.Duration]int64{
		90 * time.Second:        190,
		90*time.Second + 900e6:  191,
		1500 * time.Millisecond: 102,
		time.Second:             101,
	} {
		if got := CapExpiry(r, age); got != want {
			t.Errorf("max age %s: expiry %d, want %d", age, got, want)
		}
	}
}

// expireAt runs GC's expiry over a scope at the clock's time, under maxAge.
func expireAt(t *testing.T, s *Store, clock *testClock, scope string, maxAge time.Duration) {
	t.Helper()
	if _, err := s.UpdateIndex(context.Background(), scope, func(idx *Index) error {
		idx.Expire(clock.now().Unix(), maxAge)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// indexJSON returns a scope's index, encoded, to compare it whole.
func indexJSON(t *testing.T, s *Store, scope string) string {
	t.Helper()
	body, err := json.Marshal(mustIndex(t, s, scope))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// collectedX records X (engine entry 10 of cache c) at generation 2, and
// lets GC collect it under a 90 minute max age: its root, entry and mapping
// go, and the cache's record stays, with its generation.
func collectedX(t *testing.T) (*Store, *testClock) {
	t.Helper()
	s, clock := newClockedStore(t, NewMemObjects())
	if _, err := s.Record(context.Background(), "s1", testBundle([]val{{sender: 10}}, root(1, 0)), "c", 2); err != nil {
		t.Fatal(err)
	}
	clock.add(2 * time.Hour)
	expireAt(t, s, clock, "s1", 90*time.Minute)
	idx := mustIndex(t, s, "s1")
	if len(idx.Entries) != 0 || len(idx.Roots) != 0 {
		t.Fatalf("setup: X not collected: entries %v, roots %v", idx.Entries, idx.Roots)
	}
	if c := idx.Caches["c"]; c == nil || c.Generation != 2 || len(c.Numbers) != 0 {
		t.Fatalf("setup: cache record %+v, want generation 2 with no mappings", c)
	}
	return s, clock
}

// zerobull-consulting/dagger#59: once GC has collected a root, its entry and
// its cache mapping, a late export of the same engine entry from an older
// generation of that cache adds nothing: no number, no entry, no root. The
// cache's record, which outlives its mappings, still knows the later
// generation. The export is refused with ErrStaleGeneration, which the
// exporter doesn't retry.
func TestLateOlderGenerationDoesNotReviveCollectedRoot(t *testing.T) {
	ctx := context.Background()
	s, _ := collectedX(t)
	before := indexJSON(t, s, "s1")
	_, err := s.Record(ctx, "s1", testBundle([]val{{sender: 10}}, root(1, 0)), "c", 1)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("late gen-1 export: %v, want ErrStaleGeneration", err)
	}
	if after := indexJSON(t, s, "s1"); after != before {
		t.Fatalf("the refused export changed the index:\nbefore %s\nafter  %s", before, after)
	}
}

// The cache's own generation, and a newer one, still record a collected
// value: the engine really has it.
func TestSameAndNewerGenerationRecordAfterCollection(t *testing.T) {
	for _, gen := range []uint64{2, 3} {
		s, clock := collectedX(t)
		rec, err := s.Record(context.Background(), "s1", testBundle([]val{{sender: 10}}, root(1, 0)), "c", gen)
		if err != nil {
			t.Fatalf("generation %d: %v", gen, err)
		}
		n := rec.Numbers[1]
		r, ok := rec.Index.Roots[n]
		if !ok || r.RecordedAtUnix != clock.now().Unix() || rec.Index.Entries[n] == nil {
			t.Fatalf("generation %d: root %+v (%v), entry %+v", gen, r, ok, rec.Index.Entries[n])
		}
		if c := rec.Index.Caches["c"]; c.Generation != gen || c.Numbers[10].Number != n {
			t.Fatalf("generation %d: cache record %+v", gen, c)
		}
	}
}

// Before GC: a late export from an older generation of values the index
// holds is recorded as before (no newer record replaced, no root added or
// refreshed); one that names any value the index doesn't hold, alone or with
// values it does, is refused whole.
func TestLateOlderGenerationBeforeCollection(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	// X (10) is a root at generation 2; Y (11), its dependency, isn't.
	x, err := s.Record(ctx, "s1", testBundle([]val{{sender: 11, replacements: 1}, {sender: 10, deps: []uint64{1}, replacements: 1}}, root(2, 0)), "c", 2)
	if err != nil {
		t.Fatal(err)
	}
	recordedAt := clock.now().Unix()
	clock.add(time.Hour)

	// Y alone, as a root, from generation 1: held, so recorded, but no
	// root added, and no record replaced.
	rec, err := s.Record(ctx, "s1", testBundle([]val{{sender: 11}}, root(1, 0)), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.Index.Roots[x.Numbers[1]]; ok || len(rec.Index.Roots) != 1 {
		t.Fatalf("roots %v: the late export added one", rec.Index.Roots)
	}
	if e := rec.Index.Entries[x.Numbers[1]]; e.Source.Generation != 2 || e.Bundle != x.BundleID {
		t.Fatalf("Y's entry %+v: replaced by the older generation's", e)
	}
	// X from generation 1: its root keeps its time.
	rec, err = s.Record(ctx, "s1", testBundle([]val{{sender: 11}, {sender: 10, deps: []uint64{1}}}, root(2, 0)), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if r := rec.Index.Roots[x.Numbers[2]]; r.RecordedAtUnix != recordedAt {
		t.Fatalf("X's root %+v, want recorded at %d", r, recordedAt)
	}

	// Z (12), which generation 2 never exported, from generation 1: alone,
	// or with X as its dependency, refused whole.
	before := indexJSON(t, s, "s1")
	for name, b := range map[string]*dagql.ValueBundle{
		"alone":  testBundle([]val{{sender: 12}}, root(1, 0)),
		"with X": testBundle([]val{{sender: 11}, {sender: 10, deps: []uint64{1}}, {sender: 12, deps: []uint64{2}}}, root(3, 0), root(2, 0)),
	} {
		if _, err := s.Record(ctx, "s1", b, "c", 1); !errors.Is(err, ErrStaleGeneration) {
			t.Fatalf("%s: %v, want ErrStaleGeneration", name, err)
		}
		if after := indexJSON(t, s, "s1"); after != before {
			t.Fatalf("%s: the refused export changed the index:\nbefore %s\nafter  %s", name, before, after)
		}
	}
}

// A stale generation's export of a value the index still holds, as another
// root's dependency, whose own root expired, adds no root, even when it
// writes the value's record: through an exported mapping whose stored
// record is older still, or by taking over an import with more parts.
// (Copilot on zerobull-consulting/dagger#109, #59.)
func TestLateOlderGenerationAddsNoRootToHeldValue(t *testing.T) {
	bid := func(c string) string { return strings.Repeat(c, 64) }
	t.Run("exported", func(t *testing.T) {
		idx := NewIndex()
		// Gen 1: X (10), a root until 100, and Y (11), a root over it.
		nums, err := idx.Record(bid("a"), testBundle([]val{{sender: 10}, {sender: 11, deps: []uint64{1}}}, root(1, 100), root(2, 0)), "c", 1, testNowUnix)
		if err != nil {
			t.Fatal(err)
		}
		// Gen 3 records something else; X's root expires, X stays as Y's
		// dependency.
		if _, err := idx.Record(bid("b"), testBundle([]val{{sender: 12}}, root(1, 0)), "c", 3, testNowUnix); err != nil {
			t.Fatal(err)
		}
		idx.DropExpired(200)
		x := nums[1]
		if _, ok := idx.Roots[x]; ok || idx.Entries[x] == nil {
			t.Fatalf("setup: X root %v, entry %+v", idx.Roots, idx.Entries[x])
		}
		// Gen 2, older than 3 but newer than X's record: the record is
		// written, as before; no root.
		if _, err := idx.Record(bid("d"), testBundle([]val{{sender: 10}}, root(1, 0)), "c", 2, testNowUnix); err != nil {
			t.Fatal(err)
		}
		if e := idx.Entries[x]; e.Source.Generation != 2 {
			t.Fatalf("X's entry %+v: want gen 2's record", e)
		}
		if r, ok := idx.Roots[x]; ok {
			t.Fatalf("X got a root back: %+v", r)
		}
	})
	t.Run("imported", func(t *testing.T) {
		idx := NewIndex()
		// Cache s: X (10, an fs part), a root until 100, and Y over it.
		b := testBundle([]val{{sender: 10}, {sender: 11, deps: []uint64{1}}}, root(1, 100), root(2, 0))
		withParts(b, 1, "fs")
		nums, err := idx.Record(bid("a"), b, "s", 1, testNowUnix)
		if err != nil {
			t.Fatal(err)
		}
		x := nums[1]
		// Cache c imports X on its entry 50 at gen 2; X's root expires.
		if got := idx.RecordImportsAt("c", 2, []Import{{Number: x, Engine: 50}}, testNowUnix); len(got) != 1 {
			t.Fatalf("imports %v", got)
		}
		idx.DropExpired(200)
		if _, ok := idx.Roots[x]; ok || idx.Caches["c"].Imported[50].Number != x {
			t.Fatalf("setup: roots %v, cache c %+v", idx.Roots, idx.Caches["c"])
		}
		// c's gen 1, with more parts: it takes the entry over, as before,
		// and adds no root.
		if _, err := idx.Record(bid("d"), withParts(testBundle([]val{{sender: 50}}, root(1, 0)), 1, "fs", "execMeta"), "c", 1, testNowUnix); err != nil {
			t.Fatal(err)
		}
		if e := idx.Entries[x]; e.Source.CacheID != "c" {
			t.Fatalf("X's entry %+v: want c's record", e)
		}
		if r, ok := idx.Roots[x]; ok {
			t.Fatalf("X got a root back: %+v", r)
		}
		if err := idx.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

// The cache's record outlives its mappings, and a wipe's ForgetImports: it
// keeps refusing the stale generation until CacheRecordRetention after its
// last change, when Expire drops it. That is the bound on what the index
// keeps to remember generations; a late export after it is recorded as new.
func TestCacheRecordOutlivesItsMappings(t *testing.T) {
	ctx := context.Background()
	s, clock := collectedX(t)
	// The cache was wiped, after importing something.
	other, err := s.Record(ctx, "s1", testBundle([]val{{sender: 50}}, root(1, 0)), "other", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordImports(ctx, "s1", "c", 2, []Import{{Number: other.Numbers[1], Engine: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ForgetImports(ctx, "s1", "c"); err != nil {
		t.Fatal(err)
	}
	forgotAt := clock.now().Unix()
	c := mustIndex(t, s, "s1").Caches["c"]
	if c == nil || len(c.Imported) != 0 || len(c.Numbers) != 0 || c.Generation != 2 || c.UpdatedAtUnix != forgotAt {
		t.Fatalf("after the wipe, cache record %+v", c)
	}
	stale := testBundle([]val{{sender: 10}}, root(1, 0))
	if _, err := s.Record(ctx, "s1", stale, "c", 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("after the wipe: %v, want ErrStaleGeneration", err)
	}

	// Kept until CacheRecordRetention after its last change.
	clock.add(CacheRecordRetention - time.Second)
	expireAt(t, s, clock, "s1", 90*time.Minute)
	if mustIndex(t, s, "s1").Caches["c"] == nil {
		t.Fatal("dropped before CacheRecordRetention")
	}
	if _, err := s.Record(ctx, "s1", stale, "c", 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("a second before the retention: %v, want ErrStaleGeneration", err)
	}
	clock.add(time.Second)
	expireAt(t, s, clock, "s1", 90*time.Minute)
	idx := mustIndex(t, s, "s1")
	if idx.Caches["c"] != nil {
		t.Fatalf("kept past CacheRecordRetention: %+v", idx.Caches["c"])
	}
	// The other cache's mapping went only in the previous expiry, which
	// stamped its record then: it stays.
	if o := idx.Caches["other"]; o == nil || len(o.Numbers) != 0 || o.UpdatedAtUnix != clock.now().Unix()-1 {
		t.Fatalf("other's record %+v", o)
	}
	if rec, err := s.Record(ctx, "s1", stale, "c", 1); err != nil || len(rec.Index.Roots) != 1 {
		t.Fatalf("past the retention: %v, roots %v; want it recorded as new", err, rec.Index.Roots)
	}
}

// A cache record is never dropped while it has a mapping, however long ago
// it last changed, and one GC empties is stamped then: the retention counts
// from the last mapping going, not the last export.
func TestCacheRecordRetentionCountsFromLastMapping(t *testing.T) {
	ctx := context.Background()
	s, clock := newClockedStore(t, NewMemObjects())
	// No max age: X lives until its own expiry, past the retention.
	expires := clock.now().Add(2 * CacheRecordRetention).Unix()
	if _, err := s.Record(ctx, "s1", testBundle([]val{{sender: 10}}, root(1, expires)), "c", 2); err != nil {
		t.Fatal(err)
	}
	clock.add(CacheRecordRetention + time.Hour)
	expireAt(t, s, clock, "s1", 0)
	if c := mustIndex(t, s, "s1").Caches["c"]; c == nil || len(c.Numbers) != 1 {
		t.Fatalf("cache record with a live mapping: %+v", c)
	}
	clock.add(CacheRecordRetention)
	expireAt(t, s, clock, "s1", 0)
	c := mustIndex(t, s, "s1").Caches["c"]
	if c == nil || len(c.Numbers) != 0 || c.UpdatedAtUnix != clock.now().Unix() {
		t.Fatalf("cache record GC just emptied: %+v, want it kept and stamped %d", c, clock.now().Unix())
	}
	if _, err := s.Record(ctx, "s1", testBundle([]val{{sender: 10}}, root(1, 0)), "c", 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("late gen-1 export: %v, want ErrStaleGeneration", err)
	}
}

// A version 3 index reads with every cache record stamped with the reader's
// clock, so an empty one is kept a whole CacheRecordRetention, and is
// written back as version 4.
func TestIndexVersion3Reads(t *testing.T) {
	ctx := context.Background()
	objects := NewMemObjects()
	s, clock := newClockedStore(t, objects)
	if _, err := s.Record(ctx, "s1", chain(10), "c", 1); err != nil {
		t.Fatal(err)
	}
	body, _, _ := objects.Get(ctx, "team/ns/s1/index.json")
	var raw map[string]any
	if err := jsonUnmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	raw["version"] = 3
	for _, c := range raw["caches"].(map[string]any) {
		delete(c.(map[string]any), "updatedAtUnix")
	}
	if _, err := objects.Put(ctx, "team/ns/s1/index.json", jsonMarshal(t, raw), Condition{}); err != nil {
		t.Fatal(err)
	}
	clock.add(time.Hour)
	if c := mustIndex(t, s, "s1").Caches["c"]; c.UpdatedAtUnix != clock.now().Unix() {
		t.Fatalf("cache record %+v, want stamped %d", c, clock.now().Unix())
	}
	if _, err := s.UpdateIndex(ctx, "s1", func(*Index) error { return nil }); err != nil {
		t.Fatal(err)
	}
	body, _, _ = objects.Get(ctx, "team/ns/s1/index.json")
	if err := jsonUnmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["version"] != float64(IndexVersion) || raw["caches"].(map[string]any)["c"].(map[string]any)["updatedAtUnix"] == nil {
		t.Fatalf("written back as %s", body)
	}
}

// CheckGeneration, the exporter's early check (#111), refuses exactly what
// RecordResolvedAt refuses as a stale generation's export, and changes
// nothing.
func TestCheckGenerationAgreesWithRecord(t *testing.T) {
	bid := func(c string) string { return strings.Repeat(c, 64) }
	idx := NewIndex()
	if _, err := idx.Record(bid("a"), testBundle([]val{{sender: 10}}, root(1, 0)), "c", 2, testNowUnix); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.Record(bid("b"), testBundle([]val{{sender: 50}}, root(1, 0)), "s", 1, testNowUnix); err != nil {
		t.Fatal(err)
	}
	idx.RecordImportsAt("c", 2, []Import{{Number: idx.Caches["s"].Numbers[50].Number, Engine: 20}}, testNowUnix)
	for _, tc := range []struct {
		name       string
		cache      string
		generation uint64
		senders    []uint64
		stale      bool
	}{
		{"older, mapped", "c", 1, []uint64{10}, false},
		{"older, imported", "c", 1, []uint64{20}, false},
		{"older, one unmapped", "c", 1, []uint64{10, 11}, true},
		{"older, unmapped", "c", 1, []uint64{11}, true},
		{"same generation", "c", 2, []uint64{11}, false},
		{"newer generation", "c", 3, []uint64{11}, false},
		{"unknown cache", "d", 1, []uint64{11}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var vals []val
			for _, s := range tc.senders {
				vals = append(vals, val{sender: s})
			}
			b := testBundle(vals, root(1, 0))
			before := jsonMarshal(t, idx)
			early := idx.CheckGeneration(b, tc.cache, tc.generation)
			if !bytes.Equal(jsonMarshal(t, idx), before) {
				t.Fatal("CheckGeneration changed the index")
			}
			_, _, record := idx.Clone().RecordResolvedAt(bid("e"), b, tc.cache, tc.generation, testNowUnix)
			if got := errors.Is(early, ErrStaleGeneration); got != tc.stale {
				t.Fatalf("CheckGeneration: %v, want stale %v", early, tc.stale)
			}
			if got := errors.Is(record, ErrStaleGeneration); got != tc.stale || (!tc.stale && record != nil) {
				t.Fatalf("RecordResolvedAt: %v, want stale %v", record, tc.stale)
			}
		})
	}
}
