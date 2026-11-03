package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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

	// Index.Record (no clock) stamps nothing.
	idx := NewIndex()
	nums, err := idx.Record(rec.BundleID, testBundle([]val{{sender: 5}}, root(1, 0)), "c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := idx.Roots[nums[1]].RecordedAtUnix; got != 0 {
		t.Fatalf("Index.Record stamped %d", got)
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
	if _, _, err := objects.Get(ctx, "team/s1/gc-epoch.json"); err != nil {
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
	body, _, _ := objects.Get(ctx, "team/s1/index.json")
	var raw map[string]any
	if err := jsonUnmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	raw["version"] = 2
	for _, r := range raw["roots"].(map[string]any) {
		delete(r.(map[string]any), "recordedAtUnix")
	}
	if _, err := objects.Put(ctx, "team/s1/index.json", jsonMarshal(t, raw), Condition{}); err != nil {
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
