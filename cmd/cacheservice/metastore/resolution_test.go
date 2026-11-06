package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dagger/dagger/dagql"
)

// checkResolution checks that rec's resolution is stored, resolves the bundle
// as rec's numbers say, and is what every entry rec wrote names.
func checkResolution(t *testing.T, s *Store, scope string, rec Recorded, cacheID string) *Resolution {
	t.Helper()
	res, err := s.GetResolution(context.Background(), scope, rec.ResolutionID)
	if err != nil {
		t.Fatalf("resolution %s: %v", rec.ResolutionID, err)
	}
	if res.Bundle != rec.BundleID || res.CacheID != cacheID || len(res.Numbers) != len(rec.Numbers) {
		t.Fatalf("resolution %+v, for bundle %s from %q with %d values", res, rec.BundleID, cacheID, len(rec.Numbers))
	}
	for o, n := range rec.Numbers {
		if got, _ := res.Number(o); got != n {
			t.Fatalf("resolution gives ordinal %d %d, want %d", o, got, n)
		}
	}
	for n, e := range rec.Index.Entries {
		if e.Bundle == rec.BundleID && e.Source.CacheID == cacheID && e.Resolution != rec.ResolutionID {
			t.Fatalf("entry %d names resolution %q, want %s", n, e.Resolution, rec.ResolutionID)
		}
	}
	return res
}

// Record stores the bundle's resolution, at its documented key, and names it
// from each record it writes. Recording the same bundle the same way names
// the same resolution; another cache's export of it gets its own.
func TestRecordStoresResolution(t *testing.T) {
	ctx := context.Background()
	objects := NewMemObjects()
	s := newTestStore(t, objects)
	r1, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	checkResolution(t, s, "s1", r1, "cache-a")
	if _, _, err := objects.Get(ctx, "team/ns/s1/resolutions/"+r1.ResolutionID+".json"); err != nil {
		t.Fatalf("resolution not at its key: %v", err)
	}

	r2, err := s.Record(ctx, "s1", chain(10), "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if r2.ResolutionID != r1.ResolutionID {
		t.Fatalf("the same export resolved again: %s, want %s", r2.ResolutionID, r1.ResolutionID)
	}

	r3, err := s.Record(ctx, "s1", chain(10), "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if r3.BundleID != r1.BundleID || r3.ResolutionID == r1.ResolutionID {
		t.Fatalf("another cache's export of bundle %s: resolution %s, want one of its own", r3.BundleID, r3.ResolutionID)
	}
	checkResolution(t, s, "s1", r3, "cache-b")
	// cache-a's records still name its own.
	for _, n := range r1.Numbers {
		if e := r3.Index.Entries[n]; e.Resolution != r1.ResolutionID {
			t.Fatalf("entry %d names %s, want cache-a's %s", n, e.Resolution, r1.ResolutionID)
		}
	}
}

// #47 in the store: the resolution of a bundle holding imports gives the
// stored entries the imports named when it was recorded, and stays so after
// the imports move or are forgotten.
func TestResolutionOfImportsOutlivesThem(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	a, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	// cache-b has A's three entries on 20, 21, 22, and builds 23 on them.
	if _, err := s.RecordImports(ctx, "s1", "cache-b", 1, []Import{{a.Numbers[1], 20}, {a.Numbers[2], 21}, {a.Numbers[3], 22}}); err != nil {
		t.Fatal(err)
	}
	bundle := testBundle([]val{{sender: 20}, {sender: 21, deps: []uint64{1}}, {sender: 22, deps: []uint64{2, 1}}, {sender: 23, deps: []uint64{3}}}, root(4, 0))
	b, err := s.Record(ctx, "s1", bundle, "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	res := checkResolution(t, s, "s1", b, "cache-b")
	want := []uint64{a.Numbers[1], a.Numbers[2], a.Numbers[3], b.Numbers[4]}
	if !slices.Equal(res.Numbers, want) {
		t.Fatalf("resolution %v, want A's numbers then B's: %v", res.Numbers, want)
	}
	if _, err := s.RecordImports(ctx, "s1", "cache-b", 2, []Import{{a.Numbers[1], 30}, {a.Numbers[2], 31}, {a.Numbers[3], 32}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ForgetImports(ctx, "s1", "cache-b"); err != nil {
		t.Fatal(err)
	}
	idx := mustIndex(t, s, "s1")
	if e := idx.Entries[b.Numbers[4]]; e.Resolution != b.ResolutionID {
		t.Fatalf("after the imports went, entry %d names %q, want %s", b.Numbers[4], e.Resolution, b.ResolutionID)
	}
	again, err := s.GetResolution(ctx, "s1", b.ResolutionID)
	if err != nil || !slices.Equal(again.Numbers, want) {
		t.Fatalf("after the imports went: %+v, %v", again, err)
	}
}

// orderedObjects checks, at each index write, that every resolution the
// index names is already stored: no reader can see one that isn't. It logs
// the kind of each write that lands.
type orderedObjects struct {
	Objects
	t   *testing.T
	mu  sync.Mutex
	log []string
}

func (o *orderedObjects) Put(ctx context.Context, key string, body []byte, cond Condition) (string, error) {
	kind := "other"
	switch {
	case strings.HasSuffix(key, "/index.json"):
		kind = "index"
		idx, err := decodeIndex(body, 1000)
		if err != nil {
			o.t.Errorf("index write: %v", err)
		}
		for n, e := range idx.Entries {
			if e.Resolution == "" {
				o.t.Errorf("index write: entry %d names no resolution", n)
				continue
			}
			if _, _, err := o.Objects.Get(ctx, strings.TrimSuffix(key, "index.json")+"resolutions/"+e.Resolution+".json"); err != nil {
				o.t.Errorf("index write names entry %d's resolution %s before it is stored: %v", n, e.Resolution, err)
			}
		}
	case strings.Contains(key, "/resolutions/"):
		kind = "resolution"
	case strings.Contains(key, "/bundles/"):
		kind = "bundle"
	}
	etag, err := o.Objects.Put(ctx, key, body, cond)
	if err == nil {
		o.mu.Lock()
		o.log = append(o.log, kind)
		o.mu.Unlock()
	}
	return etag, err
}

// Record writes the bundle, then its resolution, then the index. When its
// index write loses to another writer, it resolves the bundle again against
// the newer index, and stores that resolution before it writes again.
func TestRecordWritesResolutionBeforeIndex(t *testing.T) {
	ctx := context.Background()
	ordered := &orderedObjects{Objects: NewMemObjects(), t: t}
	s := newTestStore(t, ordered)
	if _, err := s.Record(ctx, "s1", chain(10), "cache-a", 1); err != nil {
		t.Fatal(err)
	}
	if want := []string{"bundle", "resolution", "index"}; !slices.Equal(ordered.log, want) {
		t.Fatalf("writes %v, want %v", ordered.log, want)
	}

	// Another writer takes the next numbers before each of Record's first
	// two index writes: Record's numbers change each time, and so does
	// its resolution.
	ordered.log = nil
	f := &interfere{Objects: ordered, key: "team/ns/s1/index.json"}
	f.n.Store(2)
	f.other = func(idx *Index) { idx.allocate() }
	s2 := newTestStore(t, f)
	s2.sleep = func(context.Context, time.Duration) error { return nil }
	rec, err := s2.Record(ctx, "s1", chain(50), "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	// The other writer's index writes are logged as index writes too.
	if want := []string{"bundle", "resolution", "index", "resolution", "index", "resolution", "index"}; !slices.Equal(ordered.log, want) {
		t.Fatalf("writes %v, want %v", ordered.log, want)
	}
	if rec.Numbers[1] != 6 {
		t.Fatalf("numbers %v: want them after the other writer's", rec.Numbers)
	}
	checkResolution(t, s2, "s1", rec, "cache-b")
	if e := mustIndex(t, s2, "s1").Entries[6]; e.Resolution != rec.ResolutionID {
		t.Fatalf("stored entry names %q, want %s", e.Resolution, rec.ResolutionID)
	}
}

// failingPuts fails writes of keys holding match.
type failingPuts struct {
	Objects
	match string
	fail  bool
}

func (o *failingPuts) Put(ctx context.Context, key string, body []byte, cond Condition) (string, error) {
	if o.fail && strings.Contains(key, o.match) {
		return "", errors.New("service stopped")
	}
	return o.Objects.Put(ctx, key, body, cond)
}

// A service that stops between storing the resolution and writing the index
// leaves a resolution no index names, which nothing reads, and records
// nothing: the export is not stored, and a later one stores it, reusing the
// resolution already there.
func TestRecordStopsAfterResolution(t *testing.T) {
	ctx := context.Background()
	objects := &failingPuts{Objects: NewMemObjects(), match: "/index.json", fail: true}
	s := newTestStore(t, objects)
	if _, err := s.Record(ctx, "s1", chain(10), "cache-a", 1); err == nil || !strings.Contains(err.Error(), "service stopped") {
		t.Fatalf("record: %v", err)
	}
	if idx := mustIndex(t, s, "s1"); len(idx.Entries) != 0 || idx.NextNumber != 1 {
		t.Fatalf("index after the stop: %+v", idx)
	}
	objects.fail = false
	rec, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	checkResolution(t, s, "s1", rec, "cache-a")
}

// A resolution that can't be stored ends the record before the index is
// written: the index never names a resolution that isn't stored.
func TestRecordResolutionWriteFails(t *testing.T) {
	ctx := context.Background()
	objects := &failingPuts{Objects: NewMemObjects(), match: "/resolutions/", fail: true}
	s := newTestStore(t, objects)
	if _, err := s.Record(ctx, "s1", chain(10), "cache-a", 1); err == nil || !strings.Contains(err.Error(), "service stopped") {
		t.Fatalf("record: %v", err)
	}
	if _, _, err := objects.Get(ctx, "team/ns/s1/index.json"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an index was written: %v", err)
	}
}

// An index written before resolutions has records that name none: it reads,
// and they stay so until their values are recorded again.
func TestIndexBeforeResolutions(t *testing.T) {
	ctx := context.Background()
	objects := NewMemObjects()
	s := newTestStore(t, objects)
	rec, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	// The same index, as written before resolutions: version 1, and no
	// "resolution".
	legacy := strings.ReplaceAll(mustJSON(t, rec.Index), `,"resolution":"`+rec.ResolutionID+`"`, "")
	legacy = strings.Replace(legacy, `{"version":4,`, `{"version":1,`, 1)
	legacy = regexp.MustCompile(`"recordedAtUnix":[0-9]+`).ReplaceAllString(legacy, "")
	legacy = regexp.MustCompile(`"updatedAtUnix":[0-9]+,`).ReplaceAllString(legacy, "")
	if strings.Contains(legacy, "resolution") || !strings.HasPrefix(legacy, `{"version":1,`) {
		t.Fatalf("legacy index %s", legacy)
	}
	if _, err := objects.Put(ctx, "team/ns/s1/index.json", []byte(legacy), Condition{}); err != nil {
		t.Fatal(err)
	}
	idx := mustIndex(t, s, "s1")
	for n, e := range idx.Entries {
		if e.Resolution != "" {
			t.Fatalf("entry %d: resolution %q", n, e.Resolution)
		}
	}
	// Recorded again, the records name the resolution.
	rec2, err := s.Record(ctx, "s1", chain(10), "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	checkResolution(t, s, "s1", rec2, "cache-a")
	// And it was written back as the current version (4, since #59), which
	// a service that reads only version 1 refuses rather than rewrite
	// without the resolutions.
	raw, _, err := objects.Get(ctx, "team/ns/s1/index.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(raw), `{"version":4,`) {
		t.Fatalf("written back as %.40s...", raw)
	}

	idx.Entries[1].Resolution = "not-an-id"
	if err := idx.Validate(); err == nil || !strings.Contains(err.Error(), "resolution") {
		t.Fatalf("an invalid resolution ID: %v", err)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// A resolution keeps no entry alive: DropExpired drops what expired, though
// a stored resolution still names it, and never hands its number out again,
// so the resolution can't come to name another entry.
func TestResolutionAndDropExpired(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, NewMemObjects())
	// 1 and 2 are retained by root 2; 3 by root 3, which expires.
	bundle := testBundle([]val{{sender: 10}, {sender: 11, deps: []uint64{1}}, {sender: 12}}, root(2, 0), root(3, 100))
	rec, err := s.Record(ctx, "s1", bundle, "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := s.DropExpired(ctx, "s1", 200)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dropped, []uint64{rec.Numbers[3]}) {
		t.Fatalf("dropped %v, want %d", dropped, rec.Numbers[3])
	}
	res := checkResolution(t, s, "s1", rec, "cache-a")
	if n, _ := res.Number(3); n != rec.Numbers[3] {
		t.Fatalf("resolution changed: %v", res.Numbers)
	}
	next, err := s.Record(ctx, "s1", chain(50), "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range next.Numbers {
		if slices.Contains(res.Numbers, n) {
			t.Fatalf("number %d, which the resolution names, was handed out again", n)
		}
	}
}

func TestResolutionRefuses(t *testing.T) {
	ctx := context.Background()
	id := strings.Repeat("a", 64)
	for name, numbers := range map[string]map[dagql.TransferOrdinal]uint64{
		"empty":        {},
		"gap":          {1: 5, 3: 6},
		"zero number":  {1: 0},
		"same numbers": {1: 5, 2: 5},
	} {
		if _, err := NewResolution(id, "c", numbers); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := NewResolution("x", "c", map[dagql.TransferOrdinal]uint64{1: 1}); err == nil {
		t.Error("bad bundle ID: no error")
	}
	if _, err := NewResolution(id, "", map[dagql.TransferOrdinal]uint64{1: 1}); err == nil {
		t.Error("no cacheID: no error")
	}
	// An ordinal past int's range is out of range, not a panic.
	huge := dagql.TransferOrdinal(1 << 63)
	if _, err := NewResolution(id, "c", map[dagql.TransferOrdinal]uint64{huge: 1}); err == nil {
		t.Error("huge ordinal: no error")
	}
	if n, ok := (&Resolution{Numbers: []uint64{7}}).Number(huge); ok || n != 0 {
		t.Errorf("huge ordinal: %d, %v", n, ok)
	}

	objects := NewMemObjects()
	s := newTestStore(t, objects)
	res, err := NewResolution(id, "c", map[dagql.TransferOrdinal]uint64{1: 7, 2: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Check(id, "c", testBundle([]val{{sender: 1}}, root(1, 0))); err == nil {
		t.Error("check against a bundle of another size: no error")
	}
	if err := res.Check(strings.Repeat("b", 64), "c", testBundle([]val{{sender: 1}, {sender: 2}}, root(1, 0))); err == nil {
		t.Error("check against another bundle: no error")
	}
	if _, err := s.PutResolution(ctx, "s1", nil); err == nil {
		t.Error("nil resolution: no error")
	}
	rid, err := s.PutResolution(ctx, "s1", res)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetResolution(ctx, "s1", strings.Repeat("0", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := s.GetResolution(ctx, "s1", "../x"); err == nil {
		t.Fatal("bad ID: no error")
	}
	// Corrupt content at its key.
	if _, err := objects.Put(ctx, "team/ns/s1/resolutions/"+rid+".json", []byte(`{"version":1}`), Condition{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetResolution(ctx, "s1", rid); !errors.Is(err, ErrCorruptBundle) {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := s.PutResolution(ctx, "s1", res); !errors.Is(err, ErrCorruptBundle) {
		t.Fatalf("put over corrupt: %v", err)
	}
}
