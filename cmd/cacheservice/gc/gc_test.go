package gc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

const prefix = "cache"

// env is in-memory stores on one settable clock, as GC sees them through a
// MemBucket.
type env struct {
	t      *testing.T
	clock  *Now
	blobs  *blobstore.MemStore
	objs   *metastore.MemObjects
	meta   *metastore.Store
	bucket *MemBucket

	mu     sync.Mutex
	limits map[string]Limits
}

func newEnv(t *testing.T) *env {
	t.Helper()
	clock := NewNow(time.Unix(1_800_000_000, 0))
	blobs := blobstore.NewMemStore("http://blobs")
	blobs.SetClock(clock.Now)
	objs := metastore.NewMemObjects()
	objs.SetClock(clock.Now)
	meta, err := metastore.New(metastore.Config{Objects: objs, Prefix: prefix, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	// The prefix is the service's: a writer claimed it.
	if _, _, err := ownership.Check(context.Background(), objs, prefix, true, clock.Now()); err != nil {
		t.Fatal(err)
	}
	return &env{
		t: t, clock: clock, blobs: blobs, objs: objs, meta: meta,
		bucket: &MemBucket{Prefix: prefix, Blobs: blobs, Objects: objs},
		limits: map[string]Limits{},
	}
}

func (e *env) setLimits(ns string, l Limits) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.limits[ns] = l
}

func (e *env) collector(modify func(*Config)) *Collector {
	e.t.Helper()
	cfg := Config{
		Bucket: e.bucket,
		Meta:   e.meta,
		Now:    e.clock.Now,
		Limits: func(ns string) Limits {
			e.mu.Lock()
			defer e.mu.Unlock()
			return e.limits[ns]
		},
	}
	if modify != nil {
		modify(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

func (e *env) view(ns string) *blobstore.MemStore {
	e.t.Helper()
	v, err := e.blobs.In(ns)
	if err != nil {
		e.t.Fatal(err)
	}
	return v
}

// layer is a distinct blob of size bytes.
func layer(name string, size int) []byte {
	b := []byte(strings.Repeat(name, size/len(name)+1))
	return b[:size]
}

// export stores, as an export does, an engine cache's entries rooted at
// roots, with every root's fs part: its blobs promoted, its bundle and
// resolution, and the index. It returns what Record stored.
func (e *env) export(ns, cacheID string, entries []fakeengine.Entry, roots ...uint64) metastore.Recorded {
	e.t.Helper()
	c := fakeengine.New(cacheID, 1)
	for _, en := range entries {
		c.Add(en)
	}
	req := protocol.Export{Roots: roots}
	for _, en := range entries {
		req.Outputs = append(req.Outputs, protocol.ExportOutput{Number: en.Number, Address: fakeengine.FSPart})
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

// uploadKey is the key of an incomplete multipart upload a promotion left
// in a namespace: a blob key.
func uploadKey(ns, name string) string { return nsPrefix(ns) + layout.BlobRel(digest.FromString(name)) }

// nsPrefix is a namespace's prefix: cache/ns/<ns>/.
func nsPrefix(ns string) string { return prefix + "/ns/" + ns + "/" }

// keys returns the namespace's keys, relative to it, sorted.
func (e *env) keys(ns string) []string {
	var out []string
	p := nsPrefix(ns)
	_ = e.bucket.List(context.Background(), p, func(objs []Object) error {
		for _, o := range objs {
			out = append(out, strings.TrimPrefix(o.Key, p))
		}
		return nil
	})
	return out
}

func (e *env) index(ns string) *metastore.Index {
	e.t.Helper()
	idx, err := e.meta.Index(context.Background(), ns)
	if err != nil {
		e.t.Fatal(err)
	}
	return idx
}

func (e *env) run(c *Collector) []Report {
	e.t.Helper()
	reps, err := c.Run(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return reps
}

func reportOf(reps []Report, ns string) Report {
	for _, r := range reps {
		if r.Namespace == ns {
			return r
		}
	}
	return Report{}
}

// checkConsistent fails if the namespace's index names a bundle or
// resolution that isn't stored, or a stored bundle a blob that isn't.
func (e *env) checkConsistent(ns string) {
	e.t.Helper()
	ctx := context.Background()
	idx := e.index(ns)
	v := e.view(ns)
	for n, en := range idx.Entries {
		b, err := e.meta.GetBundle(ctx, ns, en.Bundle)
		if err != nil {
			e.t.Fatalf("entry %d names bundle %s: %v", n, en.Bundle, err)
		}
		if en.Resolution != "" {
			if _, err := e.meta.GetResolution(ctx, ns, en.Resolution); err != nil {
				e.t.Fatalf("entry %d names resolution %s: %v", n, en.Resolution, err)
			}
		}
		for _, d := range metastore.BundleBlobs(b) {
			if _, ok := v.Get(d); !ok {
				e.t.Fatalf("entry %d's bundle names blob %s, not stored", n, d)
			}
		}
	}
}

// buildX is a base image (1) and an exec on it (2), with layers.
func buildX(tag string) []fakeengine.Entry {
	return []fakeengine.Entry{
		{Number: 1, Recipe: tag + "-base", Layers: [][]byte{layer(tag+"base", 1000)}},
		{Number: 2, Recipe: tag + "-exec", Deps: []uint64{1}, Layers: [][]byte{layer(tag+"base", 1000), layer(tag+"exec", 2000)}},
	}
}

func countPrefix(keys []string, p string) int {
	n := 0
	for _, k := range keys {
		if strings.HasPrefix(k, p) {
			n++
		}
	}
	return n
}

// Expired roots and entries leave the index at the next run; their
// bundles, resolutions and blobs, unreferenced from then on, are deleted
// once older than the grace period, and not before.
func TestExpiredDataCollectedAfterGrace(t *testing.T) {
	e := newEnv(t)
	e.setLimits("team", Limits{MaxAge: time.Hour})
	rec := e.export("team", "a", buildX("x"), 2)
	c := e.collector(nil)
	all := e.keys("team")
	if countPrefix(all, "blobs/") != 2 || countPrefix(all, "bundles/") != 1 || countPrefix(all, "resolutions/") != 1 {
		t.Fatalf("stored %v", all)
	}

	// Within its max age: nothing changes.
	e.clock.Add(30 * time.Minute)
	rep := reportOf(e.run(c), "team")
	if rep.Expired != 0 || len(rep.Deleted) != 0 || len(e.index("team").Roots) != 1 {
		t.Fatalf("within max age: %+v", rep)
	}

	// Past it: the root and its entries leave the index. The objects were
	// written 61 minutes ago, under the 2h grace: kept.
	e.clock.Add(31 * time.Minute)
	rep = reportOf(e.run(c), "team")
	if rep.Expired != 2 || len(e.index("team").Entries) != 0 {
		t.Fatalf("past max age: expired %d, index %+v", rep.Expired, e.index("team").Entries)
	}
	if len(rep.Deleted) != 0 || !slices.Equal(e.keys("team"), all) {
		t.Fatalf("deleted within the grace period: %+v, keys %v", rep.Deleted, e.keys("team"))
	}

	// Past the grace period: deleted.
	e.clock.Add(time.Hour)
	rep = reportOf(e.run(c), "team")
	want := map[string]int{"blob": 2, "bundle": 1, "resolution": 1}
	for k, n := range want {
		if rep.Deleted[k] != n {
			t.Fatalf("deleted %v, want %v", rep.Deleted, want)
		}
	}
	if left := e.keys("team"); !slices.Equal(left, []string{"gc-epoch.json", "index.json"}) {
		t.Fatalf("left %v", left)
	}
	idx := e.index("team")
	if idx.GC == nil || idx.GC.Epoch != 1 || len(idx.GC.Deletions) != 4 {
		t.Fatalf("collection %+v", idx.GC)
	}
	for k, d := range idx.GC.Deletions {
		if d.InProgress() || d.Epoch != 1 {
			t.Fatalf("deletion %s: %+v", k, d)
		}
	}
	if ep, _ := e.meta.ReadEpoch(context.Background(), "team"); ep != 1 {
		t.Fatalf("published epoch %d", ep)
	}
	if rec.BundleID == "" {
		t.Fatal("no bundle")
	}

	// The finished deletions stay for DeletionRetention, then go.
	e.clock.Add(metastore.DeletionRetention + time.Minute)
	e.run(c)
	if idx := e.index("team"); len(idx.GC.Deletions) != 0 {
		t.Fatalf("deletions kept past retention: %v", idx.GC.Deletions)
	}
}

// A cache record GC emptied is dropped CacheRecordRetention later, by a run
// that has nothing else to change in the index (#59).
func TestEmptyCacheRecordDroppedAlone(t *testing.T) {
	e := newEnv(t)
	e.setLimits("team", Limits{MaxAge: time.Hour})
	e.export("team", "a", buildX("x"), 2)
	c := e.collector(nil)
	e.clock.Add(2 * time.Hour)
	e.run(c)
	if cr := e.index("team").Caches["a"]; cr == nil || len(cr.Numbers) != 0 {
		t.Fatalf("after expiry, cache record %+v", cr)
	}
	// Deletions finish, and are pruned: nothing left to change but the
	// record.
	e.clock.Add(3 * time.Hour)
	e.run(c)
	e.clock.Add(metastore.DeletionRetention + time.Minute)
	e.run(c)
	if idx := e.index("team"); idx.Caches["a"] == nil || len(idx.GC.Deletions) != 0 {
		t.Fatalf("setup: cache %+v, deletions %v", idx.Caches["a"], idx.GC.Deletions)
	}
	e.clock.Add(metastore.CacheRecordRetention)
	rep := reportOf(e.run(c), "team")
	if cr := e.index("team").Caches["a"]; cr != nil || rep.Caches != 1 {
		t.Fatalf("past the retention: cache record %+v, report %+v", cr, rep)
	}
}

// What a live entry names is never deleted, however old; what nothing
// names is, once past the grace period: here a bundle and blob of an export
// whose index write never happened, and the bundle a later export of the
// same root superseded (#27).
func TestUnreferencedCollectedReferencedKept(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	first := e.export("team", "a", buildX("x"), 2)
	// A later state of the exec, with a new layer: its bundle supersedes
	// the first one's for both values.
	later := buildX("x")
	later[1].Replacements = 1
	later[1].Layers = [][]byte{layer("xbase", 1000), layer("xexec2", 2000)}
	second := e.export("team", "a", later, 2)
	if first.BundleID == second.BundleID {
		t.Fatal("same bundle")
	}
	// An orphan: a promoted blob and a stored bundle nothing names.
	orphanBlob := e.view("team").Put(layer("orphan", 500))
	orphan := fakeengine.New("z", 1)
	orphan.Add(fakeengine.Entry{Number: 9, Recipe: "orphan"})
	capt, err := orphan.Capture(protocol.Export{Roots: []uint64{9}})
	if err != nil {
		t.Fatal(err)
	}
	orphanID, err := e.meta.PutBundle(ctx, "team", capt.Bundle)
	if err != nil {
		t.Fatal(err)
	}

	c := e.collector(nil)
	e.clock.Add(time.Hour)
	if rep := reportOf(e.run(c), "team"); len(rep.Deleted) != 0 {
		t.Fatalf("deleted before the grace period: %v", rep.Deleted)
	}
	e.clock.Add(100 * 24 * time.Hour)
	rep := reportOf(e.run(c), "team")
	// The first bundle, its resolution, the old exec layer, the orphan
	// bundle and blob.
	if rep.Deleted["bundle"] != 2 || rep.Deleted["resolution"] != 1 || rep.Deleted["blob"] != 2 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	keys := e.keys("team")
	for _, gone := range []string{metastore.BundleRelKey(first.BundleID), metastore.ResolutionRelKey(first.ResolutionID), metastore.BundleRelKey(orphanID), metastore.BlobRelKey(orphanBlob), metastore.BlobRelKey(digest.FromBytes(layer("xexec", 2000)))} {
		if slices.Contains(keys, gone) {
			t.Fatalf("%s kept", gone)
		}
	}
	for _, kept := range []string{metastore.BundleRelKey(second.BundleID), metastore.ResolutionRelKey(second.ResolutionID), metastore.BlobRelKey(digest.FromBytes(layer("xbase", 1000))), metastore.BlobRelKey(digest.FromBytes(layer("xexec2", 2000)))} {
		if !slices.Contains(keys, kept) {
			t.Fatalf("%s deleted", kept)
		}
	}
	e.checkConsistent("team")
}

// The max size expires the oldest roots first, by recording time, until
// what the namespace keeps fits; the rest stay.
func TestMaxSizeExpiresOldestFirst(t *testing.T) {
	e := newEnv(t)
	var recs []metastore.Recorded
	for i := range 4 {
		tag := fmt.Sprintf("r%d", i)
		recs = append(recs, e.export("team", "a", []fakeengine.Entry{
			{Number: uint64(10 + i), Recipe: tag, Layers: [][]byte{layer(tag, 10_000)}},
		}, uint64(10+i)))
		e.clock.Add(time.Minute)
	}
	// Four roots of ~10 KB each; room for two and change.
	e.setLimits("team", Limits{MaxBytes: 25_000})
	c := e.collector(nil)
	rep := reportOf(e.run(c), "team")
	if rep.SizeRoots != 2 || rep.SizeExpired != 2 {
		t.Fatalf("expired %d roots, %d entries; want the 2 oldest", rep.SizeRoots, rep.SizeExpired)
	}
	if rep.LiveBytes > 25_000 {
		t.Fatalf("live bytes %d over the limit", rep.LiveBytes)
	}
	idx := e.index("team")
	for i, rec := range recs {
		_, kept := idx.Roots[rec.Numbers[1]]
		if kept != (i >= 2) {
			t.Fatalf("root %d (recorded %d minutes in) kept: %v", i, i, kept)
		}
	}
	// Once past the grace period, their objects go; the rest stays.
	e.clock.Add(3 * time.Hour)
	rep = reportOf(e.run(c), "team")
	if rep.Deleted["blob"] != 2 || rep.Deleted["bundle"] != 2 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	e.checkConsistent("team")
}

// GC of one namespace never deletes another's data, even the same layer
// stored under both.
//
// It also pins, on purpose, that the run that expires entries doesn't
// delete their objects, however old: an import sync that read the index
// just before may still be merging them, with GETs of their blobs. The next
// run, an interval later, deletes them.
func TestNamespacesApart(t *testing.T) {
	e := newEnv(t)
	e.export("team-a", "a", buildX("x"), 2)
	e.export("team-b", "b", buildX("x"), 2)
	before := e.keys("team-b")
	e.setLimits("team-a", Limits{MaxAge: time.Minute})
	c := e.collector(nil)
	e.clock.Add(10 * time.Hour)
	reps := e.run(c)
	if r := reportOf(reps, "team-a"); r.Expired != 2 || len(r.Deleted) != 0 {
		t.Fatalf("the expiring run: %+v", r)
	}
	reps = e.run(c)
	if d := reportOf(reps, "team-a").Deleted; d["blob"] != 2 {
		t.Fatalf("team-a deleted %v", d)
	}
	if d := reportOf(reps, "team-b").Deleted; len(d) != 0 {
		t.Fatalf("team-b deleted %v", d)
	}
	if after := e.keys("team-b"); !slices.Equal(before, after) {
		t.Fatalf("team-b: %v, then %v", before, after)
	}
	e.checkConsistent("team-b")
}

// A dry run deletes nothing and changes no index, and reports what a run
// would do.
func TestDryRunDeletesNothing(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	e.view("team").PutStaged(blobstore.NewStage(), layer("staged", 100))
	e.bucket.AddUpload(Upload{Key: uploadKey("team", "x"), ID: "u", Initiated: e.clock.Now()})
	e.setLimits("team", Limits{MaxAge: time.Minute})
	e.clock.Add(24 * time.Hour)
	body := func() string {
		b, _, err := e.objs.Get(context.Background(), prefix+"/ns/team/index.json")
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	keys, idx := e.keys("team"), body()
	c := e.collector(func(cfg *Config) { cfg.DryRun = true })
	rep := reportOf(e.run(c), "team")
	if !slices.Equal(keys, e.keys("team")) || idx != body() || len(e.bucket.Uploads()) != 1 || e.bucket.Deletes() != 0 {
		t.Fatal("a dry run changed the store")
	}
	// The run that expires entries doesn't delete their objects; the next
	// does (TestNamespacesApart).
	if rep.Expired != 2 || len(rep.WouldDelete) != 2 || len(rep.Deleted) != 0 {
		t.Fatalf("dry run report: expired %d, would delete %v", rep.Expired, rep.WouldDelete)
	}
	// The real runs then do what dry runs say.
	rep = reportOf(e.run(e.collector(nil)), "team")
	if rep.Expired != 2 || rep.Deleted["staging"] != 1 || rep.Deleted["upload"] != 1 || len(rep.Deleted) != 2 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	keys = e.keys("team")
	rep = reportOf(e.run(c), "team")
	if len(rep.WouldDelete) != 4 || !slices.Equal(keys, e.keys("team")) {
		t.Fatalf("second dry run: %v", rep.WouldDelete)
	}
	rep = reportOf(e.run(e.collector(nil)), "team")
	if n := rep.Deleted["blob"] + rep.Deleted["bundle"] + rep.Deleted["resolution"]; n != 4 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
}

// Staged uploads older than the staging age are deleted when no lifecycle
// rule expires the namespace's staging prefix (and the namespace is
// reported missing one); with a rule, GC leaves them to it. Stale
// incomplete multipart uploads are aborted either way.
func TestStagingAndUploads(t *testing.T) {
	e := newEnv(t)
	e.export("team-a", "a", buildX("x"), 2)
	e.export("team-b", "b", buildX("y"), 2)
	oldA := e.view("team-a").PutStaged(blobstore.NewStage(), layer("old", 100))
	oldB := e.view("team-b").PutStaged(blobstore.NewStage(), layer("old", 100))
	e.bucket.AddUpload(Upload{Key: uploadKey("team-a", "old"), ID: "1", Initiated: e.clock.Now()})
	// A large service-hashed promotion's private copy, left by a service
	// stopped midway: an upload at a staging key.
	privateKey := nsPrefix("team-a") + layout.StagingRel(string(blobstore.NewStage()), digest.FromString("private"))
	e.bucket.AddUpload(Upload{Key: privateKey, ID: "3", Initiated: e.clock.Now()})
	e.clock.Add(3 * time.Hour)
	young := e.view("team-a").PutStaged(blobstore.NewStage(), layer("young", 100))
	e.bucket.AddUpload(Upload{Key: uploadKey("team-a", "young"), ID: "2", Initiated: e.clock.Now()})
	e.bucket.SetRules([]string{prefix + "/ns/team-b/staging/"}, nil)

	reps := e.run(e.collector(nil))
	a, b := reportOf(reps, "team-a"), reportOf(reps, "team-b")
	if a.StagingRule != "missing" || b.StagingRule != "present" {
		t.Fatalf("rules: a %s, b %s", a.StagingRule, b.StagingRule)
	}
	if a.Deleted["staging"] != 1 || a.Deleted["upload"] != 2 || a.UnexpectedCount != 0 {
		t.Fatalf("team-a deleted %v, %d unexpected", a.Deleted, a.UnexpectedCount)
	}
	if got := e.view("team-a").Staged(); !slices.Equal(got, []digest.Digest{young}) {
		t.Fatalf("team-a staged %v, want only %s (not %s)", got, young, oldA)
	}
	if got := e.view("team-b").Staged(); !slices.Equal(got, []digest.Digest{oldB}) {
		t.Fatalf("team-b, which has a rule: staged %v", got)
	}
	if u := e.bucket.Uploads(); len(u) != 1 || u[0].ID != "2" {
		t.Fatalf("uploads %v", u)
	}

	// Rules that can't be read: GC expires staging itself.
	e.bucket.SetRules(nil, ErrRulesUnknown)
	e.clock.Add(3 * time.Hour)
	reps = e.run(e.collector(nil))
	if r := reportOf(reps, "team-b"); r.StagingRule != "unknown" || r.Deleted["staging"] != 1 {
		t.Fatalf("unknown rules: %+v", r)
	}
}

// An index that names a bundle GC can't read stops the namespace's
// reachability sweep: it can't know that bundle's blobs.
func TestMissingBundleStopsSweep(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	rec := e.export("team", "a", buildX("x"), 2)
	orphan := e.view("team").Put(layer("orphan", 100))
	if err := e.objs.Delete(ctx, prefix+"/ns/team/"+metastore.BundleRelKey(rec.BundleID)); err != nil {
		t.Fatal(err)
	}
	e.clock.Add(1000 * time.Hour)
	reps, err := e.collector(nil).Run(ctx)
	if err == nil {
		t.Fatal("no error")
	}
	if r := reportOf(reps, "team"); len(r.Deleted) != 0 || r.Skipped == "" {
		t.Fatalf("report %+v", r)
	}
	if _, ok := e.view("team").Get(orphan); !ok {
		t.Fatal("a blob was deleted while a named bundle couldn't be read")
	}
}

// The race of an export against a sweep, step by step. The export checks
// a blob (present: stored, unreferenced, older than the grace period),
// then GC claims and deletes it, then the export records: refused, since
// it checked before the deletion. Tried again from the start, it finds
// the blob missing, uploads it, and is recorded; the index never names a
// missing blob. Recording while GC is deleting is refused too.
func TestExportRacesSweep(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	// Stored, then expired: its objects are unreferenced, and old.
	e.setLimits("team", Limits{MaxAge: time.Minute})
	e.export("team", "a", buildX("x"), 2)
	e.clock.Add(3 * time.Hour)
	e.run(e.collector(nil)) // expires them
	e.run(e.collector(nil)) // deletes their objects, 3h old
	e.setLimits("team", Limits{})
	// Again: stored by an export, then dropped from the index.
	e.export("team", "a", buildX("x"), 2)
	if _, err := e.meta.UpdateIndex(ctx, "team", func(idx *metastore.Index) error {
		idx.Roots = map[uint64]metastore.Root{}
		idx.DropExpired(0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.clock.Add(3 * time.Hour)

	v := e.view("team")
	exec := digest.FromBytes(layer("xexec", 2000))
	c := fakeengine.New("b", 1)
	for _, en := range buildX("x") {
		c.Add(en)
	}
	capt, err := c.Capture(protocol.Export{Roots: []uint64{2}, Outputs: []protocol.ExportOutput{{Number: 2, Address: fakeengine.FSPart}}})
	if err != nil {
		t.Fatal(err)
	}
	// The export: its fence, then its checks.
	exportTry := func() (metastore.Fence, []digest.Digest) {
		epoch, err := e.meta.ReadEpoch(ctx, "team")
		if err != nil {
			t.Fatal(err)
		}
		fence := metastore.Fence{Epoch: epoch, Started: e.clock.Now()}
		var missing []digest.Digest
		for d := range capt.Blobs {
			if ok, _ := v.Has(ctx, d); !ok {
				missing = append(missing, d)
			}
		}
		return fence, missing
	}
	fence, missing := exportTry()
	if len(missing) != 0 {
		t.Fatalf("missing %v before the sweep", missing)
	}

	// While GC deletes, recording is refused (the claims are in progress).
	gcc := e.collector(nil)
	var during error
	e.bucket.BeforeDelete(func([]string) {
		_, during = e.meta.RecordFenced(ctx, "team", capt.Bundle, "b", 1, fence)
	})
	rep := reportOf(e.run(gcc), "team")
	e.bucket.BeforeDelete(nil)
	if !errors.Is(during, metastore.ErrCollected) {
		t.Fatalf("recording during the deletes: %v", during)
	}
	if rep.Deleted["blob"] != 2 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	if _, ok := v.Get(exec); ok {
		t.Fatal("not deleted")
	}

	// After: refused too, since the export checked before the deletion.
	if _, err := e.meta.RecordFenced(ctx, "team", capt.Bundle, "b", 1, fence); !errors.Is(err, metastore.ErrCollected) {
		t.Fatalf("recording after the deletes: %v", err)
	}
	e.checkConsistent("team")

	// Tried again: new fence, new checks, uploads what is gone.
	fence, missing = exportTry()
	if len(missing) != 2 {
		t.Fatalf("missing %v after the sweep", missing)
	}
	for _, d := range missing {
		v.Put(capt.Blobs[d])
	}
	if _, err := e.meta.RecordFenced(ctx, "team", capt.Bundle, "b", 1, fence); err != nil {
		t.Fatal(err)
	}
	e.checkConsistent("team")
}

// A blob promoted for an export that isn't recorded yet is never collected:
// it is younger than the grace period, which covers an export from its
// promotions to its index write. Here the export waits most of the grace
// period (as long as an export's upload timeout and more) between the two.
func TestInFlightBlobKept(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.export("team", "a", buildX("x"), 2) // so the namespace has an index
	c := fakeengine.New("b", 1)
	c.Add(fakeengine.Entry{Number: 5, Recipe: "new", Layers: [][]byte{layer("new", 300)}})
	capt, err := c.Capture(protocol.Export{Roots: []uint64{5}, Outputs: []protocol.ExportOutput{{Number: 5, Address: fakeengine.FSPart}}})
	if err != nil {
		t.Fatal(err)
	}
	epoch, _ := e.meta.ReadEpoch(ctx, "team")
	fence := metastore.Fence{Epoch: epoch, Started: e.clock.Now()}
	for _, data := range capt.Blobs {
		e.view("team").Put(data) // promoted
	}
	e.clock.Add(DefaultGrace - time.Minute)
	if rep := reportOf(e.run(e.collector(nil)), "team"); len(rep.Deleted) != 0 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	if _, err := e.meta.RecordFenced(ctx, "team", capt.Bundle, "b", 1, fence); err != nil {
		t.Fatal(err)
	}
	e.checkConsistent("team")
}

// Two GC runs at once, as two replicas would run them, on the same stores:
// each claims only what the other hasn't, nothing is deleted twice or
// left claimed, and what is live stays.
func TestConcurrentRuns(t *testing.T) {
	e := newEnv(t)
	for i := range 6 {
		tag := fmt.Sprintf("g%d", i)
		e.export("team", "a", []fakeengine.Entry{{Number: uint64(10 + i), Recipe: tag, Layers: [][]byte{layer(tag, 1000)}}}, uint64(10+i))
	}
	live := e.export("team", "a", buildX("live"), 2)
	if _, err := e.meta.UpdateIndex(context.Background(), "team", func(idx *metastore.Index) error {
		for n := range idx.Roots {
			if n != live.Numbers[2] {
				delete(idx.Roots, n)
			}
		}
		idx.DropExpired(0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.clock.Add(3 * time.Hour)

	// A runs B in the middle of its deletes: B sees A's claims.
	a, b := e.collector(nil), e.collector(nil)
	var inner Report
	once := sync.Once{}
	e.bucket.BeforeDelete(func([]string) {
		once.Do(func() {
			reps, err := b.Run(context.Background())
			if err != nil {
				t.Error(err)
			}
			inner = reportOf(reps, "team")
		})
	})
	outer := reportOf(e.run(a), "team")
	e.bucket.BeforeDelete(nil)
	if len(inner.Deleted) != 0 {
		t.Fatalf("B deleted what A claimed: %v", inner.Deleted)
	}
	if outer.Deleted["blob"] != 6 || outer.Deleted["bundle"] != 6 {
		t.Fatalf("A deleted %v", outer.Deleted)
	}

	// And truly at once, many times, with new garbage each round.
	for round := range 5 {
		tag := fmt.Sprintf("round%d", round)
		e.view("team").Put(layer(tag, 100))
		e.clock.Add(3 * time.Hour)
		var wg sync.WaitGroup
		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := e.collector(nil).Run(context.Background()); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if _, ok := e.view("team").Get(digest.FromBytes(layer(tag, 100))); ok {
			t.Fatalf("round %d: garbage left", round)
		}
	}
	idx := e.index("team")
	for k, d := range idx.GC.Deletions {
		if d.InProgress() {
			t.Fatalf("%s left claimed", k)
		}
	}
	e.checkConsistent("team")
	if len(idx.Roots) != 1 {
		t.Fatalf("roots %v", idx.Roots)
	}
}

// A run that stopped between its claim and its finish leaves its claims in
// progress: exports naming them are refused until a later run, of any
// replica, takes them over once they are stale, deletes and finishes.
func TestStaleClaimTakenOver(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.export("team", "a", buildX("x"), 2)
	orphan := e.view("team").Put(layer("orphan", 100))
	key := metastore.BlobRelKey(orphan)
	if _, err := e.meta.UpdateIndex(ctx, "team", func(idx *metastore.Index) error {
		idx.GC = &metastore.Collection{Deletions: map[string]metastore.Deletion{key: {Run: "gone", StartedUnix: e.clock.Now().Unix()}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c := e.collector(nil)
	e.clock.Add(time.Minute)
	if rep := reportOf(e.run(c), "team"); len(rep.Deleted) != 0 {
		t.Fatalf("a fresh claim was taken over: %v", rep.Deleted)
	}
	e.clock.Add(DefaultDeleteDeadline + staleSlack)
	if rep := reportOf(e.run(c), "team"); rep.Deleted["blob"] != 1 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	if d := e.index("team").GC.Deletions[key]; d.InProgress() {
		t.Fatalf("still in progress: %+v", d)
	}
}

// The namespaces are the bucket's, whatever the tokens: one is found as
// long as it has objects.
func TestNamespacesFromBucket(t *testing.T) {
	e := newEnv(t)
	e.export("team-a", "a", buildX("x"), 2)
	e.view("team-b").Put(layer("b", 10))
	got, err := e.collector(nil).Namespaces(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"team-a", "team-b"}) {
		t.Fatalf("namespaces %v", got)
	}
}

// afterList runs a hook once, after the first listing of a namespace's
// objects: between what GC saw and its claim.
type afterList struct {
	Bucket
	once sync.Once
	hook func()
}

func (b *afterList) List(ctx context.Context, p string, fn func([]Object) error) error {
	err := b.Bucket.List(ctx, p, fn)
	b.once.Do(b.hook)
	return err
}

// The claim decides against the index it writes, not against what the run
// listed: an export recorded since that names a candidate keeps it, and a
// candidate another run has claimed since is that run's to delete.
func TestClaimRechecksIndex(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.export("team", "a", buildX("x"), 2)
	reused := e.view("team").Put(layer("reused", 300))
	other := e.view("team").Put(layer("other", 300))
	e.clock.Add(3 * time.Hour)

	b := &afterList{Bucket: e.bucket}
	b.hook = func() {
		// An export names reused (it found it stored)...
		c := fakeengine.New("b", 1)
		c.Add(fakeengine.Entry{Number: 7, Recipe: "reuse", Layers: [][]byte{layer("reused", 300)}})
		capt, err := c.Capture(protocol.Export{Roots: []uint64{7}, Outputs: []protocol.ExportOutput{{Number: 7, Address: fakeengine.FSPart}}})
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := e.meta.Record(ctx, "team", capt.Bundle, "b", 1); err != nil {
			t.Error(err)
		}
		// ...and another run claims other.
		if _, err := e.meta.UpdateIndex(ctx, "team", func(idx *metastore.Index) error {
			idx.GC = &metastore.Collection{Deletions: map[string]metastore.Deletion{metastore.BlobRelKey(other): {Run: "other", StartedUnix: e.clock.Now().Unix()}}}
			return nil
		}); err != nil {
			t.Error(err)
		}
	}
	c := e.collector(func(cfg *Config) { cfg.Bucket = b })
	rep := reportOf(e.run(c), "team")
	if rep.Deleted["blob"] != 0 {
		t.Fatalf("deleted %v", rep.Deleted)
	}
	if _, ok := e.view("team").Get(reused); !ok {
		t.Fatal("a blob an export named since the listing was deleted")
	}
	if _, ok := e.view("team").Get(other); !ok {
		t.Fatal("a blob another run claimed was deleted by this one")
	}
	if d := e.index("team").GC.Deletions[metastore.BlobRelKey(other)]; d.Run != "other" {
		t.Fatalf("the other run's claim was taken: %+v", d)
	}
	e.checkConsistent("team")
}

// Claims taken over from stopped runs count against MaxDeletes, with new
// candidates: one run never deletes more, however many stopped runs left
// claims. The rest stay claimed for later runs.
func TestStaleClaimsBoundedByMaxDeletes(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.export("team", "a", buildX("x"), 2)
	var keys []string
	for i := range 5 {
		keys = append(keys, metastore.BlobRelKey(e.view("team").Put(layer(fmt.Sprintf("stale%d", i), 100))))
	}
	if _, err := e.meta.UpdateIndex(ctx, "team", func(idx *metastore.Index) error {
		idx.GC = &metastore.Collection{Deletions: map[string]metastore.Deletion{}}
		for _, k := range keys {
			idx.GC.Deletions[k] = metastore.Deletion{Run: "gone", StartedUnix: e.clock.Now().Unix()}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.clock.Add(time.Hour)
	c := e.collector(func(cfg *Config) { cfg.MaxDeletes = 2 })
	for run, want := range []int{2, 2, 1} {
		if rep := reportOf(e.run(c), "team"); rep.Deleted["blob"] != want {
			t.Fatalf("run %d deleted %v, want %d blobs", run, rep.Deleted, want)
		}
	}
	for _, k := range keys {
		if d := e.index("team").GC.Deletions[k]; d.InProgress() {
			t.Fatalf("%s still claimed", k)
		}
	}
}

// A published epoch that lags the index's (a finish whose publish failed,
// or a replica that published late) is caught up by the next run.
func TestEpochRepublished(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.export("team", "a", buildX("x"), 2)
	if _, err := e.meta.UpdateIndex(ctx, "team", func(idx *metastore.Index) error {
		idx.GC = &metastore.Collection{Epoch: 3}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.meta.WriteEpoch(ctx, "team", 1); err != nil {
		t.Fatal(err)
	}
	e.run(e.collector(nil))
	if ep, _ := e.meta.ReadEpoch(ctx, "team"); ep != 3 {
		t.Fatalf("published epoch %d, want 3", ep)
	}
}

// A root an export records again (a new record) within the same second as
// GC's snapshot is kept by the max size: its recording time can't show it
// (one-second resolution), but its record changed.
func TestMaxSizeKeepsRootRecordedSameSecond(t *testing.T) {
	e := newEnv(t)
	var recs []metastore.Recorded
	for i := range 3 {
		tag := fmt.Sprintf("r%d", i)
		recs = append(recs, e.export("team", "a", []fakeengine.Entry{
			{Number: uint64(10 + i), Recipe: tag, Layers: [][]byte{layer(tag, 10_000)}},
		}, uint64(10+i)))
		e.clock.Add(time.Minute)
	}
	e.setLimits("team", Limits{MaxBytes: 25_000})
	// Between GC's snapshot and its update, in the same second, the oldest
	// root's value is exported again, changed.
	b := &afterList{Bucket: e.bucket, hook: func() {
		e.clock.Add(-3 * time.Minute) // its recording time, as GC read it
		e.export("team", "a", []fakeengine.Entry{{Number: 10, Replacements: 1, Recipe: "r0", Layers: [][]byte{layer("r0", 10_000)}}}, 10)
		e.clock.Add(3 * time.Minute)
	}}
	// The hook runs after the listing, which comes before the max size.
	rep := reportOf(e.run(e.collector(func(cfg *Config) { cfg.Bucket = b })), "team")
	idx := e.index("team")
	if _, ok := idx.Roots[recs[0].Numbers[1]]; !ok {
		t.Fatalf("the root recorded again was expired: %+v", rep)
	}
	// GC had chosen that root alone; it expires nothing else this run, and
	// a later run, on a fresh snapshot, enforces the limit.
	if r := idx.Roots[recs[0].Numbers[1]]; r.RecordedAtUnix != recs[0].Index.Roots[recs[0].Numbers[1]].RecordedAtUnix {
		t.Fatalf("the test's re-record isn't in the same second: %d", r.RecordedAtUnix)
	}
	if rep.SizeRoots != 0 || len(idx.Roots) != 3 {
		t.Fatalf("expired %d roots; roots %v", rep.SizeRoots, idx.Roots)
	}
}

// When the bucket's rules can't be read, the namespace's
// gc_staging_rule_missing is removed: neither 0 nor 1 would be true.
func TestStagingRuleMetricUnknown(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	c := e.collector(func(cfg *Config) { cfg.Metrics = m })
	e.bucket.SetRules([]string{prefix + "/ns/team/staging/"}, nil)
	e.run(c)
	if s := metricstest.Read(t, reg, "cacheservice_gc_staging_rule_missing", map[string]string{"scope": "team"}); !s.Found || s.Value != 0 {
		t.Fatalf("with a rule: %+v", s)
	}
	e.bucket.SetRules(nil, ErrRulesUnknown)
	e.run(c)
	if s := metricstest.Read(t, reg, "cacheservice_gc_staging_rule_missing", map[string]string{"scope": "team"}); s.Found {
		t.Fatalf("rules unknown: still %v", s.Value)
	}
}

// Known gap (zerobull-consulting/dagger#57): the hold that keeps a run from
// deleting what its own expiry just released (TestNamespacesApart) is the
// run's, not durable. Another replica's run right after sees no change, and
// deletes those objects at once. Benign for consistency (the index never
// names them); a merge in flight may hand out GETs of blobs that then go,
// and the engine computes those parts again.
func TestKnownGapHoldIsPerReplica(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	e.setLimits("team", Limits{MaxAge: time.Minute})
	e.clock.Add(10 * time.Hour)
	a, b := e.collector(nil), e.collector(nil)
	if r := reportOf(e.run(a), "team"); r.Expired != 2 || len(r.Deleted) != 0 {
		t.Fatalf("replica A, the expiring run: %+v", r)
	}
	r := reportOf(e.run(b), "team")
	if r.Deleted["blob"] != 2 {
		t.Fatalf("replica B: %+v; if it held the objects, the gap is fixed: update #57 and this test", r)
	}
	e.checkConsistent("team")
}

// sameRecord compares every field that decides what an entry keeps alive and
// until when, not only which record it is.
func TestSameRecord(t *testing.T) {
	base := metastore.Entry{Bundle: strings.Repeat("a", 64), Ordinal: 1, Source: metastore.Source{CacheID: "c", Number: 5}, Deps: []uint64{2}, Parts: []string{"p"}, ExpiresAtUnix: 10}
	if !sameRecord(base, base) {
		t.Fatal("an entry differs from itself")
	}
	for name, change := range map[string]func(*metastore.Entry){
		"deps":    func(e *metastore.Entry) { e.Deps = []uint64{2, 3} },
		"parts":   func(e *metastore.Entry) { e.Parts = []string{"p", "q"} },
		"expiry":  func(e *metastore.Entry) { e.ExpiresAtUnix = 20 },
		"bundle":  func(e *metastore.Entry) { e.Bundle = strings.Repeat("b", 64) },
		"replace": func(e *metastore.Entry) { e.Replacements = 1 },
	} {
		e := base
		e.Deps, e.Parts = slices.Clone(base.Deps), slices.Clone(base.Parts)
		change(&e)
		if sameRecord(base, e) {
			t.Errorf("%s changed, and sameRecord says same", name)
		}
	}
}

// A collector refuses a delete deadline over MaxDeleteDeadline, and takes a
// claim over only once it is MaxDeleteDeadline (plus slack) old, whatever
// its own, shorter, deadline: another replica may still be deleting under
// a longer one.
func TestStalenessIgnoresOwnDeadline(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := New(Config{Bucket: e.bucket, Meta: e.meta, DeleteDeadline: MaxDeleteDeadline + time.Second}); err == nil {
		t.Fatal("a delete deadline over the max: no error")
	}
	e.export("team", "a", buildX("x"), 2)
	orphan := e.view("team").Put(layer("orphan", 100))
	key := metastore.BlobRelKey(orphan)
	if _, err := e.meta.UpdateIndex(ctx, "team", func(idx *metastore.Index) error {
		idx.GC = &metastore.Collection{Deletions: map[string]metastore.Deletion{key: {Run: "other", StartedUnix: e.clock.Now().Unix()}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	short := e.collector(func(cfg *Config) { cfg.DeleteDeadline = time.Minute })
	// Past the short deadline plus slack, within the max one's.
	e.clock.Add(time.Minute + staleSlack + time.Minute)
	if r := reportOf(e.run(short), "team"); len(r.Deleted) != 0 {
		t.Fatalf("took over a claim another replica may still delete under: %v", r.Deleted)
	}
	e.clock.Add(MaxDeleteDeadline)
	if r := reportOf(e.run(short), "team"); r.Deleted["blob"] != 1 {
		t.Fatalf("past the max deadline: %v", r.Deleted)
	}
}

// A delete still in flight at the deadline is canceled: nothing is deleted
// after it, when another replica may take the claim over.
func TestDeletesCanceledAtDeadline(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	orphan := e.view("team").Put(layer("orphan", 100))
	e.clock.Add(3 * time.Hour)
	e.bucket.BeforeDelete(func([]string) { time.Sleep(200 * time.Millisecond) })
	c := e.collector(func(cfg *Config) { cfg.DeleteDeadline = 50 * time.Millisecond })
	reps, err := c.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("run: %v", err)
	}
	if _, ok := e.view("team").Get(orphan); !ok {
		t.Fatalf("deleted after its deadline: %+v", reportOf(reps, "team"))
	}
}

// A namespace whose index can't be read keeps its bundles and blobs (GC
// can't tell what is in use) but still has its staged and multipart
// uploads collected: those don't need the index.
func TestUnreadableIndexStillCollectsUploads(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.export("team", "a", buildX("x"), 2)
	if _, err := e.objs.Put(ctx, prefix+"/ns/team/index.json", []byte(`{"version":99,"nextNumber":1}`), metastore.Condition{}); err != nil {
		t.Fatal(err)
	}
	staged := e.view("team").PutStaged(blobstore.NewStage(), layer("staged", 100))
	e.bucket.AddUpload(Upload{Key: prefix + "/ns/team/blobs/sha256/" + strings.Repeat("0", 64), ID: "1", Initiated: e.clock.Now()})
	blobs := e.view("team").Len()
	e.clock.Add(1000 * time.Hour)
	reps, err := e.collector(nil).Run(ctx)
	if err == nil {
		t.Fatal("no error for an unreadable index")
	}
	r := reportOf(reps, "team")
	if r.Deleted["staging"] != 1 || r.Deleted["upload"] != 1 || r.Deleted["blob"] != 0 {
		t.Fatalf("report %+v", r)
	}
	if e.view("team").Len() != blobs || slices.Contains(e.view("team").Staged(), staged) {
		t.Fatal("blobs deleted, or the staged upload kept")
	}
}

// A staged upload is kept past the export upload timeout, by an hour: an
// export waits that long for its uploads, then promotes them, which takes
// minutes for a large blob. Deleting it then would cost its upload again.
func TestStagingKeptThroughPromotion(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	staged := e.view("team").PutStaged(blobstore.NewStage(), layer("staged", 100))
	e.clock.Add(exporter.DefaultUploadTimeout + 30*time.Minute)
	if r := reportOf(e.run(e.collector(nil)), "team"); r.Deleted["staging"] != 0 {
		t.Fatalf("deleted a staged upload %s after the upload timeout, before its promotion could finish", exporter.DefaultUploadTimeout+30*time.Minute)
	}
	e.clock.Add(time.Hour)
	if r := reportOf(e.run(e.collector(nil)), "team"); r.Deleted["staging"] != 1 || slices.Contains(e.view("team").Staged(), staged) {
		t.Fatalf("past the staging age: %+v", r)
	}
}

// A dry run reports the stale claims a real run would take over and
// delete, not only new candidates.
func TestDryRunReportsStaleClaims(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.export("team", "a", buildX("x"), 2)
	orphan := e.view("team").Put(layer("orphan", 100))
	key := metastore.BlobRelKey(orphan)
	if _, err := e.meta.UpdateIndex(ctx, "team", func(idx *metastore.Index) error {
		idx.GC = &metastore.Collection{Deletions: map[string]metastore.Deletion{key: {Run: "gone", StartedUnix: e.clock.Now().Unix()}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.clock.Add(MaxDeleteDeadline + staleSlack + time.Minute)
	dry := reportOf(e.run(e.collector(func(cfg *Config) { cfg.DryRun = true })), "team")
	if !slices.Contains(dry.WouldDelete, key) {
		t.Fatalf("dry run would delete %v, missing the stale claim %s", dry.WouldDelete, key)
	}
	real := reportOf(e.run(e.collector(nil)), "team")
	if real.Deleted["blob"] != 1 {
		t.Fatalf("the real run: %+v", real)
	}
}

// A claim is stamped with the time it is made, not the run's start: on a
// namespace that takes minutes to list and mark, a claim stamped with the
// start would look stale to other replicas while this run still deletes.
func TestClaimStampedAtClaimTime(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	orphan := e.view("team").Put(layer("orphan", 100))
	e.clock.Add(3 * time.Hour)
	b := &afterList{Bucket: e.bucket, hook: func() { e.clock.Add(20 * time.Minute) }} // a slow listing
	var started int64
	e.bucket.BeforeDelete(func([]string) {
		started = e.index("team").GC.Deletions[metastore.BlobRelKey(orphan)].StartedUnix
	})
	e.run(e.collector(func(cfg *Config) { cfg.Bucket = b }))
	if want := e.clock.Now().Unix(); started != want {
		t.Fatalf("claim stamped %d, want the claim's time %d (the run started %ds before)", started, want, want-started)
	}
}

// A delete whose request reports some keys failing counts the others as
// deleted, reports the failures, and goes on with the next batches; the
// failed keys stay, and nothing names them, so a later run claims them
// again.
func TestPartialDeleteFailure(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	bad := e.view("team").Put(layer("bad", 100))
	good := e.view("team").Put(layer("good", 100))
	e.bucket.FailDeletes(prefix + "/ns/team/" + metastore.BlobRelKey(bad))
	e.clock.Add(3 * time.Hour)
	reps, err := e.collector(nil).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 of 2 keys failed") {
		t.Fatalf("run: %v", err)
	}
	if r := reportOf(reps, "team"); r.Deleted["blob"] != 1 {
		t.Fatalf("counted %v, want the one deleted", r.Deleted)
	}
	if _, ok := e.view("team").Get(good); ok {
		t.Fatal("the key that didn't fail wasn't deleted")
	}
	if _, ok := e.view("team").Get(bad); !ok {
		t.Fatal("the failed key is gone")
	}
	e.bucket.FailDeletes() // nothing new
	e.bucket.failKeys = nil
	if r := reportOf(e.run(e.collector(nil)), "team"); r.Deleted["blob"] != 1 {
		t.Fatalf("a later run: %+v", r)
	}
}

// liveBytes counts the index once, each bundle and resolution once, and
// each blob once, however many bundles name it; and only what entries
// reachable from the roots kept name.
func TestLiveBytes(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2) // base 1000, exec 2000
	// Another root sharing the base layer.
	e.export("team", "a", []fakeengine.Entry{
		{Number: 1, Recipe: "x-base", Layers: [][]byte{layer("xbase", 1000)}},
		{Number: 3, Recipe: "x-other", Deps: []uint64{1}, Layers: [][]byte{layer("xbase", 1000), layer("other", 3000)}},
	}, 3)
	c := e.collector(nil)
	idx := e.index("team")
	l, err := c.list(context.Background(), "team")
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.references(context.Background(), "team", idx)
	if err != nil {
		t.Fatal(err)
	}
	var meta int64
	for _, o := range l.bundles {
		meta += o.Size
	}
	for _, o := range l.resolutions {
		meta += o.Size
	}
	if got, want := liveBytes(idx, nil, r, l), l.indexBytes+meta+1000+2000+3000; got != want {
		t.Fatalf("liveBytes %d, want %d (the shared base once)", got, want)
	}
	// Without the second root, nothing goes: the second export also carried
	// the base, whose record moved to the second bundle, which the first
	// root keeps through the base. GC keeps every blob a live bundle names
	// (the mark is per bundle), and liveBytes counts what GC keeps.
	var second uint64
	for n := range idx.Roots {
		if idx.Entries[n].Source.Number == 3 {
			second = n
		}
	}
	if got, want := liveBytes(idx, map[uint64]bool{second: true}, r, l), l.indexBytes+meta+1000+2000+3000; got != want {
		t.Fatalf("without the second root: %d, want %d", got, want)
	}
}

// slowIndexWrites delays every index write by delay.
type slowIndexWrites struct {
	metastore.Objects
	delay time.Duration
}

func (o *slowIndexWrites) Put(ctx context.Context, key string, body []byte, cond metastore.Condition) (string, error) {
	if strings.HasSuffix(key, "/index.json") {
		time.Sleep(o.delay)
	}
	return o.Objects.Put(ctx, key, body, cond)
}

// The delete deadline counts from the claim's stamp, not from after the
// claim's index update returns: when that update is slow (lost writes, a
// slow store), deletes counted from after it could run past the time other
// replicas take the claim over (its stamp plus MaxDeleteDeadline plus
// slack), and remove an object re-uploaded since. A run whose claim took
// past its deadline deletes nothing, and releases its claims.
func TestDeleteDeadlineFromClaimStamp(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	e.view("team").Put(layer("orphan", 100))
	e.clock.Add(3 * time.Hour)
	e.run(e.collector(nil)) // stamps and settles everything else first
	orphan2 := e.view("team").Put(layer("orphan2", 100))
	e.clock.Add(3 * time.Hour)
	slow, err := metastore.New(metastore.Config{Objects: &slowIndexWrites{Objects: e.objs, delay: 200 * time.Millisecond}, Prefix: prefix, Now: e.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	c := e.collector(func(cfg *Config) {
		cfg.Meta = slow
		cfg.DeleteDeadline = 100 * time.Millisecond
	})
	reps, err := c.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("run: %v", err)
	}
	if r := reportOf(reps, "team"); len(r.Deleted) != 0 {
		t.Fatalf("deleted %v after the deadline counted from the claim", r.Deleted)
	}
	if _, ok := e.view("team").Get(orphan2); !ok {
		t.Fatal("an object was deleted past the claim's deadline")
	}
	for k, d := range e.index("team").GC.Deletions {
		if d.InProgress() {
			t.Fatalf("%s left claimed", k)
		}
	}
}

// The gauges of a namespace gone from the bucket are removed, rather than
// report its last listing forever.
func TestGaugesOfGoneNamespaceRemoved(t *testing.T) {
	e := newEnv(t)
	e.export("gone", "g", buildX("g"), 2)
	e.export("team", "a", buildX("x"), 2)
	reg := prometheus.NewRegistry()
	c := e.collector(func(cfg *Config) { cfg.Metrics = metrics.New(reg) })
	e.run(c)
	labels := map[string]string{"scope": "gone", "kind": "blob"}
	if s := metricstest.Read(t, reg, "cacheservice_gc_stored_bytes", labels); !s.Found || s.Value != 3000 {
		t.Fatalf("gone, listed: %+v", s)
	}
	if s := metricstest.Read(t, reg, "cacheservice_gc_unexpected_keys", map[string]string{"scope": "gone"}); !s.Found || s.Value != 0 {
		t.Fatalf("gone, unexpected keys: %+v", s)
	}
	var keys []string
	_ = e.bucket.List(context.Background(), nsPrefix("gone"), func(objs []Object) error {
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		return nil
	})
	if _, err := e.bucket.Delete(context.Background(), keys); err != nil {
		t.Fatal(err)
	}
	e.run(c)
	if s := metricstest.Read(t, reg, "cacheservice_gc_unexpected_keys", map[string]string{"scope": "gone"}); s.Found {
		t.Fatalf("a namespace gone from the bucket still reports %v unexpected keys", s.Value)
	}
	if s := metricstest.Read(t, reg, "cacheservice_gc_stored_bytes", labels); s.Found {
		t.Fatalf("a namespace gone from the bucket still reports %v bytes", s.Value)
	}
	if s := metricstest.Read(t, reg, "cacheservice_gc_stored_bytes", map[string]string{"scope": "team", "kind": "blob"}); !s.Found {
		t.Fatal("a namespace still there lost its gauge")
	}
}

// A namespace whose index alone is over its max size has no root expired:
// expiring roots can't shrink the index, and searching for how many would
// expire every one for nothing. It is reported, and the rest of the run
// (unreferenced objects) goes on.
func TestMaxSizeUnderTheIndexExpiresNothing(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	orphan := e.view("team").Put(layer("orphan", 100))
	e.clock.Add(3 * time.Hour)
	e.setLimits("team", Limits{MaxBytes: 10})
	reps, err := e.collector(nil).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "index alone") {
		t.Fatalf("run: %v", err)
	}
	r := reportOf(reps, "team")
	if r.SizeRoots != 0 || len(e.index("team").Roots) != 1 {
		t.Fatalf("expired %d roots under a max size the index alone is over", r.SizeRoots)
	}
	if _, ok := e.view("team").Get(orphan); ok || r.Deleted["blob"] != 1 {
		t.Fatalf("the sweep didn't go on: %+v", r)
	}
}

// A bundle the index names that is gone from the bucket stops the sweep
// even when an earlier run cached its blobs: the check is against the run's
// own listing, not a read that the cache skips.
func TestMissingBundleStopsSweepWarmCache(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	rec := e.export("team", "a", buildX("x"), 2)
	c := e.collector(nil)
	e.run(c) // caches the bundle's blobs
	orphan := e.view("team").Put(layer("orphan", 100))
	if err := e.objs.Delete(ctx, prefix+"/ns/team/"+metastore.BundleRelKey(rec.BundleID)); err != nil {
		t.Fatal(err)
	}
	e.clock.Add(1000 * time.Hour)
	reps, err := c.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "not in the listing") {
		t.Fatalf("run: %v", err)
	}
	if r := reportOf(reps, "team"); len(r.Deleted) != 0 {
		t.Fatalf("deleted %v while a named bundle was missing", r.Deleted)
	}
	if _, ok := e.view("team").Get(orphan); !ok {
		t.Fatal("swept with a named bundle missing")
	}
}

// Only a lifecycle rule within a namespace's staging prefix counts as
// covering it. A broader one, over the whole prefix or namespace, doesn't
// (GC still expires the staged uploads itself), and is reported: it
// expires the namespace's live data by age, which the index still names.
func TestBroadLifecycleRule(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	staged := e.view("team").PutStaged(blobstore.NewStage(), layer("staged", 100))
	e.clock.Add(3 * time.Hour)
	e.bucket.SetRules([]string{prefix + "/"}, nil)
	r := reportOf(e.run(e.collector(nil)), "team")
	if r.StagingRule != "missing" || r.Deleted["staging"] != 1 || slices.Contains(e.view("team").Staged(), staged) {
		t.Fatalf("a broad rule counted as staging's: %+v", r)
	}
	if !slices.Equal(r.LiveRules, []string{prefix + "/"}) {
		t.Fatalf("live rules %v", r.LiveRules)
	}
	e.bucket.SetRules([]string{prefix + "/ns/team/staging/"}, nil)
	if r := reportOf(e.run(e.collector(nil)), "team"); r.StagingRule != "present" || len(r.LiveRules) != 0 {
		t.Fatalf("the staging rule: %+v", r)
	}
}

// A dry run waits for a real run of the same collector under way, rather
// than report a state half way through it.
func TestDryRunWaitsForRun(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	e.view("team").Put(layer("orphan", 100))
	e.clock.Add(3 * time.Hour)
	c := e.collector(nil)
	inDelete, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	e.bucket.BeforeDelete(func([]string) {
		once.Do(func() { close(inDelete) })
		<-release
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := c.Run(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	<-inDelete
	dry := make(chan struct{})
	go func() {
		defer close(dry)
		if _, err := c.DryRunner().Run(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-dry:
		t.Fatal("the dry run ran during the real one")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-done
	<-dry
}
