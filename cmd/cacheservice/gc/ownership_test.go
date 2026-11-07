package gc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
)

// state is the whole bucket, as a listing and the upload listing show it:
// every key with its size and time, and every upload.
func (e *env) state() []string {
	e.t.Helper()
	var out []string
	for _, o := range e.bucket.all() {
		out = append(out, fmt.Sprintf("%s %d %d", o.Key, o.Size, o.Modified.UnixNano()))
	}
	for _, u := range e.bucket.Uploads() {
		out = append(out, "upload "+u.Key+" "+u.ID)
	}
	slices.Sort(out)
	return out
}

func (e *env) put(key, body string) {
	e.t.Helper()
	if _, err := e.objs.Put(context.Background(), key, []byte(body), metastore.Condition{}); err != nil {
		e.t.Fatal(err)
	}
}

// collectable is a namespace with something every step of a run would
// delete or abort: expired entries' objects (after the max age), a stale
// staged upload and a stale incomplete multipart upload.
func (e *env) collectable(ns string) {
	e.t.Helper()
	e.export(ns, "c-"+ns, buildX(ns), 2)
	e.view(ns).PutStaged(blobstore.NewStage(), layer(ns+"staged", 100))
	e.bucket.AddUpload(Upload{Key: uploadKey(ns, ns+"-promotion"), ID: "u-" + ns, Initiated: e.clock.Now()})
	e.setLimits(ns, Limits{MaxAge: time.Minute})
}

func sameState(t *testing.T, before, after []string) {
	t.Helper()
	if !slices.Equal(before, after) {
		t.Fatalf("the bucket changed:\nbefore:\n  %s\nafter:\n  %s", strings.Join(before, "\n  "), strings.Join(after, "\n  "))
	}
}

// Under the marker, a run collects as it did before ns/: the expired
// entries' objects, a stale staged upload and a stale multipart upload, all
// at the new paths, and reports no unexpected keys.
func TestCollectsUnderMarker(t *testing.T) {
	e := newEnv(t)
	e.collectable("team")
	e.clock.Add(24 * time.Hour)
	reg := prometheus.NewRegistry()
	c := e.collector(func(cfg *Config) { cfg.Metrics = metrics.New(reg) })
	r := reportOf(e.run(c), "team")
	if r.Expired != 2 || r.Deleted["staging"] != 1 || r.Deleted["upload"] != 1 || r.UnexpectedCount != 0 {
		t.Fatalf("first run: %+v", r)
	}
	r = reportOf(e.run(c), "team")
	if n := r.Deleted["blob"] + r.Deleted["bundle"] + r.Deleted["resolution"]; n != 4 {
		t.Fatalf("second run deleted %v", r.Deleted)
	}
	for _, ns := range []string{"team", RootScope} {
		if s := metricstest.Read(t, reg, "cacheservice_gc_unexpected_keys", map[string]string{"scope": ns}); !s.Found || s.Value != 0 {
			t.Fatalf("%s unexpected keys: %+v", ns, s)
		}
	}
	// Left: the index, the epoch, and the marker.
	var keys []string
	for _, o := range e.bucket.all() {
		keys = append(keys, o.Key)
	}
	if want := []string{"cache/cacheservice.json", "cache/ns/team/gc-epoch.json", "cache/ns/team/index.json"}; !slices.Equal(keys, want) {
		t.Fatalf("keys %v, want %v", keys, want)
	}
}

// Without a marker of this layout, GC does nothing at all: no listing of
// namespaces, no index write, no delete or abort. It says why.
func TestNoMarkerDoesNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		marker string // "" deletes it
		reason string
	}{
		{name: "no marker", reason: metrics.GCRefusedNoMarker},
		{name: "another layout", marker: `{"layout":3}`, reason: metrics.GCRefusedOtherLayout},
		{name: "not JSON", marker: `{`, reason: metrics.GCRefusedMarkerUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.collectable("team")
			e.clock.Add(24 * time.Hour)
			if tc.marker == "" {
				if err := e.objs.Delete(context.Background(), layout.MarkerKey(prefix)); err != nil {
					t.Fatal(err)
				}
			} else {
				e.put(layout.MarkerKey(prefix), tc.marker)
			}
			before := e.state()
			reg := prometheus.NewRegistry()
			m := metrics.New(reg)
			for _, dry := range []bool{true, false} {
				c := e.collector(func(cfg *Config) { cfg.DryRun = dry; cfg.Metrics = m })
				reps, err := c.Run(context.Background())
				if !errors.Is(err, ErrNotOwned) || len(reps) != 0 {
					t.Fatalf("dry run %v: %v, %v; want ErrNotOwned and no reports", dry, reps, err)
				}
				if _, err := c.RunNamespace(context.Background(), "team"); !errors.Is(err, ErrNotOwned) {
					t.Fatalf("RunNamespace: %v", err)
				}
			}
			sameState(t, before, e.state())
			if s := metricstest.Read(t, reg, "cacheservice_gc_refused_total", map[string]string{"reason": tc.reason}); s.Value != 4 {
				t.Fatalf("refused %s: %+v", tc.reason, s)
			}
		})
	}
}

// A key of no known shape inside a namespace stops that namespace's
// collection for the run: nothing in it is deleted or aborted, and its index
// isn't written either (no expiry, no stamping). Other namespaces are still
// collected. The keys are logged and reported (at most 10) and counted in
// the gauge. A dry run reports the same.
func TestUnexpectedKeyInNamespace(t *testing.T) {
	stage := blobstore.NewStage()
	hex := digest.FromString("x").Encoded()
	for _, bad := range []string{
		"notes.txt",
		"index.json.bak",
		"blobs/sha256/" + hex[:63],
		"blobs/sha256/" + strings.ToUpper(hex),
		"blobs/md5/d41d8cd98f00b204e9800998ecf8427e",
		"bundles/x.json",
		"staging/" + string(stage) + "/notes",
		"staging/short/sha256/" + hex,
		"conditional-write-check/0",
	} {
		t.Run(bad, func(t *testing.T) {
			e := newEnv(t)
			e.collectable("team")
			e.collectable("other")
			e.put(nsPrefix("team")+bad, "foreign")
			e.clock.Add(24 * time.Hour)
			index := func(ns string) string {
				b, _, err := e.objs.Get(context.Background(), nsPrefix(ns)+layout.IndexName)
				if err != nil {
					t.Fatal(err)
				}
				return string(b)
			}
			teamIndex := index("team")
			teamKeys := func() []string {
				var out []string
				for _, s := range e.state() {
					if strings.Contains(s, "/ns/team/") {
						out = append(out, s)
					}
				}
				return out
			}
			before := teamKeys()
			reg := prometheus.NewRegistry()
			m := metrics.New(reg)
			for _, dry := range []bool{true, false} {
				c := e.collector(func(cfg *Config) { cfg.DryRun = dry; cfg.Metrics = m })
				reps, err := c.Run(context.Background())
				if err == nil || !strings.Contains(err.Error(), "no known shape") {
					t.Fatalf("dry run %v: %v; want the violation reported", dry, err)
				}
				r := reportOf(reps, "team")
				if r.UnexpectedCount != 1 || !slices.Equal(r.Unexpected, []string{nsPrefix("team") + bad}) ||
					r.Expired != 0 || len(r.Deleted) != 0 || len(r.WouldDelete) != 0 {
					t.Fatalf("dry run %v, team: %+v", dry, r)
				}
				o := reportOf(reps, "other")
				if dry && (o.Expired != 2 || len(o.WouldDelete) != 2) {
					t.Fatalf("dry run, other: %+v", o)
				}
				if !dry && (o.Expired != 2 || o.Deleted["staging"] != 1 || o.Deleted["upload"] != 1) {
					t.Fatalf("other: %+v", o)
				}
			}
			sameState(t, before, teamKeys())
			if index("team") != teamIndex {
				t.Fatal("the namespace's index was written")
			}
			if s := metricstest.Read(t, reg, "cacheservice_gc_unexpected_keys", map[string]string{"scope": "team"}); s.Value != 1 {
				t.Fatalf("team gauge: %+v", s)
			}
			if s := metricstest.Read(t, reg, "cacheservice_gc_unexpected_keys", map[string]string{"scope": "other"}); !s.Found || s.Value != 0 {
				t.Fatalf("other gauge: %+v", s)
			}
			// Once the key is gone (removed by whoever wrote it), the
			// namespace is collected again.
			if err := e.objs.Delete(context.Background(), nsPrefix("team")+bad); err != nil {
				t.Fatal(err)
			}
			if r := reportOf(e.run(e.collector(nil)), "team"); r.Expired != 2 || r.Deleted["staging"] != 1 || r.Deleted["upload"] != 1 {
				t.Fatalf("after the key is removed: %+v", r)
			}
		})
	}
}

// At most 10 unexpected keys are reported, and all are counted.
func TestUnexpectedKeysBounded(t *testing.T) {
	e := newEnv(t)
	e.collectable("team")
	for i := range 25 {
		e.put(fmt.Sprintf("%sjunk/%02d", nsPrefix("team"), i), "x")
	}
	reps, _ := e.collector(nil).Run(context.Background())
	if r := reportOf(reps, "team"); r.UnexpectedCount != 25 || len(r.Unexpected) != MaxUnexpectedLogged {
		t.Fatalf("report: %d keys, %d listed", r.UnexpectedCount, len(r.Unexpected))
	}
}

// A key of no known shape outside the namespaces stops the whole run:
// nothing in any namespace is deleted, aborted or written. That includes the
// layout before ns/ (default/index.json, blobs/...): the service's own data,
// once, but not of this layout, which has no migration (README, "Changing
// layouts").
func TestUnexpectedKeyOutsideNamespaces(t *testing.T) {
	hex := digest.FromString("x").Encoded()
	for _, bad := range []string{
		"cache/notes.txt",
		"cache/cacheservice.json.old",
		"cache/blobs/sha256/" + hex,
		"cache/default/index.json",
		"cache/ns/loose.json",
		"cache/ns/-bad/index.json",
		"cache/ns/a..b/index.json",
	} {
		t.Run(bad, func(t *testing.T) {
			e := newEnv(t)
			e.collectable("team")
			e.put(bad, "foreign")
			e.clock.Add(24 * time.Hour)
			before := e.state()
			reg := prometheus.NewRegistry()
			m := metrics.New(reg)
			for _, dry := range []bool{true, false} {
				c := e.collector(func(cfg *Config) { cfg.DryRun = dry; cfg.Metrics = m })
				reps, err := c.Run(context.Background())
				if !errors.Is(err, ErrNotOwned) {
					t.Fatalf("dry run %v: %v", dry, err)
				}
				if len(reps) != 1 || reps[0].Namespace != RootScope || !slices.Equal(reps[0].Unexpected, []string{bad}) {
					t.Fatalf("dry run %v: reports %+v", dry, reps)
				}
				if _, err := c.RunNamespace(context.Background(), "team"); !errors.Is(err, ErrNotOwned) {
					t.Fatalf("RunNamespace: %v", err)
				}
			}
			sameState(t, before, e.state())
			if s := metricstest.Read(t, reg, "cacheservice_gc_unexpected_keys", map[string]string{"scope": RootScope}); s.Value != 1 {
				t.Fatalf("gauge: %+v", s)
			}
			if s := metricstest.Read(t, reg, "cacheservice_gc_refused_total", map[string]string{"reason": metrics.GCRefusedRootViolation}); s.Value != 4 {
				t.Fatalf("refused: %+v", s)
			}
		})
	}
}

// An incomplete multipart upload of no known shape is never aborted, at any
// age. In a namespace, it stops that namespace's collection, as an
// unexpected key does; outside the namespaces, the whole run's.
func TestUnexpectedUploadNeverAborted(t *testing.T) {
	stage := blobstore.NewStage()
	d := digest.FromString("y")
	for _, tc := range []struct {
		key  string
		root bool
	}{
		{key: nsPrefix("team") + layout.StagingRel(string(stage), d)},
		{key: nsPrefix("team") + layout.IndexName},
		{key: nsPrefix("team") + "backups/db.tar"},
		{key: "cache/backups/db.tar", root: true},
		{key: "cache/" + layout.BlobRel(d), root: true},
		{key: "cache/ns/-x/" + layout.BlobRel(d), root: true},
	} {
		t.Run(tc.key, func(t *testing.T) {
			e := newEnv(t)
			e.collectable("team")
			e.collectable("other")
			e.bucket.AddUpload(Upload{Key: tc.key, ID: "foreign", Initiated: e.clock.Now()})
			e.clock.Add(1000 * time.Hour)
			reps, err := e.collector(nil).Run(context.Background())
			if err == nil {
				t.Fatal("no error")
			}
			if !slices.ContainsFunc(e.bucket.Uploads(), func(u Upload) bool { return u.ID == "foreign" }) {
				t.Fatal("the foreign upload was aborted")
			}
			if tc.root {
				if len(reps) != 1 || reps[0].Namespace != RootScope || reps[0].Unexpected[0] != "upload:"+tc.key {
					t.Fatalf("reports %+v", reps)
				}
				if len(e.bucket.Uploads()) != 3 {
					t.Fatalf("uploads %v", e.bucket.Uploads())
				}
				return
			}
			r := reportOf(reps, "team")
			if r.UnexpectedCount != 1 || r.Unexpected[0] != "upload:"+tc.key || len(r.Deleted) != 0 {
				t.Fatalf("team: %+v", r)
			}
			if o := reportOf(reps, "other"); o.Deleted["upload"] != 1 {
				t.Fatalf("other: %+v", o)
			}
			if !slices.ContainsFunc(e.bucket.Uploads(), func(u Upload) bool { return u.ID == "u-team" }) {
				t.Fatal("an upload of the namespace with a violation was aborted")
			}
		})
	}
}

// A prefix under ns/ without an index is reported and left alone: nothing in
// it is deleted or aborted, staged uploads and multipart uploads included.
// That changed with ns/ (before, its staged and multipart uploads were
// collected): a namespace is the service's only once it has an index, so a
// tree under ns/ that isn't the service's is never touched. A staged upload
// left by an export that never recorded waits for the namespace's first
// index, or the lifecycle rule. Its uploads in an otherwise empty
// namespace (no objects at all) are left too.
func TestNamespaceWithoutIndexLeftAlone(t *testing.T) {
	e := newEnv(t)
	e.view("fresh").Put(layer("first", 100))
	e.view("fresh").PutStaged(blobstore.NewStage(), layer("staged", 100))
	e.bucket.AddUpload(Upload{Key: uploadKey("fresh", "p"), ID: "1", Initiated: e.clock.Now()})
	e.bucket.AddUpload(Upload{Key: uploadKey("lonely", "p"), ID: "2", Initiated: e.clock.Now()})
	e.collectable("team")
	e.clock.Add(1000 * time.Hour)
	fresh := func() []string {
		var out []string
		for _, s := range e.state() {
			if strings.Contains(s, "/ns/fresh/") || strings.Contains(s, "/ns/lonely/") {
				out = append(out, s)
			}
		}
		return out
	}
	before := fresh()
	reps := e.run(e.collector(nil))
	if r := reportOf(reps, "fresh"); r.Skipped != "no index" || len(r.Deleted) != 0 {
		t.Fatalf("fresh: %+v", r)
	}
	if r := reportOf(reps, "team"); r.Deleted["upload"] != 1 {
		t.Fatalf("team: %+v", r)
	}
	sameState(t, before, fresh())
	if names, _ := e.collector(nil).Namespaces(context.Background()); !slices.Equal(names, []string{"fresh", "team"}) {
		t.Fatalf("namespaces %v", names)
	}
}

// The max size counts only the namespace's live data, of known shapes: its
// index, bundles, resolutions and blobs. Staged uploads, the epoch object and
// probes don't count; and with a key of no known shape the namespace isn't
// collected at all, so no root is expired for size, however large it is.
func TestMaxSizeIgnoresUnknownKeys(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	live := reportOf(e.run(e.collector(nil)), "team").LiveBytes
	e.view("team").PutStaged(blobstore.NewStage(), layer("staged", 1_000_000))
	e.put(nsPrefix("team")+layout.ProbeRel(1), strings.Repeat("p", 1_000_000))
	e.setLimits("team", Limits{MaxBytes: live})
	r := reportOf(e.run(e.collector(nil)), "team")
	if r.LiveBytes != live || r.SizeRoots != 0 {
		t.Fatalf("live %d (want %d), size roots %d", r.LiveBytes, live, r.SizeRoots)
	}
	e.put(nsPrefix("team")+"huge.bin", strings.Repeat("h", 10_000_000))
	e.setLimits("team", Limits{MaxBytes: 1})
	reps, _ := e.collector(nil).Run(context.Background())
	if r := reportOf(reps, "team"); r.SizeRoots != 0 || r.UnexpectedCount != 1 || len(e.index("team").Roots) != 1 {
		t.Fatalf("with an unknown key: %+v", r)
	}
}

// Without the multipart upload listing, GC can't see an upload of another
// writer's, so the run does nothing. Before ns/, the namespaces were still
// collected but for their uploads; now ownership comes first.
func TestUploadListingFailureDoesNothing(t *testing.T) {
	e := newEnv(t)
	e.collectable("team")
	e.clock.Add(24 * time.Hour)
	e.bucket.FailUploadListings(errors.New("injected"))
	before := e.state()
	reps, err := e.collector(nil).Run(context.Background())
	if !errors.Is(err, ErrNotOwned) || !strings.Contains(err.Error(), "list multipart uploads") || len(reps) != 0 {
		t.Fatalf("run: %v, %v", reps, err)
	}
	sameState(t, before, e.state())
	e.bucket.FailUploadListings(nil)
	if r := reportOf(e.run(e.collector(nil)), "team"); r.Deleted["upload"] != 1 {
		t.Fatalf("after: %+v", r)
	}
}

// listCounting counts List calls.
type listCounting struct {
	Bucket
	lists int
}

func (b *listCounting) List(ctx context.Context, p string, fn func([]Object) error) error {
	b.lists++
	return b.Bucket.List(ctx, p, fn)
}

// A prefix holding another application's tree of many top-level prefixes
// costs a bounded number of listings: once 10 keys are known, each further
// unexpected prefix is counted as one, without listing it.
func TestUnexpectedPrefixesSampledBounded(t *testing.T) {
	e := newEnv(t)
	e.collectable("team")
	for i := range 40 {
		e.put(fmt.Sprintf("cache/app-%02d/file", i), "x")
	}
	b := &listCounting{Bucket: e.bucket}
	c := e.collector(func(cfg *Config) { cfg.Bucket = b })
	reps, err := c.Run(context.Background())
	if !errors.Is(err, ErrNotOwned) || reps[0].UnexpectedCount != 40 || len(reps[0].Unexpected) != MaxUnexpectedLogged {
		t.Fatalf("%+v, %v", reps, err)
	}
	if b.lists != MaxUnexpectedLogged {
		t.Fatalf("%d listings, want %d", b.lists, MaxUnexpectedLogged)
	}
}

// A namespace skipped for a violation still reports what it holds of known
// shapes: its gc_stored_bytes don't freeze at the last clean run's.
func TestSkippedNamespaceGaugesCurrent(t *testing.T) {
	e := newEnv(t)
	e.collectable("team")
	reg := prometheus.NewRegistry()
	c := e.collector(func(cfg *Config) { cfg.Metrics = metrics.New(reg) })
	e.run(c)
	labels := map[string]string{"scope": "team", "kind": "blob"}
	first := metricstest.Read(t, reg, "cacheservice_gc_stored_bytes", labels).Value
	e.put(nsPrefix("team")+"notes.txt", "x")
	e.view("team").Put(layer("more", 5000))
	_, _ = c.Run(context.Background())
	if got := metricstest.Read(t, reg, "cacheservice_gc_stored_bytes", labels).Value; got != first+5000 {
		t.Fatalf("stored blob bytes %v, want %v", got, first+5000)
	}
}

// listHook runs before each List.
type listHook struct {
	Bucket
	before func(prefix string)
}

func (b *listHook) List(ctx context.Context, p string, fn func([]Object) error) error {
	if b.before != nil {
		b.before(p)
	}
	return b.Bucket.List(ctx, p, fn)
}

// A namespace's first index written between GC's read of it (none, so an
// empty snapshot) and its listing (which finds it) is left alone for the
// run: the empty snapshot names nothing the new index does, so its objects,
// old enough to be candidates, would be reported (dry run) or claimed
// against it. The next run collects the namespace.
func TestIndexAppearingDuringRunLeftAlone(t *testing.T) {
	e := newEnv(t)
	e.export("team", "a", buildX("x"), 2)
	ctx := context.Background()
	key := nsPrefix("team") + layout.IndexName
	body, _, err := e.objs.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.objs.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	e.clock.Add(24 * time.Hour)
	for _, dry := range []bool{true, false} {
		restored := false
		b := &listHook{Bucket: e.bucket, before: func(p string) {
			if p == nsPrefix("team") && !restored {
				restored = true
				e.put(key, string(body))
			}
		}}
		c := e.collector(func(cfg *Config) { cfg.Bucket = b; cfg.DryRun = dry })
		r := reportOf(e.run(c), "team")
		if r.Skipped != "the index appeared during the run" || len(r.WouldDelete) != 0 || len(r.Deleted) != 0 {
			t.Fatalf("dry run %v: %+v", dry, r)
		}
		if !dry {
			break
		}
		if err := e.objs.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	e.checkConsistent("team")
	if r := reportOf(e.run(e.collector(nil)), "team"); r.Skipped != "" {
		t.Fatalf("next run: %+v", r)
	}
}

// A namespace whose only content is a multipart upload of no known shape (no
// object, so no index) is a violation for RunNamespace as for Run: reported,
// counted, an error, and never aborted.
func TestUploadOnlyNamespaceViolationBothEntryPoints(t *testing.T) {
	e := newEnv(t)
	e.collectable("team")
	e.bucket.AddUpload(Upload{Key: nsPrefix("lonely") + "backups/db.tar", ID: "foreign", Initiated: e.clock.Now()})
	e.clock.Add(1000 * time.Hour)
	reg := prometheus.NewRegistry()
	c := e.collector(func(cfg *Config) { cfg.Metrics = metrics.New(reg) })
	reps, err := c.Run(context.Background())
	if r := reportOf(reps, "lonely"); err == nil || r.UnexpectedCount != 1 {
		t.Fatalf("Run: %+v, %v", r, err)
	}
	r, err := c.RunNamespace(context.Background(), "lonely")
	if err == nil || r.UnexpectedCount != 1 || r.Unexpected[0] != "upload:"+nsPrefix("lonely")+"backups/db.tar" {
		t.Fatalf("RunNamespace: %+v, %v", r, err)
	}
	if s := metricstest.Read(t, reg, "cacheservice_gc_unexpected_keys", map[string]string{"scope": "lonely"}); s.Value != 1 {
		t.Fatalf("gauge: %+v", s)
	}
	if !slices.ContainsFunc(e.bucket.Uploads(), func(u Upload) bool { return u.ID == "foreign" }) {
		t.Fatal("the foreign upload was aborted")
	}
}
