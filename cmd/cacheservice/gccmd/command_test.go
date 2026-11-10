package gccmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/cmd/cacheservice/server"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

const (
	bucketName = "bkt"
	prefix     = "cache"
)

// fixture is in-memory stores as the command sees them through its Opener:
// a prefix a writer claimed, with data exported 48 hours ago (by the clock,
// which then catches up with the real time the command uses).
type fixture struct {
	t      *testing.T
	clock  *gc.Now
	blobs  *blobstore.MemStore
	objs   *metastore.MemObjects
	meta   *metastore.Store
	bucket *gc.MemBucket
}

func newFixture(t *testing.T, claimed bool) *fixture {
	t.Helper()
	clock := gc.NewNow(time.Now().Add(-48 * time.Hour).Truncate(time.Second))
	blobs := blobstore.NewMemStore("http://blobs")
	blobs.SetClock(clock.Now)
	objs := metastore.NewMemObjects()
	objs.SetClock(clock.Now)
	meta, err := metastore.New(metastore.Config{Objects: ctxObjects{objs}, Prefix: prefix, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		if _, _, err := ownership.Check(context.Background(), objs, prefix, true, clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	return &fixture{t: t, clock: clock, blobs: blobs, objs: objs, meta: meta,
		bucket: &gc.MemBucket{Prefix: prefix, Blobs: blobs, Objects: objs}}
}

// ctxObjects fails the calls whose context ended, as S3 requests do: the
// in-memory store ignores it, which would hide a finish that was cut off.
type ctxObjects struct{ *metastore.MemObjects }

func (o ctxObjects) Get(ctx context.Context, key string) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	return o.MemObjects.Get(ctx, key)
}

func (o ctxObjects) Put(ctx context.Context, key string, body []byte, cond metastore.Condition) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return o.MemObjects.Put(ctx, key, body, cond)
}

func layer(name string, size int) []byte {
	return []byte(strings.Repeat(name, size/len(name)+1))[:size]
}

// export stores an engine cache's entries, as an export does.
func (f *fixture) export(ns, cacheID string, entries []fakeengine.Entry, roots ...uint64) {
	f.t.Helper()
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
		f.t.Fatal(err)
	}
	v, err := f.blobs.In(ns)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, data := range capt.Blobs {
		if _, ok := v.Get(digest.FromBytes(data)); !ok {
			v.Put(data)
		}
	}
	if _, err := f.meta.Record(context.Background(), ns, capt.Bundle, cacheID, 1); err != nil {
		f.t.Fatal(err)
	}
}

func build(tag string) []fakeengine.Entry {
	return []fakeengine.Entry{
		{Number: 1, Recipe: tag + "-base", Layers: [][]byte{layer(tag+"base", 1000)}},
		{Number: 2, Recipe: tag + "-exec", Deps: []uint64{1}, Layers: [][]byte{layer(tag+"base", 1000), layer(tag+"exec", 2000)}},
	}
}

// seed gives the fixture two namespaces with entries recorded 48 hours ago
// (expired by -max-age=1h), a stale staged upload, and a stale incomplete
// multipart upload; then lets the clock catch up with the real time.
func (f *fixture) seed() {
	f.t.Helper()
	for _, ns := range []string{"team-a", "team-b"} {
		f.export(ns, "c-"+ns, build(ns), 2)
		v, _ := f.blobs.In(ns)
		v.PutStaged(blobstore.NewStage(), layer(ns+"staged", 100))
		f.bucket.AddUpload(gc.Upload{Key: layout.NamespacesPrefix(prefix) + ns + "/" + layout.BlobRel(digest.FromString(ns+"-up")), ID: "u-" + ns, Initiated: f.clock.Now()})
	}
	f.clock.Add(48 * time.Hour)
}

// put writes a key straight into the bucket's object store.
func (f *fixture) put(key, body string) {
	f.t.Helper()
	if _, err := f.objs.Put(context.Background(), key, []byte(body), metastore.Condition{}); err != nil {
		f.t.Fatal(err)
	}
}

// state is every object with its size and time, every index's body, and every
// upload: all a run could change.
func (f *fixture) state() []string {
	f.t.Helper()
	var out []string
	if err := f.bucket.List(context.Background(), "", func(objs []gc.Object) error {
		for _, o := range objs {
			out = append(out, fmt.Sprintf("%s %d %d", o.Key, o.Size, o.Modified.UnixNano()))
			if strings.HasSuffix(o.Key, "/"+layout.IndexName) {
				b, _, err := f.objs.Get(context.Background(), o.Key)
				if err != nil {
					f.t.Fatal(err)
				}
				out = append(out, "body "+o.Key+" "+string(b))
			}
		}
		return nil
	}); err != nil {
		f.t.Fatal(err)
	}
	for _, u := range f.bucket.Uploads() {
		out = append(out, "upload "+u.Key+" "+u.ID)
	}
	slices.Sort(out)
	return out
}

func (f *fixture) opener(bucket gc.Bucket) Opener {
	if bucket == nil {
		bucket = f.bucket
	}
	return func(context.Context, blobstore.S3Config) (Stores, error) {
		return Stores{Bucket: bucket, Meta: f.meta}, nil
	}
}

// opened fails the test if the command opens the stores: it must have
// refused before.
func opened(t *testing.T) Opener {
	return func(context.Context, blobstore.S3Config) (Stores, error) {
		t.Error("the stores were opened, but the command should have refused before reading anything")
		return Stores{}, errors.New("opened")
	}
}

func setEnv(t *testing.T) {
	t.Helper()
	t.Setenv(blobstore.EnvBucket, bucketName)
	t.Setenv(blobstore.EnvPrefix, prefix)
	t.Setenv(blobstore.EnvRegion, "us-east-1")
}

// run runs the command, and returns its exit code, stdout and stderr.
func run(t *testing.T, open Opener, args ...string) (int, string, string) {
	t.Helper()
	setEnv(t)
	return runAsIs(t, open, args...)
}

// runAsIs is run with the environment the test already set.
func runAsIs(t *testing.T, open Opener, args ...string) (int, string, string) {
	t.Helper()
	return runCtx(t, context.Background(), open, args...)
}

// runCtx is runAsIs with the context Main runs under.
func runCtx(t *testing.T, ctx context.Context, open Opener, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := Main(ctx, args, &out, &errOut, open)
	return code, out.String(), errOut.String()
}

// reports parses stdout, which must be one JSON array of reports and
// nothing else.
func reports(t *testing.T, stdout string) []gc.Report {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	var reps []gc.Report
	if err := dec.Decode(&reps); err != nil {
		t.Fatalf("stdout isn't a JSON array of reports: %v\n%s", err, stdout)
	}
	if reps == nil {
		t.Fatalf("stdout is null, not an array:\n%s", stdout)
	}
	if _, err := dec.Token(); err != io.EOF {
		t.Fatalf("stdout holds more than the JSON array: %v\n%s", err, stdout)
	}
	return reps
}

func report(reps []gc.Report, ns string) gc.Report {
	for _, r := range reps {
		if r.Namespace == ns {
			return r
		}
	}
	return gc.Report{}
}

func confirmArg() string { return "-confirm=" + bucketName + "/" + prefix }

// limits are the explicit limits of a confirmed run that expires what seed
// recorded.
var limits = []string{"-max-age=1h", "-max-bytes=0"}

func same(t *testing.T, before, after []string) {
	t.Helper()
	if !slices.Equal(before, after) {
		t.Fatalf("the bucket changed:\nbefore:\n  %s\nafter:\n  %s", strings.Join(before, "\n  "), strings.Join(after, "\n  "))
	}
}

// Without -confirm, a run reports what it would expire and delete, and
// changes nothing: no object, no index, no upload.
func TestDryRunChangesNothing(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	before := f.state()
	args := slices.Concat([]string{}, limits)
	code, out, errOut := run(t, f.opener(nil), args...)
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	same(t, before, f.state())
	if f.bucket.Deletes() != 0 {
		t.Fatalf("a dry run called Delete %d times", f.bucket.Deletes())
	}
	reps := reports(t, out)
	if len(reps) != 2 {
		t.Fatalf("reports: %+v", reps)
	}
	for _, r := range reps {
		if !r.DryRun || r.Expired != 2 || len(r.WouldDelete) == 0 || len(r.Deleted) != 0 || len(r.Errors) != 0 {
			t.Fatalf("report of %s: %+v", r.Namespace, r)
		}
	}
	if !strings.Contains(errOut, "-confirm="+bucketName+"/"+prefix) || strings.Contains(errOut, "without limits") {
		t.Fatalf("stderr should say how to collect, and not that limits are missing:\n%s", errOut)
	}
}

// A dry run without limits works, says so on stderr, and keeps stdout the
// reports alone.
func TestDryRunWithoutLimits(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	before := f.state()
	for _, tc := range []struct {
		name    string
		args    []string
		missing string
	}{
		{"none", nil, "-max-age, -max-bytes"},
		{"only max-age", []string{"-max-age=1h"}, "-max-bytes"},
		{"only max-bytes", []string{"-max-bytes=0"}, "-max-age"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := run(t, f.opener(nil), tc.args...)
			if code != ExitOK {
				t.Fatalf("exit %d: %s", code, errOut)
			}
			reports(t, out)
			if !strings.Contains(errOut, "dry run without limits") || !strings.Contains(errOut, "(not given: "+tc.missing+")") {
				t.Fatalf("stderr:\n%s", errOut)
			}
			same(t, before, f.state())
		})
	}
}

// A confirmed run does what POST /debug/gc (Service.CollectGarbage) does on
// the same bucket contents: the same reports, the same keys left, run after
// run.
func TestConfirmedRunMatchesService(t *testing.T) {
	cmdF, svcF := newFixture(t, true), newFixture(t, true)
	cmdF.seed()
	svcF.seed()

	svc := server.New(slog.New(slog.DiscardHandler), "")
	lim, err := gc.ParseLimits(nil, gc.Limits{MaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.UseLimits("", lim.Defaults()); err != nil {
		t.Fatal(err)
	}
	if err := svc.EnableGC(context.Background(), server.GCConfig{Mode: server.GCModeBatch, Collector: gc.Config{Bucket: svcF.bucket, Meta: svcF.meta}}); err != nil {
		t.Fatal(err)
	}

	for round := 1; round <= 3; round++ {
		code, out, errOut := run(t, cmdF.opener(nil), slices.Concat([]string{confirmArg()}, limits)...)
		if code != ExitOK {
			t.Fatalf("round %d: exit %d: %s", round, code, errOut)
		}
		got := reports(t, out)
		want, err := svc.CollectGarbage(context.Background(), false)
		if err != nil {
			t.Fatal(err)
		}
		gj, _ := json.Marshal(got)
		wj, _ := json.Marshal(want)
		if string(gj) != string(wj) {
			t.Fatalf("round %d: the command reported\n%s\nthe service\n%s", round, gj, wj)
		}
		if !slices.Equal(keysOf(t, cmdF), keysOf(t, svcF)) {
			t.Fatalf("round %d: the keys left differ:\ncommand: %v\nservice: %v", round, keysOf(t, cmdF), keysOf(t, svcF))
		}
	}
	// It collected: the first round expired, the second deleted.
	for _, k := range keysOf(t, cmdF) {
		if strings.Contains(k, "/blobs/") || strings.Contains(k, "/bundles/") || strings.Contains(k, "/staging/") {
			t.Fatalf("the expired namespaces' objects are not gone: %s", k)
		}
	}
	if len(keysOf(t, cmdF)) == 0 {
		t.Fatal("the bucket is empty: the test proves nothing")
	}
}

func keysOf(t *testing.T, f *fixture) []string {
	t.Helper()
	var out []string
	if err := f.bucket.List(context.Background(), "", func(objs []gc.Object) error {
		for _, o := range objs {
			out = append(out, o.Key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// A -confirm that isn't the bucket and prefix of the settings refuses before
// anything is read, so a confirmation for one prefix can't collect another.
func TestConfirmMismatchExits2(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	before := f.state()
	for _, confirm := range []string{
		"bkt/other",
		"other/cache",
		"cache",
		"bkt/",
		"bkt/cache/ns",
		"bkt/cache/",
		"BKT/cache",
		"bkt/cache ",
		"bkt:cache",
	} {
		t.Run(confirm, func(t *testing.T) {
			code, out, errOut := run(t, opened(t), slices.Concat([]string{"-confirm=" + confirm}, limits)...)
			if code != ExitUsage {
				t.Fatalf("exit %d, want 2", code)
			}
			if out != "" || !strings.Contains(errOut, "doesn't match") || !strings.Contains(errOut, "bkt/cache") {
				t.Fatalf("stdout %q, stderr:\n%s", out, errOut)
			}
		})
	}
	same(t, before, f.state())
	if f.bucket.Deletes() != 0 {
		t.Fatal("a refused run deleted")
	}
}

// A confirmed run needs explicit limits; zeros are explicit.
func TestConfirmedRunNeedsExplicitLimits(t *testing.T) {
	nsFile := filepath.Join(t.TempDir(), "namespaces")
	if err := os.WriteFile(nsFile, []byte("team-a max-age=1h\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		args    []string
		refused string // what the message names as not given
	}{
		{name: "none", refused: "-max-age, -max-bytes"},
		{name: "only max-age", args: []string{"-max-age=1h"}, refused: "-max-bytes"},
		{name: "only max-bytes", args: []string{"-max-bytes=1GiB"}, refused: "-max-age"},
		{name: "only max-age zero", args: []string{"-max-age=0"}, refused: "-max-bytes"},
		{name: "empty namespaces file", args: []string{"-namespaces-file="}, refused: "-max-age, -max-bytes"},
		{name: "explicit zeros", args: []string{"-max-age=0", "-max-bytes=0"}},
		{name: "values", args: []string{"-max-age=720h", "-max-bytes=500GiB"}},
		{name: "namespaces file alone", args: []string{"-namespaces-file=" + nsFile}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, true)
			f.seed()
			before := f.state()
			open := f.opener(nil)
			if tc.refused != "" {
				open = opened(t)
			}
			code, out, errOut := run(t, open, slices.Concat([]string{confirmArg()}, tc.args)...)
			if tc.refused == "" {
				if code != ExitOK {
					t.Fatalf("exit %d: %s", code, errOut)
				}
				reports(t, out)
				return
			}
			if code != ExitUsage || out != "" {
				t.Fatalf("exit %d, stdout %q", code, out)
			}
			// The message names what's missing, and only that.
			if !strings.Contains(errOut, "(not given: "+tc.refused+")") {
				t.Fatalf("stderr should say %q isn't given:\n%s", tc.refused, errOut)
			}
			same(t, before, f.state())
		})
	}
}

// A namespaces file's limits apply per namespace.
func TestNamespacesFileLimits(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	nsFile := filepath.Join(t.TempDir(), "namespaces")
	if err := os.WriteFile(nsFile, []byte("team-a max-age=1h\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := run(t, f.opener(nil), confirmArg(), "-namespaces-file="+nsFile)
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	reps := reports(t, out)
	if report(reps, "team-a").Expired != 2 || report(reps, "team-b").Expired != 0 {
		t.Fatalf("team-a has a max age, team-b none: %+v", reps)
	}
}

// Bad flags exit 2, and read nothing.
func TestBadFlagsExit2(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	for _, args := range [][]string{
		{"-bogus"},
		{"-gc-mode=batch"},    // the service's, not this command's
		{"-gc-interval=1h"},   // ditto
		{"-gc-dry-run"},       // ditto: no -confirm is the dry run
		{"-gc-dry-run=false"}, // ditto
		{"extra"},
		{"-max-age=soon"},
		{"-max-age=100ms"},
		{"-max-age=-1h"},
		{"-max-bytes=lots"},
		{"-gc-grace=notaduration"},
		{"-namespaces-file=" + missing},
		{"-confirm"},
		{"-confirm="},
		{"-confirm=", "-max-age=0", "-max-bytes=0"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			code, out, errOut := run(t, opened(t), args...)
			if code != ExitUsage || out != "" || errOut == "" {
				t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
			}
		})
	}
	t.Run("no bucket", func(t *testing.T) {
		setEnv(t)
		t.Setenv(blobstore.EnvBucket, "")
		var out, errOut bytes.Buffer
		if code := Main(context.Background(), nil, &out, &errOut, opened(t)); code != ExitUsage || !strings.Contains(errOut.String(), blobstore.EnvBucket) {
			t.Fatalf("exit %d: %s", code, errOut.String())
		}
	})
}

// A clean run is 0; the stores that can't be opened, and a namespace that
// reports errors, are 1, a dry run included; the report is still printed.
func TestExitCodeError(t *testing.T) {
	t.Run("stores can't be opened", func(t *testing.T) {
		code, out, errOut := run(t, func(context.Context, blobstore.S3Config) (Stores, error) {
			return Stores{}, errors.New("no route to the bucket")
		})
		if code != ExitError || out != "" || !strings.Contains(errOut, "no route") {
			t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
		}
	})
	t.Run("a delete fails", func(t *testing.T) {
		f := newFixture(t, true)
		f.seed()
		args := slices.Concat([]string{confirmArg()}, limits)
		if code, _, errOut := run(t, f.opener(nil), args...); code != ExitOK {
			t.Fatalf("first run: %d %s", code, errOut)
		}
		for _, k := range keysOf(t, f) {
			if strings.Contains(k, "/ns/team-a/blobs/") {
				f.bucket.FailDeletes(k)
			}
		}
		code, out, errOut := run(t, f.opener(nil), args...)
		if code != ExitError {
			t.Fatalf("exit %d, stderr %q", code, errOut)
		}
		reps := reports(t, out)
		if len(report(reps, "team-a").Errors) == 0 || len(report(reps, "team-b").Errors) != 0 {
			t.Fatalf("errors should be in team-a's report alone: %+v", reps)
		}
		if report(reps, "team-b").Deleted["blob"] == 0 {
			t.Fatalf("team-b is collected despite team-a's failure: %+v", report(reps, "team-b"))
		}
	})
	t.Run("a dry run with errors", func(t *testing.T) {
		f := newFixture(t, true)
		f.seed()
		// A key of no known shape in a namespace stops its collection, with an
		// error in its report.
		f.put(layout.NamespacesPrefix(prefix)+"team-a/notes.txt", "foreign")
		before := f.state()
		code, out, errOut := run(t, f.opener(nil), limits...)
		if code != ExitError {
			t.Fatalf("exit %d, stderr %q", code, errOut)
		}
		reps := reports(t, out)
		if len(report(reps, "team-a").Errors) == 0 || !report(reps, "team-a").DryRun {
			t.Fatalf("report: %+v", reps)
		}
		same(t, before, f.state())
	})
}

// A run that refuses to do anything, because the prefix's ownership can't be
// confirmed, is 3, with a report that says why, dry or confirmed, and
// changes nothing.
func TestExitCodeNotOwned(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(f *fixture)
	}{
		{"no marker", func(f *fixture) { _ = f.objs.Delete(context.Background(), layout.MarkerKey(prefix)) }},
		{"another layout", func(f *fixture) { f.put(layout.MarkerKey(prefix), `{"layout":3}`) }},
		{"marker unreadable", func(f *fixture) { f.put(layout.MarkerKey(prefix), `not json`) }},
		{"keys of no known shape at the root", func(f *fixture) { f.put(prefix+"/notes.txt", "foreign") }},
		{"keys of no known shape under ns/", func(f *fixture) { f.put(prefix+"/ns/loose.json", "foreign") }},
		{"an upload of no known shape at the root", func(f *fixture) {
			f.bucket.AddUpload(gc.Upload{Key: prefix + "/loose", ID: "u", Initiated: f.clock.Now()})
		}},
		{"uploads can't be listed", func(f *fixture) { f.bucket.FailUploadListings(errors.New("not allowed")) }},
	} {
		for _, confirmed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, confirmed %v", tc.name, confirmed), func(t *testing.T) {
				f := newFixture(t, true)
				f.seed()
				tc.setup(f)
				before := f.state()
				args := slices.Clone(limits)
				if confirmed {
					args = append(args, confirmArg())
				}
				code, out, errOut := run(t, f.opener(nil), args...)
				if code != ExitNotOwned {
					t.Fatalf("exit %d, stderr %q", code, errOut)
				}
				reports(t, out) // an array, perhaps empty
				if !strings.Contains(errOut, "ownership") {
					t.Fatalf("stderr: %q", errOut)
				}
				same(t, before, f.state())
				if f.bucket.Deletes() != 0 {
					t.Fatal("a refused run deleted")
				}
			})
		}
	}
	t.Run("no marker: the report is an empty array", func(t *testing.T) {
		f := newFixture(t, false)
		code, out, _ := run(t, f.opener(nil))
		if code != ExitNotOwned || strings.TrimSpace(out) != "[]" {
			t.Fatalf("exit %d, stdout %q", code, out)
		}
	})
}

// A confirmed run never changes what only a dry run should: with
// -confirm and without, the same bucket, a dry run's report says what the
// confirmed run then does.
func TestDryRunReportsWhatConfirmedDoes(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	_, out, _ := run(t, f.opener(nil), limits...)
	dry := reports(t, out)
	_, out, _ = run(t, f.opener(nil), slices.Concat([]string{confirmArg()}, limits)...)
	real := reports(t, out)
	for _, d := range dry {
		r := report(real, d.Namespace)
		if r.Expired != d.Expired || r.SizeRoots != d.SizeRoots || r.Caches != d.Caches {
			t.Fatalf("%s: dry run %+v, run %+v", d.Namespace, d, r)
		}
	}
}

// signalOnDelete sends the process a SIGTERM as the first delete begins, and
// holds that delete until the run's context ended, so the signal is seen
// after the claim and before the objects are gone.
type signalOnDelete struct {
	*gc.MemBucket
	t    *testing.T
	once sync.Once
}

func (b *signalOnDelete) Delete(ctx context.Context, keys []string) ([]string, error) {
	b.once.Do(func() {
		// Two: a second signal doesn't kill the process, or cut the finish off.
		for range 2 {
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				b.t.Error(err)
				return
			}
		}
		select {
		case <-ctx.Done():
			// Let the second signal be received too, before the run ends and
			// the handler is removed.
			time.Sleep(200 * time.Millisecond)
		case <-time.After(10 * time.Second):
			b.t.Error("the SIGTERM didn't end the run's context")
		}
	})
	return b.MemBucket.Delete(ctx, keys)
}

// A SIGTERM (twice) after the run claimed objects ends the run, finishes the claims
// (none stays in progress, which other runs would wait on until stale), and
// exits 1, with the report so far.
func TestSigtermAfterClaimFinishesClaims(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	args := slices.Concat([]string{confirmArg()}, limits)
	// The first run expires the entries; the second claims their objects.
	if code, _, errOut := run(t, f.opener(nil), args...); code != ExitOK {
		t.Fatalf("first run: %d %s", code, errOut)
	}
	b := &signalOnDelete{MemBucket: f.bucket, t: t}

	type result struct {
		code        int
		out, errOut string
	}
	done := make(chan result, 1)
	go func() {
		code, out, errOut := run(t, f.opener(b), args...)
		done <- result{code, out, errOut}
	}()
	var res result
	select {
	case res = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the command didn't end after SIGTERM")
	}
	if res.code != ExitError {
		t.Fatalf("exit %d, want 1; stderr:\n%s", res.code, res.errOut)
	}
	reports(t, res.out)
	if !strings.Contains(res.errOut, "interrupted") {
		t.Fatalf("stderr should say the run was interrupted:\n%s", res.errOut)
	}
	// The claims are finished: the objects were attempted, so they are
	// recorded as deleted in an epoch, and none is still in progress.
	claims := 0
	for _, ns := range []string{"team-a", "team-b"} {
		idx, err := f.meta.Index(context.Background(), ns)
		if err != nil {
			t.Fatal(err)
		}
		if idx.GC == nil {
			continue
		}
		for k, d := range idx.GC.Deletions {
			claims++
			if d.InProgress() {
				t.Fatalf("%s: the claim on %s is still in progress after the run ended", ns, k)
			}
		}
	}
	if claims == 0 {
		t.Fatal("the run claimed nothing, so the test didn't reach the signal after a claim")
	}
	// A later run, with no signal, collects the rest.
	code, out, errOut := run(t, f.opener(nil), args...)
	if code != ExitOK {
		t.Fatalf("run after the signal: %d %s", code, errOut)
	}
	reports(t, out)
}

// An explicitly empty -confirm is a usage error that says so, not a dry run.
func TestEmptyConfirmIsNotADryRun(t *testing.T) {
	code, out, errOut := run(t, opened(t), "-confirm=", "-max-age=0", "-max-bytes=0")
	if code != ExitUsage || out != "" || !strings.Contains(errOut, "empty") || !strings.Contains(errOut, "-confirm="+bucketName+"/"+prefix) {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
	}
}

// Settings the collector refuses exit 2 before the bucket is used: the
// bucket here panics on any call.
func TestNegativeSettingsExit2BeforeAnyRequest(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	before := f.state()
	untouchable := func(context.Context, blobstore.S3Config) (Stores, error) {
		return Stores{Bucket: struct{ gc.Bucket }{}, Meta: f.meta}, nil
	}
	for _, arg := range []string{"-gc-grace=-1s", "-gc-staging-age=-1s", "-gc-max-deletes=-1"} {
		t.Run(arg, func(t *testing.T) {
			for _, confirmed := range []bool{false, true} {
				args := slices.Clone(limits)
				if confirmed {
					args = append(args, confirmArg())
				}
				code, out, errOut := run(t, untouchable, append(args, arg)...)
				if code != ExitUsage || out != "" || !strings.Contains(errOut, "negative") {
					t.Fatalf("confirmed %v: exit %d, stdout %q, stderr %q", confirmed, code, out, errOut)
				}
			}
		})
	}
	same(t, before, f.state())
}

// cancelOnList cancels the run's context at the first listing inside a
// namespace: during its collection, after the survey, before the next
// namespace.
type cancelOnList struct {
	*gc.MemBucket
	cancel context.CancelFunc
	once   sync.Once
}

func (b *cancelOnList) List(ctx context.Context, p string, fn func([]gc.Object) error) error {
	if strings.Contains(p, "/ns/team-") {
		b.once.Do(b.cancel)
	}
	return b.MemBucket.List(ctx, p, fn)
}

// A run whose context ends during its first namespace exits 1, whatever the
// reports say, prints the JSON of what it did, and the namespace it never
// reached is visible: it has no report, and stderr says the run was
// interrupted.
func TestCanceledRunExits1AndSkipIsVisible(t *testing.T) {
	f := newFixture(t, true)
	f.seed()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &cancelOnList{MemBucket: f.bucket, cancel: cancel}
	setEnv(t)
	code, out, errOut := runCtx(t, ctx, f.opener(b), slices.Concat([]string{confirmArg()}, limits)...)
	if code != ExitError {
		t.Fatalf("exit %d, want 1\nstderr:\n%s", code, errOut)
	}
	reps := reports(t, out)
	if len(reps) >= 2 || report(reps, "team-b").Namespace != "" {
		t.Fatalf("team-b should have been skipped, with no report: %+v", reps)
	}
	if !strings.Contains(errOut, "interrupted") {
		t.Fatalf("stderr should say the run was interrupted:\n%s", errOut)
	}
}
