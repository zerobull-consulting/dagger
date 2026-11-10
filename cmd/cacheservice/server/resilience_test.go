package server

// Resilience and failure handling (#12), with fake engines behind the
// engine's own remotecache.Run: a connection lost mid-upload, a service
// restart, a stalled upload, and a bundle near the merge limit. The
// real-engine versions are in cmd/cacheservice/e2e.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// waitUntil calls cond every 10ms until it is true, and fails the test,
// saying what it waited for, after timeout.
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stallStore is a MemStore served over HTTP that, while stalled, holds every
// blob PUT until it is released or its client gives up, as a blob store that
// stops reading would.
type stallStore struct {
	*blobstore.MemStore

	mu      sync.Mutex
	release chan struct{}
	held    atomic.Int64
}

func newStallStore(t *testing.T) (*stallStore, *metastore.Store) {
	t.Helper()
	s := &stallStore{MemStore: blobstore.NewMemStore("")}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		release := s.release
		s.mu.Unlock()
		if r.Method == http.MethodPut && release != nil {
			s.held.Add(1)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		s.MemStore.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	s.SetBaseURL(srv.URL)
	meta, err := metastore.New(metastore.Config{Objects: metastore.NewMemObjects()})
	if err != nil {
		t.Fatal(err)
	}
	return s, meta
}

// Stall holds PUTs from now until the returned function is called.
func (s *stallStore) Stall() (release func()) {
	ch := make(chan struct{})
	s.mu.Lock()
	s.release = ch
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.release = nil
			s.mu.Unlock()
			close(ch)
		})
	}
}

// Held is how many PUTs have been held so far.
func (s *stallStore) Held() int { return int(s.held.Load()) }

func indexEntries(t *testing.T, meta *metastore.Store) int {
	t.Helper()
	idx, err := meta.Index(context.Background(), testScope)
	if err != nil {
		t.Fatal(err)
	}
	return len(idx.Entries)
}

// Case 1 of #12: the connection drops while the engine uploads an export's
// blobs. The engine releases the export's chains when the connection ends,
// nothing of the export is published, and the export is tried again on the
// engine's next connection, where it starts cleanly and is stored. Its roots
// don't count as failed.
func TestConnectionLostMidUpload(t *testing.T) {
	blobs, meta := newStallStore(t)
	h := newExportHarnessWith(t, blobs, meta, func(c *ExportConfig) { c.NoEngineWait = 50 * time.Millisecond }, nil)
	digests := h.build()
	release := blobs.Stall()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 10*time.Second, "the export's upload to stall", func() bool { return blobs.Held() > 0 })
	if h.cache.InFlight() != 1 {
		t.Fatalf("%d exports hold their chains during the upload, want 1", h.cache.InFlight())
	}

	first := h.engine.id
	if err := h.svc.CloseEngine(first); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "the engine to release the export's chains when its connection ended", func() bool {
		return h.cache.InFlight() == 0
	})
	if n := indexEntries(t, meta); n != 0 {
		t.Fatalf("the index has %d entries from an export whose upload never finished", n)
	}
	for _, e := range h.ns().tracker.Entries(h.cache.ID) {
		if e.Failures != 0 {
			t.Fatalf("an export cut short by the connection counted as a failure: %+v", e)
		}
	}

	// The engine reconnects with backoff; the export is tried again on its
	// new connection once the store answers.
	var second *engineConn
	waitUntil(t, 10*time.Second, "the engine to reconnect", func() bool {
		second = h.svc.engineFor(h.ns(), h.cache.ID)
		return second != nil && second.id != first
	})
	release()
	h.waitIndex(10, 11)
	if !hasAll(t, scoped(t, blobs), digests) {
		t.Fatal("the blobs of the stored export aren't all stored")
	}
	waitUntil(t, 5*time.Second, "the export's chains to be released", func() bool { return h.cache.InFlight() == 0 })
	for _, e := range h.ns().tracker.Entries(h.cache.ID) {
		if !e.Stored || e.Failures != 0 {
			t.Fatalf("after the retry: %+v", e)
		}
	}
}

// Case 6 of #12, the service's side: an engine whose upload stalls answers
// nothing until its own timeout (a minute without reads, tested in
// engine/remotecache's TestStalledUploadFails and
// TestStalledUploadFreesItsSlot). The service bounds its wait for uploaded
// too (UploadTimeout, an hour by default): past it, the export fails, nothing
// is published, and the roots are tried again with backoff, here once the
// store reads again.
func TestExportUploadTimeout(t *testing.T) {
	blobs, meta := newStallStore(t)
	h := newExportHarnessWith(t, blobs, meta, func(c *ExportConfig) {
		c.Exporter.UploadTimeout = 200 * time.Millisecond
		c.Tracker.BusyBackoff = 50 * time.Millisecond
	}, nil)
	digests := h.build()
	release := blobs.Stall()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 10*time.Second, "an export to fail on its stalled upload", func() bool {
		for _, e := range h.ns().tracker.Entries(h.cache.ID) {
			if e.Failures > 0 {
				return true
			}
		}
		return false
	})
	if n := indexEntries(t, meta); n != 0 {
		t.Fatalf("the index has %d entries from an export whose upload never finished", n)
	}
	release()
	h.waitIndex(10, 11)
	if !hasAll(t, scoped(t, blobs), digests) {
		t.Fatal("the blobs of the stored export aren't all stored")
	}
}

// restartable is a service that can be stopped and started again on the
// same address and stores, as a service restart: engines reconnect to it
// with their backoff.
type restartable struct {
	t     *testing.T
	blobs blobstore.Store
	meta  *metastore.Store
	srv   *httptest.Server

	mu     sync.Mutex
	svc    *Service
	cancel context.CancelFunc
}

func newRestartable(t *testing.T, blobs blobstore.Store, meta *metastore.Store) *restartable {
	t.Helper()
	r := &restartable{t: t, blobs: blobs, meta: meta}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		svc := r.svc
		r.mu.Unlock()
		if svc == nil {
			http.Error(w, "service stopped", http.StatusServiceUnavailable)
			return
		}
		svc.Handler().ServeHTTP(w, req)
	}))
	t.Cleanup(func() {
		r.stop()
		r.srv.Close()
	})
	return r
}

// start starts a new service with the export and import paths on.
func (r *restartable) start() *Service {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	svc := New(slog.New(slog.DiscardHandler), "")
	if err := svc.EnableExport(ctx, ExportConfig{
		Exporter: exporter.Config{Blobs: r.blobs, Meta: r.meta, Scope: testScope, RetryBackoff: time.Millisecond},
		Tracker:  exporter.TrackerConfig{Settle: time.Minute, MaxDelay: time.Minute},
		Tick:     10 * time.Millisecond,
	}); err != nil {
		r.t.Fatal(err)
	}
	if err := svc.EnableImport(ctx, ImportConfig{Importer: importer.Config{Blobs: r.blobs, Meta: r.meta, Scope: testScope}}); err != nil {
		r.t.Fatal(err)
	}
	r.mu.Lock()
	r.svc, r.cancel = svc, cancel
	r.mu.Unlock()
	return svc
}

// stopDrained stops the running service as SIGTERM does: it drains, with a
// deadline of d, while it still serves, then stops.
func (r *restartable) stopDrained(d time.Duration) DrainReport {
	r.mu.Lock()
	svc := r.svc
	r.mu.Unlock()
	if svc == nil {
		return DrainReport{}
	}
	rep := drainFor(svc, d)
	r.stop()
	return rep
}

// stop stops the running service at once, as a crash or SIGKILL would: it
// refuses requests from now, ends what it runs and drops its engines'
// connections.
func (r *restartable) stop() {
	r.mu.Lock()
	svc, cancel := r.svc, r.cancel
	r.svc, r.cancel = nil, nil
	r.mu.Unlock()
	if svc == nil {
		return
	}
	cancel()
	svc.CloseEngines()
	svc.Wait()
}

// connect runs an engine with cache against the service's address until the
// test ends.
func (r *restartable) connect(cache *fakeengine.Cache) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: cache}, remotecache.Config{EngineVersion: testEngineVersion, URL: r.srv.URL, Token: "engine-token", EngineName: cache.ID})
	}()
	r.t.Cleanup(func() {
		cancel()
		<-done
	})
}

// harness is an exportHarness view of the running service, for its span,
// event and index helpers, with engine cache as its engine.
func (r *restartable) harness(svc *Service, cache *fakeengine.Cache) *exportHarness {
	return &exportHarness{t: r.t, svc: svc, srv: r.srv, cache: cache, blobs: r.blobs, meta: r.meta, ctx: context.Background()}
}

func waitConn(t *testing.T, svc *Service, cacheID string) *engineConn {
	t.Helper()
	var c *engineConn
	waitUntil(t, 10*time.Second, "engine "+cacheID+" to connect", func() bool {
		c = svc.engineFor(mustNamespace(t, svc, testScope), cacheID)
		return c != nil
	})
	return c
}

// Case 3 of #12, the import side: the importer keeps what it merged into
// each engine in memory, so a restarted service merges every stored bundle
// into every connected engine again, once, including entries an engine had
// pruned since (#12's first comment).
//
// This is accepted rather than fixed with a per-cache marker in the store:
// it happens once per service restart, not on every poll, since the new
// importer remembers what it merged again; a merge carries no layer, only
// records and presigned GETs, so the engine downloads nothing unless a
// build reads the entries; and an engine whose GC pruned the entries prunes
// them again at its next GC if they are still unused. On the real-engine
// run, the redo after a restart took a few milliseconds per bundle (see
// cmd/cacheservice/README.md, "Real-engine run"). A marker would have to be
// written on every prune event and read on every sync, a store write on a
// path that has none today.
func TestServiceRestartMergesPrunedEntriesAgainOnce(t *testing.T) {
	blobs, meta := newMemStores(t)
	r := newRestartable(t, blobs, meta)
	svc := r.start()
	a, b := fakeengine.New("cache-a", 1), fakeengine.New("cache-b", 1)
	r.connect(a)
	r.connect(b)
	waitConn(t, svc, a.ID)
	connB := waitConn(t, svc, b.ID)

	h := r.harness(svc, a)
	h.buildRecipes()
	h.exportBuild()
	exec := waitLookup(t, b, "withExec(make)", hasOffer)
	waitImports(t, meta, b.ID, 2)
	merged := b.Merges()

	// B prunes the imported exec: the importer forgets it, and doesn't
	// merge it back.
	b.Remove(exec.Number)
	r.harness(svc, b).postEvent(telemetryattrs.EngineEventPrune,
		`{"drops":[{"resultId":`+strconv.FormatUint(exec.Number, 10)+`,"droppedAtUnixNano":`+strconv.FormatInt(time.Now().UnixNano(), 10)+`}]}`)
	waitUntil(t, 5*time.Second, "the importer to forget B's pruned entry", func() bool {
		st, err := svc.Imports(connB.id)
		return err == nil && len(st.Values) == 1
	})
	if res, err := svc.Import(context.Background(), connB.id); err != nil || len(res.Merged) != 0 {
		t.Fatalf("a sync after the prune merged %+v (%v): the pruned entry would come back", res.Merged, err)
	}
	if _, ok := b.Lookup("withExec(make)"); ok {
		t.Fatal("B's pruned entry came back before the restart")
	}

	t.Log("restarting the service")
	r.stop()
	svc = r.start()
	waitConn(t, svc, a.ID)
	connB = waitConn(t, svc, b.ID)
	// The new importer knows nothing B kept: it merges the bundle again,
	// pruned entry included.
	waitLookup(t, b, "withExec(make)", hasOffer)
	if got := b.Merges(); got != merged+1 {
		t.Fatalf("B was merged %d times after the restart, want once", got-merged)
	}
	// Once: later syncs don't merge it again.
	for range 3 {
		res, err := svc.Import(context.Background(), connB.id)
		if err != nil || len(res.Merged) != 0 {
			t.Fatalf("a sync after the redo merged %+v (%v)", res.Merged, err)
		}
	}
	if got := b.Merges(); got != merged+1 {
		t.Fatalf("B was merged %d times after the restart, want once", got-merged)
	}
	// An engine's own exports are never merged into it, restart or not.
	if a.Merges() != 0 {
		t.Fatalf("A was merged its own export %d times", a.Merges())
	}
}

// Accepted, not planned to change (#45; #12, case 3): the export path learns
// entries only from session spans, and keeps them in memory. An export
// pending or in flight when the service stops without a drain (a crash,
// SIGKILL, or -drain-timeout 0) is lost: the restarted service doesn't know
// the entries, and exports them only once a later session's spans name them
// again. What was stored is not lost, and the restarted service resumes
// exporting new sessions at once. A clean stop drains first, and stores it
// (TestDrainedRestartStoresPendingExport). Decided in
// https://github.com/zerobull-consulting/dagger/issues/45.
func TestServiceRestartForgetsPendingExport(t *testing.T) {
	blobs, meta := newMemStores(t)
	r := newRestartable(t, blobs, meta)
	svc := r.start()
	a := fakeengine.New("cache-a", 1)
	r.connect(a)
	waitConn(t, svc, a.ID)
	h := r.harness(svc, a)
	h.buildRecipes()
	release := a.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "A's export to be in flight", func() bool { return a.InFlight() == 1 })

	t.Log("restarting the service mid-export, without a drain")
	r.stop()
	release()
	svc = r.start()
	conn := waitConn(t, svc, a.ID)
	// Long enough for many scheduler ticks (10ms) and a retry.
	time.Sleep(500 * time.Millisecond)
	if n := indexEntries(t, meta); n != 0 {
		t.Fatalf("the index has %d entries: a stop without a drain no longer loses them, against the decision in #45; update this test, the README, and the issue", n)
	}
	if entries, err := svc.Entries(conn.id); err != nil || len(entries) != 0 {
		t.Fatalf("the restarted service tracks %+v (%v): the tracker is now kept across a restart, against the decision in #45; update this test", entries, err)
	}

	// A later session that touches the entries exports them.
	r.harness(svc, a).postSpans("session-2", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	h = r.harness(svc, a)
	h.waitIndex(10, 11)
}

// A clean stop (SIGTERM) drains: the export in flight, and a session in its
// quiet period, are stored before the service goes away, so the restarted
// service has them without a later session naming them again. This is what
// TestServiceRestartForgetsPendingExport loses without the drain.
func TestDrainedRestartStoresPendingExport(t *testing.T) {
	blobs, meta := newMemStores(t)
	r := newRestartable(t, blobs, meta)
	svc := r.start()
	a := fakeengine.New("cache-a", 1)
	r.connect(a)
	waitConn(t, svc, a.ID)
	h := r.harness(svc, a)
	h.buildRecipes()
	a.Add(fakeengine.Entry{Number: 12, Recipe: "withExec(test)", Deps: []uint64{11}, Layers: [][]byte{testLayer("base"), testLayer("exec"), testLayer("test")}})
	release := a.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "A's export to be in flight", func() bool { return a.InFlight() == 1 })
	// Quiet: its end never arrives, and the quiet period is a minute.
	h.postSpans("session-2", cacheSpan(12, "11"))

	t.Log("stopping the service, drained, mid-export")
	done := make(chan DrainReport, 1)
	go func() { done <- r.stopDrained(time.Minute) }()
	waitUntil(t, 5*time.Second, "the drain to start", svc.Draining)
	release()
	var rep DrainReport
	select {
	case rep = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the drained stop didn't return")
	}
	if rep.Outcome != metrics.DrainComplete || rep.StoredOf(testScope, a.ID) != 3 || rep.LeftRoots() != 0 {
		t.Fatalf("drain report %+v", rep)
	}
	if n := indexEntries(t, meta); n != 3 {
		t.Fatalf("the index has %d entries after the drained stop, want 3", n)
	}

	// The restarted service has them: the index names them, and another
	// engine is merged them.
	svc = r.start()
	waitConn(t, svc, a.ID)
	b := fakeengine.New("cache-b", 1)
	r.connect(b)
	waitConn(t, svc, b.ID)
	waitLookup(t, b, "withExec(test)", hasOffer)
}

// Case 5 of #12 (its second comment): a bundle whose merge is just under the
// import path's limit is stored and imported; one over it is split by the
// export path into two bundles, each imported. Before the fix, the exporter
// checked a bundle only against its own message limit, so the third case's
// bundle, under the limit as stored but over it once the importer adds a
// presigned GET per layer, was stored whole and never imported.
func TestImportBundleNearMergeLimit(t *testing.T) {
	for _, tc := range nearMergeLimitCases {
		t.Run(tc.name, func(t *testing.T) {
			blobs, meta := newMemStores(t)
			testImportBundleNearMergeLimit(t, blobs, meta, tc)
		})
	}
}

type nearMergeLimitCase struct {
	name string
	// limit is the import path's limit, from the merge's estimated size
	// and the bundle's stored size.
	limit func(merge, stored int) int
	// bundles is how many bundles the build is stored in.
	bundles int
}

var nearMergeLimitCases = []nearMergeLimitCase{
	{name: "just under the limit", limit: func(merge, _ int) int { return merge }, bundles: 1},
	{name: "just over the limit", limit: func(merge, _ int) int { return merge - 1 }, bundles: 2},
	{name: "over the limit as a merge, under it as stored", limit: func(merge, stored int) int { return (merge + stored) / 2 }, bundles: 2},
}

// deepBuild is two builds whose chains have many layers, each with a
// presigned GET in a merge: an image (10) and another (11), 12 layers each,
// sharing none, so an export of both split in two stores each in a bundle of
// its own.
func deepBuild(cache *fakeengine.Cache) (base, exec [][]byte) {
	for i := range 12 {
		base = append(base, testLayer("base-"+strconv.Itoa(i)))
		exec = append(exec, testLayer("exec-"+strconv.Itoa(i)))
	}
	cache.Add(fakeengine.Entry{Number: 10, Recipe: "from(alpine)", Layers: base})
	cache.Add(fakeengine.Entry{Number: 11, Recipe: "withExec(make)", Layers: exec})
	return base, exec
}

// testImportBundleNearMergeLimit sets the import path's message limit by
// tc.limit, exports the build from engine A, and checks that it is stored in
// tc.bundles bundles, all merged into engine B, whose layers then pull.
func testImportBundleNearMergeLimit(t *testing.T, blobs blobstore.Store, meta *metastore.Store, tc nearMergeLimitCase) {
	bundles := tc.bundles
	// What the exporter will see: the tracker exports both entries as
	// roots, with their fs parts.
	probe := fakeengine.New("probe", 1)
	deepBuild(probe)
	capt, err := probe.Capture(protocol.Export{Roots: []uint64{10, 11}, Outputs: []protocol.ExportOutput{
		{Number: 10, Address: fakeengine.FSPart}, {Number: 11, Address: fakeengine.FSPart},
	}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(capt.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := importer.AddressBytes(context.Background(), importer.StoreSigner(scoped(t, blobs)))
	if err != nil {
		t.Fatal(err)
	}
	merge := importer.MergeSize(capt.Bundle, len(data), addr, testScope)
	limit := tc.limit(merge, len(data))
	t.Logf("the bundle is %d bytes; its merge, with %d byte GETs, is estimated at %d; the limit is %d", len(data), addr, merge, limit)

	h := newExportHarnessWith(t, blobs, meta,
		func(c *ExportConfig) { c.Exporter.MaxMergeBytes = limit },
		func(c *ImportConfig) { c.Importer.MaxMessageBytes = limit })
	base, exec := deepBuild(h.cache)
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11), carrierSpan(2))
	idx := h.waitIndex(10, 11)
	if got := len(bundlesOf(idx)); got != bundles {
		t.Fatalf("the build is stored in %d bundles, want %d", got, bundles)
	}

	b := fakeengine.New("cache-b", 1)
	eng := h.connectEngine(b)
	checkPull(t, b, "withExec(make)", exec)
	checkPull(t, b, "from(alpine)", base)
	st := waitMerged(t, h, eng)
	waitUntil(t, 10*time.Second, "every bundle to be merged into B", func() bool {
		st = h.debugImports(eng)
		return len(st.Merged) == bundles
	})
	if len(st.Failures) != 0 || b.Merges() != bundles {
		t.Fatalf("merges failed: %v; B merged %d times, want %d", st.Failures, b.Merges(), bundles)
	}
}

func bundlesOf(idx *metastore.Index) map[string]bool {
	out := map[string]bool{}
	for _, e := range idx.Entries {
		out[e.Bundle] = true
	}
	return out
}
