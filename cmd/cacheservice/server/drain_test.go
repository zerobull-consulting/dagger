package server

// Drain (#63): what a clean stop (SIGTERM) does before the service goes
// away, with fake engines behind the engine's own remotecache.Run. The
// binary's signal handling is tested in cmd/cacheservice
// (TestMinIOBinaryDrainsOnSIGTERM), the real-engine version in
// cmd/cacheservice/e2e (the drain scenario).

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// testQuiet is the harnesses' quiet period: a drain that waited it out would
// take a minute.
const testQuiet = time.Minute

// drainFor drains svc with a deadline of d from now.
func drainFor(svc *Service, d time.Duration) DrainReport {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return svc.Drain(ctx)
}

// startDrain drains svc in the background with a deadline of d from now;
// the channel gets its report.
func startDrain(t *testing.T, svc *Service, d time.Duration) <-chan DrainReport {
	t.Helper()
	done := make(chan DrainReport, 1)
	go func() { done <- drainFor(svc, d) }()
	waitUntil(t, 5*time.Second, "the drain to start", svc.Draining)
	return done
}

// waitDrain waits for a drain started by startDrain.
func waitDrain(t *testing.T, done <-chan DrainReport, timeout time.Duration) DrainReport {
	t.Helper()
	select {
	case rep := <-done:
		return rep
	case <-time.After(timeout):
		t.Fatalf("the drain didn't return in %s", timeout)
		return DrainReport{}
	}
}

// drainRunning fails the test if the drain has returned.
func drainRunning(t *testing.T, done <-chan DrainReport, what string) {
	t.Helper()
	select {
	case rep := <-done:
		t.Fatalf("the drain returned %s: %+v", what, rep)
	default:
	}
}

// indexHas reports whether the index names every number of cacheID, now.
func indexHas(t *testing.T, h *exportHarness, numbers ...uint64) bool {
	t.Helper()
	idx, err := h.meta.Index(context.Background(), testScope)
	if err != nil {
		t.Fatal(err)
	}
	c := idx.Caches[h.cache.ID]
	if c == nil {
		return false
	}
	for _, n := range numbers {
		if _, ok := c.Numbers[n]; !ok {
			return false
		}
	}
	return true
}

func newDrainHarness(t *testing.T, modify func(*ExportConfig)) (*exportHarness, *logRecorder) {
	t.Helper()
	blobs, meta := newMemStores(t)
	rec := &logRecorder{}
	h := newExportHarnessLogged(t, blobs, meta, slog.New(rec), modify, nil)
	return h, rec
}

// A session ends quietly (its end never seen) and the service is stopped
// within its quiet period: the drain exports it at once, and it is stored
// before Drain returns, long before the quiet period would have passed.
func TestDrainExportsQuietSession(t *testing.T) {
	h, rec := newDrainHarness(t, nil)
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	if indexHas(t, h, 10) {
		t.Fatal("exported before the quiet period passed: the test can't tell the drain did it")
	}

	start := time.Now()
	rep := drainFor(h.svc, 10*time.Second)
	took := time.Since(start)
	if !indexHas(t, h, 10, 11) {
		t.Fatal("the quiet session's entries weren't stored when Drain returned")
	}
	if rep.Outcome != metrics.DrainComplete || rep.Stored[h.cache.ID] != 2 || rep.LeftRoots() != 0 {
		t.Fatalf("report %+v", rep)
	}
	if took > testQuiet/4 {
		t.Fatalf("the drain took %s: it waited for the quiet period", took)
	}
	started := rec.find("drain: started")
	if len(started) != 1 || started[0]["cacheID"] != h.cache.ID || started[0]["sessions"] != "1" || started[0]["pending"] != "2" {
		t.Fatalf("drain start logged %v", started)
	}
	if stored := rec.find("drain: stored"); len(stored) != 1 || stored[0]["roots"] != "2" {
		t.Fatalf("drain stored logged %v", stored)
	}
	if done := rec.find("drain: done"); len(done) != 1 || done[0]["outcome"] != metrics.DrainComplete || done[0]["leftRoots"] != "0" {
		t.Fatalf("drain end logged %v", done)
	}
	waitMetric(t, h.svc, "cacheservice_drain_stored_roots_total", nil, 2)
	waitMetric(t, h.svc, "cacheservice_draining", nil, 1)
}

// Intended, though it may surprise: while draining, the spans of a session
// the service tracks are still taken. A session that ended just before the
// signal still sends its spans within EndGrace, and the drain waits for them
// (up to EndGrace) and exports them too. Dropping them would lose entries.
func TestDrainTakesLateSpansOfEndedSession(t *testing.T) {
	const endGrace = 30 * time.Second
	h, _ := newDrainHarness(t, func(c *ExportConfig) { c.Tracker.EndGrace = endGrace })
	h.build()
	// The end says 2 spans; one has arrived.
	h.postSpans("session-1", cacheSpan(10), carrierSpan(2))
	start := time.Now()
	done := startDrain(t, h.svc, time.Minute)
	waitUntil(t, 5*time.Second, "the drain to wait for the ended session", func() bool {
		st := h.svc.DrainStatus()
		return len(st.Left) == 1 && st.Left[0].Sessions == 1
	})
	drainRunning(t, done, "before the ended session's last span arrived")

	h.postSpans("session-1", cacheSpan(11, "10"))
	rep := waitDrain(t, done, 10*time.Second)
	if !indexHas(t, h, 10, 11) {
		t.Fatal("the span that arrived during the drain wasn't exported")
	}
	if rep.Outcome != metrics.DrainComplete || rep.Stored[h.cache.ID] != 2 || rep.Refused[metrics.DrainRefusedSpan] != 0 {
		t.Fatalf("report %+v", rep)
	}
	if took := time.Since(start); took > endGrace/2 {
		t.Fatalf("the drain took %s: it waited out EndGrace though every span was in", took)
	}
}

// An export in flight when the drain starts, held in the engine or with its
// upload stalled, completes, and is stored before Drain returns.
func TestDrainWaitsForExportInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold func(*exportHarness) (release func())
	}{
		{"export held", func(h *exportHarness) func() {
			release := h.cache.Hold()
			h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
			waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })
			return release
		}},
		{"upload stalled", func(h *exportHarness) func() {
			release := h.cache.StallUploads()
			h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
			waitUntil(t, 5*time.Second, "an upload to stall", func() bool { return h.cache.StalledUploads() > 0 })
			return release
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newDrainHarness(t, nil)
			h.build()
			release := tc.hold(h)
			defer release()
			done := startDrain(t, h.svc, time.Minute)
			waitUntil(t, 5*time.Second, "the drain to see the export in flight", func() bool {
				st := h.svc.DrainStatus()
				return len(st.Left) == 1 && st.Left[0].InFlight == 2
			})
			drainRunning(t, done, "with the export in flight")
			release()
			rep := waitDrain(t, done, 10*time.Second)
			if !indexHas(t, h, 10, 11) {
				t.Fatal("the export in flight wasn't stored when Drain returned")
			}
			if rep.Outcome != metrics.DrainComplete || rep.Stored[h.cache.ID] != 2 || rep.LeftRoots() != 0 {
				t.Fatalf("report %+v", rep)
			}
		})
	}
}

// The deadline passes with an export unfinished: Drain returns on time,
// logs what it left, and the stop that follows publishes nothing of it.
func TestDrainDeadlineLeavesExportUnfinished(t *testing.T) {
	h, rec := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })

	const deadline = 500 * time.Millisecond
	start := time.Now()
	rep := drainFor(h.svc, deadline)
	took := time.Since(start)
	if took < deadline || took > deadline+time.Second {
		t.Fatalf("Drain returned after %s, with a deadline of %s", took, deadline)
	}
	if rep.Outcome != metrics.DrainDeadline || rep.LeftRoots() != 2 || len(rep.Left) != 1 ||
		rep.Left[0].CacheID != h.cache.ID || rep.Left[0].InFlight != 2 || !rep.Left[0].Connected {
		t.Fatalf("report %+v", rep)
	}
	left := rec.find("drain: left unexported")
	if len(left) != 1 || left[0]["cacheID"] != h.cache.ID || left[0]["roots"] != "2" || left[0]["inFlight"] != "2" {
		t.Fatalf("left unexported logged %v", left)
	}
	if done := rec.find("drain: done"); len(done) != 1 || done[0]["outcome"] != metrics.DrainDeadline || done[0]["leftRoots"] != "2" {
		t.Fatalf("drain end logged %v", done)
	}
	waitMetric(t, h.svc, "cacheservice_drain_unexported_roots_total", nil, 2)

	// The stop that follows, as main does it: the export is cut off.
	h.svc.CloseEngines()
	h.cancel()
	h.svc.Wait()
	release()
	if n := indexEntries(t, h.meta); n != 0 {
		t.Fatalf("the index has %d entries of the export the deadline cut off", n)
	}
}

// While draining, new engine connections are refused with 503 and a
// Retry-After, and spans of sessions the service doesn't track are dropped.
// The connected engine keeps working: its export in flight completes.
func TestDrainRefusesNewWork(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })
	done := startDrain(t, h.svc, time.Minute)

	// A new connection: 503 before any upgrade.
	req, err := http.NewRequest(http.MethodGet, h.srv.URL+protocol.EnginePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("engine-token", "")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("a new connection while draining: %d, Retry-After %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	// An engine trying to connect stays out.
	b := fakeengine.New("cache-b", 1)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- remotecache.Run(ctx, fakeengine.Adapter{Cache: b}, remotecache.Config{URL: h.srv.URL, Token: "engine-token", EngineName: b.ID})
	}()
	defer func() {
		cancel()
		<-runDone
	}()
	waitUntil(t, 10*time.Second, "engine B to be refused", func() bool {
		return h.svc.DrainStatus().Left != nil && countRefused(h.svc, metrics.DrainRefusedConnection) >= 2
	})
	if h.svc.engineFor(h.ns(), b.ID) != nil {
		t.Fatal("engine B connected to a draining service")
	}

	// Spans of a new session of A, and of B's cache, are dropped.
	h.postSpans("session-2", cacheSpan(12), carrierSpan(1))
	h.postSpansOf(b, "session-b", cacheSpan(20), carrierSpan(1))
	if n := countRefused(h.svc, metrics.DrainRefusedSpan); n != 4 {
		t.Fatalf("%d spans and ends refused, want 4", n)
	}
	for _, e := range h.ns().tracker.Entries(h.cache.ID) {
		if e.Number == 12 {
			t.Fatalf("the tracker took a new session's entry while draining: %+v", e)
		}
	}
	if len(h.ns().tracker.Entries(b.ID)) != 0 {
		t.Fatal("the tracker took a new cache's entries while draining")
	}

	// The existing connection still works.
	release()
	rep := waitDrain(t, done, 10*time.Second)
	if !indexHas(t, h, 10, 11) || rep.Outcome != metrics.DrainComplete {
		t.Fatalf("the connected engine's export wasn't stored: %+v", rep)
	}
	if indexHas(t, h, 12) {
		t.Fatal("the new session's entry was exported")
	}
}

func countRefused(svc *Service, kind string) int {
	svc.drain.mu.Lock()
	defer svc.drain.mu.Unlock()
	return svc.drain.refused[kind]
}

// A root waiting on a retry when the drain starts is due at once, rather
// than at its retryAt; one that fails during the drain retries on its normal
// backoff, within the drain's deadline. Both are stored before Drain returns.
func TestDrainRetriesRootOnBackoff(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backoff time.Duration
		// before fails the export before the drain starts.
		before bool
	}{
		{"waiting when the drain starts", testQuiet, true},
		{"failing during the drain", 300 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newDrainHarness(t, func(c *ExportConfig) { c.Tracker.BusyBackoff = tc.backoff })
			digests := h.build()
			// The export's 3 tries upload a corrupt exec layer, which fails
			// verification: its roots count as failed, and wait a backoff.
			h.cache.CorruptUploads(digests[1], 3)
			failed := func() bool {
				for _, e := range h.ns().tracker.Entries(h.cache.ID) {
					if e.Failures > 0 && e.RetryAt != "" {
						return true
					}
				}
				return false
			}
			start := time.Now()
			if tc.before {
				h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
				waitUntil(t, 10*time.Second, "the export to fail", failed)
				rep := drainFor(h.svc, 10*time.Second)
				if !indexHas(t, h, 10, 11) || rep.Outcome != metrics.DrainComplete {
					t.Fatalf("the root waiting on its retry wasn't stored by the drain: %+v", rep)
				}
				if took := time.Since(start); took > tc.backoff/4 {
					t.Fatalf("took %s: the drain waited for the retry's backoff", took)
				}
				return
			}
			h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
			done := startDrain(t, h.svc, 20*time.Second)
			waitUntil(t, 10*time.Second, "the export to fail during the drain", failed)
			rep := waitDrain(t, done, 15*time.Second)
			if !indexHas(t, h, 10, 11) || rep.Outcome != metrics.DrainComplete || rep.Stored[h.cache.ID] != 2 {
				t.Fatalf("the root that failed during the drain wasn't retried and stored: %+v", rep)
			}
			if took := time.Since(start); took < tc.backoff {
				t.Fatalf("took %s: the retry didn't wait its backoff of %s", took, tc.backoff)
			}
		})
	}
}

// KNOWN GAP: an engine that goes away mid-drain can't reconnect to the
// draining service, which refuses every new connection: its export is
// interrupted, as on any disconnect, and isn't tried again. The drain doesn't
// wait for it: it logs the roots as left unexported, and returns. Tracked in
// https://github.com/zerobull-consulting/dagger/issues/70.
func TestDrainEngineDisconnectDoesNotHang(t *testing.T) {
	h, rec := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })
	const deadline = time.Minute
	start := time.Now()
	done := startDrain(t, h.svc, deadline)
	if err := h.svc.CloseEngine(h.engine.id); err != nil {
		t.Fatal(err)
	}
	rep := waitDrain(t, done, 10*time.Second)
	if took := time.Since(start); took > deadline/4 {
		t.Fatalf("the drain took %s after its engine went away", took)
	}
	if rep.Outcome != metrics.DrainComplete || rep.LeftRoots() != 2 || len(rep.Left) != 1 || rep.Left[0].Connected {
		t.Fatalf("report %+v", rep)
	}
	left := rec.find("drain: left unexported")
	if len(left) != 1 || left[0]["cacheID"] != h.cache.ID || left[0]["connected"] != "false" || left[0]["roots"] != "2" {
		t.Fatalf("left unexported logged %v", left)
	}
	if indexHas(t, h, 10) {
		t.Fatal("stored an export whose engine went away")
	}
}

// A canceled drain (a second signal) returns at once, whatever is left.
func TestDrainCanceled(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan DrainReport, 1)
	go func() { done <- h.svc.Drain(ctx) }()
	waitUntil(t, 5*time.Second, "the drain to start", h.svc.Draining)
	start := time.Now()
	cancel()
	rep := waitDrain(t, done, 5*time.Second)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a canceled drain took %s to return", took)
	}
	if rep.Outcome != metrics.DrainCanceled || rep.LeftRoots() != 2 {
		t.Fatalf("report %+v", rep)
	}
}

// With nothing pending, Drain returns at once.
func TestDrainNothingPending(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	h.exportBuild()
	start := time.Now()
	rep := drainFor(h.svc, time.Minute)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Drain took %s with nothing pending", took)
	}
	if rep.Outcome != metrics.DrainComplete || rep.LeftRoots() != 0 || len(rep.Stored) != 0 {
		t.Fatalf("report %+v", rep)
	}
}

// A service with no store (no bucket), whose export path is off, has nothing
// to drain: Drain returns at once, and the service still refuses new
// engines.
func TestDrainExportOff(t *testing.T) {
	svc, rec, srv := newTestService(t, "")
	start := time.Now()
	rep := drainFor(svc, time.Minute)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Drain took %s with the export path off", took)
	}
	if rep.Outcome != metrics.DrainComplete || rep.LeftRoots() != 0 {
		t.Fatalf("report %+v", rep)
	}
	if len(rec.find("drain: nothing to export (the export path is off)")) != 1 {
		t.Fatal("the drain didn't log that the export path is off")
	}
	resp, err := http.Get(srv.URL + protocol.EnginePath)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a new connection while draining: %d", resp.StatusCode)
	}
	svc.Wait()
}

// The drain stops the import sync loops and GC: no sync or GC run starts
// while the service drains, and a GC run asked for is refused.
func TestDrainStopsImportsAndGC(t *testing.T) {
	st := newGCStores(t, nil)
	h := newExportHarness(t, st.blobs, st.meta, nil)
	// Scheduled, though not within the test: the drain must end the loop.
	if err := h.svc.EnableGC(h.ctx, GCConfig{Interval: time.Hour, Collector: gc.Config{Bucket: st.bucket, Meta: st.meta, Now: st.clock.Now}}); err != nil {
		t.Fatal(err)
	}
	conn := h.engine
	waitUntil(t, 5*time.Second, "the engine's import sync loop", func() bool {
		h.svc.imports.mu.Lock()
		defer h.svc.imports.mu.Unlock()
		_, ok := h.svc.imports.triggers[conn]
		return ok
	})
	drainFor(h.svc, time.Minute)
	waitUntil(t, 5*time.Second, "the import sync loop to stop", func() bool {
		h.svc.imports.mu.Lock()
		defer h.svc.imports.mu.Unlock()
		return len(h.svc.imports.triggers) == 0
	})
	gcDone := make(chan struct{})
	go func() {
		h.svc.waitGC()
		close(gcDone)
	}()
	select {
	case <-gcDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduled GC loop still runs after the drain")
	}
	if _, err := h.svc.CollectGarbage(context.Background(), false); !errors.Is(err, ErrDraining) {
		t.Fatalf("a GC run while draining: %v", err)
	}
	resp, err := http.Post(h.debug.URL+"/debug/gc", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("POST /debug/gc while draining: %d", resp.StatusCode)
	}
	// The connection stays: exports would run over it.
	if !h.svc.connected(conn) {
		t.Fatal("the drain closed the engine's connection")
	}
}

// The tracker's drain status, as GET /debug/drain shows it, while draining.
func TestDrainDebugStatus(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	if st := h.svc.DrainStatus(); st.Draining || st.Report != nil {
		t.Fatalf("not draining: %+v", st)
	}
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })
	done := startDrain(t, h.svc, time.Minute)
	var st DrainStatus
	waitUntil(t, 5*time.Second, "GET /debug/drain to show the export in flight", func() bool {
		st = debugDrain(t, h)
		return st.Draining && st.Deadline != nil && len(st.Left) == 1 && st.Left[0].InFlight == 2
	})
	release()
	waitDrain(t, done, 10*time.Second)
	st = debugDrain(t, h)
	if !st.Draining || st.Report == nil || st.Report.Outcome != metrics.DrainComplete || !slices.Equal(keysOf(st.Report.Stored), []string{h.cache.ID}) {
		t.Fatalf("after the drain: %+v", st)
	}
}

func debugDrain(t *testing.T, h *exportHarness) DrainStatus {
	t.Helper()
	resp, err := http.Get(h.debug.URL + "/debug/drain")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st DrainStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

// A namespace the service hasn't seen until a span of it arrives during the
// drain drains from that span: the span, a new session's, is dropped, and
// the drain doesn't export it. Without tokens, the default namespace is
// created the same way, by its first request.
func TestDrainNewNamespaceTakesNoNewSession(t *testing.T) {
	h := newAuthHarness(t)
	a := fakeengine.New("cache-a", 1)
	authBuild(a, "a")
	conn := h.connect(a, "tok-a")
	release := a.Hold()
	defer release()
	h.postSpans("tok-a", a, "session-a", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "A's export to be held", func() bool { return a.InFlight() == 1 })
	h.svc.nsMu.Lock()
	_, seen := h.svc.namespaces["team-b"]
	h.svc.nsMu.Unlock()
	if seen {
		t.Fatal("team-b exists before the drain: the test can't create it during it")
	}
	done := startDrain(t, h.svc, time.Minute)

	b := fakeengine.New("cache-b", 1)
	if code := h.postSpans("tok-b", b, "session-b", cacheSpan(20), carrierSpan(1)); code != http.StatusOK {
		t.Fatalf("spans of a new namespace while draining: %d", code)
	}
	if got := h.ns("team-b").tracker.Entries(b.ID); len(got) != 0 {
		t.Fatalf("the new namespace's tracker took a new session while draining: %+v", got)
	}
	if n := countRefused(h.svc, metrics.DrainRefusedSpan); n != 2 {
		t.Fatalf("%d spans and ends refused, want 2", n)
	}
	release()
	rep := waitDrain(t, done, 10*time.Second)
	if rep.Outcome != metrics.DrainComplete || rep.Stored[a.ID] != 2 || rep.Stored[b.ID] != 0 {
		t.Fatalf("report %+v", rep)
	}
	if !h.svc.connected(conn) {
		t.Fatal("the drain closed A's connection")
	}
}

// A connection upgraded before the drain started, which says hello during
// it, is closed with 1013 (try again later), and counted as refused.
func TestDrainRefusesHelloOfLateConnection(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	raw, code := dialAs(t, h.srv, "engine-token")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("dial before the drain: %d", code)
	}
	drainFor(h.svc, time.Minute)
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "late", Generation: 1, EngineName: "late"})
	if err != nil {
		t.Fatal(err)
	}
	raw.send(env)
	if status := raw.closed(); status != websocket.StatusTryAgainLater {
		t.Fatalf("a hello while draining was closed with %d, want %d", status, websocket.StatusTryAgainLater)
	}
	if n := countRefused(h.svc, metrics.DrainRefusedConnection); n != 1 {
		t.Fatalf("%d connections counted refused, want 1", n)
	}
	if h.svc.engineFor(h.ns(), "late") != nil {
		t.Fatal("the late connection was welcomed")
	}
}

// blockingBucket holds GC's first listing until its context ends, as a GC
// run that takes long would.
type blockingBucket struct {
	gc.Bucket
	blocked chan struct{}
	once    sync.Once
}

func (b *blockingBucket) ListLevel(ctx context.Context, prefix string) ([]string, []gc.Object, error) {
	b.once.Do(func() { close(b.blocked) })
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

// A GC run asked for (POST /debug/gc) and under way when the drain starts is
// canceled, as a scheduled one is: GC is idempotent, and the next run
// finishes it.
func TestDrainCancelsGCRunAskedFor(t *testing.T) {
	st := newGCStores(t, nil)
	h := newExportHarness(t, st.blobs, st.meta, nil)
	bucket := &blockingBucket{Bucket: st.bucket, blocked: make(chan struct{})}
	if err := h.svc.EnableGC(h.ctx, GCConfig{Collector: gc.Config{Bucket: bucket, Meta: st.meta, Now: st.clock.Now}}); err != nil {
		t.Fatal(err)
	}
	ran := make(chan error, 1)
	go func() {
		_, err := h.svc.CollectGarbage(context.Background(), false)
		ran <- err
	}()
	select {
	case <-bucket.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the GC run didn't start")
	}
	drainFor(h.svc, time.Minute)
	select {
	case err := <-ran:
		if err == nil {
			t.Fatal("the canceled GC run reported no error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the GC run asked for before the drain still runs after it")
	}
}

// The roots an export stored are in the drain's report even when the drain
// ends the moment the tracker hears of the export: the export goroutine is
// held here right after, and without counting first the report would miss
// them.
func TestDrainCountsStoredBeforeItEnds(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	hold := make(chan struct{})
	h.svc.testHookExportRecorded = func() {
		if h.svc.Draining() {
			<-hold
		}
	}
	defer close(hold)
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	rep := drainFor(h.svc, 10*time.Second)
	if rep.Outcome != metrics.DrainComplete || rep.Stored[h.cache.ID] != 2 {
		t.Fatalf("the drain's report misses the roots its export stored: %+v", rep)
	}
}

// The explicit export and import hooks are new work: refused (503) while the
// service drains, as a GC run is.
func TestDrainRefusesExplicitExportAndImport(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	drainFor(h.svc, time.Minute)
	if code, _ := h.debugExport(""); code != http.StatusServiceUnavailable {
		t.Fatalf("POST /debug/engines/{conn}/export while draining: %d", code)
	}
	if code, _ := h.debugImport(h.engine); code != http.StatusServiceUnavailable {
		t.Fatalf("POST /debug/engines/{conn}/import while draining: %d", code)
	}
	if _, err := h.svc.Export(context.Background(), h.engine.id, exporter.Request{Roots: []uint64{99}}); !errors.Is(err, ErrDraining) {
		t.Fatalf("Export while draining: %v", err)
	}
}

// holdKey marks the context of a call whose index reads holdReads holds.
type holdKey struct{}

// holdReads holds the index reads made with a context marked by holdKey,
// until released.
type holdReads struct {
	metastore.Objects
	held     chan struct{}
	released chan struct{}
	once     sync.Once
}

func (o *holdReads) Get(ctx context.Context, key string) ([]byte, string, error) {
	if ctx.Value(holdKey{}) != nil {
		o.once.Do(func() { close(o.held) })
		<-o.released
	}
	return o.Objects.Get(ctx, key)
}

// KNOWN GAP: the drain doesn't wait for an explicit export (POST
// /debug/engines/{conn}/export) admitted just before it started. Once it
// started, the hook is refused (503), but one that got past that check runs
// on: of roots the tracker doesn't know, it has no in-flight state the
// drain waits on, so the drain can report it complete, and the stop that
// follows cuts it off. Cut off, it publishes nothing. It needs a local,
// unauthenticated debug request racing the signal. Tracked in
// https://github.com/zerobull-consulting/dagger/issues/72.
func TestKnownGapDrainDoesNotWaitForExplicitExport(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.cache.Add(fakeengine.Entry{Number: 99, Layers: [][]byte{testLayer("explicit")}})
	release := h.cache.Hold()
	defer release()
	done := make(chan error, 1)
	go func() {
		_, err := h.svc.Export(context.Background(), h.engine.id, exporter.Request{
			Roots:   []uint64{99},
			Outputs: []protocol.ExportOutput{{Number: 99, Address: fakeengine.FSPart}},
		})
		done <- err
	}()
	waitUntil(t, 5*time.Second, "the explicit export to be held", func() bool { return h.cache.InFlight() == 1 })
	rep := drainFor(h.svc, 10*time.Second)
	if h.cache.InFlight() != 1 || rep.Outcome != metrics.DrainComplete || rep.LeftRoots() != 0 {
		t.Fatalf("the drain waited for, or counted, the explicit export (in flight %d, report %+v): the gap is fixed; update this test, the README and the issue",
			h.cache.InFlight(), rep)
	}
	release()
	<-done
}

// KNOWN GAP: as TestKnownGapDrainDoesNotWaitForExplicitExport, for the
// explicit import (POST /debug/engines/{conn}/import): one admitted just
// before the drain runs on its caller's context, which the drain doesn't
// cancel or wait for. Tracked in
// https://github.com/zerobull-consulting/dagger/issues/72.
func TestKnownGapDrainDoesNotWaitForExplicitImport(t *testing.T) {
	var hold *holdReads
	blobs, _ := newMemStores(t)
	objs := metastore.NewMemObjects()
	hold = &holdReads{Objects: objs, held: make(chan struct{}), released: make(chan struct{})}
	meta, err := metastore.New(metastore.Config{Objects: hold})
	if err != nil {
		t.Fatal(err)
	}
	h := newExportHarness(t, blobs, meta, nil)
	defer close(hold.released)
	done := make(chan error, 1)
	go func() {
		_, err := h.svc.Import(context.WithValue(context.Background(), holdKey{}, true), h.engine.id)
		done <- err
	}()
	select {
	case <-hold.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the explicit import didn't reach its index read")
	}
	start := time.Now()
	rep := drainFor(h.svc, 10*time.Second)
	select {
	case <-done:
		t.Fatal("the explicit import ended with the drain: the gap is fixed; update this test, the README and the issue")
	default:
	}
	if took := time.Since(start); took > 5*time.Second || rep.Outcome != metrics.DrainComplete {
		t.Fatalf("the drain waited for the explicit import (%s, %+v): the gap is fixed; update this test", took, rep)
	}
}

// A drain whose deadline passed before it began starts nothing: what it
// would have exported is reported left, pending, not in flight.
// (zerobull-consulting/dagger#73, the known gap this test pinned before.)
func TestDrainStartsNothingPastItsDeadline(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	rep := h.svc.Drain(ctx)
	if rep.Outcome != metrics.DrainDeadline {
		t.Fatalf("report %+v", rep)
	}
	if len(rep.Left) != 1 || rep.Left[0].InFlight != 0 || rep.Left[0].Pending != 2 || !rep.Left[0].Connected {
		t.Fatalf("the drain past its deadline didn't leave its roots pending: %+v", rep)
	}
	// Shows nothing happens: the harness's scheduler ticks every 10ms, and
	// an export started would reach the held engine within a few ticks.
	time.Sleep(200 * time.Millisecond)
	if n := h.cache.Exports(); n != 0 || h.cache.InFlight() != 0 {
		t.Fatalf("%d exports reached the engine after a drain past its deadline", n)
	}
}
