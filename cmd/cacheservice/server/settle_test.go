package server

import (
	"context"
	"errors"
	"os"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// The engine's own requests since v1.0.0-beta.26 (settle.go,
// zerobull-consulting/dagger#227), through the engine's own client
// (remotecache.Run) where the fake engine can send them.

// settledPeriods waits until the fake engine heard n Settled replies.
func settledPeriods(t *testing.T, cache *fakeengine.Cache, n int) []uint64 {
	t.Helper()
	var got []uint64
	waitUntil(t, 10*time.Second, "the engine to hear settled", func() bool {
		got = cache.SettledPeriods()
		return len(got) >= n
	})
	return got
}

// An engine whose last session ended asks to settle: the service answers
// only once the session's spans came and its entries are stored, though the
// settle came first.
func TestSettleWaitsForTheSessionsExport(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.svc.settleMarkerWait.Store(int64(time.Minute))
	h.build()
	h.cache.SetSettleState(remotecache.SettleState{Period: 1, Live: true})
	h.cache.SetSettleState(remotecache.SettleState{Period: 1, Sessions: []protocol.SettleSession{{ID: "session-1", Marker: true}}})
	// The settle is in, and waits: no span of the session came yet.
	waitUntil(t, 10*time.Second, "the open to reach the service", func() bool {
		period, _ := h.engine.period.current()
		return period == 1
	})
	time.Sleep(300 * time.Millisecond)
	if got := h.cache.SettledPeriods(); len(got) != 0 {
		t.Fatalf("settled %v before any span of its session came", got)
	}
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	if got := settledPeriods(t, h.cache, 1); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("settled periods %v, want [1]", got)
	}
	if !indexHas(t, h, 10, 11) {
		t.Fatal("the service settled before the session's entries were stored")
	}
}

// A listed session whose spans never reach the service, as when its client
// sends its telemetry elsewhere, holds the settle only for the marker wait.
func TestSettleUnseenSessionIsBounded(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.svc.settleMarkerWait.Store(int64(300 * time.Millisecond))
	start := time.Now()
	h.cache.SetSettleState(remotecache.SettleState{Period: 1, Sessions: []protocol.SettleSession{{ID: "session-1", Marker: true}}})
	settledPeriods(t, h.cache, 1)
	if took := time.Since(start); took < 250*time.Millisecond || took > 5*time.Second {
		t.Fatalf("settled after %s, want about the 300ms marker wait", took)
	}
	// One whose engine sent no marker has no end to wait for.
	h.svc.settleMarkerWait.Store(int64(time.Minute))
	h.cache.SetSettleState(remotecache.SettleState{Period: 2, Live: true})
	start = time.Now()
	h.cache.SetSettleState(remotecache.SettleState{Period: 2, Sessions: []protocol.SettleSession{{ID: "session-2"}}})
	if got := settledPeriods(t, h.cache, 2); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("settled periods %v, want [1 2]", got)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("a session with no marker held the settle for %s", took)
	}
}

// With the export path off there is nothing to ask the engine for.
func TestSettleWithoutExportPath(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	cache := fakeengine.New("cache-a", 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: cache}, remotecache.Config{EngineVersion: testEngineVersion, URL: srv.URL, Token: "engine-token"})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitEngines(t, svc, 1)
	cache.SetSettleState(remotecache.SettleState{Period: 1, Sessions: []protocol.SettleSession{{ID: "session-1", Marker: true}}})
	settledPeriods(t, cache, 1)
}

// A settle for another period than the connection's is stale, at once or
// once a later period opens while it waits.
func TestSettleStale(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.svc.settleMarkerWait.Store(int64(time.Minute))
	c := h.engine
	c.period.open(3)
	if err := h.svc.settle(t.Context(), c, protocol.Settle{Period: 2}); !errors.Is(err, errStaleSettle) {
		t.Fatalf("a settle for an earlier period: %v, want stale", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- h.svc.settle(t.Context(), c, protocol.Settle{Period: 3, Sessions: []protocol.SettleSession{{ID: "session-1", Marker: true}}})
	}()
	select {
	case err := <-done:
		t.Fatalf("the settle returned %v with its session unseen", err)
	case <-time.After(200 * time.Millisecond):
	}
	c.period.open(4)
	select {
	case err := <-done:
		if !errors.Is(err, errStaleSettle) {
			t.Fatalf("a settle whose period ended: %v, want stale", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the settle outlived its period")
	}
}

// A settle held back by an export that gets no answer ends at the service's
// own bound, with an error reply the engine doesn't take for settled.
func TestSettleGivesUpAtItsBound(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.svc.maxSettleWait.Store(int64(300 * time.Millisecond))
	h.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}})
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), carrierSpan(1))
	start := time.Now()
	err := h.svc.settle(t.Context(), h.engine, protocol.Settle{Period: 0, Sessions: []protocol.SettleSession{{ID: "session-1", Marker: true}}})
	if !errors.Is(err, errSettleTimeout) {
		t.Fatalf("settle: %v after %s, want its timeout", err, time.Since(start))
	}
}

// A root whose next try is due past the settle's retry wait doesn't hold the
// settle back: an engine that hears no settled gives up, and then refuses
// that try and every other export. One due within it is waited for.
func TestSettleDoesNotWaitForLateRetries(t *testing.T) {
	h, _ := newMemExportHarness(t, func(c *ExportConfig) { c.Tracker.BusyBackoff = time.Hour })
	h.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}, Busy: true})
	h.postSpans("session-1", cacheSpan(10), carrierSpan(1))
	waitUntil(t, 10*time.Second, "the busy root to wait out its backoff", func() bool {
		return h.ns().tracker.FlushStatus(h.cache.ID, time.Now(), time.Now().Add(time.Minute)).Later == 1
	})
	start := time.Now()
	if err := h.svc.settle(t.Context(), h.engine, protocol.Settle{Period: 0}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the settle took %s: it waited for a retry due in an hour", took)
	}
	// The same retry, within the wait, holds it.
	h.svc.settleRetryWait.Store(int64(2 * time.Hour))
	h.svc.maxSettleWait.Store(int64(300 * time.Millisecond))
	if err := h.svc.settle(t.Context(), h.engine, protocol.Settle{Period: 0}); !errors.Is(err, errSettleTimeout) {
		t.Fatalf("settle with the retry due inside its wait: %v, want its timeout", err)
	}
}

// Roots the engine refused as paused don't hold a settle back either: the
// engine has given up on it.
func TestSettleDoesNotWaitForPausedRoots(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}, Refused: exporter.EnginePausedMessage})
	h.postSpans("session-1", cacheSpan(10), carrierSpan(1))
	waitUntil(t, 10*time.Second, "the root to be paused", func() bool {
		return h.ns().tracker.FlushStatus(h.cache.ID, time.Now(), time.Time{}).Paused == 1
	})
	if err := h.svc.settle(t.Context(), h.engine, protocol.Settle{Period: 0}); err != nil {
		t.Fatalf("settle: %v", err)
	}
}

// Roots an engine refuses because it has given up its remote-cache work
// aren't refused for good, and count no failure: they wait for the engine,
// and its next open makes them due.
func TestPausedRootsExportedAfterOpen(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	base := testLayer("base")
	paused := fakeengine.Entry{Number: 10, Layers: [][]byte{base}, Refused: exporter.EnginePausedMessage}
	h.cache.Add(paused)
	h.postSpans("session-1", cacheSpan(10), carrierSpan(1))
	var entry exporter.EntryState
	waitUntil(t, 10*time.Second, "the root to wait after the engine refused it as paused", func() bool {
		entries := h.ns().tracker.Entries(h.cache.ID)
		if len(entries) != 1 {
			return false
		}
		entry = entries[0]
		return entry.RetryAt != "" && !entry.InFlight
	})
	if entry.Refused != "" || entry.Failures != 0 || entry.Stored {
		t.Fatalf("the paused root is settled as %+v, want only waiting", entry)
	}
	exports := h.cache.Exports()
	// The engine takes exports again, and says so.
	paused.Refused = ""
	h.cache.Add(paused)
	h.cache.SetSettleState(remotecache.SettleState{Period: 1, Live: true})
	waitUntil(t, 10*time.Second, "the root to be stored after the open", func() bool { return indexHas(t, h, 10) })
	if h.cache.Exports() != exports+1 {
		t.Fatalf("%d exports after the open, want 1", h.cache.Exports()-exports)
	}
}

// A settle for a period whose open didn't come opens it, as the open would
// have: the roots the engine refused while paused are due at once, and
// don't hold the settle back for their wait.
func TestSettleOpensItsPeriod(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	paused := fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}, Refused: exporter.EnginePausedMessage}
	h.cache.Add(paused)
	h.postSpans("session-1", cacheSpan(10), carrierSpan(1))
	waitUntil(t, 10*time.Second, "the root to wait after the engine refused it as paused", func() bool {
		entries := h.ns().tracker.Entries(h.cache.ID)
		return len(entries) == 1 && entries[0].RetryAt != "" && !entries[0].InFlight
	})
	paused.Refused = ""
	h.cache.Add(paused)
	current, _ := h.engine.period.current()
	done := make(chan error, 1)
	go func() { done <- h.svc.settle(t.Context(), h.engine, protocol.Settle{Period: current + 1}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the settle waited for the paused root's own wait")
	}
	if !indexHas(t, h, 10) {
		t.Fatal("the service settled before the paused root was stored")
	}
}

// open, claims and lookup are answered, each with its reply: every miss of
// a lookup unknown, since the service looks nothing up.
func TestEngineRequestsAnswered(t *testing.T) {
	_, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "engine-token")
	if reply := e.helloAs("cache-a"); reply.Type != protocol.TypeWelcome {
		t.Fatalf("hello answered with %s", reply.Type)
	}
	ask := func(id uint64, typ protocol.Type, body any) protocol.Envelope {
		t.Helper()
		env, err := protocol.NewEnvelope(id, 0, typ, body)
		if err != nil {
			t.Fatal(err)
		}
		e.send(env)
		reply := e.next()
		if reply.Re != id {
			t.Fatalf("%s answered with a reply to %d", typ, reply.Re)
		}
		return reply
	}
	if reply := ask(2, protocol.TypeOpen, protocol.Open{Period: 1}); reply.Type != protocol.TypeOpened {
		t.Fatalf("open answered with %s", reply.Type)
	}
	if reply := ask(3, protocol.TypeClaims, protocol.Claims{Sessions: []protocol.SessionClaim{{ID: "session-1", Claim: "claim-1"}}}); reply.Type != protocol.TypeClaimed {
		t.Fatalf("claims answered with %s", reply.Type)
	}
	reply := ask(4, protocol.TypeLookup, protocol.Lookup{Misses: []protocol.LookupMiss{{Recipe: "sha256:aa"}, {Recipe: "sha256:bb"}}})
	var answer protocol.LookedUp
	if reply.Type != protocol.TypeLookedUp || reply.Decode(&answer) != nil || len(answer.Misses) != 2 ||
		answer.Misses[0].Status != protocol.LookupUnknown || answer.Misses[1].Status != protocol.LookupUnknown || answer.Misses[0].Merge != 0 {
		t.Fatalf("lookup answered with %s %+v", reply.Type, answer)
	}
	// A settle for the open period, with nothing to export: settled.
	if reply := ask(5, protocol.TypeSettle, protocol.Settle{Period: 1}); reply.Type != protocol.TypeSettled {
		t.Fatalf("settle answered with %s", reply.Type)
	}
	// One for an earlier period: stale.
	if reply := ask(6, protocol.TypeOpen, protocol.Open{Period: 2}); reply.Type != protocol.TypeOpened {
		t.Fatalf("open answered with %s", reply.Type)
	}
	reply = ask(7, protocol.TypeSettle, protocol.Settle{Period: 1})
	var e2 protocol.Error
	if reply.Type != protocol.TypeError || reply.Decode(&e2) != nil || e2.Message != protocol.StaleSettle {
		t.Fatalf("a stale settle answered with %s %q", reply.Type, e2.Message)
	}
}

// A new service has both of a settle's bounds.
func TestNewServiceSettleBounds(t *testing.T) {
	svc, _, _ := newTestService(t, "")
	if got := time.Duration(svc.settleMarkerWait.Load()); got != DefaultSettleMarkerWait {
		t.Fatalf("marker wait %s, want %s", got, DefaultSettleMarkerWait)
	}
	if got := time.Duration(svc.maxSettleWait.Load()); got != MaxSettleWait {
		t.Fatalf("settle bound %s, want %s", got, MaxSettleWait)
	}
	if got := time.Duration(svc.settleRetryWait.Load()); got != DefaultSettleRetryWait {
		t.Fatalf("retry wait %s, want %s", got, DefaultSettleRetryWait)
	}
	if got := time.Duration(svc.settleQuiet.Load()); got != DefaultSettleQuiet || got <= EngineSettleBound {
		t.Fatalf("quiet %s, want %s, past the engine's bound", got, DefaultSettleQuiet)
	}
}

// EngineSettleBound is engine/server's remoteCacheSettleBound, which that
// package doesn't export: the quiet after a settle has to outlast it.
func TestEngineSettleBoundMatchesEngine(t *testing.T) {
	src, err := os.ReadFile("../../../engine/server/engine_status.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`const remoteCacheSettleBound = (\d+) \* time\.Second`).FindSubmatch(src)
	if m == nil {
		t.Fatal("engine/server/engine_status.go no longer declares remoteCacheSettleBound as a number of seconds: find the engine's bound, and update EngineSettleBound and this test")
	}
	if got := string(m[1]) + "s"; got != EngineSettleBound.String() {
		t.Fatalf("the engine's bound is %s, and EngineSettleBound is %s", got, EngineSettleBound)
	}
}

// Once it answered settled, the service starts no export of the cache until
// the engine's bound has passed: an export in progress at that moment makes
// even a settled engine give up. An open ends the quiet.
func TestSettledCacheIsQuietUntilTheEnginesBound(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.svc.settleQuiet.Store(int64(time.Second))
	if err := h.svc.settle(t.Context(), h.engine, protocol.Settle{Period: 0}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	settled := time.Now()
	h.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}})
	h.postSpans("session-1", cacheSpan(10), carrierSpan(1))
	waitUntil(t, 10*time.Second, "the late entry to be stored", func() bool { return indexHas(t, h, 10) })
	if took := time.Since(settled); took < 900*time.Millisecond {
		t.Fatalf("exported %s after the settle, inside the quiet", took)
	}

	h.svc.settleQuiet.Store(int64(time.Hour))
	if err := h.svc.settle(t.Context(), h.engine, protocol.Settle{Period: 0}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	h.cache.Add(fakeengine.Entry{Number: 12, Layers: [][]byte{testLayer("other")}})
	h.postSpans("session-2", cacheSpan(12), carrierSpan(1))
	time.Sleep(300 * time.Millisecond)
	if indexHas(t, h, 12) {
		t.Fatal("exported inside an hour's quiet")
	}
	h.cache.SetSettleState(remotecache.SettleState{Period: 1, Live: true})
	waitUntil(t, 10*time.Second, "the entry to be stored after the open", func() bool { return indexHas(t, h, 12) })
}
