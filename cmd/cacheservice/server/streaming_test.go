package server

// Streaming exports (zerobull-consulting/dagger#134) and one export in
// flight per engine cache (zerobull-consulting/dagger#135), through the
// scheduler, with a fake engine behind the engine's own remotecache.Run.

import (
	"net/http"
	"sync"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
)

// An entry of a session that hasn't ended, and never sends a carrier, is
// stored once it settles: the scheduler wakes at its due time, not at its
// tick, which is an hour here.
func TestStreamStoresWhileSessionRuns(t *testing.T) {
	const settle = 200 * time.Millisecond
	h, _ := newMemExportHarness(t, func(c *ExportConfig) {
		c.Tick = time.Hour
		c.Tracker.Settle = settle
	})
	h.build()
	start := time.Now()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	waitUntil(t, 10*time.Second, "the running session's entries to be stored", func() bool { return indexHas(t, h, 10, 11) })
	took := time.Since(start)
	if took < settle {
		t.Fatalf("stored after %s, before Settle (%s)", took, settle)
	}
	if took > settle+2*time.Second {
		t.Fatalf("stored after %s: Settle is %s", took, settle)
	}
	t.Logf("stored %s after its last span (Settle %s)", took.Round(time.Millisecond), settle)
}

// One export of a cache at a time: entries that come due while an export of
// their cache is held wait for it, and then go in one batch together.
func TestStreamOneExportPerCache(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.build()
	h.cache.Add(fakeengine.Entry{Number: 12, Layers: [][]byte{testLayer("twelve")}})
	h.cache.Add(fakeengine.Entry{Number: 13, Layers: [][]byte{testLayer("thirteen")}})
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitUntil(t, 5*time.Second, "the first export to be held", func() bool { return h.cache.InFlight() == 1 })

	// Two sessions end, each with its spans in: due at once, and waiting.
	h.postSpans("session-2", cacheSpan(12), carrierSpan(1))
	h.postSpans("session-3", cacheSpan(13), carrierSpan(1))
	waitMetric(t, h.svc, "cacheservice_export_roots_waiting", map[string]string{"scope": testScope}, 2)
	time.Sleep(200 * time.Millisecond) // 20 ticks
	if n := h.cache.Exports(); n != 1 {
		t.Fatalf("%d exports of the cache while one is in flight", n)
	}
	release()
	waitUntil(t, 10*time.Second, "everything to be stored", func() bool { return indexHas(t, h, 10, 11, 12, 13) })
	if n, peak := h.cache.Exports(), h.cache.Peak(); n != 2 || peak != 1 {
		t.Fatalf("%d exports, %d at once; want 2 (the waiting ones in one batch), 1 at once", n, peak)
	}
	waitMetric(t, h.svc, "cacheservice_export_roots_waiting", map[string]string{"scope": testScope}, 0)
}

// More roots due than one batch holds: the batches run one after another,
// never two at once, and the second starts when the first ends, not at the
// next tick (an hour here).
func TestStreamSplitBatchesInTurn(t *testing.T) {
	h, _ := newMemExportHarness(t, func(c *ExportConfig) { c.Tick = time.Hour })
	const n = 1001
	var spans = []*tracepb.Span{}
	for i := uint64(1); i <= n; i++ {
		h.cache.Add(fakeengine.Entry{Number: i, Layers: [][]byte{testLayer("split")}})
		spans = append(spans, cacheSpan(i))
	}
	spans = append(spans, carrierSpan(n))
	h.postSpans("session-1", spans...)
	waitUntil(t, 30*time.Second, "every root to be stored", func() bool {
		for _, e := range h.ns().tracker.Entries(h.cache.ID) {
			if !e.Stored {
				return false
			}
		}
		return true
	})
	if exports, peak := h.cache.Exports(), h.cache.Peak(); exports != 2 || peak != 1 {
		t.Fatalf("%d exports, %d at once; want 2, one at a time", exports, peak)
	}
}

// The explicit hook takes the cache's slot: it waits for the scheduler's
// export in flight, then runs, never beside it.
func TestStreamHookWaitsForSlot(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), carrierSpan(1))
	waitUntil(t, 5*time.Second, "the scheduler's export to be held", func() bool { return h.cache.InFlight() == 1 })
	h.postSpans("session-2", cacheSpan(11, "10"))

	var wg sync.WaitGroup
	var code int
	wg.Add(1)
	go func() {
		defer wg.Done()
		code, _ = h.debugExport(`{"roots":[11],"outputs":[{"number":11,"address":{"part":"fs"}}]}`)
	}()
	waitUntil(t, 5*time.Second, "the hook to wait for the slot", func() bool {
		return h.ns().tracker.SlotWaiters(h.cache.ID) == 1
	})
	time.Sleep(100 * time.Millisecond)
	if n := h.cache.Exports(); n != 1 {
		t.Fatalf("%d exports while the scheduler's is in flight", n)
	}
	release()
	wg.Wait()
	if code != http.StatusOK {
		t.Fatalf("hook status %d", code)
	}
	if !indexHas(t, h, 10, 11) || h.cache.Peak() != 1 {
		t.Fatalf("stored 10 and 11: %v; peak %d, want 1", indexHas(t, h, 10, 11), h.cache.Peak())
	}
}

// The drain still exports everything, one batch at a time per cache.
func TestStreamDrainOneAtATime(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	const n = 1001
	spans := []*tracepb.Span{}
	for i := uint64(1); i <= n; i++ {
		h.cache.Add(fakeengine.Entry{Number: i, Layers: [][]byte{testLayer("drain")}})
		spans = append(spans, cacheSpan(i))
	}
	h.postSpans("session-1", spans...)
	rep := drainFor(h.svc, 30*time.Second)
	if rep.Outcome != metrics.DrainComplete || rep.Stored[h.cache.ID] != n || rep.LeftRoots() != 0 {
		t.Fatalf("report %+v", rep)
	}
	if exports, peak := h.cache.Exports(), h.cache.Peak(); exports != 2 || peak != 1 {
		t.Fatalf("%d exports, %d at once; want 2, one at a time", exports, peak)
	}
}
