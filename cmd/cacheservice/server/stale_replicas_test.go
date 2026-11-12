package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// startReplica starts another service on the same stores as a harness: a
// replica on the same prefix. It returns the replica's URL.
func startReplica(t *testing.T, ctx context.Context, blobs *blobstore.MemStore, meta *metastore.Store) (*Service, string) {
	t.Helper()
	svc := New(slog.New(slog.DiscardHandler), "")
	if err := svc.EnableExport(ctx, ExportConfig{
		Exporter: exporter.Config{Blobs: blobs, Meta: meta, Scope: testScope, RetryBackoff: time.Millisecond},
		Tracker:  exporter.TrackerConfig{Settle: time.Minute, MaxDelay: time.Minute},
		Tick:     10 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.EnableImport(ctx, ImportConfig{Importer: importer.Config{Blobs: blobs, Meta: meta, Scope: testScope}}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(svc.Handler())
	t.Cleanup(func() {
		srv.Close()
		svc.Wait()
	})
	return svc, srv.URL
}

// zerobull-consulting/dagger#129's realistic path, with two replicas on one
// prefix. Engine cache-a restarts (generation 5) and connects to replica 1,
// which merges a stored value into it and records the import: the index now
// knows generation 5. The old engine (generation 4), still connected to
// replica 2, exports its build there late, and the index refuses it as a
// stale generation's, before any upload. Then the restarted engine
// reconnects to replica 2, which only now hears of generation 5. Replica 2
// exports the build again, from the restarted engine, and the index records
// it, although no session of the new generation names those entries.
func TestStaleRefusalClearedAcrossReplicas(t *testing.T) {
	t.Run("hello", func(t *testing.T) { testStaleRefusalClearedAcrossReplicas(t, false) })
	// The restarted engine's engine.start reaches replica 2 before its
	// hello does: the root waits for the hello, instead of being exported
	// from the old engine, still connected, and refused for good.
	t.Run("event before hello", func(t *testing.T) { testStaleRefusalClearedAcrossReplicas(t, true) })
}

func testStaleRefusalClearedAcrossReplicas(t *testing.T, eventFirst bool) {
	ctx := context.Background()
	blobs, meta := newMemStores(t)
	// Replica 2, with the old engine.
	h := newExportHarness(t, blobs, meta, func(c *ExportConfig) { c.NoEngineWait = 100 * time.Millisecond })
	if h.cache.Generation != 4 {
		t.Fatalf("setup: the engine is generation %d", h.cache.Generation)
	}
	layers := h.build()

	// A value of another cache, stored, for replica 1 to merge.
	z := fakeengine.New("cache-z", 1)
	z.Add(fakeengine.Entry{Number: 30, Recipe: "from(busybox)", Layers: [][]byte{testLayer("zbase")}})
	h.connectEngine(z)
	h.postSpansOf(z, "session-z", cacheSpan(30), carrierSpan(1))
	waitMapped(t, meta, "cache-z", 30)

	// The restarted engine: the same cache, with the same entries, at
	// generation 5, connected to replica 1.
	restarted := fakeengine.New("cache-a", 5)
	restarted.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}})
	restarted.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Layers: [][]byte{testLayer("base"), testLayer("exec")}})
	r1ctx, stopR1 := context.WithCancel(h.ctx)
	_, r1URL := startReplica(t, r1ctx, blobs, meta)
	// r1done is closed when the restarted engine leaves replica 1.
	r1done := make(chan struct{})
	go func() {
		defer close(r1done)
		_ = remotecache.Run(r1ctx, fakeengine.Adapter{Cache: restarted}, remotecache.Config{EngineVersion: testEngineVersion, URL: r1URL, Token: "engine-token", EngineName: "engine-a"})
	}()
	// Cleanups run last first: this one before replica 1's, which waits
	// for its connections to end, even when the test fails before stopR1.
	t.Cleanup(func() {
		stopR1()
		<-r1done
	})
	waitIndexFor(t, meta, func(idx *metastore.Index) bool {
		c := idx.Caches["cache-a"]
		return c != nil && c.Generation == 5 && len(c.Imported) == 1
	})
	// The restarted engine leaves replica 1 (a reconnect, a rollout).
	stopR1()
	select {
	case <-r1done:
	case <-time.After(10 * time.Second):
		t.Fatal("the restarted engine didn't leave replica 1")
	}

	// The old engine's session ends on replica 2: refused, uploading nothing.
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitFor(t, "the stale refusal", func() bool {
		refused := 0
		for _, e := range h.ns().tracker.Entries(h.cache.ID) {
			if e.Refused == exporter.StaleGenerationMessage {
				refused++
			}
		}
		return refused == 2
	})
	for _, d := range layers {
		if has, err := h.blobs.Has(ctx, d); err != nil || has {
			t.Fatalf("layer %s stored by the stale export (%v)", d, err)
		}
	}

	if eventFirst {
		h.postEventOf(restarted, telemetryattrs.EngineEventStart, "{}")
		// A few export ticks with only the old engine connected.
		time.Sleep(200 * time.Millisecond)
		for _, e := range h.ns().tracker.Entries(h.cache.ID) {
			if e.Refused != "" || e.Stored {
				t.Fatalf("entry %+v, before the restarted engine's hello: want neither refused nor stored", e)
			}
		}
	}

	// The restarted engine reconnects to replica 2: the build is exported
	// from it, and recorded at generation 5.
	h.connectEngine(restarted)
	idx := waitIndexFor(t, meta, func(idx *metastore.Index) bool {
		c := idx.Caches["cache-a"]
		if c == nil {
			return false
		}
		m, ok := c.Numbers[11]
		_, root := idx.Roots[m.Number]
		return ok && m.Generation == 5 && root
	})
	if m := idx.Caches["cache-a"].Numbers[10]; m.Generation != 5 {
		t.Fatalf("entry 10's mapping %+v", m)
	}
	for _, d := range layers {
		if has, err := h.blobs.Has(ctx, d); err != nil || !has {
			t.Fatalf("layer %s not stored (%v)", d, err)
		}
	}
	// Once: not exported again after it was stored.
	exports := restarted.Exports()
	time.Sleep(300 * time.Millisecond)
	if n := restarted.Exports(); n != exports {
		t.Fatalf("exported again: %d exports, was %d", n, exports)
	}
	for _, e := range h.ns().tracker.Entries(h.cache.ID) {
		if (e.Number == 10 || e.Number == 11) && (!e.Stored || e.Refused != "") {
			t.Fatalf("entry %+v, want stored", e)
		}
	}
}

// waitFor waits, up to 20 seconds, until ok holds.
func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// An older generation's engine that connects after a newer one, as a process
// still running beside its restart, doesn't take the exports from it: the
// newest generation's connection is chosen, not the latest connection.
func TestOlderGenerationConnectingLaterDoesNotTakeExports(t *testing.T) {
	blobs, meta := newMemStores(t)
	h := newExportHarness(t, blobs, meta, func(c *ExportConfig) { c.NoEngineWait = 100 * time.Millisecond })
	build := func(c *fakeengine.Cache) {
		c.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}})
		c.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Layers: [][]byte{testLayer("base"), testLayer("exec")}})
	}
	restarted := fakeengine.New("cache-a", 5)
	build(restarted)
	h.connectEngine(restarted)
	// connectEngine returns on any connection of cache-a, which the
	// harness's engine already is: wait for this one, so the older engine
	// below connects after it.
	waitEngines(t, h.svc, 2)
	old := fakeengine.New("cache-a", 4)
	build(old)
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: old}, remotecache.Config{EngineVersion: testEngineVersion, URL: h.srv.URL, Token: "engine-token", EngineName: "engine-old"})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitEngines(t, h.svc, 3)
	if c := h.svc.engineFor(h.ns(), "cache-a"); c == nil || c.Generation() != 5 {
		t.Fatalf("engineFor chose %+v, want the generation-5 connection", c)
	}
	h.postSpansOf(restarted, "session-5", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitIndexFor(t, meta, func(idx *metastore.Index) bool {
		c := idx.Caches["cache-a"]
		if c == nil {
			return false
		}
		m, ok := c.Numbers[11]
		return ok && m.Generation == 5
	})
	if n := old.Exports(); n != 0 {
		t.Fatalf("the older engine exported %d times", n)
	}
}

// A batch claimed at a newer generation than the cache's only connection waits
// for that generation's hello (runBatch requeues it with NoEngineWait). When
// the hello arrives, the batch is exported at once, not at the end of its
// wait (zerobull-consulting/dagger#132).
func TestNewerGenerationBatchExportsAtHello(t *testing.T) {
	const wait = 10 * time.Second
	blobs, meta := newMemStores(t)
	h := newExportHarness(t, blobs, meta, func(c *ExportConfig) { c.NoEngineWait = wait })
	restarted := fakeengine.New("cache-a", 5)
	restarted.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}})
	restarted.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Layers: [][]byte{testLayer("base"), testLayer("exec")}})

	// Generation 5's session ends before its hello reaches this service
	// (another replica has it): the old engine, generation 4, is the only
	// connection, so the batch is requeued to wait for the hello.
	h.postSpansOf(restarted, "session-5", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	waitFor(t, "the batch to wait for the hello", func() bool {
		for _, e := range h.ns().tracker.Entries(h.cache.ID) {
			if e.RetryAt != "" {
				return true
			}
		}
		return false
	})

	h.connectEngine(restarted)
	start := time.Now()
	waitFor(t, "the export from the newer generation", func() bool { return restarted.Exports() > 0 })
	// Without the wake the export comes at the end of the wait; the bound
	// is half of it, far above what a loaded runner needs.
	d := time.Since(start)
	t.Logf("exported %s after the hello (NoEngineWait %s)", d, wait)
	if d > wait/2 {
		t.Fatalf("exported %s after the hello, want at once (the wait is %s)", d, wait)
	}
}

// A hello that ran Connected before a batch was put to wait for its engine
// woke nothing: choosing the connection again after the wait is marked wakes
// the batch (zerobull-consulting/dagger#225: runBatch choosing the old
// connection, the newer engine's hello, then the requeue; and the same with an
// interrupted export). The scheduler exports a woken batch at once, and an hour
// from then otherwise.
func TestEngineWaitWokenByConnectionFoundAfterwards(t *testing.T) {
	for _, tc := range []struct {
		name string
		wait func(h *exportHarness, b exporter.Batch)
	}{
		{"requeue", func(h *exportHarness, b exporter.Batch) { h.svc.requeueForEngine(h.ns(), b) }},
		{"interrupted export", func(h *exportHarness, b exporter.Batch) {
			// The engine's connection dropped mid-export: it is no longer
			// one of the service's, and the restarted engine is.
			dropped := &engineConn{id: 1 << 40, ns: h.ns(), logger: h.svc.logger}
			if out := h.svc.recordExport(context.Background(), dropped, b, exporter.Result{}, errors.New("cut short")); out != metrics.ExportInterrupted {
				t.Fatalf("outcome %q, want interrupted", out)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blobs, meta := newMemStores(t)
			h := newExportHarness(t, blobs, meta, func(c *ExportConfig) { c.NoEngineWait = time.Hour })
			restarted := fakeengine.New("cache-a", 5)
			restarted.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}})
			h.postSpansOf(restarted, "session-5", cacheSpan(10))
			// The batch is claimed by hand, as runBatch holds it, at
			// generation 5.
			batches := h.ns().tracker.All("cache-a", h.svc.export.now())
			if len(batches) != 1 || batches[0].Generation() != 5 {
				t.Fatalf("setup: batches %+v", batches)
			}
			// The newer engine says hello, and Connected runs, with
			// nothing waiting.
			h.connectEngine(restarted)
			waitEngines(t, h.svc, 2)
			h.ns().tracker.Connected("cache-a", 5, h.svc.export.now())

			tc.wait(h, batches[0])
			waitFor(t, "the export from the connected engine", func() bool { return restarted.Exports() > 0 })
		})
	}
}
