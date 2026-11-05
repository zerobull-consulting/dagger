package server

// The stop path (zerobull-consulting/dagger#77 items 2, 4 and 5,
// zerobull-consulting/dagger#73, zerobull-consulting/dagger#74 point 4):
// CloseEngines bounded and off the service's lock, an engine welcomed before
// the drain or refused, no export started past the drain's deadline, and the
// GC runs asked for waited for. The binary's bound and signals are tested in
// cmd/cacheservice (stop_test.go).

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// welcomedRaw dials a raw engine and has it welcomed.
func welcomedRaw(t *testing.T, srv *httptest.Server, cacheID string) *rawEngine {
	t.Helper()
	e := dialRaw(t, srv, "engine-token")
	if env := e.helloAs(cacheID); env.Type != protocol.TypeWelcome {
		t.Fatalf("hello of %s answered with %+v", cacheID, env)
	}
	return e
}

// Engines that never answer the close frame (raw engines that stop reading)
// don't stretch CloseEngines past its bound: their close handshakes run at
// once, not one after another, and are dropped when the bound passes. The
// service's lock isn't held meanwhile, and an engine that answers still gets
// StatusGoingAway.
func TestCloseEnginesBoundedAndOffTheLock(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	const silent = 4
	for i := range silent {
		// Welcomed, then never read again: the close frame is never
		// answered. Unanswered, one handshake waits 5s.
		welcomedRaw(t, srv, "silent-"+string(rune('a'+i)))
	}
	answering := welcomedRaw(t, srv, "answering")
	status := make(chan websocket.StatusCode, 1)
	go func() { status <- answering.closed() }()
	waitEngines(t, svc, silent+1)

	const bound = 500 * time.Millisecond
	start := time.Now()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		defer cancel()
		svc.CloseEnginesContext(ctx)
	}()
	// The answering engine's close handshake is done long before the
	// bound: CloseEngines is then waiting on the silent ones.
	select {
	case got := <-status:
		if got != websocket.StatusGoingAway {
			t.Fatalf("an engine that answers was closed with %d, want %d", got, websocket.StatusGoingAway)
		}
	case <-time.After(bound):
		t.Fatal("the answering engine's connection wasn't closed within the bound")
	}
	// Engines takes the service's lock: it returns while CloseEngines still
	// waits on the silent engines.
	engines := make(chan struct{})
	go func() {
		svc.Engines()
		close(engines)
	}()
	select {
	case <-engines:
	case <-closed:
		t.Fatal("CloseEngines returned before the silent engines' bound: the test can't tell it holds the lock")
	}
	select {
	case <-closed:
		t.Fatal("Engines returned only once CloseEngines did: CloseEngines held the service's lock")
	default:
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("CloseEngines didn't return")
	}
	// The bound, plus closeDropWait at most for the dropped connections.
	if took := time.Since(start); took > bound+closeDropWait+time.Second {
		t.Fatalf("CloseEngines took %s with %d silent engines, bound %s", took, silent, bound)
	}
	waitUntil(t, 5*time.Second, "the dropped engines to be gone", func() bool { return len(svc.Engines()) == 0 })
}

// Once CloseEngines runs, no new engine is welcomed: the engines it closed
// reconnect within about a second, while the HTTP servers still listen, and
// would otherwise be welcomed after it took its list, and never closed. A
// connection upgraded before is closed with 1001 at its hello, and a new one
// gets 503 before the upgrade.
func TestCloseEnginesRefusesNewEngines(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	upgraded := dialRaw(t, srv, "engine-token")
	svc.CloseEngines()
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "late", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	upgraded.send(env)
	if got := upgraded.closed(); got != websocket.StatusGoingAway {
		t.Fatalf("a hello after CloseEngines was closed with %d, want %d", got, websocket.StatusGoingAway)
	}
	if n := len(svc.Engines()); n != 0 {
		t.Fatalf("%d engines welcomed after CloseEngines", n)
	}
	if _, code := dialAs(t, srv, "engine-token"); code != http.StatusServiceUnavailable {
		t.Fatalf("a new engine after CloseEngines got %d, want 503", code)
	}
}

// A hello whose token was revoked is refused as revoked (4001), even while
// the service drains: the engine must learn its token is gone, not retry as
// if the service were only busy.
func TestRevokedHelloDuringDrainIsRevoked(t *testing.T) {
	h := newAuthHarness(t)
	raw, code := dialAs(t, h.srv, "tok-b")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %d", code)
	}
	drainFor(h.svc, time.Minute)
	h.reload("tok-a team-a\n")
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "cache-b", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	raw.send(env)
	if got := raw.closed(); got != protocol.CloseTokenRevoked {
		t.Fatalf("a revoked hello while draining was closed with %d, want %d", got, protocol.CloseTokenRevoked)
	}
	if n := countRefused(h.svc, metrics.DrainRefusedConnection); n != 0 {
		t.Fatalf("the revoked hello was counted as %d refused by the drain", n)
	}
}

// Connections that are still closing after CloseEngines dropped them make it
// report an error, so the stop isn't taken as clean. The drop is made to do
// nothing here, as if the library didn't close the connection at once.
func TestCloseEnginesReportsConnectionsStillClosing(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	// Never read again: the close frame is never answered.
	welcomedRaw(t, srv, "silent")
	c := waitEngines(t, svc, 1)[0]
	drop := c.drop
	c.drop = func() {}
	defer drop()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := svc.CloseEnginesContext(ctx); err == nil || !strings.Contains(err.Error(), "still closing") {
		t.Fatalf("CloseEngines with a connection still closing reported %v", err)
	}
}

// An engine whose hello races the drain's start is either welcomed before
// the drain started, and counted in its report (Engines), or refused with
// 1013 and counted as refused: never welcomed unseen by the drain. The hook
// widens the window between reading a hello and registering the connection,
// where the drain's start used to slip in; run with -race.
func TestDrainWelcomesOrRefusesEveryHello(t *testing.T) {
	const (
		iterations = 150
		engines    = 4
	)
	missed := 0
	for i := range iterations {
		svc := New(slog.New(slog.DiscardHandler), "")
		svc.testHookHello = func() { time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond) }
		srv := httptest.NewServer(svc.Handler())
		raws := make([]*rawEngine, engines)
		for j := range raws {
			raws[j] = dialRaw(t, srv, "engine-token")
		}
		var welcomed, refused atomic.Int64
		start := make(chan struct{})
		var wg sync.WaitGroup
		for j, e := range raws {
			wg.Go(func() {
				<-start
				env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "cache-" + string(rune('a'+j)), Generation: 1})
				if err != nil {
					t.Error(err)
					return
				}
				data, _ := json.Marshal(env)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := e.conn.Write(ctx, websocket.MessageText, data); err != nil {
					t.Error(err)
					return
				}
				_, reply, err := e.conn.Read(ctx)
				switch {
				case err == nil && strings.Contains(string(reply), `"`+string(protocol.TypeWelcome)+`"`):
					welcomed.Add(1)
				case websocket.CloseStatus(err) == websocket.StatusTryAgainLater:
					refused.Add(1)
				default:
					t.Errorf("hello answered with %q, %v", reply, err)
				}
			})
		}
		var rep DrainReport
		wg.Go(func() {
			<-start
			rep = svc.Drain(context.Background())
		})
		close(start)
		wg.Wait()
		if w, r := int(welcomed.Load()), int(refused.Load()); w != rep.Engines || w+r != engines || countRefused(svc, metrics.DrainRefusedConnection) != r {
			missed++
			t.Errorf("iteration %d: %d welcomed, %d refused (%d counted); the drain counted %d engines", i, w, r, countRefused(svc, metrics.DrainRefusedConnection), rep.Engines)
		}
		svc.CloseEngines()
		srv.Close()
		if missed > 3 {
			t.Fatal("stopping: engines welcomed unseen by the drain")
		}
	}
}

// A root whose retry comes due just after the drain's deadline isn't
// started: the pass that would start it begins past the deadline, and the
// drain ends there, as deadline, with the root left, not in flight. The
// clock is the export path's, so the deadline and the drain's tick can't
// race: the pass sees the time the test set.
func TestDrainDoesNotStartRetryPastItsDeadline(t *testing.T) {
	clock := gc.NewNow(time.Now())
	const backoff = time.Hour
	h, _ := newDrainHarness(t, func(c *ExportConfig) {
		c.Now = clock.Now
		c.Tracker.BusyBackoff = backoff
		c.Tracker.MaxBusyBackoff = backoff
		// A session that ended with spans still to come keeps the drain
		// busy until its deadline.
		c.Tracker.EndGrace = 10 * backoff
	})
	digests := h.build()
	h.cache.CorruptUploads(digests[1], 3)
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	h.postSpans("session-2", carrierSpan(5))
	// The deadline is a minute away on both clocks; the retry, an hour
	// after the failure, comes due after it.
	deadline := clock.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	done := make(chan DrainReport, 1)
	go func() { done <- h.svc.Drain(ctx) }()
	waitUntil(t, 10*time.Second, "the drain's export to fail, with a retry after the deadline", func() bool {
		for _, e := range h.ns().tracker.Entries(h.cache.ID) {
			if e.Failures > 0 && e.RetryAt != "" {
				return true
			}
		}
		return false
	})
	exports := h.cache.Exports()
	drainRunning(t, done, "before its deadline, with a session waiting for its spans")
	// The retry is due now, and the deadline is past.
	clock.Add(backoff + time.Second)
	rep := waitDrain(t, done, 5*time.Second)
	if rep.Outcome != metrics.DrainDeadline {
		t.Fatalf("outcome %q, want %q: %+v", rep.Outcome, metrics.DrainDeadline, rep)
	}
	if len(rep.Left) != 1 || rep.Left[0].InFlight != 0 || rep.Left[0].Later+rep.Left[0].Retrying != 2 {
		t.Fatalf("the retry past the deadline was started, or not reported left: %+v", rep)
	}
	if n := h.cache.Exports(); n != exports {
		t.Fatalf("%d exports reached the engine past the deadline", n-exports)
	}
}

// A drain canceled (a second signal) reports canceled even when what it
// waited for stopped being work it could do as it woke: here its engine went
// away then. Ending at the top of the next pass instead would find nothing
// busy, and report complete.
func TestDrainCanceledReportsCanceled(t *testing.T) {
	clock := gc.NewNow(time.Now())
	h, _ := newDrainHarness(t, func(c *ExportConfig) {
		c.Now = clock.Now
		// A session that ended with spans still to come keeps the drain
		// busy while its engine is connected.
		c.Tracker.EndGrace = time.Hour
	})
	h.postSpans("session-1", carrierSpan(5))
	h.svc.testHookDrainWoken = func() {
		if err := h.svc.CloseEngine(h.engine.id); err != nil {
			t.Error(err)
		}
		waitUntil(t, 5*time.Second, "the engine to go", func() bool { return !h.svc.connected(h.engine) })
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan DrainReport, 1)
	go func() { done <- h.svc.Drain(ctx) }()
	waitUntil(t, 5*time.Second, "the drain to start", h.svc.Draining)
	drainRunning(t, done, "with a session waiting for its spans")
	cancel()
	if rep := waitDrain(t, done, 10*time.Second); rep.Outcome != metrics.DrainCanceled {
		t.Fatalf("outcome %q, want %q: %+v", rep.Outcome, metrics.DrainCanceled, rep)
	}
}

// holdDeleteBucket holds GC's Delete until its context ends, then holds it
// on until release, as a run that unwinds mid-delete, through work that
// ignores cancellation (GC's finish update), would.
type holdDeleteBucket struct {
	gc.Bucket
	deleting  chan struct{}
	unwinding chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (b *holdDeleteBucket) Delete(ctx context.Context, keys []string) ([]string, error) {
	b.once.Do(func() { close(b.deleting) })
	<-ctx.Done()
	close(b.unwinding)
	<-b.release
	return keys, ctx.Err()
}

// A GC run asked for (POST /debug/gc) that is mid-delete when the stop
// starts is canceled, and Wait returns only once it ended: it doesn't leave
// the run unwinding. zerobull-consulting/dagger#74 point 4.
func TestWaitWaitsForGCRunAskedFor(t *testing.T) {
	st := newGCStores(t, nil)
	h := newExportHarness(t, st.blobs, st.meta, nil)
	// The namespace has an index (GC leaves one without alone), and a
	// staged upload older than the staging age: the run deletes it.
	if _, err := st.meta.UpdateIndex(context.Background(), testScope, func(*metastore.Index) error { return nil }); err != nil {
		t.Fatal(err)
	}
	key := "ns/"+testScope+"/staging/" + strings.Repeat("ab", 16) + "/sha256/" + strings.Repeat("0", 64)
	if _, err := st.objs.Put(context.Background(), key, []byte("staged"), metastore.Condition{}); err != nil {
		t.Fatal(err)
	}
	st.clock.Add(gc.DefaultStagingAge + time.Hour)
	bucket := &holdDeleteBucket{Bucket: st.bucket, deleting: make(chan struct{}), unwinding: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-bucket.release:
		default:
			close(bucket.release)
		}
	}()
	if err := h.svc.EnableGC(h.ctx, GCConfig{Collector: gc.Config{Bucket: bucket, Meta: st.meta, Now: st.clock.Now}}); err != nil {
		t.Fatal(err)
	}
	ran := make(chan error, 1)
	go func() {
		_, err := h.svc.CollectGarbage(context.Background(), false)
		ran <- err
	}()
	select {
	case <-bucket.deleting:
	case <-time.After(5 * time.Second):
		t.Fatal("the GC run didn't reach its delete")
	}
	// The stop: the paths' context ends, then Wait.
	h.cancel()
	<-bucket.unwinding
	waited := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waited <- h.svc.WaitContext(ctx)
	}()
	// Shows Wait doesn't return while the run unwinds: it would return
	// within a few milliseconds if it didn't wait for the run.
	select {
	case err := <-waited:
		t.Fatalf("Wait returned (%v) while the GC run asked for was still unwinding mid-delete", err)
	case <-time.After(200 * time.Millisecond):
	}
	if r := h.svc.Running(); r.GC != 1 {
		t.Fatalf("running %+v: the GC run asked for isn't counted", r)
	}
	close(bucket.release)
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait didn't return once the GC run ended")
	}
	if err := <-ran; err == nil {
		t.Fatal("the canceled GC run reported no error")
	}
	// Once the service waits, a run asked for is refused.
	if _, err := h.svc.CollectGarbage(context.Background(), false); !errors.Is(err, ErrStopping) {
		t.Fatalf("a GC run asked for after the stop: %v", err)
	}
}

// WaitContext returns at its bound with the work still running, and
// Running says what it is.
func TestWaitContextBounded(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	release := svc.HoldWaitForTest()
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := svc.WaitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitContext with held work: %v", err)
	}
	if r := svc.Running(); r.Held != 1 {
		t.Fatalf("running %+v", r)
	}
	release()
	if err := svc.WaitContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}
