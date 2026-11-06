package server

// The linger after a drain (zerobull-consulting/dagger#102): the engine
// address keeps answering the engines' OTLP requests, so an engine stopped
// after the drain delivers engine.stop, and the linger ends once every
// connected engine whose events come here sent it. An engine closes its
// connection before it sends engine.stop, so a disconnect ends nothing. The
// binary's flag, its bound and the second signal are tested in
// cmd/cacheservice (main_linger_test.go).

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/dagger/dagger/engine/telemetryattrs"
)

// postEngineEvent posts one engine event about cacheID (generation 1) with
// token, as the engine's event export does, and returns the status.
func postEngineEvent(t *testing.T, srv *httptest.Server, token, cacheID, kind, body string) int {
	t.Helper()
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: telemetryattrs.EngineEventScope},
			LogRecords: []*logspb.LogRecord{{
				Body: strValue(body),
				Attributes: []*commonpb.KeyValue{
					kv(telemetryattrs.EngineEventAttr, kind),
					kv(telemetryattrs.EngineCacheAttr, cacheID+"/1"),
				},
			}},
		}},
	}}}
	return otlpPostAs(t, srv.URL+"/v1/logs", token, req)
}

// lingerTest is a store-less service with engines welcomed, drained, and
// lingering in the background.
type lingerTest struct {
	t    *testing.T
	svc  *Service
	rec  *logRecorder
	srv  *httptest.Server
	done chan LingerReport
	// started is when the linger started.
	started time.Time
}

func newLingerTest(t *testing.T) *lingerTest {
	t.Helper()
	svc, rec, srv := newTestService(t, "")
	return &lingerTest{t: t, svc: svc, rec: rec, srv: srv}
}

// linger drains the service, then starts its linger, bounded by bound.
func (l *lingerTest) linger(bound time.Duration) context.CancelFunc {
	l.t.Helper()
	if rep := l.svc.Drain(context.Background()); rep.Outcome != "complete" {
		l.t.Fatalf("the drain ended %q", rep.Outcome)
	}
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	l.t.Cleanup(cancel)
	l.done = make(chan LingerReport, 1)
	l.started = time.Now()
	go func() { l.done <- l.svc.Linger(ctx) }()
	return cancel
}

// still fails the test if the linger ends within d.
func (l *lingerTest) still(d time.Duration, why string) {
	l.t.Helper()
	select {
	case rep := <-l.done:
		l.t.Fatalf("%s: the linger ended (%+v)", why, rep)
	case <-time.After(d):
	}
}

// ended waits for the linger's end, at most within d.
func (l *lingerTest) ended(d time.Duration) LingerReport {
	l.t.Helper()
	select {
	case rep := <-l.done:
		return rep
	case <-time.After(d):
		l.t.Fatalf("the linger didn't end within %s", d)
		return LingerReport{}
	}
}

func (l *lingerTest) event(cacheID, kind string) {
	l.t.Helper()
	if code := postEngineEvent(l.t, l.srv, "engine-token", cacheID, kind, `{"clean":true}`); code != http.StatusOK {
		l.t.Fatalf("POST %s of %s: %d", kind, cacheID, code)
	}
}

// After the drain, the engine address still answers OTLP: the engine's
// spans and events are answered 200 while the linger waits. Its engine
// closing its connection, as a real engine does first in its stop, doesn't
// end the linger; its engine.stop does, at once.
func TestLingerEndsOnEngineStopNotOnDisconnect(t *testing.T) {
	l := newLingerTest(t)
	e := welcomedRaw(t, l.srv, "cache-a")
	waitEngines(t, l.svc, 1)
	l.event("cache-a", telemetryattrs.EngineEventStart)
	l.linger(time.Minute)
	l.still(300*time.Millisecond, "with the engine connected")

	// Answered, and dropped: spans of a session the service never tracked.
	spans := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{Name: "call"}}}},
	}}}
	if code := otlpPostAs(t, l.srv.URL+"/v1/traces", "engine-token", spans); code != http.StatusOK {
		t.Fatalf("POST /v1/traces while lingering: %d, want 200", code)
	}

	// The engine's stop: it disconnects first.
	_ = e.conn.CloseNow()
	waitEngines(t, l.svc, 0)
	l.still(500*time.Millisecond, "after the engine disconnected, before its engine.stop")

	l.event("cache-a", telemetryattrs.EngineEventStop)
	rep := l.ended(2 * time.Second)
	if rep.Outcome != LingerStopped {
		t.Fatalf("outcome %q, want %q", rep.Outcome, LingerStopped)
	}
	if len(rep.Caches) != 1 || !rep.Caches[0].Stopped || rep.Caches[0].CacheID != "cache-a" {
		t.Fatalf("caches %+v, want cache-a stopped", rep.Caches)
	}
	if got := l.rec.find("linger: done"); len(got) != 1 || got[0]["outcome"] != LingerStopped || got[0]["stopped"] != "1" {
		t.Fatalf("linger: done logged %v", got)
	}
	if got := l.rec.find("engine event"); len(got) != 2 || got[1]["kind"] != telemetryattrs.EngineEventStop {
		t.Fatalf("engine.stop wasn't logged as an engine event: %v", got)
	}
	// GET /debug/drain reports it.
	st := l.svc.DrainStatus()
	if st.Linger == nil || st.Linger.Outcome != LingerStopped {
		t.Fatalf("drain status' linger: %+v", st.Linger)
	}
}

// It waits for every engine whose events come here: one engine.stop ends
// nothing while another engine hasn't sent its own. An engine whose events
// go elsewhere (none arrived here) isn't waited for.
func TestLingerWaitsForEveryEngineWhoseEventsComeHere(t *testing.T) {
	l := newLingerTest(t)
	welcomedRaw(t, l.srv, "cache-a")
	welcomedRaw(t, l.srv, "cache-b")
	welcomedRaw(t, l.srv, "cache-elsewhere")
	waitEngines(t, l.svc, 3)
	l.event("cache-a", telemetryattrs.EngineEventStart)
	l.event("cache-b", telemetryattrs.EngineEventPrune)
	l.linger(time.Minute)
	l.event("cache-a", telemetryattrs.EngineEventStop)
	l.still(300*time.Millisecond, "with cache-b's engine.stop still due")
	l.event("cache-b", telemetryattrs.EngineEventStop)
	rep := l.ended(2 * time.Second)
	if rep.Outcome != LingerStopped || len(rep.Caches) != 2 {
		t.Fatalf("report %+v, want cache-a and cache-b stopped", rep)
	}
}

// An engine that never sends engine.stop holds the linger only until its
// bound.
func TestLingerEndsAtItsBound(t *testing.T) {
	l := newLingerTest(t)
	welcomedRaw(t, l.srv, "cache-a")
	waitEngines(t, l.svc, 1)
	l.event("cache-a", telemetryattrs.EngineEventStart)
	const bound = 700 * time.Millisecond
	l.linger(bound)
	rep := l.ended(bound + 2*time.Second)
	took := time.Since(l.started)
	if rep.Outcome != LingerDeadline {
		t.Fatalf("outcome %q, want %q", rep.Outcome, LingerDeadline)
	}
	if took < bound || took > bound+time.Second {
		t.Fatalf("the linger took %s, want its bound %s", took, bound)
	}
	if got := l.rec.find("linger: no engine.stop"); len(got) != 1 || got[0]["cacheID"] != "cache-a" {
		t.Fatalf("the engine left waiting wasn't logged: %v", got)
	}
}

// Canceled (a second signal), it ends at once.
func TestLingerCanceledEndsAtOnce(t *testing.T) {
	l := newLingerTest(t)
	welcomedRaw(t, l.srv, "cache-a")
	waitEngines(t, l.svc, 1)
	l.event("cache-a", telemetryattrs.EngineEventStart)
	cancel := l.linger(time.Minute)
	l.still(200*time.Millisecond, "before the cancel")
	start := time.Now()
	cancel()
	if rep := l.ended(time.Second); rep.Outcome != LingerCanceled {
		t.Fatalf("outcome %q, want %q", rep.Outcome, LingerCanceled)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("the canceled linger took %s to end", took)
	}
}

// With no connected engine whose events come here, there is nothing to wait
// for: it returns at once. Nor does it linger without a drain.
func TestLingerNothingToWaitFor(t *testing.T) {
	l := newLingerTest(t)
	welcomedRaw(t, l.srv, "cache-elsewhere")
	waitEngines(t, l.svc, 1)
	if rep := l.svc.Linger(context.Background()); rep.Outcome != LingerNothing {
		t.Fatalf("without a drain: outcome %q, want %q", rep.Outcome, LingerNothing)
	}
	// An engine.stop seen before the drain doesn't count as one in it.
	l.event("cache-gone", telemetryattrs.EngineEventStart)
	l.linger(time.Minute)
	if rep := l.ended(time.Second); rep.Outcome != LingerNothing || len(rep.Caches) != 0 {
		t.Fatalf("report %+v, want nothing to wait for", rep)
	}
}

// An engine.stop that arrived before the drain, from an earlier stop of the
// same engine cache, doesn't end the linger: only one sent while draining
// does.
func TestLingerIgnoresAnEngineStopFromBeforeTheDrain(t *testing.T) {
	l := newLingerTest(t)
	welcomedRaw(t, l.srv, "cache-a")
	waitEngines(t, l.svc, 1)
	l.event("cache-a", telemetryattrs.EngineEventStop)
	l.event("cache-a", telemetryattrs.EngineEventStart)
	l.linger(time.Minute)
	l.still(300*time.Millisecond, "with only an engine.stop from before the drain")
	l.event("cache-a", telemetryattrs.EngineEventStop)
	if rep := l.ended(2 * time.Second); rep.Outcome != LingerStopped {
		t.Fatalf("outcome %q, want %q", rep.Outcome, LingerStopped)
	}
}

// With tokens, an engine.stop is taken only from the token of the cache's
// engine, as its other events are: another token of the namespace can't end
// the linger for it.
func TestLingerEngineStopOnlyFromItsEngine(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	tokens, err := ParseTokens([]byte("tok-a team\ntok-client team\n"))
	if err != nil {
		t.Fatal(err)
	}
	svc.SetTokens(tokens)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	l := &lingerTest{t: t, svc: svc, srv: srv}
	e := dialRaw(t, srv, "tok-a")
	e.helloAs("cache-a")
	waitEngines(t, svc, 1)
	if code := postEngineEvent(t, srv, "tok-a", "cache-a", telemetryattrs.EngineEventStart, `{}`); code != http.StatusOK {
		t.Fatalf("engine.start: %d", code)
	}
	l.linger(time.Minute)
	if code := postEngineEvent(t, srv, "tok-client", "cache-a", telemetryattrs.EngineEventStop, `{"clean":true}`); code != http.StatusOK {
		t.Fatalf("engine.stop from another token: %d", code)
	}
	l.still(300*time.Millisecond, "after an engine.stop from another token")
	if code := postEngineEvent(t, srv, "tok-a", "cache-a", telemetryattrs.EngineEventStop, `{"clean":true}`); code != http.StatusOK {
		t.Fatalf("engine.stop: %d", code)
	}
	if rep := l.ended(2 * time.Second); rep.Outcome != LingerStopped {
		t.Fatalf("outcome %q, want %q", rep.Outcome, LingerStopped)
	}
}

// The caches remembered sending events here are bounded: a token naming
// cache IDs without end doesn't grow the service's memory without end.
func TestLingerEventCachesBounded(t *testing.T) {
	n := &namespace{name: "ns"}
	for i := range maxEventCaches + 10 {
		n.sawEvent("cache-"+string(rune(i)), false)
	}
	if got := len(n.eventCaches); got != maxEventCaches {
		t.Fatalf("%d caches remembered, want %d", got, maxEventCaches)
	}
	// One already remembered is still marked stopped.
	n.sawEvent("cache-"+string(rune(0)), true)
	if _, stopped := n.eventState("cache-" + string(rune(0))); !stopped {
		t.Fatal("a remembered cache's engine.stop wasn't recorded")
	}
}
