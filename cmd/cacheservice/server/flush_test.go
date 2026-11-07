package server

// The flush of one engine cache (zerobull-consulting/dagger#136): POST
// /v1/engines/{cacheID}/flush on the engine handler, its token form POST
// /v1/engines/flush, and the debug twin. The exports run through the
// scheduler, with fake engines behind the engine's own remotecache.Run; the
// real-engine version is cmd/cacheservice/e2e's flush scenario.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/engine/remotecache"
)

// flushResult is a flush request's answer.
type flushResult struct {
	code int
	rep  FlushReport
	body string
}

// postFlush posts a flush to url (path and query included) with token as
// the HTTP Basic username ("" for none). Its request is bounded.
func postFlush(t *testing.T, url, token string) flushResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.SetBasicAuth(token, "")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	res := flushResult{code: resp.StatusCode, body: string(data)}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, &res.rep); err != nil {
			t.Fatalf("%s: %v", data, err)
		}
	}
	return res
}

// flushURL is the cache-ID form's URL on an engine handler at base.
func flushURL(base, cacheID, timeout string) string {
	u := base + "/v1/engines/" + cacheID + "/flush"
	if timeout != "" {
		u += "?timeout=" + timeout
	}
	return u
}

// flush flushes the harness's cache with the engine's token, and wants 200.
func (h *exportHarness) flush(timeout string) FlushReport {
	h.t.Helper()
	res := postFlush(h.t, flushURL(h.srv.URL, h.cache.ID, timeout), "engine-token")
	if res.code != http.StatusOK {
		h.t.Fatalf("flush: status %d: %s", res.code, res.body)
	}
	return res.rep
}

// startFlush flushes in the background; the channel gets the answer.
func startFlush(t *testing.T, url, token string) <-chan flushResult {
	t.Helper()
	done := make(chan flushResult, 1)
	go func() { done <- postFlush(t, url, token) }()
	return done
}

func waitFlush(t *testing.T, done <-chan flushResult, timeout time.Duration) flushResult {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(timeout):
		t.Fatalf("the flush didn't return in %s", timeout)
		return flushResult{}
	}
}

func flushRunning(t *testing.T, done <-chan flushResult, what string) {
	t.Helper()
	select {
	case res := <-done:
		t.Fatalf("the flush returned %s: %d %s", what, res.code, res.body)
	default:
	}
}

// flushesUnderWay is how many flushes of cacheID are under way.
func flushesUnderWay(svc *Service, cacheID string) int {
	svc.flushes.mu.Lock()
	defer svc.flushes.mu.Unlock()
	n := 0
	for r := range svc.flushes.runs {
		if r.cacheID == cacheID {
			n++
		}
	}
	return n
}

// indexHasCache reports whether the harness's index names every number of
// cacheID, now.
func indexHasCache(t *testing.T, h *exportHarness, cacheID string, numbers ...uint64) bool {
	t.Helper()
	idx, err := h.meta.Index(context.Background(), testScope)
	if err != nil {
		t.Fatal(err)
	}
	c := idx.Caches[cacheID]
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

// connectStoppable connects another engine with cache and the engine's
// token, and returns its connection and what stops it.
func (h *exportHarness) connectStoppable(cache *fakeengine.Cache) (*engineConn, func()) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: cache}, remotecache.Config{EngineVersion: testEngineVersion, URL: h.srv.URL, Token: "engine-token", EngineName: cache.ID})
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	h.t.Cleanup(stop)
	var c *engineConn
	waitUntil(h.t, 10*time.Second, "engine "+cache.ID+" to connect", func() bool {
		c = h.svc.engineFor(h.ns(), cache.ID)
		return c != nil
	})
	return c, stop
}

// Entries not settled yet (Settle is a minute here) are exported at once,
// and the flush returns complete, with what it stored, long before Settle.
func TestFlushExportsUnsettledEntries(t *testing.T) {
	h, rec := newDrainHarness(t, nil)
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	if indexHas(t, h, 10) {
		t.Fatal("exported before Settle passed: the test can't tell the flush did it")
	}
	start := time.Now()
	rep := h.flush("20s")
	if took := time.Since(start); took > testQuiet/4 {
		t.Fatalf("the flush took %s: it waited for Settle", took)
	}
	if !indexHas(t, h, 10, 11) {
		t.Fatal("the entries weren't stored when the flush returned")
	}
	if rep.Outcome != metrics.FlushComplete || rep.CacheID != h.cache.ID || rep.Stored != 2 || rep.Left != (FlushLeft{}) || rep.Took == "" {
		t.Fatalf("report %+v", rep)
	}
	if h.svc.Draining() {
		t.Fatal("the flush made the service drain")
	}
	done := rec.find("flush: done")
	if len(done) != 1 || done[0]["outcome"] != metrics.FlushComplete || done[0]["cacheID"] != h.cache.ID ||
		done[0]["storedRoots"] != "2" || done[0]["leftRoots"] != "0" {
		t.Fatalf("flush: done logged %v", done)
	}
	waitMetric(t, h.svc, "cacheservice_flush_total", map[string]string{"outcome": metrics.FlushComplete}, 1)
	if flushesUnderWay(h.svc, h.cache.ID) != 0 {
		t.Fatal("the flush is still registered")
	}
}

// An ended session's announced spans that arrive after the flush began are
// taken, and exported before it returns.
func TestFlushTakesLateSpans(t *testing.T) {
	// Settle long, so a flush that doesn't make the late entry due at once
	// times out; the wait for the missing span is then at most 2s.
	h, _ := newDrainHarness(t, func(c *ExportConfig) { c.Tracker.Settle = 30 * time.Second })
	h.build()
	h.postSpans("session-1", cacheSpan(10), carrierSpan(2))
	done := startFlush(t, flushURL(h.srv.URL, h.cache.ID, "20s"), "engine-token")
	waitUntil(t, 5*time.Second, "the flush to export what was in", func() bool { return indexHas(t, h, 10) })
	flushRunning(t, done, "with a counted span still missing")
	h.postSpans("session-1", cacheSpan(11, "10"))
	res := waitFlush(t, done, 15*time.Second)
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushComplete || res.rep.Stored != 2 || res.rep.Left != (FlushLeft{}) {
		t.Fatalf("flush: %d %+v %s", res.code, res.rep, res.body)
	}
	if !indexHas(t, h, 11) {
		t.Fatal("the late span's entry wasn't stored when the flush returned")
	}
}

// The wait for missing spans is reported, and bounded: with them never
// arriving, the flush returns complete once Settle (at most 2s) passed.
func TestFlushWaitsForMissingSpansBounded(t *testing.T) {
	h, _ := newDrainHarness(t, func(c *ExportConfig) { c.Tracker.Settle = 30 * time.Second })
	h.build()
	h.postSpans("session-1", cacheSpan(10), carrierSpan(2))
	rep := h.flush("0s")
	if rep.Outcome != metrics.FlushTimeout || rep.Left.WaitingSpans != 1 || rep.Left.Dirty != 1 {
		t.Fatalf("timeout=0 report %+v", rep)
	}
	start := time.Now()
	rep = h.flush("20s")
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("the flush took %s waiting for spans that never came", took)
	}
	if rep.Outcome != metrics.FlushComplete || rep.Stored != 1 {
		t.Fatalf("report %+v", rep)
	}
}

// A retry waiting out its backoff is exported at once. One that is busy
// again, and retries after the timeout, is reported later, and not waited
// for.
func TestFlushRetries(t *testing.T) {
	h, _ := newDrainHarness(t, func(c *ExportConfig) { c.Tracker.BusyBackoff = time.Hour })
	h.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}, Busy: true})
	h.postSpans("session-1", cacheSpan(10))
	// The explicit hook exports it now: busy, so it waits an hour.
	if code, _ := h.debugExport(""); code != http.StatusOK {
		t.Fatalf("export: %d", code)
	}
	if st := h.ns().tracker.FlushStatus(h.cache.ID, time.Now(), time.Now().Add(time.Minute)); st.Later != 1 {
		t.Fatalf("not waiting out its backoff: %+v", st)
	}

	// timeout=0 only reports: the retry's backoff holds.
	if rep := h.flush("0s"); rep.Outcome != metrics.FlushTimeout || rep.Left.Later != 1 {
		t.Fatalf("timeout=0: report %+v", rep)
	}
	time.Sleep(200 * time.Millisecond) // 20 scheduler ticks
	if busy := metricstest.Read(t, h.svc.registry, "cacheservice_export_roots_total", map[string]string{"result": "busy"}); busy.Value != 1 {
		t.Fatalf("timeout=0 made the retry due: %v busy answers", busy.Value)
	}

	// Busy again: due an hour after the flush's own try.
	start := time.Now()
	rep := h.flush("10s")
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the flush took %s: it waited for a retry due after its timeout", took)
	}
	if rep.Outcome != metrics.FlushTimeout || rep.Left.Later != 1 || rep.Stored != 0 {
		t.Fatalf("busy again: report %+v", rep)
	}
	// The hook's try and the flush's: the flush made the retry due at once.
	waitMetric(t, h.svc, "cacheservice_export_roots_total", map[string]string{"result": "busy"}, 2)

	// No longer busy: the flush makes the retry due at once again.
	h.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{testLayer("base")}})
	rep = h.flush("10s")
	if rep.Outcome != metrics.FlushComplete || rep.Stored != 1 || !indexHas(t, h, 10) {
		t.Fatalf("not busy: report %+v", rep)
	}
}

// The timeout passes with an export in flight: the flush returns timeout,
// with it counted in flight, and the export isn't canceled: it runs on, and
// stores its roots once released, with no second try.
func TestFlushTimeoutLeavesExportInFlight(t *testing.T) {
	h, rec := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	rep := h.flush("500ms")
	if rep.Outcome != metrics.FlushTimeout || rep.Left.InFlight != 2 || rep.Stored != 0 {
		t.Fatalf("report %+v", rep)
	}
	if h.cache.InFlight() != 1 {
		t.Fatal("the export in flight was canceled at the timeout")
	}
	if done := rec.find("flush: done"); len(done) != 1 || done[0]["outcome"] != metrics.FlushTimeout || done[0]["inFlight"] != "2" {
		t.Fatalf("flush: done logged %v", done)
	}
	release()
	h.waitIndex(10, 11)
	if n := h.cache.Exports(); n != 1 {
		t.Fatalf("%d exports: the one in flight was cut off and tried again", n)
	}
	waitMetric(t, h.svc, "cacheservice_flush_unexported_roots_total", nil, 2)
}

// A canceled request ends the wait, not the export.
func TestFlushCanceledRequestLeavesExport(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan FlushReport, 1)
	go func() {
		rep, _ := h.svc.flush(ctx, h.ns(), h.cache.ID, time.Minute)
		done <- rep
	}()
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })
	cancel()
	select {
	case rep := <-done:
		if rep.Outcome != metrics.FlushCanceled || rep.Left.InFlight != 2 {
			t.Fatalf("report %+v", rep)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the canceled flush didn't return")
	}
	if h.cache.InFlight() != 1 {
		t.Fatal("canceling the flush canceled the export")
	}
	release()
	h.waitIndex(10, 11)
}

// timeout=0 makes nothing due, exports nothing, and reports what is
// pending.
func TestFlushTimeoutZeroOnlyReports(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	rep := h.flush("0")
	if rep.Outcome != metrics.FlushTimeout || rep.Left.Dirty != 2 || rep.Stored != 0 {
		t.Fatalf("report %+v", rep)
	}
	time.Sleep(200 * time.Millisecond) // 20 scheduler ticks
	if h.cache.Exports() != 0 || indexHas(t, h, 10) {
		t.Fatal("timeout=0 exported")
	}
	// Nothing pending: complete.
	h2, _ := newDrainHarness(t, nil)
	if rep := h2.flush("0s"); rep.Outcome != metrics.FlushComplete || rep.Left != (FlushLeft{}) {
		t.Fatalf("nothing pending: report %+v", rep)
	}
}

// A flush of one cache makes only that cache's entries due, and waits only
// for it.
func TestFlushOneCacheOnly(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	other := fakeengine.New("cache-b", 4)
	other.Add(fakeengine.Entry{Number: 20, Layers: [][]byte{testLayer("other")}})
	h.connectEngine(other)
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	h.postSpansOf(other, "session-2", cacheSpan(20))
	rep := h.flush("5s")
	if rep.Outcome != metrics.FlushComplete || rep.Stored != 2 || rep.Left != (FlushLeft{}) {
		t.Fatalf("report %+v", rep)
	}
	time.Sleep(200 * time.Millisecond)
	if indexHasCache(t, h, other.ID, 20) || other.Exports() != 0 {
		t.Fatal("the other cache's entry was made due")
	}
	if st := h.ns().tracker.FlushStatus(other.ID, time.Now(), time.Time{}); st.Pending != 1 {
		t.Fatalf("the other cache: %+v", st)
	}
}

// New spans, sessions and engines are taken during a flush, and what they
// make dirty in the flushed cache is exported before it returns.
func TestFlushTakesNewWork(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	h.cache.Add(fakeengine.Entry{Number: 12, Deps: []uint64{11}, Layers: [][]byte{testLayer("base"), testLayer("exec"), testLayer("new")}})
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	done := startFlush(t, flushURL(h.srv.URL, h.cache.ID, "20s"), "engine-token")
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })

	h.postSpans("session-2", cacheSpan(12, "11"))
	other := fakeengine.New("cache-b", 4)
	h.connectEngine(other)
	if st := h.ns().tracker.FlushStatus(h.cache.ID, time.Now(), time.Time{}); st.Pending != 1 {
		t.Fatalf("the new session's span wasn't taken: %+v", st)
	}
	release()
	res := waitFlush(t, done, 15*time.Second)
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushComplete || res.rep.Stored != 3 {
		t.Fatalf("flush: %d %+v", res.code, res.rep)
	}
	if !indexHas(t, h, 10, 11, 12) {
		t.Fatal("the new session's entry wasn't stored")
	}
}

// not_connected, at once, when the cache's engine never connected, or
// disconnects during the flush.
func TestFlushEngineGone(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	res := postFlush(t, flushURL(h.srv.URL, "never-connected", "1m"), "engine-token")
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushNotConnected {
		t.Fatalf("never connected: %d %+v", res.code, res.rep)
	}

	other := fakeengine.New("cache-b", 4)
	other.Add(fakeengine.Entry{Number: 20, Layers: [][]byte{testLayer("other")}})
	_, stop := h.connectStoppable(other)
	release := other.Hold()
	defer release()
	h.postSpansOf(other, "session-1", cacheSpan(20))
	start := time.Now()
	done := startFlush(t, flushURL(h.srv.URL, other.ID, "1m"), "engine-token")
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return other.InFlight() == 1 })
	stop()
	res = waitFlush(t, done, 10*time.Second)
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushNotConnected || res.rep.Left.Roots() != 1 {
		t.Fatalf("disconnected: %d %+v", res.code, res.rep)
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Fatalf("took %s", took)
	}
}

// Two flushes of one cache wait on the same state, and both return once
// it's done, each with its own report.
func TestFlushConcurrent(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	release := h.cache.Hold()
	defer release()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	url := flushURL(h.srv.URL, h.cache.ID, "20s")
	first := startFlush(t, url, "engine-token")
	waitUntil(t, 5*time.Second, "the export to be held", func() bool { return h.cache.InFlight() == 1 })
	second := startFlush(t, url, "engine-token")
	waitUntil(t, 5*time.Second, "both flushes to wait", func() bool { return flushesUnderWay(h.svc, h.cache.ID) == 2 })
	flushRunning(t, first, "with the export held")
	release()
	for i, done := range []<-chan flushResult{first, second} {
		res := waitFlush(t, done, 10*time.Second)
		if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushComplete || res.rep.Stored != 2 {
			t.Fatalf("flush %d: %d %+v", i+1, res.code, res.rep)
		}
	}
	if n := h.cache.Exports(); n != 1 {
		t.Fatalf("%d exports: the flushes exported beside each other", n)
	}
}

// The token form flushes the cache of the connections that authenticated
// with the request's token; with none it reports not_connected, and with
// more than one cache it answers 409.
func TestFlushTokenForm(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	res := postFlush(t, h.srv.URL+"/v1/engines/flush?timeout=20s", "engine-token")
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushComplete || res.rep.CacheID != h.cache.ID || res.rep.Stored != 2 {
		t.Fatalf("flush: %d %+v %s", res.code, res.rep, res.body)
	}
	res = postFlush(t, h.srv.URL+"/v1/engines/flush", "another-token")
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushNotConnected || res.rep.CacheID != "" {
		t.Fatalf("another token: %d %+v %s", res.code, res.rep, res.body)
	}
	h.connectEngine(fakeengine.New("cache-b", 4))
	if res := postFlush(t, h.srv.URL+"/v1/engines/flush", "engine-token"); res.code != http.StatusConflict {
		t.Fatalf("two caches: %d %s", res.code, res.body)
	}
}

// The debug twin flushes a connection's cache.
func TestFlushDebugTwin(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	res := postFlush(t, h.debug.URL+"/debug/engines/"+strconv.FormatUint(h.engine.id, 10)+"/flush?timeout=20s", "")
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushComplete || res.rep.Stored != 2 {
		t.Fatalf("flush: %d %+v %s", res.code, res.rep, res.body)
	}
	if res := postFlush(t, h.debug.URL+"/debug/engines/999/flush", ""); res.code != http.StatusNotFound {
		t.Fatalf("no such engine: %d", res.code)
	}
}

// Bad requests: no token (401), a bad cache ID or timeout (400).
func TestFlushBadRequests(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	for _, tc := range []struct {
		url, token string
		code       int
	}{
		{flushURL(h.srv.URL, h.cache.ID, ""), "", http.StatusUnauthorized},
		{flushURL(h.srv.URL, "bad%20id", ""), "engine-token", http.StatusBadRequest},
		{flushURL(h.srv.URL, strings.Repeat("a", maxCacheIDLen+1), ""), "engine-token", http.StatusBadRequest},
		{flushURL(h.srv.URL, h.cache.ID, "soon"), "engine-token", http.StatusBadRequest},
		{flushURL(h.srv.URL, h.cache.ID, "-1s"), "engine-token", http.StatusBadRequest},
		{flushURL(h.srv.URL, h.cache.ID, "11m"), "engine-token", http.StatusBadRequest},
		{h.srv.URL + "/v1/engines/flush?timeout=soon", "engine-token", http.StatusBadRequest},
	} {
		if res := postFlush(t, tc.url, tc.token); res.code != tc.code {
			t.Errorf("%s (token %q): %d, want %d: %s", tc.url, tc.token, res.code, tc.code, res.body)
		}
	}
	// The cap itself is taken.
	if res := postFlush(t, flushURL(h.srv.URL, h.cache.ID, MaxFlushTimeout.String()), "engine-token"); res.code != http.StatusOK {
		t.Fatalf("timeout at the cap: %d %s", res.code, res.body)
	}
}

// With tokens: the cache's owning token flushes it; another token of its
// namespace gets 403; another namespace's token gets the same answer as for
// an unknown cache; an unknown token 401.
func TestFlushAuthWithTokens(t *testing.T) {
	h := newAuthHarness(t)
	cache := fakeengine.New("cache-a", 4)
	authBuild(cache, "a")
	h.connect(cache, "tok-a")
	if code := h.postSpans("tok-a", cache, "session-1", cacheSpan(10), cacheSpan(11, "10")); code != http.StatusOK {
		t.Fatalf("spans: %d", code)
	}
	url := flushURL(h.srv.URL, cache.ID, "20s")
	if res := postFlush(t, url, "tok-a2"); res.code != http.StatusForbidden {
		t.Fatalf("another token of the namespace: %d %s", res.code, res.body)
	}
	if res := postFlush(t, url, "unknown"); res.code != http.StatusUnauthorized {
		t.Fatalf("unknown token: %d", res.code)
	}
	elsewhere := postFlush(t, url, "tok-b")
	unknown := postFlush(t, flushURL(h.srv.URL, "no-such-cache", "20s"), "tok-b")
	elsewhere.rep.Took, unknown.rep.Took, unknown.rep.CacheID = "", "", elsewhere.rep.CacheID
	if elsewhere.code != unknown.code || elsewhere.rep != unknown.rep || elsewhere.rep.Outcome != metrics.FlushNotConnected {
		t.Fatalf("another namespace's token: %d %+v; unknown cache: %d %+v", elsewhere.code, elsewhere.rep, unknown.code, unknown.rep)
	}
	if indexHasIn(t, h, "team-a", cache.ID, 10) {
		t.Fatal("a refused flush exported")
	}
	res := postFlush(t, url, "tok-a")
	if res.code != http.StatusOK || res.rep.Outcome != metrics.FlushComplete || res.rep.Stored != 2 {
		t.Fatalf("the owner's token: %d %+v %s", res.code, res.rep, res.body)
	}
	h.waitIndex("team-a", cache.ID, 10, 11)
}

func indexHasIn(t *testing.T, h *authHarness, ns, cacheID string, n uint64) bool {
	t.Helper()
	idx, err := h.meta.Index(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	c := idx.Caches[cacheID]
	if c == nil {
		return false
	}
	_, ok := c.Numbers[n]
	return ok
}

// Without tokens, as in the sidecar, any token is taken, as on the
// WebSocket.
func TestFlushAuthWithoutTokens(t *testing.T) {
	h, _ := newDrainHarness(t, nil)
	for _, token := range []string{"engine-token", "anything", "sidecar-engine"} {
		if res := postFlush(t, flushURL(h.srv.URL, h.cache.ID, "1s"), token); res.code != http.StatusOK {
			t.Fatalf("token %q: %d %s", token, res.code, res.body)
		}
	}
}

// Refusals: export off and read-only answer 409; draining and stopping
// answer 503.
func TestFlushRefusals(t *testing.T) {
	serve := func(t *testing.T, svc *Service) string {
		srv := httptest.NewServer(svc.Handler())
		t.Cleanup(srv.Close)
		return srv.URL
	}
	off := New(slog.New(slog.DiscardHandler), "")
	readOnly := New(slog.New(slog.DiscardHandler), "")
	if err := readOnly.SetReadOnly(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		svc  *Service
		want string
	}{{"export off", off, ErrExportOff.Error()}, {"read-only", readOnly, ErrReadOnly.Error()}} {
		for _, path := range []string{"/v1/engines/cache-a/flush", "/v1/engines/flush"} {
			res := postFlush(t, serve(t, tc.svc)+path, "engine-token")
			if res.code != http.StatusConflict || !strings.Contains(res.body, tc.want) {
				t.Fatalf("%s %s: %d %s", tc.name, path, res.code, res.body)
			}
		}
	}

	h, _ := newDrainHarness(t, nil)
	drainFor(h.svc, time.Second)
	for _, path := range []string{flushURL(h.srv.URL, h.cache.ID, ""), h.srv.URL + "/v1/engines/flush"} {
		if res := postFlush(t, path, "engine-token"); res.code != http.StatusServiceUnavailable || !strings.Contains(res.body, ErrDraining.Error()) {
			t.Fatalf("draining %s: %d %s", path, res.code, res.body)
		}
	}

	h2, _ := newDrainHarness(t, nil)
	h2.svc.CloseEngines()
	if res := postFlush(t, flushURL(h2.srv.URL, h2.cache.ID, ""), "engine-token"); res.code != http.StatusServiceUnavailable || !strings.Contains(res.body, ErrStopping.Error()) {
		t.Fatalf("stopping: %d %s", res.code, res.body)
	}
	if res := postFlush(t, h2.debug.URL+"/debug/engines/1/flush", ""); res.code != http.StatusServiceUnavailable {
		t.Fatalf("stopping, debug twin: %d %s", res.code, res.body)
	}
}

// The tracker side: StartFlush makes a cache's dirty entries due at once,
// and its retries once; ending it restores Settle.
func TestTrackerStartFlush(t *testing.T) {
	now := time.Now()
	tr := exporter.NewTracker(exporter.TrackerConfig{Settle: time.Minute, MaxDelay: time.Minute})
	tr.Observe(exporter.Observation{CacheID: "c", Generation: 1, Session: "s", Number: 10, State: true, Retained: true, Parts: []string{`{"part":"fs"}`}, EndUnixNano: now.UnixNano()}, now)
	if bs := tr.Due(now); len(bs) != 0 {
		t.Fatal("due before Settle")
	}
	end := tr.StartFlush("c", now)
	bs := tr.Due(now)
	if len(bs) != 1 {
		t.Fatal("not due during the flush")
	}
	tr.Done(bs[0], exporter.Result{Stored: []uint64{10}}, now)
	end()
	end() // idempotent
	tr.Observe(exporter.Observation{CacheID: "c", Generation: 1, Session: "s", Number: 11, State: true, Retained: true, Parts: []string{`{"part":"fs"}`}, EndUnixNano: now.UnixNano()}, now)
	if bs := tr.Due(now); len(bs) != 0 {
		t.Fatal("still flushing after the flush ended")
	}
}
