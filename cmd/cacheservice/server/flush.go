package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
)

// The flush (zerobull-consulting/dagger#136): an orchestrator about to stop
// one engine asks the service, still fully up, to export what is pending
// for that engine's cache, within a bound it chooses, and to say what is
// left. It isn't a drain: the service goes on taking new engines, sessions
// and spans, and other caches are scheduled as usual.

const (
	// DefaultFlushTimeout is a flush's timeout when its request names none.
	DefaultFlushTimeout = 10 * time.Second
	// MaxFlushTimeout is the longest timeout a flush may ask for. It is
	// fixed rather than -drain-timeout's: the sidecar's new stop
	// (zerobull-consulting/dagger#137) runs its service with
	// -drain-timeout 0, and flushes before it stops the engine.
	MaxFlushTimeout = 10 * time.Minute
)

// errAmbiguousFlush is the token form's refusal when the token's
// connections hold more than one engine cache.
var errAmbiguousFlush = errors.New("the token's engine connections hold more than one engine cache: flush each with POST /v1/engines/{cacheID}/flush")

// FlushLeft is what a flush left of its cache, as exporter.DrainState counts
// it. Each root is counted once: in flight first, then dirty, then waiting
// for a retry.
type FlushLeft struct {
	// Dirty are the roots worth exporting, changed since an export last
	// claimed them, and not in flight or waiting out a retry.
	Dirty int `json:"dirty"`
	// WaitingSpans are the sessions whose spans may still come: not ended,
	// or ended with some of the cache spans their carrier counted missing,
	// waited for until Settle (at most 2s) passed since their last span.
	WaitingSpans int `json:"waitingSpans"`
	// InFlight are the roots an export holds. A flush never cancels it: it
	// runs on until it ends, or its engine stops.
	InFlight int `json:"inFlight"`
	// Retrying are the roots waiting for another try due before the flush's
	// timeout passed, and Later those whose try is due after it.
	Retrying int `json:"retrying"`
	Later    int `json:"later"`
}

// Roots is how many roots were left.
func (l FlushLeft) Roots() int { return l.Dirty + l.InFlight + l.Retrying + l.Later }

func flushLeftOf(st exporter.DrainState) FlushLeft {
	return FlushLeft{Dirty: st.Pending, WaitingSpans: st.Sessions, InFlight: st.InFlight, Retrying: st.Retrying, Later: st.Later}
}

// FlushReport is what a flush did, for one engine cache.
type FlushReport struct {
	// Outcome is metrics.FlushComplete (nothing left), FlushTimeout,
	// FlushNotConnected or FlushCanceled.
	Outcome string `json:"outcome"`
	// CacheID is the cache flushed; empty when the token form found none.
	CacheID string `json:"cacheID,omitempty"`
	// Took is how long the flush took, as a Go duration ("1.234s").
	Took string `json:"took"`
	// Stored are the roots stored by exports of the cache that ended during
	// the flush.
	Stored int       `json:"stored"`
	Left   FlushLeft `json:"left"`
}

// flushRun is a flush under way, which counts the roots its cache's exports
// stored (flushStored).
type flushRun struct {
	ns      *namespace
	cacheID string
	stored  atomic.Int64
}

// flushRuns are the flushes under way.
type flushRuns struct {
	mu   sync.Mutex
	runs map[*flushRun]struct{}
}

func (s *Service) addFlushRun(ns *namespace, cacheID string) *flushRun {
	r := &flushRun{ns: ns, cacheID: cacheID}
	s.flushes.mu.Lock()
	defer s.flushes.mu.Unlock()
	if s.flushes.runs == nil {
		s.flushes.runs = map[*flushRun]struct{}{}
	}
	s.flushes.runs[r] = struct{}{}
	return r
}

func (s *Service) removeFlushRun(r *flushRun) {
	s.flushes.mu.Lock()
	defer s.flushes.mu.Unlock()
	delete(s.flushes.runs, r)
}

// flushStored counts roots stored by an export of a namespace's cache, for
// each flush of it under way (see exportStored). It runs before the tracker hears of the
// export, so a flush that then sees nothing in flight has counted it.
func (s *Service) flushStored(ns *namespace, cacheID string, roots int) {
	s.flushes.mu.Lock()
	defer s.flushes.mu.Unlock()
	for r := range s.flushes.runs {
		if r.ns == ns && r.cacheID == cacheID {
			r.stored.Add(int64(roots))
		}
	}
}

// changed returns a channel closed the next time a tracker calls kick: an
// export ended, or something came due sooner. Flushes wait on it.
func (p *exportPath) changed() <-chan struct{} {
	p.changedMu.Lock()
	defer p.changedMu.Unlock()
	return p.changedCh
}

// broadcast wakes everything waiting on changed, if a flush waits.
func (p *exportPath) broadcast() {
	if p.changedWaiters.Load() == 0 {
		return
	}
	p.changedMu.Lock()
	defer p.changedMu.Unlock()
	close(p.changedCh)
	p.changedCh = make(chan struct{})
}

// flushRefusal is the error of a flush the service refuses now: the export
// path off or read-only, a drain, or the stop begun.
func (s *Service) flushRefusal() error {
	// The service's state first: a read-only service drains and stops too,
	// and says so as any other does.
	p := s.export
	switch {
	case s.draining.Load():
		return ErrDraining
	case s.closing.Load():
		return ErrStopping
	case p == nil:
		return s.exportOff()
	case p.ctx.Err() != nil:
		return ErrStopping
	}
	return nil
}

// flush exports what is pending for one engine cache of a namespace, and
// reports what is left (zerobull-consulting/dagger#136):
//
//   - It makes every dirty entry of the cache due now, without waiting for
//     it to settle, and every root of it waiting out a retry's backoff due
//     now, once; a retry after that waits its normal backoff. A failed root
//     whose wait began with a flush's try that failed is left to wait it
//     out, unless its value changed (see exporter.Tracker.StartFlush).
//     Entries that become dirty during the flush are due at once too.
//   - It waits for the cache's sessions whose spans may still come: not
//     ended, or ended with some of the spans their carrier counted missing,
//     until Settle (at most 2s) passed since their last span; their late
//     spans are taken and exported.
//   - The scheduler runs the exports, one at a time per cache: it starts
//     none of its own.
//   - It isn't a drain: new engines, sessions and spans are taken, and an
//     engine still running builds can keep it busy until its timeout.
//
// It returns once nothing is dirty, waiting for spans, in flight or due by
// the timeout for the cache (complete), or once the timeout passes, or only
// retries due after it are left (timeout); at once, with what is left, when
// the cache's engine isn't connected or disconnects (not_connected); and
// when ctx is canceled or the export path stops (canceled). It never
// cancels an export: one in flight when it returns runs on. A timeout of 0
// makes nothing due, and only reports. The timeout is measured on the
// export path's clock, which the tracker's retries and session waits use
// too, so what the report counts as due before it is what was waited for.
//
// The error is a refusal: flushRefusal's.
func (s *Service) flush(ctx context.Context, ns *namespace, cacheID string, timeout time.Duration) (FlushReport, error) {
	if err := s.flushRefusal(); err != nil {
		return FlushReport{}, err
	}
	p := s.export
	start := time.Now()
	deadline := p.now().Add(timeout)
	tracker := ns.tracker
	run := s.addFlushRun(ns, cacheID)
	defer s.removeFlushRun(run)
	end := func(outcome string) FlushReport {
		return s.endFlush(ns, cacheID, run, start, outcome, deadline)
	}

	connected := func() bool { return s.engineFor(ns, cacheID) != nil }
	if !connected() {
		return end(metrics.FlushNotConnected), nil
	}
	if timeout <= 0 {
		st := tracker.FlushStatus(cacheID, p.now(), deadline)
		if flushEmpty(st) {
			return end(metrics.FlushComplete), nil
		}
		return end(metrics.FlushTimeout), nil
	}

	endFlush := tracker.StartFlush(cacheID, p.now())
	defer endFlush()
	// The deadline and the wait are on one clock, the export path's, as
	// the tracker's retries are: the wait ends when that clock reaches the
	// deadline, checked at each wake (a timer set to what is left of it,
	// the poll, or a kick), never on a wall-clock timeout of its own.
	p.changedWaiters.Add(1)
	defer p.changedWaiters.Add(-1)
	poll := time.NewTicker(min(p.tick, maxDrainPoll))
	defer poll.Stop()
	// left fires when what is left of the deadline passed, as last seen.
	left := time.NewTimer(timeout)
	defer left.Stop()
	for {
		// Before the status: a kick after it wakes the wait below.
		changed := p.changed()
		now := p.now()
		st := tracker.FlushStatus(cacheID, now, deadline)
		switch {
		case flushEmpty(st):
			return end(metrics.FlushComplete), nil
		case p.ctx.Err() != nil, s.closing.Load():
			// The stop began (the paths' context ended, or CloseEngines
			// ran, in either order): its engines are closed, which isn't
			// their engine going away.
			return end(metrics.FlushCanceled), nil
		case !connected():
			return end(metrics.FlushNotConnected), nil
		case st.InFlight == 0 && st.Pending == 0 && st.Retrying == 0 && st.Sessions == 0:
			// Only retries due after the timeout: not waited for.
			return end(metrics.FlushTimeout), nil
		case !now.Before(deadline):
			return end(metrics.FlushTimeout), nil
		}
		left.Reset(deadline.Sub(now))
		select {
		case <-changed:
		case <-poll.C:
		case <-left.C:
		case <-p.ctx.Done():
			return end(metrics.FlushCanceled), nil
		case <-ctx.Done():
			return end(metrics.FlushCanceled), nil
		}
	}
}

// flushEmpty reports whether a cache has nothing left a flush waits for or
// reports: no root left, and no session waited for.
func flushEmpty(st exporter.DrainState) bool {
	return st.Roots() == 0 && st.Sessions == 0
}

// endFlush logs and measures a flush's end, and returns its report.
func (s *Service) endFlush(ns *namespace, cacheID string, run *flushRun, start time.Time, outcome string, deadline time.Time) FlushReport {
	left := flushLeftOf(ns.tracker.FlushStatus(cacheID, s.export.now(), deadline))
	took := time.Since(start)
	stored := int(run.stored.Load())
	rep := FlushReport{
		Outcome: outcome,
		CacheID: cacheID,
		Took:    took.Round(time.Millisecond).String(),
		Stored:  stored,
		Left:    left,
	}
	s.metrics.FlushEnded(outcome, took, left.Roots())
	level := s.logger.Info
	if left.Roots() > 0 || outcome == metrics.FlushTimeout || outcome == metrics.FlushCanceled {
		level = s.logger.Warn
	}
	level("flush: done", "namespace", ns.name, "cacheID", cacheID, "outcome", outcome,
		"took", took.Round(time.Millisecond), "storedRoots", stored, "leftRoots", left.Roots(),
		"dirty", left.Dirty, "waitingSpans", left.WaitingSpans, "inFlight", left.InFlight,
		"retrying", left.Retrying, "later", left.Later)
	return rep
}

// notConnectedReport is the token form's report when none of the token's
// connections is welcomed: nothing was flushed.
func (s *Service) notConnectedReport(ns *namespace, start time.Time) FlushReport {
	took := time.Since(start)
	s.metrics.FlushEnded(metrics.FlushNotConnected, took, 0)
	// The same fields as every flush's (endFlush), with no cache.
	s.logger.Info("flush: done", "namespace", ns.name, "cacheID", "", "outcome", metrics.FlushNotConnected,
		"took", took.Round(time.Millisecond), "storedRoots", 0, "leftRoots", 0,
		"dirty", 0, "waitingSpans", 0, "inFlight", 0, "retrying", 0, "later", 0,
		"reason", "no engine connection with the request's token")
	return FlushReport{Outcome: metrics.FlushNotConnected, Took: took.Round(time.Millisecond).String()}
}

// flushTimeout reads a flush request's timeout: DefaultFlushTimeout when
// it names none, and a Go duration from 0 to MaxFlushTimeout otherwise.
func flushTimeout(r *http.Request) (time.Duration, error) {
	q := r.URL.Query()
	if !q.Has("timeout") {
		return DefaultFlushTimeout, nil
	}
	v := q.Get("timeout")
	d, err := time.ParseDuration(v)
	switch {
	case err != nil:
		return 0, fmt.Errorf("timeout: %w", err)
	case d < 0:
		return 0, errors.New("timeout: negative")
	case d > MaxFlushTimeout:
		return 0, fmt.Errorf("timeout: more than %s", MaxFlushTimeout)
	}
	return d, nil
}

// flushAuth authenticates a flush request as the engine's own requests are:
// the HTTP Basic username is the token, and must be a known one when the
// service has tokens; without tokens, any token is taken, as on the
// WebSocket. It answers 401 itself.
func (s *Service) flushAuth(w http.ResponseWriter, r *http.Request) (*namespace, tokenKey, bool) {
	token, _, _ := r.BasicAuth()
	if token == "" {
		s.reject(w, r, endpointFlush, "")
		return nil, tokenKey{}, false
	}
	ns, ok := s.authenticate(w, r, endpointFlush, token)
	if !ok {
		return nil, tokenKey{}, false
	}
	return ns, keyOf(token), true
}

// serveFlush is POST /v1/engines/{cacheID}/flush, on the engine handler.
func (s *Service) serveFlush(w http.ResponseWriter, r *http.Request) {
	ns, key, ok := s.flushAuth(w, r)
	if !ok {
		return
	}
	cacheID := r.PathValue("cacheID")
	if !validEngineID(cacheID) {
		http.Error(w, "bad cache ID", http.StatusBadRequest)
		return
	}
	s.flushOne(w, r, ns, key, cacheID)
}

// serveFlushToken is POST /v1/engines/flush, the form that needs no cache
// ID: it flushes the engine cache of the welcomed connections that
// authenticated with the request's token, in its namespace. With none, it
// reports not_connected; with more than one cache, it answers 409, and the
// caller flushes each by its ID.
func (s *Service) serveFlushToken(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ns, key, ok := s.flushAuth(w, r)
	if !ok {
		return
	}
	// Before the caches: a bad timeout is a 400, and a refusal (draining,
	// stopping, export off) its own status, whatever the token holds.
	if _, err := flushTimeout(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.flushRefusal(); err != nil {
		writeFlushError(w, err)
		return
	}
	caches := s.cachesOfToken(ns, key)
	switch len(caches) {
	case 0:
		writeJSON(w, s.notConnectedReport(ns, start))
	case 1:
		s.flushOne(w, r, ns, key, caches[0])
	default:
		http.Error(w, errAmbiguousFlush.Error(), http.StatusConflict)
	}
}

// flushOne checks the owner of cacheID, then flushes it and answers with the
// report.
func (s *Service) flushOne(w http.ResponseWriter, r *http.Request, ns *namespace, key tokenKey, cacheID string) {
	timeout, err := flushTimeout(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// With tokens, only the token of the cache's latest connection speaks
	// for it, as for its engine events. A cache no connection of this
	// namespace named, as one of another namespace, is accepted, and
	// reported not connected: the answer doesn't say whether it exists
	// elsewhere.
	if s.authenticated() && !ns.ownerAccepts(cacheID, key) {
		s.logger.Warn("flush refused: the token isn't the engine cache's", "namespace", ns.name, "cacheID", cacheID)
		http.Error(w, "forbidden: the token isn't this engine cache's", http.StatusForbidden)
		return
	}
	rep, err := s.flush(r.Context(), ns, cacheID, timeout)
	if err != nil {
		writeFlushError(w, err)
		return
	}
	writeJSON(w, rep)
}

// cachesOfToken are the engine caches of the welcomed connections of a
// namespace that authenticated with token key, sorted.
func (s *Service) cachesOfToken(ns *namespace, key tokenKey) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, c := range s.conns {
		if c.ns == ns && c.key == key && !seen[c.hello.CacheID] {
			seen[c.hello.CacheID] = true
			out = append(out, c.hello.CacheID)
		}
	}
	slices.Sort(out)
	return out
}

// serveDebugFlush is POST /debug/engines/{conn}/flush: the flush of a
// connection's engine cache, on the debug API.
func (s *Service) serveDebugFlush(w http.ResponseWriter, r *http.Request) {
	conn, ok := debugConnID(w, r)
	if !ok {
		return
	}
	timeout, err := flushTimeout(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.flushRefusal(); err != nil {
		writeDebugError(w, err)
		return
	}
	c, err := s.conn(conn)
	if err != nil {
		writeDebugError(w, err)
		return
	}
	rep, err := s.flush(r.Context(), c.ns, c.CacheID(), timeout)
	if err != nil {
		writeDebugError(w, err)
		return
	}
	writeJSON(w, rep)
}

// writeFlushError answers a flush refused: 409 when the export path is off
// or read-only, 503 while draining or stopping.
func writeFlushError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrDraining), errors.Is(err, ErrStopping):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, ErrExportOff), errors.Is(err, ErrReadOnly):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
