package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
)

// ErrDraining is returned for work the service refuses while it drains.
var ErrDraining = errors.New("the service is draining before it stops")

// ErrStopping is returned for work the service refuses once its stop began
// without a drain, or after it: the paths' context ended, or CloseEngines
// or Wait ran.
var ErrStopping = errors.New("the service is stopping")

// drainRetryAfter is the Retry-After of an engine connection refused while
// draining, in seconds. The engine reconnects with its own backoff anyway.
const drainRetryAfter = "10"

// maxDrainPoll bounds how often Drain looks for exports to run and checks
// what is left.
const maxDrainPoll = 100 * time.Millisecond

// drainState is what a drain measures, for its report and GET /debug/drain.
type drainState struct {
	mu       sync.Mutex
	started  time.Time
	deadline time.Time
	// stored are the roots stored by exports that ended while draining, by
	// namespace and cache ID.
	stored  map[drainCache]int
	refused map[string]int
	// engines is how many engine connections were welcomed when the drain
	// started; it welcomes none after.
	engines int
	ended   *DrainReport
	// conns are the engine caches welcomed when the drain started: those a
	// linger may wait for (lingerWaits).
	conns []lingerWait
	// linger is the linger that followed the drain, once it started.
	linger *LingerReport
	// explicit counts the explicit exports and imports (Service.Export,
	// Service.Import) admitted and still running; the drain and the stop's
	// Wait wait for them (zerobull-consulting/dagger#72,
	// zerobull-consulting/dagger#202). They are admitted under mu, as the
	// drain starts, so one is either counted here before the drain starts,
	// and waited for, or refused (admitExplicit). explicitClosed is set,
	// under mu, once Wait began: none is admitted after it, so no Add races
	// Wait.
	explicit       workGroup
	explicitClosed bool
	// explicitEnded wakes a drain when an explicit request ends. New makes
	// it.
	explicitEnded chan struct{}
}

// DrainLeft is what a drain left unexported of one engine cache.
type DrainLeft struct {
	Namespace string `json:"namespace"`
	exporter.DrainState
	// Connected is whether the cache's engine was connected when the
	// drain ended. A drain refuses new connections, so the roots of an
	// engine that went away can't be exported until a later session names
	// them again.
	Connected bool `json:"connected"`
}

// DrainReport is what a drain did.
type DrainReport struct {
	// Outcome is metrics.DrainComplete, DrainDeadline or DrainCanceled; empty
	// while the drain runs.
	Outcome string        `json:"outcome,omitempty"`
	Took    time.Duration `json:"took"`
	// Stored are the roots stored by exports that ended while draining, by
	// namespace and cache ID: one entry per engine cache that stored any,
	// sorted as Left is.
	Stored []DrainStored `json:"stored"`
	// Left is what was left unexported, by cache.
	Left []DrainLeft `json:"left"`
	// Refused counts the new work refused: engine connections and spans.
	Refused map[string]int `json:"refused,omitempty"`
	// Engines is how many engine connections were welcomed when the drain
	// started. It welcomes no other: a connection that says hello from then
	// on is refused, and counted in Refused.
	Engines int `json:"engines"`
	// Explicit is how many explicit exports and imports, admitted before
	// the drain started, still ran when it ended: none, unless it ended at
	// its deadline or was canceled. They run on until they end, or the
	// paths' context ends, which cancels them (in the binary, as the linger
	// that follows the drain starts, or as the shutdown does); what they
	// store after the drain ended isn't in its report.
	Explicit int `json:"explicit,omitempty"`
}

// LeftRoots is how many roots were left unexported that the service could
// still have exported: not those of a paused engine (PausedRoots).
func (r DrainReport) LeftRoots() int {
	n := 0
	for _, l := range r.Left {
		n += l.Roots()
	}
	return n
}

// PausedRoots is how many roots were left because their engine refuses
// them: it has given up its remote-cache work until its next session, so
// the drain neither waited for them nor tried them again
// (exporter.DrainState.Paused).
func (r DrainReport) PausedRoots() int {
	n := 0
	for _, l := range r.Left {
		n += l.Paused
	}
	return n
}

// StoredRoots is how many roots the drain's exports stored, of every cache.
func (r DrainReport) StoredRoots() int {
	n := 0
	for _, st := range r.Stored {
		n += st.Roots
	}
	return n
}

// StoredOf is how many roots the drain's exports stored of one namespace's
// engine cache.
func (r DrainReport) StoredOf(namespace, cacheID string) int {
	for _, st := range r.Stored {
		if st.Namespace == namespace && st.CacheID == cacheID {
			return st.Roots
		}
	}
	return 0
}

// DrainStored is what a drain's exports stored of one engine cache. Two
// namespaces' engines may say hello with the same cache ID: they are two
// caches, each with its own count.
type DrainStored struct {
	Namespace string `json:"namespace"`
	CacheID   string `json:"cacheID"`
	Roots     int    `json:"roots"`
}

// drainCache names an engine cache in a drain's counts.
type drainCache struct {
	namespace, cacheID string
}

// Draining reports whether the service is draining (Drain was called).
func (s *Service) Draining() bool { return s.draining.Load() }

// Drain finishes the exports the service knows of, before it stops: what a
// clean stop (SIGTERM) does first. It returns once nothing is left that it
// could export, or ctx ends: its deadline is the drain's.
//
//  1. It takes no new work from now on. New engine connections get 503, a
//     connection upgraded before that says hello from now on is closed with
//     1013, and spans of sessions the export path doesn't track are dropped.
//     An engine is either welcomed before the drain starts, and counted in
//     its report (DrainReport.Engines), or refused. Spans and
//     session ends of the sessions it tracks are still taken: a session that
//     ended just before the drain, with some of the cache spans it counted
//     still on their way, sends them within its tracker's Settle. The
//     connected engines stay connected, since exports run over them. The
//     import sync loops and scheduled GC stop, and a GC run under way is
//     canceled; GC is idempotent, and the next run finishes it.
//  2. Every changed entry is due now, without waiting for it to settle, and
//     so are roots waiting for another try; one that fails again retries on
//     its normal backoff. A session that hasn't ended is no longer tracked,
//     and one that ended with spans missing is waited for until they are in,
//     or Settle (at most 2s) passed with none of them. Drain runs those exports, one at a
//     time per engine cache as the scheduler does, and waits for them, their
//     uploads included. Roots of an engine that is no longer connected are
//     left: it can't reconnect to a draining service.
//  3. It starts no export past its deadline, or once ctx was canceled: each
//     pass checks both before it schedules anything, so a drain whose
//     deadline passed before it began starts nothing. A retry due by the
//     deadline is waited for, and started if a pass (when an export ends,
//     and every 100ms at most) finds it due before the deadline; one due in
//     the last pass's interval is left. One due after the deadline isn't
//     waited for, and is reported left (DrainState.Later). Exports started
//     before the deadline run on until the stop that follows cuts them off.
//  4. It waits for the explicit exports and imports (Service.Export,
//     Service.Import: POST /debug/engines/{conn}/export and /import)
//     admitted before it started, up to its deadline, as for the exports it
//     runs. It doesn't cancel them: one still running when it ends is
//     counted in its report (DrainReport.Explicit), and runs on until it
//     ends or the paths' context ends, which cancels it. Roots an explicit export
//     stores while draining are counted in DrainReport.Stored. From the
//     drain's start, both hooks are refused (ErrDraining).
//
// Drain deletes nothing, and publishes nothing an export didn't verify. It
// logs what it found to do when it starts, and what it stored and left when
// it ends. An export still in flight when it returns is cut off by the stop
// that follows (CloseEngines, the end of the export path's context and
// Wait), and publishes nothing.
//
// A drain that begins once the export path's context ended (the stop
// began) exports nothing. It logs that the service is stopping, and
// reports what is left. It is complete when nothing is left that it could
// have exported (an export in flight, or a root of a connected engine) and
// no explicit request runs, and canceled, or deadline once ctx's deadline
// passed, otherwise.
//
// Call it once, before CloseEngines and Wait. A second call returns a
// canceled report at once, and doesn't end the first: only the first call's
// ctx cancels a drain, which then reports canceled, or deadline once ctx's
// deadline passed.
// The goroutines it starts are exports, which Wait waits for.
func (s *Service) Drain(ctx context.Context) DrainReport {
	start := time.Now()
	deadline, _ := ctx.Deadline()
	// The drain's state is ready before anything sees s.draining, so an
	// export that ends as the drain starts is counted.
	s.drain.mu.Lock()
	if s.draining.Load() {
		s.drain.mu.Unlock()
		return DrainReport{Outcome: metrics.DrainCanceled}
	}
	s.drain.started, s.drain.deadline = start, deadline
	s.drain.stored = map[drainCache]int{}
	explicitEnded := s.drain.explicitEnded
	// A request that ended before the drain may have left a wake.
	select {
	case <-explicitEnded:
	default:
	}
	if s.drain.refused == nil {
		s.drain.refused = map[string]int{}
	}
	// Under s.mu, which register lists a connection under: an engine is
	// either welcomed before this, and counted, or refused (register).
	s.mu.Lock()
	s.draining.Store(true)
	s.drain.engines = len(s.conns)
	s.drain.conns = make([]lingerWait, 0, len(s.conns))
	for _, c := range s.conns {
		s.drain.conns = append(s.drain.conns, lingerWait{ns: c.ns, cacheID: c.hello.CacheID, generation: c.hello.Generation})
	}
	s.mu.Unlock()
	s.drain.mu.Unlock()
	s.metrics.DrainStarted()

	// Phase 1: no new work. New connections and spans check s.draining.
	if p := s.imports; p != nil {
		p.stop()
	}
	if p := s.gc; p != nil {
		p.stop()
	}

	p := s.export
	if p == nil {
		why := "the export path is off"
		if s.readOnly {
			why = "read-only: -export=false"
		}
		s.logger.Info("drain: nothing to export (" + why + ")")
		if n := s.explicitRunning(); n > 0 {
			s.logger.Info("drain: waiting for explicit requests", "running", n, "deadline", deadline)
		}
		return s.endDrain(start, s.waitExplicit(ctx, deadline, explicitEnded), nil)
	}
	if p.ctx.Err() != nil {
		// The paths stopped before the drain began: the service is
		// stopping, and nothing can be exported. The drain is complete when
		// nothing is left that it could have exported (stoppedBusy) and no
		// explicit request runs, whatever ctx says. Else it was cut short:
		// deadline once ctx's deadline passed (on ctx's timer or the export
		// path's clock), canceled otherwise. An explicit request still
		// running isn't waited for: the paths' context ending canceled it,
		// and the stop's Wait waits for it to end.
		s.logger.Info("drain: nothing to export (the service is stopping)")
		now := p.now()
		left := s.drainLeft(now, deadline)
		outcome := metrics.DrainComplete
		if stoppedBusy(left) || s.explicitRunning() > 0 {
			var ok bool
			if outcome, ok = ctxEnded(ctx, now, deadline); !ok {
				outcome = metrics.DrainCanceled
			}
		}
		return s.endDrain(start, outcome, left)
	}
	// Namespaces created from now on drain from their first span
	// (trackSpans).
	now := p.now()
	for _, ns := range s.namespacesNow() {
		ns.tracker.StartDrain(now)
	}
	startLeft := s.drainLeft(now, deadline)
	if len(startLeft) == 0 {
		s.logger.Info("drain: started; nothing pending")
	}
	for _, l := range startLeft {
		s.logger.Info("drain: started", "namespace", l.Namespace, "cacheID", l.CacheID, "connected", l.Connected,
			"sessions", l.Sessions, "pending", l.Pending, "inFlight", l.InFlight, "retrying", l.Retrying+l.Later, "paused", l.Paused)
	}
	if deadline.IsZero() {
		s.logger.Info("drain: no deadline")
	} else {
		s.logger.Info("drain: deadline", "in", time.Until(deadline).Round(time.Millisecond))
	}

	// Phase 2: flush.
	poll := min(p.tick, maxDrainPoll)
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		now := p.now()
		left := s.drainLeft(now, deadline)
		if !drainBusy(left) && s.explicitRunning() == 0 {
			return s.endDrain(start, metrics.DrainComplete, left)
		}
		// Before anything is scheduled: a pass that begins past the
		// deadline, or once the drain was canceled, starts nothing, and
		// what it would have started is reported left. The deadline is
		// checked on the export path's clock too, which the roots'
		// retries are due on: a retry due just before the deadline, but
		// seen after it, isn't started either, whether ctx's timer has
		// fired yet or not.
		if outcome, ok := drainEnded(ctx, p.ctx, now, deadline); ok {
			return s.endDrain(start, outcome, left)
		}
		for _, ns := range s.namespacesNow() {
			s.startBatches(p.ctx, ns, ns.tracker.DrainDue(now))
			s.metrics.ExportWaiting(ns.name, ns.tracker.WaitingForSlot())
		}
		select {
		case <-ctx.Done():
		case <-p.ctx.Done():
		case <-t.C:
			continue
		case <-p.drainWake:
			// An export ended, freeing its cache for the next batch.
			continue
		case <-explicitEnded:
			// An explicit export or import ended.
			continue
		}
		// Woken by the end of ctx or of the export path: the drain ends
		// there, with that outcome, whatever is left, as it always did.
		if h := s.testHookDrainWoken; h != nil {
			h()
		}
		now = p.now()
		outcome, _ := drainEnded(ctx, p.ctx, now, time.Time{})
		return s.endDrain(start, outcome, s.drainLeft(now, deadline))
	}
}

// waitExplicit waits for the explicit exports and imports admitted before
// the drain, until ctx ends or deadline passes, and returns the drain's
// outcome. It is the drain's wait when the export path is off, and so has
// nothing else to wait for.
func (s *Service) waitExplicit(ctx context.Context, deadline time.Time, ended <-chan struct{}) string {
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		t := time.NewTimer(time.Until(deadline))
		defer t.Stop()
		timeout = t.C
	}
	for s.explicitRunning() > 0 {
		if outcome, ok := ctxEnded(ctx, time.Now(), deadline); ok {
			return outcome
		}
		select {
		case <-ended:
		case <-ctx.Done():
		case <-timeout:
			timeout = nil
		}
	}
	return metrics.DrainComplete
}

// admitExplicit admits an explicit export or import (Service.Export,
// Service.Import), unless the service drains (ErrDraining) or its stop
// began (ErrStopping): stop, the path's context, ended, or Wait began. A
// drain that starts once it was admitted waits for it, until the drain's
// deadline, and so does Wait. The caller runs it on run, which ends with
// ctx or stop, and calls done when it ends.
func (s *Service) admitExplicit(ctx, stop context.Context) (run context.Context, done func(), err error) {
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()
	// Under drain.mu, which Drain sets draining under.
	if s.draining.Load() {
		return nil, nil, ErrDraining
	}
	if s.drain.explicitClosed || stop.Err() != nil {
		return nil, nil, ErrStopping
	}
	s.drain.explicit.Add()
	ended := s.drain.explicitEnded
	// Canceled with ErrStopping as its cause, so a request the stop cut off
	// answers 503, as one refused does (stoppedCause).
	run, cancel := context.WithCancelCause(ctx)
	stopCancel := context.AfterFunc(stop, func() { cancel(ErrStopping) })
	var once sync.Once
	return run, func() {
		once.Do(func() {
			stopCancel()
			cancel(nil)
			s.drain.mu.Lock()
			s.drain.explicit.Done()
			s.drain.mu.Unlock()
			select {
			case ended <- struct{}{}:
			default:
			}
		})
	}, nil
}

// waitExplicitHooks waits for the explicit exports and imports, after the
// contexts that run them have ended, and admits none after.
func (s *Service) waitExplicitHooks() {
	s.drain.mu.Lock()
	s.drain.explicitClosed = true
	s.drain.mu.Unlock()
	s.drain.explicit.Wait()
}

// stoppedCause is err, marked ErrStopping when the stop canceled run, the
// context an explicit request ran on (admitExplicit), so its error answers
// 503.
func stoppedCause(run context.Context, err error) error {
	if err == nil || errors.Is(err, ErrStopping) || !errors.Is(context.Cause(run), ErrStopping) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrStopping, err)
}

// explicitRunning is how many explicit exports and imports run.
func (s *Service) explicitRunning() int {
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()
	return s.drain.explicit.Running()
}

// drainEnded reports whether a drain is over before its next pass, and its
// outcome: canceled when ctx was canceled, or the export path stopped;
// deadline when ctx's deadline passed, or now is past deadline.
func drainEnded(ctx, paths context.Context, now, deadline time.Time) (string, bool) {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return metrics.DrainDeadline, true
	case ctx.Err() != nil, paths.Err() != nil:
		return metrics.DrainCanceled, true
	case !deadline.IsZero() && !now.Before(deadline):
		return metrics.DrainDeadline, true
	}
	return "", false
}

// ctxEnded reports whether ctx, a drain's own context, ended, and the
// drain's outcome: deadline when ctx's deadline passed, canceled when it was
// canceled, and deadline when now is past deadline. Unlike drainEnded, it
// doesn't look at the export path.
func ctxEnded(ctx context.Context, now, deadline time.Time) (string, bool) {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return metrics.DrainDeadline, true
	case ctx.Err() != nil:
		return metrics.DrainCanceled, true
	case !deadline.IsZero() && !now.Before(deadline):
		return metrics.DrainDeadline, true
	}
	return "", false
}

// stoppedBusy reports whether a drain that began once the export path
// stopped was cut short by it: an export in flight, or a root of a
// connected engine left, whenever it is due. A session that hasn't ended,
// with no root left, isn't: the flush stops tracking those at its first
// pass (DrainDue).
func stoppedBusy(left []DrainLeft) bool {
	for _, l := range left {
		if l.InFlight > 0 || (l.Connected && l.Roots() > 0) {
			return true
		}
	}
	return false
}

// drainBusy reports whether a drain has work left it can do: an export in
// flight, or a changed entry, a session it waits for, or a retry due by the
// deadline, of a connected engine.
func drainBusy(left []DrainLeft) bool {
	for _, l := range left {
		if l.InFlight > 0 || (l.Connected && (l.Sessions > 0 || l.Pending > 0 || l.Retrying > 0)) {
			return true
		}
	}
	return false
}

// drainLeft is what every namespace's tracker has left to export.
func (s *Service) drainLeft(now, deadline time.Time) []DrainLeft {
	var out []DrainLeft
	for _, ns := range s.namespacesNow() {
		for _, st := range ns.tracker.DrainStatus(now, deadline) {
			out = append(out, DrainLeft{Namespace: ns.name, DrainState: st, Connected: s.engineFor(ns, st.CacheID) != nil})
		}
	}
	slices.SortFunc(out, func(a, b DrainLeft) int {
		return compareCaches(a.Namespace, a.CacheID, b.Namespace, b.CacheID)
	})
	return out
}

// endDrain logs and measures a drain's end, and returns its report.
func (s *Service) endDrain(start time.Time, outcome string, left []DrainLeft) DrainReport {
	took := time.Since(start)
	s.drain.mu.Lock()
	rep := DrainReport{
		Outcome:  outcome,
		Took:     took,
		Stored:   drainStoredOf(s.drain.stored),
		Left:     left,
		Refused:  maps.Clone(s.drain.refused),
		Engines:  s.drain.engines,
		Explicit: s.drain.explicit.Running(),
	}
	s.drain.ended = &rep
	s.drain.mu.Unlock()
	s.metrics.DrainEnded(outcome, took, rep.LeftRoots(), rep.PausedRoots())

	for _, st := range rep.Stored {
		s.logger.Info("drain: stored", "namespace", st.Namespace, "cacheID", st.CacheID, "roots", st.Roots)
	}
	for _, l := range left {
		if l.Paused > 0 {
			// Not a failure of the drain: nothing it could do stores them.
			s.logger.Info("drain: left for an engine that gave up its remote-cache work", "namespace", l.Namespace, "cacheID", l.CacheID,
				"roots", l.Paused, "connected", l.Connected)
		}
		if l.Roots() == 0 {
			continue
		}
		s.logger.Warn("drain: left unexported", "namespace", l.Namespace, "cacheID", l.CacheID, "roots", l.Roots(),
			"connected", l.Connected, "pending", l.Pending, "inFlight", l.InFlight, "retrying", l.Retrying+l.Later)
	}
	level := s.logger.Info
	if rep.LeftRoots() > 0 || rep.Explicit > 0 || outcome != metrics.DrainComplete {
		level = s.logger.Warn
	}
	level("drain: done", "outcome", outcome, "took", took.Round(time.Millisecond),
		"storedRoots", rep.StoredRoots(), "leftRoots", rep.LeftRoots(), "pausedRoots", rep.PausedRoots(),
		"refusedConnections", rep.Refused[metrics.DrainRefusedConnection], "refusedSpans", rep.Refused[metrics.DrainRefusedSpan],
		"engines", rep.Engines, "explicitLeft", rep.Explicit)
	return rep
}

// drainStored counts roots stored by an export of a namespace's cache that
// ended while draining.
func (s *Service) drainStored(ns *namespace, cacheID string, roots int) {
	s.metrics.DrainStored(roots)
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()
	if s.drain.stored != nil {
		s.drain.stored[drainCache{ns.name, cacheID}] += roots
	}
}

// drainStoredOf is a drain's stored counts, sorted by namespace and cache
// ID.
func drainStoredOf(m map[drainCache]int) []DrainStored {
	out := make([]DrainStored, 0, len(m))
	for k, n := range m {
		out = append(out, DrainStored{Namespace: k.namespace, CacheID: k.cacheID, Roots: n})
	}
	slices.SortFunc(out, func(a, b DrainStored) int {
		return compareCaches(a.Namespace, a.CacheID, b.Namespace, b.CacheID)
	})
	return out
}

// compareCaches orders engine caches by namespace, then cache ID: the order
// of a drain report's Stored and Left.
func compareCaches(aNamespace, aCacheID, bNamespace, bCacheID string) int {
	return cmp.Or(cmp.Compare(aNamespace, bNamespace), cmp.Compare(aCacheID, bCacheID))
}

// refusedSpan counts a span or session end dropped while draining.
func (s *Service) refusedSpan() {
	s.countRefused(metrics.DrainRefusedSpan)
}

func (s *Service) countRefused(kind string) {
	s.metrics.DrainRefused(kind)
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()
	if s.drain.refused == nil {
		s.drain.refused = map[string]int{}
	}
	s.drain.refused[kind]++
}

// refuseDraining answers an engine connection while draining.
func (s *Service) refuseDraining(w http.ResponseWriter, r *http.Request) {
	s.countRefused(metrics.DrainRefusedConnection)
	s.logger.Info("engine connection refused: draining", "remote", r.RemoteAddr)
	w.Header().Set("Retry-After", drainRetryAfter)
	http.Error(w, ErrDraining.Error(), http.StatusServiceUnavailable)
}

// DrainStatus is GET /debug/drain's answer.
type DrainStatus struct {
	Draining bool       `json:"draining"`
	Started  *time.Time `json:"started,omitempty"`
	Deadline *time.Time `json:"deadline,omitempty"`
	// Left is what is left to export now, while the drain runs.
	Left []DrainLeft `json:"left,omitempty"`
	// Report is the drain's, once it ended.
	Report *DrainReport `json:"report,omitempty"`
	// Explicit is how many explicit exports and imports admitted before the
	// drain still run, while it runs: it waits for them (Service.Drain).
	Explicit int `json:"explicit,omitempty"`
	// Linger is the linger after the drain, once it started (Service.Linger):
	// the engine caches it waits for, and its outcome once it ended.
	Linger *LingerReport `json:"linger,omitempty"`
}

// DrainStatus reports whether the service drains, and how far it got.
func (s *Service) DrainStatus() DrainStatus {
	st := DrainStatus{Draining: s.draining.Load()}
	if !st.Draining {
		return st
	}
	s.drain.mu.Lock()
	started, deadline, ended := s.drain.started, s.drain.deadline, s.drain.ended
	explicit := s.drain.explicit.Running()
	if l := s.drain.linger; l != nil {
		cp := *l
		cp.Caches = slices.Clone(l.Caches)
		st.Linger = &cp
	}
	s.drain.mu.Unlock()
	if !started.IsZero() {
		st.Started = &started
	}
	if !deadline.IsZero() {
		st.Deadline = &deadline
	}
	if ended != nil {
		st.Report = ended
		return st
	}
	st.Explicit = explicit
	if p := s.export; p != nil {
		st.Left = s.drainLeft(p.now(), deadline)
	}
	return st
}

func (s *Service) serveDrain(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.DrainStatus())
}
