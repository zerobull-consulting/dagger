package server

import (
	"cmp"
	"context"
	"errors"
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
	// cache ID.
	stored  map[string]int
	refused map[string]int
	ended   *DrainReport
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
	// cache ID.
	Stored map[string]int `json:"stored"`
	// Left is what was left unexported, by cache.
	Left []DrainLeft `json:"left"`
	// Refused counts the new work refused: engine connections and spans.
	Refused map[string]int `json:"refused,omitempty"`
}

// LeftRoots is how many roots were left unexported.
func (r DrainReport) LeftRoots() int {
	n := 0
	for _, l := range r.Left {
		n += l.Roots()
	}
	return n
}

// Draining reports whether the service is draining (Drain was called).
func (s *Service) Draining() bool { return s.draining.Load() }

// Drain finishes the exports the service knows of, before it stops: what a
// clean stop (SIGTERM) does first. It returns once nothing is left that it
// could export, or ctx ends: its deadline is the drain's.
//
//  1. It takes no new work from now on. New engine connections get 503, and
//     spans of sessions the export path doesn't track are dropped. Spans and
//     session ends of the sessions it tracks are still taken: a session that
//     ended just before the drain still sends spans within EndGrace. The
//     connected engines stay connected, since exports run over them. The
//     import sync loops and scheduled GC stop, and a GC run under way is
//     canceled; GC is idempotent, and the next run finishes it.
//  2. Every tracked session is due now: one that hasn't ended at once,
//     without waiting out the quiet period, and one that ended once its spans
//     are all in, or EndGrace passed. Roots waiting for another try are due
//     at once too; one that fails again retries on its normal backoff.
//     Drain runs those exports and waits for them, their uploads included.
//     Roots of an engine that is no longer connected are left: it can't
//     reconnect to a draining service.
//
// Drain deletes nothing, and publishes nothing an export didn't verify. It
// logs what it found to do when it starts, and what it stored and left when
// it ends. An export still in flight when it returns is cut off by the stop
// that follows (CloseEngines, the end of the export path's context and
// Wait), and publishes nothing.
//
// Call it once, before CloseEngines and Wait; a second call returns at once.
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
	s.drain.stored = map[string]int{}
	if s.drain.refused == nil {
		s.drain.refused = map[string]int{}
	}
	s.draining.Store(true)
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
	if p == nil || p.ctx.Err() != nil {
		s.logger.Info("drain: nothing to export (the export path is off)")
		return s.endDrain(start, metrics.DrainComplete, nil)
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
			"sessions", l.Sessions, "pending", l.Pending, "inFlight", l.InFlight, "retrying", l.Retrying+l.Later)
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
		for _, ns := range s.namespacesNow() {
			s.startBatches(p.ctx, ns, ns.tracker.DrainDue(now))
		}
		left := s.drainLeft(now, deadline)
		if !drainBusy(left) {
			return s.endDrain(start, metrics.DrainComplete, left)
		}
		select {
		case <-ctx.Done():
			outcome := metrics.DrainCanceled
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				outcome = metrics.DrainDeadline
			}
			return s.endDrain(start, outcome, s.drainLeft(p.now(), deadline))
		case <-p.ctx.Done():
			return s.endDrain(start, metrics.DrainCanceled, s.drainLeft(p.now(), deadline))
		case <-t.C:
		}
	}
}

// drainBusy reports whether a drain has work left it can do: an export in
// flight, or a session or retry of a connected engine due by the deadline.
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
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.CacheID, b.CacheID))
	})
	return out
}

// endDrain logs and measures a drain's end, and returns its report.
func (s *Service) endDrain(start time.Time, outcome string, left []DrainLeft) DrainReport {
	took := time.Since(start)
	s.drain.mu.Lock()
	rep := DrainReport{
		Outcome: outcome,
		Took:    took,
		Stored:  maps.Clone(s.drain.stored),
		Left:    left,
		Refused: maps.Clone(s.drain.refused),
	}
	s.drain.ended = &rep
	s.drain.mu.Unlock()
	s.metrics.DrainEnded(outcome, took, rep.LeftRoots())

	for _, id := range slices.Sorted(maps.Keys(rep.Stored)) {
		s.logger.Info("drain: stored", "cacheID", id, "roots", rep.Stored[id])
	}
	for _, l := range left {
		if l.Roots() == 0 {
			continue
		}
		s.logger.Warn("drain: left unexported", "namespace", l.Namespace, "cacheID", l.CacheID, "roots", l.Roots(),
			"connected", l.Connected, "pending", l.Pending, "inFlight", l.InFlight, "retrying", l.Retrying+l.Later)
	}
	level := s.logger.Info
	if rep.LeftRoots() > 0 || outcome != metrics.DrainComplete {
		level = s.logger.Warn
	}
	level("drain: done", "outcome", outcome, "took", took.Round(time.Millisecond),
		"storedRoots", sum(rep.Stored), "leftRoots", rep.LeftRoots(),
		"refusedConnections", rep.Refused[metrics.DrainRefusedConnection], "refusedSpans", rep.Refused[metrics.DrainRefusedSpan])
	return rep
}

// drainStored counts roots stored by an export that ended while draining.
func (s *Service) drainStored(cacheID string, roots int) {
	s.metrics.DrainStored(roots)
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()
	if s.drain.stored != nil {
		s.drain.stored[cacheID] += roots
	}
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
}

// DrainStatus reports whether the service drains, and how far it got.
func (s *Service) DrainStatus() DrainStatus {
	st := DrainStatus{Draining: s.draining.Load()}
	if !st.Draining {
		return st
	}
	s.drain.mu.Lock()
	started, deadline, ended := s.drain.started, s.drain.deadline, s.drain.ended
	s.drain.mu.Unlock()
	if !started.IsZero() {
		st.Started = &started
	}
	if !deadline.IsZero() {
		st.Deadline = &deadline
	}
	if ended != nil {
		st.Report = ended
	} else if p := s.export; p != nil {
		st.Left = s.drainLeft(p.now(), deadline)
	}
	return st
}

func (s *Service) serveDrain(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.DrainStatus())
}

func sum(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}
