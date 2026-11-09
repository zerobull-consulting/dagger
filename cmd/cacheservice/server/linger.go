package server

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// Linger outcomes.
const (
	// LingerStopped: every engine the linger waited for sent engine.stop.
	LingerStopped = "stopped"
	// LingerNothing: no engine connected when the drain started sends its
	// events here, so there was nothing to wait for.
	LingerNothing = "nothing"
	// LingerDeadline: it ended at its bound (-drain-linger).
	LingerDeadline = "deadline"
	// LingerCanceled: its context was canceled (a second signal).
	LingerCanceled = "canceled"
)

// maxLingerPoll bounds how often Linger checks for the engine.stop events it
// waits for.
const maxLingerPoll = 100 * time.Millisecond

// maxEventCaches bounds the engine caches a namespace remembers sending
// engine events here (namespace.eventCaches): any token of a namespace can
// name any cache ID in an event. Past the bound, a new cache takes the place
// of the one heard from least recently that has no live connection; while
// draining, none is replaced, and a new one isn't remembered.
const maxEventCaches = 4096

// LingerCache is one engine cache a linger waits for.
type LingerCache struct {
	Namespace string `json:"namespace"`
	CacheID   string `json:"cacheID"`
	// Stopped is whether its engine.stop arrived.
	Stopped bool `json:"stopped"`
}

// LingerReport is what a linger did.
type LingerReport struct {
	// Outcome is LingerStopped, LingerNothing, LingerDeadline or
	// LingerCanceled; empty while the linger runs.
	Outcome string        `json:"outcome,omitempty"`
	Took    time.Duration `json:"took"`
	// Caches are the engine caches it waited for, and whether each one's
	// engine.stop arrived.
	Caches []LingerCache `json:"caches"`
}

// stopped is how many of the caches sent engine.stop.
func (r LingerReport) stopped() int {
	n := 0
	for _, c := range r.Caches {
		if c.Stopped {
			n++
		}
	}
	return n
}

// Linger runs after a drain, before the stop that follows: the service keeps
// answering the engines' OTLP requests on its engine address, starting no
// new work and storing nothing, so an engine stopped after the drain can
// deliver its last cache event, engine.stop, and finish its own stop at once
// rather than retry against a closed port until its event export's bound
// (30s) runs out. See README.md, "Stopping and draining".
//
// It waits for engine.stop from each engine cache with a welcomed connection
// when the drain started, connected still or not, whose engine events reach
// this service (an event about it arrived here). An engine closes its connection in the first phase of its
// own stop, before it sends engine.stop, so a disconnect ends nothing: the
// linger ends once every such cache's engine.stop arrived, or ctx ends,
// whose deadline is its bound, or which a second signal cancels. With no
// such cache, it returns at once. engine.stop is logged and handled as any
// other engine event: nothing is deleted on it.
//
// Call it once, after Drain returned and once the context the export, import
// and GC paths run on has ended: what the drain started is then cut off, and
// publishes nothing, and nothing new starts. New engine connections are still
// refused (Drain's), and the connected ones stay connected until CloseEngines.
func (s *Service) Linger(ctx context.Context) LingerReport {
	start := time.Now()
	deadline, _ := ctx.Deadline()
	if !s.draining.Load() {
		// Only after a drain: before it, new work would still be taken.
		return LingerReport{Outcome: LingerNothing}
	}
	waits := s.lingerWaits()
	caches := lingerReportCaches(waits)
	s.drain.mu.Lock()
	s.drain.linger = &LingerReport{Caches: caches}
	s.drain.mu.Unlock()
	if len(waits) == 0 {
		s.logger.Info("linger: nothing to wait for: no engine connected at the drain's start sends its events here")
		return s.endLinger(start, LingerNothing, caches)
	}
	attrs := []any{"engines", len(waits)}
	if !deadline.IsZero() {
		attrs = append(attrs, "bound", time.Until(deadline).Round(time.Millisecond))
	}
	s.logger.Info("linger: started: answering the engines' events until each sends engine.stop", attrs...)
	for _, w := range waits {
		s.logger.Info("linger: waiting for engine.stop", "namespace", w.ns.name, "cacheID", w.cacheID)
	}

	t := time.NewTicker(maxLingerPoll)
	defer t.Stop()
	for {
		caches = lingerReportCaches(waits)
		s.drain.mu.Lock()
		s.drain.linger.Caches = caches
		s.drain.mu.Unlock()
		if lingerDone(caches) {
			return s.endLinger(start, LingerStopped, caches)
		}
		select {
		case <-ctx.Done():
			outcome := LingerCanceled
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				outcome = LingerDeadline
			}
			return s.endLinger(start, outcome, lingerReportCaches(waits))
		case <-t.C:
		}
	}
}

// lingerWait is an engine cache a linger waits for, at the generation its
// connection said hello with: an engine.stop of an earlier generation of the
// same cache, delivered late, isn't this engine's.
type lingerWait struct {
	ns         *namespace
	cacheID    string
	generation uint64
}

// lingerWaits are the engine caches a linger waits for: those with a
// welcomed connection when the drain started, connected still or not, whose
// engine events reach this service. A drain welcomes no new engine, so these
// are every engine that may still send engine.stop, an engine whose stop
// already closed its connection included: that comes first in an engine's
// stop, before its engine.stop.
func (s *Service) lingerWaits() []lingerWait {
	s.drain.mu.Lock()
	candidates := slices.Clone(s.drain.conns)
	s.drain.mu.Unlock()
	var out []lingerWait
	for _, w := range candidates {
		if seen, _ := w.ns.eventState(w.cacheID); seen && !slices.Contains(out, w) {
			out = append(out, w)
		}
	}
	slices.SortFunc(out, func(a, b lingerWait) int {
		return cmp.Or(cmp.Compare(a.ns.name, b.ns.name), cmp.Compare(a.cacheID, b.cacheID))
	})
	return out
}

// lingerReportCaches is where each of waits is: whether its engine.stop
// arrived.
func lingerReportCaches(waits []lingerWait) []LingerCache {
	out := make([]LingerCache, 0, len(waits))
	for _, w := range waits {
		stopped := w.ns.stoppedWhileDraining(w.cacheID, w.generation)
		out = append(out, LingerCache{Namespace: w.ns.name, CacheID: w.cacheID, Stopped: stopped})
	}
	return out
}

func lingerDone(caches []LingerCache) bool {
	for _, c := range caches {
		if !c.Stopped {
			return false
		}
	}
	return true
}

// endLinger logs a linger's end, and returns its report.
func (s *Service) endLinger(start time.Time, outcome string, caches []LingerCache) LingerReport {
	rep := LingerReport{Outcome: outcome, Took: time.Since(start), Caches: caches}
	s.drain.mu.Lock()
	s.drain.linger = &rep
	s.drain.mu.Unlock()
	level := s.logger.Info
	if outcome == LingerDeadline || outcome == LingerCanceled {
		level = s.logger.Warn
		for _, c := range caches {
			if !c.Stopped {
				s.logger.Warn("linger: no engine.stop", "namespace", c.Namespace, "cacheID", c.CacheID)
			}
		}
	}
	level("linger: done", "outcome", outcome, "took", rep.Took.Round(time.Millisecond),
		"engines", len(caches), "stopped", rep.stopped())
	return rep
}

// noteEngineEvent remembers that an engine event about cache arrived from
// token key, for a linger: that the cache's events reach this service, and,
// for engine.stop while draining, that its engine stopped. With tokens, an
// event is taken only from its cache's owner (ownerAccepts), as trackEvent
// takes it.
func (s *Service) noteEngineEvent(ns *namespace, key tokenKey, kind, cache string) {
	cacheID, generation, ok := parseEngineCache(cache)
	if !ok {
		// Every event comes here, whatever its kind: the one place an
		// event refused for its cache attribute is counted.
		if cache != "" {
			s.refusedEngineIDs(s.logger, metrics.EngineIDSourceEvent, 1, len(cache), 0)
		}
		return
	}
	if s.authenticated() && !ns.ownerAccepts(cacheID, key) {
		return
	}
	draining := s.draining.Load()
	stop := kind == telemetryattrs.EngineEventStop && draining
	ns.sawEvent(cacheID, generation, stop, stop && s.inDrainSnapshot(ns, cacheID, generation), draining)
}

// inDrainSnapshot reports whether cacheID's generation, in ns, had a welcomed
// connection when the drain started (drain.conns): a linger may wait for it,
// so its engine.stop is recorded whatever maxStoppedGenerations says.
func (s *Service) inDrainSnapshot(ns *namespace, cacheID string, generation uint64) bool {
	s.drain.mu.Lock()
	defer s.drain.mu.Unlock()
	return slices.Contains(s.drain.conns, lingerWait{ns: ns, cacheID: cacheID, generation: generation})
}

// eventCache is what a namespace remembers of an engine cache whose events
// arrive here.
type eventCache struct {
	// last is when its latest event arrived.
	last time.Time
	// stopped are the generations whose engine.stop arrived while
	// draining: each one in the drain's snapshot (drain.conns), and at most
	// maxStoppedGenerations others.
	stopped []uint64
	// others is how many of stopped aren't in the drain's snapshot.
	others int
}

// maxStoppedGenerations bounds the generations outside the drain's snapshot
// an eventCache remembers stopping: a token can send engine.stop for any
// generation. Those in the snapshot, which a linger may wait for, are always
// remembered; they are bounded by the connections welcomed before the drain
// (zerobull-consulting/dagger#126).
const maxStoppedGenerations = 8

// sawEvent records that an engine event about cacheID arrived, and with
// stop, that it was engine.stop of generation, while draining; inSnapshot is
// whether that generation is in the drain's snapshot. It remembers at most
// maxEventCaches caches: past that, a new cache replaces the one heard from
// least recently that has no live connection, unless draining is set, when
// the caches a linger may wait for must stay, and the new one isn't
// remembered.
func (n *namespace) sawEvent(cacheID string, generation uint64, stop, inSnapshot, draining bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := n.now()
	if c := n.eventCaches[cacheID]; c != nil {
		c.last = now
		if stop {
			c.stop(generation, inSnapshot)
		}
		return
	}
	if len(n.eventCaches) >= maxEventCaches {
		if draining || !n.evictEventCacheLocked() {
			return
		}
	}
	if n.eventCaches == nil {
		n.eventCaches = map[string]*eventCache{}
	}
	c := &eventCache{last: now}
	if stop {
		c.stop(generation, inSnapshot)
	}
	n.eventCaches[cacheID] = c
}

// stop records generation's engine.stop: always when inSnapshot, else only
// while fewer than maxStoppedGenerations others are recorded.
func (c *eventCache) stop(generation uint64, inSnapshot bool) {
	if slices.Contains(c.stopped, generation) {
		return
	}
	if !inSnapshot {
		if c.others >= maxStoppedGenerations {
			return
		}
		c.others++
	}
	c.stopped = append(c.stopped, generation)
}

// stoppedWhileDraining reports whether engine.stop of cacheID's generation
// arrived while draining.
func (n *namespace) stoppedWhileDraining(cacheID string, generation uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	c := n.eventCaches[cacheID]
	return c != nil && slices.Contains(c.stopped, generation)
}

// evictEventCacheLocked forgets the remembered cache heard from least
// recently that has no live connection, and reports whether there was one.
func (n *namespace) evictEventCacheLocked() bool {
	var (
		oldest string
		at     time.Time
	)
	for id, c := range n.eventCaches {
		if o := n.owners[id]; o != nil && o.live > 0 {
			continue
		}
		if oldest == "" || c.last.Before(at) {
			oldest, at = id, c.last
		}
	}
	if oldest == "" {
		return false
	}
	delete(n.eventCaches, oldest)
	return true
}

// eventState reports whether an engine event about cacheID arrived (seen),
// and whether an engine.stop of any of its generations did while draining
// (stopped).
func (n *namespace) eventState(cacheID string) (seen, stopped bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	c := n.eventCaches[cacheID]
	if c == nil {
		return false, false
	}
	return true, len(c.stopped) > 0
}
