package exporter

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// Defaults for TrackerConfig.
const (
	// DefaultSettle is how long an entry must go without a span naming it
	// before it is exported.
	DefaultSettle = time.Second
	// DefaultMaxDelay bounds how long a changed entry waits for its spans to
	// settle: an entry dirty this long is exported even while spans still
	// name it, so long, busy sessions stream.
	DefaultMaxDelay = 10 * time.Second
	// DefaultForgetAfter is how long an entry that isn't worth exporting
	// (not retained, pruned or expired), or a session, is kept after a span
	// last named it.
	DefaultForgetAfter = 10 * time.Second
	// DefaultBusyBackoff is the wait before a busy root is exported again;
	// it doubles each time, up to DefaultMaxBusyBackoff.
	DefaultBusyBackoff    = 2 * time.Second
	DefaultMaxBusyBackoff = 5 * time.Minute
	// DefaultMaxFailures is how many failed exports of the same value a
	// root gets before the tracker stops trying it.
	DefaultMaxFailures = 5
	// DefaultPausedWait is how long a root waits to be exported again after
	// its engine refused it as paused (Result.Paused), unless the engine
	// opens a new idle period or connects again first, which make it due.
	DefaultPausedWait = time.Minute
	// seenSessionRetention is how long the tracker remembers a session it
	// stopped tracking, for UnseenSessions.
	seenSessionRetention = 2 * time.Minute
	// maxSeenSessions bounds the sessions a cache remembers so; past it the
	// memory starts afresh.
	maxSeenSessions = 4096
	// maxBatchRoots bounds the roots of one export, so its request stays
	// small. More roots due wait for the next batch.
	maxBatchRoots = 1000
	// maxDrainSessionWait bounds how long a drain waits for the spans an
	// ended session counted and hasn't sent, from its last span or carrier:
	// Settle, but no longer than this (the former EndGrace), so a long
	// Settle doesn't hold a drain idle until its deadline.
	maxDrainSessionWait = 2 * time.Second
)

// TrackerConfig configures a Tracker.
type TrackerConfig struct {
	Settle         time.Duration
	MaxDelay       time.Duration
	ForgetAfter    time.Duration
	BusyBackoff    time.Duration
	MaxBusyBackoff time.Duration
	MaxFailures    int
	PausedWait     time.Duration
	// Notify, if set, is called when something may have become due sooner
	// than NextDue said, or an export slot was freed: the scheduler should
	// call Due again. It is called with the tracker's lock held, so it
	// must not block or call the tracker.
	Notify func()
}

// Tracker learns which entries each engine cache has from session spans and
// engine cache events, and decides which to export, and when. It follows
// ENTRY_DISCOVERY.md:
//
//   - Entries are keyed by cache ID and engine number. Observations from a
//     generation older than the newest seen for the cache are ignored.
//   - An entry is worth exporting when a span showed it retained. Its parts
//     are the union of what its spans reported, for the value at the
//     highest replacement count seen.
//   - An entry is dirty from the first span that changes its value (a new
//     replacement count, a new part, deps that grew, or retained turning
//     true) until an export claims it. A dirty entry worth exporting is due
//     once Settle passed with no span naming it, or once it has been dirty
//     for MaxDelay, whichever comes first.
//   - A session's carrier span only speeds this up: once it arrived and
//     every cache span it counted is in, the session's retained entries are
//     due at once. A session whose carrier never comes, or whose spans are
//     missing, holds nothing back.
//   - An entry already stored with the same replacement count and parts is
//     not exported again. Nor is one imported from the store with them
//     (Imported), one refused for the same value, or one whose expiry or
//     retention has lapsed.
//   - A root its engine refused because it has given up its remote-cache
//     work until its next session (Result.Paused) waits for the engine: it
//     is due again when the engine opens a new idle period (Opened) or
//     connects again, or after PausedWait. It counts no failure.
//   - Busy roots, and roots whose export failed, are exported again later,
//     with backoff. A root whose value changed while its export was in
//     flight is dirty again, and due by the same rule.
//   - At most one export of each cache is in flight (its slot): Due hands
//     out no batch for a cache whose slot is taken, and what becomes due
//     meanwhile goes in the next batch, so batches grow with load. Done,
//     Interrupted and Requeue free the slot.
//
// Its methods are safe for concurrent use.
type Tracker struct {
	cfg TrackerConfig

	mu     sync.Mutex
	caches map[string]*trackedCache
	// draining is set by StartDrain: the tracker takes spans only for the
	// sessions it tracks, and DrainDue makes everything pending due.
	draining bool
	// flushing counts the flushes of each cache under way (StartFlush):
	// while one is, every dirty entry of the cache is due at once, as in a
	// drain, but spans and sessions are taken as usual.
	flushing map[string]int

	// slots are the caches with an export in flight, by cache ID, and
	// slotWaiters the explicit exports waiting for a cache's slot
	// (AcquireSlot). Due hands out nothing for a cache in either. freed is
	// closed, and replaced, whenever a slot is freed.
	slots       map[string]bool
	slotWaiters map[string]int
	freed       chan struct{}
	// next is the earliest time something may become due (NextDue); zero
	// when nothing is pending. waiting is how many roots the last Due or
	// DrainDue found due and held back for a slot.
	next    time.Time
	waiting int
}

type trackedCache struct {
	id         string
	generation uint64
	entries    map[uint64]*trackedEntry
	sessions   map[string]*trackedSession
	// imports are the values merged into the cache from the store, by
	// engine number: what the merge left on each entry. Kept apart from
	// entries, which are forgotten when their spans stop.
	imports map[uint64]*exportedValue
	// engineWakes counts the times the cache's engine came back for the
	// entries that wait for it (wakeEngineWaitLocked): a batch claimed
	// before the latest one doesn't make its roots wait for the next.
	engineWakes uint64
	// quietUntil, when set, holds the cache's exports back until then
	// (Quiet): its engine settled, and pauses if an export is in progress
	// when its bound after its last session passes.
	quietUntil time.Time
	// seen are the sessions no longer tracked, with when their tracking
	// ended: every counted span in, or their spans stopped (UnseenSessions).
	seen map[string]time.Time
}

// untrackLocked stops tracking a session, and remembers it as seen.
func (c *trackedCache) untrackLocked(id string, now time.Time) {
	delete(c.sessions, id)
	if c.seen == nil || len(c.seen) >= maxSeenSessions {
		c.seen = map[string]time.Time{}
	}
	c.seen[id] = now
}

// trackedSession is what the carrier's speed-up needs of a session.
type trackedSession struct {
	generation uint64
	candidates map[uint64]bool
	seenSpans  int
	// expectSpans is the carrier's count, or -1 before the carrier.
	expectSpans int
	// lastSpanAt is when a span or the carrier of the session last came.
	lastSpanAt time.Time
}

func (s *trackedSession) ended() bool { return s.expectSpans >= 0 }

type trackedEntry struct {
	typ          string
	replacements uint64
	deps         []uint64
	// parts are the part addresses seen for the value, by their JSON.
	parts map[string]dagql.PersistedPartAddress

	retained bool
	// retainedAtNano is the end time of the state span retained was last
	// set from, and pruneNano the latest prune that dropped the edge.
	retainedAtNano   int64
	pruneNano        int64
	stateEndNano     int64
	retentionExpires int64
	expires          int64

	// The export state of the value.
	stored         *exportedValue
	refused        *exportedValue
	refusedMessage string
	// retryAt is when a busy or failed root is due again; zero when it
	// isn't waiting.
	retryAt time.Time
	// engineWaitAt is set while the entry waits for its cache's engine to
	// connect (Requeue, Interrupted), not for a backoff: Connected makes it
	// due. Every other write of retryAt clears it.
	engineWaitAt time.Time
	// paused is set while the wait is for an engine that refused the root
	// because it has given up its remote-cache work (Result.Paused); see
	// isPaused.
	paused   bool
	busy     int
	failures int
	// failedValue is the value failures counts for.
	failedValue *exportedValue
	// flushRetry is where the entry is in a flush's retry of a failed
	// root (see StartFlush and flushRetry).
	flushRetry flushRetry
	// inFlight is set while a batch holds the entry.
	inFlight bool
	// recheck is set when a newer generation cleared the entry's
	// stale-generation refusal (see cache): the next Due or DrainDue
	// exports it if it is still worth exporting and nothing is scheduled.
	recheck bool
	// lastSeen is when a span last named the entry.
	lastSeen time.Time
	// dirtySince is when the value first changed since an export last
	// claimed the entry; zero when it hasn't.
	dirtySince time.Time
	// accel is set when a session that named the entry ended with all its
	// spans in: due at once, without waiting for Settle.
	accel bool
	// changedAt is when a span last changed the value, and busyAt when the
	// engine last answered busy for it: zero unless retryAt is a busy wait.
	changedAt time.Time
	busyAt    time.Time
}

// isPaused reports whether the entry waits for an engine that has given up
// its remote-cache work until its next session: only the engine's open or
// a new connection ends that, so a drain or a flush neither waits for the
// entry nor tries it (DrainState.Paused).
func (e *trackedEntry) isPaused() bool {
	return e.paused && !e.engineWaitAt.IsZero()
}

// flushRetry is how a flush's retry of a failed root stands. A flush cuts
// short a failed root's wait (flushRetryCut) unless a try that a flush made
// due started it (flushRetryFailed): so each failure wait is cut short by a
// flush at most once in a row, and a caller that loops flushes can't make a
// failing root reach MaxFailures at its own rate.
type flushRetry uint8

const (
	// flushRetryNone: the entry's wait, if any, wasn't cut short by a
	// flush, and wasn't started by a try a flush made due.
	flushRetryNone flushRetry = iota
	// flushRetryCut: a flush cut short the wait after a failed try; the
	// entry's next try is the flush's.
	flushRetryCut
	// flushRetryTrying: the flush's try is in flight.
	flushRetryTrying
	// flushRetryFailed: the flush's try failed, for the value it is
	// waiting with. No flush cuts the wait it started short; one does
	// once the value changed.
	flushRetryFailed
)

// exportedValue is the state of a value an export covered.
type exportedValue struct {
	replacements uint64
	parts        []string
}

// covers reports whether v covers the entry's current value.
func (v *exportedValue) covers(e *trackedEntry) bool {
	if v == nil || v.replacements != e.replacements {
		return false
	}
	for k := range e.parts {
		if !slices.Contains(v.parts, k) {
			return false
		}
	}
	return true
}

// same reports whether v and o are the same value state.
func (v *exportedValue) same(o *exportedValue) bool {
	return v != nil && o != nil && v.replacements == o.replacements && slices.Equal(v.parts, o.parts)
}

func (e *trackedEntry) value() *exportedValue {
	return &exportedValue{replacements: e.replacements, parts: slices.Sorted(maps.Keys(e.parts))}
}

// NewTracker returns an empty Tracker.
func NewTracker(cfg TrackerConfig) *Tracker {
	if cfg.Settle <= 0 {
		cfg.Settle = DefaultSettle
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultMaxDelay
	}
	if cfg.ForgetAfter <= 0 {
		cfg.ForgetAfter = DefaultForgetAfter
	}
	if cfg.BusyBackoff <= 0 {
		cfg.BusyBackoff = DefaultBusyBackoff
	}
	if cfg.MaxBusyBackoff <= 0 {
		cfg.MaxBusyBackoff = DefaultMaxBusyBackoff
	}
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = DefaultMaxFailures
	}
	if cfg.PausedWait <= 0 {
		cfg.PausedWait = DefaultPausedWait
	}
	return &Tracker{
		cfg:         cfg,
		caches:      map[string]*trackedCache{},
		slots:       map[string]bool{},
		slotWaiters: map[string]int{},
		flushing:    map[string]int{},
		freed:       make(chan struct{}),
	}
}

// Observation is what one cache span says about an entry.
type Observation struct {
	CacheID    string
	Generation uint64
	Session    string
	Number     uint64
	// State is set for a call span (it has an outcome), which carries the
	// entry's whole state; a lazy-evaluation span carries only deps and
	// parts.
	State        bool
	Type         string
	Deps         []uint64
	HasDeps      bool
	Parts        []string
	Retained     bool
	RetentionExp int64
	Expires      int64
	Replacements uint64
	EndUnixNano  int64
}

// cache returns the tracked cache, or nil when generation is older than the
// newest seen for it.
func (t *Tracker) cache(cacheID string, generation uint64) *trackedCache {
	c := t.caches[cacheID]
	if c == nil {
		c = &trackedCache{id: cacheID, generation: generation, entries: map[uint64]*trackedEntry{}, sessions: map[string]*trackedSession{}, imports: map[uint64]*exportedValue{}}
		t.caches[cacheID] = c
	}
	if generation < c.generation {
		return nil
	}
	if generation > c.generation {
		// A refusal of a stale generation's export (the index had a
		// later one) says nothing of this generation's: it may record
		// the same value, so it is exported again (#59). The entry is
		// rechecked at the next Due or DrainDue, which schedules it as
		// done does a root whose refusal it dropped (#129): nothing
		// else would, unless a session of this generation names it.
		for _, e := range c.entries {
			if e.refused != nil && e.refusedMessage == StaleGenerationMessage {
				e.refused, e.refusedMessage = nil, ""
				e.recheck = true
				// Due at the next Due or DrainDue: wake the scheduler.
				t.notifyLocked()
			}
		}
	}
	c.generation = generation
	return c
}

func (c *trackedCache) session(id string, generation uint64) *trackedSession {
	s := c.sessions[id]
	if s == nil {
		s = &trackedSession{generation: generation, candidates: map[uint64]bool{}, expectSpans: -1}
		c.sessions[id] = s
	}
	return s
}

// Observe records one cache span. It reports whether the span was taken:
// while draining (StartDrain), a span of a session the tracker doesn't track,
// or of no session, is new work, and is dropped.
func (t *Tracker) Observe(o Observation, now time.Time) bool {
	if o.CacheID == "" || o.Number == 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining && !t.tracksSessionLocked(o.CacheID, o.Generation, o.Session) {
		return false
	}
	c := t.cache(o.CacheID, o.Generation)
	if c == nil {
		return false
	}
	var sess *trackedSession
	if o.Session != "" {
		sess = c.session(o.Session, o.Generation)
		sess.seenSpans++
		sess.lastSpanAt = now
	}
	e := c.entries[o.Number]
	if e == nil {
		e = &trackedEntry{parts: map[string]dagql.PersistedPartAddress{}}
		c.entries[o.Number] = e
	}
	// Whether the value was settled (an export or an import covers it, or
	// it isn't retained) before this span: a change then starts a new
	// dirty period, and MaxDelay counts from it.
	settled := t.settledLocked(c, o.Number, e)
	changed := false
	e.lastSeen = now
	switch {
	case o.Replacements < e.replacements:
		// An older value. It still names the entry: its settle restarts.
		t.scheduleLocked(c, o.Number, e, now)
		t.accelerateIfInLocked(c, o.Session, sess, now)
		return true
	case o.Replacements > e.replacements:
		// A new value: reset what belonged to the old one.
		e.replacements = o.Replacements
		e.deps = nil
		e.parts = map[string]dagql.PersistedPartAddress{}
		changed = true
	}
	if o.Type != "" {
		e.typ = o.Type
	}
	if o.HasDeps && len(o.Deps) >= len(e.deps) {
		if !slices.Equal(o.Deps, e.deps) {
			changed = true
		}
		e.deps = slices.Clone(o.Deps)
	}
	for _, p := range o.Parts {
		var addr dagql.PersistedPartAddress
		if err := json.Unmarshal([]byte(p), &addr); err != nil || addr.Part == "" {
			continue
		}
		// Keyed as the store keys parts, so imports compare.
		k := metastore.PartKey(addr)
		if _, ok := e.parts[k]; !ok {
			changed = true
		}
		e.parts[k] = addr
	}
	if o.State && o.EndUnixNano >= e.stateEndNano {
		e.stateEndNano = o.EndUnixNano
		e.retentionExpires = o.RetentionExp
		e.expires = o.Expires
		if o.EndUnixNano > e.pruneNano {
			if o.Retained && !e.retained {
				changed = true
			}
			e.retained = o.Retained
			e.retainedAtNano = o.EndUnixNano
		}
	}
	if changed {
		e.changedAt = now
		// A carrier speeds up the value it saw. This span changes it: it
		// settles as any change does, unless its own session's carrier
		// then speeds it up again (accelerateIfInLocked, below).
		e.accel = false
		if e.dirtySince.IsZero() || settled {
			e.dirtySince = now
		}
	}
	if sess != nil && e.retained {
		sess.candidates[o.Number] = true
	}
	t.scheduleLocked(c, o.Number, e, now)
	t.accelerateIfInLocked(c, o.Session, sess, now)
	return true
}

// settledLocked reports whether entry n's current value needs no export: it
// isn't retained, or an export or import covers it.
func (t *Tracker) settledLocked(c *trackedCache, n uint64, e *trackedEntry) bool {
	return !e.retained || e.stored.covers(e) || e.refused.covers(e) || c.imports[n].covers(e)
}

// accelerateIfInLocked makes a session's candidates due at once, and stops
// tracking the session, once its carrier came and every cache span it
// counted is in.
func (t *Tracker) accelerateIfInLocked(c *trackedCache, id string, s *trackedSession, now time.Time) {
	if s == nil || !s.ended() || s.seenSpans < s.expectSpans {
		return
	}
	for n := range s.candidates {
		if e := c.entries[n]; e != nil {
			e.accel = true
			t.scheduleLocked(c, n, e, now)
		}
	}
	c.untrackLocked(id, now)
}

// scheduleLocked brings NextDue forward to when entry n is due, if that is
// sooner, and then tells the scheduler (TrackerConfig.Notify).
func (t *Tracker) scheduleLocked(c *trackedCache, n uint64, e *trackedEntry, now time.Time) {
	if e.inFlight || t.heldLocked(c.id) || !t.wantedLocked(c, n, e, now) {
		// A cache whose slot is taken is scheduled when it's freed
		// (freeSlotLocked notifies): waking the scheduler for it now would
		// only run Due for nothing, on every span.
		return
	}
	if at, ok := t.dueAtLocked(e, now, t.urgentLocked(c.id)); ok {
		t.soonLocked(at)
	}
}

// urgentLocked reports whether a cache's dirty entries are due at once,
// without waiting for them to settle: while the tracker drains, or a flush
// of the cache is under way.
func (t *Tracker) urgentLocked(cacheID string) bool {
	return t.draining || t.flushingLocked(cacheID)
}

// flushingLocked reports whether a flush of cacheID is under way
// (StartFlush).
func (t *Tracker) flushingLocked(cacheID string) bool {
	return t.flushing[cacheID] > 0
}

// heldLocked reports whether cacheID's slot is taken, or an explicit export
// waits for it: Due hands out nothing for it.
func (t *Tracker) heldLocked(cacheID string) bool {
	return t.slots[cacheID] || t.slotWaiters[cacheID] > 0
}

// soonLocked brings NextDue forward to at, if that is sooner, and then tells
// the scheduler.
func (t *Tracker) soonLocked(at time.Time) {
	if !t.next.IsZero() && !at.Before(t.next) {
		return
	}
	t.next = at
	t.notifyLocked()
}

func (t *Tracker) notifyLocked() {
	if t.cfg.Notify != nil {
		t.cfg.Notify()
	}
}

// dueAtLocked returns when an entry worth exporting, and not in flight, is
// due: once Settle passed since a span last named it or MaxDelay since it
// became dirty, or now when a carrier sped it up or a newer generation
// cleared its refusal; for a drain, every such entry is due now. A retry
// (busy, failed, or given back for its engine) is a floor: a root waiting
// one isn't due before its retryAt, so its backoff holds; except a busy
// wait, which ends once the engine is done with the value (its session's
// carrier with every span in, or a change after the busy answer). It
// reports false when nothing makes it due.
func (t *Tracker) dueAtLocked(e *trackedEntry, now time.Time, drain bool) (time.Time, bool) {
	var at time.Time
	ok := false
	consider := func(x time.Time) {
		if !ok || x.Before(at) {
			at, ok = x, true
		}
	}
	if !e.dirtySince.IsZero() {
		consider(e.lastSeen.Add(t.cfg.Settle))
		consider(e.dirtySince.Add(t.cfg.MaxDelay))
	}
	if e.accel || e.recheck || drain {
		consider(now)
	}
	switch {
	case e.retryAt.IsZero():
	case !e.busyAt.IsZero() && (e.accel || e.changedAt.After(e.busyAt)):
		// A busy wait ends early once the engine is done with the value:
		// its session ended with every span in, or a span changed it since
		// the busy answer (and it settled, as above).
		consider(e.retryAt)
	case !ok || at.Before(e.retryAt):
		at, ok = e.retryAt, true
	}
	return at, ok
}

// tracksSessionLocked reports whether a session of a cache's current
// generation is tracked: seen, and not exported yet.
func (t *Tracker) tracksSessionLocked(cacheID string, generation uint64, session string) bool {
	c := t.caches[cacheID]
	if c == nil || session == "" || generation != c.generation {
		return false
	}
	s := c.sessions[session]
	return s != nil && s.generation == c.generation
}

// SessionEnded records a session's carrier span: the session ended, and had
// cacheSpans spans naming an entry. Once they are all in, the session's
// retained entries are due at once. It reports whether it was taken: while
// draining, the end of a session the tracker doesn't track is dropped.
func (t *Tracker) SessionEnded(cacheID string, generation uint64, session string, cacheSpans int, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining && !t.tracksSessionLocked(cacheID, generation, session) {
		return false
	}
	c := t.cache(cacheID, generation)
	if c == nil || session == "" {
		return false
	}
	s := c.session(session, generation)
	s.expectSpans = max(cacheSpans, 0)
	s.lastSpanAt = now
	t.accelerateIfInLocked(c, session, s, now)
	return true
}

// Drop is one retention edge an engine.prune event dropped.
type Drop struct {
	Number            uint64
	DroppedAtUnixNano int64
}

// Pruned records an engine.prune event.
func (t *Tracker) Pruned(cacheID string, generation uint64, drops []Drop) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cache(cacheID, generation)
	if c == nil {
		return
	}
	for _, d := range drops {
		_, imported := c.imports[d.Number]
		delete(c.imports, d.Number)
		e := c.entries[d.Number]
		if e == nil {
			continue
		}
		if imported && e.dirtySince.IsZero() {
			// No longer the stored value: changed, as of its last span,
			// if still retained (a span after the prune retained it).
			e.dirtySince = e.lastSeen
			t.notifyLocked()
		}
		if d.DroppedAtUnixNano > e.pruneNano {
			e.pruneNano = d.DroppedAtUnixNano
		}
		if e.retainedAtNano < e.pruneNano {
			e.retained = false
		}
	}
}

// Started records an engine.start event: a new generation, and maybe a wiped
// cache, whose entries are all gone.
func (t *Tracker) Started(cacheID string, generation uint64, wipedCache string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cache(cacheID, generation)
	if wipedCache != "" && wipedCache != cacheID {
		delete(t.caches, wipedCache)
	}
}

// Connected records an engine's hello: its cache reports in at generation,
// as a span or event of that generation would. A newer generation clears the
// cache's stale-generation refusals, which are exported again (see cache),
// even when no span or event of that generation reaches this service, as
// when it goes to another replica. An entry waiting for an engine of that
// generation to connect (see Requeue, Interrupted) is due at now: not at the
// end of its wait. A backoff after a failed or busy export is left to run.
func (t *Tracker) Connected(cacheID string, generation uint64, now time.Time) {
	if cacheID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cache(cacheID, generation)
	if c == nil {
		// An older generation than the tracker's: the engine entries
		// wait for is not this one. Past this check the hello is at the
		// tracker's generation or newer, so the connection can serve them.
		return
	}
	t.wakeEngineWaitLocked(c, now)
}

// Opened records that an engine of cacheID at generation opened a new idle
// period (protocol.Open): a session started on it, so it takes exports
// again. The entries that wait for the engine, as those it refused while
// paused do (Result.Paused), are due at now.
func (t *Tracker) Opened(cacheID string, generation uint64, now time.Time) {
	t.WakeEngineWait(cacheID, generation, now)
}

// Quiet holds the exports of cacheID back until until, unless a flush or a
// drain makes them urgent, or its engine opens a new idle period or
// connects again first. A settle that answered settled calls it: the engine
// gives up its remote-cache work, and cancels its exports, if one is in
// progress when its bound after its last session passes, settled or not;
// once that moment has passed with none, it takes exports until its next
// session. So what comes due meanwhile, as a retry or a late span's entry,
// waits for it.
//
// wakes is EngineWakes as read before the caller decided the engine was
// settled: if the engine opened a period or connected since, that decision
// is of an earlier period, and the quiet isn't set. Without it, an open
// that lands between the decision and this call would end the quiet first,
// and this call would then put the earlier period's back.
func (t *Tracker) Quiet(cacheID string, until time.Time, wakes uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.caches[cacheID]; c != nil && c.engineWakes == wakes && until.After(c.quietUntil) {
		c.quietUntil = until
	}
}

// EngineWakes counts the times the engine of cacheID opened a new idle
// period or connected (Opened, Connected, WakeEngineWait), for Quiet.
func (t *Tracker) EngineWakes(cacheID string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.caches[cacheID]; c != nil {
		return c.engineWakes
	}
	return 0
}

// WakeEngineWait makes the entries of a cache that wait for its engine due at
// now, when an engine of generation, or a newer one than the tracker's, is
// connected. Unlike Connected it records nothing of the generation: it is for
// a connection already welcomed, found after a requeue (see Requeue) that a
// hello's Connected ran too early to wake.
func (t *Tracker) WakeEngineWait(cacheID string, generation uint64, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.caches[cacheID]; c != nil && generation >= c.generation {
		t.wakeEngineWaitLocked(c, now)
	}
}

func (t *Tracker) wakeEngineWaitLocked(c *trackedCache, now time.Time) {
	c.engineWakes++
	// A session started, or the engine connected anew: its bound after
	// the last session no longer runs.
	c.quietUntil = time.Time{}
	woke := false
	for _, e := range c.entries {
		// A paused entry's own wait may have passed with a drain leaving
		// it be: it is due now all the same.
		if !e.engineWaitAt.IsZero() && (e.retryAt.After(now) || e.paused) {
			e.retryAt, e.engineWaitAt = now, time.Time{}
			woke = true
		}
	}
	if woke {
		t.soonLocked(now)
	}
}

// ImportedValue is a stored value merged into an engine cache: the engine
// entry it landed on, at what replacement count, with which parts complete
// or offered.
type ImportedValue struct {
	Number       uint64
	Replacements uint64
	Parts        []dagql.PersistedPartAddress
}

// Imported records values merged into an engine cache from the store. While
// an entry has the replacement count and no part beyond what its merge left,
// it is the stored value, and is not exported: an import isn't the engine's
// own entry. One that changed is exported, and the index (metastore's
// Record) keeps the stored entry's number for it.
func (t *Tracker) Imported(cacheID string, generation uint64, values []ImportedValue) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cache(cacheID, generation)
	if c == nil {
		return
	}
	for _, v := range values {
		if v.Number == 0 {
			continue
		}
		ev := &exportedValue{replacements: v.Replacements}
		for _, p := range v.Parts {
			ev.parts = append(ev.parts, metastore.PartKey(p))
		}
		slices.Sort(ev.parts)
		ev.parts = slices.Compact(ev.parts)
		c.imports[v.Number] = ev
	}
}

// Batch is one export to run: roots of one cache, with the parts to upload.
type Batch struct {
	CacheID string
	Request Request
	// values are the root values the batch covers, by number.
	values map[uint64]*exportedValue
	// generation is the cache's generation when the batch was claimed.
	generation uint64
	// engineWakes is the cache's count of them when the batch was claimed.
	engineWakes uint64
	// slot is set when the batch holds its cache's slot (Due, DrainDue):
	// Done, Interrupted or Requeue frees it.
	slot bool
}

// Generation is the cache's generation when the batch was claimed. An
// engine connection of an older generation must not export it: the index
// would refuse what that generation no longer holds, and the refusal would
// be settled against this generation.
func (b Batch) Generation() uint64 { return b.generation }

// Due returns the exports due at now: at most one batch per cache, for the
// caches with no export in flight (see Tracker). A batch holds its cache's
// slot until Done, Interrupted or Requeue is called for it, and each root is
// in at most one batch until then. Roots due beyond a batch's bound wait for
// the next one.
func (t *Tracker) Due(now time.Time) []Batch {
	return t.due(now, false)
}

// DrainDue is Due for a service that is stopping (see StartDrain): it doesn't
// wait for entries to settle. Every entry worth exporting is due now, except
// one waiting out a retry's backoff with its value unchanged since, which is
// due at its retryAt, as in Due; StartDrain made those waiting then due at
// once. Sessions that haven't ended are no longer tracked, so their later
// spans are refused; an ended session whose counted spans are still missing
// is tracked until they are in, or until Settle (at most
// maxDrainSessionWait) passed with none of them.
func (t *Tracker) DrainDue(now time.Time) []Batch {
	return t.due(now, true)
}

// NextDue is the earliest time something not yet due may become due,
// as the last Due or DrainDue, and the spans since, saw it; zero when
// nothing is pending. A cache whose slot is taken doesn't count: freeing a
// slot calls TrackerConfig.Notify.
func (t *Tracker) NextDue() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.next
}

// WaitingForSlot is how many roots the last Due or DrainDue found due and
// held back because their cache's slot was taken, or beyond a batch's bound.
func (t *Tracker) WaitingForSlot() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.waiting
}

func (t *Tracker) due(now time.Time, drain bool) []Batch {
	t.mu.Lock()
	defer t.mu.Unlock()
	var batches []Batch
	t.next, t.waiting = time.Time{}, 0
	for _, cacheID := range slices.Sorted(maps.Keys(t.caches)) {
		c := t.caches[cacheID]
		for id, s := range c.sessions {
			switch {
			case s.generation < c.generation,
				// Kept while its carrier could still speed up an entry
				// that hasn't settled.
				now.Sub(s.lastSpanAt) >= max(t.cfg.ForgetAfter, t.cfg.Settle),
				// A drain exports what such a session named at once,
				// and takes no more of it; an ended one is waited for
				// until its spans are in, or have settled.
				drain && (!s.ended() || now.Sub(s.lastSpanAt) >= t.drainSessionWait()):
				c.untrackLocked(id, now)
			}
		}
		for id, at := range c.seen {
			if now.Sub(at) >= seenSessionRetention {
				delete(c.seen, id)
			}
		}
		held := t.heldLocked(cacheID)
		urgent := drain || t.flushingLocked(cacheID)
		type dueRoot struct {
			n  uint64
			at time.Time
		}
		var due []dueRoot
		for n, e := range c.entries {
			if e.inFlight {
				continue
			}
			if !t.wantedLocked(c, n, e, now) {
				// No longer worth exporting, as once stored, pruned or
				// expired: stop waiting, so the entry can be forgotten.
				e.retryAt, e.engineWaitAt, e.recheck, e.accel, e.dirtySince = time.Time{}, time.Time{}, false, false, time.Time{}
				// An entry the engine no longer keeps (not retained,
				// pruned or expired) is forgotten once its spans have
				// stopped coming. A stored one is kept, so its stored
				// value is remembered.
				if t.lapsedLocked(e, now) && now.Sub(e.lastSeen) >= t.cfg.ForgetAfter {
					delete(c.entries, n)
				}
				continue
			}
			if t.draining && e.isPaused() {
				// A drain asks a paused engine nothing more: its answer
				// is known, and only its open or a new connection, which
				// make the entry due, change it.
				continue
			}
			at, ok := t.dueAtLocked(e, now, urgent)
			if !ok {
				// Worth exporting with nothing scheduled: treated as
				// changed now, so it settles and is exported. Nothing is
				// known to leave an entry so; this keeps one from waiting
				// forever.
				e.dirtySince = now
				at, _ = t.dueAtLocked(e, now, urgent)
			}
			if now.Before(at) {
				if !held && (t.next.IsZero() || at.Before(t.next)) {
					// Not soonLocked: the scheduler, which called this,
					// reads NextDue next.
					t.next = at
				}
				continue
			}
			due = append(due, dueRoot{n, at})
		}
		if held {
			t.waiting += len(due)
			continue
		}
		if !urgent && now.Before(c.quietUntil) {
			// Quiet: what is due waits for the engine's bound to pass.
			if len(due) > 0 && (t.next.IsZero() || c.quietUntil.Before(t.next)) {
				t.next = c.quietUntil
			}
			continue
		}
		// The longest due first, so that under a backlog of more than a
		// batch no root waits behind newer ones; then by number.
		slices.SortFunc(due, func(a, b dueRoot) int {
			return cmp.Or(a.at.Compare(b.at), cmp.Compare(a.n, b.n))
		})
		roots := make([]uint64, len(due))
		for i, d := range due {
			roots[i] = d.n
		}
		if len(roots) > maxBatchRoots {
			t.waiting += len(roots) - maxBatchRoots
			roots = roots[:maxBatchRoots]
		}
		if bs := t.batchesLocked(cacheID, c, roots, now); len(bs) > 0 {
			b := bs[0]
			b.slot = true
			t.slots[cacheID] = true
			batches = append(batches, b)
		}
	}
	return batches
}

// drainSessionWait is how long a drain waits for an ended session's missing
// spans, from its last span or carrier.
func (t *Tracker) drainSessionWait() time.Duration {
	return min(t.cfg.Settle, maxDrainSessionWait)
}

// lapsedLocked reports whether the engine no longer keeps an entry: it isn't
// retained, or its expiry or retention passed.
func (t *Tracker) lapsedLocked(e *trackedEntry, now time.Time) bool {
	unix := now.Unix()
	return !e.retained || (e.expires != 0 && e.expires <= unix) || (e.retentionExpires != 0 && e.retentionExpires <= unix)
}

// All returns batches of every root of cacheID worth exporting at now, due
// or not: the explicit hook, which holds the cache's slot (AcquireSlot)
// while it runs them.
func (t *Tracker) All(cacheID string, now time.Time) []Batch {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.caches[cacheID]
	if c == nil {
		return nil
	}
	return t.batchesLocked(cacheID, c, slices.Sorted(maps.Keys(c.entries)), now)
}

func (t *Tracker) batchesLocked(cacheID string, c *trackedCache, numbers []uint64, now time.Time) []Batch {
	var batches []Batch
	var cur *Batch
	for _, n := range numbers {
		e := c.entries[n]
		if e == nil {
			continue
		}
		if !t.wantedLocked(c, n, e, now) {
			if !e.inFlight {
				// No longer worth a retry, as once pruned or expired: stop
				// waiting, so the entry can be forgotten.
				e.retryAt, e.engineWaitAt = time.Time{}, time.Time{}
			}
			continue
		}
		if cur == nil || len(cur.Request.Roots) >= maxBatchRoots {
			batches = append(batches, Batch{CacheID: cacheID, values: map[uint64]*exportedValue{}, generation: c.generation, engineWakes: c.engineWakes})
			cur = &batches[len(batches)-1]
		}
		t.claimLocked(e)
		cur.Request.Roots = append(cur.Request.Roots, n)
		for _, k := range slices.Sorted(maps.Keys(e.parts)) {
			cur.Request.Outputs = append(cur.Request.Outputs, protocol.ExportOutput{Number: n, Address: e.parts[k]})
		}
		cur.values[n] = e.value()
	}
	return batches
}

// claimLocked marks an entry in flight: the batch exports its value as it is
// now, so it is no longer dirty. A change from now on makes it dirty again.
func (t *Tracker) claimLocked(e *trackedEntry) {
	e.inFlight = true
	if e.flushRetry == flushRetryCut {
		e.flushRetry = flushRetryTrying
	} else {
		e.flushRetry = flushRetryNone
	}
	e.retryAt, e.engineWaitAt, e.recheck, e.paused = time.Time{}, time.Time{}, false, false
	e.dirtySince, e.accel, e.busyAt = time.Time{}, false, time.Time{}
}

// AcquireSlot waits for cacheID's slot, for an export the caller runs itself
// (the explicit hook), and takes it. While it waits, Due hands out no batch
// for the cache, so the hook runs once the export in flight ends. It returns
// the function that frees the slot, or ctx's error if ctx ends first.
func (t *Tracker) AcquireSlot(ctx context.Context, cacheID string) (func(), error) {
	t.mu.Lock()
	t.slotWaiters[cacheID]++
	for t.slots[cacheID] {
		freed := t.freed
		t.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			t.mu.Lock()
			t.leaveSlotQueueLocked(cacheID)
			t.mu.Unlock()
			return nil, ctx.Err()
		}
		t.mu.Lock()
	}
	t.leaveSlotQueueLocked(cacheID)
	t.slots[cacheID] = true
	t.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.freeSlotLocked(cacheID)
		})
	}, nil
}

// SlotWaiters is how many explicit exports wait for cacheID's slot.
func (t *Tracker) SlotWaiters(cacheID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.slotWaiters[cacheID]
}

func (t *Tracker) leaveSlotQueueLocked(cacheID string) {
	if t.slotWaiters[cacheID]--; t.slotWaiters[cacheID] <= 0 {
		delete(t.slotWaiters, cacheID)
		// What waited behind the hook may be due now.
		t.notifyLocked()
	}
}

// freeSlotLocked frees cacheID's slot, wakes what waits for it, and tells
// the scheduler: the cache may have batches due.
func (t *Tracker) freeSlotLocked(cacheID string) {
	if !t.slots[cacheID] {
		return
	}
	delete(t.slots, cacheID)
	close(t.freed)
	t.freed = make(chan struct{})
	t.notifyLocked()
}

// wantedLocked reports whether entry n should be exported now.
func (t *Tracker) wantedLocked(c *trackedCache, n uint64, e *trackedEntry, now time.Time) bool {
	if e.inFlight || t.lapsedLocked(e, now) {
		return false
	}
	if e.stored.covers(e) || e.refused.covers(e) || c.imports[n].covers(e) {
		return false
	}
	if e.failedValue.covers(e) && e.failures >= t.cfg.MaxFailures {
		return false
	}
	return true
}

// Done records what an export of a batch did. Roots the result doesn't
// settle, as when the export failed as a whole (an error reply from the
// engine, or a timeout), count as failed, and are tried again later with
// backoff. An export cut short by its engine disconnecting, or the service
// stopping, goes to Interrupted instead, which counts no failure.
func (t *Tracker) Done(b Batch, res Result, now time.Time) {
	t.done(b, res, now, time.Time{})
}

// Interrupted records what an export of a batch did before something other
// than the export failed it, such as its engine disconnecting or the
// service stopping. Unlike Done, the roots the result doesn't settle don't
// count as failed: they are due again at retryAt.
func (t *Tracker) Interrupted(b Batch, res Result, now, retryAt time.Time) {
	t.done(b, res, now, retryAt)
}

func (t *Tracker) done(b Batch, res Result, now, interruptedRetry time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if b.slot {
		defer t.freeSlotLocked(b.CacheID)
	}
	c := t.caches[b.CacheID]
	if c == nil {
		return
	}
	// Results apply only to the batch's own roots: a result can name a root
	// the tracker didn't claim, whose state the batch says nothing about.
	entry := func(n uint64) *trackedEntry {
		if _, ok := b.values[n]; !ok {
			return nil
		}
		return c.entries[n]
	}
	settled := map[uint64]bool{}
	for _, n := range res.Stored {
		if e := entry(n); e != nil {
			e.stored = b.values[n]
			e.busy, e.failures, e.failedValue = 0, 0, nil
		}
		settled[n] = true
	}
	for _, n := range res.Gone {
		if entry(n) != nil {
			delete(c.entries, n)
		}
		settled[n] = true
	}
	for _, s := range res.Refused {
		if s.Message == StaleGenerationMessage && b.generation < c.generation {
			// The index refused an older generation's export, and a
			// newer one reported in while it was in flight: that one's
			// export is accepted, so the refusal is dropped (#59).
			settled[s.Number] = true
			continue
		}
		if e := entry(s.Number); e != nil {
			e.refused = b.values[s.Number]
			e.refusedMessage = s.Message
		}
		settled[s.Number] = true
	}
	for _, n := range res.Paused {
		// The engine's refusal, not the value's: it waits for the engine,
		// as a root given back for a connection does. An engine that came
		// back while the export was in flight (Opened, Connected) isn't
		// waited for again: the root is due now.
		if e := entry(n); e != nil {
			if b.engineWakes != c.engineWakes {
				e.retryAt, e.engineWaitAt = now, time.Time{}
			} else {
				at := now.Add(t.cfg.PausedWait)
				e.retryAt, e.engineWaitAt, e.paused = at, at, true
			}
			e.flushRetry = givenBack(e.flushRetry)
		}
		settled[n] = true
	}
	for _, s := range res.Busy {
		if e := entry(s.Number); e != nil {
			e.retryAt, e.engineWaitAt = now.Add(t.backoff(e.busy)), time.Time{}
			e.busy++
			e.busyAt = now
		}
		settled[s.Number] = true
	}
	failed := func(n uint64) {
		e := c.entries[n]
		if e == nil {
			return
		}
		v := b.values[n]
		if !e.failedValue.same(v) {
			e.failedValue, e.failures = v, 0
		}
		e.failures++
		if e.failures < t.cfg.MaxFailures {
			e.retryAt, e.engineWaitAt = now.Add(t.backoff(e.failures-1)), time.Time{}
			if e.flushRetry == flushRetryTrying {
				e.flushRetry = flushRetryFailed
			}
		}
	}
	for _, f := range res.Failed {
		failed(f.Number)
		settled[f.Number] = true
	}
	for n := range b.values {
		if !settled[n] {
			if !interruptedRetry.IsZero() {
				if e := c.entries[n]; e != nil {
					e.retryAt, e.engineWaitAt = interruptedRetry, interruptedRetry
					e.flushRetry = givenBack(e.flushRetry)
				}
			} else {
				failed(n)
			}
		}
		e := c.entries[n]
		if e == nil {
			continue
		}
		e.inFlight = false
		if e.flushRetry == flushRetryTrying {
			// Stored, refused, gone, busy, or failed for the last time.
			e.flushRetry = flushRetryNone
		}
		// The value may have changed since the snapshot exported, or been
		// stored or refused for it. A value that changed in flight is
		// dirty again, and is due once it settles, or MaxDelay after the
		// change, so a value that keeps changing isn't exported back to
		// back. One still worth exporting with nothing scheduled and no
		// change, as when a stale refusal was dropped, is due now.
		if t.wantedLocked(c, n, e, now) {
			if e.retryAt.IsZero() && e.dirtySince.IsZero() {
				e.retryAt, e.engineWaitAt = now, time.Time{}
			}
			t.scheduleLocked(c, n, e, now)
		}
	}
}

// Claim marks the tracked entries among roots in flight, for an export the
// caller runs itself, and returns the batch to pass to Done. Roots the
// tracker doesn't know are left out of the batch, and their results are
// ignored. It fails if any root is already in flight.
func (t *Tracker) Claim(cacheID string, req Request) (Batch, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := Batch{CacheID: cacheID, Request: req, values: map[uint64]*exportedValue{}}
	c := t.caches[cacheID]
	if c == nil {
		return b, nil
	}
	b.generation, b.engineWakes = c.generation, c.engineWakes
	for _, n := range req.Roots {
		if e := c.entries[n]; e != nil && e.inFlight {
			return Batch{}, fmt.Errorf("root %d is already being exported", n)
		}
	}
	for _, n := range req.Roots {
		if e := c.entries[n]; e != nil {
			t.claimLocked(e)
			b.values[n] = e.value()
		}
	}
	return b, nil
}

// givenBack is a flush retry's state once its try was given back before it
// ran (Requeue, Interrupted): the try that runs later is still the flush's.
func givenBack(r flushRetry) flushRetry {
	if r == flushRetryTrying {
		return flushRetryCut
	}
	return r
}

func (t *Tracker) backoff(n int) time.Duration {
	d := t.cfg.BusyBackoff
	for range min(n, 20) {
		d *= 2
		if d >= t.cfg.MaxBusyBackoff {
			return t.cfg.MaxBusyBackoff
		}
	}
	return d
}

// EntryState is a tracked entry, for the debug API.
type EntryState struct {
	Number       uint64   `json:"number"`
	Type         string   `json:"type,omitempty"`
	Replacements uint64   `json:"replacements,omitempty"`
	Deps         []uint64 `json:"deps,omitempty"`
	Parts        []string `json:"parts,omitempty"`
	Retained     bool     `json:"retained"`
	Stored       bool     `json:"stored"`
	Refused      string   `json:"refused,omitempty"`
	Failures     int      `json:"failures,omitempty"`
	RetryAt      string   `json:"retryAt,omitempty"`
	// Dirty is whether the value changed since an export last claimed the
	// entry, and is worth exporting.
	Dirty bool `json:"dirty"`
	// InFlight is whether an export holds the entry.
	InFlight bool `json:"inFlight,omitempty"`
	// DueAt is when the entry is due (RFC 3339, with milliseconds), if it
	// is worth exporting and not in flight: it may be in the past while its
	// cache's slot is taken.
	DueAt string `json:"dueAt,omitempty"`
}

// Entries returns the tracked entries of a cache, by number, as of now.
func (t *Tracker) Entries(cacheID string) []EntryState {
	return t.EntriesAt(cacheID, time.Now())
}

// EntriesAt returns the tracked entries of a cache, by number, as of now.
func (t *Tracker) EntriesAt(cacheID string, now time.Time) []EntryState {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.caches[cacheID]
	if c == nil {
		return nil
	}
	out := make([]EntryState, 0, len(c.entries))
	for _, n := range slices.Sorted(maps.Keys(c.entries)) {
		e := c.entries[n]
		s := EntryState{
			Number:       n,
			Type:         e.typ,
			Replacements: e.replacements,
			Deps:         e.deps,
			Parts:        slices.Sorted(maps.Keys(e.parts)),
			Retained:     e.retained,
			Stored:       e.stored.covers(e) || c.imports[n].covers(e),
			Failures:     e.failures,
			InFlight:     e.inFlight,
		}
		if e.refused.covers(e) {
			s.Refused = e.refusedMessage
			if s.Refused == "" {
				s.Refused = "refused"
			}
		}
		if !e.retryAt.IsZero() {
			s.RetryAt = e.retryAt.UTC().Format(time.RFC3339)
		}
		if !e.inFlight && t.wantedLocked(c, n, e, now) {
			s.Dirty = !e.dirtySince.IsZero()
			if at, ok := t.dueAtLocked(e, now, t.urgentLocked(cacheID)); ok {
				s.DueAt = at.UTC().Format("2006-01-02T15:04:05.000Z07:00")
			}
		}
		out = append(out, s)
	}
	return out
}

// Requeue puts a batch back, untried, to be due again at at: its engine
// wasn't connected, or was at an older generation. It frees the batch's
// slot.
func (t *Tracker) Requeue(b Batch, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if b.slot {
		defer t.freeSlotLocked(b.CacheID)
	}
	c := t.caches[b.CacheID]
	if c == nil {
		return
	}
	for n := range b.values {
		if e := c.entries[n]; e != nil {
			e.inFlight = false
			e.retryAt, e.engineWaitAt = at, at
			e.flushRetry = givenBack(e.flushRetry)
			t.soonLocked(at)
		}
	}
}

// StartDrain makes the tracker drain, for a service that is stopping: from
// now on it takes spans and session ends only for the sessions it tracks
// (Observe, SessionEnded), DrainDue doesn't wait for entries to settle, and
// every root waiting for another try is due at once. Retries after this one wait their normal backoff. It can't be undone:
// a drained service stops. Calls after the first do nothing.
func (t *Tracker) StartDrain(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining {
		return
	}
	t.draining = true
	for _, c := range t.caches {
		for _, e := range c.entries {
			if !e.inFlight && e.retryAt.After(now) && !e.isPaused() {
				e.retryAt, e.engineWaitAt = now, time.Time{}
			}
		}
	}
	t.notifyLocked()
}

// DrainState is what a cache has left to export, as a drain sees it. Each
// root is counted once: in flight first, then dirty, then waiting for a
// retry.
type DrainState struct {
	CacheID string `json:"cacheID"`
	// Sessions are the sessions the drain still waits for: before the
	// first DrainDue, those that haven't ended; after, those that ended
	// with some of their counted spans missing, until Settle passes with
	// none of them.
	Sessions int `json:"sessions"`
	// Pending are the dirty roots: worth exporting, not in flight, and not
	// waiting out a retry with an unchanged value. DrainDue makes them due
	// at once.
	Pending int `json:"pending"`
	// InFlight are the roots an export holds now.
	InFlight int `json:"inFlight"`
	// Retrying are the roots waiting for another try that is due by the
	// deadline, and Later those whose try is due after it.
	Retrying int `json:"retrying"`
	Later    int `json:"later"`
	// Paused are the roots the cache's engine refused because it has given
	// up its remote-cache work until its next session (Result.Paused). A
	// drain or a flush neither waits for them nor tries them again: they
	// are left, and are lost if the engine stops before a session starts
	// on it. They aren't among Roots.
	Paused int `json:"paused,omitempty"`
}

// Roots is how many roots the cache has left that the service can still
// export: all but Paused.
func (s DrainState) Roots() int { return s.Pending + s.InFlight + s.Retrying + s.Later }

// DrainStatus returns what each cache has left to export at now, for a drain
// that ends at deadline (zero: none). Caches with nothing left are left out.
func (t *Tracker) DrainStatus(now, deadline time.Time) []DrainState {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []DrainState
	for _, cacheID := range slices.Sorted(maps.Keys(t.caches)) {
		st := t.leftLocked(t.caches[cacheID], now, deadline, true)
		if st.Sessions > 0 || st.Roots() > 0 || st.Paused > 0 {
			out = append(out, st)
		}
	}
	return out
}

// leftLocked is what cache c has left to export at now, for a drain (drain)
// or a flush that ends at deadline. A drain waits for the sessions that
// haven't ended too, until its first DrainDue stops tracking them, and for
// those that ended with some of their counted spans missing until Settle (at
// most maxDrainSessionWait) passed with none of them. A flush waits for both
// kinds by that last rule alone: a session still sending spans, or whose
// carrier or last spans are on their way, as when the flush comes right
// after its client returned, holds it until Settle (at most
// maxDrainSessionWait) passed since its last span.
func (t *Tracker) leftLocked(c *trackedCache, now, deadline time.Time, drain bool) DrainState {
	st := DrainState{CacheID: c.id}
	for n, e := range c.entries {
		switch {
		case e.inFlight:
			st.InFlight++
		case !t.wantedLocked(c, n, e, now):
		case e.isPaused():
			st.Paused++
		case e.recheck && e.retryAt.IsZero():
			// Due at the next Due or DrainDue (see cache).
			st.Retrying++
		case e.retryAt.IsZero():
			st.Pending++
		case !deadline.IsZero() && e.retryAt.After(deadline):
			st.Later++
		default:
			st.Retrying++
		}
	}
	for _, s := range c.sessions {
		if s.generation < c.generation {
			continue
		}
		recent := now.Sub(s.lastSpanAt) < t.drainSessionWait()
		missing := s.ended() && s.seenSpans < s.expectSpans
		if (drain && (!s.ended() || (missing && recent))) || (!drain && (!s.ended() || missing) && recent) {
			st.Sessions++
		}
	}
	return st
}

// UnseenSessions is how many of the given sessions of cacheID the tracker has
// seen nothing of: no span and no carrier, now or in the last
// seenSessionRetention. A settle (protocol.Settle) waits a while for those
// whose engine sent their completion marker: their spans are on their way,
// and FlushStatus counts a session only once one of them came.
func (t *Tracker) UnseenSessions(cacheID string, sessions []string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.caches[cacheID]
	if c == nil {
		return len(sessions)
	}
	n := 0
	for _, id := range sessions {
		if _, tracked := c.sessions[id]; tracked {
			continue
		}
		if _, seen := c.seen[id]; seen {
			continue
		}
		n++
	}
	return n
}

// StartFlush starts a flush of one cache: until end is called, every
// dirty entry of cacheID is due at once, without waiting for it to settle,
// as in a drain; and every root of it waiting for another try is due now,
// once. Retries after that wait their normal backoff. A failed root whose
// wait began with a try a flush made due, and that failed again, isn't due
// at once: it waits out that backoff, and a flush after its next try, or
// once its value changed, may cut its wait short again. So looped flushes retry a failing root at most
// once per backoff of its own, and it reaches MaxFailures at most twice as
// fast as its backoff alone would take it there; a busy root, which doesn't count toward MaxFailures, or one
// waiting for its engine to connect, is due at every flush. One its engine
// refused as paused isn't (DrainState.Paused). Unlike StartDrain, it
// changes nothing else: spans, session ends and other caches are taken and
// scheduled as usual, and the scheduler runs the exports (Due), one at a
// time per cache. Flushes of one cache may overlap: the cache is flushing
// until each has ended. FlushStatus is what the flush waits for.
func (t *Tracker) StartFlush(cacheID string, now time.Time) (end func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flushing[cacheID]++
	if c := t.caches[cacheID]; c != nil {
		for _, e := range c.entries {
			if e.inFlight || !e.retryAt.After(now) || e.isPaused() || (e.flushRetry == flushRetryFailed && e.failedValue.covers(e)) {
				continue
			}
			// A wait after a failed try: neither busy nor for the engine.
			e.flushRetry = flushRetryNone
			if e.busyAt.IsZero() && e.engineWaitAt.IsZero() && e.failures > 0 {
				e.flushRetry = flushRetryCut
			}
			e.retryAt, e.engineWaitAt = now, time.Time{}
		}
	}
	// Due at once: wake the scheduler. It finds nothing to do for a cache
	// whose slot is taken, and is woken again when it's freed.
	t.notifyLocked()
	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.flushing[cacheID]--; t.flushing[cacheID] <= 0 {
				delete(t.flushing, cacheID)
			}
		})
	}
}

// FlushStatus is what cacheID has left to export at now, for a flush that
// ends at deadline: what a drain would count of it (see DrainState), except
// that Sessions counts the sessions waited for by a flush's rule (see
// leftLocked). A cache the tracker doesn't know has nothing left.
func (t *Tracker) FlushStatus(cacheID string, now, deadline time.Time) DrainState {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.caches[cacheID]
	if c == nil {
		return DrainState{CacheID: cacheID}
	}
	return t.leftLocked(c, now, deadline, false)
}
