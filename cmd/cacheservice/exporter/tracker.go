package exporter

import (
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
	// DefaultQuietPeriod is how long a session must go without a cache span
	// before its candidates are exported, when its end isn't seen.
	DefaultQuietPeriod = 10 * time.Second
	// DefaultEndGrace is how long to wait, after a session's end, for cache
	// spans it said were coming.
	DefaultEndGrace = 2 * time.Second
	// DefaultBusyBackoff is the wait before a busy root is exported again;
	// it doubles each time, up to DefaultMaxBusyBackoff.
	DefaultBusyBackoff    = 2 * time.Second
	DefaultMaxBusyBackoff = 5 * time.Minute
	// DefaultMaxFailures is how many failed exports of the same value a
	// root gets before the tracker stops trying it.
	DefaultMaxFailures = 5
	// maxBatchRoots bounds the roots of one export, so its request stays
	// small. A larger batch is split.
	maxBatchRoots = 1000
)

// TrackerConfig configures a Tracker.
type TrackerConfig struct {
	QuietPeriod    time.Duration
	EndGrace       time.Duration
	BusyBackoff    time.Duration
	MaxBusyBackoff time.Duration
	MaxFailures    int
}

// Tracker learns which entries each engine cache has from session spans and
// engine cache events, and decides which to export, and when. It follows
// ENTRY_DISCOVERY.md:
//
//   - Entries are keyed by cache ID and engine number. Observations from a
//     generation older than the newest seen for the cache are ignored.
//   - An entry is a candidate when a span showed it retained. Its parts are
//     the union of what its spans reported, for the value at the highest
//     replacement count seen.
//   - A session's candidates are exported when the session ends (its
//     carrier span, once all its cache spans arrived, or EndGrace after it),
//     or after QuietPeriod without a cache span from it.
//   - An entry already stored with the same replacement count and parts is
//     not exported again. Nor is one imported from the store with them
//     (Imported), one refused for the same value, or one whose expiry or
//     retention has lapsed.
//   - Busy roots, and roots whose export failed, are exported again later,
//     with backoff.
//
// Its methods are safe for concurrent use.
type Tracker struct {
	cfg TrackerConfig

	mu     sync.Mutex
	caches map[string]*trackedCache
	// draining is set by StartDrain: the tracker takes spans only for the
	// sessions it tracks, and DrainDue makes everything pending due.
	draining bool
}

type trackedCache struct {
	generation uint64
	entries    map[uint64]*trackedEntry
	sessions   map[string]*trackedSession
	// imports are the values merged into the cache from the store, by
	// engine number: what the merge left on each entry. Kept apart from
	// entries, which are forgotten when their spans stop.
	imports map[uint64]*exportedValue
}

type trackedSession struct {
	generation uint64
	candidates map[uint64]bool
	seenSpans  int
	// expectSpans is the carrier's count, or -1 before the carrier.
	expectSpans int
	endedAt     time.Time
	lastSpanAt  time.Time
}

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
	retryAt  time.Time
	busy     int
	failures int
	// failedValue is the value failures counts for.
	failedValue *exportedValue
	// inFlight is set while a batch holds the entry.
	inFlight bool
	// lastSeen is when a span last named the entry.
	lastSeen time.Time
}

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
	if cfg.QuietPeriod <= 0 {
		cfg.QuietPeriod = DefaultQuietPeriod
	}
	if cfg.EndGrace <= 0 {
		cfg.EndGrace = DefaultEndGrace
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
	return &Tracker{cfg: cfg, caches: map[string]*trackedCache{}}
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
		c = &trackedCache{generation: generation, entries: map[uint64]*trackedEntry{}, sessions: map[string]*trackedSession{}, imports: map[uint64]*exportedValue{}}
		t.caches[cacheID] = c
	}
	if generation < c.generation {
		return nil
	}
	if generation > c.generation {
		// A refusal of a stale generation's export (the index had a
		// later one) says nothing of this generation's: it may record
		// the same value, so it is exported again (#59).
		for _, e := range c.entries {
			if e.refused != nil && e.refusedMessage == StaleGenerationMessage {
				e.refused, e.refusedMessage = nil, ""
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
	e.lastSeen = now
	switch {
	case o.Replacements < e.replacements:
		// An older value.
		return true
	case o.Replacements > e.replacements:
		// A new value: reset what belonged to the old one.
		e.replacements = o.Replacements
		e.deps = nil
		e.parts = map[string]dagql.PersistedPartAddress{}
	}
	if o.Type != "" {
		e.typ = o.Type
	}
	if o.HasDeps && len(o.Deps) >= len(e.deps) {
		e.deps = slices.Clone(o.Deps)
	}
	for _, p := range o.Parts {
		var addr dagql.PersistedPartAddress
		if err := json.Unmarshal([]byte(p), &addr); err != nil || addr.Part == "" {
			continue
		}
		// Keyed as the store keys parts, so imports compare.
		e.parts[metastore.PartKey(addr)] = addr
	}
	if o.State && o.EndUnixNano >= e.stateEndNano {
		e.stateEndNano = o.EndUnixNano
		e.retentionExpires = o.RetentionExp
		e.expires = o.Expires
		if o.EndUnixNano > e.pruneNano {
			e.retained = o.Retained
			e.retainedAtNano = o.EndUnixNano
		}
	}
	if sess != nil && e.retained {
		sess.candidates[o.Number] = true
	}
	return true
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
// cacheSpans spans naming an entry. It reports whether it was taken: while
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
	s.expectSpans = cacheSpans
	s.endedAt = now
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
		delete(c.imports, d.Number)
		e := c.entries[d.Number]
		if e == nil {
			continue
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
}

// Due returns the exports due at now: the candidates of sessions that ended
// or went quiet, and roots due for another try. Each root is in at most one
// batch until Done is called for it.
func (t *Tracker) Due(now time.Time) []Batch {
	return t.due(now, false)
}

// DrainDue is Due for a service that is stopping (see StartDrain): it doesn't
// wait out the quiet period. Every session that hasn't ended is due now. A
// session that ended is due once its spans are all in, or EndGrace after
// its end, as in Due: the spans it said were coming are still on their way,
// and are taken. Roots waiting for another try are due at their retryAt, as
// in Due; StartDrain made those waiting then due at once.
func (t *Tracker) DrainDue(now time.Time) []Batch {
	return t.due(now, true)
}

func (t *Tracker) due(now time.Time, drain bool) []Batch {
	t.mu.Lock()
	defer t.mu.Unlock()
	var batches []Batch
	for _, cacheID := range slices.Sorted(maps.Keys(t.caches)) {
		c := t.caches[cacheID]
		roots := map[uint64]bool{}
		for id, s := range c.sessions {
			if s.generation < c.generation {
				delete(c.sessions, id)
				continue
			}
			ended := !s.endedAt.IsZero()
			ready := (ended && (s.seenSpans >= s.expectSpans || now.Sub(s.endedAt) >= t.cfg.EndGrace)) ||
				(!ended && (drain || (!s.lastSpanAt.IsZero() && now.Sub(s.lastSpanAt) >= t.cfg.QuietPeriod)))
			if !ready {
				continue
			}
			for n := range s.candidates {
				roots[n] = true
			}
			// Exported, whether it ended or went quiet. A later span of a
			// quiet session starts it again.
			delete(c.sessions, id)
		}
		for n, e := range c.entries {
			if !e.retryAt.IsZero() && !now.Before(e.retryAt) {
				roots[n] = true
			}
			// An entry without retention isn't exported; once its spans
			// have stopped coming, forget it.
			if !e.retained && !e.inFlight && e.retryAt.IsZero() && !roots[n] && now.Sub(e.lastSeen) >= t.cfg.QuietPeriod {
				delete(c.entries, n)
			}
		}
		batches = append(batches, t.batchesLocked(cacheID, c, slices.Sorted(maps.Keys(roots)), now)...)
	}
	return batches
}

// All returns batches of every root of cacheID worth exporting at now, due
// or not: the explicit hook.
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
				e.retryAt = time.Time{}
			}
			continue
		}
		if cur == nil || len(cur.Request.Roots) >= maxBatchRoots {
			batches = append(batches, Batch{CacheID: cacheID, values: map[uint64]*exportedValue{}, generation: c.generation})
			cur = &batches[len(batches)-1]
		}
		e.inFlight = true
		e.retryAt = time.Time{}
		cur.Request.Roots = append(cur.Request.Roots, n)
		for _, k := range slices.Sorted(maps.Keys(e.parts)) {
			cur.Request.Outputs = append(cur.Request.Outputs, protocol.ExportOutput{Number: n, Address: e.parts[k]})
		}
		cur.values[n] = e.value()
	}
	return batches
}

// wantedLocked reports whether entry n should be exported now.
func (t *Tracker) wantedLocked(c *trackedCache, n uint64, e *trackedEntry, now time.Time) bool {
	if e.inFlight || !e.retained {
		return false
	}
	unix := now.Unix()
	if (e.expires != 0 && e.expires <= unix) || (e.retentionExpires != 0 && e.retentionExpires <= unix) {
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
	for _, s := range res.Busy {
		if e := entry(s.Number); e != nil {
			e.retryAt = now.Add(t.backoff(e.busy))
			e.busy++
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
			e.retryAt = now.Add(t.backoff(e.failures - 1))
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
					e.retryAt = interruptedRetry
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
		// Spans that named the entry during the export were dropped with
		// their session, since it was in flight; and its value may have
		// changed since the snapshot exported, been stored or refused. If
		// it is still worth exporting and nothing is scheduled, export it
		// again now.
		if e.retryAt.IsZero() && t.wantedLocked(c, n, e, now) {
			e.retryAt = now
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
	b.generation = c.generation
	for _, n := range req.Roots {
		if e := c.entries[n]; e != nil && e.inFlight {
			return Batch{}, fmt.Errorf("root %d is already being exported", n)
		}
	}
	for _, n := range req.Roots {
		if e := c.entries[n]; e != nil {
			e.inFlight = true
			e.retryAt = time.Time{}
			b.values[n] = e.value()
		}
	}
	return b, nil
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
}

// Entries returns the tracked entries of a cache, by number.
func (t *Tracker) Entries(cacheID string) []EntryState {
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
		out = append(out, s)
	}
	return out
}

// Requeue puts a batch back, untried, to be due again at at: its engine
// wasn't connected.
func (t *Tracker) Requeue(b Batch, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.caches[b.CacheID]
	if c == nil {
		return
	}
	for n := range b.values {
		if e := c.entries[n]; e != nil {
			e.inFlight = false
			e.retryAt = at
		}
	}
}

// StartDrain makes the tracker drain, for a service that is stopping: from
// now on it takes spans and session ends only for the sessions it tracks
// (Observe, SessionEnded), and every root waiting for another try is due at
// once. Retries after this one wait their normal backoff. It can't be undone:
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
			if !e.inFlight && e.retryAt.After(now) {
				e.retryAt = now
			}
		}
	}
}

// DrainState is what a cache has left to export, as a drain sees it. Each
// root is counted once: in flight first, then waiting for a retry, then
// pending in a session.
type DrainState struct {
	CacheID string `json:"cacheID"`
	// Sessions are the sessions not exported yet: ended ones waiting for
	// their spans, within EndGrace, and, before DrainDue runs, the others.
	Sessions int `json:"sessions"`
	// Pending are the roots of those sessions worth exporting.
	Pending int `json:"pending"`
	// InFlight are the roots an export holds now.
	InFlight int `json:"inFlight"`
	// Retrying are the roots waiting for another try that is due by the
	// deadline, and Later those whose try is due after it.
	Retrying int `json:"retrying"`
	Later    int `json:"later"`
}

// Roots is how many roots the cache has left to export.
func (s DrainState) Roots() int { return s.Pending + s.InFlight + s.Retrying + s.Later }

// DrainStatus returns what each cache has left to export at now, for a drain
// that ends at deadline (zero: none). Caches with nothing left are left out.
func (t *Tracker) DrainStatus(now, deadline time.Time) []DrainState {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []DrainState
	for _, cacheID := range slices.Sorted(maps.Keys(t.caches)) {
		c := t.caches[cacheID]
		st := DrainState{CacheID: cacheID}
		counted := map[uint64]bool{}
		for n, e := range c.entries {
			switch {
			case e.inFlight:
				st.InFlight++
			case e.retryAt.IsZero() || !t.wantedLocked(c, n, e, now):
				continue
			case !deadline.IsZero() && e.retryAt.After(deadline):
				st.Later++
			default:
				st.Retrying++
			}
			counted[n] = true
		}
		for _, s := range c.sessions {
			if s.generation < c.generation {
				continue
			}
			st.Sessions++
			for n := range s.candidates {
				if e := c.entries[n]; e != nil && !counted[n] && t.wantedLocked(c, n, e, now) {
					st.Pending++
					counted[n] = true
				}
			}
		}
		if st.Sessions > 0 || st.Roots() > 0 {
			out = append(out, st)
		}
	}
	return out
}
