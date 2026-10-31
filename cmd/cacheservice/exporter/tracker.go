package exporter

import (
	"encoding/json"
	"maps"
	"slices"
	"sync"
	"time"

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
//     not exported again. Nor is one refused for the same value, or one
//     whose expiry or retention has lapsed.
//   - Busy roots, and roots whose export failed, are exported again later,
//     with backoff.
//
// Its methods are safe for concurrent use.
type Tracker struct {
	cfg TrackerConfig

	mu     sync.Mutex
	caches map[string]*trackedCache
}

type trackedCache struct {
	generation uint64
	entries    map[uint64]*trackedEntry
	sessions   map[string]*trackedSession
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
		c = &trackedCache{generation: generation, entries: map[uint64]*trackedEntry{}, sessions: map[string]*trackedSession{}}
		t.caches[cacheID] = c
	}
	if generation < c.generation {
		return nil
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

// Observe records one cache span.
func (t *Tracker) Observe(o Observation, now time.Time) {
	if o.CacheID == "" || o.Number == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cache(o.CacheID, o.Generation)
	if c == nil {
		return
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
		return
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
		if _, ok := e.parts[p]; ok {
			continue
		}
		var addr dagql.PersistedPartAddress
		if err := json.Unmarshal([]byte(p), &addr); err != nil || addr.Part == "" {
			continue
		}
		e.parts[p] = addr
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
}

// SessionEnded records a session's carrier span: the session ended, and had
// cacheSpans spans naming an entry.
func (t *Tracker) SessionEnded(cacheID string, generation uint64, session string, cacheSpans int, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cache(cacheID, generation)
	if c == nil || session == "" {
		return
	}
	s := c.session(session, generation)
	s.expectSpans = cacheSpans
	s.endedAt = now
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

// Batch is one export to run: roots of one cache, with the parts to upload.
type Batch struct {
	CacheID string
	Request Request
	// values are the root values the batch covers, by number.
	values map[uint64]*exportedValue
}

// Due returns the exports due at now: the candidates of sessions that ended
// or went quiet, and roots due for another try. Each root is in at most one
// batch until Done is called for it.
func (t *Tracker) Due(now time.Time) []Batch {
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
				(!ended && !s.lastSpanAt.IsZero() && now.Sub(s.lastSpanAt) >= t.cfg.QuietPeriod)
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
		if !t.wantedLocked(e, now) {
			if !e.inFlight {
				// No longer worth a retry, as once pruned or expired: stop
				// waiting, so the entry can be forgotten.
				e.retryAt = time.Time{}
			}
			continue
		}
		if cur == nil || len(cur.Request.Roots) >= maxBatchRoots {
			batches = append(batches, Batch{CacheID: cacheID, values: map[uint64]*exportedValue{}})
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

// wantedLocked reports whether an entry should be exported now.
func (t *Tracker) wantedLocked(e *trackedEntry, now time.Time) bool {
	if e.inFlight || !e.retained {
		return false
	}
	unix := now.Unix()
	if (e.expires != 0 && e.expires <= unix) || (e.retentionExpires != 0 && e.retentionExpires <= unix) {
		return false
	}
	if e.stored.covers(e) || e.refused.covers(e) {
		return false
	}
	if e.failedValue.covers(e) && e.failures >= t.cfg.MaxFailures {
		return false
	}
	return true
}

// Done records what an export of a batch did. Roots the result doesn't
// settle, as when the batch failed as a whole (the engine disconnected),
// count as failed, and are tried again later.
func (t *Tracker) Done(b Batch, res Result, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.caches[b.CacheID]
	if c == nil {
		return
	}
	settled := map[uint64]bool{}
	for _, n := range res.Stored {
		if e := c.entries[n]; e != nil {
			e.stored = b.values[n]
			e.busy, e.failures, e.failedValue = 0, 0, nil
			if !e.stored.covers(e) {
				// Spans that arrived during the export added parts, or
				// a new value: export it again.
				e.retryAt = now
			}
		}
		settled[n] = true
	}
	for _, n := range res.Gone {
		delete(c.entries, n)
		settled[n] = true
	}
	for _, s := range res.Refused {
		if e := c.entries[s.Number]; e != nil {
			e.refused = b.values[s.Number]
			e.refusedMessage = s.Message
		}
		settled[s.Number] = true
	}
	for _, s := range res.Busy {
		if e := c.entries[s.Number]; e != nil {
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
		if e := c.entries[n]; e != nil {
			e.inFlight = false
		}
		if !settled[n] {
			failed(n)
		}
	}
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
			Stored:       e.stored.covers(e),
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
