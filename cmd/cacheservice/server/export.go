package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// exportPath pushes engines' cache entries to the store: each namespace's
// tracker learns entries from OTLP, and the scheduler exports them, with the
// namespace's exporter, when they are due.
type exportPath struct {
	// cfg and trackerCfg configure each namespace's exporter and tracker;
	// cfg.Scope is replaced by the namespace.
	cfg        exporter.Config
	trackerCfg exporter.TrackerConfig
	// tick is the longest the scheduler waits before it looks for due
	// exports again; it looks sooner when a tracker's NextDue says so, or
	// a tracker calls kick. noEngineWait is how long a batch whose engine
	// isn't connected waits.
	tick         time.Duration
	noEngineWait time.Duration
	now          func() time.Time
	// ctx runs the scheduler and every export.
	ctx context.Context
	// wake and drainWake wake the scheduler and the drain: a tracker's
	// entries came due sooner, or a cache's slot was freed (kick).
	wake, drainWake chan struct{}
	// changedCh is closed, and replaced, on every kick: flushes wait on it
	// (changed).
	changedMu      sync.Mutex
	changedCh      chan struct{}
	changedWaiters atomic.Int32

	wg workGroup
}

// ExportConfig configures the export path.
type ExportConfig struct {
	Exporter exporter.Config
	// Tracker configures each namespace's tracker; its Notify is the
	// export path's.
	Tracker exporter.TrackerConfig
	// Tick is the longest the scheduler waits between looks for due
	// exports (default 1s). It looks sooner when a tracker's next due time
	// comes, or an export ends.
	Tick         time.Duration
	NoEngineWait time.Duration
	// Now is the export path's clock, which the trackers and the drain
	// run on; nil: time.Now. Tests set it.
	Now func() time.Time
}

// EnableExport turns on the export path, and runs its scheduler until ctx
// ends. Without it, the service exports nothing on its own. Each namespace
// gets an exporter of cfg.Exporter, with the namespace as its scope and its
// own view of the blob store (see blobstore.Namespacer); cfg.Exporter.Scope
// is the namespace of a service without tokens. Call it before the service
// serves engines.
func (s *Service) EnableExport(ctx context.Context, cfg ExportConfig) error {
	if s.readOnly {
		return errors.New("export path: the service is read-only (SetReadOnly)")
	}
	if cfg.Exporter.Logger == nil {
		cfg.Exporter.Logger = s.logger.With("component", "export")
	}
	if cfg.Exporter.Metrics == nil {
		cfg.Exporter.Metrics = s.metrics
	}
	if cfg.Exporter.Meta != nil && cfg.Exporter.Meta.Metrics() == nil {
		cfg.Exporter.Meta = cfg.Exporter.Meta.WithMetrics(s.metrics)
	}
	// Checked now, so a bad config fails here rather than on a connection.
	if _, err := exporter.New(cfg.Exporter); err != nil {
		return err
	}
	if _, err := namespaceBlobs(cfg.Exporter.Blobs, cfg.Exporter.Scope); err != nil {
		return fmt.Errorf("exporter: %w", err)
	}
	if err := s.setDefaultNamespace(cfg.Exporter.Scope); err != nil {
		return err
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.NoEngineWait <= 0 {
		cfg.NoEngineWait = 10 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	p := &exportPath{
		cfg:          cfg.Exporter,
		trackerCfg:   cfg.Tracker,
		tick:         cfg.Tick,
		noEngineWait: cfg.NoEngineWait,
		now:          cfg.Now,
		ctx:          ctx,
		wake:         make(chan struct{}, 1),
		drainWake:    make(chan struct{}, 1),
		changedCh:    make(chan struct{}),
	}
	p.trackerCfg.Notify = p.kick
	s.export = p
	s.metrics.ExportEnabled(true)
	p.wg.Add()
	go func() {
		defer p.wg.Done()
		s.scheduleExports(ctx)
	}()
	return nil
}

// kick wakes the scheduler and the drain, without blocking: a tracker
// calls it with its lock held (exporter.TrackerConfig.Notify).
func (p *exportPath) kick() {
	for _, ch := range []chan struct{}{p.wake, p.drainWake} {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	p.broadcast()
}

// minSchedulerWait bounds how often the scheduler looks for due exports
// when a next due time is already past, as when its clock stands still.
const minSchedulerWait = 5 * time.Millisecond

// scheduleExports runs the exports that come due until ctx ends, then waits
// for those in flight. It looks for them when the earliest next due time of
// a namespace's tracker comes, when an export ends (its slot is freed) or a
// tracker says entries came due sooner, and at least every tick.
func (s *Service) scheduleExports(ctx context.Context) {
	p := s.export
	timer := time.NewTimer(p.tick)
	defer timer.Stop()
	for {
		// Drain schedules the exports while the service drains.
		var next time.Time
		if !s.draining.Load() {
			for _, ns := range s.namespacesNow() {
				s.startBatches(ctx, ns, ns.tracker.Due(p.now()))
				s.metrics.ExportWaiting(ns.name, ns.tracker.WaitingForSlot())
				if n := ns.tracker.NextDue(); !n.IsZero() && (next.IsZero() || n.Before(next)) {
					next = n
				}
			}
		}
		wait := p.tick
		if !next.IsZero() {
			wait = min(wait, max(next.Sub(p.now()), minSchedulerWait))
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-p.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

// startBatches runs batches of a namespace, each in its own goroutine, which
// Wait waits for. A tracker hands out at most one batch per engine cache at
// a time, so batches of one cache run one after another.
func (s *Service) startBatches(ctx context.Context, ns *namespace, batches []exporter.Batch) {
	p := s.export
	for _, b := range batches {
		p.wg.Add()
		go func() {
			defer p.wg.Done()
			s.runBatch(ctx, ns, b)
		}()
	}
}

// runBatch exports one batch of a namespace on the engine connected there
// with its cache.
func (s *Service) runBatch(ctx context.Context, ns *namespace, b exporter.Batch) {
	p := s.export
	eng := s.engineFor(ns, b.CacheID)
	if eng == nil {
		s.logger.Debug("export: engine not connected; waiting", "namespace", ns.name, "cacheID", b.CacheID, "roots", len(b.Request.Roots))
		ns.tracker.Requeue(b, p.now().Add(p.noEngineWait))
		return
	}
	if eng.Generation() < b.Generation() {
		// The tracker heard of a newer generation (a span, an event, or
		// another replica's work) than the connection here: wait for that
		// generation's hello rather than export from an older one.
		s.logger.Debug("export: engine connected at an older generation; waiting", "namespace", ns.name, "cacheID", b.CacheID,
			"generation", eng.Generation(), "batchGeneration", b.Generation(), "roots", len(b.Request.Roots))
		ns.tracker.Requeue(b, p.now().Add(p.noEngineWait))
		return
	}
	_, _ = s.exportBatch(ctx, eng, b)
}

// exportBatch exports b on eng, records its result in the tracker, and
// measures it.
func (s *Service) exportBatch(ctx context.Context, eng *engineConn, b exporter.Batch) (exporter.Result, error) {
	start := time.Now()
	res, err := eng.ns.exporter.Export(metrics.WithCaller(ctx, metrics.CallerExport), eng, b.Request)
	// Counted before the tracker hears of it: once it does, the drain may
	// see nothing in flight and end its report.
	if len(res.Stored) > 0 && s.draining.Load() {
		s.drainStored(eng.CacheID(), len(res.Stored))
	}
	s.flushStored(eng.ns, eng.CacheID(), len(res.Stored))
	outcome := s.recordExport(ctx, eng, b, res, err)
	if s.testHookExportRecorded != nil {
		s.testHookExportRecorded()
	}
	s.metrics.ExportBatch(metrics.ExportBatch{
		Scope:    eng.ns.name,
		Outcome:  outcome,
		Duration: time.Since(start),
		Roots:    len(b.Request.Roots),
		Stored:   len(res.Stored),
		Gone:     len(res.Gone),
		Busy:     len(res.Busy),
		Refused:  len(res.Refused),
		Failed:   len(res.Failed),
		Uploaded: len(res.Uploaded),
		Present:  len(res.Present),
		Rejected: len(res.Rejected),
		Bundles:  len(res.Bundles),
	})
	return res, err
}

// recordExport records an export's result in the tracker. An export cut
// short because its engine went away, or ctx ended, is not a failure of its
// roots: they wait for the engine and are tried again. It returns the
// batch's outcome, for the metrics.
func (s *Service) recordExport(ctx context.Context, eng *engineConn, b exporter.Batch, res exporter.Result, err error) string {
	p := s.export
	tracker := eng.ns.tracker
	if err != nil && (ctx.Err() != nil || !s.connected(eng)) {
		eng.logger.Info("export interrupted", "roots", len(b.Request.Roots), "error", err)
		tracker.Interrupted(b, res, p.now(), p.now().Add(p.noEngineWait))
		return metrics.ExportInterrupted
	}
	outcome := metrics.OutcomeOK
	if err != nil {
		eng.logger.Warn("export failed", "roots", len(b.Request.Roots), "error", err)
		outcome = metrics.OutcomeError
	}
	tracker.Done(b, res, p.now())
	if len(res.Bundles) > 0 {
		s.triggerImports(eng.ns, eng.CacheID())
	}
	return outcome
}

// connected reports whether c is still a welcomed connection.
func (s *Service) connected(c *engineConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[c.id] == c
}

// engineFor returns the welcomed connection of an engine cache in a
// namespace of its newest generation, and of those the latest. An older
// generation's engine that connects after a newer one (a process still
// running beside its restart) is not chosen over it: the tracker exports for
// the newest generation it heard of, and runBatch won't export from an older
// one. A connection of another namespace that says hello with the same cache
// ID is never chosen: its token gives it no say over this namespace's
// exports.
func (s *Service) engineFor(ns *namespace, cacheID string) *engineConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest *engineConn
	for _, c := range s.conns {
		if c.ns != ns || c.hello.CacheID != cacheID {
			continue
		}
		if latest == nil || c.hello.Generation > latest.hello.Generation ||
			(c.hello.Generation == latest.hello.Generation && c.id > latest.id) {
			latest = c
		}
	}
	return latest
}

// engineConn is an exporter.Engine.
var _ exporter.Engine = (*engineConn)(nil)

func (c *engineConn) CacheID() string    { return c.hello.CacheID }
func (c *engineConn) Generation() uint64 { return c.hello.Generation }

func (c *engineConn) Request(ctx context.Context, typ protocol.Type, body json.RawMessage) (protocol.Envelope, error) {
	return c.request(ctx, typ, body)
}

// trackSpans feeds a namespace's tracker the cache spans of a trace request
// authenticated for it. Spans reach the service under the token of the
// client that ran the session, not the engine's, so any token of the
// namespace is taken (see the README, "Auth and namespaces").
func (s *Service) trackSpans(ns *namespace, req *coltracepb.ExportTraceServiceRequest) {
	p := s.export
	if p == nil {
		return
	}
	tracker := ns.tracker
	now := p.now()
	// A namespace created, or a request taken, as the drain starts drains
	// before it takes a span: what isn't tracked yet is new work.
	draining := s.draining.Load()
	if draining {
		tracker.StartDrain(now)
	}
	for _, rs := range req.ResourceSpans {
		res := rs.GetResource().GetAttributes()
		cache := stringAttr(res, telemetryattrs.EngineCacheAttr)
		if cache == "" {
			// Not an engine session's spans.
			continue
		}
		cacheID, generation, ok := parseEngineCache(cache)
		session := stringAttr(res, telemetryattrs.EngineSessionAttr)
		if !ok || (session != "" && !validEngineID(session)) {
			// Their IDs would key the tracker's state: none of these
			// spans is taken.
			s.refusedEngineIDs(metrics.EngineIDSourceSpan, resourceSpanCount(rs), len(cache), len(session))
			continue
		}
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				attrs := span.Attributes
				if n, ok := attrValue(attrs, telemetryattrs.CacheSessionSpansAttr); ok {
					count, err := strconv.Atoi(n.GetStringValue())
					if err == nil && !tracker.SessionEnded(cacheID, generation, session, count, now) && draining {
						s.refusedSpan()
					}
				}
				number, err := strconv.ParseUint(stringAttr(attrs, telemetryattrs.CacheResultIDAttr), 10, 64)
				if err != nil || number == 0 {
					continue
				}
				o := exporter.Observation{
					CacheID:      cacheID,
					Generation:   generation,
					Session:      session,
					Number:       number,
					State:        stringAttr(attrs, telemetryattrs.CacheOutcomeAttr) != "",
					Type:         stringAttr(attrs, telemetryattrs.CacheTypeAttr),
					Parts:        stringsAttr(attrs, telemetryattrs.CachePartsAttr),
					Retained:     stringAttr(attrs, telemetryattrs.CacheRetainedAttr) == "true",
					RetentionExp: intAttr(attrs, telemetryattrs.CacheRetentionExpiresAttr),
					Expires:      intAttr(attrs, telemetryattrs.CacheExpiresAttr),
					Replacements: uint64(intAttr(attrs, telemetryattrs.CacheReplacementsAttr)),
					EndUnixNano:  int64(span.EndTimeUnixNano),
				}
				if _, ok := attrValue(attrs, telemetryattrs.CacheDepsAttr); ok {
					o.HasDeps = true
					for _, d := range stringsAttr(attrs, telemetryattrs.CacheDepsAttr) {
						if n, err := strconv.ParseUint(d, 10, 64); err == nil {
							o.Deps = append(o.Deps, n)
						}
					}
				}
				if !tracker.Observe(o, now) && draining {
					s.refusedSpan()
				}
			}
		}
	}
}

// trackEvent feeds a namespace's tracker and importer one engine cache
// event, from a request authenticated with token key. With tokens, an event
// about a cache whose latest connection had another token is dropped, as is
// a start that reports wiping such a cache: only an engine's own token
// speaks for it.
func (s *Service) trackEvent(ns *namespace, key tokenKey, kind, cache, body string) {
	p := s.export
	if p == nil && s.imports == nil {
		return
	}
	// Only the kinds acted on below go further: kind comes from the
	// request, and names a metric label when the event is dropped.
	if kind != telemetryattrs.EngineEventStart && kind != telemetryattrs.EngineEventPrune {
		return
	}
	cacheID, generation, ok := parseEngineCache(cache)
	if !ok {
		s.refusedEngineIDs(metrics.EngineIDSourceEvent, 1, len(cache), 0)
		return
	}
	authenticated := s.authenticated()
	if authenticated && !ns.ownerAccepts(cacheID, key) {
		s.ignoredEvent(ns, kind, cacheID)
		return
	}
	switch kind {
	case telemetryattrs.EngineEventStart:
		var ev telemetryattrs.EngineStartEvent
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			s.logger.Warn("undecodable engine event", "kind", kind, "error", err)
			return
		}
		wiped := ev.WipedCache
		if wiped != "" && !validEngineID(wiped) {
			// The start is taken; the wiped cache, which no cache can
			// be named, isn't.
			s.refusedEngineIDs(metrics.EngineIDSourceEvent, 1, len(wiped), 0)
			wiped = ""
		}
		if wiped != "" && authenticated && !ns.ownerAccepts(wiped, key) {
			s.ignoredEvent(ns, kind+" (wiped cache)", wiped)
			wiped = ""
		}
		if wiped != "" && wiped != cacheID {
			ns.disown(wiped)
		}
		if p != nil {
			ns.tracker.Started(cacheID, generation, wiped)
		}
		if s.imports != nil && wiped != "" {
			s.forgetWiped(ns, wiped)
		}
	case telemetryattrs.EngineEventPrune:
		var ev telemetryattrs.EnginePruneEvent
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			s.logger.Warn("undecodable engine event", "kind", kind, "error", err)
			return
		}
		drops := make([]exporter.Drop, 0, len(ev.Drops))
		numbers := make([]uint64, 0, len(ev.Drops))
		for _, d := range ev.Drops {
			drops = append(drops, exporter.Drop{Number: d.ResultID, DroppedAtUnixNano: d.DroppedAtUnixNano})
			numbers = append(numbers, d.ResultID)
		}
		if p != nil {
			ns.tracker.Pruned(cacheID, generation, drops)
		}
		if s.imports != nil {
			ns.importer.Pruned(cacheID, generation, numbers)
		}
	}
}

// parseEngineCache splits dagger.io/engine.cache, "<cacheID>/<generation>".
// A cache ID that isn't a valid engine ID (validEngineID) is refused: it
// would key the tracker's, importer's and linger's state.
func parseEngineCache(v string) (string, uint64, bool) {
	i := strings.LastIndexByte(v, '/')
	if i <= 0 || !validEngineID(v[:i]) {
		return "", 0, false
	}
	gen, err := strconv.ParseUint(v[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return v[:i], gen, true
}

// resourceSpanCount is how many spans rs holds.
func resourceSpanCount(rs *tracepb.ResourceSpans) int {
	n := 0
	for _, ss := range rs.ScopeSpans {
		n += len(ss.Spans)
	}
	return n
}

// refusedEngineIDs counts n spans, events or hellos (source, a
// metrics.EngineIDSource constant) dropped because an engine cache ID or
// session in them isn't a valid engine ID, and logs it: at warning level at
// most once a minute per source (rejectionLog), the rest at debug level. The
// IDs themselves aren't logged, only their lengths.
func (s *Service) refusedEngineIDs(source string, n, cacheLen, sessionLen int) {
	s.metrics.EngineIDsRefused(source, n)
	level := slog.LevelDebug
	if s.engineIDRefusals.allow(source, time.Now()) {
		level = slog.LevelWarn
	}
	s.logger.Log(context.Background(), level, LogEngineIDsRefused, "source", source, "count", n,
		"cacheLength", cacheLen, "sessionLength", sessionLen, "maxLength", MaxEngineIDLen)
}

func attrValue(kvs []*commonpb.KeyValue, key string) (*commonpb.AnyValue, bool) {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.GetValue(), true
		}
	}
	return nil, false
}

func stringsAttr(kvs []*commonpb.KeyValue, key string) []string {
	v, ok := attrValue(kvs, key)
	if !ok {
		return nil
	}
	var out []string
	for _, e := range v.GetArrayValue().GetValues() {
		out = append(out, e.GetStringValue())
	}
	return out
}

func intAttr(kvs []*commonpb.KeyValue, key string) int64 {
	n, _ := strconv.ParseInt(stringAttr(kvs, key), 10, 64)
	return n
}

// serveExport is the explicit export hook: POST
// /debug/engines/{conn}/export exports the posted roots and outputs, or,
// with no roots, every tracked entry of the engine worth exporting, and
// answers with what was stored.
func (s *Service) serveExport(w http.ResponseWriter, r *http.Request) {
	if s.export == nil {
		writeDebugError(w, s.exportOff())
		return
	}
	conn, ok := debugConnID(w, r)
	if !ok {
		return
	}
	var req exporter.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "body: "+err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
	defer cancel()
	res, err := s.Export(ctx, conn, req)
	if err != nil {
		writeDebugError(w, err)
		return
	}
	writeJSON(w, res)
}

func (s *Service) serveEntries(w http.ResponseWriter, r *http.Request) {
	if s.export == nil {
		writeDebugError(w, s.exportOff())
		return
	}
	conn, ok := debugConnID(w, r)
	if !ok {
		return
	}
	entries, err := s.Entries(conn)
	if err != nil {
		writeDebugError(w, err)
		return
	}
	writeJSON(w, entries)
}

// ignoredEvent logs an engine event dropped because it came with a token
// other than the engine's.
func (s *Service) ignoredEvent(ns *namespace, kind, cacheID string) {
	s.metrics.IgnoredEvent(ns.name, kind)
	s.logger.Warn("engine event ignored: it came with a token other than the engine's",
		"namespace", ns.name, "kind", kind, "cacheID", cacheID)
}

// waitExports waits for the exports in flight, after the context that runs
// them has ended.
func (s *Service) waitExports() {
	if s.export != nil {
		s.export.wg.Wait()
	}
}
