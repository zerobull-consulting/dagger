package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

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
	// tick is how often the scheduler looks for due exports, and
	// noEngineWait how long a batch whose engine isn't connected waits.
	tick         time.Duration
	noEngineWait time.Duration
	now          func() time.Time
	// ctx runs the scheduler and every export.
	ctx context.Context

	wg sync.WaitGroup
}

// ExportConfig configures the export path.
type ExportConfig struct {
	Exporter     exporter.Config
	Tracker      exporter.TrackerConfig
	Tick         time.Duration
	NoEngineWait time.Duration
}

// EnableExport turns on the export path, and runs its scheduler until ctx
// ends. Without it, the service exports nothing on its own. Each namespace
// gets an exporter of cfg.Exporter, with the namespace as its scope and its
// own view of the blob store (see blobstore.Namespacer); cfg.Exporter.Scope
// is the namespace of a service without tokens. Call it before the service
// serves engines.
func (s *Service) EnableExport(ctx context.Context, cfg ExportConfig) error {
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
	p := &exportPath{
		cfg:          cfg.Exporter,
		trackerCfg:   cfg.Tracker,
		tick:         cfg.Tick,
		noEngineWait: cfg.NoEngineWait,
		now:          time.Now,
		ctx:          ctx,
	}
	s.export = p
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		s.scheduleExports(ctx)
	}()
	return nil
}

// scheduleExports runs the exports that come due until ctx ends, then waits
// for those in flight.
func (s *Service) scheduleExports(ctx context.Context) {
	p := s.export
	t := time.NewTicker(p.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// Drain schedules the exports while the service drains.
		if s.draining.Load() {
			continue
		}
		for _, ns := range s.namespacesNow() {
			s.startBatches(ctx, ns, ns.tracker.Due(p.now()))
		}
	}
}

// startBatches runs batches of a namespace, each in its own goroutine, which
// Wait waits for.
func (s *Service) startBatches(ctx context.Context, ns *namespace, batches []exporter.Batch) {
	p := s.export
	for _, b := range batches {
		p.wg.Add(1)
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
	_, _ = s.exportBatch(ctx, eng, b)
}

// exportBatch exports b on eng, records its result in the tracker, and
// measures it.
func (s *Service) exportBatch(ctx context.Context, eng *engineConn, b exporter.Batch) (exporter.Result, error) {
	start := time.Now()
	res, err := eng.ns.exporter.Export(metrics.WithCaller(ctx, metrics.CallerExport), eng, b.Request)
	outcome := s.recordExport(ctx, eng, b, res, err)
	if len(res.Stored) > 0 && s.draining.Load() {
		s.drainStored(eng.CacheID(), len(res.Stored))
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

// engineFor returns the latest welcomed connection of an engine cache in a
// namespace. A connection of another namespace that says hello with the same
// cache ID is never chosen: its token gives it no say over this namespace's
// exports.
func (s *Service) engineFor(ns *namespace, cacheID string) *engineConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest *engineConn
	for _, c := range s.conns {
		if c.ns == ns && c.hello.CacheID == cacheID && (latest == nil || c.id > latest.id) {
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
		cacheID, generation, ok := parseEngineCache(stringAttr(res, telemetryattrs.EngineCacheAttr))
		if !ok {
			continue
		}
		session := stringAttr(res, telemetryattrs.EngineSessionAttr)
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
func parseEngineCache(v string) (string, uint64, bool) {
	i := strings.LastIndexByte(v, '/')
	if i <= 0 {
		return "", 0, false
	}
	gen, err := strconv.ParseUint(v[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return v[:i], gen, true
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
		writeDebugError(w, ErrExportOff)
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
		writeDebugError(w, ErrExportOff)
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
