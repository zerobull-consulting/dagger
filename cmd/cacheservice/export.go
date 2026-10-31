package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// exportPath pushes engines' cache entries to the store: the tracker learns
// entries from OTLP, and the scheduler exports them when they are due.
type exportPath struct {
	tracker  *exporter.Tracker
	exporter *exporter.Exporter
	// tick is how often the scheduler looks for due exports, and
	// noEngineWait how long a batch whose engine isn't connected waits.
	tick         time.Duration
	noEngineWait time.Duration
	now          func() time.Time

	wg sync.WaitGroup
}

// exportConfig configures the export path.
type exportConfig struct {
	Exporter     exporter.Config
	Tracker      exporter.TrackerConfig
	Tick         time.Duration
	NoEngineWait time.Duration
}

// enableExport turns on the export path, and runs its scheduler until ctx
// ends. Without it, the service exports nothing on its own.
func (s *service) enableExport(ctx context.Context, cfg exportConfig) error {
	if cfg.Exporter.Logger == nil {
		cfg.Exporter.Logger = s.logger.With("component", "export")
	}
	x, err := exporter.New(cfg.Exporter)
	if err != nil {
		return err
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.NoEngineWait <= 0 {
		cfg.NoEngineWait = 10 * time.Second
	}
	p := &exportPath{
		tracker:      exporter.NewTracker(cfg.Tracker),
		exporter:     x,
		tick:         cfg.Tick,
		noEngineWait: cfg.NoEngineWait,
		now:          time.Now,
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
func (s *service) scheduleExports(ctx context.Context) {
	p := s.export
	t := time.NewTicker(p.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, b := range p.tracker.Due(p.now()) {
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				s.runBatch(ctx, b)
			}()
		}
	}
}

// runBatch exports one batch on the engine connected with its cache.
func (s *service) runBatch(ctx context.Context, b exporter.Batch) {
	p := s.export
	eng := s.engineFor(b.CacheID)
	if eng == nil {
		s.logger.Debug("export: engine not connected; waiting", "cacheID", b.CacheID, "roots", len(b.Request.Roots))
		p.tracker.Requeue(b, p.now().Add(p.noEngineWait))
		return
	}
	res, err := p.exporter.Export(ctx, eng, b.Request)
	if err != nil {
		eng.logger.Warn("export failed", "roots", len(b.Request.Roots), "error", err)
	}
	p.tracker.Done(b, res, p.now())
}

// engineFor returns the latest welcomed connection of an engine cache.
func (s *service) engineFor(cacheID string) *engineConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest *engineConn
	for _, c := range s.conns {
		if c.hello.CacheID == cacheID && (latest == nil || c.id > latest.id) {
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

// trackSpans feeds the tracker the cache spans of a trace request.
func (s *service) trackSpans(req *coltracepb.ExportTraceServiceRequest) {
	p := s.export
	if p == nil {
		return
	}
	now := p.now()
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
					if err == nil {
						p.tracker.SessionEnded(cacheID, generation, session, count, now)
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
				p.tracker.Observe(o, now)
			}
		}
	}
}

// trackEvent feeds the tracker one engine cache event.
func (s *service) trackEvent(kind, cache, body string) {
	p := s.export
	if p == nil {
		return
	}
	cacheID, generation, ok := parseEngineCache(cache)
	if !ok {
		return
	}
	switch kind {
	case telemetryattrs.EngineEventStart:
		var ev telemetryattrs.EngineStartEvent
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			s.logger.Warn("undecodable engine event", "kind", kind, "error", err)
			return
		}
		p.tracker.Started(cacheID, generation, ev.WipedCache)
	case telemetryattrs.EngineEventPrune:
		var ev telemetryattrs.EnginePruneEvent
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			s.logger.Warn("undecodable engine event", "kind", kind, "error", err)
			return
		}
		drops := make([]exporter.Drop, 0, len(ev.Drops))
		for _, d := range ev.Drops {
			drops = append(drops, exporter.Drop{Number: d.ResultID, DroppedAtUnixNano: d.DroppedAtUnixNano})
		}
		p.tracker.Pruned(cacheID, generation, drops)
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
func (s *service) serveExport(w http.ResponseWriter, r *http.Request) {
	p := s.export
	if p == nil {
		http.Error(w, "the export path is off: no store is configured", http.StatusConflict)
		return
	}
	c := s.debugConn(w, r)
	if c == nil {
		return
	}
	var req exporter.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "body: "+err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
	defer cancel()
	if len(req.Roots) > 0 {
		res, err := p.exporter.Export(ctx, c, req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, res)
		return
	}
	var all exporter.Result
	batches := p.tracker.All(c.CacheID(), p.now())
	for i, b := range batches {
		res, err := p.exporter.Export(ctx, c, b.Request)
		p.tracker.Done(b, res, p.now())
		if err != nil {
			// Hand the batches not run back to the scheduler.
			for _, rest := range batches[i+1:] {
				p.tracker.Requeue(rest, p.now())
			}
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		all.Merge(res)
	}
	writeJSON(w, all)
}

// serveEntries lists what the tracker knows of an engine's cache.
func (s *service) serveEntries(w http.ResponseWriter, r *http.Request) {
	p := s.export
	if p == nil {
		http.Error(w, "the export path is off: no store is configured", http.StatusConflict)
		return
	}
	c := s.debugConn(w, r)
	if c == nil {
		return
	}
	entries := p.tracker.Entries(c.CacheID())
	if entries == nil {
		entries = []exporter.EntryState{}
	}
	writeJSON(w, entries)
}

func (s *service) debugConn(w http.ResponseWriter, r *http.Request) *engineConn {
	id, err := strconv.ParseUint(r.PathValue("conn"), 10, 64)
	if err != nil {
		http.Error(w, "bad conn", http.StatusBadRequest)
		return nil
	}
	s.mu.Lock()
	c := s.conns[id]
	s.mu.Unlock()
	if c == nil {
		http.Error(w, "no such engine", http.StatusNotFound)
		return nil
	}
	return c
}

// waitExports waits for the exports in flight, after the context that runs
// them has ended.
func (s *service) waitExports() {
	if s.export != nil {
		s.export.wg.Wait()
	}
}
