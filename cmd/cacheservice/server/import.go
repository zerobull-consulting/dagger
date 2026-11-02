package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// importPath merges stored bundles into connected engines, so a fresh engine
// reuses what other engines exported. Each welcomed connection gets a sync
// loop: it syncs once on welcome, again whenever another engine's export is
// stored, and every poll interval, which picks up bundles stored by other
// service replicas.
type importPath struct {
	// cfg configures each namespace's importer; cfg.Scope is replaced by
	// the namespace.
	cfg importer.Config
	// poll is how often each engine is synced without a trigger (0: never).
	poll time.Duration
	// ctx ends every sync loop when the service stops.
	ctx context.Context

	mu       sync.Mutex
	triggers map[*engineConn]chan struct{}
	// closed is set when the service waits for the loops: no loop starts
	// after it, so no wg.Add races wg.Wait.
	closed bool
	wg     sync.WaitGroup
}

// ImportConfig configures the import path.
type ImportConfig struct {
	Importer importer.Config
	Poll     time.Duration
}

// DefaultImportPoll is how often the import path looks for new bundles
// without a trigger.
const DefaultImportPoll = 30 * time.Second

// EnableImport turns on the import path until ctx ends. Turn on the export
// path first: the import path tells each namespace's tracker what each merge
// left in an engine cache, so an unchanged import isn't exported back. Each
// namespace gets an importer of cfg.Importer, with the namespace as its scope
// and its own view of the blob store; cfg.Importer.Scope is the namespace of
// a service without tokens. Call it before the service serves engines.
func (s *Service) EnableImport(ctx context.Context, cfg ImportConfig) error {
	if cfg.Importer.Logger == nil {
		cfg.Importer.Logger = s.logger.With("component", "import")
	}
	if cfg.Importer.Meta != nil && cfg.Importer.Meta.Metrics() == nil {
		cfg.Importer.Meta = cfg.Importer.Meta.WithMetrics(s.metrics)
	}
	if cfg.Importer.Metrics == nil {
		cfg.Importer.Metrics = s.metrics
	}
	// Checked now, so a bad config fails here rather than on a connection.
	if _, err := importer.New(cfg.Importer); err != nil {
		return err
	}
	if err := s.setDefaultNamespace(cfg.Importer.Scope); err != nil {
		return err
	}
	s.imports = &importPath{cfg: cfg.Importer, poll: cfg.Poll, ctx: ctx, triggers: map[*engineConn]chan struct{}{}}
	return nil
}

// startImports runs c's sync loop until connCtx or the service ends.
func (s *Service) startImports(connCtx context.Context, c *engineConn) {
	p := s.imports
	if p == nil {
		return
	}
	trigger := make(chan struct{}, 1)
	trigger <- struct{}{}
	p.mu.Lock()
	if p.closed || p.ctx.Err() != nil {
		p.mu.Unlock()
		return
	}
	p.triggers[c] = trigger
	p.wg.Add(1)
	p.mu.Unlock()
	ctx, cancel := context.WithCancel(connCtx)
	stop := context.AfterFunc(p.ctx, cancel)
	go func() {
		defer p.wg.Done()
		defer cancel()
		defer stop()
		defer func() {
			p.mu.Lock()
			delete(p.triggers, c)
			p.mu.Unlock()
		}()
		var tick <-chan time.Time
		if p.poll > 0 {
			t := time.NewTicker(p.poll)
			defer t.Stop()
			tick = t.C
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-trigger:
			case <-tick:
			}
			s.syncImports(ctx, c)
		}
	}()
}

// syncImports syncs one engine and logs what it did.
func (s *Service) syncImports(ctx context.Context, c *engineConn) importer.Result {
	res, err := s.syncMeasured(ctx, c)
	if err != nil && ctx.Err() == nil && s.connected(c) {
		c.logger.Warn("import sync failed", "error", err)
	}
	if len(res.Merged) > 0 || len(res.Failed) > 0 {
		c.logger.Info("import sync", "merged", len(res.Merged), "failed", len(res.Failed),
			"accepted", res.Offers.Accepted, "alreadyComplete", res.Offers.AlreadyComplete)
	}
	return res
}

// syncMeasured syncs one engine, measures the sync, and labels its index
// reads and updates as the import path's, whether a sync loop or the debug
// API asked for it.
func (s *Service) syncMeasured(ctx context.Context, c *engineConn) (importer.Result, error) {
	start := time.Now()
	res, err := c.ns.importer.Sync(metrics.WithCaller(ctx, metrics.CallerImport), c)
	s.metrics.ImportSync(c.ns.name, err != nil, time.Since(start))
	return res, err
}

// triggerImports syncs every connected engine of a namespace but those of
// cache except, which just exported what there is to import. Engines of
// other namespaces can't import it.
func (s *Service) triggerImports(ns *namespace, except string) {
	p := s.imports
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for c, trigger := range p.triggers {
		if c.ns != ns || c.CacheID() == except {
			continue
		}
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
}

// forgetWiped forgets a wiped engine cache: what the importer knows of it,
// now, and its imports in the index, in the background, so the telemetry
// request that reported the wipe doesn't wait on the store. A merge whose
// imports land after the index write finds the cache forgotten, and drops
// them again. A write cut short by the service stopping leaves the dead
// cache's imports in the index; its ID never connects again, so nothing
// uses them, and they go when their entries are dropped.
func (s *Service) forgetWiped(ns *namespace, cacheID string) {
	p := s.imports
	ns.importer.Forget(cacheID)
	p.mu.Lock()
	if p.closed || p.ctx.Err() != nil {
		p.mu.Unlock()
		return
	}
	p.wg.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.wg.Done()
		ctx, cancel := context.WithTimeout(p.ctx, time.Minute)
		defer cancel()
		if err := ns.importer.ForgetImports(ctx, cacheID); err != nil {
			s.logger.Warn("forgetting a wiped cache's imports failed", "namespace", ns.name, "cacheID", cacheID, "error", err)
		}
	}()
}

// renew answers an engine's renew: fresh download addresses when the import
// path is on and every layer is stored in the connection's namespace,
// unavailable otherwise. The renewal key must be the namespace (the importer
// sends it as every offered chain's), and each layer must be stored under
// the namespace's own blob keys, so a connection can't renew another
// namespace's blobs, whatever digests it names.
//
// Renewals are not limited to the digests offered to the connection: an
// engine renews the parts of entries merged before a reconnect, or before
// the service restarted, which this process never offered it. Within its
// namespace, a token could have those blobs merged into its engine anyway.
// TestRenewNotLimitedToOfferedDigests pins it.
func (s *Service) renew(ctx context.Context, c *engineConn, req protocol.Renew) protocol.Renewed {
	if s.imports == nil {
		return protocol.Renewed{Unavailable: true}
	}
	return c.ns.importer.Renew(ctx, req)
}

// waitImports waits for the sync loops, after the context that runs them has
// ended.
func (s *Service) waitImports() {
	if p := s.imports; p != nil {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.wg.Wait()
	}
}

// serveImport is the explicit import hook: POST /debug/engines/{conn}/import
// syncs the engine now, and answers with what was merged and offered.
func (s *Service) serveImport(w http.ResponseWriter, r *http.Request) {
	if s.imports == nil {
		writeDebugError(w, ErrImportOff)
		return
	}
	conn, ok := debugConnID(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
	defer cancel()
	res, err := s.Import(ctx, conn)
	if err != nil {
		writeDebugError(w, err)
		return
	}
	writeJSON(w, res)
}

// serveImports shows what the import path knows of an engine's cache: the
// bundles merged into it, and the entries they landed on.
func (s *Service) serveImports(w http.ResponseWriter, r *http.Request) {
	if s.imports == nil {
		writeDebugError(w, ErrImportOff)
		return
	}
	conn, ok := debugConnID(w, r)
	if !ok {
		return
	}
	st, err := s.Imports(conn)
	if err != nil {
		writeDebugError(w, err)
		return
	}
	writeJSON(w, st)
}

// importer.Engine is satisfied by the connection, as exporter.Engine is.
var _ importer.Engine = (*engineConn)(nil)
