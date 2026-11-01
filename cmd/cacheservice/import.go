package main

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// importPath merges stored bundles into connected engines, so a fresh engine
// reuses what other engines exported. Each welcomed connection gets a sync
// loop: it syncs once on welcome, again whenever another engine's export is
// stored, and every poll interval, which picks up bundles stored by other
// service replicas.
type importPath struct {
	importer *importer.Importer
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

// importConfig configures the import path.
type importConfig struct {
	Importer importer.Config
	Poll     time.Duration
}

// DefaultImportPoll is how often the import path looks for new bundles
// without a trigger.
const DefaultImportPoll = 30 * time.Second

// enableImport turns on the import path until ctx ends.
func (s *service) enableImport(ctx context.Context, cfg importConfig) error {
	if cfg.Importer.Logger == nil {
		cfg.Importer.Logger = s.logger.With("component", "import")
	}
	im, err := importer.New(cfg.Importer)
	if err != nil {
		return err
	}
	s.imports = &importPath{importer: im, poll: cfg.Poll, ctx: ctx, triggers: map[*engineConn]chan struct{}{}}
	return nil
}

// startImports runs c's sync loop until connCtx or the service ends.
func (s *service) startImports(connCtx context.Context, c *engineConn) {
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
func (s *service) syncImports(ctx context.Context, c *engineConn) importer.Result {
	res, err := s.imports.importer.Sync(ctx, c)
	if err != nil && ctx.Err() == nil && s.connected(c) {
		c.logger.Warn("import sync failed", "error", err)
	}
	if len(res.Merged) > 0 || len(res.Failed) > 0 {
		c.logger.Info("import sync", "merged", len(res.Merged), "failed", len(res.Failed),
			"accepted", res.Offers.Accepted, "alreadyComplete", res.Offers.AlreadyComplete)
	}
	return res
}

// triggerImports syncs every connected engine but those of cache except,
// which just exported what there is to import.
func (s *service) triggerImports(except string) {
	p := s.imports
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for c, trigger := range p.triggers {
		if c.CacheID() == except {
			continue
		}
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
}

// renew answers an engine's renew: fresh download addresses when the import
// path is on and every layer is stored, unavailable otherwise.
func (s *service) renew(ctx context.Context, req protocol.Renew) protocol.Renewed {
	if s.imports == nil {
		return protocol.Renewed{Unavailable: true}
	}
	return s.imports.importer.Renew(ctx, req)
}

// waitImports waits for the sync loops, after the context that runs them has
// ended.
func (s *service) waitImports() {
	if p := s.imports; p != nil {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.wg.Wait()
	}
}

// serveImport is the explicit import hook: POST /debug/engines/{conn}/import
// syncs the engine now, and answers with what was merged and offered.
func (s *service) serveImport(w http.ResponseWriter, r *http.Request) {
	if s.imports == nil {
		http.Error(w, "the import path is off: no store is configured", http.StatusConflict)
		return
	}
	c := s.debugConn(w, r)
	if c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
	defer cancel()
	res, err := s.imports.importer.Sync(ctx, c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, res)
}

// serveImports shows what the import path knows of an engine's cache: the
// bundles merged into it, and the entries they landed on.
func (s *service) serveImports(w http.ResponseWriter, r *http.Request) {
	if s.imports == nil {
		http.Error(w, "the import path is off: no store is configured", http.StatusConflict)
		return
	}
	c := s.debugConn(w, r)
	if c == nil {
		return
	}
	st, ok := s.imports.importer.State(c.CacheID())
	if !ok {
		st = importer.State{CacheID: c.CacheID(), Generation: c.Generation(), Merged: []importer.MergedBundle{}, Values: map[uint64]importer.Value{}}
	}
	writeJSON(w, st)
}

// importer.Engine is satisfied by the connection, as exporter.Engine is.
var _ importer.Engine = (*engineConn)(nil)
