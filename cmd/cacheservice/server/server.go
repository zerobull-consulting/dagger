// Package server is the remote cache service, without the process around it:
// cmd/cacheservice parses flags and listens, and tests can run the same
// service in-process, on addresses and stores of their choosing.
//
// A Service serves two handlers:
//
//   - Handler, for engines: a WebSocket at protocol.EnginePath that takes the
//     HTTP Basic username as the engine token (any token) and answers hello
//     with welcome; OTLP/HTTP receivers at /v1/logs, /v1/traces and
//     /v1/metrics, where engines whose DAGGER_CLOUD_URL points here deliver
//     their cache events and session spans; and PUT /blobs/{digest}, a
//     stand-in for presigned upload URLs that checks each blob against its
//     digest and keeps nothing;
//
//   - DebugHandler, for operators, behind an admin token when one is set
//     (UseDebugTokenFile): it lists the
//     connected engines, sends a request to one, and shows and triggers
//     what the export and import paths do for it.
//
// With no store, the service only logs what engines send. EnableS3 (or
// EnableExport and EnableImport, with stores of the caller's choosing) turns
// on the export path, which learns each engine's retained entries from
// session spans and stores them when their session ends, and the import path,
// which merges stored entries into the engines that lack them.
//
// To stop a service cleanly: Drain, while it still serves, with the drain's
// deadline in its context; then cancel the context passed to the Enable
// methods, CloseEngines, stop serving, and Wait. Without Drain, the exports
// pending or in flight are lost (cmd/cacheservice's README, "Stopping and
// draining").
package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// S3Options configures the paths EnableS3 turns on.
type S3Options struct {
	// Scope is the scope entries are recorded in and merged from.
	Scope string
	// Tracker configures how the export path learns entries and when it
	// exports them.
	Tracker exporter.TrackerConfig
	// NoExport leaves the export path off, and makes the service read-only
	// (see SetReadOnly): it only imports, and never writes to or deletes
	// from the bucket, so it runs with a read-only key. GC must be nil, and
	// NoImport unset.
	NoExport bool
	// NoImport leaves the import path off.
	NoImport bool
	// ImportPoll is how often the import path looks for stored entries a
	// connected engine lacks; 0 means only on hello and after an export.
	ImportPoll time.Duration
	// NoEngineWait is how long an export waits for its engine to connect,
	// or to reconnect after its connection was lost mid-export; 0 means
	// the export path's default (10s).
	NoEngineWait time.Duration
	// GC turns on garbage collection when set (see EnableGC); its
	// Collector's Bucket and Meta are the S3 stores'.
	GC *GCConfig
}

// SetReadOnly makes the service read-only, as -export=false does: it never
// writes to or deletes from its store. The export path stays off:
// EnableExport and EnableGC refuse, spans are received and dropped, the
// debug export hook answers ErrReadOnly and POST /debug/gc ErrGCReadOnly
// (409 both), and a drain has nothing to flush. The import path merges,
// offers and renews as before, but records no imports in the index
// (importer.Config.NoRecordImports): they exist only for this engine's
// later exports. Call it before the Enable methods.
func (s *Service) SetReadOnly() error {
	if s.export != nil || s.imports != nil || s.gc != nil {
		return errors.New("read-only: call SetReadOnly before the Enable methods")
	}
	s.readOnly = true
	return nil
}

// EnableS3 turns on the export path, unless opts.NoExport is set, and the
// import path, unless opts.NoImport is, with the S3 blob and metadata stores
// of cfg, until ctx ends. First it checks that the prefix is the service's
// (ownership.Check): a writer claims an empty prefix, and both kinds of
// instance refuse one holding anything else, or another layout. With the
// export path on, it then refuses a store that ignores conditional writes,
// which would let index updates overwrite each other; that check writes, so
// it also refuses a read-only key.
func (s *Service) EnableS3(ctx context.Context, cfg blobstore.S3Config, opts S3Options) error {
	// Before the stores are built: a refused combination touches nothing.
	if err := s.checkS3Options(opts); err != nil {
		return err
	}
	blobs, err := blobstore.NewS3(ctx, cfg)
	if err != nil {
		return err
	}
	// One client for the blob store, the metadata store and GC: they share
	// cfg, so they reach S3 the same way.
	client := blobs.Client()
	meta, err := metastore.NewS3WithClient(ctx, cfg, client)
	if err != nil {
		return err
	}
	return s.enableStores(ctx, storeSet{
		Blobs: blobs,
		Meta:  meta,
		GCBucket: func() (gc.Bucket, error) {
			// A custom endpoint is an S3-compatible store, which may list
			// multipart uploads only by exact key (MinIO); AWS S3 lists them
			// by prefix.
			return &gc.S3Bucket{Client: client, Bucket: cfg.Bucket, FilterAllUploads: cfg.Endpoint != ""}, nil
		},
		LogAttrs: []any{"bucket", cfg.Bucket, "prefix", cfg.Prefix},
	}, opts)
}

// storeSet is the stores the paths EnableS3 turns on run with.
type storeSet struct {
	Blobs blobstore.Store
	Meta  *metastore.Store
	// GCBucket returns the bucket GC lists and deletes, when GC is on.
	GCBucket func() (gc.Bucket, error)
	// LogAttrs say where the stores are, in the startup log.
	LogAttrs []any
	// Owner is where the ownership check runs; nil is Meta's objects,
	// which must then implement ownership.Store.
	Owner ownership.Store
}

// checkS3Options refuses the options that can't go together, or with the
// service's state: a read-only service (SetReadOnly) takes only NoExport,
// since the export path's conditional write check would write.
func (s *Service) checkS3Options(opts S3Options) error {
	// Before the ownership check, which may claim the prefix.
	if err := metastore.ValidateScope(opts.Scope); err != nil {
		return err
	}
	if !opts.NoExport {
		if s.readOnly {
			return errors.New("the service is read-only (SetReadOnly): set NoExport")
		}
		return nil
	}
	if opts.NoImport {
		return errors.New("both paths off (NoExport and NoImport): the service would have nothing to do")
	}
	if opts.GC != nil {
		return errors.New("gc with NoExport: a read-only service never deletes from the bucket")
	}
	return nil
}

// enableStores is EnableS3 on stores of the caller's choosing: tests run it on
// in-memory stores, with the same options.
func (s *Service) enableStores(ctx context.Context, st storeSet, opts S3Options) error {
	if err := s.checkS3Options(opts); err != nil {
		return err
	}
	where := append(slices.Clip(st.LogAttrs), "scope", opts.Scope)
	if err := s.checkOwnership(ctx, st, !opts.NoExport, where); err != nil {
		return err
	}
	if opts.NoExport {
		if err := s.SetReadOnly(); err != nil {
			return err
		}
		// The check is of the index's conditional writes, which only the
		// export path makes; and it writes, which a read-only key can't.
		s.logger.Info("conditional write check skipped: the instance is read-only (-export=false), and never writes the index")
		s.logger.Info("export path off: read-only (-export=false); spans are received and dropped", where...)
	} else {
		if err := st.Meta.VerifyConditionalWrites(ctx, opts.Scope); err != nil {
			return err
		}
		s.logger.Info("export path on", where...)
		if err := s.EnableExport(ctx, ExportConfig{
			Exporter:     exporter.Config{Blobs: st.Blobs, Meta: st.Meta, Scope: opts.Scope},
			Tracker:      opts.Tracker,
			NoEngineWait: opts.NoEngineWait,
		}); err != nil {
			return err
		}
	}
	if opts.NoImport {
		s.logger.Info("import path off")
	} else {
		s.logger.Info("import path on", "scope", opts.Scope, "poll", opts.ImportPoll, "recordImports", !opts.NoExport)
		if err := s.EnableImport(ctx, ImportConfig{
			Importer: importer.Config{Blobs: st.Blobs, Meta: st.Meta, Scope: opts.Scope},
			Poll:     opts.ImportPoll,
		}); err != nil {
			return err
		}
	}
	if opts.GC == nil {
		s.logger.Info("gc off")
		return nil
	}
	bucket, err := st.GCBucket()
	if err != nil {
		return err
	}
	g := *opts.GC
	g.Collector.Bucket = bucket
	g.Collector.Meta = st.Meta
	return s.EnableGC(ctx, g)
}

// checkOwnership is the startup check of the prefix (ownership.Check),
// before anything else touches the bucket: a writer claims an empty prefix,
// and every instance refuses a prefix that isn't the service's or is of
// another layout, without writing or deleting anything there. The
// conditional write check runs after it, in the scope's namespace: it
// overwrites its object, which the marker, never rewritten, can't be.
func (s *Service) checkOwnership(ctx context.Context, st storeSet, writer bool, where []any) error {
	owner := st.Owner
	if owner == nil {
		o, ok := st.Meta.Objects().(ownership.Store)
		if !ok {
			return errors.New("ownership check: the metadata store's objects can't be listed")
		}
		owner = o
	}
	state, m, err := ownership.Check(ctx, owner, st.Meta.Prefix(), writer, time.Now())
	if err != nil {
		return fmt.Errorf("ownership check: %w", err)
	}
	attrs := append(slices.Clip(where), "layout", layout.Version, "state", state.String())
	switch state {
	case ownership.Empty:
		s.logger.Warn("no store exists under the prefix yet: nothing to import until a writer (the cache-warming job) creates one", attrs...)
	case ownership.Claimed:
		s.logger.Info("prefix claimed: it was empty, and the ownership marker is created", append(attrs, "marker", layout.MarkerKey(st.Meta.Prefix()))...)
	default:
		s.logger.Info("prefix owned", append(attrs, "markerCreated", m.Created, "markerServiceVersion", m.ServiceVersion)...)
	}
	return nil
}

// Wait waits for the exports in flight, those a drain started included, the
// explicit exports and imports (Service.Export, Service.Import), the import
// sync loops and GC, runs asked for (CollectGarbage) included, after the
// context that runs them has ended. No explicit request is admitted once it
// began (ErrStopping).
func (s *Service) Wait() {
	s.waitExplicitHooks()
	s.waitExports()
	s.waitImports()
	s.waitGC()
	s.testHeld.Wait()
}

// HoldWaitForTest makes Wait wait for work that ignores cancellation, as
// GC's finish update and a promotion's cleanup do against a store that
// doesn't answer, until release is called. It is for the binary's tests of
// the stop's bound (cmd/cacheservice): nothing else calls it.
func (s *Service) HoldWaitForTest() (release func()) {
	s.testHeld.Add()
	var once sync.Once
	return func() { once.Do(s.testHeld.Done) }
}

// WaitContext is Wait, bounded by ctx: it returns ctx's cause if ctx ends
// first, with the work still running (see Running). The wait goes on in the
// background until that work ends; a process that stops on the error ends
// it with the process.
func (s *Service) WaitContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Both may be ready, as when ctx had ended before the call: what
		// ended in time counts.
		select {
		case <-done:
			return nil
		default:
		}
		return context.Cause(ctx)
	}
}

// Running is the work a service still runs, for a stop cut off by its bound
// to log.
type Running struct {
	// Exports are the exports in flight, and the export path's scheduler.
	Exports int
	// Imports are the import sync loops, and the index writes of wiped
	// caches.
	Imports int
	// GC are the GC runs, asked for or scheduled, and its schedule.
	GC int
	// Explicit are the explicit exports and imports (Service.Export,
	// Service.Import).
	Explicit int
	// Engines are the engine connections still welcomed. Wait doesn't
	// wait for them: CloseEngines closes them.
	Engines int
	// Held is the work HoldWaitForTest holds.
	Held int
}

// LogAttrs returns the counts of r as key-value pairs for slog.
func (r Running) LogAttrs() []any {
	attrs := []any{"exports", r.Exports, "imports", r.Imports, "gc", r.GC, "engines", r.Engines}
	if r.Explicit > 0 {
		attrs = append(attrs, "explicit", r.Explicit)
	}
	if r.Held > 0 {
		attrs = append(attrs, "held", r.Held)
	}
	return attrs
}

// Running returns the work the service runs now.
func (s *Service) Running() Running {
	var r Running
	if p := s.export; p != nil {
		r.Exports = p.wg.Running()
	}
	if p := s.imports; p != nil {
		r.Imports = p.wg.Running()
	}
	if p := s.gc; p != nil {
		r.GC = p.wg.Running()
	}
	r.Explicit = s.drain.explicitWG.Running()
	r.Held = s.testHeld.Running()
	s.mu.Lock()
	r.Engines = len(s.conns)
	s.mu.Unlock()
	return r
}

// workGroup is a sync.WaitGroup that counts what it waits for, so a stop cut
// off by its bound can say what was still running.
type workGroup struct {
	wg sync.WaitGroup
	n  atomic.Int64
}

func (g *workGroup) Add() {
	g.n.Add(1)
	g.wg.Add(1)
}

func (g *workGroup) Done() {
	g.n.Add(-1)
	g.wg.Done()
}

func (g *workGroup) Wait() { g.wg.Wait() }

// Running is how many Adds aren't Done yet.
func (g *workGroup) Running() int { return int(g.n.Load()) }
