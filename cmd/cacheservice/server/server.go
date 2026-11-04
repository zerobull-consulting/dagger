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
//   - DebugHandler, unauthenticated, for local use only: it lists the
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
	"slices"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
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

// ReadOnly reports whether the service is read-only (SetReadOnly).
func (s *Service) ReadOnly() bool { return s.readOnly }

// EnableS3 turns on the export path, unless opts.NoExport is set, and the
// import path, unless opts.NoImport is, with the S3 blob and metadata stores
// of cfg, until ctx ends. With the export path on, it refuses a store that
// ignores conditional writes, which would let index updates overwrite each
// other; that check writes, so it also refuses a read-only key.
func (s *Service) EnableS3(ctx context.Context, cfg blobstore.S3Config, opts S3Options) error {
	if err := checkS3Options(opts); err != nil {
		return err
	}
	blobs, err := blobstore.NewS3(ctx, cfg)
	if err != nil {
		return err
	}
	meta, err := metastore.NewS3(ctx, cfg)
	if err != nil {
		return err
	}
	return s.enableStores(ctx, Stores{
		Blobs: blobs,
		Meta:  meta,
		GCBucket: func() (gc.Bucket, error) {
			client, err := blobstore.NewS3Client(ctx, cfg)
			if err != nil {
				return nil, err
			}
			// A custom endpoint is an S3-compatible store, which may list
			// multipart uploads only by exact key (MinIO); AWS S3 lists them
			// by prefix.
			return &gc.S3Bucket{Client: client, Bucket: cfg.Bucket, FilterAllUploads: cfg.Endpoint != ""}, nil
		},
		LogAttrs: []any{"bucket", cfg.Bucket, "prefix", cfg.Prefix},
	}, opts)
}

// Stores are the stores the paths EnableS3 turns on run with.
type Stores struct {
	Blobs blobstore.Store
	Meta  *metastore.Store
	// GCBucket returns the bucket GC lists and deletes, when GC is on.
	GCBucket func() (gc.Bucket, error)
	// LogAttrs say where the stores are, in the startup log.
	LogAttrs []any
}

// checkS3Options refuses the options that can't go together.
func checkS3Options(opts S3Options) error {
	if !opts.NoExport {
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
func (s *Service) enableStores(ctx context.Context, st Stores, opts S3Options) error {
	if err := checkS3Options(opts); err != nil {
		return err
	}
	where := append(slices.Clip(st.LogAttrs), "scope", opts.Scope)
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

// Wait waits for the exports in flight, those a drain started included, the
// import sync loops and GC, after the context that runs them has ended.
func (s *Service) Wait() {
	s.waitExports()
	s.waitImports()
	s.waitGC()
}
