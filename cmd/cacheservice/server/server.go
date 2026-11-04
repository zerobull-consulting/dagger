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
// deadline in its context; then CloseEngines, stop serving, cancel the
// context passed to the Enable methods, and Wait. Without Drain, the exports
// pending or in flight are lost (cmd/cacheservice's README, "Stopping and
// draining").
package server

import (
	"context"
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

// EnableS3 turns on the export path, and the import path unless
// opts.NoImport is set, with the S3 blob and metadata stores of cfg, until
// ctx ends. It refuses a store that ignores conditional writes, which would
// let index updates overwrite each other.
func (s *Service) EnableS3(ctx context.Context, cfg blobstore.S3Config, opts S3Options) error {
	blobs, err := blobstore.NewS3(ctx, cfg)
	if err != nil {
		return err
	}
	meta, err := metastore.NewS3(ctx, cfg)
	if err != nil {
		return err
	}
	if err := meta.VerifyConditionalWrites(ctx, opts.Scope); err != nil {
		return err
	}
	s.logger.Info("export path on", "bucket", cfg.Bucket, "prefix", cfg.Prefix, "scope", opts.Scope)
	if err := s.EnableExport(ctx, ExportConfig{
		Exporter:     exporter.Config{Blobs: blobs, Meta: meta, Scope: opts.Scope},
		Tracker:      opts.Tracker,
		NoEngineWait: opts.NoEngineWait,
	}); err != nil {
		return err
	}
	if opts.NoImport {
		s.logger.Info("import path off")
	} else {
		s.logger.Info("import path on", "scope", opts.Scope, "poll", opts.ImportPoll)
		if err := s.EnableImport(ctx, ImportConfig{
			Importer: importer.Config{Blobs: blobs, Meta: meta, Scope: opts.Scope},
			Poll:     opts.ImportPoll,
		}); err != nil {
			return err
		}
	}
	if opts.GC == nil {
		s.logger.Info("gc off")
		return nil
	}
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		return err
	}
	g := *opts.GC
	// A custom endpoint is an S3-compatible store, which may list multipart
	// uploads only by exact key (MinIO); AWS S3 lists them by prefix.
	g.Collector.Bucket = &gc.S3Bucket{Client: client, Bucket: cfg.Bucket, FilterAllUploads: cfg.Endpoint != ""}
	g.Collector.Meta = meta
	return s.EnableGC(ctx, g)
}

// Wait waits for the exports in flight, those a drain started included, the
// import sync loops and GC, after the context that runs them has ended.
func (s *Service) Wait() {
	s.waitExports()
	s.waitImports()
	s.waitGC()
}
