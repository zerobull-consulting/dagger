// Command cacheservice is a protocol spike of the remote cache service: the
// smallest service an engine talks to, built to observe the engine's real
// behavior before designing storage. It stores nothing.
//
// It serves:
//
//   - a WebSocket at protocol.EnginePath that takes the HTTP Basic username
//     as the engine token (any token), answers hello with welcome, answers
//     renew with unavailable, and logs every envelope;
//
//   - OTLP/HTTP receivers at /v1/logs, /v1/traces and /v1/metrics, so that an
//     engine whose DAGGER_CLOUD_URL points here delivers its cache events
//     (the engine.* log records) here too, and they are logged;
//
//   - PUT /blobs/{digest}, a stand-in for presigned upload URLs that checks
//     each blob against its digest and keeps nothing;
//
//   - with CACHESERVICE_S3_BUCKET set, the export path: it learns each
//     engine's retained entries from session spans, exports them when their
//     session ends, has the engine upload their layers to S3 through
//     presigned PUTs, and records their bundle in the scope's index;
//
//   - on a separate, local-only address, a debug API that lists the connected
//     engines and sends a request to one, to probe what the engine answers:
//
//     GET  /debug/engines
//     POST /debug/engines/{conn}/request?type=export   (body: the request body)
//     POST /debug/engines/{conn}/export                (export now; body: optional roots)
//     GET  /debug/engines/{conn}/entries               (what the export path tracks)
//
// Point an engine at it with:
//
//	_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL=http://<host>:8090
//	DAGGER_CLOUD_TOKEN=<anything>
//	DAGGER_CLOUD_URL=http://<host>:8090   (optional: receive the cache events)
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

func main() {
	var (
		listen      string
		debugListen string
		dumpDir     string
		verbose     bool
		scope       string
		quiet       time.Duration
		endGrace    time.Duration
	)
	flag.StringVar(&listen, "listen", ":8090", "address of the engine WebSocket and the OTLP receivers")
	flag.StringVar(&debugListen, "debug-listen", "127.0.0.1:8091", "address of the unauthenticated debug API (empty: off)")
	flag.StringVar(&dumpDir, "dump-dir", "", "write every envelope and engine event body to this directory")
	flag.BoolVar(&verbose, "v", false, "log pings and every cache span")
	flag.StringVar(&scope, "scope", "default", "the scope exported entries are recorded in")
	flag.DurationVar(&quiet, "export-quiet", exporter.DefaultQuietPeriod, "export a session's entries after this long without a cache span from it, if its end isn't seen")
	flag.DurationVar(&endGrace, "export-end-grace", exporter.DefaultEndGrace, "after a session ends, wait this long for the cache spans it said were coming")
	flag.Parse()

	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if dumpDir != "" {
		if err := os.MkdirAll(dumpDir, 0o755); err != nil {
			logger.Error("create dump dir", "error", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc := newService(logger, dumpDir)
	if os.Getenv(blobstore.EnvBucket) != "" {
		if err := enableS3Export(ctx, svc, scope, exporter.TrackerConfig{QuietPeriod: quiet, EndGrace: endGrace}); err != nil {
			logger.Error("export path", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Info("export path off: set " + blobstore.EnvBucket + " to store exports in S3")
	}

	servers := []*http.Server{{Addr: listen, Handler: svc.handler()}}
	if debugListen != "" {
		servers = append(servers, &http.Server{Addr: debugListen, Handler: svc.debugHandler()})
	}
	errs := make(chan error, len(servers))
	for _, srv := range servers {
		logger.Info("listening", "addr", srv.Addr)
		go func() {
			if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errs <- err
			}
		}()
	}

	select {
	case <-ctx.Done():
	case err := <-errs:
		logger.Error("serve", "error", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	svc.closeAll()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}
	// End the exports in flight, and wait for them.
	stop()
	svc.waitExports()
}

// enableS3Export turns on the export path with the S3 blob and metadata
// stores from the CACHESERVICE_S3_* settings. It refuses a store that
// ignores conditional writes, which would let index updates overwrite each
// other.
func enableS3Export(ctx context.Context, svc *service, scope string, tracker exporter.TrackerConfig) error {
	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
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
	if err := meta.VerifyConditionalWrites(ctx, scope); err != nil {
		return err
	}
	svc.logger.Info("export path on", "bucket", cfg.Bucket, "prefix", cfg.Prefix, "scope", scope)
	return svc.enableExport(ctx, exportConfig{
		Exporter: exporter.Config{Blobs: blobs, Meta: meta, Scope: scope},
		Tracker:  tracker,
	})
}
