// Command cacheservice is the remote cache service. It began as a protocol
// spike that stored nothing; with CACHESERVICE_S3_BUCKET set, it stores
// engines' cache entries in S3 and hands them to other engines.
//
// The service itself is package server; this command parses flags, reads
// the CACHESERVICE_S3_* settings and listens. It serves:
//
//   - a WebSocket at protocol.EnginePath that takes the HTTP Basic username
//     as the engine token, answers hello with welcome, answers renew
//     (unavailable without a store), and logs every envelope. With
//     -tokens-file, it refuses unknown tokens with 401 before the upgrade,
//     and each token's namespace decides everything its engines export,
//     import, are offered and renew; SIGHUP reloads the file and closes
//     the connections of removed tokens with status 4001. Without it, any
//     token is taken, every engine is in the -scope namespace, and the
//     service logs a warning that it is unauthenticated;
//
//   - OTLP/HTTP receivers at /v1/logs, /v1/traces and /v1/metrics, so that an
//     engine whose DAGGER_CLOUD_URL points here delivers its cache events
//     (the engine.* log records) here too, and they are logged. With
//     -tokens-file, they take the same tokens, and feed only their
//     namespace;
//
//   - PUT /blobs/{digest}, a stand-in for presigned upload URLs that checks
//     each blob against its digest and keeps nothing;
//
//   - with CACHESERVICE_S3_BUCKET set, the export path: it learns each
//     engine's retained entries from session spans, exports them when their
//     session ends, has the engine upload their layers to S3 through
//     presigned PUTs, and records their bundle in the scope's index;
//
//   - with CACHESERVICE_S3_BUCKET set, the import path too: on each engine's
//     hello, after another engine's export is stored, and every poll
//     interval, it merges the stored bundles the engine lacks, with presigned
//     GETs for their layers, offers the stored parts the merged entries
//     lack, and answers renew with fresh GETs;
//
//   - on a separate, local-only address, a debug API, not authenticated and
//     across every namespace, that lists the connected engines and sends a
//     request to one, to probe what the engine answers:
//
//     GET  /debug/engines
//     POST /debug/engines/{conn}/request?type=export   (body: the request body)
//     POST /debug/engines/{conn}/export                (export now; body: optional roots)
//     GET  /debug/engines/{conn}/entries               (what the export path tracks)
//     POST /debug/engines/{conn}/import                (merge and offer now)
//     GET  /debug/engines/{conn}/imports               (what the import path merged)
//     GET  /metrics                                    (Prometheus metrics)
//
//   - with -metrics-listen set, /metrics alone on that address, for a
//     scraper that shouldn't reach the debug API.
//
// With -tls-cert and -tls-key, the engine address serves HTTPS (wss:// for
// the WebSocket); otherwise run it behind a TLS-terminating proxy. See
// README.md, "Auth and namespaces".
//
// Point an engine at it with:
//
//	_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL=http://<host>:8090
//	DAGGER_CLOUD_TOKEN=<a token of the tokens file; anything without one>
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
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

func main() {
	var (
		listen        string
		debugListen   string
		metricsListen string
		dumpDir       string
		verbose       bool
		scope         string
		quiet         time.Duration
		endGrace      time.Duration
		imports       bool
		importPoll    time.Duration
		tokensFile    string
		tlsCert       string
		tlsKey        string
	)
	flag.StringVar(&listen, "listen", ":8090", "address of the engine WebSocket and the OTLP receivers")
	flag.StringVar(&debugListen, "debug-listen", "127.0.0.1:8091", "address of the unauthenticated debug API (empty: off)")
	flag.StringVar(&metricsListen, "metrics-listen", "", "address serving only the Prometheus metrics at /metrics (empty: off; the debug API serves them too)")
	flag.StringVar(&dumpDir, "dump-dir", "", "write every envelope and engine event body to this directory")
	flag.BoolVar(&verbose, "v", false, "log pings and every cache span")
	flag.StringVar(&scope, "scope", server.DefaultNamespace, "the namespace of every engine when there is no -tokens-file")
	flag.StringVar(&tokensFile, "tokens-file", "", "file of engine tokens and their namespaces, one '<token> <namespace>' per line; SIGHUP reloads it (empty: take any token, all in -scope)")
	flag.StringVar(&tlsCert, "tls-cert", "", "serve the engine address over TLS with this certificate (PEM; with -tls-key)")
	flag.StringVar(&tlsKey, "tls-key", "", "the key of -tls-cert (PEM)")
	flag.DurationVar(&quiet, "export-quiet", exporter.DefaultQuietPeriod, "export a session's entries after this long without a cache span from it, if its end isn't seen")
	flag.DurationVar(&endGrace, "export-end-grace", exporter.DefaultEndGrace, "after a session ends, wait this long for the cache spans it said were coming")
	flag.BoolVar(&imports, "import", true, "merge stored entries into connected engines (needs "+blobstore.EnvBucket+")")
	flag.DurationVar(&importPoll, "import-poll", server.DefaultImportPoll, "how often to look for stored entries a connected engine lacks (0: only on hello and after an export)")
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

	if (tlsCert == "") != (tlsKey == "") {
		logger.Error("set both -tls-cert and -tls-key, or neither")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc := server.New(logger, dumpDir)
	// The namespace of every engine without tokens, with a store or not.
	if err := svc.SetDefaultNamespace(scope); err != nil {
		logger.Error("scope", "error", err)
		os.Exit(2)
	}
	if tokensFile != "" {
		if err := svc.UseTokensFile(tokensFile); err != nil {
			logger.Error("tokens", "error", err)
			os.Exit(1)
		}
		// SIGHUP reloads the file; a file that doesn't parse leaves the
		// tokens as they were.
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		go func() {
			for range hup {
				_ = svc.ReloadTokens()
			}
		}()
	} else {
		logger.Warn("UNAUTHENTICATED: no -tokens-file, so the service takes any token, and every engine that reaches it " +
			"shares namespace " + scope + ": it can read everything stored there and add to it. " +
			"Set -tokens-file to authenticate engines and keep namespaces apart.")
	}
	if os.Getenv(blobstore.EnvBucket) != "" {
		cfg, err := blobstore.S3ConfigFromEnv()
		if err == nil {
			err = svc.EnableS3(ctx, cfg, server.S3Options{
				Scope:      scope,
				Tracker:    exporter.TrackerConfig{QuietPeriod: quiet, EndGrace: endGrace},
				NoImport:   !imports,
				ImportPoll: importPoll,
			})
		}
		if err != nil {
			logger.Error("export path", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Info("export path off: set " + blobstore.EnvBucket + " to store exports in S3")
	}

	servers := []*http.Server{{Addr: listen, Handler: svc.Handler()}}
	if debugListen != "" {
		servers = append(servers, &http.Server{Addr: debugListen, Handler: svc.DebugHandler()})
	}
	if metricsListen != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", svc.MetricsHandler())
		servers = append(servers, &http.Server{Addr: metricsListen, Handler: mux})
	}
	errs := make(chan error, len(servers))
	for i, srv := range servers {
		// Only the engine address is served over TLS: the debug and
		// metrics addresses are for local use.
		tls := i == 0 && tlsCert != ""
		logger.Info("listening", "addr", srv.Addr, "tls", tls)
		go func() {
			var err error
			if tls {
				err = srv.ListenAndServeTLS(tlsCert, tlsKey)
			} else {
				err = srv.ListenAndServe()
			}
			if !errors.Is(err, http.ErrServerClosed) {
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
	svc.CloseEngines()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}
	// End the exports in flight, and wait for them.
	stop()
	svc.Wait()
}
