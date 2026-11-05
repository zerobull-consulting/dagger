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
//     scraper that shouldn't reach the debug API;
//
//   - on SIGTERM or an interrupt, a drain before it stops: it takes no new
//     engine or session, exports what its tracked sessions retained, without
//     waiting out their quiet period, and waits for those exports and their
//     uploads, up to -drain-timeout (30s; 0: no drain). The engines stay
//     connected meanwhile, since exports run over them: stop the service
//     before its engines. The drain starts no export past its deadline. A
//     shutdown follows, bounded at 5s: it closes the engine connections
//     (within 2s), shuts the HTTP servers down and waits for what is still
//     running. So the whole stop takes at most -drain-timeout plus 5s: give
//     the process manager a stop timeout longer than that. A second signal
//     ends the drain at once; a signal during the shutdown exits at once.
//     The process exits 0 only when everything ended within the bound, and
//     1 when a signal or the bound cut the shutdown off, logging what was
//     still running. GET /debug/drain shows the drain. See README.md,
//     "Stopping and draining".
//
//   - with -export=false, a read-only instance: the import path alone. It
//     never writes to or deletes from the bucket, so it runs with a
//     read-only key: no export path (spans are received and dropped), no
//     conditional write check at startup, no GC, and no import records in
//     the index. It refuses -import=false, no CACHESERVICE_S3_BUCKET, and
//     an explicit nonzero -gc-interval. See README.md, "Read-only
//     instances".
//
//   - with CACHESERVICE_S3_BUCKET set, garbage collection, every
//     -gc-interval: per namespace in the bucket, it expires entries past
//     their expiry, -max-age or -max-bytes (per namespace with
//     -namespaces-file, which SIGHUP reloads too), and deletes what no live
//     entry names, and staged and incomplete multipart uploads older than
//     -gc-staging-age (an export's upload timeout plus an hour). POST
//     /debug/gc runs it now (?dry-run: report only). See
//     README.md, "Retention and garbage collection".
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
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
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
		exports       bool
		importPoll    time.Duration
		tokensFile    string
		tlsCert       string
		tlsKey        string
		nsFile        string
		maxAge        string
		maxBytes      string
		gcInterval    time.Duration
		gcDryRun      bool
		gcGrace       time.Duration
		gcStagingAge  time.Duration
		gcMaxDeletes  int
		drainTimeout  time.Duration
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
	flag.BoolVar(&exports, "export", true, "export engines' entries to the bucket, and run garbage collection (needs "+blobstore.EnvBucket+"); false: read-only, it only imports, and never writes to or deletes from the bucket, so it runs with a read-only key; can't be combined with -import=false, and needs "+blobstore.EnvBucket)
	flag.BoolVar(&imports, "import", true, "merge stored entries into connected engines (needs "+blobstore.EnvBucket+")")
	flag.DurationVar(&importPoll, "import-poll", server.DefaultImportPoll, "how often to look for stored entries a connected engine lacks (0: only on hello and after an export)")
	flag.StringVar(&nsFile, "namespaces-file", "", "file of per-namespace limits, one '<namespace> max-age=<duration> max-bytes=<size>' per line, '*' for the defaults; SIGHUP reloads it (empty: -max-age and -max-bytes for every namespace)")
	flag.StringVar(&maxAge, "max-age", "0", "default max age of a namespace's roots, from when an export last recorded them, such as 720h (0: no limit)")
	flag.StringVar(&maxBytes, "max-bytes", "0", "default max size of what a namespace keeps, such as 500GiB (0: no limit)")
	flag.DurationVar(&gcInterval, "gc-interval", gc.DefaultInterval, "time between garbage collection runs, jittered by 25% (0: only POST /debug/gc; needs "+blobstore.EnvBucket+"); GC is off with -export=false, which refuses an explicit nonzero value")
	flag.BoolVar(&gcDryRun, "gc-dry-run", false, "garbage collection logs what it would expire and delete, and changes nothing")
	flag.DurationVar(&gcGrace, "gc-grace", gc.DefaultGrace, "how old an unreferenced bundle, resolution or blob must be before garbage collection deletes it")
	flag.DurationVar(&gcStagingAge, "gc-staging-age", gc.DefaultStagingAge, "how old a staged upload or incomplete multipart upload must be before garbage collection deletes it")
	flag.DurationVar(&drainTimeout, "drain-timeout", DefaultDrainTimeout, "on SIGTERM or an interrupt, how long to finish pending exports before stopping (0: stop at once); a shutdown of at most 5s follows, so the process manager's stop timeout must be longer than this plus 5s")
	flag.IntVar(&gcMaxDeletes, "gc-max-deletes", gc.DefaultMaxDeletes, "most bundles, resolutions and blobs one garbage collection run deletes per namespace")
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

	if drainTimeout < 0 {
		logger.Error("-drain-timeout must not be negative (0: no drain)")
		os.Exit(2)
	}

	if err := checkReadOnly(exports, imports, os.Getenv(blobstore.EnvBucket) != "", gcInterval, explicitFlags()); err != nil {
		logger.Error(err.Error())
		os.Exit(2)
	}

	if (tlsCert == "") != (tlsKey == "") {
		logger.Error("set both -tls-cert and -tls-key, or neither")
		os.Exit(2)
	}

	// The first SIGTERM or interrupt drains, the second ends the drain, and
	// one during the shutdown that follows exits at once (stopService).
	// ctx runs the export, import and GC paths: it ends once the drain is
	// done (shutdown), so the exports the drain waits for aren't cut off,
	// or at once with -drain-timeout 0.
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	svc := server.New(logger, dumpDir)
	// The namespace of every engine without tokens, with a store or not.
	if err := svc.SetDefaultNamespace(scope); err != nil {
		logger.Error("scope", "error", err)
		os.Exit(2)
	}
	defaults, err := parseLimits(maxAge, maxBytes)
	if err != nil {
		logger.Error("limits", "error", err)
		os.Exit(2)
	}
	if err := svc.UseLimits(nsFile, defaults); err != nil {
		logger.Error("namespaces file", "error", err)
		os.Exit(1)
	}
	if tokensFile != "" || nsFile != "" {
		// SIGHUP reloads the files; one that doesn't parse leaves what it
		// set as it was.
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		go func() {
			for range hup {
				if tokensFile != "" {
					_ = svc.ReloadTokens()
				}
				if nsFile != "" {
					_ = svc.ReloadLimits()
				}
			}
		}()
	}
	if tokensFile != "" {
		if err := svc.UseTokensFile(tokensFile); err != nil {
			logger.Error("tokens", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Warn("UNAUTHENTICATED: no -tokens-file, so the service takes any token, and every engine that reaches it " +
			"shares namespace " + scope + ": it can read everything stored there and add to it. " +
			"Set -tokens-file to authenticate engines and keep namespaces apart.")
	}
	store := os.Getenv(blobstore.EnvBucket) != ""
	if store {
		cfg, err := blobstore.S3ConfigFromEnv()
		if err == nil {
			opts := server.S3Options{
				Scope:      scope,
				Tracker:    exporter.TrackerConfig{QuietPeriod: quiet, EndGrace: endGrace},
				NoExport:   !exports,
				NoImport:   !imports,
				ImportPoll: importPoll,
			}
			// A read-only instance never deletes from the bucket: GC is
			// off whatever -gc-interval says (its default; an explicit
			// one was refused above).
			if exports {
				opts.GC = &server.GCConfig{
					Interval: gcInterval,
					Collector: gc.Config{
						DryRun:     gcDryRun,
						Grace:      gcGrace,
						StagingAge: gcStagingAge,
						MaxDeletes: gcMaxDeletes,
					},
				}
			}
			err = svc.EnableS3(ctx, cfg, opts)
		}
		if err != nil {
			path := "export path"
			if !exports {
				path = "import path (read-only)"
			}
			logger.Error(path, "error", err)
			os.Exit(1)
		}
	} else {
		logger.Info("export path off: set " + blobstore.EnvBucket + " to store exports in S3")
	}
	logger.Info("mode", "export", onOff(store && exports), "import", onOff(store && imports), "gc", onOff(store && exports), "readOnly", !exports)

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

	code := 0
	select {
	case sig := <-sigs:
		logger.Info("stopping", "signal", sig.String(), "drainTimeout", drainTimeout, "shutdownGrace", shutdownGrace)
		if drainTimeout > 0 {
			drain(svc, logger, sigs, drainTimeout)
		}
	case err := <-errs:
		logger.Error("serve", "error", err)
		code = 1
	}
	if !stopService(svc, servers, stop, sigs, logger, shutdownGrace) {
		code = 1
	}
	os.Exit(code)
}

// shutdownGrace bounds the shutdown that follows the drain, or the first
// signal with -drain-timeout 0: closing the engine connections, shutting
// the HTTP servers down and waiting for the service's paths. The stop as a
// whole takes at most -drain-timeout plus this, which the process manager's
// stop timeout must be longer than.
const shutdownGrace = 5 * time.Second

// stopTestHoldEnv, set to 1 in a binary built with the cacheservice_testhooks
// tag (main_testhooks.go), makes the stop wait for work that ignores
// cancellation and never ends, so the tests can check the stop's bound on the
// built binary. A binary built without the tag ignores it.
const stopTestHoldEnv = "_CACHESERVICE_TEST_HOLD_STOP"

// testHookStop, set only in a binary built with the cacheservice_testhooks
// tag, runs as the shutdown begins.
var testHookStop func(*server.Service, *slog.Logger)

// stopService runs shutdown, bounded by grace, and reports whether the
// service stopped cleanly: everything ended within grace, and no signal cut
// the shutdown off. A signal during the shutdown ends it at once: it logs
// what was still running, and returns false. So does the bound, should
// shutdown itself overrun it. A false stop exits 1, so a process manager,
// and the tests, can tell it from a clean one (0).
func stopService(svc *server.Service, servers []*http.Server, cancelPaths context.CancelFunc, sigs <-chan os.Signal, logger *slog.Logger, grace time.Duration) bool {
	if testHookStop != nil {
		testHookStop(svc, logger)
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- shutdown(svc, servers, cancelPaths, grace-shutdownMargin) }()
	// shutdown's steps are bounded by grace less the margin; this backstop,
	// at grace, is for a step that overruns its own bound.
	bound := time.NewTimer(grace)
	defer bound.Stop()
	select {
	case err := <-done:
		if err != nil {
			logger.Error("stopped: the shutdown's bound passed with work still running; exiting without waiting for it",
				append([]any{"bound", grace, "error", err}, svc.Running().LogAttrs()...)...)
			return false
		}
		logger.Info("stopped", "took", time.Since(start).Round(time.Millisecond))
		return true
	case sig := <-sigs:
		logger.Error("signal during the shutdown: exiting at once",
			append([]any{"signal", sig.String()}, svc.Running().LogAttrs()...)...)
		return false
	case <-bound.C:
		logger.Error("stopped: the shutdown overran its bound; exiting without waiting for it",
			append([]any{"bound", grace}, svc.Running().LogAttrs()...)...)
		return false
	}
}

// shutdownMargin is how much of stopService's grace shutdown leaves unused,
// so that the backstop at grace, for a step that overruns its bound, is
// still within the documented bound.
const shutdownMargin = 500 * time.Millisecond

// shutdown stops svc after its drain, or without one, within grace: it ends
// what is still in flight (exports, import syncs, GC runs) by canceling the
// context the service's paths run on, closes the engine connections (within
// server.CloseEnginesTimeout), shuts the servers down, and waits for their
// requests and the service's paths, until grace has passed since it began.
// It returns an error when that bound cut the wait off, with work still
// running: the service's paths, or an HTTP request. The cancel comes first, as when the signal itself canceled that
// context: a request tied to it, such as POST /debug/gc, then ends at once
// rather than holding the shutdown for grace.
func shutdown(svc *server.Service, servers []*http.Server, cancelPaths context.CancelFunc, grace time.Duration) error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), grace, errors.New("shutdown grace passed"))
	defer cancel()
	cancelPaths()
	closeCtx, cancelClose := context.WithTimeout(ctx, server.CloseEnginesTimeout)
	svc.CloseEnginesContext(closeCtx)
	cancelClose()
	var errs []error
	for _, srv := range servers {
		// A request still running when ctx ends: the stop isn't clean.
		if err := srv.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("http server %s: requests still running: %w", srv.Addr, err))
		}
	}
	if err := svc.WaitContext(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// DefaultDrainTimeout is -drain-timeout's default.
const DefaultDrainTimeout = 30 * time.Second

// drain runs svc's drain for at most timeout, while the servers still serve:
// the engines stay connected, and the spans of the sessions it tracks still
// arrive. A second signal cancels it: it returns once the drain did, which
// is at once. Only this goroutine reads sigs meanwhile, so a further signal
// stays queued for the shutdown that follows, which exits on it.
func drain(svc *server.Service, logger *slog.Logger, sigs <-chan os.Signal, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Drain(ctx)
	}()
	select {
	case <-done:
		return
	case sig := <-sigs:
		logger.Warn("second signal: stopping without finishing the drain", "signal", sig.String())
		cancel()
	}
	<-done
}

// explicitFlags returns the flags set on the command line, by name.
func explicitFlags() map[string]bool {
	set := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
	return set
}

// checkReadOnly refuses the flag combinations -export=false can't honor:
// with -import=false too, or with no bucket (store), the service would have
// nothing to do; and an explicit nonzero -gc-interval asks for GC, which a
// read-only instance never runs (it never deletes from the bucket). The
// default -gc-interval alone is fine: GC is then off. set holds the flags
// set on the command line.
func checkReadOnly(exports, imports, store bool, gcInterval time.Duration, set map[string]bool) error {
	if exports {
		return nil
	}
	if !imports {
		return errors.New("-export=false and -import=false: with both paths off the service has nothing to do; drop one of them")
	}
	if !store {
		return errors.New("-export=false without " + blobstore.EnvBucket + ": a read-only instance only imports from the bucket, so it would have nothing to do; set " + blobstore.EnvBucket + ", or drop -export=false")
	}
	if set["gc-interval"] && gcInterval != 0 {
		return fmt.Errorf("-gc-interval=%s and -export=false: a read-only instance never runs garbage collection, which deletes from the bucket; drop -gc-interval, or set it to 0", gcInterval)
	}
	return nil
}

func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

// parseLimits reads -max-age and -max-bytes.
func parseLimits(maxAge, maxBytes string) (gc.Limits, error) {
	age, err := gc.ParseAge(maxAge)
	if err != nil {
		return gc.Limits{}, fmt.Errorf("-max-age: %w", err)
	}
	size, err := gc.ParseSize(maxBytes)
	if err != nil {
		return gc.Limits{}, fmt.Errorf("-max-bytes: %w", err)
	}
	return gc.Limits{MaxAge: age, MaxBytes: size}, nil
}
