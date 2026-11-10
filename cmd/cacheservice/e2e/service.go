package e2e

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// Service is a cache service run in this process, on the Docker bridge
// gateway, with MinIO behind the run's S3 proxy.
type Service struct {
	*server.Service
	Name string
	// URL is where engines reach it, for _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL
	// and DAGGER_CLOUD_URL.
	URL string
	// Logs records everything it logs, at every level. It also goes to
	// RunDir/<name>.log.
	Logs *LogRecorder

	opts ServiceOptions
	stop func()
}

// ServiceOptions configure a service.
type ServiceOptions struct {
	// GetExpiry is how long its presigned GETs last (0: the store's
	// default).
	GetExpiry time.Duration
	// ImportPoll is how often it looks for entries a connected engine
	// lacks (0: server.DefaultImportPoll).
	ImportPoll time.Duration
	// Addr is the address to listen on (host:port); empty picks a free
	// port on the bridge gateway.
	Addr string
	// TokensFile, when set, authenticates engines with it (server's
	// UseTokensFile); ReloadTokens reads it again, as SIGHUP does for the
	// command. Without it, the service takes any token, in the run's
	// scope.
	TokensFile string
	// NamespacesFile, when set, gives namespaces their limits (server's
	// UseLimits); ReloadLimits reads it again.
	NamespacesFile string
	// GCGrace is GC's grace period (0: its default). GC runs only when a
	// scenario calls CollectGarbage.
	GCGrace time.Duration
	// ReadOnly runs it as -export=false does: the import path alone, no GC,
	// and nothing written to or deleted from the bucket.
	ReadOnly bool
	// Proxy is the S3 proxy it, and the engines it hands presigned URLs to,
	// reach the store through (nil: the run's, Env.Proxy).
	Proxy *S3Proxy
	// AccessKey and SecretKey are its store credentials (empty: the
	// run's).
	AccessKey, SecretKey string
}

// noEngineWait is how long an export waits for its engine to reconnect:
// short, so the scenarios that drop connections don't wait the default 10s.
const noEngineWait = 2 * time.Second

// StartService starts a service on the run's bucket prefix and scope. Two
// services on one run are replicas of each other.
func (e *Env) StartService(name string, opts ServiceOptions) *Service {
	t := e.T
	t.Helper()
	logFile, err := os.Create(filepath.Join(e.RunDir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	logs := &LogRecorder{}
	logger := slog.New(logs.Handler(slog.NewTextHandler(logFile, &slog.HandlerOptions{Level: slog.LevelDebug})))

	cfg := e.S3Config
	cfg.Endpoint = e.Proxy.URL
	if opts.Proxy != nil {
		cfg.Endpoint = opts.Proxy.URL
	}
	if opts.AccessKey != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey, "")
	}
	// The proxy forwards in the store's addressing; a proxy's address has no
	// bucket subdomains.
	cfg.PathStyle = true
	// The proxy has its own guard; the direct client's is bound to the store.
	cfg.HTTPClient = nil
	if opts.GetExpiry > 0 {
		cfg.GetExpiry = opts.GetExpiry
	}
	poll := opts.ImportPoll
	if poll == 0 {
		poll = server.DefaultImportPoll
	}

	ctx, cancel := context.WithCancel(context.Background())
	svc := server.New(logger, "")
	// An engine built from this checkout reports a development version,
	// which no accept line names.
	if e.Settings.BuildEngine {
		svc.AllowEngineVersionMismatch()
	}
	s3opts := server.S3Options{
		Scope:        e.Scope,
		Tracker:      exporter.TrackerConfig{},
		NoExport:     opts.ReadOnly,
		ImportPoll:   poll,
		NoEngineWait: noEngineWait,
	}
	if !opts.ReadOnly {
		// Only when a scenario asks (CollectGarbage).
		s3opts.GC = &server.GCConfig{Mode: server.GCModeBatch, Collector: gc.Config{Grace: opts.GCGrace}}
	}
	if err := svc.EnableS3(ctx, cfg, s3opts); err != nil {
		cancel()
		t.Fatalf("service %s: %v", name, err)
	}
	if opts.NamespacesFile != "" {
		if err := svc.UseLimits(opts.NamespacesFile, gc.Limits{}); err != nil {
			cancel()
			t.Fatalf("service %s: %v", name, err)
		}
	}
	if opts.TokensFile != "" {
		if err := svc.UseTokensFile(opts.TokensFile); err != nil {
			cancel()
			t.Fatalf("service %s: %v", name, err)
		}
	}
	// On the bridge gateway only: without tokens it takes any token and
	// any OTLP request, so it isn't exposed beyond this host.
	addr := opts.Addr
	if addr == "" {
		addr = net.JoinHostPort(e.Gateway, "0")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("service %s: %v", name, err)
	}
	// As the command serves it: with its header timeout.
	srv := server.NewHTTPServer("", svc.Handler())
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "error", err)
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			// Stop taking requests first, so no engine connects to the
			// service as it goes away.
			shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			_ = srv.Shutdown(shutdown)
			svc.CloseEngines()
			cancel()
			svc.Wait()
			_ = logFile.Close()
		})
	}
	e.root.Cleanup(stop)
	s := &Service{Service: svc, Name: name, URL: "http://" + ln.Addr().String(), Logs: logs, opts: opts, stop: stop}
	e.mu.Lock()
	e.services = append(e.services, s)
	e.mu.Unlock()
	t.Logf("service %s on %s (GET expiry %s, import poll %s)", name, s.URL, cfg.GetExpiry, poll)
	return s
}

// Stop stops the service at once, as its process ending without a drain
// would (a crash, SIGKILL, or -drain-timeout 0): it closes its listener and
// its engines' connections, and ends what it runs. Its in-memory state goes
// with it. For a clean stop, DrainFor first.
func (s *Service) Stop() { s.stop() }

// DrainFor drains the service with a deadline of timeout from now, while it
// still serves, as the command does on SIGTERM before it stops (see
// server.Service.Drain). Stop it after.
func (s *Service) DrainFor(timeout time.Duration) server.DrainReport {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Drain(ctx)
}

// Services are every service the run started, stopped ones included, in
// the order they started.
func (e *Env) Services() []*Service {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.services)
}

// RestartService stops svc, waits down, and starts a new service, name, with
// the same options on the same address, as a restart of the service's
// process. Engines pointed at svc reconnect to it with their backoff; Attach
// them to it.
func (e *Env) RestartService(svc *Service, name string, down time.Duration) *Service {
	e.T.Helper()
	svc.Stop()
	time.Sleep(down)
	opts := svc.opts
	opts.Addr = strings.TrimPrefix(svc.URL, "http://")
	return e.StartService(name, opts)
}

// EngineInfo is the connection's entry in Engines.
func (s *Service) EngineInfo(conn uint64) (server.EngineInfo, bool) {
	for _, info := range s.Engines() {
		if info.Conn == conn {
			return info, true
		}
	}
	return server.EngineInfo{}, false
}

// maxConn is the highest connection number so far (0: none).
func (s *Service) maxConn() uint64 {
	var n uint64
	for _, info := range s.Engines() {
		n = max(n, info.Conn)
	}
	return n
}
