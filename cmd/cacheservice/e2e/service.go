package e2e

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
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
}

// ServiceOptions configure a service.
type ServiceOptions struct {
	// GetExpiry is how long its presigned GETs last (0: the store's
	// default).
	GetExpiry time.Duration
	// ImportPoll is how often it looks for entries a connected engine
	// lacks (0: server.DefaultImportPoll).
	ImportPoll time.Duration
}

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
	if opts.GetExpiry > 0 {
		cfg.GetExpiry = opts.GetExpiry
	}
	poll := opts.ImportPoll
	if poll == 0 {
		poll = server.DefaultImportPoll
	}

	ctx, cancel := context.WithCancel(context.Background())
	svc := server.New(logger, "")
	if err := svc.EnableS3(ctx, cfg, server.S3Options{
		Scope:      e.Scope,
		Tracker:    exporter.TrackerConfig{QuietPeriod: exporter.DefaultQuietPeriod, EndGrace: exporter.DefaultEndGrace},
		ImportPoll: poll,
	}); err != nil {
		cancel()
		t.Fatalf("service %s: %v", name, err)
	}
	// On the bridge gateway only: it takes any token and any OTLP request,
	// so it isn't exposed beyond this host.
	ln, err := net.Listen("tcp", net.JoinHostPort(e.Gateway, "0"))
	if err != nil {
		cancel()
		t.Fatalf("service %s: %v", name, err)
	}
	srv := &http.Server{Handler: svc.Handler()}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "error", err)
		}
	}()
	e.root.Cleanup(func() {
		svc.CloseEngines()
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = srv.Shutdown(shutdown)
		cancel()
		svc.Wait()
		_ = logFile.Close()
	})
	s := &Service{Service: svc, Name: name, URL: "http://" + ln.Addr().String(), Logs: logs}
	t.Logf("service %s on %s (GET expiry %s, import poll %s)", name, s.URL, cfg.GetExpiry, poll)
	return s
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
