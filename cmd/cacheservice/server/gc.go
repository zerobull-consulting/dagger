package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/gc"
)

// GCConfig configures garbage collection.
type GCConfig struct {
	// Collector configures the collector. Its Limits and Logger are the
	// service's when unset; Metrics too.
	Collector gc.Config
	// Interval is the time between runs, jittered by ±25%. Zero runs only
	// when asked (CollectGarbage, POST /debug/gc).
	Interval time.Duration
}

// gcPath is garbage collection, when it is on.
type gcPath struct {
	cfg       gc.Config
	collector *gc.Collector
	wg        sync.WaitGroup
}

// limitsState is the service's per-namespace limits and where they come
// from, for ReloadLimits.
type limitsState struct {
	mu       sync.Mutex
	file     *gc.LimitsFile
	path     string
	defaults gc.Limits
}

// UseLimits sets every namespace's retention limits: defaults, and, when
// path isn't empty, the namespaces file's lines over them (see
// gc.LimitsFile); with an empty path, every namespace has the defaults.
// ReloadLimits reads the file again. A service that never calls UseLimits
// has no limits at all.
// Call it before engines connect. The import path caps the root expiry of
// its merges at the namespace's max age even with GC off.
func (s *Service) UseLimits(path string, defaults gc.Limits) error {
	var (
		f   *gc.LimitsFile
		err error
	)
	if path == "" {
		f, err = gc.ParseLimits(nil, defaults)
	} else {
		f, err = gc.LoadLimitsFile(path, defaults)
	}
	if err != nil {
		return err
	}
	s.limits.mu.Lock()
	defer s.limits.mu.Unlock()
	s.limits.file, s.limits.path, s.limits.defaults = f, path, defaults
	s.logger.Info("limits loaded", "file", path, "namespaces", f.Namespaces(),
		"defaultMaxAge", f.Defaults().MaxAge, "defaultMaxBytes", f.Defaults().MaxBytes)
	return nil
}

// ErrNoNamespacesFile is returned by ReloadLimits without a namespaces
// file.
var ErrNoNamespacesFile = errors.New("the service has no namespaces file")

// ReloadLimits reads the namespaces file again, as on SIGHUP. A file that
// can't be read or parsed changes nothing: the limits loaded before stay.
func (s *Service) ReloadLimits() error {
	s.limits.mu.Lock()
	defer s.limits.mu.Unlock()
	if s.limits.path == "" {
		return ErrNoNamespacesFile
	}
	f, err := gc.LoadLimitsFile(s.limits.path, s.limits.defaults)
	if err != nil {
		s.logger.Error("namespaces file not reloaded; the limits loaded before stay", "file", s.limits.path, "error", err)
		return err
	}
	s.limits.file = f
	s.logger.Info("namespaces file reloaded", "file", s.limits.path, "namespaces", f.Namespaces())
	return nil
}

// Limits returns a namespace's limits now.
func (s *Service) Limits(ns string) gc.Limits {
	s.limits.mu.Lock()
	defer s.limits.mu.Unlock()
	return s.limits.file.For(ns)
}

// EnableGC turns on garbage collection, and runs it every cfg.Interval
// until ctx ends. It collects every namespace in the bucket, not only those
// with tokens, with the service's limits.
func (s *Service) EnableGC(ctx context.Context, cfg GCConfig) error {
	// Only 0 turns the schedule off: a negative interval is a typo, and
	// taking it as 0 would leave retention off without a word.
	if cfg.Interval < 0 {
		return fmt.Errorf("gc: negative interval %s (0 runs GC only when asked)", cfg.Interval)
	}
	c := cfg.Collector
	if c.Limits == nil {
		c.Limits = s.Limits
	}
	if c.Logger == nil {
		c.Logger = s.logger.With("component", "gc")
	}
	if c.Metrics == nil {
		c.Metrics = s.metrics
	}
	if c.Meta != nil && c.Meta.Metrics() == nil {
		c.Meta = c.Meta.WithMetrics(s.metrics)
	}
	collector, err := gc.New(c)
	if err != nil {
		return err
	}
	p := &gcPath{cfg: c, collector: collector}
	s.gc = p
	if cfg.Interval > 0 {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			collector.Loop(ctx, cfg.Interval)
		}()
	}
	s.logger.Info("gc on", "interval", cfg.Interval, "dryRun", c.DryRun)
	return nil
}

// ErrGCOff is returned by CollectGarbage when GC is off.
var ErrGCOff = errors.New("gc is off")

// CollectGarbage runs garbage collection now, over every namespace, and
// returns what it did. dryRun makes this run delete nothing, whatever the
// configuration.
func (s *Service) CollectGarbage(ctx context.Context, dryRun bool) ([]gc.Report, error) {
	p := s.gc
	if p == nil {
		return nil, ErrGCOff
	}
	c := p.collector
	if dryRun && !p.cfg.DryRun {
		cfg := p.cfg
		cfg.DryRun = true
		var err error
		if c, err = gc.New(cfg); err != nil {
			return nil, err
		}
	}
	return c.Run(ctx)
}

// serveGC is POST /debug/gc[?dry-run=1]: run GC now. A dry-run value that
// isn't a boolean is refused (400), rather than taken as false and run for
// real. A run with errors answers 500, with its reports.
func (s *Service) serveGC(w http.ResponseWriter, r *http.Request) {
	dry := false
	if v := r.URL.Query().Get("dry-run"); v != "" {
		var err error
		if dry, err = strconv.ParseBool(v); err != nil {
			http.Error(w, "dry-run: want true or false (1 or 0)", http.StatusBadRequest)
			return
		}
	}
	reports, err := s.CollectGarbage(r.Context(), dry)
	if errors.Is(err, ErrGCOff) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	out := struct {
		Reports []gc.Report `json:"reports"`
		Error   string      `json:"error,omitempty"`
	}{Reports: reports}
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		out.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Service) waitGC() {
	if s.gc != nil {
		s.gc.wg.Wait()
	}
}
