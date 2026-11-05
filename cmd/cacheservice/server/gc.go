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
	// ctx runs every GC run, scheduled or asked for; stop ends it, and
	// with it the run under way, when the service starts draining.
	ctx  context.Context
	stop context.CancelFunc
	// wg counts the scheduled loop and every run asked for (Wait waits
	// for them). closed is set, under mu, once the service waits: no run
	// starts after it, so no wg.Add races wg.Wait.
	mu     sync.Mutex
	closed bool
	wg     workGroup
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
	if s.readOnly {
		return ErrGCReadOnly
	}
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
	ctx, stop := context.WithCancel(ctx)
	p := &gcPath{cfg: c, collector: collector, ctx: ctx, stop: stop}
	s.gc = p
	if cfg.Interval > 0 {
		p.wg.Add()
		go func() {
			defer p.wg.Done()
			collector.Loop(ctx, cfg.Interval)
		}()
	}
	s.logger.Info("gc on", "interval", cfg.Interval, "dryRun", c.DryRun)
	return nil
}

var (
	// ErrGCOff is returned by CollectGarbage when GC is off.
	ErrGCOff = errors.New("gc is off")
	// ErrGCReadOnly is returned by CollectGarbage, and EnableGC, on a
	// read-only service (SetReadOnly, -export=false): GC deletes from the
	// bucket, which such a service never does.
	ErrGCReadOnly = errors.New("gc is off: the instance is read-only (-export=false), and never deletes from the bucket")
)

// CollectGarbage runs garbage collection now, over every namespace, and
// returns what it did. Wait waits for the run; a drain or the stop cancels
// it. dryRun makes this run delete nothing, whatever the
// configuration.
func (s *Service) CollectGarbage(ctx context.Context, dryRun bool) ([]gc.Report, error) {
	if s.readOnly {
		return nil, ErrGCReadOnly
	}
	p := s.gc
	if p == nil {
		return nil, ErrGCOff
	}
	if s.draining.Load() {
		return nil, ErrDraining
	}
	// Counted, so the stop's Wait waits for the run to end, rather than
	// leave it unwinding mid-delete; one asked for once the stop began is
	// refused.
	p.mu.Lock()
	if p.closed || p.ctx.Err() != nil {
		p.mu.Unlock()
		if s.draining.Load() {
			return nil, ErrDraining
		}
		return nil, ErrStopping
	}
	p.wg.Add()
	p.mu.Unlock()
	defer p.wg.Done()
	// A drain cancels a run asked for too, not only the scheduled ones.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(p.ctx, cancel)()
	c := p.collector
	if dryRun {
		// Shares the collector's run lock: it waits for a run under way.
		c = c.DryRunner()
	}
	return c.Run(ctx)
}

// serveGC is POST /debug/gc[?dry-run[=1]]: run GC now. A dry-run
// parameter with no value is a dry run; one whose value isn't a boolean is
// refused (400). Neither is taken as false and run for real. A run with
// errors answers 500, with its reports.
func (s *Service) serveGC(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dry := false
	if q.Has("dry-run") {
		dry = true
		if v := q.Get("dry-run"); v != "" {
			var err error
			if dry, err = strconv.ParseBool(v); err != nil {
				http.Error(w, "dry-run: want true or false (1 or 0), or no value", http.StatusBadRequest)
				return
			}
		}
	}
	reports, err := s.CollectGarbage(r.Context(), dry)
	if errors.Is(err, ErrGCOff) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if errors.Is(err, ErrGCReadOnly) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if errors.Is(err, ErrDraining) || errors.Is(err, ErrStopping) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
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

// waitGC waits for the scheduled loop and the runs asked for, after the
// context that runs them has ended.
func (s *Service) waitGC() {
	if p := s.gc; p != nil {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.wg.Wait()
	}
}
