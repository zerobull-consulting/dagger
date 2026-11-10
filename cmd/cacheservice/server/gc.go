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

// GCMode says who collects garbage in a bucket.
type GCMode string

const (
	// GCModeService runs GC in the service, every GCConfig.Interval, and
	// when asked (POST /debug/gc). The default of -gc-mode.
	GCModeService GCMode = "service"
	// GCModeBatch has no schedule in the service: a job (cacheservice gc)
	// collects the bucket. POST /debug/gc still runs GC, on demand.
	GCModeBatch GCMode = "batch"
	// GCModeOff collects nothing, and refuses POST /debug/gc (409): the
	// bucket grows without bound.
	GCModeOff GCMode = "off"
)

// GCModes lists the modes, as -gc-mode spells them.
var GCModes = []GCMode{GCModeService, GCModeBatch, GCModeOff}

// ParseGCMode reads a -gc-mode value. An unknown one is an error that lists
// the modes.
func ParseGCMode(v string) (GCMode, error) {
	for _, m := range GCModes {
		if string(m) == v {
			return m, nil
		}
	}
	return "", fmt.Errorf("-gc-mode=%q: want one of %s, %s or %s", v, GCModeService, GCModeBatch, GCModeOff)
}

// GCConfig configures garbage collection.
type GCConfig struct {
	// Mode is who collects. It must be set: GCModeService needs a positive
	// Interval, and GCModeBatch and GCModeOff need none.
	Mode GCMode
	// Collector configures the collector. Its Limits and Logger are the
	// service's when unset; Metrics too.
	Collector gc.Config
	// Interval is the time between runs, jittered by ±25%, in GCModeService.
	// GCModeBatch and GCModeOff take none.
	Interval time.Duration
}

// gcPath is garbage collection, when it is on.
type gcPath struct {
	cfg       gc.Config
	mode      GCMode
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

// EnableGC sets up garbage collection for cfg.Mode. In GCModeService it runs
// every cfg.Interval until ctx ends; in GCModeBatch it only runs when asked
// (CollectGarbage, POST /debug/gc); in GCModeOff it collects nothing, and
// CollectGarbage answers ErrGCModeOff. It collects every namespace in the
// bucket, not only those with tokens, with the service's limits.
func (s *Service) EnableGC(ctx context.Context, cfg GCConfig) error {
	if s.readOnly {
		return ErrGCReadOnly
	}
	switch cfg.Mode {
	case GCModeService:
		// A zero or negative interval is a typo: the schedule is the mode's
		// whole point, and "no schedule" is GCModeBatch or GCModeOff.
		if cfg.Interval <= 0 {
			return fmt.Errorf("gc: interval %s in mode %s: want a positive interval; for no schedule use -gc-mode=%s or -gc-mode=%s", cfg.Interval, cfg.Mode, GCModeBatch, GCModeOff)
		}
	case GCModeBatch, GCModeOff:
		if cfg.Interval != 0 {
			return fmt.Errorf("gc: interval %s in mode %s: it has no schedule, so an interval would do nothing", cfg.Interval, cfg.Mode)
		}
	default:
		return fmt.Errorf("gc: unknown mode %q", cfg.Mode)
	}
	if cfg.Mode == GCModeOff {
		s.gc = &gcPath{mode: GCModeOff}
		s.logger.Warn("gc off", "mode", GCModeOff, "reason", "nothing collects this bucket, so it grows without bound; -gc-mode=batch is for a cacheservice gc job, -gc-mode=service for the service's own schedule")
		return nil
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
	p := &gcPath{cfg: c, mode: cfg.Mode, collector: collector, ctx: ctx, stop: stop}
	s.gc = p
	if cfg.Mode == GCModeService {
		p.wg.Add()
		go func() {
			defer p.wg.Done()
			collector.Loop(ctx, cfg.Interval)
		}()
	}
	s.logger.Info("gc on", "mode", cfg.Mode, "interval", cfg.Interval, "dryRun", c.DryRun)
	return nil
}

var (
	// ErrGCOff is returned by CollectGarbage when GC is off.
	ErrGCOff = errors.New("gc is off")
	// ErrGCReadOnly is returned by CollectGarbage, and EnableGC, on a
	// read-only service (SetReadOnly, -export=false): GC deletes from the
	// bucket, which such a service never does.
	ErrGCReadOnly = errors.New("gc is off: the instance is read-only (-export=false), and never deletes from the bucket")
	// ErrGCModeOff is returned by CollectGarbage in GCModeOff.
	ErrGCModeOff = errors.New("gc is off: -gc-mode=off, so nothing collects this bucket; use -gc-mode=batch or -gc-mode=service to run GC")
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
	if p.mode == GCModeOff {
		return nil, ErrGCModeOff
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
	if errors.Is(err, ErrGCReadOnly) || errors.Is(err, ErrGCModeOff) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if errors.Is(err, ErrDraining) || errors.Is(err, ErrStopping) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	out := struct {
		Mode    GCMode      `json:"mode"`
		Reports []gc.Report `json:"reports"`
		Error   string      `json:"error,omitempty"`
	}{Mode: s.gc.mode, Reports: reports}
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
