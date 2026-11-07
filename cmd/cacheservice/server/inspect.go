package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
)

// What the debug API serves, as Go calls: tests that run the service
// in-process read and drive it through these instead of over HTTP.

var (
	// ErrNoEngine is returned for a connection number no engine has.
	ErrNoEngine = errors.New("no such engine")
	// ErrExportOff and ErrImportOff are returned when the path is off: no
	// store is configured.
	ErrExportOff = errors.New("the export path is off: no store is configured")
	ErrImportOff = errors.New("the import path is off: no store is configured")
	// ErrReadOnly is returned for an export asked of a read-only service
	// (SetReadOnly, -export=false): it has a store, and exports nothing to
	// it. It is distinct from ErrExportOff, so an operator can tell a
	// read-only instance from one with no store.
	ErrReadOnly = errors.New("the export path is off: the instance is read-only (-export=false)")
)

// exportOff is the error of an export asked of a service whose export path
// is off: ErrReadOnly or ErrExportOff.
func (s *Service) exportOff() error {
	if s.readOnly {
		return ErrReadOnly
	}
	return ErrExportOff
}

// ExportConflictError is returned by Export when the requested roots can't
// be claimed, such as when an export of them is already in flight.
type ExportConflictError struct{ Err error }

func (e *ExportConflictError) Error() string { return e.Err.Error() }
func (e *ExportConflictError) Unwrap() error { return e.Err }

// Engines lists the welcomed engine connections, in connection order.
func (s *Service) Engines() []EngineInfo {
	s.mu.Lock()
	infos := make([]EngineInfo, 0, len(s.conns))
	for _, c := range s.conns {
		infos = append(infos, EngineInfo{
			Conn:            c.id,
			Remote:          c.remote,
			Token:           c.token,
			Namespace:       c.ns.name,
			ConnectedAt:     c.connectedAt,
			CacheID:         c.hello.CacheID,
			Generation:      c.hello.Generation,
			EngineVersion:   c.hello.EngineVersion,
			VersionAccepted: c.versionAccepted,
			EngineName:      c.hello.EngineName,
		})
	}
	s.mu.Unlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Conn < infos[j].Conn })
	return infos
}

func (s *Service) conn(id uint64) (*engineConn, error) {
	s.mu.Lock()
	c := s.conns[id]
	s.mu.Unlock()
	if c == nil {
		return nil, fmt.Errorf("%w: connection %d", ErrNoEngine, id)
	}
	return c, nil
}

// Entries is what the export path tracks of a connection's engine cache.
func (s *Service) Entries(conn uint64) ([]exporter.EntryState, error) {
	p := s.export
	if p == nil {
		return nil, s.exportOff()
	}
	c, err := s.conn(conn)
	if err != nil {
		return nil, err
	}
	entries := c.ns.tracker.EntriesAt(c.CacheID(), p.now())
	if entries == nil {
		entries = []exporter.EntryState{}
	}
	return entries, nil
}

// Export exports a connection's entries now: req's roots and outputs, or,
// with no roots, every tracked entry worth exporting. It returns what was
// stored.
//
// It takes the engine cache's slot, as the scheduler's exports do: it waits
// for the export of that cache in flight to end, and the scheduler starts
// none for it while Export waits or runs (zerobull-consulting/dagger#135).
// The wait ends with ctx, whose error it then returns.
func (s *Service) Export(ctx context.Context, conn uint64, req exporter.Request) (exporter.Result, error) {
	p := s.export
	if p == nil {
		return exporter.Result{}, s.exportOff()
	}
	// New work: the drain runs what is pending itself.
	if s.draining.Load() {
		return exporter.Result{}, ErrDraining
	}
	c, err := s.conn(conn)
	if err != nil {
		return exporter.Result{}, err
	}
	release, err := c.ns.tracker.AcquireSlot(ctx, c.CacheID())
	if err != nil {
		return exporter.Result{}, fmt.Errorf("waiting for the export of cache %s in flight: %w", c.CacheID(), err)
	}
	defer release()
	if s.draining.Load() {
		return exporter.Result{}, ErrDraining
	}
	if len(req.Roots) > 0 {
		b, err := c.ns.tracker.Claim(c.CacheID(), req)
		if err != nil {
			return exporter.Result{}, &ExportConflictError{err}
		}
		return s.exportBatch(ctx, c, b)
	}
	var all exporter.Result
	batches := c.ns.tracker.All(c.CacheID(), p.now())
	for i, b := range batches {
		res, err := s.exportBatch(ctx, c, b)
		if err != nil {
			// Hand the batches not run back to the scheduler.
			for _, rest := range batches[i+1:] {
				c.ns.tracker.Requeue(rest, p.now())
			}
			return all, err
		}
		all.Merge(res)
	}
	return all, nil
}

// Imports is what the import path merged into a connection's engine cache.
func (s *Service) Imports(conn uint64) (importer.State, error) {
	if s.imports == nil {
		return importer.State{}, ErrImportOff
	}
	c, err := s.conn(conn)
	if err != nil {
		return importer.State{}, err
	}
	st, ok := c.ns.importer.State(c.CacheID())
	if !ok {
		st = importer.State{CacheID: c.CacheID(), Generation: c.Generation(), Merged: []importer.MergedBundle{}, Values: map[uint64]importer.Value{}}
	}
	return st, nil
}

// Import merges and offers now what a connection's engine lacks, and
// returns what was merged, failed and offered.
func (s *Service) Import(ctx context.Context, conn uint64) (importer.Result, error) {
	if s.imports == nil {
		return importer.Result{}, ErrImportOff
	}
	if s.draining.Load() {
		return importer.Result{}, ErrDraining
	}
	c, err := s.conn(conn)
	if err != nil {
		return importer.Result{}, err
	}
	return s.syncMeasured(ctx, c)
}

// debugConnID reads the {conn} path value, and answers 400 when it isn't a
// number.
func debugConnID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	id, err := strconv.ParseUint(r.PathValue("conn"), 10, 64)
	if err != nil {
		http.Error(w, "bad conn", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

// writeDebugError answers with the status that fits an accessor's error.
func writeDebugError(w http.ResponseWriter, err error) {
	var conflict *ExportConflictError
	switch {
	case errors.Is(err, ErrNoEngine):
		http.Error(w, "no such engine", http.StatusNotFound)
	case errors.Is(err, ErrDraining):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, ErrExportOff), errors.Is(err, ErrReadOnly), errors.Is(err, ErrImportOff), errors.Is(err, ErrGCReadOnly), errors.As(err, &conflict):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
}
