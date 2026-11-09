package metastore

import (
	"context"
	"errors"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3util"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
)

func newMetricsStore(t *testing.T, objects Objects, maxAttempts int) (*Store, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	s, err := New(Config{Objects: objects, MaxAttempts: maxAttempts, Metrics: metrics.New(reg)})
	if err != nil {
		t.Fatal(err)
	}
	s.sleep = func(context.Context, time.Duration) error { return nil }
	return s, reg
}

// Reads and updates of the index record its size and contents, labeled by
// the caller in their context.
func TestMetricsIndexShape(t *testing.T) {
	s, reg := newMetricsStore(t, NewMemObjects(), 0)
	ctx := metrics.WithCaller(context.Background(), metrics.CallerExport)

	// An empty scope reads as an empty index.
	mustIndex(t, s, "s1")
	other := map[string]string{"scope": "s1", "caller": metrics.CallerOther}
	if got := metricstest.Value(t, reg, "cacheservice_index_reads_total", other); got != 1 {
		t.Fatalf("reads %v, want 1", got)
	}

	if _, err := s.Record(ctx, "s1", chain(10), "cache-a", 1); err != nil {
		t.Fatal(err)
	}
	scope := map[string]string{"scope": "s1"}
	for name, want := range map[string]float64{
		"cacheservice_index_entries": 3,
		"cacheservice_index_roots":   1,
		"cacheservice_index_caches":  1,
	} {
		if got := metricstest.Value(t, reg, name, scope); got != want {
			t.Errorf("%s %v, want %v", name, got, want)
		}
	}
	size := metricstest.Value(t, reg, "cacheservice_index_size_bytes", scope)
	if size <= 0 {
		t.Fatalf("index size %v", size)
	}
	export := map[string]string{"scope": "s1", "caller": metrics.CallerExport}
	if got := metricstest.Value(t, reg, "cacheservice_index_write_bytes_total", export); got != size {
		t.Fatalf("write bytes %v, want the index's %v", got, size)
	}
	ok := map[string]string{"scope": "s1", "caller": metrics.CallerExport, "outcome": metrics.OutcomeOK}
	if got := metricstest.Value(t, reg, "cacheservice_index_updates_total", ok); got != 1 {
		t.Fatalf("ok updates %v, want 1", got)
	}

	// Reading the index again reads its bytes.
	mustIndex(t, s, "s1")
	if got := metricstest.Value(t, reg, "cacheservice_index_read_bytes_total", other); got != size {
		t.Fatalf("read bytes %v, want %v", got, size)
	}
}

// An update that loses to other writers counts each conflict and attempt.
func TestMetricsIndexConflicts(t *testing.T) {
	f := &interfere{Objects: NewMemObjects(), key: "ns/s1/index.json", other: func(idx *Index) { idx.allocate() }}
	f.n.Store(2)
	s, reg := newMetricsStore(t, f, 0)
	ctx := metrics.WithCaller(context.Background(), metrics.CallerImport)
	if _, err := s.UpdateIndex(ctx, "s1", func(idx *Index) error { idx.allocate(); return nil }); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"scope": "s1", "caller": metrics.CallerImport}
	if got := metricstest.Value(t, reg, "cacheservice_index_conflicts_total", labels); got != 2 {
		t.Fatalf("conflicts %v, want 2", got)
	}
	if s := metricstest.Read(t, reg, "cacheservice_index_update_attempts", labels); s.Count != 1 || s.Sum != 3 {
		t.Fatalf("attempts %+v, want one update of 3 attempts", s)
	}
	if got := metricstest.Value(t, reg, "cacheservice_index_reads_total", labels); got != 3 {
		t.Fatalf("reads %v, want one per attempt", got)
	}
	labels["outcome"] = metrics.OutcomeOK
	if got := metricstest.Value(t, reg, "cacheservice_index_updates_total", labels); got != 1 {
		t.Fatalf("ok updates %v, want 1", got)
	}
}

func TestMetricsIndexContention(t *testing.T) {
	f := &interfere{Objects: NewMemObjects(), key: "ns/s1/index.json", other: func(*Index) {}}
	f.n.Store(1 << 30)
	s, reg := newMetricsStore(t, f, 4)
	_, err := s.UpdateIndex(context.Background(), "s1", func(*Index) error { return nil })
	if !errors.Is(err, ErrContention) {
		t.Fatalf("err %v, want ErrContention", err)
	}
	labels := map[string]string{"scope": "s1", "caller": metrics.CallerOther}
	if got := metricstest.Value(t, reg, "cacheservice_index_conflicts_total", labels); got != 4 {
		t.Fatalf("conflicts %v, want 4", got)
	}
	labels["outcome"] = metrics.OutcomeContention
	if got := metricstest.Value(t, reg, "cacheservice_index_updates_total", labels); got != 1 {
		t.Fatalf("contention updates %v, want 1", got)
	}
}

func TestMetricsIndexAbortedAndError(t *testing.T) {
	s, reg := newMetricsStore(t, NewMemObjects(), 0)
	ctx := context.Background()
	refuse := errors.New("refused")
	if _, err := s.UpdateIndex(ctx, "s1", func(*Index) error { return refuse }); !errors.Is(err, refuse) {
		t.Fatalf("err %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	f := &interfere{Objects: NewMemObjects(), key: "ns/s1/index.json", other: func(*Index) {}}
	f.n.Store(1)
	s2, reg2 := newMetricsStore(t, f, 0)
	s2.sleep = s3util.SleepCtx
	if _, err := s2.UpdateIndex(canceled, "s1", func(*Index) error { return nil }); err == nil {
		t.Fatal("canceled update succeeded")
	}
	labels := map[string]string{"scope": "s1", "outcome": metrics.OutcomeAborted}
	if got := metricstest.Value(t, reg, "cacheservice_index_updates_total", labels); got != 1 {
		t.Fatalf("aborted updates %v, want 1", got)
	}
	labels["outcome"] = metrics.OutcomeError
	if got := metricstest.Value(t, reg2, "cacheservice_index_updates_total", labels); got != 1 {
		t.Fatalf("error updates %v, want 1", got)
	}
}

func TestMetricsBundles(t *testing.T) {
	s, reg := newMetricsStore(t, NewMemObjects(), 0)
	ctx := context.Background()
	bundle := chain(10)
	id, body, err := BundleID(bundle)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := s.PutBundle(ctx, "s1", bundle); err != nil {
			t.Fatal(err)
		}
	}
	// The second store read the stored bundle to compare it.
	if got := metricstest.Value(t, reg, "cacheservice_bundle_reads_total", map[string]string{"scope": "s1"}); got != 1 {
		t.Fatalf("bundle reads %v after storing it again, want 1", got)
	}
	if _, err := s.GetBundle(ctx, "s1", id); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"cacheservice_bundle_writes_total", map[string]string{"scope": "s1", "outcome": metrics.BundleWritten}, 1},
		{"cacheservice_bundle_writes_total", map[string]string{"scope": "s1", "outcome": metrics.BundleExisting}, 1},
		{"cacheservice_bundle_reads_total", map[string]string{"scope": "s1"}, 2},
		{"cacheservice_bundle_read_bytes_total", map[string]string{"scope": "s1"}, float64(2 * len(body))},
	} {
		if got := metricstest.Value(t, reg, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	if s := metricstest.Read(t, reg, "cacheservice_bundle_write_size_bytes", map[string]string{"scope": "s1"}); s.Count != 2 || s.Sum != float64(2*len(body)) {
		t.Fatalf("bundle sizes %+v", s)
	}
}

// WithMetrics leaves the Store it was called on as it was.
func TestWithMetrics(t *testing.T) {
	s := newTestStore(t, NewMemObjects())
	if s.Metrics() != nil {
		t.Fatal("store without metrics has some")
	}
	m := metrics.New(nil)
	if got := s.WithMetrics(m); got.Metrics() != m || s.Metrics() != nil {
		t.Fatal("WithMetrics changed the original, or didn't set them")
	}
}

// Each recorded export stores its resolution: written once, existing when
// the same export is recorded again; reading it back is counted.
func TestMetricsResolutions(t *testing.T) {
	s, reg := newMetricsStore(t, NewMemObjects(), 0)
	ctx := context.Background()
	rec, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(ctx, "s1", chain(10), "cache-a", 2); err != nil {
		t.Fatal(err)
	}
	for outcome, want := range map[string]float64{metrics.BundleWritten: 1, metrics.BundleExisting: 1} {
		if got := metricstest.Value(t, reg, "cacheservice_resolution_writes_total", map[string]string{"scope": "s1", "outcome": outcome}); got != want {
			t.Errorf("resolution writes %s %v, want %v", outcome, got, want)
		}
	}
	// The existing one was read to check it.
	if got := metricstest.Value(t, reg, "cacheservice_resolution_reads_total", map[string]string{"scope": "s1"}); got != 1 {
		t.Fatalf("resolution reads %v, want 1", got)
	}
	if _, err := s.GetResolution(ctx, "s1", rec.ResolutionID); err != nil {
		t.Fatal(err)
	}
	if got := metricstest.Value(t, reg, "cacheservice_resolution_reads_total", map[string]string{"scope": "s1"}); got != 2 {
		t.Fatalf("resolution reads %v, want 2", got)
	}
	if got := metricstest.Value(t, reg, "cacheservice_resolution_read_bytes_total", map[string]string{"scope": "s1"}); got <= 0 {
		t.Fatalf("resolution read bytes %v", got)
	}
}
