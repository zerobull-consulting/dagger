package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
)

func newTest(t *testing.T) (*metrics.Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return metrics.New(reg), reg
}

// A nil *Metrics records nothing, and doesn't panic.
func TestNilRecordsNothing(t *testing.T) {
	var m *metrics.Metrics
	ctx := context.Background()
	m.IndexRead(ctx, "s", time.Millisecond, metrics.IndexShape{Bytes: 1})
	m.IndexWrite(ctx, "s", true, metrics.IndexShape{Bytes: 1})
	m.IndexUpdate(ctx, "s", metrics.OutcomeOK, 1, time.Millisecond)
	m.BundleWrite("s", metrics.BundleWritten, 1)
	m.BundleRead("s", 1)
	m.ImportBundleCache(true)
	m.ImportSync(false, time.Millisecond)
	m.ExportBatch(metrics.ExportBatch{Outcome: metrics.OutcomeOK})
	if m.Collectors() != nil {
		t.Fatal("nil metrics has collectors")
	}
}

func TestNewWithoutRegistry(t *testing.T) {
	m := metrics.New(nil)
	if len(m.Collectors()) == 0 {
		t.Fatal("no collectors")
	}
	// The collectors can still be registered elsewhere.
	reg := prometheus.NewRegistry()
	reg.MustRegister(m.Collectors()...)
}

func TestCaller(t *testing.T) {
	ctx := context.Background()
	if got := metrics.Caller(ctx); got != metrics.CallerOther {
		t.Fatalf("caller %q, want %q", got, metrics.CallerOther)
	}
	if got := metrics.Caller(metrics.WithCaller(ctx, metrics.CallerImport)); got != metrics.CallerImport {
		t.Fatalf("caller %q, want %q", got, metrics.CallerImport)
	}
	if got := metrics.Caller(metrics.WithCaller(ctx, "")); got != metrics.CallerOther {
		t.Fatalf("empty caller %q, want %q", got, metrics.CallerOther)
	}
}

func TestIndexReadsAndWrites(t *testing.T) {
	m, reg := newTest(t)
	ctx := metrics.WithCaller(context.Background(), metrics.CallerExport)
	read := metrics.IndexShape{Bytes: 100, Entries: 3, Roots: 1, Caches: 1}
	m.IndexRead(ctx, "team", 20*time.Millisecond, read)
	m.IndexRead(ctx, "team", 30*time.Millisecond, read)
	labels := map[string]string{"scope": "team", "caller": metrics.CallerExport}
	if got := metricstest.Value(t, reg, "cacheservice_index_reads_total", labels); got != 2 {
		t.Fatalf("reads %v, want 2", got)
	}
	if got := metricstest.Value(t, reg, "cacheservice_index_read_bytes_total", labels); got != 200 {
		t.Fatalf("read bytes %v, want 200", got)
	}
	if s := metricstest.Read(t, reg, "cacheservice_index_read_duration_seconds", labels); s.Count != 2 || s.Sum < 0.049 || s.Sum > 0.051 {
		t.Fatalf("read duration %+v, want 2 samples summing to 0.05", s)
	}

	// A lost write counts its bytes and a conflict, and leaves the gauges.
	m.IndexWrite(ctx, "team", false, metrics.IndexShape{Bytes: 500, Entries: 9})
	if got := metricstest.Value(t, reg, "cacheservice_index_conflicts_total", labels); got != 1 {
		t.Fatalf("conflicts %v, want 1", got)
	}
	scope := map[string]string{"scope": "team"}
	if got := metricstest.Value(t, reg, "cacheservice_index_entries", scope); got != 3 {
		t.Fatalf("entries %v after a lost write, want 3", got)
	}
	// A won write sets them.
	m.IndexWrite(ctx, "team", true, metrics.IndexShape{Bytes: 600, Entries: 10, Roots: 4, Caches: 2})
	for name, want := range map[string]float64{
		"cacheservice_index_size_bytes": 600,
		"cacheservice_index_entries":    10,
		"cacheservice_index_roots":      4,
		"cacheservice_index_caches":     2,
	} {
		if got := metricstest.Value(t, reg, name, scope); got != want {
			t.Errorf("%s %v, want %v", name, got, want)
		}
	}
	if got := metricstest.Value(t, reg, "cacheservice_index_write_bytes_total", labels); got != 1100 {
		t.Fatalf("write bytes %v, want 1100", got)
	}
}

func TestIndexUpdate(t *testing.T) {
	m, reg := newTest(t)
	ctx := context.Background()
	m.IndexUpdate(ctx, "team", metrics.OutcomeOK, 3, time.Second)
	m.IndexUpdate(ctx, "team", metrics.OutcomeContention, 50, time.Second)
	m.IndexUpdate(ctx, "team", metrics.OutcomeError, 0, time.Second)
	for outcome, want := range map[string]float64{metrics.OutcomeOK: 1, metrics.OutcomeContention: 1, metrics.OutcomeError: 1} {
		labels := map[string]string{"scope": "team", "caller": metrics.CallerOther, "outcome": outcome}
		if got := metricstest.Value(t, reg, "cacheservice_index_updates_total", labels); got != want {
			t.Errorf("%s updates %v, want %v", outcome, got, want)
		}
	}
	// An update that failed before its first attempt observes no attempts.
	labels := map[string]string{"scope": "team", "caller": metrics.CallerOther}
	if s := metricstest.Read(t, reg, "cacheservice_index_update_attempts", labels); s.Count != 2 || s.Sum != 53 {
		t.Fatalf("attempts %+v, want 2 samples summing to 53", s)
	}
	if s := metricstest.Read(t, reg, "cacheservice_index_update_duration_seconds", labels); s.Count != 3 {
		t.Fatalf("durations %+v, want 3 samples", s)
	}
}

func TestBundlesAndImports(t *testing.T) {
	m, reg := newTest(t)
	m.BundleWrite("team", metrics.BundleWritten, 2048)
	m.BundleWrite("team", metrics.BundleExisting, 2048)
	m.BundleRead("team", 4096)
	m.ImportBundleCache(true)
	m.ImportBundleCache(false)
	m.ImportBundleCache(false)
	m.ImportSync(false, time.Millisecond)
	m.ImportSync(true, time.Millisecond)
	for _, c := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"cacheservice_bundle_writes_total", map[string]string{"outcome": metrics.BundleWritten}, 1},
		{"cacheservice_bundle_writes_total", map[string]string{"outcome": metrics.BundleExisting}, 1},
		{"cacheservice_bundle_reads_total", map[string]string{"scope": "team"}, 1},
		{"cacheservice_bundle_read_bytes_total", map[string]string{"scope": "team"}, 4096},
		{"cacheservice_import_bundle_cache_total", map[string]string{"result": "hit"}, 1},
		{"cacheservice_import_bundle_cache_total", map[string]string{"result": "miss"}, 2},
		{"cacheservice_import_syncs_total", map[string]string{"outcome": metrics.OutcomeOK}, 1},
		{"cacheservice_import_syncs_total", map[string]string{"outcome": metrics.OutcomeError}, 1},
	} {
		if got := metricstest.Value(t, reg, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	if s := metricstest.Read(t, reg, "cacheservice_bundle_write_size_bytes", map[string]string{"scope": "team"}); s.Count != 2 || s.Sum != 4096 {
		t.Fatalf("bundle sizes %+v", s)
	}
}

func TestExportBatch(t *testing.T) {
	m, reg := newTest(t)
	m.ExportBatch(metrics.ExportBatch{
		Outcome: metrics.OutcomeOK, Duration: time.Second, Roots: 5,
		Stored: 3, Gone: 1, Refused: 1, Uploaded: 2, Present: 4, Bundles: 1,
	})
	m.ExportBatch(metrics.ExportBatch{Outcome: metrics.ExportInterrupted, Roots: 2})
	for _, c := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"cacheservice_export_batches_total", map[string]string{"outcome": metrics.OutcomeOK}, 1},
		{"cacheservice_export_batches_total", map[string]string{"outcome": metrics.ExportInterrupted}, 1},
		{"cacheservice_export_roots_total", map[string]string{"result": "stored"}, 3},
		{"cacheservice_export_roots_total", map[string]string{"result": "gone"}, 1},
		{"cacheservice_export_roots_total", map[string]string{"result": "refused"}, 1},
		{"cacheservice_export_blobs_total", map[string]string{"result": "uploaded"}, 2},
		{"cacheservice_export_blobs_total", map[string]string{"result": "present"}, 4},
		{"cacheservice_export_bundles_total", nil, 1},
	} {
		if got := metricstest.Value(t, reg, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	// Results with no roots aren't reported.
	if s := metricstest.Read(t, reg, "cacheservice_export_roots_total", map[string]string{"result": "busy"}); s.Found {
		t.Fatalf("busy roots reported: %+v", s)
	}
	if s := metricstest.Read(t, reg, "cacheservice_export_batch_roots", nil); s.Count != 2 || s.Sum != 7 {
		t.Fatalf("batch roots %+v", s)
	}
}
