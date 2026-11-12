package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
)

// waitMetric waits until the service's metric with labels reaches at least
// want.
func waitMetric(t *testing.T, svc *Service, name string, labels map[string]string, want float64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := metricstest.Read(t, svc.registry, name, labels)
		got := s.Value + float64(s.Count)
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s%v = %v, want at least %v", name, labels, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// An export and an import record what they cost, labeled by their path,
// and the debug API serves it.
func TestMetricsOfExportAndImport(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.buildRecipes()
	h.exportBuild()
	b := fakeengine.New("cache-b", 1)
	eng := h.connectEngine(b)
	waitLookup(t, b, "withExec(make)", hasOffer)

	svc := h.svc
	waitMetric(t, svc, "cacheservice_export_batches_total", map[string]string{"outcome": metrics.OutcomeOK}, 1)
	waitMetric(t, svc, "cacheservice_export_bundles_total", nil, 1)
	waitMetric(t, svc, "cacheservice_export_roots_total", map[string]string{"result": "stored"}, 1)
	waitMetric(t, svc, "cacheservice_export_blobs_total", map[string]string{"result": "uploaded"}, 2)
	waitMetric(t, svc, "cacheservice_bundle_writes_total", map[string]string{"scope": testScope, "outcome": metrics.BundleWritten}, 1)
	waitMetric(t, svc, "cacheservice_index_updates_total",
		map[string]string{"scope": testScope, "caller": metrics.CallerExport, "outcome": metrics.OutcomeOK}, 1)
	waitMetric(t, svc, "cacheservice_index_reads_total", map[string]string{"scope": testScope, "caller": metrics.CallerImport}, 1)
	// Engine B's merge recorded its imports in the index.
	waitMetric(t, svc, "cacheservice_index_updates_total",
		map[string]string{"scope": testScope, "caller": metrics.CallerImport, "outcome": metrics.OutcomeOK}, 1)
	waitMetric(t, svc, "cacheservice_import_bundle_cache_total", map[string]string{"result": "miss"}, 1)
	waitMetric(t, svc, "cacheservice_import_syncs_total", map[string]string{"outcome": metrics.OutcomeOK}, 1)
	waitMetric(t, svc, "cacheservice_index_entries", map[string]string{"scope": testScope}, 2)

	// A sync the debug API asks for is measured as the import path's too.
	syncs := map[string]string{"outcome": metrics.OutcomeOK}
	before := metricstest.Value(t, svc.registry, "cacheservice_import_syncs_total", syncs)
	importReads := map[string]string{"scope": testScope, "caller": metrics.CallerImport}
	readsBefore := metricstest.Value(t, svc.registry, "cacheservice_index_reads_total", importReads)
	if code, _ := h.debugImport(eng); code != http.StatusOK {
		t.Fatalf("debug import: %d", code)
	}
	waitMetric(t, svc, "cacheservice_import_syncs_total", syncs, before+1)
	waitMetric(t, svc, "cacheservice_index_reads_total", importReads, readsBefore+1)

	resp, err := http.Get(h.debug.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: %d %s", resp.StatusCode, body)
	}
	for _, want := range []string{
		`cacheservice_export_batches_total{outcome="ok",scope="team"}`,
		`cacheservice_index_size_bytes{scope="team"}`,
		"go_goroutines",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("GET /metrics lacks %s", want)
		}
	}
}

// A metadata store configured with metrics of its own keeps them.
func TestMetricsStoreKeepsItsOwn(t *testing.T) {
	reg := prometheus.NewRegistry()
	meta, err := metastore.New(metastore.Config{Objects: metastore.NewMemObjects(), Metrics: metrics.New(reg)})
	if err != nil {
		t.Fatal(err)
	}
	blobs := blobstore.NewMemStore("")
	blobSrv := httptest.NewServer(blobs)
	t.Cleanup(blobSrv.Close)
	blobs.SetBaseURL(blobSrv.URL)
	h := newExportHarness(t, blobs, meta, nil)
	h.buildRecipes()
	h.exportBuild()

	labels := map[string]string{"scope": testScope, "caller": metrics.CallerExport, "outcome": metrics.OutcomeOK}
	if got := metricstest.Value(t, reg, "cacheservice_index_updates_total", labels); got < 1 {
		t.Fatalf("the store's own metrics saw %v updates", got)
	}
	if s := metricstest.Read(t, h.svc.registry, "cacheservice_index_updates_total", labels); s.Found {
		t.Fatalf("the service recorded the store's updates: %+v", s)
	}
}
