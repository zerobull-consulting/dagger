package importer

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// Merging one stored bundle into two engines reads it from the store once:
// the second merge finds it in memory.
func TestMetricsBundleCache(t *testing.T) {
	f := newFixture(t)
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	f.im.cfg.Metrics = m
	f.im.cfg.Meta = f.meta.WithMetrics(m)
	f.store(chainBundle(10, "base", "exec"), "cache-a")
	ctx := context.Background()
	for _, cacheID := range []string{"cache-b", "cache-c"} {
		eng := &scriptedEngine{cacheID: cacheID, generation: 1, onMerge: landAll(false), onOffer: answerAll(protocol.OfferAccepted)}
		res, err := f.im.Sync(ctx, eng)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Merged) != 1 {
			t.Fatalf("%s: result %+v", cacheID, res)
		}
	}
	for result, want := range map[string]float64{"miss": 1, "hit": 1} {
		if got := metricstest.Value(t, reg, "cacheservice_import_bundle_cache_total", map[string]string{"result": result}); got != want {
			t.Errorf("%s %v, want %v", result, got, want)
		}
	}
	if got := metricstest.Value(t, reg, "cacheservice_bundle_reads_total", map[string]string{"scope": scope}); got != 1 {
		t.Fatalf("bundle reads %v, want 1", got)
	}
}
