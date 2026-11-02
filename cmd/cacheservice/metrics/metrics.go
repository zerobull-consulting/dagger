// Package metrics measures the service's storage costs, so that changes to
// how the metadata store lays out its index (see
// https://github.com/zerobull-consulting/dagger/issues/46) can be decided
// from real runs: how large each scope's index is, how often its conditional
// writes lose to another writer, how long reads and updates take, how many
// bytes an import sync reads, and how large bundles are.
//
// A nil *Metrics records nothing, so packages take one optionally.
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Callers label the path that reads or updates the index.
const (
	CallerExport = "export"
	CallerImport = "import"
	CallerOther  = "other"
)

// Update outcomes of UpdateIndex.
const (
	// OutcomeOK wrote the index.
	OutcomeOK = "ok"
	// OutcomeContention lost its conditional write MaxAttempts times.
	OutcomeContention = "contention"
	// OutcomeAborted wrote nothing because the change refused, or left the
	// index invalid.
	OutcomeAborted = "aborted"
	// OutcomeError failed to read or write the store, or was canceled.
	OutcomeError = "error"
)

// Bundle write outcomes of PutBundle.
const (
	// BundleWritten stored a new bundle.
	BundleWritten = "written"
	// BundleExisting found the bundle already stored, and wrote nothing.
	BundleExisting = "existing"
)

var (
	// durationBuckets span 5ms to about 40s.
	durationBuckets = prometheus.ExponentialBuckets(0.005, 2, 14)
	// sizeBuckets span 1 KiB to 256 MiB.
	sizeBuckets = prometheus.ExponentialBuckets(1024, 4, 10)
	// attemptBuckets cover UpdateIndex's attempts, up to its default limit.
	attemptBuckets = []float64{1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 50}
	// rootBuckets cover an export batch's roots, up to its limit of 1,000.
	rootBuckets = prometheus.ExponentialBuckets(1, 2, 11)
)

// Metrics holds the service's collectors.
type Metrics struct {
	indexReads          *prometheus.CounterVec
	indexReadBytes      *prometheus.CounterVec
	indexReadDuration   *prometheus.HistogramVec
	indexUpdates        *prometheus.CounterVec
	indexUpdateAttempts *prometheus.HistogramVec
	indexUpdateDuration *prometheus.HistogramVec
	indexConflicts      *prometheus.CounterVec
	indexWriteBytes     *prometheus.CounterVec
	indexSizeBytes      *prometheus.GaugeVec
	indexEntries        *prometheus.GaugeVec
	indexRoots          *prometheus.GaugeVec
	indexCaches         *prometheus.GaugeVec
	bundleWrites        *prometheus.CounterVec
	bundleWriteSize     *prometheus.HistogramVec
	bundleReads         *prometheus.CounterVec
	bundleReadBytes     *prometheus.CounterVec
	importBundleCache   *prometheus.CounterVec
	importSyncs         *prometheus.CounterVec
	importSyncDuration  prometheus.Histogram
	exportBatches       *prometheus.CounterVec
	exportBatchDuration prometheus.Histogram
	exportBatchRoots    prometheus.Histogram
	exportRoots         *prometheus.CounterVec
	exportBlobs         *prometheus.CounterVec
	exportBundles       prometheus.Counter
	collectors          []prometheus.Collector
}

// New returns the service's collectors, registered with reg. A nil reg
// registers nothing, as tests that only read the collectors want.
func New(reg prometheus.Registerer) *Metrics {
	const ns = "cacheservice"
	m := &Metrics{
		indexReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "index_reads_total",
			Help: "Reads of a scope's whole index, by the path that read it.",
		}, []string{"scope", "caller"}),
		indexReadBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "index_read_bytes_total",
			Help: "Bytes of index read, by the path that read it. Divide the import path's by cacheservice_import_syncs_total for bytes read per sync.",
		}, []string{"scope", "caller"}),
		indexReadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "index_read_duration_seconds",
			Help:    "Time to read and decode a scope's index.",
			Buckets: durationBuckets,
		}, []string{"scope", "caller"}),
		indexUpdates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "index_updates_total",
			Help: "Index updates, by outcome: ok, contention (lost its conditional write too many times), aborted (the change refused) or error.",
		}, []string{"scope", "caller", "outcome"}),
		indexUpdateAttempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "index_update_attempts",
			Help:    "Read-change-write attempts per index update. More than 1 means another writer won in between.",
			Buckets: attemptBuckets,
		}, []string{"scope", "caller"}),
		indexUpdateDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "index_update_duration_seconds",
			Help:    "Time per index update, across every attempt and backoff.",
			Buckets: durationBuckets,
		}, []string{"scope", "caller"}),
		indexConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "index_conflicts_total",
			Help: "Conditional index writes that lost to another writer (412, or 409 for a write in flight).",
		}, []string{"scope", "caller"}),
		indexWriteBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "index_write_bytes_total",
			Help: "Bytes of index written, counting writes that lost.",
		}, []string{"scope", "caller"}),
		indexSizeBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "index_size_bytes",
			Help: "Size of a scope's index, as last read or written.",
		}, []string{"scope"}),
		indexEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "index_entries",
			Help: "Entries in a scope's index, as last read or written.",
		}, []string{"scope"}),
		indexRoots: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "index_roots",
			Help: "Retained roots in a scope's index, as last read or written.",
		}, []string{"scope"}),
		indexCaches: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "index_caches",
			Help: "Engine caches with mappings in a scope's index, as last read or written.",
		}, []string{"scope"}),
		bundleWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "bundle_writes_total",
			Help: "Bundles stored, by outcome: written, or existing (already stored, nothing written).",
		}, []string{"scope", "outcome"}),
		bundleWriteSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "bundle_write_size_bytes",
			Help:    "Size of each bundle stored, written or existing.",
			Buckets: sizeBuckets,
		}, []string{"scope"}),
		bundleReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "bundle_reads_total",
			Help: "Bundles read from the store.",
		}, []string{"scope"}),
		bundleReadBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "bundle_read_bytes_total",
			Help: "Bytes of bundles read from the store.",
		}, []string{"scope"}),
		importBundleCache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "import_bundle_cache_total",
			Help: "Bundles the import path needed, by result: hit (in its memory) or miss (read from the store).",
		}, []string{"result"}),
		importSyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "import_syncs_total",
			Help: "Import syncs of one engine, by outcome: ok or error.",
		}, []string{"outcome"}),
		importSyncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "import_sync_duration_seconds",
			Help:    "Time per import sync of one engine.",
			Buckets: durationBuckets,
		}),
		exportBatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "export_batches_total",
			Help: "Export batches run on an engine, by outcome: ok, error, or interrupted (the engine went away, or the service stopped).",
		}, []string{"outcome"}),
		exportBatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "export_batch_duration_seconds",
			Help:    "Time per export batch, from export to the index update.",
			Buckets: durationBuckets,
		}),
		exportBatchRoots: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "export_batch_roots",
			Help:    "Roots per export batch.",
			Buckets: rootBuckets,
		}),
		exportRoots: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "export_roots_total",
			Help: "Roots of export batches, by result: stored, gone, busy, refused or failed.",
		}, []string{"result"}),
		exportBlobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "export_blobs_total",
			Help: "Layer blobs exports needed, by result: uploaded, or present (the store had them).",
		}, []string{"result"}),
		exportBundles: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "export_bundles_total",
			Help: "Bundles exports published.",
		}),
	}
	m.collectors = []prometheus.Collector{
		m.indexReads, m.indexReadBytes, m.indexReadDuration,
		m.indexUpdates, m.indexUpdateAttempts, m.indexUpdateDuration,
		m.indexConflicts, m.indexWriteBytes,
		m.indexSizeBytes, m.indexEntries, m.indexRoots, m.indexCaches,
		m.bundleWrites, m.bundleWriteSize, m.bundleReads, m.bundleReadBytes,
		m.importBundleCache, m.importSyncs, m.importSyncDuration,
		m.exportBatches, m.exportBatchDuration, m.exportBatchRoots,
		m.exportRoots, m.exportBlobs, m.exportBundles,
	}
	if reg != nil {
		reg.MustRegister(m.collectors...)
	}
	return m
}

// Collectors returns every collector, to register or read.
func (m *Metrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}
	return m.collectors
}

type callerKey struct{}

// WithCaller labels the index reads and updates made with ctx as caller's.
func WithCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// Caller returns ctx's caller label, or CallerOther.
func Caller(ctx context.Context) string {
	if c, ok := ctx.Value(callerKey{}).(string); ok && c != "" {
		return c
	}
	return CallerOther
}

// IndexShape is what an index holds, for the size gauges.
type IndexShape struct {
	Bytes   int
	Entries int
	Roots   int
	Caches  int
}

// IndexRead records a read of a scope's index, and its shape.
func (m *Metrics) IndexRead(ctx context.Context, scope string, d time.Duration, shape IndexShape) {
	if m == nil {
		return
	}
	caller := Caller(ctx)
	m.indexReads.WithLabelValues(scope, caller).Inc()
	m.indexReadBytes.WithLabelValues(scope, caller).Add(float64(shape.Bytes))
	m.indexReadDuration.WithLabelValues(scope, caller).Observe(d.Seconds())
	m.setShape(scope, shape)
}

// IndexWrite records a conditional write of a scope's index, won or lost.
// A won write sets the size gauges to its shape.
func (m *Metrics) IndexWrite(ctx context.Context, scope string, won bool, shape IndexShape) {
	if m == nil {
		return
	}
	caller := Caller(ctx)
	m.indexWriteBytes.WithLabelValues(scope, caller).Add(float64(shape.Bytes))
	if !won {
		m.indexConflicts.WithLabelValues(scope, caller).Inc()
		return
	}
	m.setShape(scope, shape)
}

func (m *Metrics) setShape(scope string, shape IndexShape) {
	m.indexSizeBytes.WithLabelValues(scope).Set(float64(shape.Bytes))
	m.indexEntries.WithLabelValues(scope).Set(float64(shape.Entries))
	m.indexRoots.WithLabelValues(scope).Set(float64(shape.Roots))
	m.indexCaches.WithLabelValues(scope).Set(float64(shape.Caches))
}

// IndexUpdate records one UpdateIndex call: its outcome, the attempts it
// made, and its time across them.
func (m *Metrics) IndexUpdate(ctx context.Context, scope, outcome string, attempts int, d time.Duration) {
	if m == nil {
		return
	}
	caller := Caller(ctx)
	m.indexUpdates.WithLabelValues(scope, caller, outcome).Inc()
	if attempts > 0 {
		m.indexUpdateAttempts.WithLabelValues(scope, caller).Observe(float64(attempts))
	}
	m.indexUpdateDuration.WithLabelValues(scope, caller).Observe(d.Seconds())
}

// BundleWrite records a stored bundle and its size.
func (m *Metrics) BundleWrite(scope, outcome string, bytes int) {
	if m == nil {
		return
	}
	m.bundleWrites.WithLabelValues(scope, outcome).Inc()
	m.bundleWriteSize.WithLabelValues(scope).Observe(float64(bytes))
}

// BundleRead records a bundle read from the store.
func (m *Metrics) BundleRead(scope string, bytes int) {
	if m == nil {
		return
	}
	m.bundleReads.WithLabelValues(scope).Inc()
	m.bundleReadBytes.WithLabelValues(scope).Add(float64(bytes))
}

// ImportBundleCache records whether a bundle the import path needed was in
// its memory.
func (m *Metrics) ImportBundleCache(hit bool) {
	if m == nil {
		return
	}
	result := "miss"
	if hit {
		result = "hit"
	}
	m.importBundleCache.WithLabelValues(result).Inc()
}

// ImportSync records one engine's import sync.
func (m *Metrics) ImportSync(failed bool, d time.Duration) {
	if m == nil {
		return
	}
	outcome := OutcomeOK
	if failed {
		outcome = OutcomeError
	}
	m.importSyncs.WithLabelValues(outcome).Inc()
	m.importSyncDuration.Observe(d.Seconds())
}

// ExportBatch is what one export batch did, for ExportBatch.
type ExportBatch struct {
	// Outcome is OutcomeOK, OutcomeError or ExportInterrupted.
	Outcome  string
	Duration time.Duration
	// Roots is how many roots the batch asked for.
	Roots                               int
	Stored, Gone, Busy, Refused, Failed int
	Uploaded, Present                   int
	Bundles                             int
}

// ExportInterrupted is an export cut short because its engine went away or
// the service stopped.
const ExportInterrupted = "interrupted"

// ExportBatch records one export batch.
func (m *Metrics) ExportBatch(b ExportBatch) {
	if m == nil {
		return
	}
	m.exportBatches.WithLabelValues(b.Outcome).Inc()
	m.exportBatchDuration.Observe(b.Duration.Seconds())
	m.exportBatchRoots.Observe(float64(b.Roots))
	for result, n := range map[string]int{
		"stored": b.Stored, "gone": b.Gone, "busy": b.Busy, "refused": b.Refused, "failed": b.Failed,
	} {
		if n > 0 {
			m.exportRoots.WithLabelValues(result).Add(float64(n))
		}
	}
	if b.Uploaded > 0 {
		m.exportBlobs.WithLabelValues("uploaded").Add(float64(b.Uploaded))
	}
	if b.Present > 0 {
		m.exportBlobs.WithLabelValues("present").Add(float64(b.Present))
	}
	m.exportBundles.Add(float64(b.Bundles))
}
