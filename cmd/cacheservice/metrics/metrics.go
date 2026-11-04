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
	CallerGC     = "gc"
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
	// promotionBuckets span 10ms to about 22 minutes: a promotion takes
	// time in proportion to the blob's size.
	promotionBuckets = prometheus.ExponentialBuckets(0.01, 2, 18)
)

// Promotion outcomes of an uploaded blob (see blobstore.Outcome), and
// PromotionError for a promotion the store failed.
const PromotionError = "error"

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
	resolutionWrites    *prometheus.CounterVec
	resolutionWriteSize *prometheus.HistogramVec
	resolutionReads     *prometheus.CounterVec
	resolutionReadBytes *prometheus.CounterVec
	importResCache      *prometheus.CounterVec
	importSyncs         *prometheus.CounterVec
	importSyncDuration  prometheus.Histogram
	exportBatches       *prometheus.CounterVec
	exportBatchDuration prometheus.Histogram
	exportBatchRoots    prometheus.Histogram
	exportRoots         *prometheus.CounterVec
	exportBlobs         *prometheus.CounterVec
	exportBundles       prometheus.Counter
	authRejected        *prometheus.CounterVec
	tokensRevoked       *prometheus.CounterVec
	eventsIgnored       *prometheus.CounterVec
	blobPromotions      *prometheus.CounterVec
	blobHashedBytes     *prometheus.CounterVec
	blobPromoteDuration *prometheus.HistogramVec
	gcStoredBytes       *prometheus.GaugeVec
	gcStoredObjects     *prometheus.GaugeVec
	gcDeletedObjects    *prometheus.CounterVec
	gcDeletedBytes      *prometheus.CounterVec
	gcRuns              *prometheus.CounterVec
	gcRunDuration       *prometheus.HistogramVec
	gcEntriesExpired    *prometheus.CounterVec
	gcStagingRule       *prometheus.GaugeVec
	exportCollected     *prometheus.CounterVec
	draining            prometheus.Gauge
	exportEnabled       prometheus.Gauge
	drains              *prometheus.HistogramVec
	drainStoredRoots    prometheus.Counter
	drainLeftRoots      prometheus.Counter
	drainRefused        *prometheus.CounterVec
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
		resolutionWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "resolution_writes_total",
			Help: "Bundle resolutions stored (one per recorded export, more when its index write lost), by outcome: written, or existing (already stored, nothing written).",
		}, []string{"scope", "outcome"}),
		resolutionWriteSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "resolution_write_size_bytes",
			Help:    "Size of each resolution stored, written or existing.",
			Buckets: sizeBuckets,
		}, []string{"scope"}),
		resolutionReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "resolution_reads_total",
			Help: "Bundle resolutions read from the store.",
		}, []string{"scope"}),
		resolutionReadBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "resolution_read_bytes_total",
			Help: "Bytes of resolutions read from the store.",
		}, []string{"scope"}),
		importResCache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "import_resolution_cache_total",
			Help: "Resolutions the import path needed, by result: hit (in its memory) or miss (read from the store).",
		}, []string{"result"}),
		importSyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "import_syncs_total",
			Help: "Import syncs of one engine, by scope (the engine's namespace) and outcome: ok or error.",
		}, []string{"scope", "outcome"}),
		importSyncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "import_sync_duration_seconds",
			Help:    "Time per import sync of one engine.",
			Buckets: durationBuckets,
		}),
		exportBatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "export_batches_total",
			Help: "Export batches run on an engine, by scope (the engine's namespace) and outcome: ok, error, or interrupted (the engine went away, or the service stopped).",
		}, []string{"scope", "outcome"}),
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
			Help: "Layer blobs exports needed, by result: uploaded (and verified), present (the store had them), or rejected (an upload that failed verification, per try).",
		}, []string{"result"}),
		exportBundles: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "export_bundles_total",
			Help: "Bundles exports published.",
		}),
		authRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "auth_rejected_total",
			Help: "Requests refused for a missing or unknown token, by endpoint: engine (the WebSocket) or otlp.",
		}, []string{"endpoint"}),
		tokensRevoked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "connections_revoked_total",
			Help: "Engine connections closed because their token was removed from its namespace, by scope.",
		}, []string{"scope"}),
		eventsIgnored: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "engine_events_ignored_total",
			Help: "Engine events dropped because they came with a token other than the engine's, by scope and kind.",
		}, []string{"scope", "kind"}),
		blobPromotions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "blob_promotions_total",
			Help: "Uploaded blobs verified and promoted to their digest keys, by scope and outcome: promoted, existing (already stored), mismatch (content not the digest's), missing (nothing uploaded) or error.",
		}, []string{"scope", "outcome"}),
		blobHashedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "blob_hashed_bytes_total",
			Help: "Bytes of uploaded blobs hashed to verify them, by scope and by whom: store (as it copied them) or service (read through the service).",
		}, []string{"scope", "by"}),
		blobPromoteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "blob_promotion_duration_seconds",
			Help:    "Time to verify and promote one uploaded blob, waiting for a slot included. It grows with the blob's size.",
			Buckets: promotionBuckets,
		}, []string{"scope"}),
		gcStoredBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "gc_stored_bytes",
			Help: "Bytes stored under the scope's prefix, by kind (blob, bundle, resolution, index, staging, other), as GC's last listing found them.",
		}, []string{"scope", "kind"}),
		gcStoredObjects: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "gc_stored_objects",
			Help: "Objects stored under the scope's prefix, by kind, as GC's last listing found them.",
		}, []string{"scope", "kind"}),
		gcDeletedObjects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gc_deleted_objects_total",
			Help: "Objects GC deleted, by scope and kind: blob, bundle, resolution, staging, or upload (an incomplete multipart upload aborted).",
		}, []string{"scope", "kind"}),
		gcDeletedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gc_deleted_bytes_total",
			Help: "Bytes GC deleted, by scope and kind, as its listing sized them (0 for aborted uploads).",
		}, []string{"scope", "kind"}),
		gcRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gc_runs_total",
			Help: "GC runs of one scope, by outcome: ok, error, or dry_run.",
		}, []string{"scope", "outcome"}),
		gcRunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "gc_run_duration_seconds",
			Help:    "Time of one scope's GC run.",
			Buckets: promotionBuckets,
		}, []string{"scope"}),
		gcEntriesExpired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gc_entries_expired_total",
			Help: "Index entries GC dropped, by scope and reason: expired (their own expiry, or the scope's max age) or size (the scope's max bytes).",
		}, []string{"scope", "reason"}),
		gcStagingRule: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "gc_staging_rule_missing",
			Help: "1 when the bucket's lifecycle rules don't expire the scope's staging/ prefix (GC expires it itself), 0 when they do; absent when the rules can't be read.",
		}, []string{"scope"}),
		exportCollected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "export_collected_total",
			Help: "Export tries refused because GC deleted, or was deleting, an object they named; each is tried again.",
		}, []string{"scope"}),
		draining: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "draining",
			Help: "1 while the service drains its exports before it stops (SIGTERM), 0 otherwise.",
		}),
		exportEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "export_enabled",
			Help: "1 when the export path is on, 0 when it is off: read-only (-export=false), or no bucket. A read-only instance never writes to or deletes from the bucket.",
		}),
		drains: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "drain_duration_seconds",
			Help:    "Time of each drain, by outcome: complete (nothing left it could export), deadline (-drain-timeout passed) or canceled (a second signal).",
			Buckets: promotionBuckets,
		}, []string{"outcome"}),
		drainStoredRoots: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "drain_stored_roots_total",
			Help: "Roots stored by exports that ended while the service drained.",
		}),
		drainLeftRoots: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "drain_unexported_roots_total",
			Help: "Roots left unexported when a drain ended: pending, in flight, waiting for a retry, or of an engine no longer connected.",
		}),
		drainRefused: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "drain_refused_total",
			Help: "New work refused while draining, by kind: connection (an engine WebSocket, 503) or span (a cache span or session end of a session the service doesn't track).",
		}, []string{"kind"}),
	}
	m.collectors = []prometheus.Collector{
		m.indexReads, m.indexReadBytes, m.indexReadDuration,
		m.indexUpdates, m.indexUpdateAttempts, m.indexUpdateDuration,
		m.indexConflicts, m.indexWriteBytes,
		m.indexSizeBytes, m.indexEntries, m.indexRoots, m.indexCaches,
		m.bundleWrites, m.bundleWriteSize, m.bundleReads, m.bundleReadBytes,
		m.resolutionWrites, m.resolutionWriteSize, m.resolutionReads, m.resolutionReadBytes,
		m.importBundleCache, m.importResCache, m.importSyncs, m.importSyncDuration,
		m.exportBatches, m.exportBatchDuration, m.exportBatchRoots,
		m.exportRoots, m.exportBlobs, m.exportBundles,
		m.authRejected, m.tokensRevoked, m.eventsIgnored,
		m.blobPromotions, m.blobHashedBytes, m.blobPromoteDuration,
		m.gcStoredBytes, m.gcStoredObjects, m.gcDeletedObjects, m.gcDeletedBytes,
		m.gcRuns, m.gcRunDuration, m.gcEntriesExpired, m.gcStagingRule,
		m.exportCollected,
		m.draining, m.exportEnabled, m.drains, m.drainStoredRoots, m.drainLeftRoots, m.drainRefused,
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

// ResolutionWrite records a stored resolution and its size; outcome is
// BundleWritten or BundleExisting.
func (m *Metrics) ResolutionWrite(scope, outcome string, bytes int) {
	if m == nil {
		return
	}
	m.resolutionWrites.WithLabelValues(scope, outcome).Inc()
	m.resolutionWriteSize.WithLabelValues(scope).Observe(float64(bytes))
}

// ResolutionRead records a resolution read from the store.
func (m *Metrics) ResolutionRead(scope string, bytes int) {
	if m == nil {
		return
	}
	m.resolutionReads.WithLabelValues(scope).Inc()
	m.resolutionReadBytes.WithLabelValues(scope).Add(float64(bytes))
}

// ImportResolutionCache records whether a resolution the import path needed
// was in its memory.
func (m *Metrics) ImportResolutionCache(hit bool) {
	if m == nil {
		return
	}
	result := "miss"
	if hit {
		result = "hit"
	}
	m.importResCache.WithLabelValues(result).Inc()
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

// ImportSync records one engine's import sync, in the engine's scope.
func (m *Metrics) ImportSync(scope string, failed bool, d time.Duration) {
	if m == nil {
		return
	}
	outcome := OutcomeOK
	if failed {
		outcome = OutcomeError
	}
	m.importSyncs.WithLabelValues(scope, outcome).Inc()
	m.importSyncDuration.Observe(d.Seconds())
}

// ExportBatch is what one export batch did, for ExportBatch.
type ExportBatch struct {
	// Scope is the namespace of the batch's engine.
	Scope string
	// Outcome is OutcomeOK, OutcomeError or ExportInterrupted.
	Outcome  string
	Duration time.Duration
	// Roots is how many roots the batch asked for.
	Roots                               int
	Stored, Gone, Busy, Refused, Failed int
	// Uploaded and Present are blobs stored (uploaded and verified, or
	// already there), Rejected uploads that failed verification.
	Uploaded, Present, Rejected int
	Bundles                     int
}

// ExportInterrupted is an export cut short because its engine went away or
// the service stopped.
const ExportInterrupted = "interrupted"

// ExportBatch records one export batch.
func (m *Metrics) ExportBatch(b ExportBatch) {
	if m == nil {
		return
	}
	m.exportBatches.WithLabelValues(b.Scope, b.Outcome).Inc()
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
	if b.Rejected > 0 {
		m.exportBlobs.WithLabelValues("rejected").Add(float64(b.Rejected))
	}
	m.exportBundles.Add(float64(b.Bundles))
}

// AuthRejected records a request refused for a missing or unknown token.
func (m *Metrics) AuthRejected(endpoint string) {
	if m == nil {
		return
	}
	m.authRejected.WithLabelValues(endpoint).Inc()
}

// TokenRevoked records a connection closed because its token was removed.
func (m *Metrics) TokenRevoked(scope string) {
	if m == nil {
		return
	}
	m.tokensRevoked.WithLabelValues(scope).Inc()
}

// IgnoredEvent records an engine event dropped because it came with a token
// other than the engine's.
func (m *Metrics) IgnoredEvent(scope, kind string) {
	if m == nil {
		return
	}
	m.eventsIgnored.WithLabelValues(scope, kind).Inc()
}

// BlobPromotion records one uploaded blob's verification and promotion in a
// scope: its outcome (a blobstore.Outcome, or PromotionError), who hashed it
// ("store", "service", or "" when nothing was) and how many bytes, and how
// long it took. Every label is the service's own: scope is a validated
// namespace, and outcome and hashedBy are constants.
func (m *Metrics) BlobPromotion(scope, outcome, hashedBy string, bytes int64, d time.Duration) {
	if m == nil {
		return
	}
	m.blobPromotions.WithLabelValues(scope, outcome).Inc()
	if hashedBy != "" {
		m.blobHashedBytes.WithLabelValues(scope, hashedBy).Add(float64(bytes))
	}
	m.blobPromoteDuration.WithLabelValues(scope).Observe(d.Seconds())
}

// GC kinds of stored and deleted objects.
const (
	KindBlob       = "blob"
	KindBundle     = "bundle"
	KindResolution = "resolution"
	KindIndex      = "index"
	KindStaging    = "staging"
	KindUpload     = "upload"
	KindOther      = "other"
)

// GC run outcomes.
const (
	GCOK     = "ok"
	GCError  = "error"
	GCDryRun = "dry_run"
)

// GC reasons for expired entries.
const (
	ExpiredByExpiry = "expired"
	ExpiredBySize   = "size"
)

// GCStored records what GC's listing of a scope found, by kind. Every label
// is the service's own: scope is a validated namespace, kind a constant.
func (m *Metrics) GCStored(scope string, bytes, objects map[string]int64) {
	if m == nil {
		return
	}
	for _, k := range []string{KindBlob, KindBundle, KindResolution, KindIndex, KindStaging, KindOther} {
		m.gcStoredBytes.WithLabelValues(scope, k).Set(float64(bytes[k]))
		m.gcStoredObjects.WithLabelValues(scope, k).Set(float64(objects[k]))
	}
}

// GCDeleted records objects GC deleted in a scope.
func (m *Metrics) GCDeleted(scope, kind string, objects int, bytes int64) {
	if m == nil || objects == 0 {
		return
	}
	m.gcDeletedObjects.WithLabelValues(scope, kind).Add(float64(objects))
	m.gcDeletedBytes.WithLabelValues(scope, kind).Add(float64(bytes))
}

// GCRun records one scope's GC run.
func (m *Metrics) GCRun(scope, outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.gcRuns.WithLabelValues(scope, outcome).Inc()
	m.gcRunDuration.WithLabelValues(scope).Observe(d.Seconds())
}

// GCExpired records index entries GC dropped.
func (m *Metrics) GCExpired(scope, reason string, entries int) {
	if m == nil || entries == 0 {
		return
	}
	m.gcEntriesExpired.WithLabelValues(scope, reason).Add(float64(entries))
}

// GCStagingRule records whether a lifecycle rule expires the scope's
// staging prefix.
func (m *Metrics) GCStagingRule(scope string, missing bool) {
	if m == nil {
		return
	}
	v := 0.0
	if missing {
		v = 1
	}
	m.gcStagingRule.WithLabelValues(scope).Set(v)
}

// GCStagingRuleUnknown removes the scope's gc_staging_rule_missing, when
// the bucket's rules can't be read: neither 0 nor 1 would be true.
func (m *Metrics) GCStagingRuleUnknown(scope string) {
	if m == nil {
		return
	}
	m.gcStagingRule.DeleteLabelValues(scope)
}

// GCForget removes a scope's GC gauges, for a namespace no longer in the
// bucket: they would otherwise report its last listing forever.
func (m *Metrics) GCForget(scope string) {
	if m == nil {
		return
	}
	for _, k := range []string{KindBlob, KindBundle, KindResolution, KindIndex, KindStaging, KindOther} {
		m.gcStoredBytes.DeleteLabelValues(scope, k)
		m.gcStoredObjects.DeleteLabelValues(scope, k)
	}
	m.gcStagingRule.DeleteLabelValues(scope)
}

// ExportCollected records an export try refused by GC's fence.
func (m *Metrics) ExportCollected(scope string) {
	if m == nil {
		return
	}
	m.exportCollected.WithLabelValues(scope).Inc()
}

// Drain outcomes.
const (
	// DrainComplete left nothing it could export.
	DrainComplete = "complete"
	// DrainDeadline ended at its deadline (-drain-timeout).
	DrainDeadline = "deadline"
	// DrainCanceled ended because its context was canceled (a second
	// signal).
	DrainCanceled = "canceled"
)

// Kinds of work refused while draining.
const (
	DrainRefusedConnection = "connection"
	DrainRefusedSpan       = "span"
)

// ExportEnabled records whether the export path is on.
func (m *Metrics) ExportEnabled(on bool) {
	if m == nil {
		return
	}
	v := 0.0
	if on {
		v = 1
	}
	m.exportEnabled.Set(v)
}

// DrainStarted records that the service started draining.
func (m *Metrics) DrainStarted() {
	if m == nil {
		return
	}
	m.draining.Set(1)
}

// DrainEnded records a drain's end: its outcome, how long it took, and how
// many roots it left unexported. The service is still draining (it stops).
func (m *Metrics) DrainEnded(outcome string, d time.Duration, left int) {
	if m == nil {
		return
	}
	m.drains.WithLabelValues(outcome).Observe(d.Seconds())
	m.drainLeftRoots.Add(float64(left))
}

// DrainStored records roots stored by an export that ended while draining.
func (m *Metrics) DrainStored(roots int) {
	if m == nil {
		return
	}
	m.drainStoredRoots.Add(float64(roots))
}

// DrainRefused records new work refused while draining, of kind
// DrainRefusedConnection or DrainRefusedSpan.
func (m *Metrics) DrainRefused(kind string) {
	if m == nil {
		return
	}
	m.drainRefused.WithLabelValues(kind).Inc()
}
