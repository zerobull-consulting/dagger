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
	// otlpSizeBuckets span 256 bytes to 256 MiB, past every route's limit.
	otlpSizeBuckets = prometheus.ExponentialBuckets(256, 4, 11)
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
	versionMismatch     *prometheus.CounterVec
	eventsIgnored       *prometheus.CounterVec
	engineIDsRefused    *prometheus.CounterVec
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
	gcUnexpectedKeys    *prometheus.GaugeVec
	gcRefused           *prometheus.CounterVec
	exportCollected     *prometheus.CounterVec
	draining            prometheus.Gauge
	exportEnabled       prometheus.Gauge
	exportWaiting       *prometheus.GaugeVec
	drains              *prometheus.HistogramVec
	drainStoredRoots    prometheus.Counter
	drainLeftRoots      prometheus.Counter
	drainRefused        *prometheus.CounterVec
	flushes             *prometheus.CounterVec
	flushDuration       prometheus.Histogram
	flushLeftRoots      prometheus.Counter
	otlpRefused         *prometheus.CounterVec
	otlpRequestBytes    *prometheus.HistogramVec
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
			Help: "Engine cache records in a scope's index, as last read or written, including those with no mappings left, kept for their generation (metastore.CacheRecordRetention).",
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
			Help: "Requests refused for a missing or unknown token, by endpoint: engine (the WebSocket), otlp, or debug (the debug API).",
		}, []string{"endpoint"}),
		tokensRevoked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "connections_revoked_total",
			Help: "Engine connections closed because their token was removed from its namespace, by scope.",
		}, []string{"scope"}),
		versionMismatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "engine_version_mismatch_total",
			Help: "Engine hellos whose version isn't on an accept line of the service's manifest, by outcome: refused (closed before the welcome, status 4002) or allowed (welcomed by -allow-engine-version-mismatch). The version itself is in the log, not a label.",
		}, []string{"outcome"}),
		eventsIgnored: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "engine_events_ignored_total",
			Help: "Engine events dropped because they came with a token other than the engine's, by scope and kind.",
		}, []string{"scope", "kind"}),
		engineIDsRefused: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "engine_ids_refused_total",
			Help: "What was refused because its engine cache attribute isn't \"<cacheID>/<generation>\" with a valid cache ID, or its cache ID or session isn't valid (1 to 128 letters, digits, '.', '_' or '-'), by source: span (dropped), event (dropped), wiped_cache (an engine start's wiped cache, ignored; the start is taken) or hello (refused). Nothing refused keys the service's state.",
		}, []string{"source"}),
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
		gcUnexpectedKeys: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "gc_unexpected_keys",
			Help: "Keys and multipart uploads of no known shape GC's last run found: per namespace, or scope _root outside the namespaces (a lower bound there: one page per unexpected prefix). Any is an ownership violation, and GC changed nothing in that namespace, or for _root anywhere.",
		}, []string{"scope"}),
		gcRefused: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "gc_refused_total",
			Help: "GC runs that did nothing at all because the prefix's ownership couldn't be confirmed, by reason: no_marker, other_layout, marker_unreadable, root_violation, uploads_unlisted.",
		}, []string{"reason"}),
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
		exportWaiting: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "export_roots_waiting",
			Help: "Roots of a scope due for export and waiting, as the scheduler last saw them, for their engine cache's export in flight to end (one export per engine cache at a time) or for the next batch.",
		}, []string{"scope"}),
		drains: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "drain_duration_seconds",
			Help:    "Time of each drain, by outcome: complete (nothing left it could export), deadline (-drain-timeout passed) or canceled (a second signal, or a drain that began after the export path stopped): both cut short, usually with work left, which the drain report's left and drain_unexported_roots_total count.",
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
		flushes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "flush_total",
			Help: "Flushes of one engine cache (POST /v1/engines/{cacheID}/flush), by outcome: complete (nothing left), timeout (something left when its timeout passed, or only retries due after it), not_connected (its engine wasn't connected) or canceled (the request or the export path ended).",
		}, []string{"outcome"}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Name: "flush_duration_seconds",
			Help:    "Time of each flush of one engine cache, from its start to its report.",
			Buckets: promotionBuckets,
		}),
		flushLeftRoots: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "flush_unexported_roots_total",
			Help: "Roots a flush left unexported when it ended: dirty, in flight, or waiting for a retry. Each flush adds what it left, so overlapping flushes of one cache each count the same roots: a count per flush, not of the roots lost.",
		}),
		otlpRefused: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "otlp_refused_total",
			Help: "OTLP requests refused for their body, by route (traces, logs, metrics) and reason: too_large (413, over the route's limit as received or decoded, or decoding to a message the decode budget could never hold), busy (503, the decode budget full), timeout (408, not received within the read timeout) or too_deep (400, nested past the limit). Nothing of a refused request is recorded.",
		}, []string{"route", "reason"}),
		otlpRequestBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "otlp_request_bytes",
			Help:    "Size of each OTLP request taken, by route and form: received (the body as sent, gzipped or not) or decoded (gunzipped). Compare with the routes' limits.",
			Buckets: otlpSizeBuckets,
		}, []string{"route", "form"}),
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
		m.authRejected, m.tokensRevoked, m.versionMismatch, m.eventsIgnored, m.engineIDsRefused,
		m.blobPromotions, m.blobHashedBytes, m.blobPromoteDuration,
		m.gcStoredBytes, m.gcStoredObjects, m.gcDeletedObjects, m.gcDeletedBytes,
		m.gcRuns, m.gcRunDuration, m.gcEntriesExpired, m.gcStagingRule, m.gcUnexpectedKeys, m.gcRefused,
		m.exportCollected,
		m.draining, m.exportEnabled, m.exportWaiting, m.drains, m.drainStoredRoots, m.drainLeftRoots, m.drainRefused,
		m.flushes, m.flushDuration, m.flushLeftRoots,
		m.otlpRefused, m.otlpRequestBytes,
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

// Outcomes of an engine hello whose version isn't accepted.
const (
	VersionMismatchRefused = "refused"
	VersionMismatchAllowed = "allowed"
)

// EngineVersionMismatch counts an engine hello whose version isn't
// accepted: refused, or allowed by -allow-engine-version-mismatch.
func (m *Metrics) EngineVersionMismatch(refused bool) {
	if m == nil {
		return
	}
	outcome := VersionMismatchAllowed
	if refused {
		outcome = VersionMismatchRefused
	}
	m.versionMismatch.WithLabelValues(outcome).Inc()
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

// Sources of what is dropped for an invalid engine cache ID or session, the
// source label of EngineIDsRefused: a fixed set, never taken from the
// request.
const (
	EngineIDSourceSpan       = "span"
	EngineIDSourceEvent      = "event"
	EngineIDSourceWipedCache = "wiped_cache"
	EngineIDSourceHello      = "hello"
)

// EngineIDsRefused records n spans, events, wiped caches or hellos, of
// source (an EngineIDSource constant), refused for an invalid engine cache
// attribute, cache ID or session.
func (m *Metrics) EngineIDsRefused(source string, n int) {
	if m == nil {
		return
	}
	m.engineIDsRefused.WithLabelValues(source).Add(float64(n))
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

// Reasons a GC run did nothing at all.
const (
	GCRefusedNoMarker         = "no_marker"
	GCRefusedOtherLayout      = "other_layout"
	GCRefusedMarkerUnreadable = "marker_unreadable"
	GCRefusedRootViolation    = "root_violation"
	GCRefusedUploadsUnlisted  = "uploads_unlisted"
)

// GCUnexpected records how many keys of no known shape GC found in a scope:
// a validated namespace, or gc.RootScope. No label comes from the bucket.
func (m *Metrics) GCUnexpected(scope string, n int) {
	if m == nil {
		return
	}
	m.gcUnexpectedKeys.WithLabelValues(scope).Set(float64(n))
}

// GCRefused records a GC run that did nothing, for reason (a GCRefused*
// constant).
func (m *Metrics) GCRefused(reason string) {
	if m == nil {
		return
	}
	m.gcRefused.WithLabelValues(reason).Inc()
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
	m.gcUnexpectedKeys.DeleteLabelValues(scope)
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
	// DrainDeadline was cut short at its deadline (-drain-timeout),
	// usually with work left.
	DrainDeadline = "deadline"
	// DrainCanceled was cut short because its context was canceled (a
	// second signal), or it began after the export path stopped, usually
	// with work left.
	DrainCanceled = "canceled"
)

// Flush outcomes.
const (
	// FlushComplete left nothing of its cache to export.
	FlushComplete = "complete"
	// FlushTimeout left something when its timeout passed, or only retries
	// due after it.
	FlushTimeout = "timeout"
	// FlushNotConnected found its cache's engine not connected.
	FlushNotConnected = "not_connected"
	// FlushCanceled ended because its request was canceled, or the export
	// path stopped.
	FlushCanceled = "canceled"
)

// FlushEnded records a flush's end: its outcome, how long it took, and how
// many roots it left unexported.
func (m *Metrics) FlushEnded(outcome string, d time.Duration, left int) {
	if m == nil {
		return
	}
	m.flushes.WithLabelValues(outcome).Inc()
	m.flushDuration.Observe(d.Seconds())
	m.flushLeftRoots.Add(float64(left))
}

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

// ExportWaiting records how many roots of a scope are due and waiting for
// their engine cache's slot, or the next batch.
func (m *Metrics) ExportWaiting(scope string, roots int) {
	if m == nil {
		return
	}
	m.exportWaiting.WithLabelValues(scope).Set(float64(roots))
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

// OTLP routes, the route label of the OTLP metrics: a fixed set, never taken
// from the request.
const (
	OTLPRouteTraces  = "traces"
	OTLPRouteLogs    = "logs"
	OTLPRouteMetrics = "metrics"
)

// Reasons an OTLP request is refused for its body.
const (
	// OTLPRefusedTooLarge is a body over its route's limit, as received or
	// decoded, or one that decodes to a message the decode budget could
	// never hold (413).
	OTLPRefusedTooLarge = "too_large"
	// OTLPRefusedBusy is a body that didn't fit in the decode budget (503).
	OTLPRefusedBusy = "busy"
	// OTLPRefusedTimeout is a body not received within the read timeout
	// (408).
	OTLPRefusedTimeout = "timeout"
	// OTLPRefusedTooDeep is a body that nests messages (or, for JSON,
	// objects and arrays) deeper than the service decodes (400).
	OTLPRefusedTooDeep = "too_deep"
)

// OTLPRefused records an OTLP request refused on route, one of the
// OTLPRoute constants, for reason, one of the OTLPRefused constants.
func (m *Metrics) OTLPRefused(route, reason string) {
	if m == nil {
		return
	}
	m.otlpRefused.WithLabelValues(route, reason).Inc()
}

// OTLPAccepted records the size of an OTLP request taken on route: as
// received, and decoded.
func (m *Metrics) OTLPAccepted(route string, received, decoded int64) {
	if m == nil {
		return
	}
	m.otlpRequestBytes.WithLabelValues(route, "received").Observe(float64(received))
	m.otlpRequestBytes.WithLabelValues(route, "decoded").Observe(float64(decoded))
}
