package server

// A read-only service (-export=false, zerobull-consulting/dagger#65): it
// imports, offers and renews, and never writes to or deletes from the
// bucket. These run it the way EnableS3 does (enableStores), on in-memory
// stores a writer service filled first, behind a metadata store that
// refuses every write, as a read-only key would.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// refuseWrites is a metadata store's objects behind a read-only key: reads
// go through, and every Put and Delete is refused and counted.
type refuseWrites struct {
	metastore.Objects
	mu     sync.Mutex
	writes []string
}

var errAccessDenied = errors.New("AccessDenied: the key is read-only")

func (r *refuseWrites) Put(_ context.Context, key string, _ []byte, _ metastore.Condition) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, "put "+key)
	return "", fmt.Errorf("put %s: %w", key, errAccessDenied)
}

func (r *refuseWrites) Delete(_ context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, "delete "+key)
	return fmt.Errorf("delete %s: %w", key, errAccessDenied)
}

// ListUploadKeys lists uploads, as the key may.
func (r *refuseWrites) ListUploadKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	return r.Objects.(ownership.Store).ListUploadKeys(ctx, prefix, max)
}

// ListKeys lists, as a read-only key may (s3:ListBucket).
func (r *refuseWrites) ListKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	return r.Objects.(ownership.Store).ListKeys(ctx, prefix, max)
}

func (r *refuseWrites) attempted() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.writes)
}

// sharedStores are a writer's in-memory stores, which a read-only service
// then reads.
type sharedStores struct {
	blobs *blobstore.MemStore
	objs  *metastore.MemObjects
	meta  *metastore.Store
}

func newSharedStores(t *testing.T) sharedStores {
	t.Helper()
	blobs := blobstore.NewMemStore("")
	blobSrv := httptest.NewServer(blobs)
	t.Cleanup(blobSrv.Close)
	blobs.SetBaseURL(blobSrv.URL)
	objs := metastore.NewMemObjects()
	meta, err := metastore.New(metastore.Config{Objects: objs})
	if err != nil {
		t.Fatal(err)
	}
	// The writer claimed the prefix, as its startup does.
	if _, _, err := ownership.Check(context.Background(), objs, "", true, time.Now()); err != nil {
		t.Fatal(err)
	}
	return sharedStores{blobs: blobs, objs: objs, meta: meta}
}

// snapshot is a full listing of both stores: every metadata object with its
// ETag, and every blob, promoted or staged.
func (s sharedStores) snapshot(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, o := range s.objs.List("") {
		_, etag, err := s.objs.Get(context.Background(), o.Key)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("meta %s %d %s", o.Key, o.Size, etag))
	}
	for _, b := range s.blobs.Objects() {
		out = append(out, fmt.Sprintf("blob %s %s %s %d", b.NS, b.Stage, b.Digest, b.Size))
	}
	out = append(out, fmt.Sprintf("blob puts %d", s.blobs.Puts()))
	slices.Sort(out)
	return out
}

// readOnlyHarness is a read-only service on a writer's stores.
type readOnlyHarness struct {
	t     *testing.T
	svc   *Service
	srv   *httptest.Server
	debug *httptest.Server
	logs  *logRecorder
	objs  *refuseWrites
	ctx   context.Context
}

// readOnlyOptions are what cmd/cacheservice passes with -export=false and
// every other flag at its default: no GC. The tracker's times are short, so
// that a tracker, if there were one, would export within the tests' waits.
func readOnlyOptions() S3Options {
	return S3Options{
		Scope:    testScope,
		Tracker:  exporter.TrackerConfig{Settle: 50 * time.Millisecond, MaxDelay: 50 * time.Millisecond},
		NoExport: true,
	}
}

func newReadOnlyHarness(t *testing.T, stores sharedStores, opts S3Options) *readOnlyHarness {
	t.Helper()
	return newReadOnlyHarnessWrapped(t, stores, opts, nil)
}

// newReadOnlyHarnessWrapped is newReadOnlyHarness, with the service's index
// reads going through wrap, when set.
func newReadOnlyHarnessWrapped(t *testing.T, stores sharedStores, opts S3Options, wrap func(metastore.Objects) metastore.Objects) *readOnlyHarness {
	t.Helper()
	objs := &refuseWrites{Objects: stores.objs}
	var read metastore.Objects = objs
	if wrap != nil {
		read = wrap(objs)
	}
	meta, err := metastore.New(metastore.Config{Objects: read})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	logs := &logRecorder{}
	svc := New(slog.New(logs), "")
	err = svc.enableStores(ctx, storeSet{
		Blobs: stores.blobs,
		Meta:  meta,
		GCBucket: func() (gc.Bucket, error) {
			return nil, errors.New("a read-only service asked for the GC bucket")
		},
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(svc.Handler())
	debug := httptest.NewServer(svc.DebugHandler())
	t.Cleanup(func() {
		cancel()
		svc.CloseEngines()
		svc.Wait()
		srv.Close()
		debug.Close()
	})
	return &readOnlyHarness{t: t, svc: svc, srv: srv, debug: debug, logs: logs, objs: objs, ctx: ctx}
}

// connect connects an engine with cache, and returns its connection number.
func (h *readOnlyHarness) connect(cache *fakeengine.Cache) uint64 {
	h.t.Helper()
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: cache}, remotecache.Config{EngineVersion: testEngineVersion, URL: h.srv.URL, Token: "engine-token", EngineName: cache.ID})
	}()
	h.t.Cleanup(func() {
		cancel()
		<-done
	})
	var conn uint64
	waitUntil(h.t, 10*time.Second, "engine "+cache.ID+" to connect", func() bool {
		for _, e := range h.svc.Engines() {
			if e.CacheID == cache.ID {
				conn = e.Conn
				return true
			}
		}
		return false
	})
	return conn
}

// waitMerged waits until the import path merged n bundles into conn's
// engine: the importer's own state, not the engine's entries.
func (h *readOnlyHarness) waitMerged(conn uint64, n int) {
	h.t.Helper()
	waitUntil(h.t, 10*time.Second, fmt.Sprintf("%d merged bundles", n), func() bool {
		st, err := h.svc.Imports(conn)
		return err == nil && len(st.Merged) == n
	})
}

func (h *readOnlyHarness) postSpans(cache *fakeengine.Cache, session string, spans ...*tracepb.Span) {
	h.t.Helper()
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			kv(telemetryattrs.EngineCacheAttr, cache.ID+"/"+strconv.FormatUint(cache.Generation, 10)),
			kv(telemetryattrs.EngineSessionAttr, session),
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
	if code := otlpPost(h.t, h.srv.URL+"/v1/traces", req, &coltracepb.ExportTraceServiceResponse{}, false, false); code != http.StatusOK {
		h.t.Fatalf("POST /v1/traces: %d", code)
	}
}

// postStart posts an engine.start of cache that reports wiping wiped.
func (h *readOnlyHarness) postStart(cache *fakeengine.Cache, wiped string) {
	h.t.Helper()
	body, err := json.Marshal(telemetryattrs.EngineStartEvent{WipedCache: wiped})
	if err != nil {
		h.t.Fatal(err)
	}
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: telemetryattrs.EngineEventScope},
			LogRecords: []*logspb.LogRecord{{
				Body: strValue(string(body)),
				Attributes: []*commonpb.KeyValue{
					kv(telemetryattrs.EngineEventAttr, telemetryattrs.EngineEventStart),
					kv(telemetryattrs.EngineCacheAttr, cache.ID+"/"+strconv.FormatUint(cache.Generation, 10)),
				},
			}},
		}},
	}}}
	if code := otlpPost(h.t, h.srv.URL+"/v1/logs", req, &collogspb.ExportLogsServiceResponse{}, false, false); code != http.StatusOK {
		h.t.Fatalf("POST /v1/logs: %d", code)
	}
}

// post posts to the debug API, and returns the status and body.
func (h *readOnlyHarness) post(path string) (int, string) {
	h.t.Helper()
	return debugPost(h.t, h.debug.URL+path)
}

func debugPost(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// writerStored has a writer service store engine A's build (10 and 11,
// recipes from(alpine) and withExec(make)) in stores, and returns its layers.
// The writer is stopped before it returns, so nothing else writes.
func writerStored(t *testing.T, stores sharedStores) [][]byte {
	t.Helper()
	h := newExportHarness(t, stores.blobs, stores.meta, nil)
	layers := h.buildRecipes()
	h.exportBuild()
	h.cancel()
	h.svc.Wait()
	return layers
}

// The read-only service merges what a writer stored into a fresh engine,
// whose layers it pulls, and renews their addresses. A session of the engine
// then ends and its quiet period passes, a wiped-cache event arrives, and
// nothing is written: the index and the bucket list exactly as before, and
// the metadata store saw no write at all, not even a refused one.
func TestReadOnlyImportsAndWritesNothing(t *testing.T) {
	stores := newSharedStores(t)
	layers := writerStored(t, stores)
	before := stores.snapshot(t)

	h := newReadOnlyHarness(t, stores, readOnlyOptions())
	b := fakeengine.New("cache-b", 1)
	conn := h.connect(b)
	h.waitMerged(conn, 1)
	e := waitLookup(t, b, "withExec(make)", hasOffer)
	if ok, err := b.Renew(context.Background(), e.Number); err != nil || !ok {
		t.Fatalf("renew: %v, %v", ok, err)
	}
	checkPull(t, b, "withExec(make)", layers)
	checkPull(t, b, "from(alpine)", layers[:1])

	// B builds on what it merged, and its sessions end: one with its end
	// seen, and one only by its quiet period. Spans are received and
	// dropped, which is intended: a read-only service has no tracker, so
	// nothing is recorded, exported or uploaded.
	b.Add(fakeengine.Entry{Number: 20, Deps: []uint64{e.Number}, Layers: [][]byte{testLayer("new")}})
	b.Add(fakeengine.Entry{Number: 21, Deps: []uint64{20}, Layers: [][]byte{testLayer("newer")}})
	h.postSpans(b, "session-1", cacheSpan(20, strconv.FormatUint(e.Number, 10)), carrierSpan(1))
	h.postSpans(b, "session-2", cacheSpan(21, "20"))
	// A wiped cache's imports would be dropped from the index; a read-only
	// service records none, so it drops none.
	h.postStart(b, "cache-wiped")

	// Nothing happening can only be shown by waiting. 1s is 20 times the
	// quiet period and the end grace (50ms each): with a tracker, both
	// sessions would have been due within 100ms, and the writer harness's
	// exports are stored within that.
	time.Sleep(time.Second)

	if got := h.objs.attempted(); len(got) > 0 {
		t.Errorf("the read-only service tried to write the metadata store: %v", got)
	}
	if after := stores.snapshot(t); !slices.Equal(before, after) {
		t.Errorf("the stores changed:\nbefore %v\nafter  %v", before, after)
	}
	if _, err := h.svc.Entries(conn); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Entries: %v, want ErrReadOnly", err)
	}
	if len(h.logs.find("conditional write check skipped: the instance is read-only (-export=false), and never writes the index")) != 1 {
		t.Error("no log that the conditional write check was skipped")
	}
}

// A read-only service with imports recorded would try to write the index on
// every merge: this is the write the importer skips
// (importer.Config.NoRecordImports). Pinned so a change that turns it back on
// is caught here, and not by a read-only key in production.
func TestReadOnlyRecordsNoImports(t *testing.T) {
	stores := newSharedStores(t)
	writerStored(t, stores)
	h := newReadOnlyHarness(t, stores, readOnlyOptions())
	conn := h.connect(fakeengine.New("cache-b", 1))
	h.waitMerged(conn, 1)
	idx, err := stores.meta.Index(context.Background(), testScope)
	if err != nil {
		t.Fatal(err)
	}
	if c := idx.Caches["cache-b"]; c != nil {
		t.Fatalf("the index has cache-b: %+v", c)
	}
	if len(h.logs.find("import: recording imports failed; trying again at the next sync")) > 0 {
		t.Fatal("the import path tried to record imports")
	}
}

// The debug export hook, the entries hook and POST /debug/gc answer 409 on a
// read-only service, with messages that say so, distinct from those of a
// service with no store.
func TestReadOnlyDebugHooksRefuse(t *testing.T) {
	stores := newSharedStores(t)
	writerStored(t, stores)
	h := newReadOnlyHarness(t, stores, readOnlyOptions())
	conn := h.connect(fakeengine.New("cache-b", 1))
	path := "/debug/engines/" + strconv.FormatUint(conn, 10)

	for _, tc := range []struct {
		name string
		do   func() (int, string)
		want string
	}{
		{"export", func() (int, string) { return h.post(path + "/export") }, ErrReadOnly.Error()},
		{"entries", func() (int, string) { return debugGet(t, h.debug.URL+path+"/entries") }, ErrReadOnly.Error()},
		{"gc", func() (int, string) { return h.post("/debug/gc") }, ErrGCReadOnly.Error()},
		{"gc dry run", func() (int, string) { return h.post("/debug/gc?dry-run") }, ErrGCReadOnly.Error()},
	} {
		code, body := tc.do()
		if code != http.StatusConflict || !strings.Contains(body, tc.want) || !strings.Contains(body, "read-only (-export=false)") {
			t.Errorf("%s: %d %q, want 409 %q", tc.name, code, body, tc.want)
		}
	}
	if _, err := h.svc.Export(context.Background(), conn, exporter.Request{}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Export: %v, want ErrReadOnly", err)
	}
	if _, err := h.svc.CollectGarbage(context.Background(), true); !errors.Is(err, ErrGCReadOnly) {
		t.Errorf("CollectGarbage: %v, want ErrGCReadOnly", err)
	}

	// A service with no store says so instead.
	plain := New(slog.New(slog.DiscardHandler), "")
	if _, err := plain.Export(context.Background(), 1, exporter.Request{}); !errors.Is(err, ErrExportOff) || errors.Is(err, ErrReadOnly) {
		t.Errorf("no store: Export: %v, want ErrExportOff", err)
	}
	if _, err := plain.CollectGarbage(context.Background(), false); !errors.Is(err, ErrGCOff) {
		t.Errorf("no store: CollectGarbage: %v, want ErrGCOff", err)
	}
}

func debugGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// heldListing is holdReads over refuseWrites, still listable, as the
// ownership check needs.
type heldListing struct {
	*holdReads
	refuseWrites *refuseWrites
}

func (h heldListing) ListKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	return h.refuseWrites.ListKeys(ctx, prefix, max)
}

func (h heldListing) ListUploadKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	return h.refuseWrites.ListUploadKeys(ctx, prefix, max)
}

// The drain of a read-only service, which has no export path, still waits
// for an explicit import admitted before it, up to its deadline
// (zerobull-consulting/dagger#72).
func TestReadOnlyDrainWaitsForExplicitImport(t *testing.T) {
	for _, hang := range []bool{false, true} {
		t.Run(map[bool]string{false: "ends", true: "hangs"}[hang], func(t *testing.T) {
			hold := &holdReads{held: make(chan struct{}), released: make(chan struct{})}
			h := newReadOnlyHarnessWrapped(t, newSharedStores(t), readOnlyOptions(), func(o metastore.Objects) metastore.Objects {
				hold.Objects = o
				return heldListing{holdReads: hold, refuseWrites: o.(*refuseWrites)}
			})
			var once sync.Once
			release := func() { once.Do(func() { close(hold.released) }) }
			defer release()
			conn := h.connect(fakeengine.New("cache-b", 1))
			done := make(chan error, 1)
			go func() {
				_, err := h.svc.Import(context.WithValue(context.Background(), holdKey{}, true), conn)
				done <- err
			}()
			select {
			case <-hold.held:
			case <-time.After(5 * time.Second):
				t.Fatal("the explicit import didn't reach its index read")
			}
			d := time.Minute
			if hang {
				d = 300 * time.Millisecond
			}
			drained := startDrain(t, h.svc, d)
			if hang {
				rep := waitDrain(t, drained, 5*time.Second)
				if rep.Outcome != metrics.DrainDeadline || rep.Explicit != 1 {
					t.Fatalf("drain report %+v", rep)
				}
				release()
				<-done
				return
			}
			select {
			case rep := <-drained:
				t.Fatalf("the drain didn't wait for the explicit import: %+v", rep)
			case <-time.After(300 * time.Millisecond):
			}
			if _, err := h.svc.Import(context.Background(), conn); !errors.Is(err, ErrDraining) {
				t.Fatalf("an explicit import during the drain: %v", err)
			}
			release()
			if err := <-done; err != nil {
				t.Fatalf("the explicit import: %v", err)
			}
			rep := waitDrain(t, drained, 5*time.Second)
			if rep.Outcome != metrics.DrainComplete || rep.Explicit != 0 {
				t.Fatalf("drain report %+v", rep)
			}
		})
	}
}

// The drain of a read-only service has nothing to flush: it returns at
// once, complete, and its log says why.
func TestReadOnlyDrainReturnsAtOnce(t *testing.T) {
	stores := newSharedStores(t)
	writerStored(t, stores)
	h := newReadOnlyHarness(t, stores, readOnlyOptions())
	b := fakeengine.New("cache-b", 1)
	conn := h.connect(b)
	h.waitMerged(conn, 1)
	b.Add(fakeengine.Entry{Number: 20, Layers: [][]byte{testLayer("new")}})
	h.postSpans(b, "session-1", cacheSpan(20))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	start := time.Now()
	rep := h.svc.Drain(ctx)
	if took := time.Since(start); took > time.Second {
		t.Errorf("the drain took %s", took)
	}
	if rep.Outcome != metrics.DrainComplete || len(rep.Left) != 0 || len(rep.Stored) != 0 {
		t.Errorf("drain report %+v", rep)
	}
	if len(h.logs.find("drain: nothing to export (read-only: -export=false)")) != 1 {
		t.Error("the drain didn't log why it had nothing to export")
	}
	if got := h.objs.attempted(); len(got) > 0 {
		t.Errorf("writes: %v", got)
	}
}

// GC is off on a read-only service: with the options -export=false passes
// (no GC), none runs, scheduled or asked for; GC options with NoExport are
// refused, and so is EnableGC on a read-only service.
func TestReadOnlyNoGC(t *testing.T) {
	stores := newSharedStores(t)
	writerStored(t, stores)
	h := newReadOnlyHarness(t, stores, readOnlyOptions())
	if h.svc.gc != nil {
		t.Fatal("gc is on")
	}
	if len(h.logs.find("gc off")) != 1 {
		t.Error("no gc off log")
	}
	if err := h.svc.EnableGC(context.Background(), GCConfig{Interval: time.Hour}); !errors.Is(err, ErrGCReadOnly) {
		t.Errorf("EnableGC: %v, want ErrGCReadOnly", err)
	}

	opts := readOnlyOptions()
	opts.GC = &GCConfig{Interval: time.Hour}
	svc := New(slog.New(slog.DiscardHandler), "")
	err := svc.enableStores(context.Background(), storeSet{Blobs: stores.blobs, Meta: stores.meta}, opts)
	if err == nil || !strings.Contains(err.Error(), "NoExport") {
		t.Errorf("GC with NoExport: %v", err)
	}
	opts = readOnlyOptions()
	opts.NoImport = true
	if err := New(slog.New(slog.DiscardHandler), "").enableStores(context.Background(), storeSet{Blobs: stores.blobs, Meta: stores.meta}, opts); err == nil {
		t.Error("NoExport with NoImport was taken")
	}
}

// cacheservice_export_enabled tells a read-only instance (0) from a writer
// (1), so a dashboard can tell a read-only sidecar from a broken one.
func TestReadOnlyExportEnabledMetric(t *testing.T) {
	stores := newSharedStores(t)
	h := newExportHarness(t, stores.blobs, stores.meta, nil)
	if got := metricstest.Read(t, h.svc.registry, "cacheservice_export_enabled", nil).Value; got != 1 {
		t.Errorf("writer: export_enabled %v, want 1", got)
	}
	ro := newReadOnlyHarness(t, stores, readOnlyOptions())
	if got := metricstest.Read(t, ro.svc.registry, "cacheservice_export_enabled", nil).Value; got != 0 {
		t.Errorf("read-only: export_enabled %v, want 0", got)
	}
}

// A read-only service takes only NoExport: the export path's conditional
// write check would write, so the options are refused before it runs.
func TestReadOnlyRefusesExportOptions(t *testing.T) {
	stores := newSharedStores(t)
	objs := &refuseWrites{Objects: stores.objs}
	meta, err := metastore.New(metastore.Config{Objects: objs})
	if err != nil {
		t.Fatal(err)
	}
	svc := New(slog.New(slog.DiscardHandler), "")
	if err := svc.SetReadOnly(); err != nil {
		t.Fatal(err)
	}
	opts := readOnlyOptions()
	opts.NoExport = false
	if err := svc.enableStores(context.Background(), storeSet{Blobs: stores.blobs, Meta: meta}, opts); err == nil {
		t.Fatal("a read-only service took the export path's options")
	}
	if got := objs.attempted(); len(got) > 0 {
		t.Fatalf("writes before the refusal: %v", got)
	}
}

// SetReadOnly after a path is on is refused: the paths would disagree.
func TestSetReadOnlyAfterEnable(t *testing.T) {
	stores := newSharedStores(t)
	h := newExportHarness(t, stores.blobs, stores.meta, nil)
	if err := h.svc.SetReadOnly(); err == nil {
		t.Fatal("SetReadOnly after EnableExport was taken")
	}
}
