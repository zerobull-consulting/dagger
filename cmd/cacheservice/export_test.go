package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

const testScope = "team"

// exportHarness is the service with its export path on, and a fake engine
// cache connected to it through the engine's own remotecache.Run.
type exportHarness struct {
	t      *testing.T
	svc    *service
	srv    *httptest.Server
	debug  *httptest.Server
	cache  *fakeengine.Cache
	blobs  blobstore.Store
	meta   *metastore.Store
	engine *engineConn
}

func newExportHarness(t *testing.T, blobs blobstore.Store, meta *metastore.Store, modify func(*exportConfig)) *exportHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	svc := newService(slog.New(slog.DiscardHandler), "")
	cfg := exportConfig{
		Exporter: exporter.Config{Blobs: blobs, Meta: meta, Scope: testScope, RetryBackoff: time.Millisecond},
		Tracker:  exporter.TrackerConfig{QuietPeriod: time.Minute, EndGrace: 50 * time.Millisecond},
		Tick:     10 * time.Millisecond,
	}
	if modify != nil {
		modify(&cfg)
	}
	err := svc.enableExport(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(svc.handler())
	debug := httptest.NewServer(svc.debugHandler())
	cache := fakeengine.New("cache-a", 4)
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: cache}, remotecache.Config{URL: srv.URL, Token: "engine-token", EngineName: "engine-a"})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		svc.waitExports()
		srv.Close()
		debug.Close()
	})
	h := &exportHarness{t: t, svc: svc, srv: srv, debug: debug, cache: cache, blobs: blobs, meta: meta}
	h.engine = waitEngines(t, svc, 1)[0]
	return h
}

// newMemExportHarness uses in-memory stores.
func newMemExportHarness(t *testing.T, modify func(*exportConfig)) (*exportHarness, *blobstore.MemStore) {
	t.Helper()
	blobs := blobstore.NewMemStore("")
	blobSrv := httptest.NewServer(blobs)
	t.Cleanup(blobSrv.Close)
	blobs.SetBaseURL(blobSrv.URL)
	meta, err := metastore.New(metastore.Config{Objects: metastore.NewMemObjects()})
	if err != nil {
		t.Fatal(err)
	}
	return newExportHarness(t, blobs, meta, modify), blobs
}

func testLayer(s string) []byte { return []byte(strings.Repeat(s, 512)) }

// build adds a build's entries: a base image (10) and an exec on it (11).
func (h *exportHarness) build() []digest.Digest {
	base, exec := testLayer("base"), testLayer("exec")
	h.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{base}})
	h.cache.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Layers: [][]byte{base, exec}})
	return []digest.Digest{digest.FromBytes(base), digest.FromBytes(exec)}
}

func stringsKV(k string, vs ...string) *commonpb.KeyValue {
	values := make([]*commonpb.AnyValue, len(vs))
	for i, v := range vs {
		values[i] = strValue(v)
	}
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: values}},
	}}
}

// cacheSpan is a call span of a retained entry with an fs part.
func cacheSpan(number uint64, deps ...string) *tracepb.Span {
	return &tracepb.Span{
		Name:            "call",
		EndTimeUnixNano: uint64(time.Now().UnixNano()),
		Attributes: []*commonpb.KeyValue{
			kv(telemetryattrs.CacheResultIDAttr, strconv.FormatUint(number, 10)),
			kv(telemetryattrs.CacheOutcomeAttr, "executed"),
			kv(telemetryattrs.CacheTypeAttr, "Container"),
			kv(telemetryattrs.CacheRetainedAttr, "true"),
			stringsKV(telemetryattrs.CacheDepsAttr, deps...),
			stringsKV(telemetryattrs.CachePartsAttr, `{"part":"fs"}`),
		},
	}
}

func carrierSpan(cacheSpans int) *tracepb.Span {
	return &tracepb.Span{Name: "session complete", Attributes: []*commonpb.KeyValue{
		kv(telemetryattrs.CacheSessionSpansAttr, strconv.Itoa(cacheSpans)),
	}}
}

// postSpans posts a session's spans as the engine forwards them.
func (h *exportHarness) postSpans(session string, spans ...*tracepb.Span) {
	h.t.Helper()
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			kv(telemetryattrs.EngineCacheAttr, h.cache.ID+"/"+strconv.FormatUint(h.cache.Generation, 10)),
			kv(telemetryattrs.EngineSessionAttr, session),
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
	if code := otlpPost(h.t, h.srv.URL+"/v1/traces", req, &coltracepb.ExportTraceServiceResponse{}, false, false); code != http.StatusOK {
		h.t.Fatalf("status %d", code)
	}
}

func (h *exportHarness) postEvent(kind, body string) {
	h.t.Helper()
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: telemetryattrs.EngineEventScope},
			LogRecords: []*logspb.LogRecord{{
				Body: strValue(body),
				Attributes: []*commonpb.KeyValue{
					kv(telemetryattrs.EngineEventAttr, kind),
					kv(telemetryattrs.EngineCacheAttr, h.cache.ID+"/"+strconv.FormatUint(h.cache.Generation, 10)),
				},
			}},
		}},
	}}}
	if code := otlpPost(h.t, h.srv.URL+"/v1/logs", req, &collogspb.ExportLogsServiceResponse{}, false, false); code != http.StatusOK {
		h.t.Fatalf("status %d", code)
	}
}

// waitIndex waits until the scope's index has an entry for each engine
// number, and returns it.
func (h *exportHarness) waitIndex(numbers ...uint64) *metastore.Index {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		idx, err := h.meta.Index(context.Background(), testScope)
		if err != nil {
			h.t.Fatal(err)
		}
		if c := idx.Caches[h.cache.ID]; c != nil {
			all := true
			for _, n := range numbers {
				if _, ok := c.Numbers[n]; !ok {
					all = false
				}
			}
			if all {
				return idx
			}
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for entries %v in the index", numbers)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// debugExport calls the explicit export hook.
func (h *exportHarness) debugExport(body string) (int, exporter.Result) {
	h.t.Helper()
	url := h.debug.URL + "/debug/engines/" + strconv.FormatUint(h.engine.id, 10) + "/export"
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var res exporter.Result
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, &res); err != nil {
			h.t.Fatalf("%s: %v", data, err)
		}
	}
	return resp.StatusCode, res
}

// hasAll reports whether the store has every digest.
func hasAll(t *testing.T, store blobstore.Store, digests []digest.Digest) bool {
	t.Helper()
	for _, d := range digests {
		ok, err := store.Has(context.Background(), d)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return false
		}
	}
	return true
}

// After a build's session ends, its retained entries' layers and bundle are
// in the store, and the index maps them. Exporting them again uploads
// nothing.
func TestExportAfterSessionEnd(t *testing.T) {
	h, blobs := newMemExportHarness(t, nil)
	layers := h.build()

	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	h.postSpans("session-1", carrierSpan(2))
	idx := h.waitIndex(10, 11)

	if !hasAll(t, blobs, layers) {
		t.Fatal("layers not stored")
	}
	entry := idx.Entries[idx.Caches[h.cache.ID].Numbers[11].Number]
	bundle, err := h.meta.GetBundle(context.Background(), testScope, entry.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Outputs) == 0 || bundle.Outputs[0].Chain == nil {
		t.Fatalf("bundle has no outputs: %+v", bundle)
	}
	if idx.Caches[h.cache.ID].Generation != h.cache.Generation {
		t.Fatalf("recorded generation %d", idx.Caches[h.cache.ID].Generation)
	}
	puts := blobs.Puts()
	if puts != 2 {
		t.Fatalf("%d PUTs, want 2", puts)
	}

	// The same entries in a later session: already stored, not exported.
	exports := h.cache.Exports()
	h.postSpans("session-2", cacheSpan(11, "10"), carrierSpan(1))
	time.Sleep(200 * time.Millisecond)
	if h.cache.Exports() != exports {
		t.Fatal("stored entries exported again")
	}

	// Asked explicitly, the repeat export uploads nothing new, and stores
	// the same bundle.
	code, res := h.debugExport(`{"roots":[10,11],"outputs":[{"number":10,"address":{"part":"fs"}},{"number":11,"address":{"part":"fs"}}]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if !slices.Equal(res.Stored, []uint64{10, 11}) || len(res.Uploaded) != 0 || len(res.Present) != 2 {
		t.Fatalf("repeat export: %+v", res)
	}
	if !slices.Equal(res.Bundles, []string{entry.Bundle}) {
		t.Fatalf("repeat export stored bundle %v, want %s", res.Bundles, entry.Bundle)
	}
	if blobs.Puts() != puts {
		t.Fatalf("repeat export uploaded %d blobs", blobs.Puts()-puts)
	}
	// The explicit export is recorded in the tracker.
	for _, e := range h.svc.export.tracker.Entries(h.cache.ID) {
		if !e.Stored {
			t.Fatalf("entry %d not recorded as stored", e.Number)
		}
	}
}

// The explicit hook with no roots exports every tracked entry worth it.
func TestExportHookExportsTracked(t *testing.T) {
	h, blobs := newMemExportHarness(t, nil)
	layers := h.build()
	h.cache.Add(fakeengine.Entry{Number: 12, Busy: true})
	// No carrier, and the quiet period is long: nothing is due yet.
	h.postSpans("session-1", cacheSpan(11, "10"), cacheSpan(12))

	code, res := h.debugExport("")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if !slices.Equal(res.Stored, []uint64{11}) || len(res.Busy) != 1 || res.Busy[0].Number != 12 {
		t.Fatalf("result %+v", res)
	}
	if !hasAll(t, blobs, layers) {
		t.Fatal("layers not stored")
	}

	resp, err := http.Get(h.debug.URL + "/debug/engines/" + strconv.FormatUint(h.engine.id, 10) + "/entries")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var entries []exporter.EntryState
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || !entries[0].Stored || entries[1].Stored || entries[1].RetryAt == "" {
		t.Fatalf("entries %+v", entries)
	}
}

// Busy roots are exported again once they settle.
func TestExportRetriesBusy(t *testing.T) {
	h, _ := newMemExportHarness(t, func(c *exportConfig) { c.Tracker.BusyBackoff = 50 * time.Millisecond })
	h.build()
	h.cache.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Busy: true, Layers: [][]byte{testLayer("base")}})
	h.postSpans("session-1", cacheSpan(11, "10"), carrierSpan(1))

	deadline := time.Now().Add(5 * time.Second)
	for h.svc.export.tracker.Entries(h.cache.ID)[0].RetryAt == "" {
		if time.Now().After(deadline) {
			t.Fatal("busy root not scheduled for a retry")
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.build() // no longer busy
	h.waitIndex(11)
}

// A pruned entry isn't exported; a wiped cache is forgotten.
func TestExportEngineEvents(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	h.build()
	h.postSpans("session-1", cacheSpan(11, "10"))
	h.postEvent(telemetryattrs.EngineEventPrune, `{"drops":[{"resultId":11,"droppedAtUnixNano":`+strconv.FormatInt(time.Now().Add(time.Hour).UnixNano(), 10)+`}]}`)
	if e := h.svc.export.tracker.Entries(h.cache.ID); len(e) != 1 || e[0].Retained {
		t.Fatalf("entries after prune: %+v", e)
	}
	h.postEvent(telemetryattrs.EngineEventStart, `{"wipedCache":"cache-a"}`)
	// The start is of the same cache ID, so nothing is wiped.
	if e := h.svc.export.tracker.Entries(h.cache.ID); len(e) != 1 {
		t.Fatalf("entries after start: %+v", e)
	}
	// Undecodable events are ignored.
	h.postEvent(telemetryattrs.EngineEventPrune, `{`)
	h.postEvent(telemetryattrs.EngineEventStart, `{`)
}

// A session whose engine isn't connected waits for it.
func TestExportWaitsForEngine(t *testing.T) {
	h, _ := newMemExportHarness(t, func(c *exportConfig) { c.NoEngineWait = 20 * time.Millisecond })
	other := &exportHarness{t: t, srv: h.srv, cache: fakeengine.New("cache-b", 1)}
	other.postSpans("session-1", cacheSpan(5), carrierSpan(1))
	time.Sleep(100 * time.Millisecond)
	e := h.svc.export.tracker.Entries("cache-b")
	if len(e) != 1 || e[0].Stored || e[0].Failures != 0 {
		t.Fatalf("entries %+v", e)
	}
}

func TestExportHookOff(t *testing.T) {
	svc := newService(slog.New(slog.DiscardHandler), "")
	debug := httptest.NewServer(svc.debugHandler())
	defer debug.Close()
	for _, req := range []struct{ method, path string }{
		{http.MethodPost, "/debug/engines/1/export"},
		{http.MethodGet, "/debug/engines/1/entries"},
	} {
		r, _ := http.NewRequest(req.method, debug.URL+req.path, bytes.NewReader(nil))
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("%s %s: status %d", req.method, req.path, resp.StatusCode)
		}
	}
}

func TestExportHookErrors(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	for _, c := range []struct {
		path, body string
		want       int
	}{
		{"/debug/engines/x/export", "", http.StatusBadRequest},
		{"/debug/engines/99/export", "", http.StatusNotFound},
		{"/debug/engines/" + strconv.FormatUint(h.engine.id, 10) + "/export", "{", http.StatusBadRequest},
	} {
		resp, err := http.Post(h.debug.URL+c.path, "application/json", strings.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Fatalf("%s: status %d, want %d", c.path, resp.StatusCode, c.want)
		}
	}
}

func TestParseEngineCache(t *testing.T) {
	for _, c := range []struct {
		in  string
		id  string
		gen uint64
		ok  bool
	}{
		{"abc/3", "abc", 3, true},
		{"a/b/7", "a/b", 7, true},
		{"abc", "", 0, false},
		{"/3", "", 0, false},
		{"abc/x", "", 0, false},
	} {
		id, gen, ok := parseEngineCache(c.in)
		if id != c.id || gen != c.gen || ok != c.ok {
			t.Errorf("%q: got %q %d %v", c.in, id, gen, ok)
		}
	}
}

// When one of the hook's batches fails, the batches after it go back to the
// scheduler instead of staying in flight.
func TestExportHookRequeuesBatchesNotRun(t *testing.T) {
	h, _ := newMemExportHarness(t, func(c *exportConfig) { c.Tick = time.Hour })
	// 1 depends on an entry the cache doesn't have, so the first batch's
	// capture fails.
	h.cache.Add(fakeengine.Entry{Number: 1, Deps: []uint64{9999}})
	var spans []*tracepb.Span
	for n := uint64(1); n <= 1001; n++ {
		if n > 1 {
			h.cache.Add(fakeengine.Entry{Number: n, Layers: [][]byte{testLayer(strconv.FormatUint(n, 10))}})
		}
		spans = append(spans, cacheSpan(n))
	}
	h.postSpans("session-1", spans...)

	if code, _ := h.debugExport(""); code != http.StatusBadGateway {
		t.Fatalf("status %d", code)
	}
	entries := h.svc.export.tracker.Entries(h.cache.ID)
	last := entries[len(entries)-1]
	if last.Number != 1001 || last.RetryAt == "" || last.Failures != 0 {
		t.Fatalf("batch not run wasn't requeued: %+v", last)
	}
	// Due again, so a scheduler tick runs it.
	if b := h.svc.export.tracker.Due(time.Now()); len(b) == 0 {
		t.Fatal("requeued batch not due")
	}
}
