package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// fakeEngine answers the exporter's requests the way the engine's session
// does: it holds an export's chains until its upload, PUTs the blobs the
// upload asks for, and answers too large exports with TooLargeMessage.
type fakeEngine struct {
	cache *fakeengine.Cache

	mu      sync.Mutex
	exports map[string]fakeengine.Capture
	nextID  int
	// uploads are the URL maps of the uploads received, in order.
	uploads []map[digest.Digest]string
	// replyErr, when set, answers every request with an error reply.
	replyErr string
	// inFlight and peak count exports between export and upload.
	inFlight, peak int
	// hold blocks each export's reply until closed.
	hold chan struct{}
	// mangle, when set, changes each capture before it is sent.
	mangle func(*fakeengine.Capture)
}

func newFakeEngine(cache *fakeengine.Cache) *fakeEngine {
	return &fakeEngine{cache: cache, exports: map[string]fakeengine.Capture{}}
}

func (e *fakeEngine) CacheID() string    { return e.cache.ID }
func (e *fakeEngine) Generation() uint64 { return e.cache.Generation }

func (e *fakeEngine) Request(ctx context.Context, typ protocol.Type, body json.RawMessage) (protocol.Envelope, error) {
	e.mu.Lock()
	replyErr, hold := e.replyErr, e.hold
	e.mu.Unlock()
	if replyErr != "" {
		return protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: replyErr})
	}
	switch typ {
	case protocol.TypeExport:
		var req protocol.Export
		if err := json.Unmarshal(body, &req); err != nil {
			return protocol.Envelope{}, err
		}
		capt, err := e.cache.Capture(req)
		if err != nil {
			return protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: err.Error()})
		}
		if capt.Bundle == nil {
			return protocol.NewEnvelope(1, 1, protocol.TypeExported, protocol.Exported{Skipped: capt.Skipped})
		}
		if e.mangle != nil {
			e.mangle(&capt)
		}
		e.mu.Lock()
		e.nextID++
		id := fmt.Sprintf("export-%d", e.nextID)
		e.exports[id] = capt
		e.inFlight++
		e.peak = max(e.peak, e.inFlight)
		e.mu.Unlock()
		if hold != nil {
			select {
			case <-hold:
			case <-ctx.Done():
				return protocol.Envelope{}, ctx.Err()
			}
		}
		return protocol.NewEnvelope(1, 1, protocol.TypeExported, protocol.Exported{ExportID: id, Bundle: capt.Bundle, Skipped: capt.Skipped})
	case protocol.TypeUpload:
		var req protocol.Upload
		if err := json.Unmarshal(body, &req); err != nil {
			return protocol.Envelope{}, err
		}
		e.mu.Lock()
		capt, ok := e.exports[req.ExportID]
		delete(e.exports, req.ExportID)
		if ok {
			e.inFlight--
		}
		e.uploads = append(e.uploads, req.URLs)
		e.mu.Unlock()
		if !ok {
			return protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: "no export " + req.ExportID})
		}
		reply := protocol.Uploaded{Done: []digest.Digest{}}
		for d, url := range req.URLs {
			if err := put(ctx, url, capt.Blobs[d]); err != nil {
				reply.Failed = append(reply.Failed, protocol.UploadFailure{Digest: d, Message: err.Error()})
				continue
			}
			reply.Done = append(reply.Done, d)
		}
		return protocol.NewEnvelope(1, 1, protocol.TypeUploaded, reply)
	}
	return protocol.Envelope{}, fmt.Errorf("unexpected request %s", typ)
}

func put(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return errors.New(resp.Status)
	}
	return nil
}

func (e *fakeEngine) uploadMaps() []map[digest.Digest]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.uploads)
}

type testEnv struct {
	blobs *blobstore.MemStore
	meta  *metastore.Store
	x     *Exporter
	cache *fakeengine.Cache
	eng   *fakeEngine
}

func newTestEnv(t *testing.T, modify func(*Config)) *testEnv {
	t.Helper()
	blobs := blobstore.NewMemStore("")
	srv := httptest.NewServer(blobs)
	t.Cleanup(srv.Close)
	blobs.SetBaseURL(srv.URL)
	meta, err := metastore.New(metastore.Config{Objects: metastore.NewMemObjects()})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Blobs: blobs, Meta: meta, Scope: "team"}
	if modify != nil {
		modify(&cfg)
	}
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	x.sleep = func(context.Context, time.Duration) error { return nil }
	cache := fakeengine.New("cache-a", 2)
	return &testEnv{blobs: blobs, meta: meta, x: x, cache: cache, eng: newFakeEngine(cache)}
}

func fsOutputs(numbers ...uint64) []protocol.ExportOutput {
	var out []protocol.ExportOutput
	for _, n := range numbers {
		out = append(out, protocol.ExportOutput{Number: n, Address: fakeengine.FSPart})
	}
	return out
}

func layer(s string) []byte { return []byte(strings.Repeat(s, 64)) }

// A build's entries: a base image (10), and an exec on it (11).
func (e *testEnv) addBuild() {
	e.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{layer("base")}})
	e.cache.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Layers: [][]byte{layer("base"), layer("exec")}})
}

func TestExportStoresBlobsAndBundle(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	ctx := context.Background()

	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{11}) || len(res.Bundles) != 1 || len(res.Failed) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	for _, l := range [][]byte{layer("base"), layer("exec")} {
		if got, ok := e.blobs.Get(digest.FromBytes(l)); !ok || !bytes.Equal(got, l) {
			t.Fatalf("layer %s not stored", digest.FromBytes(l))
		}
	}
	if len(res.Uploaded) != 2 || len(res.Present) != 0 {
		t.Fatalf("uploaded %v, present %v", res.Uploaded, res.Present)
	}
	// The bundle and its values are recorded.
	bundle, err := e.meta.GetBundle(ctx, "team", res.Bundles[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Values) != 2 || len(bundle.Outputs) != 2 {
		t.Fatalf("bundle: %d values, %d outputs", len(bundle.Values), len(bundle.Outputs))
	}
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 2 || len(idx.Roots) != 1 {
		t.Fatalf("index: %d entries, %d roots", len(idx.Entries), len(idx.Roots))
	}
	mapping := idx.Caches["cache-a"]
	if mapping == nil || mapping.Generation != 2 || mapping.Numbers[11].Number == 0 {
		t.Fatalf("index mapping: %+v", mapping)
	}
}

func TestRepeatedExportUploadsNothingNew(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	ctx := context.Background()
	req := Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)}
	first, err := e.x.Export(ctx, e.eng, req)
	if err != nil {
		t.Fatal(err)
	}
	puts := e.blobs.Puts()

	second, err := e.x.Export(ctx, e.eng, req)
	if err != nil {
		t.Fatal(err)
	}
	if e.blobs.Puts() != puts {
		t.Fatalf("repeat export uploaded %d blobs", e.blobs.Puts()-puts)
	}
	if len(second.Uploaded) != 0 || len(second.Present) != 2 {
		t.Fatalf("second export: uploaded %v, present %v", second.Uploaded, second.Present)
	}
	// The repeat sent an empty URL map, which released the chains.
	uploads := e.eng.uploadMaps()
	if len(uploads) != 2 || len(uploads[1]) != 0 {
		t.Fatalf("uploads: %v", uploads)
	}
	// The same bundle, the same service numbers.
	if !slices.Equal(first.Bundles, second.Bundles) {
		t.Fatalf("bundles: %v then %v", first.Bundles, second.Bundles)
	}
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 2 || idx.NextNumber != 3 {
		t.Fatalf("index after repeat: %d entries, next %d", len(idx.Entries), idx.NextNumber)
	}
}

func TestExportSharedLayerUploadedOnce(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.cache.Add(fakeengine.Entry{Number: 12, Deps: []uint64{10}, Layers: [][]byte{layer("base"), layer("other")}})
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11, 12}, Outputs: fsOutputs(10, 11, 12)})
	if err != nil {
		t.Fatal(err)
	}
	// base, exec, other: base once, though three outputs list it.
	if len(res.Uploaded) != 3 || e.blobs.Puts() != 3 {
		t.Fatalf("uploaded %v, puts %d", res.Uploaded, e.blobs.Puts())
	}
}

func TestExportSkippedRoots(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.cache.Add(fakeengine.Entry{Number: 20, Busy: true})
	e.cache.Add(fakeengine.Entry{Number: 21, Refused: "no codec"})
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11, 20, 21, 99}, Outputs: fsOutputs(11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{11}) {
		t.Fatalf("stored %v", res.Stored)
	}
	if !slices.Equal(res.Gone, []uint64{99}) {
		t.Fatalf("gone %v", res.Gone)
	}
	if len(res.Busy) != 1 || res.Busy[0].Number != 20 {
		t.Fatalf("busy %v", res.Busy)
	}
	if len(res.Refused) != 1 || res.Refused[0].Number != 21 || res.Refused[0].Message != "no codec" {
		t.Fatalf("refused %v", res.Refused)
	}
}

func TestExportNothingSurvivedSendsNoUpload(t *testing.T) {
	e := newTestEnv(t, nil)
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Gone, []uint64{1, 2}) || len(res.Stored) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if n := len(e.eng.uploadMaps()); n != 0 {
		t.Fatalf("%d uploads sent", n)
	}
}

func TestExportRetriesFailedUploads(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	exec := digest.FromBytes(layer("exec"))
	e.blobs.FailPuts(exec, 1)

	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{11}) || len(res.Failed) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	uploads := e.eng.uploadMaps()
	if len(uploads) != 2 {
		t.Fatalf("%d uploads, want 2", len(uploads))
	}
	// The retry asks only for the blob that failed.
	if _, ok := uploads[1][exec]; !ok || len(uploads[1]) != 1 {
		t.Fatalf("retry asked for %v", uploads[1])
	}
	if e.cache.Exports() != 2 {
		t.Fatalf("%d exports, want 2: the engine releases the chains with its answer", e.cache.Exports())
	}
}

func TestExportGivesUpOnFailingUploads(t *testing.T) {
	e := newTestEnv(t, func(c *Config) { c.UploadAttempts = 2 })
	e.addBuild()
	e.blobs.FailPuts(digest.FromBytes(layer("exec")), 100)
	ctx := context.Background()

	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stored) != 0 || len(res.Failed) != 1 || res.Failed[0].Number != 11 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if e.cache.Exports() != 2 {
		t.Fatalf("%d exports, want 2", e.cache.Exports())
	}
	// Nothing is published while a blob is missing.
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 0 {
		t.Fatalf("index has %d entries", len(idx.Entries))
	}
}

func TestExportSplitsTooLarge(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.cache.Add(fakeengine.Entry{Number: 12, Layers: [][]byte{layer("twelve")}})
	e.cache.Add(fakeengine.Entry{Number: 13, Layers: [][]byte{layer("thirteen")}})
	e.cache.MaxRoots = 1

	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{13, 11, 12}, Outputs: fsOutputs(10, 11, 12, 13)})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res.Stored)
	if !slices.Equal(res.Stored, []uint64{11, 12, 13}) || len(res.Bundles) != 3 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(res.Uploaded) != 4 {
		t.Fatalf("uploaded %v", res.Uploaded)
	}
}

func TestExportOneRootTooLarge(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	// Even one root's reply is too large.
	e.eng.replyErr = protocol.TooLargeMessage
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Failed) != 2 || len(res.Stored) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	for _, f := range res.Failed {
		if !strings.Contains(f.Error, "message limit") {
			t.Fatalf("failure: %q", f.Error)
		}
	}
}

func TestExportUploadRequestTooLarge(t *testing.T) {
	// Room for an export request and an upload of one URL, not two.
	e := newTestEnv(t, func(c *Config) { c.MaxMessageBytes = 520 })
	e.cache.Add(fakeengine.Entry{Number: 1, Layers: [][]byte{layer("one")}})
	e.cache.Add(fakeengine.Entry{Number: 2, Layers: [][]byte{layer("two")}})

	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{1, 2}, Outputs: fsOutputs(1, 2)})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res.Stored)
	if !slices.Equal(res.Stored, []uint64{1, 2}) || len(res.Uploaded) != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	uploads := e.eng.uploadMaps()
	// The first, too large, upload was never sent; its export was released
	// with an empty one. Then each half uploaded its blob.
	if len(uploads) != 3 || len(uploads[0]) != 0 || len(uploads[1]) != 1 || len(uploads[2]) != 1 {
		t.Fatalf("uploads: %v", uploads)
	}
}

func TestExportBlobStoreFailureReleases(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.x.cfg.Blobs = failingStore{}
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Failed) != 1 || !strings.Contains(res.Failed[0].Error, "blob store") {
		t.Fatalf("unexpected result: %+v", res)
	}
	uploads := e.eng.uploadMaps()
	if len(uploads) != 1 || len(uploads[0]) != 0 {
		t.Fatalf("the export wasn't released: %v", uploads)
	}
}

// A bundle listing one layer with two sizes fails as invalid, not as a blob
// store failure, and its chains are released.
func TestExportInvalidBundleReleases(t *testing.T) {
	e := newTestEnv(t, nil)
	e.cache.Add(fakeengine.Entry{Number: 1, Layers: [][]byte{layer("a")}})
	e.cache.Add(fakeengine.Entry{Number: 2, Layers: [][]byte{layer("a")}})
	e.eng.mangle = func(c *fakeengine.Capture) {
		c.Bundle.Outputs[1].Chain.Layers[0].Descriptor.Size++
	}
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{1, 2}, Outputs: fsOutputs(1, 2)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Failed) != 2 || !strings.HasPrefix(res.Failed[0].Error, "invalid bundle:") {
		t.Fatalf("unexpected result: %+v", res)
	}
	if uploads := e.eng.uploadMaps(); len(uploads) != 1 || len(uploads[0]) != 0 {
		t.Fatalf("the export wasn't released: %v", uploads)
	}
}

type failingStore struct{ blobstore.Store }

func (failingStore) Has(context.Context, digest.Digest) (bool, error) {
	return false, errors.New("s3 down")
}

func TestExportEngineError(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.eng.replyErr = "cache closed"
	_, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{11}})
	if err == nil || !strings.Contains(err.Error(), "cache closed") {
		t.Fatalf("got %v", err)
	}
}

func TestExportsInFlightPerEngine(t *testing.T) {
	e := newTestEnv(t, nil)
	for n := uint64(1); n <= 10; n++ {
		e.cache.Add(fakeengine.Entry{Number: n, Layers: [][]byte{layer(fmt.Sprint(n))}})
	}
	hold := make(chan struct{})
	e.eng.hold = hold
	var wg sync.WaitGroup
	var stored atomic.Int64
	for n := uint64(1); n <= 10; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{n}, Outputs: fsOutputs(n)})
			if err == nil {
				stored.Add(int64(len(res.Stored)))
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.cache.Exports() < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := e.cache.Exports(); got != 4 {
		t.Fatalf("%d exports in flight, want 4", got)
	}
	close(hold)
	wg.Wait()
	if stored.Load() != 10 {
		t.Fatalf("stored %d", stored.Load())
	}
	if e.eng.peak > 4 {
		t.Fatalf("peak %d exports in flight", e.eng.peak)
	}
}

func TestExportCancelled(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.eng.hold = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if _, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("no stores accepted")
	}
	meta, _ := metastore.New(metastore.Config{Objects: metastore.NewMemObjects()})
	if _, err := New(Config{Blobs: blobstore.NewMemStore(""), Meta: meta, Scope: "blobs"}); err == nil {
		t.Fatal("reserved scope accepted")
	}
}

func TestNeededBlobsConflictingSizes(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	capt, err := e.cache.Capture(protocol.Export{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := neededBlobs(capt.Bundle)
	if err != nil || len(blobs) != 2 {
		t.Fatalf("blobs %v, err %v", blobs, err)
	}
	capt.Bundle.Outputs[0].Chain.Layers[0].Descriptor.Size++
	if _, err := neededBlobs(capt.Bundle); err == nil {
		t.Fatal("conflicting sizes accepted")
	}
}
