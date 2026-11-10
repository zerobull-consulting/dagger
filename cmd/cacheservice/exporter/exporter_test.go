package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
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
	// noPut lists blobs the engine reports done without uploading them.
	noPut map[digest.Digest]bool
	// onUpload, when set, runs when an upload that asks for blobs arrives,
	// before the engine answers it.
	onUpload func()
	// cutUploads, when set, fails every blob but the first of each upload
	// with it, unsent, as an upload the engine cut off does.
	cutUploads string
	// errOnce answers the next request of a type with an error reply, once;
	// an upload so answered finds its export dropped. pauseAfter then makes
	// the engine refuse every root of its later exports as paused.
	errOnce    map[protocol.Type]string
	pauseAfter bool
	paused     bool
	// requests counts the requests received, by type.
	requests map[protocol.Type]int
	// hangUploads leaves every upload that asks for blobs unanswered.
	hangUploads bool
	// tooLargeAfter, when set, answers the export request after that many
	// with TooLargeMessage, once.
	tooLargeAfter int
}

func newFakeEngine(cache *fakeengine.Cache) *fakeEngine {
	return &fakeEngine{cache: cache, exports: map[string]fakeengine.Capture{}}
}

func (e *fakeEngine) CacheID() string    { return e.cache.ID }
func (e *fakeEngine) Generation() uint64 { return e.cache.Generation }

func (e *fakeEngine) Request(ctx context.Context, typ protocol.Type, body json.RawMessage) (protocol.Envelope, error) {
	e.mu.Lock()
	replyErr, hold := e.replyErr, e.hold
	if e.requests == nil {
		e.requests = map[protocol.Type]int{}
	}
	e.requests[typ]++
	once, cutOnce := e.errOnce[typ]
	if cutOnce {
		delete(e.errOnce, typ)
		e.paused = e.pauseAfter
		if typ == protocol.TypeUpload {
			e.exports = map[string]fakeengine.Capture{}
			e.inFlight = 0
		}
	}
	paused := e.paused
	tooLarge := typ == protocol.TypeExport && e.tooLargeAfter > 0 && e.requests[typ] == e.tooLargeAfter+1
	if tooLarge {
		e.tooLargeAfter = 0
	}
	e.mu.Unlock()
	if tooLarge {
		return protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: protocol.TooLargeMessage})
	}
	if replyErr != "" {
		return protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: replyErr})
	}
	if cutOnce {
		return protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: once})
	}
	if paused && typ == protocol.TypeExport {
		var req protocol.Export
		if err := json.Unmarshal(body, &req); err != nil {
			return protocol.Envelope{}, err
		}
		skipped := make([]protocol.SkippedRoot, len(req.Roots))
		for i, n := range req.Roots {
			skipped[i] = protocol.SkippedRoot{Number: n, Reason: protocol.SkipRefused, Message: EnginePausedMessage}
		}
		return protocol.NewEnvelope(1, 1, protocol.TypeExported, protocol.Exported{Skipped: skipped})
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
		onUpload, cut, hang := e.onUpload, e.cutUploads, e.hangUploads
		if cut != "" && e.pauseAfter {
			e.paused = true
		}
		e.mu.Unlock()
		if onUpload != nil && len(req.URLs) > 0 {
			onUpload()
		}
		if hang && len(req.URLs) > 0 {
			<-ctx.Done()
			return protocol.Envelope{}, ctx.Err()
		}
		if !ok {
			return protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: "no export " + req.ExportID})
		}
		reply := protocol.Uploaded{Done: []digest.Digest{}}
		for i, d := range slices.Sorted(maps.Keys(req.URLs)) {
			url := req.URLs[d]
			if cut != "" && i > 0 {
				reply.Failed = append(reply.Failed, protocol.UploadFailure{Digest: d, Message: cut})
				continue
			}
			e.mu.Lock()
			skip := e.noPut[d]
			e.mu.Unlock()
			if skip {
				reply.Done = append(reply.Done, d)
				continue
			}
			if err := put(ctx, url, e.cache.UploadBody(d, capt.Blobs[d])); err != nil {
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
	blobs   *blobstore.MemStore
	objects *metastore.MemObjects
	meta    *metastore.Store
	x       *Exporter
	cache   *fakeengine.Cache
	eng     *fakeEngine
}

func newTestEnv(t *testing.T, modify func(*Config)) *testEnv {
	t.Helper()
	blobs := blobstore.NewMemStore("")
	srv := httptest.NewServer(blobs)
	t.Cleanup(srv.Close)
	blobs.SetBaseURL(srv.URL)
	objects := metastore.NewMemObjects()
	meta, err := metastore.New(metastore.Config{Objects: objects})
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
	return &testEnv{blobs: blobs, objects: objects, meta: meta, x: x, cache: cache, eng: newFakeEngine(cache)}
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

// A real engine annotates a layer with buildkit/createdat only once it has
// compressed it, so its first export of an exec's layers leaves the
// annotation out and the next one has it (#30). The repeat must still store
// the same bundle: a new one would move the entries in the index and be
// merged again into every engine.
func TestRepeatedExportCreatedAtAnnotationSameBundle(t *testing.T) {
	e := newTestEnv(t, nil)
	e.cache.CreatedAtAnnotation = true
	e.addBuild()
	ctx := context.Background()
	req := Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)}
	first, err := e.x.Export(ctx, e.eng, req)
	if err != nil {
		t.Fatal(err)
	}
	keys := slices.Sorted(slices.Values(e.objects.Keys()))
	second, err := e.x.Export(ctx, e.eng, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Bundles) != 1 || !slices.Equal(first.Bundles, second.Bundles) {
		t.Fatalf("bundles: %v then %v", first.Bundles, second.Bundles)
	}
	if got := slices.Sorted(slices.Values(e.objects.Keys())); !slices.Equal(got, keys) {
		t.Fatalf("objects: %v, then %v", keys, got)
	}
	// What is stored has no annotation, and keeps the layers' creation time.
	b, err := e.meta.GetBundle(ctx, "team", first.Bundles[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range b.Outputs {
		for _, l := range o.Chain.Layers {
			if len(l.Descriptor.Annotations) != 0 || l.CreatedAt == nil {
				t.Fatalf("stored layer %s: annotations %v, createdAt %v", l.Descriptor.Digest, l.Descriptor.Annotations, l.CreatedAt)
			}
		}
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

// mergeSize is what the exporter estimates for the merge of an export of
// roots, with an fs output each.
func (e *testEnv) mergeSize(t *testing.T, roots ...uint64) int {
	t.Helper()
	capt, err := e.cache.Capture(protocol.Export{Roots: roots, Outputs: fsOutputs(roots...)})
	if err != nil {
		t.Fatal(err)
	}
	stripVolatile(capt.Bundle)
	size, err := importer.MergeSize(capt.Bundle, e.x.addressSize(context.Background()), "team")
	if err != nil {
		t.Fatal(err)
	}
	return size
}

// manyLayers is a chain of n distinct layers, as a deep image has: its
// merge carries a presigned GET for each.
func manyLayers(name string, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = layer(fmt.Sprintf("%s-%d", name, i))
	}
	return out
}

// The import path sends a stored bundle whole, with a presigned GET per
// layer of each chain, and can't split it (#12). An export whose bundle
// would make a merge over the import path's limit is split like one too
// large for a message, and each half, under the limit, is stored.
func TestExportSplitsBundleTooLargeToImport(t *testing.T) {
	probe := newTestEnv(t, nil)
	add := func(e *testEnv) {
		e.cache.Add(fakeengine.Entry{Number: 1, Layers: manyLayers("one", 8)})
		e.cache.Add(fakeengine.Entry{Number: 2, Layers: manyLayers("two", 8)})
	}
	add(probe)
	one, two, both := probe.mergeSize(t, 1), probe.mergeSize(t, 2), probe.mergeSize(t, 1, 2)
	limit := both - 1
	if one > limit || two > limit {
		t.Fatalf("merge sizes: %d and %d alone, %d together", one, two, both)
	}

	e := newTestEnv(t, func(c *Config) { c.MaxMergeBytes = limit })
	add(e)
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{1, 2}, Outputs: fsOutputs(1, 2)})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res.Stored)
	if !slices.Equal(res.Stored, []uint64{1, 2}) || len(res.Bundles) != 2 || len(res.Failed) != 0 {
		t.Fatalf("want both roots stored, each in its own bundle: %+v", res)
	}
	// The first export, of both, was released without an upload of any
	// blob: an empty upload, then one per half.
	uploads := e.eng.uploadMaps()
	if len(uploads) != 3 || len(uploads[0]) != 0 || len(uploads[1]) != 8 || len(uploads[2]) != 8 {
		t.Fatalf("uploads of %v blobs, want 0 (the release), 8 and 8", mapSizes(uploads))
	}
	if e.cache.InFlight() != 0 {
		t.Fatalf("%d exports still hold their chains", e.cache.InFlight())
	}
}

// A root whose bundle alone is too large to import fails, with why, and
// nothing is stored: no blob, no bundle, no index entry.
func TestExportOneRootBundleTooLargeToImport(t *testing.T) {
	probe := newTestEnv(t, nil)
	probe.cache.Add(fakeengine.Entry{Number: 1, Layers: manyLayers("one", 8)})
	limit := probe.mergeSize(t, 1) - 1

	e := newTestEnv(t, func(c *Config) { c.MaxMergeBytes = limit })
	e.cache.Add(fakeengine.Entry{Number: 1, Layers: manyLayers("one", 8)})
	ctx := context.Background()
	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{1}, Outputs: fsOutputs(1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stored) != 0 || len(res.Failed) != 1 || !strings.Contains(res.Failed[0].Error, "too large to import") {
		t.Fatalf("want root 1 failed as too large to import: %+v", res)
	}
	if puts := e.blobs.Puts(); puts != 0 {
		t.Fatalf("%d blobs uploaded for a bundle that isn't stored", puts)
	}
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 0 || slices.ContainsFunc(e.objects.Keys(), func(k string) bool { return strings.Contains(k, "/bundles/") }) {
		t.Fatalf("stored anyway: %d index entries, objects %v", len(idx.Entries), e.objects.Keys())
	}
	if e.cache.InFlight() != 0 {
		t.Fatalf("%d exports still hold their chains", e.cache.InFlight())
	}
}

func mapSizes(ms []map[digest.Digest]string) []int {
	out := make([]int, len(ms))
	for i, m := range ms {
		out[i] = len(m)
	}
	return out
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

// PresignGet fails too: the exporter then sizes merges with
// importer.DefaultAddressBytes.
func (failingStore) PresignGet(context.Context, digest.Digest) (blobstore.Address, error) {
	return blobstore.Address{}, errors.New("can't sign")
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
	if _, err := New(Config{Blobs: blobstore.NewMemStore(""), Meta: meta, Scope: "a/b"}); err == nil {
		t.Fatal("bad scope accepted")
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

// Known gap (#37): an importing engine's export of an imported value whose
// bundle holds a part the stored record lacks, but lacks one it holds, is
// reported as stored, though the index kept the stored record and the new
// part isn't in it. The tracker then counts the value as stored and doesn't
// try again. Replacing the stored record would lose its part, and an entry
// names one record, so the parts of two bundles can't be combined. See
// metastore's TestRecordKnownGapImportWithOtherParts and "Imported entries"
// in README.md.
func TestExportKnownGapImportWithOtherParts(t *testing.T) {
	e := newTestEnv(t, nil)
	ctx := context.Background()
	// Another cache's record of the value, with fs and execMeta parts.
	stored := &dagql.ValueBundle{
		Version: metastore.BundleVersion,
		Roots:   []dagql.TransferredRoot{{Ordinal: 1}},
		Values:  []dagql.TransferredValue{{Ordinal: 1, SenderNumber: 5, Record: dagql.PersistedRecord{ResultID: 5}}},
		Outputs: []dagql.TransferredOutput{
			{Ordinal: 1, Address: dagql.PersistedPartAddress{Part: "fs"}, State: "completed"},
			{Ordinal: 1, Address: dagql.PersistedPartAddress{Part: "execMeta"}, State: "completed"},
		},
	}
	rec, err := e.meta.Record(ctx, "team", stored, "cache-z", 1)
	if err != nil {
		t.Fatal(err)
	}
	n := rec.Numbers[1]
	before := rec.Index.Entries[n]
	// Merged into this engine on entry 20, which exports it with another
	// part only.
	if _, err := e.meta.RecordImports(ctx, "team", e.cache.ID, e.cache.Generation, []metastore.Import{{Number: n, Engine: 20}}); err != nil {
		t.Fatal(err)
	}
	e.cache.Add(fakeengine.Entry{Number: 20, Layers: [][]byte{layer("base")}})
	e.eng.mangle = func(c *fakeengine.Capture) {
		for i := range c.Bundle.Outputs {
			c.Bundle.Outputs[i].Address.Part = "newPart"
		}
	}

	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{20}, Outputs: fsOutputs(20)})
	if err != nil {
		t.Fatal(err)
	}
	// Reported as stored...
	if !slices.Equal(res.Stored, []uint64{20}) || len(res.Failed) != 0 {
		t.Fatalf("result %+v", res)
	}
	// ...but the index kept the stored record: the new part isn't in it.
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	after := idx.Entries[n]
	if after.Bundle != before.Bundle || after.Source.CacheID != "cache-z" || slices.Contains(after.Parts, `{"part":"newPart"}`) || idx.NextNumber != n+1 {
		t.Fatalf("entry %d: %+v (next %d)", n, after, idx.NextNumber)
	}
}

// recordNewer records, for the test engine's cache, a value of another
// engine entry (99) at a newer generation than the engine's: the index then
// knows a later generation of the cache, as when another replica recorded a
// restarted engine's export.
func (e *testEnv) recordNewer(t *testing.T, generation uint64) {
	t.Helper()
	newer := &dagql.ValueBundle{
		Version: metastore.BundleVersion,
		Roots:   []dagql.TransferredRoot{{Ordinal: 1}},
		Values:  []dagql.TransferredValue{{Ordinal: 1, SenderNumber: 99, Record: dagql.PersistedRecord{ResultID: 99}}},
	}
	if _, err := e.meta.Record(context.Background(), "team", newer, e.cache.ID, generation); err != nil {
		t.Fatal(err)
	}
}

func (e *testEnv) indexBytes(t *testing.T) []byte {
	t.Helper()
	key, err := e.meta.IndexKey("team")
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := e.objects.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// A stale generation's export (#59) is refused before any blob is asked for
// (#111): the engine gets no upload URL, nothing is stored, and the index
// and the other objects are as they were. The newer generation's export of
// the same value is recorded.
func TestExportStaleGenerationUploadsNothing(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	ctx := context.Background()
	e.recordNewer(t, e.cache.Generation+1)
	before := e.indexBytes(t)
	keys := slices.Sorted(slices.Values(e.objects.Keys()))

	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stored) != 0 || len(res.Failed) != 0 || len(res.Bundles) != 0 ||
		len(res.Refused) != 1 || res.Refused[0].Number != 11 || res.Refused[0].Message != StaleGenerationMessage {
		t.Fatalf("result %+v, want root 11 refused as a stale generation's", res)
	}
	if len(res.Uploaded) != 0 || len(res.Present) != 0 {
		t.Fatalf("uploaded %v, present %v", res.Uploaded, res.Present)
	}
	// One upload, empty: it releases the chains.
	if ups := e.eng.uploadMaps(); len(ups) != 1 || len(ups[0]) != 0 {
		t.Fatalf("uploads %v, want one release asking for nothing", ups)
	}
	for _, l := range [][]byte{layer("base"), layer("exec")} {
		if _, ok := e.blobs.Get(digest.FromBytes(l)); ok {
			t.Fatalf("layer %s stored", digest.FromBytes(l))
		}
	}
	if after := e.indexBytes(t); !bytes.Equal(after, before) {
		t.Fatal("index changed")
	}
	if got := slices.Sorted(slices.Values(e.objects.Keys())); !slices.Equal(got, keys) {
		t.Fatalf("objects %v, want %v: nothing published", got, keys)
	}

	// A newer generation of the cache exports the same value: recorded.
	e.cache.Generation += 2
	res, err = e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{11}) || len(res.Refused) != 0 || len(res.Uploaded) != 2 {
		t.Fatalf("newer generation's export: %+v", res)
	}
	idx, err := e.meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if m := idx.Caches[e.cache.ID]; m == nil || m.Generation != e.cache.Generation || m.Numbers[11].Number == 0 || idx.Roots[m.Numbers[11].Number].RecordedAtUnix == 0 {
		t.Fatalf("index after the newer generation's export: %+v", m)
	}
}

// The early check is only a shortcut: a newer generation recorded after it,
// while the engine uploads, is still refused by the index update, which
// adds no root or entry (#59, #111).
func TestExportStaleGenerationAfterEarlyCheck(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	ctx := context.Background()
	var once sync.Once
	var before []byte
	e.eng.onUpload = func() {
		once.Do(func() {
			e.recordNewer(t, e.cache.Generation+1)
			before = e.indexBytes(t)
		})
	}

	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if before == nil {
		t.Fatal("no upload asked for blobs: the early check refused it")
	}
	if len(res.Stored) != 0 || len(res.Failed) != 0 ||
		len(res.Refused) != 1 || res.Refused[0].Number != 11 || res.Refused[0].Message != StaleGenerationMessage {
		t.Fatalf("result %+v, want root 11 refused as a stale generation's", res)
	}
	if after := e.indexBytes(t); !bytes.Equal(after, before) {
		t.Fatal("index changed by the refused export")
	}
}

// Since v1.0.0-beta.26 an engine sends an output whose layers are the first
// of another output's chain as a prefix of that chain. The stored bundle
// gives each output its own chain: it is merged whole into other engines,
// and the blobs it names are read from its chains.
func TestExportStoresPrefixedOutputsWithTheirChains(t *testing.T) {
	e := newTestEnv(t, nil)
	e.cache.ChainPrefixes = true
	e.addBuild()
	prefixes := 0
	e.eng.mangle = func(capt *fakeengine.Capture) {
		for _, o := range capt.Bundle.Outputs {
			if o.Prefix != nil && o.Chain == nil {
				prefixes++
			}
		}
	}
	ctx := context.Background()
	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if prefixes != 1 {
		t.Fatalf("the engine sent %d prefixed outputs, want the base's", prefixes)
	}
	if !slices.Equal(res.Stored, []uint64{11}) || len(res.Bundles) != 1 || len(res.Uploaded) != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	b, err := e.meta.GetBundle(ctx, "team", res.Bundles[0])
	if err != nil {
		t.Fatal(err)
	}
	layers := map[int]int{}
	for _, o := range b.Outputs {
		if o.Prefix != nil || o.Chain == nil {
			t.Fatalf("stored output %d: prefix %v, chain %v", o.Ordinal, o.Prefix, o.Chain)
		}
		layers[len(o.Chain.Layers)]++
	}
	if len(b.Outputs) != 2 || layers[1] != 1 || layers[2] != 1 {
		t.Fatalf("stored chains by length: %v, want one of the base's layer and one of both", layers)
	}
}

// A layer's first export says the export computed its blob, and every later
// one doesn't: the same values are the same bundle all the same.
func TestRepeatedExportComputedLayersSameBundle(t *testing.T) {
	e := newTestEnv(t, nil)
	e.cache.ComputedLayers = true
	e.addBuild()
	ctx := context.Background()
	req := Request{Roots: []uint64{11}, Outputs: fsOutputs(10, 11)}
	computed := 0
	e.eng.mangle = func(capt *fakeengine.Capture) {
		for _, o := range capt.Bundle.Outputs {
			for _, l := range o.Chain.Layers {
				if l.Computed {
					computed++
				}
			}
		}
	}
	first, err := e.x.Export(ctx, e.eng, req)
	if err != nil {
		t.Fatal(err)
	}
	if computed == 0 {
		t.Fatal("the first export marked no layer computed: the test proves nothing")
	}
	second, err := e.x.Export(ctx, e.eng, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Bundles) != 1 || !slices.Equal(first.Bundles, second.Bundles) {
		t.Fatalf("bundles: %v then %v", first.Bundles, second.Bundles)
	}
}

// Roots an engine refuses because it has given up its remote-cache work
// are paused, not refused: with its skip, and when it cancels an export in
// progress with that error.
func TestExportPausedRoots(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.cache.Add(fakeengine.Entry{Number: 20, Refused: EnginePausedMessage})
	e.cache.Add(fakeengine.Entry{Number: 21, Refused: "no codec"})
	ctx := context.Background()
	res, err := e.x.Export(ctx, e.eng, Request{Roots: []uint64{20, 21}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Paused, []uint64{20}) || len(res.Refused) != 1 || res.Refused[0].Number != 21 || len(res.Failed) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	e.eng.replyErr = "context canceled: " + EnginePausedMessage
	res, err = e.x.Export(ctx, e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatalf("an export the engine canceled as paused failed: %v", err)
	}
	if !slices.Equal(res.Paused, []uint64{10, 11}) || len(res.Failed) != 0 || len(res.Stored) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

// An engine that gives up its remote-cache work during an upload answers it,
// with the blobs it never sent failed by that error: the roots are paused at
// once, with no further try, and the blob that arrived is stored.
func TestExportPausedDuringUpload(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.eng.cutUploads = "context canceled: " + EnginePausedMessage
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Paused, []uint64{10, 11}) || len(res.Failed) != 0 || len(res.Stored) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(e.eng.uploads) != 1 || len(e.eng.uploads[0]) < 2 {
		t.Fatalf("want one upload of several blobs, got %v", e.eng.uploads)
	}
	if len(res.Uploaded) != 1 {
		t.Fatalf("want the blob that arrived stored, got %v", res.Uploaded)
	}

	// The next export asks only for the blobs still missing.
	e.eng.cutUploads = ""
	res, err = e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{10, 11}) || len(res.Present) != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

// An engine that gives up its remote-cache work doesn't always say so of the
// export it cuts off: one waiting for its upload is dropped without a word,
// so the upload finds no export, and a capture building layers ends with
// its context's error. The export is asked again, once, and the paused
// engine refuses its roots: paused, with no failure.
func TestExportCutByPausedEngine(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     protocol.Type
		message string
	}{
		{"dropped before its upload", protocol.TypeUpload, `no export "export-1"`},
		{"canceled while capturing", protocol.TypeExport, "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, nil)
			e.addBuild()
			e.eng.errOnce = map[protocol.Type]string{tc.typ: tc.message}
			e.eng.pauseAfter = true
			res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
			if err != nil {
				t.Fatalf("an export the engine cut off as it paused failed: %v", err)
			}
			if !slices.Equal(res.Paused, []uint64{10, 11}) || len(res.Failed) != 0 || len(res.Stored) != 0 {
				t.Fatalf("unexpected result: %+v", res)
			}
			if got := e.eng.requests[protocol.TypeExport]; got != 2 {
				t.Fatalf("%d export requests, want 2", got)
			}
		})
	}
}

// The blobs a paused engine was sending fail with their context's error,
// not the pause's: the export is asked again, and its roots are paused.
func TestExportUploadCanceledByPausedEngine(t *testing.T) {
	for _, message := range []string{
		"upload: context canceled",
		// A PUT cut before it connected ends with the net package's error.
		"upload: dial tcp 10.0.0.1:443: operation was canceled",
	} {
		t.Run(message, func(t *testing.T) {
			e := newTestEnv(t, nil)
			e.addBuild()
			e.eng.cutUploads = message
			e.eng.pauseAfter = true
			res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(res.Paused, []uint64{10, 11}) || len(res.Failed) != 0 || len(res.Stored) != 0 {
				t.Fatalf("unexpected result: %+v", res)
			}
			if got := e.eng.requests[protocol.TypeUpload]; got != 1 {
				t.Fatalf("%d uploads, want 1: no upload attempt was spent on the cut", got)
			}
		})
	}
}

// A cut that no pause explains is one more try of the export, which goes
// on; an engine that keeps cutting it off fails its roots.
func TestExportCutWithoutPause(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.eng.errOnce = map[protocol.Type]string{protocol.TypeUpload: `no export "export-1"`}
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{10, 11}) || len(res.Paused) != 0 || len(res.Failed) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}

	e = newTestEnv(t, nil)
	e.addBuild()
	e.eng.replyErr = "context canceled"
	res, err = e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Failed) != 2 || !strings.Contains(res.Failed[0].Error, "cut the export off 2 times") || len(res.Stored) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

// EnginePausedMessage is engine/server's errRemoteCacheTimedOut, which that
// package doesn't export.
func TestEnginePausedMessageMatchesEngine(t *testing.T) {
	src, err := os.ReadFile("../../../engine/server/remote_cache.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`errRemoteCacheTimedOut = errors\.New\("([^"]+)"\)`).FindSubmatch(src)
	if m == nil {
		t.Fatal("engine/server/remote_cache.go no longer declares errRemoteCacheTimedOut as errors.New of a literal: " +
			"find the message of the roots an engine refuses once it has given up its remote-cache work, and update EnginePausedMessage and this test")
	}
	if string(m[1]) != EnginePausedMessage {
		t.Fatalf("the engine refuses roots with %q once it has given up its remote-cache work, and EnginePausedMessage is %q: "+
			"the service would settle those roots as refused for good", m[1], EnginePausedMessage)
	}
}

// The texts replyError takes for an export the engine cut off are the
// engine's own, which it doesn't export: a reworded one would make a pause
// that cuts an export count as its roots' failure again, with every other
// test still passing.
func TestEngineCutMessagesMatchEngine(t *testing.T) {
	src, err := os.ReadFile("../../../engine/remotecache/session.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`replyError\(env, fmt\.Errorf\("([^"%]*)%q", req\.ExportID\)\)`).FindSubmatch(src)
	if m == nil {
		t.Fatal("engine/remotecache/session.go's handleUpload no longer answers an upload that names no export with fmt.Errorf of a literal and the export's ID: " +
			"find its reply, and update engineNoExportPrefix and this test")
	}
	if string(m[1]) != engineNoExportPrefix {
		t.Fatalf("the engine answers an upload that names no export with %q, and engineNoExportPrefix is %q", m[1], engineNoExportPrefix)
	}
	// The other text is the standard library's, which the engine's capture
	// and uploads end with when their context is canceled.
	if err := replyError(mustError(t, "upload: "+context.Canceled.Error())); !errors.Is(err, errEngineCut) {
		t.Fatalf("a canceled context's error reply is %v, want errEngineCut", err)
	}
	if err := replyError(mustError(t, fmt.Sprintf(engineNoExportPrefix+"%q", "x-1"))); !errors.Is(err, errEngineCut) {
		t.Fatalf("the engine's no-export reply is %v, want errEngineCut", err)
	}
	// And the net package's, for a connection a canceled context cut off
	// while it was being made, as a blob's PUT is: what a dial with a
	// canceled context returns.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
	if err == nil || !strings.Contains(err.Error(), netCanceledMessage) {
		t.Fatalf("a dial with a canceled context returns %v, and netCanceledMessage is %q", err, netCanceledMessage)
	}
	if !canceledMessage("upload: "+err.Error()) || canceledMessage("upload: 503 Service Unavailable") {
		t.Fatal("canceledMessage doesn't tell a canceled dial's error from a store's")
	}
}

func mustError(t *testing.T, message string) protocol.Envelope {
	t.Helper()
	env, err := protocol.NewEnvelope(1, 1, protocol.TypeError, protocol.Error{Message: message})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// heldFirst makes the engine's first bundle carry a held row, as after a
// merge of the service's landed on it.
func heldFirst(e *testEnv, then func()) {
	first := true
	e.eng.mangle = func(c *fakeengine.Capture) {
		if !first {
			return
		}
		first = false
		c.Bundle.Values[0].Held = digest.FromString("held")
		if then != nil {
			then()
		}
	}
}

// A release that fails made the engine forget nothing, so asking again
// would get the same held rows: the roots fail, and nothing is asked again.
func TestExportHeldRowsReleaseFails(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	heldFirst(e, func() {
		e.eng.mu.Lock()
		e.eng.errOnce = map[protocol.Type]string{protocol.TypeUpload: "boom"}
		e.eng.mu.Unlock()
	})
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Failed) != 2 || !strings.Contains(res.Failed[0].Error, "release of a bundle with held rows") || len(res.Stored) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if got := e.eng.requests[protocol.TypeExport]; got != 1 {
		t.Fatalf("%d export requests, want 1: the export was asked again after its release failed", got)
	}
}

// An engine that gives up its remote-cache work between a bundle with held
// rows and its release answers the release as it does any upload then: with
// the pause's error, or with no export of that ID. Neither is a failure of
// the roots: they are paused.
func TestExportHeldRowsReleaseCutByPausedEngine(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		exports     int
	}{
		{"the pause's error", "context canceled: " + EnginePausedMessage, 1},
		{"no export of that ID", engineNoExportPrefix + `"export-1"`, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, nil)
			e.addBuild()
			heldFirst(e, func() {
				e.eng.mu.Lock()
				e.eng.errOnce = map[protocol.Type]string{protocol.TypeUpload: tc.reply}
				e.eng.pauseAfter = true
				e.eng.mu.Unlock()
			})
			res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(res.Paused, []uint64{10, 11}) || len(res.Failed) != 0 || len(res.Stored) != 0 {
				t.Fatalf("unexpected result: %+v", res)
			}
			if got := e.eng.requests[protocol.TypeExport]; got != tc.exports {
				t.Fatalf("%d export requests, want %d", got, tc.exports)
			}
		})
	}
}

// Known gap, pinned on purpose (zerobull-consulting/dagger#227): an upload
// the engine never answers holds its export, and with it the cache's export
// slot, until UploadTimeout (an hour by default). An engine that gives up
// its remote-cache work at the instant the upload reaches it can drop it so
// (engine/remotecache's handleUpload hands it to an export that is already
// ending). Nothing ends the wait sooner: the service can't tell such an
// upload from a slow one. Closing it needs the engine to answer every
// upload it takes.
func TestKnownGapUnansweredUploadHoldsExportUntilUploadTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	e := newTestEnv(t, func(c *Config) { c.UploadTimeout = timeout })
	e.addBuild()
	e.eng.hangUploads = true
	start := time.Now()
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if took := time.Since(start); took < timeout {
		t.Fatalf("the export ended after %s, before UploadTimeout: the gap is closed, update the README", took)
	}
	if err == nil || len(res.Stored) != 0 {
		t.Fatalf("an export whose upload got no answer: %+v, %v; want an error", res, err)
	}
}

// Known gap, pinned on purpose (zerobull-consulting/dagger#227): an export
// whose first try came back with held rows keeps that try's skipped roots,
// and when the next try, with the rows in full, is too large, the split
// asks for the first try's roots again. A root both tries skipped is
// settled twice: here busy twice, which the tracker's Done counts as two
// busy answers, so its backoff doubles once more than it should. Nothing is
// stored wrong. Closing it means splitting only the roots still unsettled.
func TestKnownGapSplitAfterHeldRowsSettlesRootTwice(t *testing.T) {
	e := newTestEnv(t, nil)
	e.addBuild()
	e.cache.Add(fakeengine.Entry{Number: 20, Busy: true})
	heldFirst(e, func() {
		e.eng.mu.Lock()
		// The upload that releases the held bundle is answered; the export
		// after it is too large.
		e.eng.errOnce = map[protocol.Type]string{}
		e.eng.tooLargeAfter = e.eng.requests[protocol.TypeExport]
		e.eng.mu.Unlock()
	})
	res, err := e.x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11, 20}, Outputs: fsOutputs(10, 11)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Stored, []uint64{10, 11}) {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(res.Busy) != 2 || res.Busy[0].Number != 20 || res.Busy[1].Number != 20 {
		t.Fatalf("root 20 is busy %d times in the result, want 2: the gap is closed, update the README", len(res.Busy))
	}
}

// Known gap, pinned on purpose (zerobull-consulting/dagger#227): the bound
// on a bundle (MaxMergeBytes) is on its merge as sent, which lists each
// layer once, but a bundle is stored with every output's chain in full, so
// that it stands on its own. Outputs that build on each other, as a
// Dockerfile's steps do, make the stored bundle larger than its merge by
// about half the chain's depth, and no bound holds it: every engine's
// import path reads and keeps it. Closing it means storing the bundle with
// its layers listed once, or bounding the stored size too.
func TestKnownGapStoredBundleLargerThanItsMerge(t *testing.T) {
	const depth = 60
	e := newTestEnv(t, nil)
	e.cache.ChainPrefixes = true
	var layers [][]byte
	var roots []uint64
	for i := range depth {
		n := uint64(100 + i)
		layers = append(layers, layer(fmt.Sprintf("step-%d", i)))
		entry := fakeengine.Entry{Number: n, Layers: slices.Clone(layers)}
		if i > 0 {
			entry.Deps = []uint64{n - 1}
		}
		e.cache.Add(entry)
		roots = append(roots, n)
	}
	ctx := context.Background()
	res, err := e.x.Export(ctx, e.eng, Request{Roots: roots[depth-1:], Outputs: fsOutputs(roots...)})
	if err != nil || len(res.Bundles) != 1 {
		t.Fatalf("export: %+v, %v", res, err)
	}
	stored, err := e.meta.GetBundle(ctx, "team", res.Bundles[0])
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	merge, err := importer.MergeSize(stored, e.x.addressSize(ctx), "team")
	if err != nil {
		t.Fatal(err)
	}
	listed := 0
	for _, o := range stored.Outputs {
		listed += len(o.Chain.Layers)
	}
	t.Logf("%d outputs on a chain %d deep: the stored bundle lists %d layers in %d bytes; its merge is bounded at %d bytes", len(stored.Outputs), depth, listed, len(data), merge)
	if listed != depth*(depth+1)/2 {
		t.Fatalf("the stored bundle lists %d layers, want every chain in full (%d)", listed, depth*(depth+1)/2)
	}
	if len(data) <= merge {
		t.Fatalf("the stored bundle is %d bytes, within its merge's %d: the gap is closed, update the README", len(data), merge)
	}
}
