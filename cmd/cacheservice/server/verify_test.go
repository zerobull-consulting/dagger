package server

// Fake-engine tests of upload verification (#23): engines upload to staging
// keys, and the service promotes a blob to its digest key only once its
// content hashes to the digest. The engine side is the engine's own
// remotecache.Run, unchanged: a bare PUT with Content-Length.

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/dagql"
)

const buildExport = `{"roots":[10,11],"outputs":[{"number":10,"address":{"part":"fs"}},{"number":11,"address":{"part":"fs"}}]}`

// An engine that uploads a same-size body with the wrong content gets no
// bundle published, and the wrong content never reaches the digest key. Its
// next export is asked for that blob again (and only it), and once the
// upload is right, the bundle is published and another engine pulls the
// right layers.
func TestVerifyLyingUploadNotPublished(t *testing.T) {
	h, blobs := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	base, exec := digest.FromBytes(layers[0]), digest.FromBytes(layers[1])
	h.cache.CorruptUploads(exec, -1)

	code, res := h.debugExport(buildExport)
	if code != 200 {
		t.Fatalf("export: %d", code)
	}
	if len(res.Stored) != 0 || len(res.Bundles) != 0 || len(res.Failed) != 2 || len(res.Rejected) != exporterAttempts {
		t.Fatalf("lying export: %+v", res)
	}
	if _, ok := blobs.Get(exec); ok {
		t.Fatal("the wrong content reached the digest key")
	}
	if left := blobs.Staged(); len(left) != 0 {
		t.Fatalf("staged blobs left: %v", left)
	}
	idx, err := h.meta.Index(context.Background(), testScope)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 0 || len(idx.Roots) != 0 {
		t.Fatalf("index has %d entries, %d roots", len(idx.Entries), len(idx.Roots))
	}
	waitMetric(t, h.svc, "cacheservice_blob_promotions_total", map[string]string{"scope": testScope, "outcome": "mismatch"}, exporterAttempts)
	waitMetric(t, h.svc, "cacheservice_export_blobs_total", map[string]string{"result": "rejected"}, exporterAttempts)

	h.cache.CorruptUploads(exec, 0)
	code, res = h.debugExport(buildExport)
	if code != 200 || len(res.Stored) != 2 || len(res.Bundles) != 1 {
		t.Fatalf("honest export: %d %+v", code, res)
	}
	if !slices.Equal(res.Uploaded, []digest.Digest{exec}) || !slices.Equal(res.Present, []digest.Digest{base}) {
		t.Fatalf("honest export uploaded %v, found %v present; want only %s asked for again", res.Uploaded, res.Present, exec)
	}
	b := fakeengine.New("cache-b", 1)
	h.connectEngine(b)
	checkPull(t, b, "withExec(make)", layers)
}

// exporterAttempts is how many times the harness's exporter tries one
// export (exporter.DefaultUploadAttempts).
const exporterAttempts = 3

// A correct upload is staged, verified, promoted, and its staged copy
// dropped; the bundle is published, with its promotions measured.
func TestVerifyCorrectUploadPromoted(t *testing.T) {
	h, blobs := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	h.exportBuild()
	for _, l := range layers {
		if got, ok := blobs.Get(digest.FromBytes(l)); !ok || digest.FromBytes(got) != digest.FromBytes(l) {
			t.Fatalf("layer %s not stored", digest.FromBytes(l))
		}
	}
	if left := blobs.Staged(); len(left) != 0 {
		t.Fatalf("staged blobs left: %v", left)
	}
	waitMetric(t, h.svc, "cacheservice_blob_promotions_total", map[string]string{"scope": testScope, "outcome": "promoted"}, 2)
	want := float64(len(layers[0]) + len(layers[1]))
	if got := metricstest.Value(t, h.svc.registry, "cacheservice_blob_hashed_bytes_total", map[string]string{"scope": testScope, "by": "service"}); got != want {
		t.Fatalf("bytes hashed %v, want %v", got, want)
	}
	if s := metricstest.Read(t, h.svc.registry, "cacheservice_blob_promotion_duration_seconds", map[string]string{"scope": testScope}); s.Count != 2 {
		t.Fatalf("promotion durations %+v", s)
	}
}

// Two engines exporting the same blob at once, each to a stage of its own,
// both end correct: one promotion stores it, the other finds it stored, and
// both bundles are published with the blob verified.
func TestVerifyRacingExports(t *testing.T) {
	h, blobs := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	exec := digest.FromBytes(layers[1])
	other := fakeengine.New("cache-c", 1)
	other.Add(fakeengine.Entry{Number: 10, Recipe: "from(alpine)", Layers: [][]byte{layers[0]}})
	other.Add(fakeengine.Entry{Number: 11, Recipe: "withExec(make)", Deps: []uint64{10}, Layers: layers})
	otherConn := h.connectEngine(other)

	// Neither promotes exec until both have staged it: both were asked
	// for it, and both uploaded it.
	var mu sync.Mutex
	arrived := 0
	both := make(chan struct{})
	blobs.BeforePromote(func(ctx context.Context, _ string, d digest.Digest) error {
		if d != exec {
			return nil
		}
		mu.Lock()
		arrived++
		if arrived == 2 {
			close(both)
		}
		mu.Unlock()
		select {
		case <-both:
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
		}
		return nil
	})
	var wg sync.WaitGroup
	for _, c := range []*engineConn{h.engine, otherConn} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, res := h.debugExportOf(c, buildExport)
			if code != 200 || len(res.Stored) != 2 || len(res.Bundles) != 1 || !slices.Contains(res.Uploaded, exec) {
				t.Errorf("%s's export: %d %+v", c.CacheID(), code, res)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	if arrived != 2 {
		t.Fatalf("%d promotions of exec, want both exports racing", arrived)
	}
	mu.Unlock()
	if got, ok := blobs.Get(exec); !ok || digest.FromBytes(got) != exec {
		t.Fatal("exec isn't stored correct")
	}
	if left := blobs.Staged(); len(left) != 0 {
		t.Fatalf("staged blobs left: %v", left)
	}
	for outcome := range map[string]bool{"promoted": true, "existing": true} {
		if got := metricstest.Value(t, h.svc.registry, "cacheservice_blob_promotions_total", map[string]string{"scope": testScope, "outcome": outcome}); got < 1 {
			t.Errorf("%s promotions: %v", outcome, got)
		}
	}
}

// Pinned on purpose: a restart between the engine's uploaded and the
// promotion leaves the staged uploads where they are. The service doesn't
// scan staging on startup: a staged object belongs to an export whose state
// is lost with the restart (#45), so nothing would publish a bundle for it,
// and the next export of the entries asks for the blobs again, under a new
// stage, and promotes those. The orphans are never read (Has and GETs look
// at digest keys only), and the bucket's lifecycle rule expires them. The
// cost is the blobs' upload again.
func TestVerifyRestartBetweenUploadedAndPromotion(t *testing.T) {
	blobs, meta := newMemStores(t)
	view := memIn(t, blobs, testScope)
	r := newRestartable(t, blobs, meta)
	svc := r.start()
	a := fakeengine.New("cache-a", 1)
	base, exec := testLayer("base"), testLayer("exec")
	a.Add(fakeengine.Entry{Number: 10, Recipe: "from(alpine)", Layers: [][]byte{base}})
	a.Add(fakeengine.Entry{Number: 11, Recipe: "withExec(make)", Deps: []uint64{10}, Layers: [][]byte{base, exec}})
	r.connect(a)
	waitConn(t, svc, a.ID)

	// Promotions hang until the service stops.
	reached := make(chan struct{}, 2)
	blobs.BeforePromote(func(ctx context.Context, _ string, _ digest.Digest) error {
		reached <- struct{}{}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	r.harness(svc, a).postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	<-reached
	r.stop()
	blobs.BeforePromote(nil)
	if view.Len() != 0 || len(view.Staged()) != 2 {
		t.Fatalf("after the stop: %d stored, %d staged; want the uploads staged only", view.Len(), len(view.Staged()))
	}

	svc = r.start()
	conn := waitConn(t, svc, a.ID)
	// Nothing is promoted on startup, nor when the namespace is first used.
	if _, err := svc.Import(t.Context(), conn.id); err != nil {
		t.Fatal(err)
	}
	if view.Len() != 0 || len(view.Staged()) != 2 {
		t.Fatalf("after the restart: %d stored, %d staged", view.Len(), len(view.Staged()))
	}
	// The next session's export asks for both blobs again, and stores them.
	h := r.harness(svc, a)
	h.postSpans("session-2", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	h.waitIndex(10, 11)
	if !hasAll(t, view, []digest.Digest{digest.FromBytes(base), digest.FromBytes(exec)}) {
		t.Fatal("the blobs aren't stored after the next export")
	}
	if len(view.Staged()) != 2 {
		t.Fatalf("%d staged, want the 2 orphans left to the lifecycle rule", len(view.Staged()))
	}
	waitMetric(t, svc, "cacheservice_export_blobs_total", map[string]string{"result": "uploaded"}, 2)
}

// Nothing under staging is ever offered, merged or renewed: a staged upload
// that was never promoted (here an orphan, and a staged second copy of a
// stored blob) doesn't make the blob available, and every address an engine
// gets is a digest key's.
func TestVerifyStagingNeverServed(t *testing.T) {
	h, blobs := newMemExportHarness(t, nil)
	layers := h.buildRecipes()
	h.exportBuild()
	orphan := testLayer("orphan")
	od := digest.FromBytes(orphan)
	for _, l := range [][]byte{orphan, layers[1]} {
		put, err := blobs.PresignPut(t.Context(), blobstore.NewStage(), digest.FromBytes(l), int64(len(l)))
		if err != nil {
			t.Fatal(err)
		}
		if code := httpPut(t, put.URL, l); code != 200 {
			t.Fatalf("staging PUT: %d", code)
		}
	}
	if len(blobs.Staged()) != 2 {
		t.Fatalf("%d staged", len(blobs.Staged()))
	}

	b := fakeengine.New("cache-b", 1)
	eng := h.connectEngine(b)
	checkPull(t, b, "withExec(make)", layers)
	if code, _ := h.debugImport(eng); code != 200 {
		t.Fatalf("import: %d", code)
	}
	for _, recipe := range []string{"from(alpine)", "withExec(make)"} {
		e := waitLookup(t, b, recipe, nil)
		if e.Offer != nil {
			checkDigestAddresses(t, e.Offer.Chain.Addresses)
		}
	}
	addrs, ok, err := b.RenewLayers(t.Context(), testScope, exportLayers(layers...))
	if err != nil || !ok {
		t.Fatalf("renew: %v, %v", ok, err)
	}
	checkDigestAddresses(t, addrs)
	if addrs, ok, err := b.RenewLayers(t.Context(), testScope, exportLayers(orphan)); err != nil || ok || len(addrs) != 0 {
		t.Fatalf("renew of a blob only staged: %v, %v, %v", addrs, ok, err)
	}
	if has, _ := blobs.Has(t.Context(), od); has {
		t.Fatal("a staged blob is reported stored")
	}
}

// checkDigestAddresses checks that every address is the GET of its digest
// key in the harness's namespace, never a staging key.
func checkDigestAddresses(t *testing.T, addrs map[digest.Digest]dagql.BlobAddress) {
	t.Helper()
	for d, a := range addrs {
		u, err := url.Parse(a.URL)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(u.Path, "/staging/") || u.Path != "/mem/"+testScope+"/"+d.String() {
			t.Fatalf("address %s of %s isn't its digest key's", u.Path, d)
		}
	}
}

// Staging and promotion never cross namespaces: every promotion of an
// engine's uploads runs in its own namespace, a lying upload in one leaves
// nothing staged or stored in the other, and the other still uploads (and
// verifies) its own copy of a shared layer.
func TestVerifyNamespacesApart(t *testing.T) {
	h := newAuthHarness(t)
	var mu sync.Mutex
	promoted := map[string][]digest.Digest{}
	h.blobs.BeforePromote(func(_ context.Context, ns string, d digest.Digest) error {
		mu.Lock()
		defer mu.Unlock()
		promoted[ns] = append(promoted[ns], d)
		return nil
	})
	a, b := fakeengine.New("cache-a", 1), fakeengine.New("cache-b", 1)
	layersA, layersB := authBuild(a, "a"), authBuild(b, "b")
	base, execA, execB := digest.FromBytes(layersA[0]), digest.FromBytes(layersA[1]), digest.FromBytes(layersB[1])
	a.CorruptUploads(execA, 1)
	h.connect(a, "tok-a")
	h.connect(b, "tok-b")
	h.export(a, "tok-a")
	h.export(b, "tok-b")

	inA, inB := memIn(t, h.blobs, "team-a"), memIn(t, h.blobs, "team-b")
	mu.Lock()
	defer mu.Unlock()
	if got := slices.Sorted(slices.Values(promoted["team-a"])); !slices.Equal(got, slices.Sorted(slices.Values([]digest.Digest{base, execA, execA}))) {
		t.Fatalf("team-a's promotions %v", got)
	}
	if got := slices.Sorted(slices.Values(promoted["team-b"])); !slices.Equal(got, slices.Sorted(slices.Values([]digest.Digest{base, execB}))) {
		t.Fatalf("team-b's promotions %v", got)
	}
	if len(promoted) != 2 {
		t.Fatalf("promotions in %v", keysOf(promoted))
	}
	if !hasAll(t, inA, []digest.Digest{base, execA}) || !hasAll(t, inB, []digest.Digest{base, execB}) || hasAll(t, inB, []digest.Digest{execA}) {
		t.Fatal("blobs not stored in their own namespace only")
	}
	if len(inA.Staged())+len(inB.Staged())+len(h.blobs.Staged()) != 0 {
		t.Fatalf("staged left: team-a %v, team-b %v, top %v", inA.Staged(), inB.Staged(), h.blobs.Staged())
	}
	waitMetric(t, h.svc, "cacheservice_blob_promotions_total", map[string]string{"scope": "team-a", "outcome": "mismatch"}, 1)
	if s := metricstest.Read(t, h.svc.registry, "cacheservice_blob_promotions_total", map[string]string{"scope": "team-b", "outcome": "mismatch"}); s.Found {
		t.Fatalf("a mismatch counted in team-b: %+v", s)
	}
}

// httpPut uploads body as the engine does: a bare PUT with Content-Length.
func httpPut(t *testing.T, address string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, address, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}
