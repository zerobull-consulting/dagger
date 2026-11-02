package server

// Auth and namespaces (#10): tokens map to namespaces, and a token sees only
// its own namespace's data. The engines here are the engine's own
// remotecache.Run over a real WebSocket, with the fake engine cache behind
// it, or hand-driven connections for what a real engine never sends.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/opencontainers/go-digest"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/snapshots"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// The tokens of the tests: two of team A's (its engine's, and another
// engine's or a client's), and team B's.
const testTokens = `
# team A
tok-a    team-a
tok-a2   team-a

# team B
tok-b    team-b
`

var testTokenNS = map[string]string{"tok-a": "team-a", "tok-a2": "team-a", "tok-b": "team-b"}

// authHarness is a service with tokens, its export and import paths on
// in-memory stores, and no engine yet.
type authHarness struct {
	t   *testing.T
	svc *Service
	srv *httptest.Server
	rec *logRecorder
	// blobs is the in-memory blob store, nil on other stores.
	blobs  *blobstore.MemStore
	meta   *metastore.Store
	tokens string
	ctx    context.Context
}

func newAuthHarness(t *testing.T) *authHarness {
	t.Helper()
	blobs, meta := newMemStores(t)
	h := newAuthHarnessOn(t, blobs, meta)
	h.blobs = blobs
	return h
}

// newAuthHarnessOn is newAuthHarness on stores of the caller's choosing.
func newAuthHarnessOn(t *testing.T, blobs blobstore.Store, meta *metastore.Store) *authHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rec := &logRecorder{}
	svc := New(slog.New(rec), "")
	if err := svc.EnableExport(ctx, ExportConfig{
		Exporter: exporter.Config{Blobs: blobs, Meta: meta, Scope: testScope, RetryBackoff: time.Millisecond},
		Tracker:  exporter.TrackerConfig{QuietPeriod: time.Minute, EndGrace: 50 * time.Millisecond},
		Tick:     10 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.EnableImport(ctx, ImportConfig{Importer: importer.Config{Blobs: blobs, Meta: meta, Scope: testScope}}); err != nil {
		t.Fatal(err)
	}
	h := &authHarness{t: t, svc: svc, rec: rec, meta: meta, tokens: filepath.Join(t.TempDir(), "tokens"), ctx: ctx}
	h.writeTokens(testTokens)
	if err := svc.UseTokensFile(h.tokens); err != nil {
		t.Fatal(err)
	}
	h.srv = httptest.NewServer(svc.Handler())
	t.Cleanup(func() {
		cancel()
		svc.CloseEngines()
		svc.Wait()
		h.srv.Close()
	})
	return h
}

func (h *authHarness) writeTokens(content string) {
	h.t.Helper()
	if err := os.WriteFile(h.tokens, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// reload rewrites the tokens file and reloads it, as SIGHUP does.
func (h *authHarness) reload(content string) {
	h.t.Helper()
	h.writeTokens(content)
	if err := h.svc.ReloadTokens(); err != nil {
		h.t.Fatal(err)
	}
}

func (h *authHarness) ns(name string) *namespace {
	h.t.Helper()
	return mustNamespace(h.t, h.svc, name)
}

// connect runs a fake engine with token until the test ends, and returns
// its connection.
func (h *authHarness) connect(cache *fakeengine.Cache, token string) *engineConn {
	h.t.Helper()
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: cache}, remotecache.Config{URL: h.srv.URL, Token: token, EngineName: cache.ID})
	}()
	h.t.Cleanup(func() {
		cancel()
		<-done
	})
	ns := h.ns(testTokenNS[token])
	var c *engineConn
	waitUntil(h.t, 10*time.Second, "engine "+cache.ID+" to connect", func() bool {
		c = h.svc.engineFor(ns, cache.ID)
		return c != nil
	})
	return c
}

// build adds a build to cache: a base image (10) whose layer every team's
// build shares, and an exec on it (11) whose layer is the team's own.
// It returns the exec's layers.
func authBuild(cache *fakeengine.Cache, team string) [][]byte {
	base, exec := testLayer("base"), testLayer("exec-"+team)
	cache.Add(fakeengine.Entry{Number: 10, Recipe: "from(alpine)", Layers: [][]byte{base}})
	cache.Add(fakeengine.Entry{Number: 11, Recipe: "withExec(make)", Deps: []uint64{10}, Layers: [][]byte{base, exec}})
	return [][]byte{base, exec}
}

// export posts a build session's spans under token, and waits until the
// namespace's index maps them.
func (h *authHarness) export(cache *fakeengine.Cache, token string) *metastore.Index {
	h.t.Helper()
	if code := h.postSpans(token, cache, "session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2)); code != http.StatusOK {
		h.t.Fatalf("spans: status %d", code)
	}
	return h.waitIndex(testTokenNS[token], cache.ID, 10, 11)
}

func (h *authHarness) waitIndex(ns, cacheID string, numbers ...uint64) *metastore.Index {
	h.t.Helper()
	return waitIndexIn(h.t, h.meta, ns, cacheID, numbers...)
}

// waitIndexIn waits until a namespace's index maps each number of an engine
// cache, and returns it.
func waitIndexIn(t *testing.T, meta *metastore.Store, ns, cacheID string, numbers ...uint64) *metastore.Index {
	t.Helper()
	var idx *metastore.Index
	waitUntil(t, 20*time.Second, "entries of "+cacheID+" in "+ns+"'s index", func() bool {
		var err error
		idx, err = meta.Index(context.Background(), ns)
		if err != nil {
			t.Fatal(err)
		}
		c := idx.Caches[cacheID]
		if c == nil {
			return false
		}
		for _, n := range numbers {
			if _, ok := c.Numbers[n]; !ok {
				return false
			}
		}
		return true
	})
	return idx
}

func (h *authHarness) postSpans(token string, cache *fakeengine.Cache, session string, spans ...*tracepb.Span) int {
	h.t.Helper()
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			kv(telemetryattrs.EngineCacheAttr, cache.ID+"/"+strconv.FormatUint(cache.Generation, 10)),
			kv(telemetryattrs.EngineSessionAttr, session),
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
	return otlpPostAs(h.t, h.srv.URL+"/v1/traces", token, req)
}

// postEvent posts one engine event about cache ("<id>/<generation>") under
// token, as the engine's log exporter does.
func (h *authHarness) postEvent(token, cache, kind string, body any) int {
	h.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		h.t.Fatal(err)
	}
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: telemetryattrs.EngineEventScope},
			LogRecords: []*logspb.LogRecord{{
				Body: strValue(string(data)),
				Attributes: []*commonpb.KeyValue{
					kv(telemetryattrs.EngineEventAttr, kind),
					kv(telemetryattrs.EngineCacheAttr, cache),
				},
			}},
		}},
	}}}
	return otlpPostAs(h.t, h.srv.URL+"/v1/logs", token, req)
}

func cacheAttr(c *fakeengine.Cache) string { return c.ID + "/" + strconv.FormatUint(c.Generation, 10) }

func pruneOf(numbers ...uint64) telemetryattrs.EnginePruneEvent {
	var ev telemetryattrs.EnginePruneEvent
	for _, n := range numbers {
		ev.Drops = append(ev.Drops, telemetryattrs.EngineRetentionDrop{ResultID: n, DroppedAtUnixNano: time.Now().UnixNano()})
	}
	return ev
}

// otlpPostAs posts an OTLP request with token as its HTTP Basic username,
// as the engine's Cloud exporters do, or with no credentials when token is
// empty, and returns the status.
func otlpPostAs(t *testing.T, url, token string, msg proto.Message) int {
	t.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/x-protobuf")
	if token != "" {
		req.SetBasicAuth(token, "")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// dialAs dials the engine WebSocket with token (none when empty), and
// returns the connection, or the HTTP status of a refused upgrade.
func dialAs(t *testing.T, srv *httptest.Server, token string) (*rawEngine, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	header := http.Header{}
	if token != "" {
		header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(token+":")))
	}
	conn, resp, err := websocket.Dial(ctx, srv.URL+protocol.EnginePath, &websocket.DialOptions{HTTPHeader: header})
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		if resp == nil {
			t.Fatal(err)
		}
		return nil, resp.StatusCode
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &rawEngine{t: t, conn: conn}, http.StatusSwitchingProtocols
}

func (e *rawEngine) helloAs(cacheID string) protocol.Envelope {
	e.t.Helper()
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: cacheID, Generation: 1, EngineName: cacheID})
	if err != nil {
		e.t.Fatal(err)
	}
	e.send(env)
	return e.next()
}

// recordsAt returns the records logged with msg at level.
func (h *logRecorder) recordsAt(msg string, level slog.Level) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Message == msg && r.Level == level {
			n++
		}
	}
	return n
}

// errorsLogged returns the messages logged at error level.
func (h *logRecorder) errorsLogged() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var msgs []string
	for _, r := range h.records {
		if r.Level >= slog.LevelError {
			msgs = append(msgs, r.Message)
		}
	}
	return msgs
}

// An unknown token, or none, is refused with 401 before the upgrade: no
// protocol message, no listed engine, and its OTLP requests are refused the
// same way. A known token is welcomed in its namespace.
func TestUnknownTokenRefused(t *testing.T) {
	h := newAuthHarness(t)
	for _, token := range []string{"", "nope", "tok-a "} {
		if _, code := dialAs(t, h.srv, token); code != http.StatusUnauthorized {
			t.Fatalf("token %q: upgrade answered %d", token, code)
		}
		for _, path := range []string{"/v1/logs", "/v1/traces", "/v1/metrics"} {
			if code := otlpPostAs(t, h.srv.URL+path, token, &collogspb.ExportLogsServiceRequest{}); code != http.StatusUnauthorized {
				t.Fatalf("token %q: POST %s answered %d", token, path, code)
			}
		}
	}
	if n := len(h.svc.Engines()); n != 0 {
		t.Fatalf("%d engines listed", n)
	}
	// The engine's probe of the Cloud URL carries no credentials.
	resp, err := http.Head(h.srv.URL + "/v1/traces")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /v1/traces: %d", resp.StatusCode)
	}

	e, code := dialAs(t, h.srv, "tok-a2")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("known token: %d", code)
	}
	if env := e.helloAs("cache-x"); env.Type != protocol.TypeWelcome {
		t.Fatalf("hello answered %+v", env)
	}
	infos := h.svc.Engines()
	if len(infos) != 1 || infos[0].Namespace != "team-a" || infos[0].Token != tokenFingerprint("tok-a2") {
		t.Fatalf("engines %+v", infos)
	}
	if code := otlpPostAs(t, h.srv.URL+"/v1/logs", "tok-b", &collogspb.ExportLogsServiceRequest{}); code != http.StatusOK {
		t.Fatalf("known token's OTLP: %d", code)
	}
	// No token value is ever logged, only fingerprints.
	h.rec.mu.Lock()
	defer h.rec.mu.Unlock()
	for _, r := range h.rec.records {
		r.Attrs(func(a slog.Attr) bool {
			if v := a.Value.String(); strings.Contains(v, "tok-a") || strings.Contains(v, "nope") {
				t.Errorf("%q logged a token: %s=%s", r.Message, a.Key, v)
			}
			return true
		})
	}
}

// A token's namespace is checked with the tokens of the moment, so a token
// can't be welcomed in a namespace it no longer belongs to: one whose
// upgrade passed, but which a reload removed before its hello, is closed
// with 4001 instead of welcomed.
func TestTokenRemovedBeforeHelloNotWelcomed(t *testing.T) {
	h := newAuthHarness(t)
	e, code := dialAs(t, h.srv, "tok-b")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %d", code)
	}
	h.reload("tok-a team-a\n")
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "cache-b", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	e.send(env)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, data, err := e.conn.Read(ctx)
	if err == nil {
		t.Fatalf("got %s, want the connection closed", data)
	}
	if got := websocket.CloseStatus(err); got != protocol.CloseTokenRevoked {
		t.Fatalf("close status %d, want %d (%v)", got, protocol.CloseTokenRevoked, err)
	}
	if n := len(h.svc.Engines()); n != 0 {
		t.Fatalf("%d engines listed", n)
	}
}

// Two namespaces export the same build: the base layer is the same blob in
// both, the exec's differs. Each namespace's index records only its own
// engine's entries, and each blob is stored under its namespace's keys, so
// the shared base layer is stored twice: blobs are not shared across
// namespaces. A fresh engine of each namespace gets only its own
// namespace's entries merged, with GETs of its own namespace's blobs, and
// engines of the other namespace get no merges at all.
func TestNamespacesKeepExportsApart(t *testing.T) {
	h := newAuthHarness(t)
	a, b := fakeengine.New("cache-a", 1), fakeengine.New("cache-b", 1)
	layersA, layersB := authBuild(a, "a"), authBuild(b, "b")
	connA, connB := h.connect(a, "tok-a"), h.connect(b, "tok-b")

	idxA := h.export(a, "tok-a")
	idxB := h.export(b, "tok-b")
	if idxA.Caches["cache-b"] != nil || idxB.Caches["cache-a"] != nil {
		t.Fatalf("an index maps the other namespace's engine: A's %v, B's %v", keysOf(idxA.Caches), keysOf(idxB.Caches))
	}
	inA, inB := memIn(t, h.blobs, "team-a"), memIn(t, h.blobs, "team-b")
	base := digest.FromBytes(layersA[0])
	for name, check := range map[string]bool{
		"A's base in team-a":   hasAll(t, inA, []digest.Digest{base}),
		"B's base in team-b":   hasAll(t, inB, []digest.Digest{base}),
		"A's exec in team-a":   hasAll(t, inA, []digest.Digest{digest.FromBytes(layersA[1])}),
		"B's exec in team-b":   hasAll(t, inB, []digest.Digest{digest.FromBytes(layersB[1])}),
		"!A's exec in team-b":  !hasAll(t, inB, []digest.Digest{digest.FromBytes(layersA[1])}),
		"!B's exec in team-a":  !hasAll(t, inA, []digest.Digest{digest.FromBytes(layersB[1])}),
		"!a blob at the top":   h.blobs.Len() == 0,
		"!a blob in the scope": memIn(t, h.blobs, testScope).Len() == 0,
	} {
		if !check {
			t.Errorf("%s: no", strings.TrimPrefix(name, "!"))
		}
	}

	// A and B sync again now: nothing of the other's reaches them.
	for _, c := range []*engineConn{connA, connB} {
		res, err := h.svc.Import(t.Context(), c.id)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Merged) != 0 {
			t.Fatalf("%s: merged %+v", c.CacheID(), res.Merged)
		}
	}
	if a.Merges() != 0 || b.Merges() != 0 {
		t.Fatalf("merges into A %d, into B %d", a.Merges(), b.Merges())
	}

	// A fresh engine of each namespace pulls its own namespace's layers,
	// through its own namespace's addresses.
	for _, tc := range []struct {
		token, team string
		want        [][]byte
	}{{"tok-a2", "team-a", layersA}, {"tok-b", "team-b", layersB}} {
		fresh := fakeengine.New("fresh-"+tc.team, 1)
		c := h.connect(fresh, tc.token)
		checkPull(t, fresh, "withExec(make)", tc.want)
		for _, n := range []uint64{1000, 1001} {
			e, ok := fresh.Entry(n)
			if !ok {
				continue
			}
			checkAddresses(t, tc.team, e)
		}
		st, err := h.svc.Imports(c.id)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range st.Merged {
			want := map[string]string{"team-a": "cache-a", "team-b": "cache-b"}[tc.team]
			if m.Source != want {
				t.Fatalf("%s merged a bundle of %s", fresh.ID, m.Source)
			}
		}
	}
}

// checkAddresses checks that every address of an entry's offer is one of
// namespace ns's blobs.
func checkAddresses(t *testing.T, ns string, e fakeengine.Entry) {
	t.Helper()
	if e.Offer == nil {
		return
	}
	for d, a := range e.Offer.Chain.Addresses {
		u, err := url.Parse(a.URL)
		if err != nil {
			t.Fatal(err)
		}
		if want := "/mem/" + ns + "/" + d.String(); u.Path != want {
			t.Fatalf("entry %d: address %s, want path %s", e.Number, u.Path, want)
		}
	}
	if e.Offer.Chain.RenewalKey != ns {
		t.Fatalf("entry %d: renewal key %q, want %q", e.Number, e.Offer.Chain.RenewalKey, ns)
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// exportLayers are the chain layers of blobs, as an offer or a renew names
// them.
func exportLayers(blobs ...[]byte) []snapshots.ExportLayer {
	out := make([]snapshots.ExportLayer, len(blobs))
	for i, b := range blobs {
		out[i].Descriptor.Digest = digest.FromBytes(b)
		out[i].Descriptor.Size = int64(len(b))
		out[i].Descriptor.MediaType = "application/vnd.oci.image.layer.v1.tar+gzip"
	}
	return out
}

// An engine can't renew another namespace's blobs, whatever renewal key and
// digests it names: renew answers from the connection's own namespace, so B,
// which knows A's digests, gets no GET for them.
func TestRenewAcrossNamespacesRefused(t *testing.T) {
	h := newAuthHarness(t)
	a, b := fakeengine.New("cache-a", 1), fakeengine.New("cache-b", 1)
	layersA := authBuild(a, "a")
	h.connect(a, "tok-a")
	h.connect(b, "tok-b")
	h.export(a, "tok-a")

	ctx := t.Context()
	for _, tc := range []struct {
		key    string
		layers [][]byte
	}{
		{"team-a", layersA},        // A's own renewal key
		{"team-b", layersA},        // B's key, A's blobs
		{"team-b", layersA[1:]},    // B's key, A's exec only
		{testScope, layersA},       // the default scope's key
		{"../team-a", layersA[:1]}, // a key that is not a namespace
		{"team-a/blobs", layersA[:1]},
	} {
		addrs, ok, err := b.RenewLayers(ctx, tc.key, exportLayers(tc.layers...))
		if err != nil {
			t.Fatal(err)
		}
		if ok || len(addrs) != 0 {
			t.Fatalf("B renewed %d layers under %q: %v", len(tc.layers), tc.key, addrs)
		}
	}
}

// Renewals are not limited to the digests offered to the connection: A
// renews the layers it uploaded itself, which nobody offered it. An engine
// renews offers merged into it before a reconnect or a service restart,
// which no connection of this process offered it; and within its namespace,
// a token could have those blobs merged into its engine anyway, so tying
// renewals to offers would protect nothing. The GETs are its namespace's.
func TestRenewNotLimitedToOfferedDigests(t *testing.T) {
	h := newAuthHarness(t)
	a := fakeengine.New("cache-a", 1)
	layersA := authBuild(a, "a")
	connA := h.connect(a, "tok-a")
	h.export(a, "tok-a")
	if st, err := h.svc.Imports(connA.id); err != nil || len(st.Merged) != 0 {
		t.Fatalf("A had something merged: %+v, %v", st, err)
	}
	addrs, ok, err := a.RenewLayers(t.Context(), "team-a", exportLayers(layersA...))
	if err != nil || !ok {
		t.Fatalf("renew: %v, %v", ok, err)
	}
	checkAddresses(t, "team-a", fakeengine.Entry{Offer: &dagql.PersistedPartOffer{Chain: dagql.OfferedChain{Addresses: addrs, RenewalKey: "team-a"}}})
	if len(addrs) != 2 {
		t.Fatalf("addresses %v", addrs)
	}
}

// Engine events and spans steer what the service knows of an engine:
// forged ones must not. Engine A exports, and engine A2 has A's entries
// merged. Then:
//
//   - with no token, or an unknown one, nothing is taken (401);
//   - with team B's token, they reach only team B's namespace, which knows
//     no engine of team A;
//   - with another token of team A, an event about an engine whose latest
//     connection had another token is dropped: the service takes engine
//     events about an engine only from its own token;
//
// and A's own events about A, and A2's about A2, still change what the
// service knows.
func TestForgedTelemetryIgnored(t *testing.T) {
	h := newAuthHarness(t)
	a, a2 := fakeengine.New("cache-a", 4), fakeengine.New("cache-a2", 1)
	authBuild(a, "a")
	h.connect(a, "tok-a")
	h.export(a, "tok-a")
	connA2 := h.connect(a2, "tok-a2")
	waitUntil(t, 10*time.Second, "A's entries to be merged into A2", func() bool {
		st, err := h.svc.Imports(connA2.id)
		return err == nil && len(st.Merged) > 0 && len(st.Values) > 0
	})
	nsA := h.ns("team-a")
	a2InIndex := func(idx *metastore.Index) bool {
		c := idx.Caches["cache-a2"]
		return c != nil && len(c.Imported) > 0
	}
	waitIndexOf(t, h.meta, "team-a", "A2's imports in the index", a2InIndex)

	retained := func(n uint64) bool {
		for _, e := range nsA.tracker.Entries("cache-a") {
			if e.Number == n {
				return e.Retained
			}
		}
		t.Fatalf("entry %d not tracked", n)
		return false
	}
	importsOfA2 := func() int {
		st, ok := nsA.importer.State("cache-a2")
		if !ok {
			return -1
		}
		return len(st.Values)
	}
	valuesA2 := importsOfA2()
	if valuesA2 <= 0 || !retained(10) || !retained(11) {
		t.Fatalf("before: A2's imports %d, A's entries retained %v %v", valuesA2, retained(10), retained(11))
	}
	unchanged := func(when string) {
		t.Helper()
		if !retained(10) || !retained(11) {
			t.Fatalf("%s: A's entries no longer retained", when)
		}
		if got := importsOfA2(); got != valuesA2 {
			t.Fatalf("%s: the importer knows %d values of A2, not %d", when, got, valuesA2)
		}
		idx, err := h.meta.Index(t.Context(), "team-a")
		if err != nil {
			t.Fatal(err)
		}
		if c := idx.Caches["cache-a2"]; c == nil || len(c.Imported) == 0 {
			t.Fatalf("%s: A2's imports are gone from the index", when)
		}
	}
	a2Numbers := []uint64{}
	if st, ok := nsA.importer.State("cache-a2"); ok {
		for _, v := range st.Values {
			a2Numbers = append(a2Numbers, v.Number)
		}
	}
	wipeA2 := telemetryattrs.EngineStartEvent{WipedCache: "cache-a2"}

	// No token, or an unknown one: refused.
	for _, token := range []string{"", "nope"} {
		if code := h.postEvent(token, cacheAttr(a), telemetryattrs.EngineEventPrune, pruneOf(10, 11)); code != http.StatusUnauthorized {
			t.Fatalf("token %q: prune answered %d", token, code)
		}
		if code := h.postEvent(token, "cache-x/1", telemetryattrs.EngineEventStart, wipeA2); code != http.StatusUnauthorized {
			t.Fatalf("token %q: start answered %d", token, code)
		}
	}
	unchanged("with no token or an unknown one")

	// Team B's token: taken, in team B's namespace, where these caches are
	// strangers. Its spans don't feed team A's tracker either.
	for _, post := range []func() int{
		func() int {
			return h.postEvent("tok-b", cacheAttr(a), telemetryattrs.EngineEventPrune, pruneOf(10, 11))
		},
		func() int {
			return h.postEvent("tok-b", cacheAttr(a2), telemetryattrs.EngineEventPrune, pruneOf(a2Numbers...))
		},
		func() int { return h.postEvent("tok-b", "cache-x/1", telemetryattrs.EngineEventStart, wipeA2) },
		func() int { return h.postSpans("tok-b", a, "session-forged", cacheSpan(12), carrierSpan(1)) },
	} {
		if code := post(); code != http.StatusOK {
			t.Fatalf("team B's token: %d", code)
		}
	}
	unchanged("with team B's token")
	for _, e := range nsA.tracker.Entries("cache-a") {
		if e.Number == 12 {
			t.Fatal("team B's spans reached team A's tracker")
		}
	}

	// Team A's other tokens: an event about an engine whose connection
	// had another token is dropped, including a start that reports wiping
	// it.
	for _, post := range []func() int{
		func() int {
			return h.postEvent("tok-a2", cacheAttr(a), telemetryattrs.EngineEventPrune, pruneOf(10, 11))
		},
		func() int {
			return h.postEvent("tok-a", cacheAttr(a2), telemetryattrs.EngineEventPrune, pruneOf(a2Numbers...))
		},
		func() int { return h.postEvent("tok-a", cacheAttr(a), telemetryattrs.EngineEventStart, wipeA2) },
		func() int { return h.postEvent("tok-a2", cacheAttr(a), telemetryattrs.EngineEventStart, wipeA2) },
	} {
		if code := post(); code != http.StatusOK {
			t.Fatalf("team A's token: %d", code)
		}
	}
	// Give the index write a wipe would start a moment to land.
	time.Sleep(100 * time.Millisecond)
	unchanged("with team A's other token")
	if n := h.rec.recordsAt("engine event ignored: it came with a token other than the engine's", slog.LevelWarn); n != 4 {
		t.Fatalf("%d events logged as ignored, want 4", n)
	}

	// The engines' own tokens are heard.
	if code := h.postEvent("tok-a", cacheAttr(a), telemetryattrs.EngineEventPrune, pruneOf(11)); code != http.StatusOK {
		t.Fatalf("A's prune: %d", code)
	}
	if retained(11) || !retained(10) {
		t.Fatalf("A's own prune: retained 10 %v, 11 %v", retained(10), retained(11))
	}
	if code := h.postEvent("tok-a2", "cache-a2-new/1", telemetryattrs.EngineEventStart, wipeA2); code != http.StatusOK {
		t.Fatalf("A2's start: %d", code)
	}
	if importsOfA2() != -1 {
		t.Fatal("A2's own wipe: the importer still knows its cache")
	}
	waitIndexOf(t, h.meta, "team-a", "A2's wipe to drop its imports from the index", func(idx *metastore.Index) bool { return !a2InIndex(idx) })
}

// waitIndexOf waits until a namespace's index satisfies ok.
func waitIndexOf(t *testing.T, meta *metastore.Store, ns, what string, ok func(*metastore.Index) bool) {
	t.Helper()
	waitUntil(t, 10*time.Second, what, func() bool {
		idx, err := meta.Index(context.Background(), ns)
		if err != nil {
			t.Fatal(err)
		}
		return ok(idx)
	})
}

// Spans reach the service under the token of the client that ran the
// session, not the engine's (the engine forwards them with the client's
// Cloud credential), so spans are taken from any token of the engine's
// namespace: a session that A2's token reports for engine A exports A's
// entries. Tokens of a namespace trust each other this far.
func TestSpansTakenFromAnyTokenOfNamespace(t *testing.T) {
	h := newAuthHarness(t)
	a := fakeengine.New("cache-a", 1)
	authBuild(a, "a")
	h.connect(a, "tok-a")
	h.export(a, "tok-a2")
}

// A connection of team A's other token that says hello with engine A's
// cache ID becomes that cache's latest connection: the service then routes
// A's exports to it, and takes engine events about A from its token. The
// namespace is the trust boundary; within it, a cache ID isn't bound to a
// token. Known gap: #51.
func TestKnownGapSameNamespaceTokenClaimsCacheID(t *testing.T) {
	h := newAuthHarness(t)
	a := fakeengine.New("cache-a", 1)
	authBuild(a, "a")
	connA := h.connect(a, "tok-a")
	h.export(a, "tok-a")
	nsA := h.ns("team-a")

	e, code := dialAs(t, h.srv, "tok-a2")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %d", code)
	}
	if env := e.helloAs("cache-a"); env.Type != protocol.TypeWelcome {
		t.Fatalf("hello: %+v", env)
	}
	if c := h.svc.engineFor(nsA, "cache-a"); c == nil || c == connA {
		t.Fatal("the claiming connection is not A's latest")
	}
	if code := h.postEvent("tok-a2", cacheAttr(a), telemetryattrs.EngineEventPrune, pruneOf(10)); code != http.StatusOK {
		t.Fatalf("prune: %d", code)
	}
	for _, en := range nsA.tracker.Entries("cache-a") {
		if en.Number == 10 && en.Retained {
			t.Fatal("the gap is closed: update this test and #51")
		}
	}
}

// Removing a token from the file, and reloading it, closes its engines'
// connections with 4001, and refuses their reconnections with 401, which
// the engine retries with its backoff. Its refusals are logged at warning
// level once, then at debug level, never at error level. A token moved to
// another namespace is closed too; the others stay connected.
func TestRevokedTokenDisconnected(t *testing.T) {
	h := newAuthHarness(t)
	a, b := fakeengine.New("cache-a", 1), fakeengine.New("cache-b", 1)
	connA := h.connect(a, "tok-a")
	h.connect(b, "tok-b")
	rawB, code := dialAs(t, h.srv, "tok-b")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %d", code)
	}
	rawB.helloAs("cache-b-raw")
	rawA2, _ := dialAs(t, h.srv, "tok-a2")
	rawA2.helloAs("cache-a2")

	// tok-b goes; tok-a2 moves to team B.
	h.reload("tok-a team-a\ntok-a2 team-b\n")
	if got := rawB.closed(); got != protocol.CloseTokenRevoked {
		t.Fatalf("B closed with %d, want %d", got, protocol.CloseTokenRevoked)
	}
	if got := rawA2.closed(); got != protocol.CloseTokenRevoked {
		t.Fatalf("A2 closed with %d, want %d", got, protocol.CloseTokenRevoked)
	}
	if _, code := dialAs(t, h.srv, "tok-b"); code != http.StatusUnauthorized {
		t.Fatalf("B's reconnection answered %d", code)
	}
	// The real engine B keeps retrying (1s, then 2s), refused each time.
	waitUntil(t, 10*time.Second, "B's engine to retry twice", func() bool {
		return h.rec.recordsAt("request refused: unknown token", slog.LevelDebug) >= 2
	})
	if n := h.rec.recordsAt("request refused: unknown token", slog.LevelWarn); n != 1 {
		t.Fatalf("%d refusals logged at warning level, want 1", n)
	}
	if errs := h.rec.errorsLogged(); len(errs) > 0 {
		t.Fatalf("logged at error level: %q", errs)
	}
	infos := h.svc.Engines()
	if len(infos) != 1 || infos[0].Conn != connA.id {
		t.Fatalf("engines %+v, want only A", infos)
	}
	if n := h.rec.recordsAt("closing the engine's connection: its token was revoked", slog.LevelInfo); n != 3 {
		t.Fatalf("%d revocations logged, want 3", n)
	}
}

// A tokens file that doesn't parse changes nothing on reload: the tokens
// loaded before stay, and no connection is closed.
func TestBadTokensFileKeepsTokens(t *testing.T) {
	h := newAuthHarness(t)
	raw, _ := dialAs(t, h.srv, "tok-b")
	raw.helloAs("cache-b")
	h.writeTokens("tok-a\n")
	if err := h.svc.ReloadTokens(); err == nil {
		t.Fatal("a bad file reloaded")
	}
	if _, code := dialAs(t, h.srv, "tok-b"); code != http.StatusSwitchingProtocols {
		t.Fatalf("tok-b refused after a failed reload: %d", code)
	}
	if n := len(h.svc.Engines()); n != 1 {
		t.Fatalf("%d engines", n)
	}
}

// Without tokens, any token is taken, every engine is in the default
// namespace (the paths' scope), and OTLP requests need no credentials:
// the service behaves as before tokens existed.
func TestUnauthenticatedDefaultNamespace(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	if ns, err := h.svc.Namespace(h.engine.id); err != nil || ns != testScope {
		t.Fatalf("namespace %q, %v", ns, err)
	}
	if code := otlpPostAs(t, h.srv.URL+"/v1/logs", "", &collogspb.ExportLogsServiceRequest{}); code != http.StatusOK {
		t.Fatalf("OTLP without a token: %d", code)
	}
	if err := h.svc.ReloadTokens(); err != ErrNoTokensFile {
		t.Fatalf("reload: %v", err)
	}
}

func TestParseTokens(t *testing.T) {
	tf, err := ParseTokens([]byte(testTokens))
	if err != nil {
		t.Fatal(err)
	}
	if tf.Len() != 3 || tf.Namespaces() != 2 {
		t.Fatalf("%d tokens, %d namespaces", tf.Len(), tf.Namespaces())
	}
	for token, want := range map[string]string{"tok-a": "team-a", "tok-a2": "team-a", "tok-b": "team-b", "tok-c": "", "": "", "tok-a ": "", "TOK-A": ""} {
		got, ok := tf.Lookup(token)
		if got != want || ok != (want != "") {
			t.Errorf("%q: %q, %v; want %q", token, got, ok, want)
		}
	}
	for _, bad := range []string{
		"tok-a\n",                      // no namespace
		"tok-a team-a extra\n",         // three fields
		"tok-a team/a\n",               // not one key segment
		"tok-a ..\n",                   // not a namespace
		"tok-a blobs\n",                // reserved
		"tok-a team-a\ntok-a team-b\n", // twice
		"tok:a team-a\n",               // HTTP Basic ends the username at ':'
	} {
		_, err := ParseTokens([]byte(bad))
		if err == nil {
			t.Errorf("%q parsed", bad)
			continue
		}
		if strings.Contains(err.Error(), "tok-a") {
			t.Errorf("%q: the error names the token: %v", bad, err)
		}
	}
	if tf, err := ParseTokens(nil); err != nil || tf.Len() != 0 {
		t.Fatalf("empty: %v", err)
	}
}

// Refusals are logged at warning level once per token a minute, and at
// most rejectionWarnings times a minute in all.
func TestRejectionLog(t *testing.T) {
	var l rejectionLog
	t0 := time.Now()
	if !l.allow("x", t0) || l.allow("x", t0.Add(time.Second)) || l.allow("x", t0.Add(59*time.Second)) {
		t.Fatal("a token's refusals within a minute")
	}
	if !l.allow("x", t0.Add(61*time.Second)) {
		t.Fatal("a token's refusal a minute later")
	}
	t1 := t0.Add(10 * time.Minute)
	n := 0
	for i := range 3 * rejectionWarnings {
		if l.allow(strconv.Itoa(i), t1) {
			n++
		}
	}
	if n != rejectionWarnings {
		t.Fatalf("%d of many tokens' refusals logged at warning level, want %d", n, rejectionWarnings)
	}
}

// Event kinds the service doesn't act on are ignored before the owner check,
// so a token can't make up kinds that name metric series: only engine.start
// and engine.prune are ever counted as ignored.
func TestUnknownEventKindsNotCounted(t *testing.T) {
	h := newAuthHarness(t)
	a := fakeengine.New("cache-a", 1)
	h.connect(a, "tok-a")
	for i := range 20 {
		if code := h.postEvent("tok-a2", cacheAttr(a), "made-up-"+strconv.Itoa(i), pruneOf(10)); code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
	}
	if n := h.rec.recordsAt("engine event ignored: it came with a token other than the engine's", slog.LevelWarn); n != 0 {
		t.Fatalf("%d made-up events logged as ignored", n)
	}
	resp := httptest.NewRecorder()
	h.svc.MetricsHandler().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := resp.Body.String(); strings.Contains(body, "made-up") {
		t.Fatal("a made-up event kind names a metric series")
	}
}

// hiddenStore is a blob store that can't keep namespaces apart.
type hiddenStore struct{ blobstore.Store }

// The export and import paths refuse a blob store that can't keep
// namespaces apart: sharing one would let a namespace renew, be offered,
// or skip uploading another's blobs.
func TestEnableRefusesStoreWithoutNamespaces(t *testing.T) {
	blobs, meta := newMemStores(t)
	store := hiddenStore{blobs}
	ctx := t.Context()
	svc := New(slog.New(slog.DiscardHandler), "")
	if err := svc.EnableExport(ctx, ExportConfig{Exporter: exporter.Config{Blobs: store, Meta: meta, Scope: testScope}}); err == nil {
		t.Fatal("the export path took a store without namespaces")
	}
	if err := svc.EnableImport(ctx, ImportConfig{Importer: importer.Config{Blobs: store, Meta: meta, Scope: testScope}}); err == nil {
		t.Fatal("the import path took a store without namespaces")
	}
	svc.Wait()
}

// A revoked connection gets nothing more of its namespace while its close
// handshake runs (up to 5s with an engine that doesn't answer it): it is
// marked revoked and its import loop stops, cancelling a merge in flight,
// before the reload returns, and its renewals are refused.
func TestRevocationStopsImportsAtOnce(t *testing.T) {
	h := newAuthHarness(t)
	a := fakeengine.New("cache-a", 1)
	layersA := authBuild(a, "a")
	h.connect(a, "tok-a")
	h.export(a, "tok-a")
	nsA := h.ns("team-a")

	// An engine of team A that takes the merge of A's entries and then
	// reads nothing more: neither the merge's reply nor the close frame
	// ever comes back.
	raw, code := dialAs(t, h.srv, "tok-a2")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %d", code)
	}
	raw.helloAs("cache-r")
	if env := raw.next(); env.Type != protocol.TypeMerge {
		t.Fatalf("got %s, want the merge of A's entries", env.Type)
	}
	c := h.svc.engineFor(nsA, "cache-r")
	if c == nil {
		t.Fatal("not connected")
	}
	looping := func() bool {
		p := h.svc.imports
		p.mu.Lock()
		defer p.mu.Unlock()
		_, ok := p.triggers[c]
		return ok
	}
	if !looping() {
		t.Fatal("no import loop")
	}

	h.reload("tok-a team-a\ntok-b team-b\n")
	if !c.revoked.Load() {
		t.Fatal("not marked revoked when the reload returned")
	}
	waitUntil(t, time.Second, "the import loop to stop, well before the close handshake times out", func() bool { return !looping() })
	renewed := h.svc.renew(t.Context(), c, protocol.Renew{RenewalKey: "team-a", Layers: exportLayers(layersA...)})
	if !renewed.Unavailable || len(renewed.Addresses) != 0 {
		t.Fatalf("a revoked connection renewed: %+v", renewed)
	}
	if res := h.svc.syncImports(t.Context(), c); len(res.Merged) != 0 {
		t.Fatalf("a revoked connection synced: %+v", res)
	}
}

// blockingTokens holds the first lookup of one token until released, to
// interleave token reloads.
type blockingTokens struct {
	Tokens
	token   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingTokens) Lookup(token string) (string, bool) {
	if token == b.token {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.Tokens.Lookup(token)
}

// Token reloads run one at a time: one that replaces the tokens while
// another is still revoking waits for it, so no connection is revoked by a
// set of tokens another reload has already replaced.
func TestTokenReloadsSerialized(t *testing.T) {
	h := newAuthHarness(t)
	raw, _ := dialAs(t, h.srv, "tok-b")
	raw.helloAs("cache-b")

	without, err := ParseTokens([]byte("tok-a team-a\n"))
	if err != nil {
		t.Fatal(err)
	}
	with, err := ParseTokens([]byte(testTokens))
	if err != nil {
		t.Fatal(err)
	}
	first := &blockingTokens{Tokens: without, token: "tok-b", entered: make(chan struct{}), release: make(chan struct{})}
	doneFirst, doneSecond := make(chan int, 1), make(chan int, 1)
	go func() { doneFirst <- h.svc.SetTokens(first) }()
	<-first.entered // the first is checking tok-b's connection
	go func() { doneSecond <- h.svc.SetTokens(with) }()
	select {
	case <-doneSecond:
		t.Fatal("a second SetTokens ran while the first was revoking")
	case <-time.After(200 * time.Millisecond):
	}
	close(first.release)
	if n := <-doneFirst; n != 1 {
		t.Fatalf("the first SetTokens revoked %d connections, want 1", n)
	}
	if n := <-doneSecond; n != 0 {
		t.Fatalf("the second SetTokens revoked %d connections, want 0", n)
	}
	if got := raw.closed(); got != protocol.CloseTokenRevoked {
		t.Fatalf("closed with %d", got)
	}
	// The tokens in effect are the second set's, which has tok-b.
	if _, code := dialAs(t, h.srv, "tok-b"); code != http.StatusSwitchingProtocols {
		t.Fatalf("tok-b refused after the second set: %d", code)
	}
}

// Dump file names carry what engines send (an event's kind, an envelope's
// type), so they can't name a path: an event kind of "../" segments is
// dumped inside the dump directory, and nothing is written outside it.
func TestDumpNamesStayInDumpDir(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "dump")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	_, _, srv := newTestService(t, dir)
	kind := "../../escaped"
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Body:       strValue(`{"forged":true}`),
			Attributes: []*commonpb.KeyValue{kv(telemetryattrs.EngineEventAttr, kind), kv(telemetryattrs.EngineCacheAttr, "c/1")},
		}}}},
	}}}
	if code := otlpPostAs(t, srv.URL+"/v1/logs", "any", req); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "dump" {
		t.Fatalf("written outside the dump directory: %v", entries)
	}
	dumped, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dumped) != 1 || dumped[0].Name() != "000001-event-.._.._escaped.json" {
		t.Fatalf("dumped %v", dumped)
	}
	if got := dumpName("a/b\\c d\x00é"); got != "a_b_c_d___" {
		t.Fatalf("dumpName: %q", got)
	}
}

// Without a store, the default namespace is still the one the command's
// -scope names (SetDefaultNamespace).
func TestDefaultNamespaceWithoutStore(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	if err := svc.SetDefaultNamespace("team"); err != nil {
		t.Fatal(err)
	}
	raw, code := dialAs(t, srv, "anything")
	if code != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %d", code)
	}
	raw.helloAs("cache-x")
	if infos := svc.Engines(); len(infos) != 1 || infos[0].Namespace != "team" {
		t.Fatalf("engines %+v", infos)
	}
	if err := svc.SetDefaultNamespace("bad/scope"); err == nil {
		t.Fatal("a bad scope was taken")
	}
}

// A namespace keeps the owners of its live connections, and of caches whose
// connections ended within ownerIdleTTL (an engine that reconnects, or whose
// telemetry arrives late, is still spoken for only by its token); older idle
// owners go, and at most maxIdleOwners idle ones stay, the longest idle going
// first. So a token that says hello with ever new cache IDs leaves bounded
// state.
func TestOwnersBounded(t *testing.T) {
	prevTTL, prevMax := ownerIdleTTL, maxIdleOwners
	ownerIdleTTL, maxIdleOwners = time.Hour, 3
	t.Cleanup(func() { ownerIdleTTL, maxIdleOwners = prevTTL, prevMax })
	clock := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	n := &namespace{name: "team-a", owners: map[string]*owner{}, now: func() time.Time { return clock }}
	a, b := keyOf("tok-a"), keyOf("tok-b")

	n.own("live", a) // stays connected throughout
	n.own("gone", a)
	n.release("gone")
	clock = clock.Add(30 * time.Minute)
	if n.ownerAccepts("gone", b) || !n.ownerAccepts("gone", a) {
		t.Fatal("an owner idle for 30 minutes no longer speaks for its cache")
	}
	clock = clock.Add(31 * time.Minute)
	n.own("other", a) // sweeps
	if _, ok := n.owners["gone"]; ok {
		t.Fatal("an owner idle for over an hour was kept")
	}

	// Many short connections with new cache IDs: at most 3 idle owners
	// stay, the newest, besides the live ones.
	for i := range 50 {
		clock = clock.Add(time.Second)
		id := "churn-" + strconv.Itoa(i)
		n.own(id, b)
		n.release(id)
	}
	idle := 0
	for id, o := range n.owners {
		if o.live == 0 {
			idle++
			if id != "churn-49" && id != "churn-48" && id != "churn-47" && id != "churn-46" {
				t.Errorf("kept idle owner %s", id)
			}
		}
	}
	if idle > 4 {
		t.Fatalf("%d idle owners kept", idle)
	}
	if !n.ownerAccepts("live", a) || n.ownerAccepts("live", b) || n.owners["other"] == nil {
		t.Fatal("a live owner was dropped")
	}
}

// A real connection's end releases its ownership, and its token still
// speaks for the cache after it disconnects (within ownerIdleTTL).
func TestOwnerReleasedOnDisconnect(t *testing.T) {
	h := newAuthHarness(t)
	raw, _ := dialAs(t, h.srv, "tok-a")
	raw.helloAs("cache-r")
	ns := h.ns("team-a")
	ns.mu.Lock()
	live := ns.owners["cache-r"].live
	ns.mu.Unlock()
	if live != 1 {
		t.Fatalf("live %d", live)
	}
	_ = raw.conn.CloseNow()
	waitUntil(t, 5*time.Second, "the owner to be released", func() bool {
		ns.mu.Lock()
		defer ns.mu.Unlock()
		return ns.owners["cache-r"].live == 0
	})
	if ns.ownerAccepts("cache-r", keyOf("tok-a2")) {
		t.Fatal("another token speaks for a cache that just disconnected")
	}
}

// Revocation racing registration: a connection whose token a reload removes
// while it says hello is either refused at hello or revoked by the reload;
// once both are done, it is never listed, and its import loop is gone.
func TestRevocationRacingRegistration(t *testing.T) {
	h := newAuthHarness(t)
	without, err := ParseTokens([]byte("tok-a team-a\n"))
	if err != nil {
		t.Fatal(err)
	}
	with, err := ParseTokens([]byte(testTokens))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		h.svc.SetTokens(with)
		raw, code := dialAs(t, h.srv, "tok-b")
		if code != http.StatusSwitchingProtocols {
			t.Fatalf("round %d: upgrade %d", i, code)
		}
		env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "cache-race-" + strconv.Itoa(i), Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.svc.SetTokens(without)
		}()
		raw.send(env)
		<-done
		// Read until the service closes the connection: a welcome may
		// come first, when the reload ran after register.
		if got := raw.closed(); got != protocol.CloseTokenRevoked {
			t.Fatalf("round %d: closed with %d, want %d", i, got, protocol.CloseTokenRevoked)
		}
		for _, info := range h.svc.Engines() {
			if info.Token == tokenFingerprint("tok-b") {
				t.Fatalf("round %d: a revoked token's connection is listed: %+v", i, info)
			}
		}
		waitUntil(t, 5*time.Second, "no import loop of tok-b", func() bool {
			p := h.svc.imports
			p.mu.Lock()
			defer p.mu.Unlock()
			for c := range p.triggers {
				if c.secret == "tok-b" {
					return false
				}
			}
			return true
		})
	}
}
