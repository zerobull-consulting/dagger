package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// What real engines send: a cache identity from uuid.NewString (dagql's
// Cache.openIdentity), and a session ID from crypto/rand.Text (the engine
// client's).
const (
	realCacheID = "0b6e5a51-7c1e-4d0a-9f3c-2b8d1e6f4a7c"
	realSession = "KZ3QF6M2XH7TDLWJ4YNRBV5PCA"
)

func TestValidEngineID(t *testing.T) {
	for _, c := range []struct {
		id string
		ok bool
	}{
		{realCacheID, true},
		{realSession, true},
		{"cache-a.b_c", true},
		{strings.Repeat("a", MaxEngineIDLen), true},
		{"", false},
		{strings.Repeat("a", MaxEngineIDLen+1), false},
		{"a/b", false},
		{"a b", false},
		{"a\nb", false},
		{"a:b", false},
		{"é", false},
	} {
		require.Equal(t, c.ok, validEngineID(c.id), "%q", c.id)
	}
}

// postResourceSpans posts spans under a resource naming cache (the whole
// "<cacheID>/<generation>") and session, either of which may be malformed.
func postResourceSpans(t *testing.T, url, cache, session string, spans ...*tracepb.Span) {
	t.Helper()
	attrs := []*commonpb.KeyValue{kv(telemetryattrs.EngineCacheAttr, cache)}
	if session != "" {
		attrs = append(attrs, kv(telemetryattrs.EngineSessionAttr, session))
	}
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   &resourcepb.Resource{Attributes: attrs},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
	if code := otlpPost(t, url+"/v1/traces", req, &coltracepb.ExportTraceServiceResponse{}, false, false); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
}

func engineIDsRefused(t *testing.T, svc *Service, source string) float64 {
	t.Helper()
	return metricstest.Value(t, svc.registry, "cacheservice_engine_ids_refused_total", map[string]string{"source": source})
}

// Spans whose cache ID or session isn't a valid engine ID key no tracker
// state, and are counted; those of real engines' IDs are tracked
// (zerobull-consulting/dagger#88).
func TestTrackSpansValidatesEngineIDs(t *testing.T) {
	long := strings.Repeat("x", MaxEngineIDLen+1)
	for _, c := range []struct {
		name, cacheID, session string
		tracked                bool
	}{
		{"real engine IDs", realCacheID, realSession, true},
		{"no session", "cache-nosession", "", true},
		{"longest cache ID", strings.Repeat("c", MaxEngineIDLen), realSession, true},
		{"longest session", "cache-longsession", strings.Repeat("s", MaxEngineIDLen), true},
		{"over-long cache ID", long, realSession, false},
		{"malformed cache ID", "bad cache!", realSession, false},
		{"cache ID with a slash", "a/b", realSession, false},
		{"over-long session", "cache-badsession", long, false},
		{"malformed session", "cache-badsession2", "bad session\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := newMemExportHarness(t, nil)
			before := engineIDsRefused(t, h.svc, "span")
			postResourceSpans(t, h.srv.URL, c.cacheID+"/1", c.session, cacheSpan(5), cacheSpan(6))
			entries := h.ns().tracker.Entries(c.cacheID)
			refused := engineIDsRefused(t, h.svc, "span") - before
			if c.tracked {
				require.Len(t, entries, 2)
				require.Zero(t, refused)
			} else {
				require.Empty(t, entries)
				require.Equal(t, 2.0, refused, "each span dropped is counted")
			}
		})
	}
}

// A resource with no engine cache attribute isn't an engine session's: its
// spans are neither tracked nor counted as refused.
func TestTrackSpansWithoutCacheNotCounted(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{cacheSpan(5)}}},
	}}}
	require.Equal(t, http.StatusOK, otlpPost(t, h.srv.URL+"/v1/traces", req, &coltracepb.ExportTraceServiceResponse{}, false, false))
	require.Zero(t, engineIDsRefused(t, h.svc, "span"))
}

// An engine event whose cache ID isn't a valid engine ID is dropped and
// counted; a start naming an invalid wiped cache is taken without it.
func TestTrackEventValidatesEngineIDs(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	sawEvent := func(cacheID string) bool {
		ns := h.ns()
		ns.mu.Lock()
		defer ns.mu.Unlock()
		return ns.eventCaches[cacheID] != nil
	}
	long := strings.Repeat("x", MaxEngineIDLen+1)
	for _, bad := range []string{long, "bad cache!"} {
		require.Equal(t, http.StatusOK, postEngineEvent(t, h.srv, "", bad, telemetryattrs.EngineEventPrune, `{"drops":[]}`))
		require.False(t, sawEvent(bad))
	}
	require.Equal(t, 2.0, engineIDsRefused(t, h.svc, "event"))

	require.Equal(t, http.StatusOK, postEngineEvent(t, h.srv, "", realCacheID, telemetryattrs.EngineEventStart, `{"wipedCache":"`+long+`"}`))
	require.True(t, sawEvent(realCacheID), "the start is taken")
	require.Equal(t, 3.0, engineIDsRefused(t, h.svc, "event"), "its wiped cache is counted")
}

// A hello whose cache ID isn't a valid engine ID is answered with an error
// and never registered.
func TestHelloValidatesCacheID(t *testing.T) {
	for _, id := range []string{"", strings.Repeat("x", MaxEngineIDLen+1), "bad cache!"} {
		svc, _, srv := newTestService(t, "")
		e := dialRaw(t, srv, "tok")
		env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{EngineVersion: testEngineVersion, CacheID: id, Generation: 1, EngineName: "e"})
		require.NoError(t, err)
		e.send(env)
		reply := e.next()
		require.Equal(t, protocol.TypeError, reply.Type, "%q: hello answered with %+v", id, reply)
		require.Equal(t, "invalid cache ID", decodeBody[protocol.Error](t, reply).Message)
		require.Empty(t, svc.Engines())
		require.Equal(t, 1.0, engineIDsRefused(t, svc, "hello"))
	}
	// A real engine's is welcomed.
	svc, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{EngineVersion: testEngineVersion, CacheID: realCacheID, Generation: 1, EngineName: "e"})
	require.NoError(t, err)
	e.send(env)
	require.Equal(t, protocol.TypeWelcome, e.next().Type)
	waitUntil(t, 5*time.Second, "the engine registered", func() bool { return len(svc.Engines()) == 1 })
	require.Zero(t, engineIDsRefused(t, svc, "hello"))
}
