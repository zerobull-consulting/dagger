package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// testOTLPLimit is the route limit the tests set, small enough to build
// bodies at it quickly; the defaults are checked on their own
// (TestOTLPDefaultLimits).
const testOTLPLimit = 64 << 10

// otlpCase is one OTLP route, with a request for it padded by pad, and the
// log message that shows the service recorded it.
type otlpCase struct {
	path     string
	route    string
	build    func(pad string) proto.Message
	recorded string
}

func otlpCases() []otlpCase {
	padKV := func(pad string) *commonpb.KeyValue { return kv("pad", pad) }
	return []otlpCase{
		{
			path: "/v1/traces", route: metrics.OTLPRouteTraces, recorded: "otlp traces",
			build: func(pad string) proto.Message {
				return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
					Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv(telemetryattrs.EngineCacheAttr, "cache-1"), padKV(pad)}},
					ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
						{Name: "cached", Attributes: []*commonpb.KeyValue{kv(telemetryattrs.CacheResultIDAttr, "42")}},
					}}},
				}}}
			},
		},
		{
			path: "/v1/logs", route: metrics.OTLPRouteLogs, recorded: "engine event",
			build: func(pad string) proto.Message {
				req := engineLogs()
				req.ResourceLogs[0].Resource.Attributes = append(req.ResourceLogs[0].Resource.Attributes, padKV(pad))
				return req
			},
		},
		{
			path: "/v1/metrics", route: metrics.OTLPRouteMetrics, recorded: "otlp metrics",
			build: func(pad string) proto.Message {
				return &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
					Resource:     &resourcepb.Resource{Attributes: []*commonpb.KeyValue{padKV(pad)}},
					ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{Name: "a"}}}},
				}}}
			},
		},
	}
}

// encodeOTLP encodes msg as protobuf or JSON.
func encodeOTLP(t *testing.T, msg proto.Message, asJSON bool) []byte {
	t.Helper()
	var (
		data []byte
		err  error
	)
	if asJSON {
		data, err = protojson.Marshal(msg)
	} else {
		data, err = proto.Marshal(msg)
	}
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// sized encodes build's request, padded so that it is exactly size bytes.
func sized(t *testing.T, build func(string) proto.Message, asJSON bool, size int) []byte {
	t.Helper()
	pad := 0
	for range 10 {
		data := encodeOTLP(t, build(strings.Repeat("x", pad)), asJSON)
		if len(data) == size {
			return data
		}
		pad += size - len(data)
		if pad < 0 {
			t.Fatalf("a request of %d bytes is too small for its content", size)
		}
	}
	t.Fatalf("no padding makes the request %d bytes", size)
	return nil
}

func gzipped(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// postBody posts data to url as it is, with the token, if any, as the Basic
// username, and returns the response, its body read.
func postBody(t *testing.T, url string, data []byte, asJSON, gz bool, token string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/x-protobuf")
	if asJSON {
		req.Header.Set("Content-Type", "application/json")
	}
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if token != "" {
		req.SetBasicAuth(token, "")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

// limitedService is a test service whose OTLP routes take testOTLPLimit
// bytes, with a budget of budget bytes.
func limitedService(t *testing.T, budget int64) (*Service, *logRecorder, *httptest.Server) {
	t.Helper()
	svc, rec, srv := newTestService(t, "")
	setOTLPLimits(svc, testOTLPLimit, budget)
	return svc, rec, srv
}

// setOTLPLimits sets every route's limit and the budget, before the service
// takes requests.
func setOTLPLimits(svc *Service, limit, budget int64) {
	svc.otlp.traces.limit = limit
	svc.otlp.logs.limit = limit
	svc.otlp.metrics.limit = limit
	svc.otlp.budget = newByteBudget(budget)
}

func refusedCount(t *testing.T, svc *Service, route, reason string) float64 {
	t.Helper()
	return metricstest.Value(t, svc.registry, "cacheservice_otlp_refused_total", map[string]string{"route": route, "reason": reason})
}

// The service is built with the documented limits, and serves every listener
// with a header timeout and no request timeout, which would cut off the
// engine WebSocket.
func TestOTLPDefaultLimits(t *testing.T) {
	svc := New(nil, "")
	lim := svc.otlp
	if lim.traces.limit != MaxOTLPTracesBytes || lim.logs.limit != MaxOTLPLogsBytes || lim.metrics.limit != MaxOTLPMetricsBytes {
		t.Fatalf("route limits %+v", lim)
	}
	if lim.budget.size != OTLPDecodeBudget || lim.readTimeout != OTLPReadTimeout {
		t.Fatalf("budget %d, read timeout %s", lim.budget.size, lim.readTimeout)
	}
	// The budget holds an at-limit request of every route.
	if OTLPDecodeBudget < max(MaxOTLPTracesBytes, MaxOTLPLogsBytes, MaxOTLPMetricsBytes) {
		t.Fatal("the budget is smaller than a route's limit")
	}
	srv := NewHTTPServer(":0", svc.Handler())
	if srv.MaxHeaderBytes != MaxHeaderBytes {
		t.Fatalf("max header bytes %d", srv.MaxHeaderBytes)
	}
	if srv.ReadHeaderTimeout != ReadHeaderTimeout || srv.ReadTimeout != 0 || srv.WriteTimeout != 0 {
		t.Fatalf("server timeouts: header %s, read %s, write %s", srv.ReadHeaderTimeout, srv.ReadTimeout, srv.WriteTimeout)
	}
}

// A body at the limit is taken and recorded on every route, protobuf and
// JSON, plain and gzipped (the decoded size at the limit); one byte over is
// answered 413, and nothing of it is recorded.
func TestOTLPBodyAtAndOverLimit(t *testing.T) {
	for _, c := range otlpCases() {
		for _, asJSON := range []bool{false, true} {
			for _, gz := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s,json=%t,gzip=%t", c.route, asJSON, gz), func(t *testing.T) {
					svc, rec, srv := limitedService(t, 4*testOTLPLimit)
					at := sized(t, c.build, asJSON, testOTLPLimit)
					over := sized(t, c.build, asJSON, testOTLPLimit+1)
					if gz {
						at, over = gzipped(t, at), gzipped(t, over)
					}
					if resp := postBody(t, srv.URL+c.path, at, asJSON, gz, ""); resp.StatusCode != http.StatusOK {
						t.Fatalf("at the limit: %d", resp.StatusCode)
					}
					if n := len(rec.find(c.recorded)); n != 1 {
						t.Fatalf("at the limit: %d %q logged, want 1", n, c.recorded)
					}
					resp := postBody(t, srv.URL+c.path, over, asJSON, gz, "")
					if resp.StatusCode != http.StatusRequestEntityTooLarge {
						t.Fatalf("one byte over: %d", resp.StatusCode)
					}
					if !resp.Close {
						t.Error("the connection of a refused request stays open")
					}
					if n := len(rec.find(c.recorded)); n != 1 {
						t.Fatalf("one byte over: %d %q logged, want only the first", n, c.recorded)
					}
					if got := refusedCount(t, svc, c.route, metrics.OTLPRefusedTooLarge); got != 1 {
						t.Fatalf("refused %v", got)
					}
					if used := svc.otlp.budget.inUse(); used != 0 {
						t.Fatalf("%d bytes of the budget still reserved", used)
					}
				})
			}
		}
	}
}

// countingReadCloser counts the bytes read through it: a decompressed stream,
// or a request body.
type countingReadCloser struct {
	io.ReadCloser
	mu sync.Mutex
	n  int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.mu.Lock()
	c.n += int64(n)
	c.mu.Unlock()
	return n, err
}

func (c *countingReadCloser) count() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// A gzip bomb, a few kilobytes that expand far past the limit, is answered
// 413 on every route, and the service reads at most one byte past the limit
// from the decompressed stream: what it holds is bounded by the limit, not by
// what the body expands to.
func TestOTLPGzipBomb(t *testing.T) {
	// 16 MiB of zeros, 256 times the limit, compress to about 16 KiB, well
	// within the limit as received.
	bomb := gzipped(t, make([]byte, 16<<20))
	if len(bomb) > testOTLPLimit/2 {
		t.Fatalf("the bomb is %d bytes compressed", len(bomb))
	}
	for _, c := range otlpCases() {
		t.Run(c.route, func(t *testing.T) {
			svc, rec, srv := limitedService(t, 4*testOTLPLimit)
			var counted *countingReadCloser
			svc.otlp.gunzip = func(r io.Reader) (io.ReadCloser, error) {
				gz, err := gunzipReader(r)
				if err != nil {
					return nil, err
				}
				counted = &countingReadCloser{ReadCloser: gz}
				return counted, nil
			}
			resp := postBody(t, srv.URL+c.path, bomb, false, true, "")
			if resp.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("status %d", resp.StatusCode)
			}
			if counted == nil {
				t.Fatal("the body was never gunzipped")
			}
			if n := counted.count(); n > testOTLPLimit+1 {
				t.Fatalf("read %d bytes from the decompressed stream, limit %d", n, testOTLPLimit)
			}
			if n := len(rec.find(c.recorded)); n != 0 {
				t.Fatalf("%d %q logged", n, c.recorded)
			}
			if got := refusedCount(t, svc, c.route, metrics.OTLPRefusedTooLarge); got != 1 {
				t.Fatalf("refused %v", got)
			}
		})
	}
}

// A gzipped body over the limit as received is answered 413 too, even if it
// would decode to less (the limit on the body as sent, http.MaxBytesReader).
func TestOTLPGzippedBodyOverLimitAsReceived(t *testing.T) {
	random := make([]byte, testOTLPLimit)
	_, _ = rand.Read(random)
	// Random bytes don't compress: gzipped, they are a little larger.
	body := gzipped(t, random)
	if len(body) <= testOTLPLimit {
		t.Fatalf("gzipped body of %d bytes", len(body))
	}
	for _, c := range otlpCases() {
		svc, _, srv := limitedService(t, 4*testOTLPLimit)
		var counted *countingReadCloser
		svc.otlp.gunzip = func(r io.Reader) (io.ReadCloser, error) {
			gz, err := gunzipReader(r)
			if err != nil {
				return nil, err
			}
			counted = &countingReadCloser{ReadCloser: gz}
			return counted, nil
		}
		if resp := postBody(t, srv.URL+c.path, body, false, true, ""); resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s: status %d", c.route, resp.StatusCode)
		}
		// The decoded stream ends short of the limit: the raw limit stopped
		// it.
		if counted == nil || counted.count() > testOTLPLimit {
			t.Fatalf("%s: decoded stream read %v", c.route, counted)
		}
	}
}

// An over-limit request is dropped whole, never partly recorded: a body
// whose first bytes are a valid batch of spans, a session's cache span and
// its end, followed by more than the limit allows, tracks nothing. The
// service decodes only a body read whole, so this pins that no partial
// decode reaches the tracker.
func TestOTLPOverLimitTracksNothing(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	setOTLPLimits(h.svc, testOTLPLimit, 4*testOTLPLimit)
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			kv(telemetryattrs.EngineCacheAttr, h.cache.ID+"/"+strconv.FormatUint(h.cache.Generation, 10)),
			kv(telemetryattrs.EngineSessionAttr, "s1"),
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{cacheSpan(7), carrierSpan(1)}}},
	}}}
	valid := encodeOTLP(t, req, false)
	// Protobuf concatenates: the valid batch, then the same batch again
	// until the body is over the limit, still a valid message as a whole.
	var body []byte
	for len(body) <= testOTLPLimit {
		body = append(body, valid...)
	}
	for _, gz := range []bool{false, true} {
		data := body
		if gz {
			data = gzipped(t, body)
		}
		if resp := postBody(t, h.srv.URL+"/v1/traces", data, false, gz, ""); resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("gzip=%t: status %d", gz, resp.StatusCode)
		}
	}
	entries, err := h.svc.Entries(h.engine.id)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("an over-limit request tracked %+v", entries)
	}
	// The same batch, within the limit, is tracked.
	if resp := postBody(t, h.srv.URL+"/v1/traces", valid, false, false, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	waitUntil(t, 5*time.Second, "the batch tracked", func() bool {
		entries, err := h.svc.Entries(h.engine.id)
		return err == nil && len(entries) == 1
	})
}

// With tokens, a request with no token, or an unknown one, is refused with
// 401 before any of its body is read: an unauthenticated client can't make
// the service read, decompress or hold anything.
func TestOTLPUnauthenticatedBodyNotRead(t *testing.T) {
	h := newAuthHarness(t)
	handler := h.svc.Handler()
	for _, c := range otlpCases() {
		for _, token := range []string{"", "tok-unknown"} {
			for _, gz := range []bool{false, true} {
				data := make([]byte, 1<<20)
				if gz {
					data = gzipped(t, data)
				}
				body := &countingReadCloser{ReadCloser: io.NopCloser(bytes.NewReader(data))}
				req := httptest.NewRequest(http.MethodPost, c.path, body)
				req.Header.Set("Content-Type", "application/x-protobuf")
				if gz {
					req.Header.Set("Content-Encoding", "gzip")
				}
				if token != "" {
					req.SetBasicAuth(token, "")
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, req)
				if w.Code != http.StatusUnauthorized {
					t.Fatalf("%s, token %q: status %d", c.path, token, w.Code)
				}
				if n := body.count(); n != 0 {
					t.Fatalf("%s, token %q: %d bytes of the body read", c.path, token, n)
				}
			}
		}
	}
	// A known token's request is read and taken.
	if resp := postBody(t, h.srv.URL+"/v1/metrics", encodeOTLP(t, otlpCases()[2].build(""), false), false, false, "tok-a"); resp.StatusCode != http.StatusOK {
		t.Fatalf("with a token: %d", resp.StatusCode)
	}
}

// slowPost starts a POST of a declared length to srv on a raw connection,
// and sends sent bytes of its body; the caller sends the rest, or none.
func slowPost(t *testing.T, srv *httptest.Server, path string, length int, sent []byte) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	// A response that never comes fails the test rather than hanging it.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: test\r\nContent-Type: application/x-protobuf\r\nContent-Length: %d\r\n\r\n", path, length)
	if _, err := conn.Write(sent); err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

func readResponse(t *testing.T, br *bufio.Reader) *http.Response {
	t.Helper()
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

// The bodies being read at once share the decode budget: a request that
// doesn't fit in what is left is answered 503 with Retry-After, which OTLP
// exporters retry, records nothing, and is taken once the budget frees.
// Requests well within it are never refused, however many: only bytes count.
//
// Sizes: the budget is 152 KiB, and every request reserves 16 KiB for
// itself, and a few hundred bytes more for its headers (otlpHeaderReserve).
// The held request holds 48 KiB (itself, and a 32 KiB buffer for the
// 20 KiB it sent), and needs about 147 KiB to complete (a 64 KiB buffer and
// its message). A 40 KiB body needs 112 KiB as its buffer grows to 64 KiB,
// and 122 KiB with its message: it fits alone, not beside the held 48 KiB.
func TestOTLPDecodeBudget(t *testing.T) {
	cases := otlpCases()
	svc, rec, srv := limitedService(t, 152<<10)
	// A request that holds half the budget while its body arrives: 20 KiB
	// sent, into a buffer of 32 KiB reserved.
	big := sized(t, cases[2].build, false, testOTLPLimit)
	held := 20 << 10
	conn, br := slowPost(t, srv, "/v1/metrics", len(big), big[:held])
	waitUntil(t, 5*time.Second, "the first body to be reserved", func() bool { return svc.otlp.budget.inUse() >= otlpRequestReserve+32<<10 })

	for _, c := range cases {
		over := sized(t, c.build, false, 40<<10)
		resp := postBody(t, srv.URL+c.path, over, false, false, "")
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != strconv.Itoa(otlpRetryAfter) {
			t.Fatalf("%s past the budget: %d, Retry-After %q", c.route, resp.StatusCode, resp.Header.Get("Retry-After"))
		}
		if got := refusedCount(t, svc, c.route, metrics.OTLPRefusedBusy); got != 1 {
			t.Fatalf("%s: busy %v", c.route, got)
		}
		// Small requests still fit, however many.
		for range 20 {
			if resp := postBody(t, srv.URL+c.path, encodeOTLP(t, c.build(""), false), false, false, ""); resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: a small request: %d", c.route, resp.StatusCode)
			}
		}
	}
	if n := len(rec.find("otlp metrics")); n != 20 {
		t.Fatalf("%d metric requests recorded, want the 20 small ones", n)
	}

	// The first request completes, and frees the budget.
	if _, err := conn.Write(big[held:]); err != nil {
		t.Fatal(err)
	}
	if resp := readResponse(t, br); resp.StatusCode != http.StatusOK {
		t.Fatalf("the held request: %d", resp.StatusCode)
	}
	waitUntil(t, 5*time.Second, "the budget to be free", func() bool { return svc.otlp.budget.inUse() == 0 })
	for _, c := range cases {
		if resp := postBody(t, srv.URL+c.path, sized(t, c.build, false, 40<<10), false, false, ""); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s once the budget freed: %d", c.route, resp.StatusCode)
		}
	}
}

// A body sent too slowly is cut off at the read timeout with 408, records
// nothing, and gives back what it held of the budget. The engine WebSocket on
// the same listener, under the same header timeout, stays connected well past
// both timeouts.
func TestOTLPSlowBodyCutOff(t *testing.T) {
	rec := &logRecorder{}
	svc := New(slog.New(rec), "")
	setOTLPLimits(svc, testOTLPLimit, 152<<10)
	const timeout = 200 * time.Millisecond
	svc.otlp.readTimeout = timeout
	srv := httptest.NewUnstartedServer(nil)
	srv.Config = NewHTTPServer("", svc.Handler())
	srv.Config.ReadHeaderTimeout = timeout
	srv.Start()
	t.Cleanup(srv.Close)

	eng := dialRaw(t, srv, "engine-token")
	eng.hello()

	big := sized(t, otlpCases()[2].build, false, testOTLPLimit)
	held := 20 << 10
	start := time.Now()
	_, br := slowPost(t, srv, "/v1/metrics", len(big), big[:held])
	waitUntil(t, 5*time.Second, "the body to be reserved", func() bool { return svc.otlp.budget.inUse() >= otlpRequestReserve+32<<10 })
	// While it is held, a request that doesn't fit is refused...
	if resp := postBody(t, srv.URL+"/v1/metrics", sized(t, otlpCases()[2].build, false, 40<<10), false, false, ""); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("past the budget: %d", resp.StatusCode)
	}
	// ...until the read timeout cuts the slow one off.
	resp := readResponse(t, br)
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("slow body: %d", resp.StatusCode)
	}
	if took := time.Since(start); took < timeout {
		t.Fatalf("cut off after %s, before the timeout", took)
	}
	if got := refusedCount(t, svc, metrics.OTLPRouteMetrics, metrics.OTLPRefusedTimeout); got != 1 {
		t.Fatalf("timeouts %v", got)
	}
	waitUntil(t, 5*time.Second, "the budget to be free", func() bool { return svc.otlp.budget.inUse() == 0 })
	if resp := postBody(t, srv.URL+"/v1/metrics", sized(t, otlpCases()[2].build, false, 40<<10), false, false, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("once freed: %d", resp.StatusCode)
	}
	if n := len(rec.find("otlp metrics")); n != 1 {
		t.Fatalf("%d metric requests recorded, want only the last", n)
	}

	// Five times both timeouts: a deadline on the WebSocket's connection
	// would have closed it by now. The sleep only shows that nothing
	// happens; what it waits for can't be observed otherwise.
	time.Sleep(5 * timeout)
	env, _ := protocol.NewEnvelope(2, 0, protocol.TypeRenew, protocol.Renew{RenewalKey: "k"})
	eng.send(env)
	if reply := eng.next(); reply.Type != protocol.TypeRenewed || reply.Re != 2 {
		t.Fatalf("renew answered with %+v", reply)
	}
	if n := len(svc.Engines()); n != 1 {
		t.Fatalf("%d engines connected", n)
	}
}

// A request whose headers arrive too slowly is cut off by the header timeout
// every listener sets, before any handler runs.
func TestSlowHeadersCutOff(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewUnstartedServer(nil)
	srv.Config = NewHTTPServer("", svc.Handler())
	srv.Config.ReadHeaderTimeout = 200 * time.Millisecond
	srv.Start()
	t.Cleanup(srv.Close)
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /v1/traces HTTP/1.1\r\nHost: test\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	// The server closes the connection, with or without an answer.
	_, err = io.Copy(io.Discard, conn)
	if err != nil {
		t.Fatalf("the connection wasn't closed: %v", err)
	}
}

// Refusals are logged at warning level at most once a minute per route and
// reason, so a client sending over-limit or malformed requests by the
// thousand can't flood the log; the metrics count every one.
func TestOTLPRefusalWarningsRateLimited(t *testing.T) {
	svc, rec, srv := limitedService(t, 4*testOTLPLimit)
	c := otlpCases()[0]
	over := sized(t, c.build, false, testOTLPLimit+1)
	for range 20 {
		if resp := postBody(t, srv.URL+c.path, over, false, false, ""); resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if resp := postBody(t, srv.URL+c.path, []byte("\xff\xff\xff"), false, false, ""); resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	warned := map[string]int{}
	for _, msg := range []string{"otlp request refused", "undecodable otlp request"} {
		for _, r := range rec.findRecords(msg) {
			if r.Level == slog.LevelWarn {
				warned[msg]++
			}
		}
		if n := len(rec.find(msg)); n != 20 {
			t.Fatalf("%d %q logged, want 20 at some level", n, msg)
		}
	}
	if warned["otlp request refused"] != 1 || warned["undecodable otlp request"] != 1 {
		t.Fatalf("warnings %v, want one of each", warned)
	}
	if got := refusedCount(t, svc, c.route, metrics.OTLPRefusedTooLarge); got != 20 {
		t.Fatalf("refused %v", got)
	}
}

// The budget's peak is the most reserved at once, which the e2e run reads
// from the log (budgetPeak) to check the budget against real traffic.
func TestOTLPBudgetPeak(t *testing.T) {
	b := newByteBudget(100)
	if !b.reserve(60) || !b.reserve(30) || b.reserve(20) {
		t.Fatal("reservations past the size")
	}
	b.release(90)
	if !b.reserve(10) || b.inUse() != 10 || b.peakUse() != 90 {
		t.Fatalf("in use %d, peak %d", b.inUse(), b.peakUse())
	}
}

// findRecords returns the records with message msg.
func (h *logRecorder) findRecords(msg string) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var found []slog.Record
	for _, r := range h.records {
		if r.Message == msg {
			found = append(found, r)
		}
	}
	return found
}

// shortIdleTimeout makes NewHTTPServer set d as its IdleTimeout for the
// test.
func shortIdleTimeout(t *testing.T, d time.Duration) {
	old := idleTimeout
	idleTimeout = d
	t.Cleanup(func() { idleTimeout = old })
}

// A keep-alive connection between requests is closed after IdleTimeout
// (zerobull-consulting/dagger#83): without a bound, it stayed open for as long
// as a client liked, at the cost of a goroutine and a file descriptor.
func TestIdleKeepAliveConnectionClosed(t *testing.T) {
	const timeout = 200 * time.Millisecond
	shortIdleTimeout(t, timeout)
	svc := New(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewUnstartedServer(nil)
	srv.Config = NewHTTPServer("", svc.Handler())
	srv.Start()
	t.Cleanup(srv.Close)
	body := encodeOTLP(t, otlpCases()[2].build(""), false)
	conn, br := slowPost(t, srv, "/v1/metrics", len(body), body)
	if resp := readResponse(t, br); resp.StatusCode != http.StatusOK {
		t.Fatalf("first request: %d", resp.StatusCode)
	}
	// The server closes the idle connection: a read fails (EOF, or a reset)
	// before the read deadline, and not before about the timeout.
	start := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err := br.ReadByte()
	if err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("idle connection not closed: %v", err)
	}
	if took := time.Since(start); took < timeout/2 {
		t.Errorf("idle connection closed after %v, want about %v", took, timeout)
	}
}

// IdleTimeout is above the engine Cloud exporters' own idle connection
// timeout, so their client always closes an idle connection first.
func TestIdleTimeoutAboveExporterIdleConnTimeout(t *testing.T) {
	if got := NewHTTPServer("", nil).IdleTimeout; got <= ExporterIdleConnTimeout {
		t.Fatalf("IdleTimeout %v is not above the exporters' %v", got, ExporterIdleConnTimeout)
	}
}

// A client with the exporters' transport settings (IdleConnTimeout below the
// server's IdleTimeout, as in the engine) keeps POSTing across gaps longer
// than its idle timeout without a transport error: it closes the connection
// first, and dials another. Times are scaled down from 90s and IdleTimeout
// with the ratio kept.
func TestExporterClientReusesConnectionsAcrossIdleGaps(t *testing.T) {
	clientIdle := 300 * time.Millisecond
	serverIdle := time.Duration(float64(clientIdle) * float64(IdleTimeout) / float64(ExporterIdleConnTimeout))
	shortIdleTimeout(t, serverIdle)
	svc := New(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewUnstartedServer(nil)
	srv.Config = NewHTTPServer("", svc.Handler())
	srv.Start()
	t.Cleanup(srv.Close)
	tr := &http.Transport{ForceAttemptHTTP2: true, MaxIdleConns: 100, IdleConnTimeout: clientIdle}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	body := encodeOTLP(t, otlpCases()[2].build(""), false)
	// A gap past the client's idle timeout and short of the server's: the
	// client has closed the connection, the server hasn't.
	gap := clientIdle + (serverIdle-clientIdle)/2
	for i := range 5 {
		resp, err := client.Post(srv.URL+"/v1/metrics", "application/x-protobuf", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST %d: %v", i, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST %d: %d", i, resp.StatusCode)
		}
		time.Sleep(gap)
	}
}

// budgetCheckingReader checks, at each read, that the buffer it is asked to
// fill is reserved: what was read so far, and what it may read now, are
// within what the budget holds.
type budgetCheckingReader struct {
	t      *testing.T
	r      io.Reader
	budget *byteBudget
	read   int64
}

func (c *budgetCheckingReader) Read(p []byte) (int, error) {
	// The one byte read past a full buffer, at the limit, is the probe for
	// a body too large: a byte on the stack, not the buffer.
	probe := len(p) == 1 && c.read == c.budget.inUse()
	if need := c.read + int64(len(p)); c.budget.inUse() < need && !probe {
		c.t.Errorf("asked to read into %d bytes, with %d read, while %d are reserved", len(p), c.read, c.budget.inUse())
	}
	n, err := c.r.Read(p)
	c.read += int64(n)
	return n, err
}

// The buffer a body is read into is reserved before anything is read into
// it, so concurrent requests can't each read into memory the budget doesn't
// count. A body at the limit reserves exactly the limit, and one past it is
// refused without a buffer larger than the limit.
func TestOTLPBodyReadOnlyIntoReservedBytes(t *testing.T) {
	for _, size := range []int{0, 1, 511, 512, 513, 5000, testOTLPLimit - 1, testOTLPLimit} {
		// 1.5 times the limit as an at-limit buffer last grows.
		budget := newByteBudget(2 * testOTLPLimit)
		r := &budgetCheckingReader{t: t, r: bytes.NewReader(make([]byte, size)), budget: budget}
		data, reserved, err := readOTLPBody(r, testOTLPLimit, budget)
		if err != nil || len(data) != size {
			t.Fatalf("%d bytes: read %d, %v", size, len(data), err)
		}
		if reserved != int64(cap(data)) || budget.inUse() != reserved || reserved > max(2*int64(size), otlpMinBuffer) {
			t.Fatalf("%d bytes: %d reserved, buffer of %d, %d in use", size, reserved, cap(data), budget.inUse())
		}
	}
	budget := newByteBudget(2 * testOTLPLimit)
	r := &budgetCheckingReader{t: t, r: bytes.NewReader(make([]byte, testOTLPLimit+1)), budget: budget}
	if _, reserved, err := readOTLPBody(r, testOTLPLimit, budget); !errors.Is(err, errOTLPTooLarge) || reserved != testOTLPLimit {
		t.Fatalf("one byte over: %d reserved, %v", reserved, err)
	}
	// A budget too small for the next buffer refuses before reading into
	// it.
	small := newByteBudget(4 << 10)
	r = &budgetCheckingReader{t: t, r: bytes.NewReader(make([]byte, 6<<10)), budget: small}
	if _, reserved, err := readOTLPBody(r, testOTLPLimit, small); !errors.Is(err, errOTLPBusy) || reserved != 2<<10 || r.read > 2<<10 {
		t.Fatalf("past the budget: %d reserved, %d read, %v", reserved, r.read, err)
	}
}

// Over HTTP/2 (with -tls-cert), the read timeout bounds a slow body too:
// http.ResponseController's read deadline is per stream there.
func TestOTLPSlowBodyCutOffHTTP2(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	svc.otlp.readTimeout = 200 * time.Millisecond
	srv := httptest.NewUnstartedServer(svc.Handler())
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pr, pw := io.Pipe()
	t.Cleanup(func() { pw.Close() })
	go func() { _, _ = pw.Write([]byte{0x0a}) }() // a first byte, then nothing
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/metrics", pr)
	req.Header.Set("Content-Type", "application/x-protobuf")
	client := srv.Client()
	client.Timeout = 10 * time.Second
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("%s: %d", resp.Proto, resp.StatusCode)
	}
	if got := refusedCount(t, svc, metrics.OTLPRouteMetrics, metrics.OTLPRefusedTimeout); got != 1 {
		t.Fatalf("timeouts %v", got)
	}
}

// While a buffer grows, the old one is still held as the new one is filled
// with its copy: both are reserved until the copy is done, so the budget
// counts the moment a request holds the most.
func TestOTLPBufferGrowthReservesBoth(t *testing.T) {
	// 2049 bytes grow the buffer 512, 1 KiB, 2 KiB, 4 KiB: at its last
	// growth, 2 KiB and 4 KiB at once.
	budget := newByteBudget(1 << 20)
	data, reserved, err := readOTLPBody(bytes.NewReader(make([]byte, 2049)), testOTLPLimit, budget)
	if err != nil || len(data) != 2049 || reserved != 4<<10 || budget.peakUse() != 6<<10 {
		t.Fatalf("read %d, %d reserved, peak %d, %v", len(data), reserved, budget.peakUse(), err)
	}
	// A budget that holds the final buffer, but not both at its growth,
	// refuses.
	tight := newByteBudget(4 << 10)
	if _, reserved, err := readOTLPBody(bytes.NewReader(make([]byte, 2049)), testOTLPLimit, tight); !errors.Is(err, errOTLPBusy) || reserved != 2<<10 {
		t.Fatalf("tight budget: %d reserved, %v", reserved, err)
	}
}

// A gzipped body's decompressor is reserved before it is built, and given
// back on every path: a client that only sends gzip headers can't hold
// decompressors the budget doesn't count.
func TestOTLPGzipDecompressorReserved(t *testing.T) {
	c := otlpCases()[2]
	body := gzipped(t, encodeOTLP(t, c.build(""), false))

	svc, _, srv := limitedService(t, otlpGzipReserve/2)
	built := 0
	svc.otlp.gunzip = func(r io.Reader) (io.ReadCloser, error) {
		built++
		return gunzipReader(r)
	}
	resp := postBody(t, srv.URL+c.path, body, false, true, "")
	if resp.StatusCode != http.StatusServiceUnavailable || built != 0 {
		t.Fatalf("gzipped past the budget: %d, %d decompressors built", resp.StatusCode, built)
	}
	if resp := postBody(t, srv.URL+c.path, encodeOTLP(t, c.build(""), false), false, false, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("plain: %d", resp.StatusCode)
	}

	svc, _, srv = limitedService(t, 4*testOTLPLimit)
	for _, tc := range []struct {
		data []byte
		want int
	}{
		{body, http.StatusOK},
		{[]byte("not gzip"), http.StatusBadRequest},
		{gzipped(t, make([]byte, 16<<20)), http.StatusRequestEntityTooLarge},
	} {
		if resp := postBody(t, srv.URL+c.path, tc.data, false, true, ""); resp.StatusCode != tc.want {
			t.Fatalf("status %d, want %d", resp.StatusCode, tc.want)
		}
		if used := svc.otlp.budget.inUse(); used != 0 {
			t.Fatalf("after a %d: %d bytes still reserved", tc.want, used)
		}
	}
	if peak := svc.otlp.budget.peakUse(); peak < otlpGzipReserve {
		t.Fatalf("peak %d: the decompressor wasn't reserved", peak)
	}
}

// On a real listener, a request refused for its token is answered at once,
// before any of its body is read; net/http then reads on its body, to reuse
// the connection, but only until the read timeout, which is set before the
// token is checked. A client that trickles the body after a 401 can't hold the
// connection, or the handler, longer than that.
func TestOTLPRefusedTokenSlowBodyCutOff(t *testing.T) {
	h := newAuthHarness(t)
	const timeout = 200 * time.Millisecond
	h.svc.otlp.readTimeout = timeout
	for _, token := range []string{"", "tok-unknown"} {
		conn, err := net.Dial("tcp", h.srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		// Before the harness's cleanup, which waits for the server's
		// handlers: a connection left open would hold one.
		t.Cleanup(func() { conn.Close() })
		auth := ""
		if token != "" {
			auth = "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(token+":")) + "\r\n"
		}
		// A body declared, a few bytes of it sent, and no more.
		fmt.Fprintf(conn, "POST /v1/traces HTTP/1.1\r\nHost: test\r\nContent-Type: application/x-protobuf\r\n%sContent-Length: 1000\r\n\r\n0123456789", auth)
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		br := bufio.NewReader(conn)
		if resp := readResponse(t, br); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("token %q: %d", token, resp.StatusCode)
		}
		start := time.Now()
		// The server closes the connection once its read of the rest of
		// the body times out.
		if _, err := br.ReadByte(); !errors.Is(err, io.EOF) && !isConnReset(err) {
			t.Fatalf("token %q: the connection stayed open: %v", token, err)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("token %q: closed after %s", token, took)
		}
		conn.Close()
	}
}

func isConnReset(err error) bool {
	return err != nil && strings.Contains(err.Error(), "connection reset")
}

// emptyEntries is a body of n empty entries of field 1, the top-level
// repeated field of every OTLP request (resource_spans, resource_logs,
// resource_metrics): 2 bytes each on the wire, a whole message each decoded.
func emptyEntries(n int) []byte {
	b := make([]byte, 0, 2*n)
	for range n {
		b = append(b, 0x0a, 0x00)
	}
	return b
}

// A body within its limit that decodes to far more than the budget, such as
// tens of thousands of empty entries, is refused with 413 before it is
// decoded, protobuf or JSON: the budget counts the decoded message, not only
// the bytes it comes from. Nothing of it is recorded.
func TestOTLPAmplifiedBodyRefused(t *testing.T) {
	jsonEntries := func(field string, n int) []byte {
		var b bytes.Buffer
		b.WriteString(`{"` + field + `":[`)
		for i := range n {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString("{}")
		}
		b.WriteString("]}")
		return b.Bytes()
	}
	fields := map[string]string{
		metrics.OTLPRouteTraces: "resourceSpans", metrics.OTLPRouteLogs: "resourceLogs", metrics.OTLPRouteMetrics: "resourceMetrics",
	}
	for _, c := range otlpCases() {
		for _, asJSON := range []bool{false, true} {
			svc, rec, srv := limitedService(t, 4*testOTLPLimit)
			body := emptyEntries(testOTLPLimit/2 - 1)
			if asJSON {
				body = jsonEntries(fields[c.route], testOTLPLimit/3-10)
			}
			if len(body) > testOTLPLimit {
				t.Fatalf("a body of %d bytes", len(body))
			}
			if resp := postBody(t, srv.URL+c.path, body, asJSON, false, ""); resp.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("%s, json=%t: %d", c.route, asJSON, resp.StatusCode)
			}
			if n := len(rec.find(c.recorded)); n != 0 {
				t.Fatalf("%s: %d %q logged", c.route, n, c.recorded)
			}
			if got := refusedCount(t, svc, c.route, metrics.OTLPRefusedTooLarge); got != 1 {
				t.Fatalf("%s: refused %v", c.route, got)
			}
			if used := svc.otlp.budget.inUse(); used != 0 {
				t.Fatalf("%s: %d bytes still reserved", c.route, used)
			}
		}
	}
}

// The estimate of a decoded message errs high: an empty entry counts at
// least its Go struct and its slice element, a real request a small multiple
// of its size, and a body that isn't valid protobuf is an error, as decoding
// it would be.
func TestOTLPDecodedSizeEstimate(t *testing.T) {
	const n = 1000
	est, err := otlpDecodedSize(emptyEntries(n), &coltracepb.ExportTraceServiceRequest{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if per := est / n; per < int64(unsafe.Sizeof(tracepb.ResourceSpans{}))+8 {
		t.Fatalf("%d bytes per empty ResourceSpans", per)
	}
	for _, c := range otlpCases() {
		msg := c.build("")
		data := encodeOTLP(t, msg, false)
		est, err := otlpDecodedSize(data, msg, false)
		if err != nil || est < int64(len(data)) || est > 64*int64(len(data))+4096 {
			t.Fatalf("%s: %d bytes estimated for %d, %v", c.route, est, len(data), err)
		}
		// A string is counted at its length.
		padded := encodeOTLP(t, c.build(strings.Repeat("x", 10000)), false)
		if pest, _ := otlpDecodedSize(padded, msg, false); pest < est+10000 {
			t.Fatalf("%s: a 10000-byte string added %d", c.route, pest-est)
		}
	}
	if _, err := otlpDecodedSize([]byte("\xff\xff\xff"), &coltracepb.ExportTraceServiceRequest{}, false); err == nil {
		t.Fatal("no error for an invalid body")
	}
}

// Every request being read reserves a fixed share of the budget for itself,
// so requests that stall after a byte of their body can't each hold a
// connection and a handler for the price of a 512-byte buffer: past what the
// budget holds, the next is refused with 503 at once.
func TestOTLPStalledRequestsBounded(t *testing.T) {
	// Each request also reserves its headers (otlpHeaderReserve), which
	// slowPost writes as below, so the budget has that much more per request.
	parsed, err := http.ReadRequest(bufio.NewReader(strings.NewReader(
		"POST /v1/metrics HTTP/1.1\r\nHost: test\r\nContent-Type: application/x-protobuf\r\nContent-Length: 1000\r\n\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	each := otlpRequestReserve + otlpMinBuffer + otlpHeaderReserve(parsed)
	svc, _, srv := limitedService(t, 2*each+1024)
	body := encodeOTLP(t, otlpCases()[2].build(""), false)
	for range 2 {
		slowPost(t, srv, "/v1/metrics", 1000, body[:1])
	}
	waitUntil(t, 5*time.Second, "two stalled requests to be reserved", func() bool {
		return svc.otlp.budget.inUse() >= 2*each
	})
	_, br := slowPost(t, srv, "/v1/metrics", 1000, body[:1])
	if resp := readResponse(t, br); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a third stalled request: %d", resp.StatusCode)
	}
}

// headerServer serves svc as the command's listeners do (NewHTTPServer, so
// with MaxHeaderBytes), on a loopback address.
func headerServer(t *testing.T, svc *Service) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewHTTPServer("", svc.Handler())
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

// A request's headers are in the decode budget (zerobull-consulting/dagger#85):
// one with 24 KiB of headers that stalls its body holds them in the budget,
// beside otlpRequestReserve and its first buffer, until it ends. Without the
// charge it held only otlpRequestReserve plus the buffer.
func TestOTLPRequestHeadersInBudget(t *testing.T) {
	svc, _, _ := limitedService(t, 4*testOTLPLimit)
	conn, err := net.Dial("tcp", headerServer(t, svc))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	pad := strings.Repeat("a", 24<<10)
	fmt.Fprintf(conn, "POST /v1/metrics HTTP/1.1\r\nHost: test\r\nX-Pad: %s\r\nContent-Type: application/x-protobuf\r\nContent-Length: 1000\r\n\r\n\x0a", pad)
	floor := int64(otlpRequestReserve+otlpMinBuffer) + int64(len(pad))
	waitUntil(t, 5*time.Second, "the stalled request's headers to be reserved", func() bool { return svc.otlp.budget.inUse() >= floor })
	// Its headers, and a little over for the others a client sends.
	if used := svc.otlp.budget.inUse(); used > floor+2<<10 {
		t.Fatalf("%d bytes reserved for a request of %d bytes of headers", used, len(pad))
	}
	// Closing the connection ends the request, and gives everything back.
	conn.Close()
	waitUntil(t, 5*time.Second, "the budget to be released", func() bool { return svc.otlp.budget.inUse() == 0 })
}

// The header charge is the bytes of every key and value, plus a cost for each
// key and each value, the host and the request URI twice.
func TestOTLPHeaderReserve(t *testing.T) {
	r, err := http.NewRequest("POST", "http://example.test/v1/traces", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.RequestURI = "/v1/traces"
	r.Header = http.Header{"A": {"bb", "c"}, "Dd": {"e"}}
	want := int64(len("example.test")+2*len("/v1/traces")) +
		(1 + otlpHeaderKeyCost + 2 + otlpHeaderValueCost + 1 + otlpHeaderValueCost) +
		(2 + otlpHeaderKeyCost + 1 + otlpHeaderValueCost)
	if got := otlpHeaderReserve(r); got != want {
		t.Fatalf("reserve %d, want %d", got, want)
	}
}

// The reserve covers what net/http's parsed headers hold, for the two shapes
// that cost most per wire byte, each at the size the listener allows:
// thousands of tiny distinct keys (a map slot and a slice each) and one key
// repeated (a string header per value). The heap figures are what
// http.ReadRequest held for these requests, measured with the GC before and
// after (9,244 keys: 967,022 bytes; 13,916 values: 277,624 bytes); the
// reserve may not be under them, and is not more than twice them.
func TestOTLPHeaderReserveCoversParsedHeaders(t *testing.T) {
	for _, c := range []struct {
		name string
		// head is what precedes the generated lines in the header block.
		head string
		line func(i int) string
		// tail ends the last line.
		tail string
		heap int64
	}{
		{"distinct keys", "", func(i int) string { return fmt.Sprintf("%x:b\r\n", i) }, "", 967022},
		{"one key repeated", "", func(int) string { return "a:b\r\n" }, "", 277624},
		// net/http moves the names of a Trailer header out of r.Header
		// into r.Trailer (14,783 names, 1,508,797 bytes).
		{"trailer names", "Transfer-Encoding: chunked\r\nTrailer: ", func(i int) string { return fmt.Sprintf("%x,", i) }, "zz\r\n", 1508797},
	} {
		t.Run(c.name, func(t *testing.T) {
			var b strings.Builder
			b.WriteString("POST /v1/traces HTTP/1.1\r\nHost: t\r\n" + c.head)
			for i := 0; b.Len() < MaxHeaderBytes+4<<10-20; i++ {
				b.WriteString(c.line(i))
			}
			b.WriteString(c.tail + "\r\n")
			r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(b.String())))
			if err != nil {
				t.Fatal(err)
			}
			got := otlpHeaderReserve(r)
			if got < c.heap*99/100 || got > 2*c.heap {
				t.Fatalf("reserve %d for headers that held %d", got, c.heap)
			}
		})
	}
}

// A request whose headers would reserve more than MaxOTLPHeaderReserve is
// answered 431 before it reserves anything or its body is read, whatever
// shape they take: one long header, thousands of short ones, or a long list of
// trailer names. One within it is taken. The refusal is counted as too_large.
func TestOTLPHeaderReserveCap(t *testing.T) {
	svc, rec, _ := limitedService(t, 4*testOTLPLimit)
	addr := headerServer(t, svc)
	body := encodeOTLP(t, otlpCases()[2].build(""), false)
	post := func(head, lines string) int {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		fmt.Fprintf(conn, "POST /v1/metrics HTTP/1.1\r\nHost: test\r\nContent-Type: application/x-protobuf\r\nContent-Length: %d\r\nConnection: close\r\n%s%s\r\n%s", len(body), head, lines, body)
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	many := func(n int) string {
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, "h%x:b\r\n", i)
		}
		return b.String()
	}
	if status := post("", many(100)+"X-Pad: "+strings.Repeat("a", 12<<10)+"\r\n"); status != http.StatusOK {
		t.Fatalf("headers within the cap: %d", status)
	}
	if n := len(rec.find("otlp metrics")); n != 1 {
		t.Fatalf("%d metric requests recorded", n)
	}
	for _, c := range []struct{ name, lines string }{
		{"one long header", "X-Pad: " + strings.Repeat("a", 40<<10) + "\r\n"},
		{"many short headers", many(3000)},
	} {
		if status := post("", c.lines); status != http.StatusRequestHeaderFieldsTooLarge {
			t.Fatalf("%s: %d", c.name, status)
		}
	}
	if got := refusedCount(t, svc, "metrics", metrics.OTLPRefusedTooLarge); got != 2 {
		t.Fatalf("too_large refusals: %v", got)
	}
	if n := len(rec.find("otlp metrics")); n != 1 {
		t.Fatalf("%d metric requests recorded after the refusals", n)
	}
	if used := svc.otlp.budget.inUse(); used != 0 {
		t.Fatalf("%d bytes still reserved", used)
	}
}

// A request with more than MaxHeaderBytes of headers is refused 431 by the
// listener, before the handler: it reserves nothing and records nothing. One
// just under the limit is taken.
func TestOTLPRequestOverHeaderLimit(t *testing.T) {
	svc, rec, _ := limitedService(t, 4*testOTLPLimit)
	addr := headerServer(t, svc)
	body := encodeOTLP(t, otlpCases()[2].build(""), false)
	post := func(pad int) *http.Response {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		fmt.Fprintf(conn, "POST /v1/metrics HTTP/1.1\r\nHost: test\r\nX-Pad: %s\r\nContent-Type: application/x-protobuf\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
			strings.Repeat("a", pad), len(body), body)
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if resp := post(24 << 10); resp.StatusCode != http.StatusOK {
		t.Fatalf("headers within the limits: %d", resp.StatusCode)
	}
	if n := len(rec.find("otlp metrics")); n != 1 {
		t.Fatalf("%d metric requests recorded", n)
	}
	// net/http allows 4 KiB beyond MaxHeaderBytes (its read buffer).
	if resp := post(MaxHeaderBytes + 4<<10); resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("headers over the limit: %d", resp.StatusCode)
	}
	if n := len(rec.find("otlp metrics")); n != 1 {
		t.Fatalf("%d metric requests recorded after the refusal", n)
	}
	if used := svc.otlp.budget.inUse(); used != 0 {
		t.Fatalf("%d bytes still reserved", used)
	}
}

// net/http bounds a request's trailers to a few KiB, so they hold nothing the
// per-request reserve doesn't cover: a chunked request whose trailer is 1 KiB
// is taken, so is one of 4,056 bytes, and one of 4 KiB (net/http's read
// buffer, as measured with Go 1.26.6; a Go upgrade that moves it turns this
// red, and the README's claim with it) is answered 400 and records nothing.
func TestOTLPRequestOverTrailerLimit(t *testing.T) {
	svc, rec, _ := limitedService(t, 4*testOTLPLimit)
	addr := headerServer(t, svc)
	body := encodeOTLP(t, otlpCases()[2].build(""), false)
	post := func(trailer int) int {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		fmt.Fprintf(conn, "POST /v1/metrics HTTP/1.1\r\nHost: test\r\nContent-Type: application/x-protobuf\r\nTransfer-Encoding: chunked\r\nTrailer: X-T\r\nConnection: close\r\n\r\n%x\r\n%s\r\n0\r\nX-T: %s\r\n\r\n",
			len(body), body, strings.Repeat("a", trailer))
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for i, n := range []int{1 << 10, 4056} {
		if status := post(n); status != http.StatusOK {
			t.Fatalf("a trailer of %d bytes: %d", n, status)
		}
		if got := len(rec.find("otlp metrics")); got != i+1 {
			t.Fatalf("%d metric requests recorded", got)
		}
	}
	if status := post(4 << 10); status != http.StatusBadRequest {
		t.Fatalf("a 4 KiB trailer: %d", status)
	}
	if n := len(rec.find("otlp metrics")); n != 2 {
		t.Fatalf("%d metric requests recorded after the refusal", n)
	}
	if used := svc.otlp.budget.inUse(); used != 0 {
		t.Fatalf("%d bytes still reserved", used)
	}
}

// A refused request doesn't hold its headers while its stalled body is
// drained: net/http reads what it can of an unread body after the handler,
// up to the read deadline (30s for an OTLP body), and the request, headers
// included, stays alive meanwhile. Measured: 200 requests refused by the
// header cap with 40 KiB headers and stalled bodies held 12.7 MB for 30s with
// nothing reserved. A refusal cuts the wait to otlpRefusalDrain: the answer
// comes at once, and the connection closes within a couple of seconds.
func TestOTLPRefusedStalledBodyConnectionClosed(t *testing.T) {
	svc, _, _ := limitedService(t, 4*testOTLPLimit)
	conn, err := net.Dial("tcp", headerServer(t, svc))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	// The read timeout is 30s: a server that kept draining would outlast this.
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(conn, "POST /v1/metrics HTTP/1.1\r\nHost: test\r\nX-Pad: %s\r\nContent-Length: 100000\r\n\r\nx", strings.Repeat("a", 40<<10))
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status %d", resp.StatusCode)
	}
	io.Copy(io.Discard, resp.Body)
	start := time.Now()
	if _, err := br.ReadByte(); !errors.Is(err, io.EOF) && !isConnReset(err) {
		t.Fatalf("the connection stayed open after the refusal: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("closed %s after the answer", took)
	}
}

// Known gap, zerobull-consulting/dagger#219: a request refused for its token (401) before decodeOTLP has
// reserved nothing, not even its headers, though net/http holds them while it
// reads and discards the body, up to the read timeout. Measured: 200 such
// requests with 60 KiB headers and a stalled body grew the heap by 18 MB
// with nothing reserved. MaxHeaderBytes bounds each at about 68 KiB, 15 times
// less than net/http's default. When the reserve moves ahead of the token
// check, this test changes with it.
func TestOTLPRefusedTokenHeadersNotInBudgetKnownGap(t *testing.T) {
	svc, rec, _ := limitedService(t, 4*testOTLPLimit)
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte("good-token ns1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := svc.UseTokensFile(tokens); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", headerServer(t, svc))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	fmt.Fprintf(conn, "POST /v1/metrics HTTP/1.1\r\nHost: test\r\nAuthorization: Basic YmFkOg==\r\nX-Pad: %s\r\nContent-Length: 100000\r\n\r\nx", strings.Repeat("a", 48<<10))
	// The handler has refused the token. The answer isn't written until
	// net/http has read what it will of the unfinished body, which a stalled
	// client holds to the read timeout: the request still holds its headers,
	// and the budget holds none of them.
	waitUntil(t, 5*time.Second, "the token to be refused", func() bool {
		return len(rec.find("request refused: unknown token")) > 0
	})
	if used := svc.otlp.budget.inUse(); used != 0 {
		t.Fatalf("%d bytes reserved: the refused request's headers are counted now; update the known gap", used)
	}
}

// Known gap, zerobull-consulting/dagger#86: walking a deeply nested body
// costs stack the decode budget doesn't count. OTLP's AnyValue nests through
// ArrayValue a few bytes per level, so a body of a few KB can nest thousands
// of messages deep; otlpDecodedSize walks it recursively, as
// proto.Unmarshal does, up to otlpMaxDepth (10,000), and counts each level's
// structs but not the stack frames the walk takes. Such a body is estimated
// and taken. When #86 bounds the depth or walks iteratively, this test
// changes with it.
func TestOTLPDeeplyNestedBodyTaken(t *testing.T) {
	// 2,000 AnyValues, each in an ArrayValue: 4,000 nested messages, within
	// otlpMaxDepth and proto.Unmarshal's own limit.
	const depth = 2000
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "leaf"}}
	for range depth {
		v = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{v}}}}
	}
	req := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource:     &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{Key: "nested", Value: v}}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{Name: "a"}}}},
	}}}
	body := encodeOTLP(t, req, false)
	if len(body) > testOTLPLimit {
		t.Fatalf("a body of %d bytes", len(body))
	}
	if _, err := otlpDecodedSize(body, req, false); err != nil {
		t.Fatalf("estimate: %v", err)
	}
	// Each level's structs are counted (about 150 bytes), so the budget is
	// large enough for them; the stack isn't.
	_, rec, srv := limitedService(t, 4<<20)
	if resp := postBody(t, srv.URL+"/v1/metrics", body, false, false, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("a body nested %d deep: %d", depth, resp.StatusCode)
	}
	if n := len(rec.find("otlp metrics")); n != 1 {
		t.Fatalf("%d metric requests recorded", n)
	}
}

// An unknown start-group is kept by proto.Unmarshal as raw bytes, so the
// estimate charges it at least its wire size, not a flat cost.
func TestOTLPUnknownGroupChargedItsSize(t *testing.T) {
	// group is field num as a start-group holding a bytes field of size
	// bytes. 100 is not a field of any message below; 1 (ResourceMetrics) is.
	group := func(num protowire.Number, size int) []byte {
		b := protowire.AppendTag(nil, num, protowire.StartGroupType)
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, make([]byte, size))
		return protowire.AppendTag(b, num, protowire.EndGroupType)
	}
	req := &colmetricspb.ExportMetricsServiceRequest{}
	flat, err := otlpDecodedSize(nil, req, false)
	if err != nil {
		t.Fatal(err)
	}
	half := testOTLPLimit / 2
	unknownGroup := group(100, half)
	wrongType := group(1, half)
	for _, c := range []struct {
		name string
		body []byte
		// unknown is the wire size of the field the decoder keeps as
		// unknown bytes, tags included; nested says it is kept in the
		// request's first ResourceMetrics, not the request.
		unknown int
		nested  bool
	}{
		// An unknown group at the top level.
		{"top level", unknownGroup, len(unknownGroup), false},
		// One inside a known nested message (ResourceMetrics, field 1).
		{"nested", protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), unknownGroup), len(unknownGroup), true},
		// A known field number (ResourceMetrics, 1) sent as a group, so
		// the wrong wire type for its number.
		{"wrong wire type", wrongType, len(wrongType), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := otlpDecodedSize(c.body, &colmetricspb.ExportMetricsServiceRequest{}, false)
			if err != nil {
				t.Fatalf("estimate: %v", err)
			}
			// The decoder keeps the field as unknown bytes, tags included,
			// all of them.
			m := &colmetricspb.ExportMetricsServiceRequest{}
			if err := proto.Unmarshal(c.body, m); err != nil {
				t.Fatal(err)
			}
			kept := m.ProtoReflect().GetUnknown()
			if c.nested {
				if len(m.ResourceMetrics) != 1 {
					t.Fatalf("%d ResourceMetrics decoded", len(m.ResourceMetrics))
				}
				kept = m.ResourceMetrics[0].ProtoReflect().GetUnknown()
			}
			if len(kept) != c.unknown {
				t.Fatalf("decoder kept %d unknown bytes, want %d", len(kept), c.unknown)
			}
			// The empty request's estimate is its struct; the field adds at
			// least its whole wire size on top (more when nested: the
			// ResourceMetrics, and its slot in the list).
			if got-flat < int64(c.unknown) {
				t.Fatalf("a field of %d wire bytes added %d to the estimate", c.unknown, got-flat)
			}
		})
	}

	// A small field is charged the flat cost, not its few bytes: an unknown
	// varint (field 100) adds otlpFieldCost.
	small, err := otlpDecodedSize(protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1), req, false)
	if err != nil {
		t.Fatal(err)
	}
	if small-flat != otlpFieldCost {
		t.Fatalf("an unknown varint added %d to the estimate, want %d", small-flat, otlpFieldCost)
	}

	// End to end: a budget that admits the request's reserve and reading
	// the body, but not the estimate on top, refuses it as too large (413).
	// Reading a body of about 32 KiB holds otlpRequestReserve plus a 32 KiB
	// and a 64 KiB buffer at its peak (114,688 bytes), then the 64 KiB buffer
	// (81,920); the group's estimate is its 32,776 bytes plus the request's
	// struct, so 114,700 admits the read and the flat charge (82,008 held) but
	// not the size charge (about 114,784).
	// A 413 here comes from the estimate: the body is far under the route's
	// limit, and a read the budget can't hold is a 503.
	body := unknownGroup
	if len(body) != 32776 {
		t.Fatalf("body of %d bytes: the budget below is sized for 32776", len(body))
	}
	// The request's headers are reserved too (otlpHeaderReserve), so the
	// request is written by hand, to know what they come to, and the budget
	// is that much larger.
	raw := fmt.Sprintf("POST /v1/metrics HTTP/1.1\r\nHost: test\r\nContent-Type: application/x-protobuf\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	parsed, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	_, _, srv := limitedService(t, 114700+otlpHeaderReserve(parsed))
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	io.WriteString(conn, raw)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

// proto.Unmarshal keeps each unknown field by appending it to one slice per
// message, and append holds the old array and the new one at once while it
// grows. The estimate covers that peak: the old and the new array of the
// largest growth step, which this test finds by appending the same fields
// itself (append's growth is deterministic, so no timing or heap sampling is
// involved), after checking that the decoder ends with the same capacity.
// The margin is none: the estimate must be at least the peak.
// zerobull-consulting/dagger#204 measured peak live heap with the GC at 1.5
// to 1.6 times the wire bytes; the growth step itself holds up to 2.26 times
// (64 KiB fields, 23 of them), which otlpUnknownGrowth = 3 covers.
func TestOTLPUnknownFieldsEstimateCoversDecodePeak(t *testing.T) {
	varint := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
	bytesField := func(size int) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), make([]byte, size))
	}
	// peak is the largest old and new array append holds at once.
	peak := func(fields [][]byte) (peak int64, final int) {
		var s []byte
		for _, f := range fields {
			before := cap(s)
			s = append(s, f...)
			if cap(s) != before {
				peak = max(peak, int64(before+cap(s)))
			}
		}
		return peak, cap(s)
	}
	repeat := func(f []byte, n int) [][]byte {
		out := make([][]byte, n)
		for i := range out {
			out[i] = f
		}
		return out
	}
	for _, c := range []struct {
		name   string
		fields [][]byte
		// nested says the fields are inside the request's first
		// ResourceSpans, not the request.
		nested bool
	}{
		{"1000 varints", repeat(varint, 1000), false},
		{"100000 varints", repeat(varint, 100000), false},
		{"1000 bytes of 8", repeat(bytesField(8), 1000), false},
		{"100000 bytes of 8", repeat(bytesField(8), 100000), false},
		{"100 bytes of 64", repeat(bytesField(64), 100), false},
		{"10000 bytes of 64", repeat(bytesField(64), 10000), false},
		{"100000 bytes of 64", repeat(bytesField(64), 100000), false},
		{"1000 bytes of 1024", repeat(bytesField(1024), 1000), false},
		{"100 bytes of 64 KiB", repeat(bytesField(64<<10), 100), false},
		{"nested, 10000 bytes of 64", repeat(bytesField(64), 10000), true},
		{"mixed sizes", func() [][]byte {
			var out [][]byte
			for i := range 5000 {
				out = append(out, [][]byte{varint, bytesField(8), bytesField(64), bytesField(1000)}[i%4])
			}
			return out
		}(), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := bytes.Join(c.fields, nil)
			if c.nested {
				body = protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), body)
			}
			p, finalCap := peak(c.fields)
			m := &coltracepb.ExportTraceServiceRequest{}
			if err := proto.Unmarshal(body, m); err != nil {
				t.Fatal(err)
			}
			kept := m.ProtoReflect().GetUnknown()
			if c.nested {
				if len(m.ResourceSpans) != 1 {
					t.Fatalf("%d ResourceSpans decoded", len(m.ResourceSpans))
				}
				kept = m.ResourceSpans[0].ProtoReflect().GetUnknown()
			}
			if cap(kept) != finalCap {
				t.Fatalf("decoder kept %d bytes in %d, appending gives %d", len(kept), cap(kept), finalCap)
			}
			est, err := otlpDecodedSize(body, &coltracepb.ExportTraceServiceRequest{}, false)
			if err != nil {
				t.Fatal(err)
			}
			if est < p {
				t.Fatalf("estimate %d is under the %d bytes the decode holds at its peak (%.2f times)", est, p, float64(est)/float64(p))
			}
		})
	}

	// The peak depends on where the body ends relative to append's growth
	// steps, so sweep its length: every count of fields up to 300, for
	// these field sizes. 3 makes 6-byte fields, whose 43rd lands the
	// worst peak found over every size from 1 to 4096 bytes and every 997th
	// above, 2.977 times the wire bytes (the least headroom under
	// otlpUnknownGrowth = 3); 300 and 700 fall between the others.
	for _, size := range []int{3, 64, 300, 700, 1024, 64 << 10} {
		f := bytesField(size)
		var body, sim []byte
		var p int64
		for n := 1; n <= 300; n++ {
			body = append(body, f...)
			before := cap(sim)
			sim = append(sim, f...)
			if cap(sim) != before {
				p = max(p, int64(before+cap(sim)))
			}
			est, err := otlpDecodedSize(body, &commonpb.AnyValue{}, false)
			if err != nil {
				t.Fatal(err)
			}
			if est < p {
				t.Fatalf("%d fields of %d bytes: estimate %d is under the %d bytes held at the peak", n, size, est, p)
			}
		}
	}
}

// proto.Unmarshal keeps an unknown field byte for byte, and a varint can be
// padded (protowire accepts up to 10 bytes), so the estimate charges the
// bytes consumed, not the canonical size of the tag and length. Tiny fields
// with a padded length prefix are the case the 16-byte floor didn't cover:
// 12 bytes kept per field, at 3 times the 3-byte canonical size.
func TestOTLPPaddedVarintsChargedAsConsumed(t *testing.T) {
	pad := func(b []byte, v uint64, n int) []byte {
		for range n - 1 {
			b = append(b, byte(v&0x7f)|0x80)
			v >>= 7
		}
		return append(b, byte(v))
	}
	tag := uint64(99<<3 | uint64(protowire.BytesType))
	for _, c := range []struct {
		name           string
		tagN, lenN, sz int
	}{
		{"canonical", 2, 1, 0},
		{"padded length", 2, 10, 0},
		{"padded tag", 10, 1, 0},
		{"padded both", 10, 10, 0},
		{"padded length, 64 bytes", 2, 10, 64},
	} {
		t.Run(c.name, func(t *testing.T) {
			field := pad(nil, tag, c.tagN)
			field = pad(field, uint64(c.sz), c.lenN)
			field = append(field, make([]byte, c.sz)...)
			const n = 10000
			body := bytes.Repeat(field, n)
			m := &commonpb.AnyValue{}
			if err := proto.Unmarshal(body, m); err != nil {
				t.Fatal(err)
			}
			kept := len(m.ProtoReflect().GetUnknown())
			est, err := otlpDecodedSize(body, &commonpb.AnyValue{}, false)
			if err != nil {
				t.Fatal(err)
			}
			// The simulated peak of appending what the decoder kept: the
			// bytes it keeps are the ones the estimate must charge.
			var sim []byte
			var peak int64
			for range n {
				before := cap(sim)
				sim = append(sim, make([]byte, kept/n)...)
				if cap(sim) != before {
					peak = max(peak, int64(before+cap(sim)))
				}
			}
			if est < peak {
				t.Fatalf("estimate %d is under the %d bytes the decode holds at its peak: the decoder kept %d bytes for %d on the wire",
					est, peak, kept, len(body))
			}
		})
	}
}

// Known fields of the right wire type are decoded into the message, not kept
// as unknown bytes, so they aren't charged for append's growth.
func TestOTLPKnownScalarsNotChargedForGrowth(t *testing.T) {
	// Span.start_time_unix_nano (7) is a fixed64; Span.kind (6) a varint.
	var span []byte
	for range 1000 {
		span = protowire.AppendTag(span, 7, protowire.Fixed64Type)
		span = protowire.AppendFixed64(span, 1)
	}
	known, err := otlpDecodedSize(span, &tracepb.Span{}, false)
	if err != nil {
		t.Fatal(err)
	}
	empty, _ := otlpDecodedSize(nil, &tracepb.Span{}, false)
	if per := (known - empty) / 1000; per != max(9, otlpFieldCost) {
		t.Fatalf("a known fixed64 field is charged %d bytes", per)
	}
	// The same bytes under a field the message doesn't know are charged
	// otlpUnknownGrowth times.
	var unknown []byte
	for range 1000 {
		unknown = protowire.AppendTag(unknown, 99, protowire.Fixed64Type)
		unknown = protowire.AppendFixed64(unknown, 1)
	}
	got, err := otlpDecodedSize(unknown, &tracepb.Span{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if per := (got - empty) / 1000; per < otlpUnknownGrowth*9 {
		t.Fatalf("an unknown fixed64 field is charged %d bytes", per)
	}
}

// A known field number sent with the wrong wire type is kept as unknown bytes,
// so it is charged like an unknown field: three times its wire size, at least
// the header.
func TestOTLPWrongWireTypeChargedAsUnknown(t *testing.T) {
	// Each case sends 1000 copies of one field, the number and wire type
	// being wrong for the message.
	for _, c := range []struct {
		name  string
		field []byte
	}{
		// Span.parent_span_id (4) is bytes, sent as a varint.
		{"varint for a bytes field", protowire.AppendVarint(protowire.AppendTag(nil, 4, protowire.VarintType), 1)},
		// Span.events (11) is a message, sent as a fixed64.
		{"fixed64 for a message field", protowire.AppendFixed64(protowire.AppendTag(nil, 11, protowire.Fixed64Type), 1)},
		// Span.kind (6) is a varint, sent as a fixed64.
		{"fixed64 for a varint field", protowire.AppendFixed64(protowire.AppendTag(nil, 6, protowire.Fixed64Type), 1)},
		// Span.start_time_unix_nano (7) is a fixed64, sent as a varint.
		{"varint for a fixed64 field", protowire.AppendVarint(protowire.AppendTag(nil, 7, protowire.VarintType), 1)},
		// Span.kind (6) is a varint, sent as 100 bytes.
		{"bytes for a varint field", protowire.AppendBytes(protowire.AppendTag(nil, 6, protowire.BytesType), make([]byte, 100))},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := bytes.Repeat(c.field, 1000)
			empty, err := otlpDecodedSize(nil, &tracepb.Span{}, false)
			if err != nil {
				t.Fatal(err)
			}
			got, err := otlpDecodedSize(body, &tracepb.Span{}, false)
			if err != nil {
				t.Fatal(err)
			}
			want := max(otlpUnknownGrowth*int64(len(c.field)), otlpFieldCost)
			if per := (got - empty) / 1000; per < want {
				t.Fatalf("a %d-byte field is charged %d bytes, want at least %d", len(c.field), per, want)
			}
			// The decoder does keep them as unknown bytes.
			m := &tracepb.Span{}
			if err := proto.Unmarshal(body, m); err != nil {
				t.Fatal(err)
			}
			if kept := len(m.ProtoReflect().GetUnknown()); kept != len(body) {
				t.Fatalf("decoder kept %d of %d bytes as unknown", kept, len(body))
			}
		})
	}
}

// Known gap: a repeated string or bytes field takes the packed-scalar branch,
// so an element is charged its length times 16, and an empty one nothing,
// though it decodes to a 16-byte string header in a growing slice.
// EntityRef.id_keys (Resource.entity_refs, field 3) is the only such field in
// OTLP's messages (zerobull-consulting/dagger#217 tracks the fix). A separate
// run of 1,000,000 empty elements for zerobull-consulting/dagger#204, not
// what this test builds (it uses n = 100000): 2,000,012 bytes on the wire,
// estimated at 462 bytes, allocated 88 MB in total and retained 15.8 MB.
func TestOTLPEmptyRepeatedStringsKnownGap(t *testing.T) {
	const n = 100000
	var ent []byte
	for range n {
		ent = protowire.AppendTag(ent, 3, protowire.BytesType) // id_keys
		ent = protowire.AppendBytes(ent, nil)
	}
	wrap := func(num protowire.Number, b []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, num, protowire.BytesType), b)
	}
	// ExportTraceServiceRequest.resource_spans (1) > ResourceSpans.resource
	// (1) > Resource.entity_refs (3) > EntityRef.id_keys (3).
	body := wrap(1, wrap(1, wrap(3, ent)))
	m := &coltracepb.ExportTraceServiceRequest{}
	if err := proto.Unmarshal(body, m); err != nil {
		t.Fatal(err)
	}
	keys := m.ResourceSpans[0].Resource.EntityRefs[0].IdKeys
	if len(keys) != n {
		t.Fatalf("%d id_keys decoded", len(keys))
	}
	held := int64(len(keys)) * int64(unsafe.Sizeof(""))
	est, err := otlpDecodedSize(body, &coltracepb.ExportTraceServiceRequest{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if est >= held {
		t.Fatalf("the estimate %d now covers the %d bytes of string headers: the gap is closed, "+
			"so replace this test with one that checks it", est, held)
	}
}
