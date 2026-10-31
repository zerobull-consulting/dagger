package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// logRecorder is a slog handler that keeps every record, to check what the
// service logs.
type logRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *logRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logRecorder) WithGroup(string) slog.Handler      { return h }

// find returns the attributes of every record with the message msg.
func (h *logRecorder) find(msg string) []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var found []map[string]string
	for _, r := range h.records {
		if r.Message != msg {
			continue
		}
		attrs := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		found = append(found, attrs)
	}
	return found
}

func newTestService(t *testing.T, dumpDir string) (*service, *logRecorder, *httptest.Server) {
	t.Helper()
	rec := &logRecorder{}
	svc := newService(slog.New(rec), dumpDir)
	srv := httptest.NewServer(svc.handler())
	t.Cleanup(srv.Close)
	return svc, rec, srv
}

// rawEngine is a hand-driven engine connection, to send the service what the
// real engine never would.
type rawEngine struct {
	t    *testing.T
	conn *websocket.Conn
}

func dialRaw(t *testing.T, srv *httptest.Server, token string) *rawEngine {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	header := http.Header{}
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(token+":")))
	conn, resp, err := websocket.Dial(ctx, srv.URL+protocol.EnginePath, &websocket.DialOptions{HTTPHeader: header})
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &rawEngine{t: t, conn: conn}
}

func (e *rawEngine) send(env protocol.Envelope) {
	e.t.Helper()
	data, err := json.Marshal(env)
	if err != nil {
		e.t.Fatal(err)
	}
	e.sendRaw(websocket.MessageText, data)
}

func (e *rawEngine) sendRaw(typ websocket.MessageType, data []byte) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), 10*time.Second)
	defer cancel()
	if err := e.conn.Write(ctx, typ, data); err != nil {
		e.t.Fatal(err)
	}
}

func (e *rawEngine) next() protocol.Envelope {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), 10*time.Second)
	defer cancel()
	_, data, err := e.conn.Read(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		e.t.Fatal(err)
	}
	return env
}

// closed waits for the service to close the connection, and returns the
// close status.
func (e *rawEngine) closed() websocket.StatusCode {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), 10*time.Second)
	defer cancel()
	for {
		_, _, err := e.conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				e.t.Fatal("the service did not close the connection")
			}
			return websocket.CloseStatus(err)
		}
	}
}

func (e *rawEngine) hello() {
	e.t.Helper()
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{
		CacheID: "cache-raw", Generation: 9, EngineVersion: "v-raw", EngineName: "raw",
	})
	if err != nil {
		e.t.Fatal(err)
	}
	e.send(env)
	welcome := e.next()
	if welcome.Type != protocol.TypeWelcome || welcome.Re != 1 {
		e.t.Fatalf("hello answered with %+v", welcome)
	}
}

func decodeBody[T any](t *testing.T, env protocol.Envelope) T {
	t.Helper()
	var v T
	if err := env.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// The service answers hello with welcome, numbering its own messages from 1
// and naming the hello in Re.
func TestHelloWelcome(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	env, _ := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "c", Generation: 2})
	e.send(env)
	welcome := e.next()
	if welcome.Type != protocol.TypeWelcome || welcome.ID != 1 || welcome.Re != 1 {
		t.Fatalf("unexpected welcome: %+v", welcome)
	}
	c := waitEngines(t, svc, 1)[0]
	if c.hello.CacheID != "c" || c.hello.Generation != 2 {
		t.Fatalf("hello not recorded: %+v", c.hello)
	}
}

// A first message other than hello is answered with an error, and the
// connection ends without the engine being listed.
func TestFirstMessageMustBeHello(t *testing.T) {
	for name, env := range map[string]protocol.Envelope{
		"not hello":   {ID: 1, Type: protocol.TypeRenew, Body: json.RawMessage(`{}`)},
		"hello reply": {ID: 1, Re: 5, Type: protocol.TypeHello, Body: json.RawMessage(`{}`)},
		"bad body":    {ID: 1, Type: protocol.TypeHello, Body: json.RawMessage(`"not an object"`)},
	} {
		t.Run(name, func(t *testing.T) {
			svc, _, srv := newTestService(t, "")
			e := dialRaw(t, srv, "tok")
			e.send(env)
			reply := e.next()
			if reply.Type != protocol.TypeError || reply.Re != 1 {
				t.Fatalf("expected an error reply to 1, got %+v", reply)
			}
			e.closed()
			svc.mu.Lock()
			n := len(svc.conns)
			svc.mu.Unlock()
			if n != 0 {
				t.Fatalf("%d engines listed after a refused hello", n)
			}
		})
	}
}

// Nothing is stored, so every renewal is unavailable.
func TestRenewIsUnavailable(t *testing.T) {
	_, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	e.hello()
	env, _ := protocol.NewEnvelope(2, 0, protocol.TypeRenew, protocol.Renew{RenewalKey: "k"})
	e.send(env)
	reply := e.next()
	if reply.Type != protocol.TypeRenewed || reply.Re != 2 {
		t.Fatalf("renew answered with %+v", reply)
	}
	if renewed := decodeBody[protocol.Renewed](t, reply); !renewed.Unavailable {
		t.Fatalf("renewal not unavailable: %+v", renewed)
	}
}

// An engine request the service does not serve is answered with an error,
// and the connection stays up.
func TestUnexpectedEngineRequest(t *testing.T) {
	_, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	e.hello()
	e.send(protocol.Envelope{ID: 2, Type: protocol.TypeMerge, Body: json.RawMessage(`{}`)})
	reply := e.next()
	if reply.Type != protocol.TypeError || reply.Re != 2 {
		t.Fatalf("merge answered with %+v", reply)
	}
	if msg := decodeBody[protocol.Error](t, reply).Message; !strings.Contains(msg, "merge") {
		t.Fatalf("error does not name the request: %q", msg)
	}

	// Still served.
	env, _ := protocol.NewEnvelope(3, 0, protocol.TypeRenew, protocol.Renew{})
	e.send(env)
	if reply := e.next(); reply.Re != 3 {
		t.Fatalf("connection not served after the error: %+v", reply)
	}
}

// Binary and undecodable messages are skipped; a reply to no request is
// logged and dropped. None ends the connection.
func TestServiceSkipsBadMessages(t *testing.T) {
	_, rec, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	e.sendRaw(websocket.MessageBinary, []byte("binary"))
	e.sendRaw(websocket.MessageText, []byte("{not json"))
	e.hello()
	e.send(protocol.Envelope{ID: 2, Re: 99, Type: protocol.TypeExported, Body: json.RawMessage(`{}`)})
	env, _ := protocol.NewEnvelope(3, 0, protocol.TypeRenew, protocol.Renew{})
	e.send(env)
	if reply := e.next(); reply.Re != 3 {
		t.Fatalf("connection not served: %+v", reply)
	}
	for _, msg := range []string{"ignoring non-text message", "undecodable message", "reply to no pending request"} {
		if len(rec.find(msg)) != 1 {
			t.Errorf("expected one %q log", msg)
		}
	}
}

// A service request waiting on an engine that disconnects fails.
func TestRequestFailsWhenEngineDisconnects(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	e.hello()
	c := waitEngines(t, svc, 1)[0]

	errs := make(chan error, 1)
	go func() {
		_, err := c.request(t.Context(), protocol.TypeExport, json.RawMessage(`{"roots":[1]}`))
		errs <- err
	}()
	if req := e.next(); req.Type != protocol.TypeExport || req.Re != 0 {
		t.Fatalf("unexpected request: %+v", req)
	}
	_ = e.conn.CloseNow()
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("request succeeded on a closed connection")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("request did not fail")
	}
	waitEngines(t, svc, 0)
}

// A service request ends when its context does, and stops waiting for the
// reply.
func TestRequestCancelled(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	e.hello()
	c := waitEngines(t, svc, 1)[0]

	ctx, cancel := context.WithCancel(t.Context())
	errs := make(chan error, 1)
	go func() {
		_, err := c.request(ctx, protocol.TypeOffer, json.RawMessage(`{}`))
		errs <- err
	}()
	e.next()
	cancel()
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("cancelled request succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled request did not return")
	}
	c.mu.Lock()
	n := len(c.pending)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d requests still pending", n)
	}
}

// The service's requests and replies share one numbering.
func TestServiceNumbersItsMessages(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	e := dialRaw(t, srv, "tok")
	e.hello() // welcome is 1
	c := waitEngines(t, svc, 1)[0]

	go func() { _, _ = c.request(t.Context(), protocol.TypeMerge, json.RawMessage(`{}`)) }()
	req := e.next()
	if req.ID != 2 {
		t.Fatalf("request numbered %d", req.ID)
	}
	env, _ := protocol.NewEnvelope(2, 0, protocol.TypeRenew, protocol.Renew{})
	e.send(env)
	if reply := e.next(); reply.ID != 3 {
		t.Fatalf("reply numbered %d", reply.ID)
	}
}

// closeAll closes every engine with going away.
func TestCloseAll(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	a, b := dialRaw(t, srv, "a"), dialRaw(t, srv, "b")
	a.hello()
	b.hello()
	waitEngines(t, svc, 2)
	svc.closeAll()
	for _, e := range []*rawEngine{a, b} {
		if status := e.closed(); status != websocket.StatusGoingAway {
			t.Fatalf("closed with %d", status)
		}
	}
	waitEngines(t, svc, 0)
}

// The service logs a fingerprint of the token, never the token.
func TestTokenFingerprint(t *testing.T) {
	if tokenFingerprint("a") == tokenFingerprint("b") {
		t.Fatal("different tokens share a fingerprint")
	}
	if tokenFingerprint("a") != tokenFingerprint("a") {
		t.Fatal("fingerprint is not stable")
	}
	if fp := tokenFingerprint("secret"); len(fp) != 8 || strings.Contains(fp, "secret") {
		t.Fatalf("unexpected fingerprint %q", fp)
	}

	svc, rec, srv := newTestService(t, "")
	e := dialRaw(t, srv, "very-secret-token")
	e.hello()
	waitEngines(t, svc, 1)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, r := range rec.records {
		r.Attrs(func(a slog.Attr) bool {
			if strings.Contains(a.Value.String(), "very-secret-token") {
				t.Errorf("token logged in %q", r.Message)
			}
			return true
		})
	}
}

// With a dump directory, every envelope's body is written to it, named by
// sequence, connection, direction, type and ID.
func TestDumpEnvelopes(t *testing.T) {
	dir := t.TempDir()
	_, _, srv := newTestService(t, dir)
	e := dialRaw(t, srv, "tok")
	e.hello()

	want := []string{"000001-conn1-in-hello-id1.json", "000002-conn1-out-welcome-id1.json"}
	var names []string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		names = names[:0]
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		if len(names) == len(want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("dumped %v, want %v", names, want)
	}
	data, err := os.ReadFile(filepath.Join(dir, want[0]))
	if err != nil {
		t.Fatal(err)
	}
	var hello protocol.Hello
	if err := json.Unmarshal(data, &hello); err != nil || hello.CacheID != "cache-raw" {
		t.Fatalf("dumped hello %s: %v", data, err)
	}
}

func TestSummarize(t *testing.T) {
	env := func(typ protocol.Type, body any) protocol.Envelope {
		e, err := protocol.NewEnvelope(1, 0, typ, body)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	for _, tc := range []struct {
		env  protocol.Envelope
		want map[string]any
	}{
		{env(protocol.TypeHello, protocol.Hello{CacheID: "c", Generation: 4}), map[string]any{"cacheID": "c", "generation": uint64(4)}},
		{env(protocol.TypeRenew, protocol.Renew{RenewalKey: "k"}), map[string]any{"renewalKey": "k", "layers": 0}},
		{env(protocol.TypeExported, protocol.Exported{ExportID: "x"}), map[string]any{"exportID": "x", "bundle": false}},
		{env(protocol.TypeUploaded, protocol.Uploaded{}), map[string]any{"done": 0, "failed": 0}},
		{env(protocol.TypeMerged, protocol.Merged{Generation: 6}), map[string]any{"generation": uint64(6)}},
		{env(protocol.TypeOffered, protocol.Offered{Items: []protocol.OfferedItem{{Number: 1}}}), map[string]any{"items": 1}},
		{env(protocol.TypeError, protocol.Error{Message: "boom"}), map[string]any{"message": "boom"}},
		{env(protocol.TypeWelcome, protocol.Welcome{}), nil},
		{protocol.Envelope{Type: protocol.TypeHello, Body: json.RawMessage(`"bad"`)}, nil},
	} {
		attrs := summarize(tc.env)
		if len(tc.want) == 0 {
			if len(attrs) != 0 {
				t.Errorf("%s: unexpected summary %v", tc.env.Type, attrs)
			}
			continue
		}
		got := map[string]any{}
		for i := 0; i+1 < len(attrs); i += 2 {
			got[attrs[i].(string)] = attrs[i+1]
		}
		for k, v := range tc.want {
			if got[k] != v {
				t.Errorf("%s: %s = %v, want %v", tc.env.Type, k, got[k], v)
			}
		}
	}
}

func TestDebugAPIErrors(t *testing.T) {
	svc := newService(slog.New(slog.DiscardHandler), "")
	debug := httptest.NewServer(svc.debugHandler())
	defer debug.Close()

	post := func(path, body string) int {
		resp, err := http.Post(debug.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for name, tc := range map[string]struct {
		path, body string
		want       int
	}{
		"bad conn":       {"/debug/engines/abc/request?type=export", `{}`, http.StatusBadRequest},
		"not a request":  {"/debug/engines/1/request?type=welcome", `{}`, http.StatusBadRequest},
		"no type":        {"/debug/engines/1/request", `{}`, http.StatusBadRequest},
		"bad body":       {"/debug/engines/1/request?type=export", `{`, http.StatusBadRequest},
		"no such engine": {"/debug/engines/1/request?type=export", `{}`, http.StatusNotFound},
	} {
		if code := post(tc.path, tc.body); code != tc.want {
			t.Errorf("%s: status %d, want %d", name, code, tc.want)
		}
	}

	resp, err := http.Get(debug.URL + "/debug/engines")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("no engines listed as %s", body)
	}

	// The debug API is not served on the engine address.
	_, _, srv := newTestService(t, "")
	resp, err = http.Get(srv.URL + "/debug/engines")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("debug API on the engine address: status %d", resp.StatusCode)
	}
}

// The debug API lists engines in connection order, with their hellos.
func TestDebugListsEnginesInOrder(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	debug := httptest.NewServer(svc.debugHandler())
	defer debug.Close()
	for range 3 {
		dialRaw(t, srv, "tok").hello()
	}
	waitEngines(t, svc, 3)

	resp, err := http.Get(debug.URL + "/debug/engines")
	if err != nil {
		t.Fatal(err)
	}
	var infos []engineInfo
	err = json.NewDecoder(resp.Body).Decode(&infos)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	for i, info := range infos {
		if info.Conn != uint64(i+1) || info.CacheID != "cache-raw" || info.Generation != 9 || info.EngineName != "raw" {
			t.Fatalf("engine %d: %+v", i, info)
		}
		if info.Token != tokenFingerprint("tok") {
			t.Fatalf("engine %d listed with token %q", i, info.Token)
		}
	}
}

// The debug API relays the engine's error reply as the reply.
func TestDebugRelaysEngineError(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	debug := httptest.NewServer(svc.debugHandler())
	defer debug.Close()
	e := dialRaw(t, srv, "tok")
	e.hello()
	c := waitEngines(t, svc, 1)[0]

	go func() {
		req := e.next()
		reply, _ := protocol.NewEnvelope(2, req.ID, protocol.TypeError, protocol.Error{Message: "nope"})
		e.send(reply)
	}()
	resp, err := http.Post(debug.URL+"/debug/engines/"+strconv.FormatUint(c.id, 10)+"/request?type=merge",
		"application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var reply protocol.Envelope
	err = json.NewDecoder(resp.Body).Decode(&reply)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || reply.Type != protocol.TypeError {
		t.Fatalf("status %d, reply %+v", resp.StatusCode, reply)
	}
}

func TestBlobPutBadDigest(t *testing.T) {
	_, _, srv := newTestService(t, "")
	for _, d := range []string{"not-a-digest", "sha256:short"} {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/blobs/"+d, strings.NewReader("x"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d", d, resp.StatusCode)
		}
	}
	// Only PUT uploads.
	resp, err := http.Get(srv.URL + "/blobs/sha256:" + strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("GET of a blob succeeded")
	}
}

func strValue(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func kv(k, v string) *commonpb.KeyValue { return &commonpb.KeyValue{Key: k, Value: strValue(v)} }

func arrayKV(k string, n int) *commonpb.KeyValue {
	values := make([]*commonpb.AnyValue, n)
	for i := range values {
		values[i] = strValue(strconv.Itoa(i))
	}
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: values}},
	}}
}

func engineLogs() *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("service.instance.id", "engine-1")}},
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: "dagger.io/engine"},
			LogRecords: []*logspb.LogRecord{
				{
					Body:       strValue(`{"entries":3}`),
					Attributes: []*commonpb.KeyValue{kv(telemetryattrs.EngineEventAttr, "cache.export"), kv(telemetryattrs.EngineCacheAttr, "cache-1")},
				},
				{Body: strValue("an ordinary log line")},
			},
		}},
	}}}
}

// otlpPost posts msg to path as protobuf or JSON, gzipped or not, and decodes
// the response into resp.
func otlpPost(t *testing.T, url string, msg, resp proto.Message, asJSON, gz bool) int {
	t.Helper()
	var (
		data []byte
		err  error
	)
	contentType := "application/x-protobuf"
	if asJSON {
		contentType = "application/json"
		data, err = protojson.Marshal(msg)
	} else {
		data, err = proto.Marshal(msg)
	}
	if err != nil {
		t.Fatal(err)
	}
	if gz {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		_, _ = w.Write(data)
		_ = w.Close()
		data = buf.Bytes()
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	req.Header.Set("Content-Type", contentType)
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return res.StatusCode
	}
	if got := res.Header.Get("Content-Type"); got != contentType {
		t.Errorf("answered %s with %s", contentType, got)
	}
	if asJSON {
		err = protojson.Unmarshal(body, resp)
	} else {
		err = proto.Unmarshal(body, resp)
	}
	if err != nil {
		t.Fatalf("undecodable response %q: %v", body, err)
	}
	return res.StatusCode
}

// The OTLP log receiver takes protobuf and JSON, gzipped or not, answers in
// kind, and logs the engine's cache events.
func TestOTLPLogs(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		for _, gz := range []bool{false, true} {
			t.Run("json="+strconv.FormatBool(asJSON)+",gzip="+strconv.FormatBool(gz), func(t *testing.T) {
				dir := t.TempDir()
				_, rec, srv := newTestService(t, dir)
				code := otlpPost(t, srv.URL+"/v1/logs", engineLogs(), &collogspb.ExportLogsServiceResponse{}, asJSON, gz)
				if code != http.StatusOK {
					t.Fatalf("status %d", code)
				}
				events := rec.find("engine event")
				if len(events) != 1 {
					t.Fatalf("%d engine events logged", len(events))
				}
				ev := events[0]
				if ev["kind"] != "cache.export" || ev["cache"] != "cache-1" || ev["instance"] != "engine-1" || ev["scope"] != "dagger.io/engine" {
					t.Fatalf("unexpected event log: %v", ev)
				}
				data, err := os.ReadFile(filepath.Join(dir, "000001-event-cache.export.json"))
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != `{"entries":3}` {
					t.Fatalf("dumped %s", data)
				}
			})
		}
	}
}

func TestOTLPTraces(t *testing.T) {
	_, rec, srv := newTestService(t, "")
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv(telemetryattrs.EngineCacheAttr, "cache-1")}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			{Name: "plain"},
			{Name: "cached", Attributes: []*commonpb.KeyValue{
				kv(telemetryattrs.CacheResultIDAttr, "42"),
				kv(telemetryattrs.CacheOutcomeAttr, "hit"),
				arrayKV(telemetryattrs.CacheDepsAttr, 2),
				arrayKV(telemetryattrs.CachePartsAttr, 3),
			}},
		}}},
	}}}
	if code := otlpPost(t, srv.URL+"/v1/traces", req, &coltracepb.ExportTraceServiceResponse{}, false, false); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	traces := rec.find("otlp traces")
	if len(traces) != 1 || traces[0]["spans"] != "2" || traces[0]["cacheSpans"] != "1" {
		t.Fatalf("unexpected trace logs: %v", traces)
	}
	spans := rec.find("cache span")
	if len(spans) != 1 {
		t.Fatalf("%d cache spans logged", len(spans))
	}
	if s := spans[0]; s["result"] != "42" || s["outcome"] != "hit" || s["cache"] != "cache-1" || s["deps"] != "2" || s["parts"] != "3" {
		t.Fatalf("unexpected cache span log: %v", s)
	}
}

func TestOTLPMetrics(t *testing.T) {
	_, rec, srv := newTestService(t, "")
	req := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{Name: "a"}, {Name: "b"}}}},
	}}}
	if code := otlpPost(t, srv.URL+"/v1/metrics", req, &colmetricspb.ExportMetricsServiceResponse{}, true, false); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if logs := rec.find("otlp metrics"); len(logs) != 1 || logs[0]["metrics"] != "2" {
		t.Fatalf("unexpected metric logs: %v", logs)
	}
}

func TestOTLPRejectsBadRequests(t *testing.T) {
	_, _, srv := newTestService(t, "")
	for name, tc := range map[string]struct {
		contentType, encoding, body string
	}{
		"bad protobuf": {"application/x-protobuf", "", "\xff\xff\xff"},
		"bad json":     {"application/json", "", "{"},
		"bad gzip":     {"application/x-protobuf", "gzip", "not gzip"},
	} {
		for _, path := range []string{"/v1/logs", "/v1/traces", "/v1/metrics"} {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s %s: status %d", name, path, resp.StatusCode)
			}
		}
	}
}

func TestUnhandledRequest(t *testing.T) {
	_, rec, srv := newTestService(t, "")
	resp, err := http.Get(srv.URL + "/nowhere")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if logs := rec.find("unhandled request"); len(logs) != 1 || logs[0]["path"] != "/nowhere" {
		t.Fatalf("unexpected logs: %v", logs)
	}
}

func TestHelpers(t *testing.T) {
	if got := truncate("abc", 3); got != "abc" {
		t.Errorf("truncate kept %q", got)
	}
	if got := truncate("abcdef", 3); got != "abc…" {
		t.Errorf("truncate made %q", got)
	}
	attrs := []*commonpb.KeyValue{kv("a", "1"), arrayKV("list", 4)}
	if stringAttr(attrs, "a") != "1" || stringAttr(attrs, "missing") != "" || stringAttr(attrs, "list") != "" {
		t.Error("stringAttr")
	}
	if arrayLen(attrs, "list") != 4 || arrayLen(attrs, "a") != 0 || arrayLen(attrs, "missing") != 0 {
		t.Error("arrayLen")
	}
}
