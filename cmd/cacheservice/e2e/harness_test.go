package e2e

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
)

// The proxy forwards with the Host header it got, which presigned URLs are
// signed for, and records each request with the store's status.
func TestS3ProxyRecords(t *testing.T) {
	var hosts []string
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hosts = append(hosts, r.Host)
		switch {
		case r.Method == http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
		case strings.HasSuffix(r.URL.Path, "/gone"):
			http.Error(w, "no such key", http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer store.Close()
	p := StartS3Proxy(t, "127.0.0.1", store.URL)

	d := digest.FromString("layer")
	blob := p.URL + "/bucket/prefix/blobs/sha256/" + d.Encoded()
	staged := p.URL + "/bucket/prefix/staging/0123456789abcdef0123456789abcdef/sha256/" + d.Encoded()
	before := p.Mark()
	resp, err := http.Get(blob)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("body %q", body)
	}
	req, _ := http.NewRequest(http.MethodPut, staged, strings.NewReader("data"))
	if resp, err = http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// A promotion's part copy, a PUT to the digest key, is the service's,
	// not an engine's upload.
	req, _ = http.NewRequest(http.MethodPut, blob+"?partNumber=1&uploadId=x", nil)
	if resp, err = http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mid := p.Mark()
	if resp, err = http.Get(p.URL + "/bucket/prefix/e2e/gone"); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	want := strings.TrimPrefix(p.URL, "http://")
	for _, h := range hosts {
		if h != want {
			t.Fatalf("the store got Host %q, want the proxy's %q", h, want)
		}
	}
	gets, puts := p.Blobs(before, http.MethodGet), p.Blobs(before, http.MethodPut)
	if len(gets) != 1 || len(puts) != 1 || !gets[0].IsBlobOf(d) || !puts[0].IsBlobOf(d) || gets[0].Status != http.StatusOK {
		t.Fatalf("blob GETs %+v, PUTs %+v", gets, puts)
	}
	rest := p.Since(mid)
	if len(rest) != 1 || rest[0].Status != http.StatusNotFound || rest[0].IsBlob() {
		t.Fatalf("after the mark: %+v", rest)
	}
	if time.Since(rest[0].Time) > time.Minute {
		t.Fatalf("request time %s", rest[0].Time)
	}
}

// Faults answer matching requests with an S3 error up to their count, or
// stall them until removed, and are recorded; other requests reach the
// store.
func TestS3ProxyFaults(t *testing.T) {
	var reached []string
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = append(reached, r.Method+" "+r.URL.Path)
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer store.Close()
	p := StartS3Proxy(t, "127.0.0.1", store.URL)
	put := func(path string) int {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, p.URL+path, strings.NewReader("data"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable && !strings.Contains(string(body), "<Code>SlowDown</Code>") {
			t.Fatalf("error body %q", body)
		}
		return resp.StatusCode
	}
	slow := p.Inject(&Fault{Name: "slow", Match: Methods("/blobs/", http.MethodPut), Status: http.StatusServiceUnavailable, Code: "SlowDown", Count: 2})
	got := []int{put("/b/blobs/sha256/1"), put("/b/other"), put("/b/blobs/sha256/1"), put("/b/blobs/sha256/1")}
	if !slices.Equal(got, []int{503, 200, 503, 200}) || p.Hits(slow) != 2 {
		t.Fatalf("statuses %v, hits %d", got, p.Hits(slow))
	}
	if !slices.Equal(reached, []string{"PUT /b/other", "PUT /b/blobs/sha256/1"}) {
		t.Fatalf("the store got %v", reached)
	}
	if rs := p.Since(0); rs[0].Fault != "slow" || rs[0].Status != 503 || rs[1].Fault != "" {
		t.Fatalf("recorded %+v", rs)
	}

	stall := p.Inject(&Fault{Name: "stall", Match: Methods("/stalled")})
	done := make(chan int, 1)
	go func() { done <- put("/b/stalled") }()
	deadline := time.Now().Add(5 * time.Second)
	for p.Hits(stall) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the request didn't reach the stall")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case code := <-done:
		t.Fatalf("a stalled request was answered %d", code)
	case <-time.After(100 * time.Millisecond):
	}
	p.Remove(stall)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("a released request was answered %d", code)
	}
	if rs := p.Since(0); !rs[len(rs)-1].Stalled || rs[len(rs)-1].Status != 200 {
		t.Fatalf("recorded %+v", rs[len(rs)-1])
	}
}

// The recorder keeps records at every level, with the attributes added by
// With, and still writes to its handler at that handler's level.
func TestLogRecorder(t *testing.T) {
	var out strings.Builder
	rec := &LogRecorder{}
	logger := slog.New(rec.Handler(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo})))
	logger.Info("before")
	mark := rec.Mark()
	conn := logger.With("conn", uint64(7))
	conn.Info("envelope", "type", "renew", "layers", 3)
	logger.Debug("cache span", "cache", "cache-a/2", "name", "Container.withExec(...)", "outcome", "hit")
	logger.Debug("cache span", "cache", "cache-ab/1", "name", "Container.withExec(...)", "outcome", "executed")

	envs := rec.Since(mark, "envelope", func(r LogRecord) bool { return r.Attr("conn") == "7" })
	if len(envs) != 1 || envs[0].Attr("type") != "renew" || envs[0].Attr("layers") != "3" {
		t.Fatalf("envelopes %+v", envs)
	}
	if spans := rec.CacheSpans(0, "cache-a", "Container.withExec"); len(spans) != 1 || spans[0].Attr("outcome") != "hit" {
		t.Fatalf("cache-a's spans %+v", spans)
	}
	if n := len(rec.Since(0, "", nil)); n != 4 {
		t.Fatalf("%d records", n)
	}
	if strings.Contains(out.String(), "cache span") || !strings.Contains(out.String(), "conn=7") {
		t.Fatalf("written:\n%s", out.String())
	}
}

func TestField(t *testing.T) {
	res := map[string]any{"a": map[string]any{"b": "x", "n": 3.0}}
	for path, want := range map[string]string{"a.b": "x", "a.n": "3", "a.c": "", "a.b.c": ""} {
		if got := Field(res, path); got != want {
			t.Errorf("%s: %q, want %q", path, got, want)
		}
	}
	if got := stampSums("exec1 u1 aaaa\nexec2 u2 bbbb\n"); got != "aaaa\nbbbb" {
		t.Errorf("stampSums: %q", got)
	}
}

// A Rewrite fault passes the request on with its body replaced, at the same
// length, and the store answers it: as an engine uploading other content of
// the declared size would.
func TestS3ProxyRewrite(t *testing.T) {
	var got []string
	var lengths []int64
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = append(got, string(body))
		lengths = append(lengths, r.ContentLength)
	}))
	defer store.Close()
	p := StartS3Proxy(t, "127.0.0.1", store.URL)
	f := p.Inject(&Fault{Name: "corrupt", Match: Methods("/staging/", http.MethodPut), Rewrite: SameSizeCorruption, Count: 1})
	for range 2 {
		req, _ := http.NewRequest(http.MethodPut, p.URL+"/b/staging/s/sha256/1", strings.NewReader("data"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	if len(got) != 2 || got[0] == "data" || len(got[0]) != 4 || lengths[0] != 4 || got[1] != "data" || p.Hits(f) != 1 {
		t.Fatalf("the store got %q (lengths %v), hits %d", got, lengths, p.Hits(f))
	}
	rs := p.Since(0)
	if !rs[0].Rewritten || rs[0].Fault != "corrupt" || rs[0].Status != http.StatusOK || rs[1].Rewritten || !rs[0].IsBlob() {
		t.Fatalf("recorded %+v", rs)
	}
}

// The OTLP size check reads every request taken, counts refusals of every
// kind, undecodable ones included, and sets apart a request logged without
// its sizes rather than reading them as 0.
func TestOTLPSizesOf(t *testing.T) {
	rec := &LogRecorder{}
	logger := slog.New(rec.Handler(nil))
	logger.Debug("otlp request accepted", "route", "traces", "receivedBytes", 100, "decodedBytes", 300, "messageBytes", 900, "headerBytes", 700, "budgetPeak", 300)
	logger.Debug("otlp request accepted", "route", "traces", "receivedBytes", 50, "decodedBytes", 50, "messageBytes", 100, "headerBytes", 800, "budgetPeak", 900)
	logger.Debug("otlp request accepted", "route", "logs", "receivedBytes", 7, "decodedBytes", 7, "messageBytes", 70, "headerBytes", 5, "budgetPeak", 7)
	logger.Debug("otlp request accepted", "route", "logs", "receivedBytes", "x", "decodedBytes", 7, "messageBytes", 70, "headerBytes", 5, "budgetPeak", 7)
	logger.Debug("otlp request accepted", "route", "logs", "receivedBytes", 7, "decodedBytes", 7, "messageBytes", 70, "budgetPeak", 7)
	logger.Debug("otlp request accepted", "route", "logs", "receivedBytes", 7, "decodedBytes", 7, "budgetPeak", 7, "headerBytes", 5)
	logger.Warn("otlp request refused", "route", "traces", "reason", "too_large")
	logger.Warn("undecodable otlp request", "path", "/v1/logs")
	sizes := otlpSizesOf(rec)
	tr, lg := sizes.largest["traces"], sizes.largest["logs"]
	if tr == nil || tr.n != 2 || tr.received != 100 || tr.decoded != 300 || tr.message != 900 || tr.headers != 800 || lg == nil || lg.n != 1 || sizes.budgetPeak != 900 {
		t.Fatalf("sizes %+v %+v, peak %d", tr, lg, sizes.budgetPeak)
	}
	if len(sizes.refused) != 2 || len(sizes.malformed) != 3 {
		t.Fatalf("%d refused, %d malformed", len(sizes.refused), len(sizes.malformed))
	}
}
