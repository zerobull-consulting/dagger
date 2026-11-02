package e2e

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	req, _ := http.NewRequest(http.MethodPut, blob, strings.NewReader("data"))
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
