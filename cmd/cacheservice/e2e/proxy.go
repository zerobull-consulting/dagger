package e2e

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
)

// S3Request is one request through the S3 proxy.
type S3Request struct {
	// Time is when the proxy received the request.
	Time   time.Time
	Method string
	// Path is the request path: /<bucket>/<key> (the services use
	// path-style addressing).
	Path string
	// Status is the store's answer (0: not answered yet).
	Status int
}

// IsBlob reports whether the request is for a layer blob's object.
func (r S3Request) IsBlob() bool { return strings.Contains(r.Path, "/blobs/") }

// IsBlobOf reports whether the request is for the blob of digest d.
func (r S3Request) IsBlobOf(d digest.Digest) bool {
	return strings.HasSuffix(r.Path, "/blobs/"+d.Algorithm().String()+"/"+d.Encoded())
}

// S3Proxy sits between everything and the S3 store, and records each
// request: the services are configured with its address as their S3
// endpoint, so the presigned URLs they hand engines point at it too. It
// keeps the Host header, which presigned URLs are signed for.
//
// It replaces tracing the store: which blobs an engine read or uploaded,
// when, and what the store answered.
type S3Proxy struct {
	// URL is the proxy's address, as engines in containers reach it.
	URL string

	mu   sync.Mutex
	reqs []S3Request
}

// StartS3Proxy starts a proxy to the S3 endpoint target, listening on host
// (an address engine containers reach) on a free port, until the test ends.
func StartS3Proxy(t *testing.T, host, target string) *S3Proxy {
	t.Helper()
	to, err := url.Parse(target)
	if err != nil {
		t.Fatalf("S3 endpoint %q: %v", target, err)
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatalf("S3 proxy: %v", err)
	}
	p := &S3Proxy{URL: "http://" + ln.Addr().String()}
	rp := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(to)
			// Presigned URLs are signed for the proxy's host: the store
			// checks the signature against the Host it receives.
			r.Out.Host = r.In.Host
		},
		// Stream bodies as they come: layer blobs are large.
		FlushInterval: -1,
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Recorded as it arrives, with its status set as the response
		// starts: before the client can see the response, so a client
		// that got one always finds its request recorded.
		p.mu.Lock()
		i := len(p.reqs)
		p.reqs = append(p.reqs, S3Request{Time: time.Now(), Method: r.Method, Path: r.URL.Path})
		p.mu.Unlock()
		sw := &statusWriter{ResponseWriter: w, onStatus: func(code int) {
			p.mu.Lock()
			p.reqs[i].Status = code
			p.mu.Unlock()
		}}
		rp.ServeHTTP(sw, r)
		sw.WriteHeader(http.StatusOK) // no-op unless nothing was written
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return p
}

// Mark returns a position in the record: Since(mark) reads what comes after
// it.
func (p *S3Proxy) Mark() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.reqs)
}

// Since returns the requests after mark.
func (p *S3Proxy) Since(mark int) []S3Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]S3Request(nil), p.reqs[min(mark, len(p.reqs)):]...)
}

// Blobs returns the requests after mark for layer blobs with method.
func (p *S3Proxy) Blobs(mark int, method string) []S3Request {
	var out []S3Request
	for _, r := range p.Since(mark) {
		if r.Method == method && r.IsBlob() {
			out = append(out, r)
		}
	}
	return out
}

type statusWriter struct {
	http.ResponseWriter
	onStatus func(int)
	wrote    bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	// Informational responses, such as 100 Continue to a PUT, come before
	// the final one: pass them on.
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	w.wrote = true
	w.onStatus(code)
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	w.WriteHeader(http.StatusOK)
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
