package e2e

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
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
	// Status is the store's answer (0: not answered yet), or the injected
	// fault's.
	Status int
	// Fault names the fault the proxy answered with instead of the store
	// ("" when the store answered), and Stalled is whether a stall held it.
	Fault   string
	Stalled bool
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

	mu     sync.Mutex
	reqs   []S3Request
	faults []*Fault
}

// Fault makes the proxy answer some requests itself instead of passing them
// to the store: with an S3 error (Status and Code), or by stalling them until
// Release. It applies to requests that Match, up to Count of them (0: until
// removed), from when it is injected until it is removed.
type Fault struct {
	// Name is how the requests it answered are recorded.
	Name string
	// Match picks the requests; nil matches every one.
	Match func(method, path string) bool
	// Status and Code are the S3 error to answer with, such as 503
	// SlowDown or 500 InternalError. Status 0 stalls the request instead.
	Status int
	Code   string
	// Count is how many requests it answers; 0 means every matching one.
	Count int

	hits    int
	release chan struct{}
	removed bool
}

// Inject adds a fault, and returns it.
func (p *S3Proxy) Inject(f *Fault) *Fault {
	p.mu.Lock()
	defer p.mu.Unlock()
	f.release = make(chan struct{})
	p.faults = append(p.faults, f)
	return f
}

// Remove removes a fault, and releases the requests it stalled: they go on
// to the store.
func (p *S3Proxy) Remove(f *Fault) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f.removed {
		return
	}
	f.removed = true
	close(f.release)
	for i, g := range p.faults {
		if g == f {
			p.faults = append(p.faults[:i], p.faults[i+1:]...)
			break
		}
	}
}

// Hits is how many requests the fault answered or stalled so far.
func (p *S3Proxy) Hits(f *Fault) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return f.hits
}

// fault returns the fault that takes a request, if any, and counts it.
func (p *S3Proxy) faultLocked(method, path string) *Fault {
	for _, f := range p.faults {
		if f.Count > 0 && f.hits >= f.Count {
			continue
		}
		if f.Match == nil || f.Match(method, path) {
			f.hits++
			return f
		}
	}
	return nil
}

// Methods matches requests with one of methods whose path contains part.
func Methods(part string, methods ...string) func(method, path string) bool {
	return func(method, path string) bool {
		return strings.Contains(path, part) && (len(methods) == 0 || slices.Contains(methods, method))
	}
}

// writeS3Error answers with an S3 error document, as the store would.
func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<Error><Code>%s</Code><Message>injected by the e2e S3 proxy</Message></Error>`, code)
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
		f := p.faultLocked(r.Method, r.URL.Path)
		if f != nil {
			p.reqs[i].Fault = f.Name
			if f.Status != 0 {
				p.reqs[i].Status = f.Status
			} else {
				p.reqs[i].Stalled = true
			}
		}
		p.mu.Unlock()
		if f != nil && f.Status != 0 {
			// Drain the body, as a store answering an error would read it,
			// so the client sees the answer rather than a reset.
			_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 64<<20))
			writeS3Error(w, f.Status, f.Code)
			return
		}
		if f != nil {
			select {
			case <-f.release:
			case <-r.Context().Done():
				return
			}
		}
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
