package e2e

import (
	"bytes"
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

	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
)

// S3Request is one request through the S3 proxy.
type S3Request struct {
	// Time is when the proxy received the request (a bulk delete: once
	// its guard has read the body).
	Time   time.Time
	Method string
	// Path is the request path: /<bucket>/<key> (the services use
	// path-style addressing), and Query its query string.
	Path  string
	Query string
	// Status is the store's answer (0: not answered yet), the injected
	// fault's, or 403 when the proxy's guard refused the request (with no
	// Fault).
	Status int
	// Fault names the fault the proxy answered with instead of the store
	// ("" when the store answered), Stalled is whether a stall held it, and
	// Rewritten whether a fault rewrote its body before the store got it.
	Fault     string
	Stalled   bool
	Rewritten bool
}

// IsBlob reports whether the request is an engine's, for a layer blob: an
// upload, which goes to a staging key (ns/<ns>/staging/<stage>/<alg>/<hex>), or
// a download, of a digest key (ns/<ns>/blobs/<alg>/<hex>). The service's own
// requests for blobs (HEADs, and a promotion's copies and reads) are not.
func (r S3Request) IsBlob() bool {
	switch r.Method {
	case http.MethodPut:
		// The service never PUTs to a staging key: its copies go to
		// digest keys.
		return r.IsStaging()
	case http.MethodGet:
		return strings.Contains(r.Path, "/blobs/")
	}
	return false
}

// IsStaging reports whether the request is for a staged upload's object.
func (r S3Request) IsStaging() bool { return strings.Contains(r.Path, "/staging/") }

// IsBlobOf reports whether the request is an engine's for the blob of digest
// d (see IsBlob).
func (r S3Request) IsBlobOf(d digest.Digest) bool {
	return r.IsBlob() && strings.HasSuffix(r.Path, "/"+d.Algorithm().String()+"/"+d.Encoded())
}

// S3Proxy sits between everything and the S3 store, and records each
// request: the services are configured with its address as their S3
// endpoint, so the presigned URLs they hand engines point at it too. It
// keeps the Host header, which presigned URLs are signed for, or, for a store
// that routes on its own name, checks the signature and signs again
// (Upstream).
//
// It replaces tracing the store: which blobs an engine read or uploaded,
// when, and what the store answered.
type S3Proxy struct {
	// URL is the proxy's address, as engines in containers reach it.
	URL string

	mu     sync.Mutex
	reqs   []S3Request
	faults []*Fault
	guard  *s3test.Guard
}

// ProxyOptions configure a proxy beyond its plain forwarding.
type ProxyOptions struct {
	// Upstream, when set, signs requests again for the store; see Upstream.
	Upstream *Upstream
	// Guard, when set, answers 403 to requests it refuses, which don't reach
	// the store. Guard.Refused and Guard.Touched say what it saw.
	Guard *s3test.Guard
}

// Fault makes the proxy answer some requests itself instead of passing them
// to the store: with an S3 error (Status and Code), or by stalling them until
// Release; or pass them on with their body rewritten (Rewrite). It applies to
// requests that Match, up to Count of them (0: until removed), from when it
// is injected until it is removed.
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
	// Rewrite, when set, replaces the request's body before it goes on to
	// the store, which then answers it: as an engine that uploads other
	// content would. Status is ignored.
	Rewrite func(body []byte) []byte

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

// maxRewriteBytes bounds the body a Rewrite fault holds in memory.
const maxRewriteBytes = 256 << 20

// SameSizeCorruption rewrites a body to other content of the same size: its
// first byte flipped. A presigned PUT's signed Content-Length still matches,
// so S3 takes it.
func SameSizeCorruption(body []byte) []byte {
	out := bytes.Clone(body)
	if len(out) > 0 {
		out[0] ^= 0xff
	}
	return out
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
	return StartS3ProxyWith(t, host, target, ProxyOptions{})
}

// StartS3ProxyWith is StartS3Proxy with options.
func StartS3ProxyWith(t *testing.T, host, target string, opts ProxyOptions) *S3Proxy {
	t.Helper()
	to, err := url.Parse(target)
	if err != nil {
		t.Fatalf("S3 endpoint %q: %v", target, err)
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatalf("S3 proxy: %v", err)
	}
	p := &S3Proxy{URL: "http://" + ln.Addr().String(), guard: opts.Guard}
	rp := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			if opts.Upstream != nil {
				// Upstream.forward made the request the store's.
				r.Out.URL, r.Out.Host = r.In.URL, r.In.Host
				return
			}
			r.SetURL(to)
			// Presigned URLs are signed for the proxy's host: the store
			// checks the signature against the Host it receives.
			r.Out.Host = r.In.Host
		},
		// Stream bodies as they come: layer blobs are large.
		FlushInterval: -1,
	}
	if opts.Upstream != nil && opts.Upstream.Transport != nil {
		rp.Transport = opts.Upstream.Transport
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Recorded as it arrives (a bulk delete: once the guard has read
		// its body), with its status set as the response starts: before
		// the client can see the response, so a client that got one always
		// finds its request recorded.
		//
		// The guard goes first, outside p.mu: it reads a bulk delete's
		// keys from the body, off the network, and a slow body would hold
		// up every other request and the test's reads of the record.
		var refused error
		if p.guard != nil {
			refused = p.guard.CheckRecord(r)
		}
		p.mu.Lock()
		i := len(p.reqs)
		p.reqs = append(p.reqs, S3Request{Time: time.Now(), Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery})
		if refused != nil {
			p.reqs[i].Status = http.StatusForbidden
			p.mu.Unlock()
			writeS3Error(w, http.StatusForbidden, "AccessDenied")
			return
		}
		f := p.faultLocked(r.Method, r.URL.Path)
		if f != nil {
			p.reqs[i].Fault = f.Name
			if f.Rewrite != nil {
				p.reqs[i].Rewritten = true
			} else if f.Status != 0 {
				p.reqs[i].Status = f.Status
			} else {
				p.reqs[i].Stalled = true
			}
		}
		p.mu.Unlock()
		if f != nil && f.Rewrite != nil {
			body, err := io.ReadAll(io.LimitReader(r.Body, maxRewriteBytes+1))
			if err != nil || len(body) > maxRewriteBytes {
				writeS3Error(w, http.StatusBadRequest, "IncompleteBody")
				return
			}
			body = f.Rewrite(body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
			f = nil
		}
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
		if opts.Upstream != nil {
			// After the faults: a Rewrite changes the body, which the
			// signature doesn't cover, but its length must be final.
			if err := opts.Upstream.forward(r, to); err != nil {
				t.Logf("S3 proxy: %s %s: %v", r.Method, r.URL.Path, err)
				writeS3Error(sw, http.StatusForbidden, "SignatureDoesNotMatch")
				return
			}
		}
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
