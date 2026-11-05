package server

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
)

// What one OTLP request may carry, and what all of them may hold at once.
// cmd/cacheservice's README, "OTLP limits", says how they were chosen: from
// the largest requests real engines sent in the e2e run, and from the
// engine's own bound on what it sends.
const (
	// MaxOTLPTracesBytes bounds a POST /v1/traces body, both as received
	// and once gunzipped. Session spans come from the engine's Cloud
	// forwarder, which keeps each request to 4 MiB of protobuf unless a
	// single span is larger (engine/server/session_cloud_forwarder.go,
	// cloudForwardMaxPayloadSize): this is 16 times that.
	MaxOTLPTracesBytes = 64 << 20
	// MaxOTLPLogsBytes bounds a POST /v1/logs body, as received and
	// decoded. Session logs come through the same forwarder, with the same
	// 4 MiB bound; the engine's cache events come in batches of at most 128
	// records (engine/telemetry/logbatch.go), each one event.
	MaxOTLPLogsBytes = 64 << 20
	// MaxOTLPMetricsBytes bounds a POST /v1/metrics body, as received and
	// decoded. The service only counts the metrics, and the engine sends
	// few.
	MaxOTLPMetricsBytes = 16 << 20

	// OTLPDecodeBudget bounds the memory every OTLP request being read at
	// once holds, across the three routes: twice the largest limit, so an
	// at-limit request fits beside any number of the engines' usual ones,
	// which are tens of kilobytes. Each request reserves its buffer before
	// allocating it, as its body arrives (readOTLPBody): at most twice what
	// it has sent, three times while its buffer grows, and 1.5 times the
	// limit at the limit's last growth; and a gzipped one its decompressor
	// too (otlpGzipReserve), before it is built. So two at-limit requests at
	// once may be refused one of them, and retried. A request that would take it past the budget is
	// answered 503 with Retry-After, which OTLP exporters retry. The heap
	// the requests take is a small multiple of it: the buffers, and the
	// messages decoded from them.
	OTLPDecodeBudget = 2 * MaxOTLPTracesBytes

	// OTLPReadTimeout bounds reading one OTLP body, from when its handler
	// starts: a client sending slowly can't hold the budget, or its
	// connection, longer. The engine's Cloud exporters give up on a request
	// after 10s (engine/telemetry.CloudExportTimeout). It applies to the
	// OTLP routes alone, not to the engine WebSocket on the same listener.
	OTLPReadTimeout = 30 * time.Second

	// ReadHeaderTimeout bounds reading a request's headers on every
	// listener (NewHTTPServer). Only the headers: the engine WebSocket,
	// which outlives any request timeout, is unaffected once upgraded.
	ReadHeaderTimeout = 10 * time.Second

	// otlpRetryAfter is the Retry-After of a 503 for a full budget, in
	// seconds.
	otlpRetryAfter = 1
)

// NewHTTPServer is an http.Server for one of the service's handlers, with the
// timeouts every listener sets: ReadHeaderTimeout, and no ReadTimeout or
// WriteTimeout, which would cut off the engine WebSocket. The OTLP routes
// bound their bodies themselves (OTLPReadTimeout).
func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: ReadHeaderTimeout}
}

// otlpRoute is one OTLP POST route: its name, a constant used as its metric
// label, and its limit.
type otlpRoute struct {
	name  string
	limit int64
}

// otlpLimits are a service's OTLP limits: the constants, unless a test
// changed them.
type otlpLimits struct {
	traces, logs, metrics otlpRoute
	budget                *byteBudget
	readTimeout           time.Duration
	// gunzip opens a gzipped body; tests wrap it to count what is read
	// from the decompressed stream.
	gunzip func(io.Reader) (io.ReadCloser, error)
}

func defaultOTLPLimits() otlpLimits {
	return otlpLimits{
		traces:      otlpRoute{name: metrics.OTLPRouteTraces, limit: MaxOTLPTracesBytes},
		logs:        otlpRoute{name: metrics.OTLPRouteLogs, limit: MaxOTLPLogsBytes},
		metrics:     otlpRoute{name: metrics.OTLPRouteMetrics, limit: MaxOTLPMetricsBytes},
		budget:      newByteBudget(OTLPDecodeBudget),
		readTimeout: OTLPReadTimeout,
		gunzip:      gunzipReader,
	}
}

// byteBudget is a count of bytes that reservations may not take past its
// size.
type byteBudget struct {
	mu   sync.Mutex
	size int64
	used int64
	// peak is the most used at once, for the tests and the logs.
	peak int64
}

func newByteBudget(size int64) *byteBudget { return &byteBudget{size: size} }

// reserve takes n bytes, or nothing and false when they don't fit.
func (b *byteBudget) reserve(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+n > b.size {
		return false
	}
	b.used += n
	b.peak = max(b.peak, b.used)
	return true
}

func (b *byteBudget) release(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= n
}

// inUse returns the bytes reserved now.
func (b *byteBudget) inUse() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// peakUse returns the most bytes reserved at once so far.
func (b *byteBudget) peakUse() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peak
}

var (
	// errOTLPTooLarge is a body that decodes to more than its route's
	// limit.
	errOTLPTooLarge = errors.New("request body too large")
	// errOTLPBusy is a body that doesn't fit in what the decode budget has
	// left.
	errOTLPBusy = errors.New("too many OTLP bytes being read at once")
)

// otlpChunk bounds how much of a body is read at a time, and otlpMinBuffer
// is the first buffer a body is read into.
const (
	otlpChunk     = 32 << 10
	otlpMinBuffer = 512
)

// otlpGzipReserve is what a gzipped body's decompressor is reserved for,
// before it is built: compress/flate's 32 KiB window, its Huffman tables and
// the gzip reader's read buffer come to about 45 KiB. Without it, clients
// sending a valid gzip header and then stalling would each hold a
// decompressor the budget doesn't count, until the read timeout.
const otlpGzipReserve = 64 << 10

// readOTLPBody reads body, the decoded stream, up to limit bytes, into a
// buffer whose capacity it reserves from budget before allocating it: every
// byte read lands in reserved space, so the bodies being read at once never
// hold more than the budget, however many there are. The buffer doubles from
// 512 bytes up to limit, as the body arrives; while it grows, the old buffer
// and the new are both reserved, until the copy is done. So a client holds at
// most twice what it has sent, three times while its buffer grows, and an
// at-limit body the limit, 1.5 times as its buffer last grows. Once the buffer
// is full at the limit, one more byte, read alone, tells it the body is too
// large. It returns the bytes reserved, which the caller releases, whatever
// the error.
func readOTLPBody(body io.Reader, limit int64, budget *byteBudget) ([]byte, int64, error) {
	var (
		data     []byte
		reserved int64
	)
	for {
		if len(data) == cap(data) {
			if int64(len(data)) >= limit {
				var probe [1]byte
				n, err := io.ReadFull(body, probe[:])
				switch {
				case n > 0:
					return nil, reserved, errOTLPTooLarge
				case errors.Is(err, io.EOF):
					return data, reserved, nil
				}
				return nil, reserved, err
			}
			// The new buffer is reserved whole, since the old one is still
			// held while it is copied, and the old one released only after.
			grown := min(max(2*int64(cap(data)), otlpMinBuffer), limit)
			if !budget.reserve(grown) {
				return nil, reserved, errOTLPBusy
			}
			reserved += grown
			next := make([]byte, len(data), grown)
			copy(next, data)
			budget.release(int64(cap(data)))
			reserved -= int64(cap(data))
			data = next
		}
		want := min(cap(data)-len(data), otlpChunk)
		n, err := body.Read(data[len(data) : len(data)+want])
		data = data[:len(data)+n]
		if errors.Is(err, io.EOF) {
			return data, reserved, nil
		}
		if err != nil {
			return nil, reserved, err
		}
	}
}

// otlpRefusal is how a failed read of an OTLP body is answered: its status,
// and its reason as a metric label, or "" for a request that is malformed
// rather than refused.
func otlpRefusal(err error) (int, string) {
	var maxBytes *http.MaxBytesError
	var netErr net.Error
	switch {
	case errors.As(err, &maxBytes), errors.Is(err, errOTLPTooLarge):
		return http.StatusRequestEntityTooLarge, metrics.OTLPRefusedTooLarge
	case errors.Is(err, errOTLPBusy):
		return http.StatusServiceUnavailable, metrics.OTLPRefusedBusy
	case errors.As(err, &netErr) && netErr.Timeout():
		return http.StatusRequestTimeout, metrics.OTLPRefusedTimeout
	}
	return http.StatusBadRequest, ""
}

// refuseOTLP answers a request whose body wasn't read whole: it records
// nothing from it, and the connection closes after the answer, so the server
// doesn't read on what the client is still sending. Refusals are logged at
// warning level at most once a minute per route and reason, as refused tokens
// are (rejectionLog), so a client sending them by the thousand can't flood
// the log; the others at debug level. The metrics count every one.
func (s *Service) refuseOTLP(w http.ResponseWriter, r *http.Request, route otlpRoute, status int, reason string, err error) {
	w.Header().Set("Connection", "close")
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", strconv.Itoa(otlpRetryAfter))
	}
	key := reason
	if key == "" {
		key = "undecodable"
	}
	level := slog.LevelDebug
	if s.otlpRefusals.allow(route.name+" "+key, time.Now()) {
		level = slog.LevelWarn
	}
	if reason != "" {
		s.metrics.OTLPRefused(route.name, reason)
		s.logger.Log(r.Context(), level, "otlp request refused", "route", route.name, "reason", reason, "status", status,
			"limit", route.limit, "remote", r.RemoteAddr, "contentLength", r.ContentLength, "error", err)
	} else {
		s.logger.Log(r.Context(), level, "undecodable otlp request", "path", r.URL.Path, "error", err)
	}
	http.Error(w, err.Error(), status)
	_ = r.Body.Close()
}
