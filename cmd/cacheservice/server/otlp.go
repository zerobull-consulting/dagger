package server

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/dagger/dagger/engine/telemetryattrs"
)

// otlpRoutes receives OTLP/HTTP at the paths the engine's Cloud exporters
// post to under DAGGER_CLOUD_URL. The engine's cache events arrive as log
// records, under the engine's own token; session spans carry the cache
// entries their calls touched, under the token of the session's client.
// With tokens, each POST must carry a known token as its HTTP Basic
// username, and feeds only its namespace's paths (see otlpAuth), which is
// checked before any of its body is read. Each body is then bounded (see
// decodeOTLP), and a request refused for it records nothing.
func (s *Service) otlpRoutes(mux *http.ServeMux) {
	// Before taking over a session's telemetry, the engine probes the Cloud
	// URL with HEAD /v1/traces, with no credentials; any answer will do.
	// It carries nothing, so it isn't authenticated.
	mux.HandleFunc("HEAD /v1/traces", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("otlp probe", "method", r.Method, "path", r.URL.Path, "userAgent", r.UserAgent())
	})
	mux.HandleFunc("POST /v1/logs", s.otlpDeadline(func(w http.ResponseWriter, r *http.Request) {
		ns, key, ok := s.otlpAuth(w, r)
		if !ok {
			return
		}
		var req collogspb.ExportLogsServiceRequest
		release, ok := s.decodeOTLP(w, r, s.otlp.logs, &req)
		if !ok {
			return
		}
		defer release()
		s.logRecords(ns, key, &req)
		writeOTLP(w, r, &collogspb.ExportLogsServiceResponse{})
	}))
	mux.HandleFunc("POST /v1/traces", s.otlpDeadline(func(w http.ResponseWriter, r *http.Request) {
		ns, _, ok := s.otlpAuth(w, r)
		if !ok {
			return
		}
		var req coltracepb.ExportTraceServiceRequest
		release, ok := s.decodeOTLP(w, r, s.otlp.traces, &req)
		if !ok {
			return
		}
		defer release()
		s.logSpans(&req)
		s.trackSpans(ns, &req)
		writeOTLP(w, r, &coltracepb.ExportTraceServiceResponse{})
	}))
	mux.HandleFunc("POST /v1/metrics", s.otlpDeadline(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := s.otlpAuth(w, r); !ok {
			return
		}
		var req colmetricspb.ExportMetricsServiceRequest
		release, ok := s.decodeOTLP(w, r, s.otlp.metrics, &req)
		if !ok {
			return
		}
		defer release()
		points := 0
		for _, rm := range req.ResourceMetrics {
			for _, sm := range rm.ScopeMetrics {
				points += len(sm.Metrics)
			}
		}
		s.logger.Debug("otlp metrics", "metrics", points)
		writeOTLP(w, r, &colmetricspb.ExportMetricsServiceResponse{})
	}))
}

// otlpDeadline runs an OTLP POST handler with the read timeout already set on
// its connection, before the token is checked: what is read of the request
// after its headers, by the handler or by net/http after it, ends by then.
// A request refused before its body is read (401) still has net/http read up
// to 256 KiB of it after the handler, and discard it, to reuse the
// connection; the deadline bounds how long a client can hold the handler
// that way. The server clears the deadline before reading the next request.
func (s *Service) otlpDeadline(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(s.otlp.readTimeout)); err != nil {
			s.logger.Debug("otlp request: no read deadline", "error", err)
		}
		h(w, r)
	}
}

func (s *Service) logRecords(ns *namespace, key tokenKey, req *collogspb.ExportLogsServiceRequest) {
	other := 0
	for _, rl := range req.ResourceLogs {
		instance := stringAttr(rl.GetResource().GetAttributes(), "service.instance.id")
		for _, sl := range rl.ScopeLogs {
			for _, rec := range sl.LogRecords {
				kind := stringAttr(rec.Attributes, telemetryattrs.EngineEventAttr)
				if kind == "" {
					other++
					continue
				}
				body := rec.GetBody().GetStringValue()
				cache := stringAttr(rec.Attributes, telemetryattrs.EngineCacheAttr)
				s.logger.Info("engine event", "namespace", ns.name, "kind", cutString(kind, 200), "cache", cutString(cache, 200), "instance", cutString(instance, 200),
					"scope", cutString(sl.GetScope().GetName(), 200), "bodyBytes", len(body), "body", cutString(body, 512))
				if s.dumpDir != "" {
					s.dump(fmt.Sprintf("%06d-event-%s.json", s.dumpSeq.Add(1), kind), []byte(body))
				}
				s.trackEvent(ns, key, kind, cache, body)
				s.noteEngineEvent(ns, key, kind, cache)
			}
		}
	}
	if other > 0 {
		s.logger.Debug("otlp logs", "records", other)
	}
}

func (s *Service) logSpans(req *coltracepb.ExportTraceServiceRequest) {
	spans, cacheSpans := 0, 0
	for _, rs := range req.ResourceSpans {
		cache := stringAttr(rs.GetResource().GetAttributes(), telemetryattrs.EngineCacheAttr)
		session := stringAttr(rs.GetResource().GetAttributes(), telemetryattrs.EngineSessionAttr)
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spans++
				result := stringAttr(span.Attributes, telemetryattrs.CacheResultIDAttr)
				if result == "" {
					continue
				}
				cacheSpans++
				s.logger.Debug("cache span", "cache", cutString(cache, 200), "session", cutString(session, 200), "name", cutString(span.Name, 120),
					"result", cutString(result, 200),
					"outcome", cutString(stringAttr(span.Attributes, telemetryattrs.CacheOutcomeAttr), 200),
					"type", cutString(stringAttr(span.Attributes, telemetryattrs.CacheTypeAttr), 200),
					"retained", cutString(stringAttr(span.Attributes, telemetryattrs.CacheRetainedAttr), 200),
					"deps", arrayLen(span.Attributes, telemetryattrs.CacheDepsAttr),
					"parts", arrayLen(span.Attributes, telemetryattrs.CachePartsAttr))
			}
		}
	}
	s.logger.Info("otlp traces", "spans", spans, "cacheSpans", cacheSpans)
}

// decodeOTLP reads an OTLP/HTTP request body, protobuf or JSON, gzipped or
// not, within route's limit and the service's decode budget, and in at most
// the OTLP read timeout. The body as received and the decoded stream are
// limited separately, so a gzip bomb stops at the limit however well it
// compresses. A body over either limit is answered 413, one that doesn't fit
// in the budget 503 with Retry-After, and one too slow 408; with ok false,
// nothing of the request may be recorded. With ok true, release gives its
// bytes back to the budget: call it once the request is recorded.
func (s *Service) decodeOTLP(w http.ResponseWriter, r *http.Request, route otlpRoute, msg proto.Message) (release func(), ok bool) {
	s.logger.Debug("otlp request", "path", r.URL.Path, "userAgent", r.UserAgent(),
		"contentType", r.Header.Get("Content-Type"), "contentEncoding", r.Header.Get("Content-Encoding"),
		"contentLength", r.ContentLength)
	lim := s.otlp
	// The read deadline is the route's (otlpDeadline). Everything the
	// request holds is reserved before it is allocated, and given back by
	// release: the request itself (its goroutine and the server's buffers),
	// a gzipped body's decompressor, the body's buffer, and the decoded
	// message.
	var held int64
	release = func() { lim.budget.release(held) }
	refuse := func(err error) (func(), bool) {
		release()
		status, reason := otlpRefusal(err)
		s.refuseOTLP(w, r, route, status, reason, err)
		return nil, false
	}
	if !lim.budget.reserve(otlpRequestReserve) {
		return refuse(errOTLPBusy)
	}
	held = otlpRequestReserve
	received := &countingReader{r: http.MaxBytesReader(w, r.Body, route.limit)}
	var body io.Reader = received
	if r.Header.Get("Content-Encoding") == "gzip" {
		if !lim.budget.reserve(otlpGzipReserve) {
			return refuse(errOTLPBusy)
		}
		held += otlpGzipReserve
		gz, err := lim.gunzip(received)
		if err != nil {
			return refuse(err)
		}
		defer gz.Close()
		body = gz
	}
	data, reserved, err := readOTLPBody(body, route.limit, lim.budget)
	held += reserved
	if err != nil {
		return refuse(err)
	}
	// The message decoded from the body can take far more than the body
	// (otlpDecodedSize): it is estimated, and reserved, before it is
	// decoded. One that could never fit in the budget is too large.
	estimate, err := otlpDecodedSize(data, msg, isJSON(r))
	if err != nil {
		return refuse(err)
	}
	switch {
	case estimate+held > lim.budget.size:
		return refuse(errOTLPDecodesTooLarge)
	case !lim.budget.reserve(estimate):
		return refuse(errOTLPBusy)
	}
	held += estimate
	if isJSON(r) {
		err = protojson.Unmarshal(data, msg)
	} else {
		err = proto.Unmarshal(data, msg)
	}
	if err != nil {
		return refuse(err)
	}
	s.metrics.OTLPAccepted(route.name, received.n, int64(len(data)))
	s.logger.Debug("otlp request accepted", "route", route.name, "receivedBytes", received.n,
		"decodedBytes", len(data), "messageBytes", estimate, "budgetPeak", lim.budget.peakUse())
	return release, true
}

// gunzipReader opens a gzipped body.
func gunzipReader(r io.Reader) (io.ReadCloser, error) { return gzip.NewReader(r) }

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func writeOTLP(w http.ResponseWriter, r *http.Request, msg proto.Message) {
	var (
		data []byte
		err  error
	)
	if isJSON(r) {
		w.Header().Set("Content-Type", "application/json")
		data, err = protojson.Marshal(msg)
	} else {
		w.Header().Set("Content-Type", "application/x-protobuf")
		data, err = proto.Marshal(msg)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(data)
}

func isJSON(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Content-Type"), "application/json")
}

func stringAttr(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func arrayLen(kvs []*commonpb.KeyValue, key string) int {
	for _, kv := range kvs {
		if kv.Key == key {
			return len(kv.GetValue().GetArrayValue().GetValues())
		}
	}
	return 0
}
