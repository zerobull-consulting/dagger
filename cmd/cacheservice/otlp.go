package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"

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
// records; session spans carry the cache entries their calls touched.
func (s *service) otlpRoutes(mux *http.ServeMux) {
	// Before taking over a session's telemetry, the engine probes the Cloud
	// URL with HEAD /v1/traces; any answer will do.
	mux.HandleFunc("HEAD /v1/traces", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("otlp probe", "method", r.Method, "path", r.URL.Path, "userAgent", r.UserAgent())
	})
	mux.HandleFunc("POST /v1/logs", func(w http.ResponseWriter, r *http.Request) {
		var req collogspb.ExportLogsServiceRequest
		if !s.decodeOTLP(w, r, &req) {
			return
		}
		s.logRecords(&req)
		writeOTLP(w, r, &collogspb.ExportLogsServiceResponse{})
	})
	mux.HandleFunc("POST /v1/traces", func(w http.ResponseWriter, r *http.Request) {
		var req coltracepb.ExportTraceServiceRequest
		if !s.decodeOTLP(w, r, &req) {
			return
		}
		s.logSpans(&req)
		s.trackSpans(&req)
		writeOTLP(w, r, &coltracepb.ExportTraceServiceResponse{})
	})
	mux.HandleFunc("POST /v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		var req colmetricspb.ExportMetricsServiceRequest
		if !s.decodeOTLP(w, r, &req) {
			return
		}
		points := 0
		for _, rm := range req.ResourceMetrics {
			for _, sm := range rm.ScopeMetrics {
				points += len(sm.Metrics)
			}
		}
		s.logger.Debug("otlp metrics", "metrics", points)
		writeOTLP(w, r, &colmetricspb.ExportMetricsServiceResponse{})
	})
}

func (s *service) logRecords(req *collogspb.ExportLogsServiceRequest) {
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
				s.logger.Info("engine event", "kind", kind, "cache", cache, "instance", instance,
					"scope", sl.GetScope().GetName(), "bodyBytes", len(body), "body", truncate(body, 512))
				if s.dumpDir != "" {
					s.dump(fmt.Sprintf("%06d-event-%s.json", s.dumpSeq.Add(1), kind), []byte(body))
				}
				s.trackEvent(kind, cache, body)
			}
		}
	}
	if other > 0 {
		s.logger.Debug("otlp logs", "records", other)
	}
}

func (s *service) logSpans(req *coltracepb.ExportTraceServiceRequest) {
	spans, cacheSpans := 0, 0
	for _, rs := range req.ResourceSpans {
		cache := stringAttr(rs.GetResource().GetAttributes(), telemetryattrs.EngineCacheAttr)
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spans++
				result := stringAttr(span.Attributes, telemetryattrs.CacheResultIDAttr)
				if result == "" {
					continue
				}
				cacheSpans++
				s.logger.Debug("cache span", "cache", cache, "name", truncate(span.Name, 120),
					"result", result,
					"outcome", stringAttr(span.Attributes, telemetryattrs.CacheOutcomeAttr),
					"type", stringAttr(span.Attributes, telemetryattrs.CacheTypeAttr),
					"retained", stringAttr(span.Attributes, telemetryattrs.CacheRetainedAttr),
					"deps", arrayLen(span.Attributes, telemetryattrs.CacheDepsAttr),
					"parts", arrayLen(span.Attributes, telemetryattrs.CachePartsAttr))
			}
		}
	}
	s.logger.Info("otlp traces", "spans", spans, "cacheSpans", cacheSpans)
}

// decodeOTLP reads an OTLP/HTTP request body, protobuf or JSON, gzipped or
// not.
func (s *service) decodeOTLP(w http.ResponseWriter, r *http.Request, msg proto.Message) bool {
	s.logger.Debug("otlp request", "path", r.URL.Path, "userAgent", r.UserAgent(),
		"contentType", r.Header.Get("Content-Type"), "contentEncoding", r.Header.Get("Content-Encoding"),
		"contentLength", r.ContentLength)
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return false
		}
		defer gz.Close()
		body = gz
	}
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	if isJSON(r) {
		err = protojson.Unmarshal(data, msg)
	} else {
		err = proto.Unmarshal(data, msg)
	}
	if err != nil {
		s.logger.Warn("undecodable otlp request", "path", r.URL.Path, "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
