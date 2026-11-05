package e2e

import (
	"strconv"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// otlpMargin is the least factor by which every OTLP route's limit must
// exceed the largest request real engines sent in the run. Below it, an engine
// change grew its requests toward the limit: measure again before raising
// either (cmd/cacheservice's README, "OTLP limits").
const otlpMargin = 10

// otlpSizes runs last, over what every service of the run logged: the largest
// OTLP request taken on each route, as received and decoded, and the most of
// the decode budget in use at once. It fails if any real-engine request was
// refused, or came within otlpMargin of its route's limit, or of the budget.
func (s *state) otlpSizes(t *testing.T) {
	limits := map[string]int64{
		metrics.OTLPRouteTraces:  server.MaxOTLPTracesBytes,
		metrics.OTLPRouteLogs:    server.MaxOTLPLogsBytes,
		metrics.OTLPRouteMetrics: server.MaxOTLPMetricsBytes,
	}
	type sizes struct{ n, received, decoded int64 }
	largest := map[string]*sizes{}
	var budget int64
	refused := 0
	for _, svc := range s.env.Services() {
		for _, rec := range svc.Logs.Since(0, "otlp request accepted", nil) {
			route := rec.Attr("route")
			sz := largest[route]
			if sz == nil {
				sz = &sizes{}
				largest[route] = sz
			}
			sz.n++
			sz.received = max(sz.received, attrInt(rec, "receivedBytes"))
			sz.decoded = max(sz.decoded, attrInt(rec, "decodedBytes"))
			budget = max(budget, attrInt(rec, "budgetInUse"))
		}
		for _, rec := range svc.Logs.Since(0, "otlp request refused", nil) {
			refused++
			t.Logf("service %s refused an OTLP request: %v", svc.Name, rec.Attrs)
		}
	}
	Check(t, refused == 0, "no real-engine OTLP request refused (%d refused)", refused)
	for _, route := range []string{metrics.OTLPRouteTraces, metrics.OTLPRouteLogs, metrics.OTLPRouteMetrics} {
		sz := largest[route]
		if sz == nil {
			// The engines may not send metrics in a run this short; traces
			// and logs carry what the export path needs.
			Check(t, route == metrics.OTLPRouteMetrics, "%s: requests taken", route)
			continue
		}
		limit := limits[route]
		Check(t, max(sz.received, sz.decoded)*otlpMargin <= limit,
			"%s: %d requests, largest %d bytes received, %d decoded; limit %d (%.0fx the largest)",
			route, sz.n, sz.received, sz.decoded, limit, float64(limit)/float64(max(sz.received, sz.decoded, 1)))
	}
	Check(t, budget*otlpMargin <= server.OTLPDecodeBudget,
		"decode budget: at most %d bytes in use at once; budget %d", budget, int64(server.OTLPDecodeBudget))
}

func attrInt(rec LogRecord, key string) int64 {
	n, _ := strconv.ParseInt(rec.Attr(key), 10, 64)
	return n
}
