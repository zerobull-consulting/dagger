package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// otlpMargin is the least factor by which every OTLP route's limit must
// exceed the largest request real engines sent in the run. Below it, an engine
// change grew its requests toward the limit: measure again before raising
// either (cmd/cacheservice's README, "OTLP limits").
const otlpMargin = 10

// otlpRoutes are the OTLP routes, and their limits.
var otlpRoutes = []struct {
	name  string
	limit int64
}{
	{metrics.OTLPRouteTraces, server.MaxOTLPTracesBytes},
	{metrics.OTLPRouteLogs, server.MaxOTLPLogsBytes},
	{metrics.OTLPRouteMetrics, server.MaxOTLPMetricsBytes},
}

// otlpSizes is what the services logged of the OTLP requests they took and
// refused.
type otlpSizes struct {
	// largest is, per route, how many requests it took, and the largest as
	// received and decoded.
	largest map[string]*routeSizes
	// budgetPeak is the most of the decode budget in use at once.
	budgetPeak int64
	// refused are the requests refused, for their size, their speed or the
	// budget ("otlp request refused"), or as undecodable.
	refused []LogRecord
	// malformed are the records of requests taken without a size, which
	// the checks can't count.
	malformed []LogRecord
}

// routeSizes are a route's requests taken: how many, and the largest as
// received, decoded, and as the decoded message's estimated heap.
type routeSizes struct{ n, received, decoded, message, headers int64 }

// otlpSizesOf reads the OTLP records of the logs.
func otlpSizesOf(logs ...*LogRecorder) otlpSizes {
	out := otlpSizes{largest: map[string]*routeSizes{}}
	for _, l := range logs {
		for _, rec := range l.Since(0, "otlp request accepted", nil) {
			received, err1 := attrInt(rec, "receivedBytes")
			decoded, err2 := attrInt(rec, "decodedBytes")
			peak, err3 := attrInt(rec, "budgetPeak")
			message, err4 := attrInt(rec, "messageBytes")
			headers, err5 := attrInt(rec, "headerBytes")
			if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
				out.malformed = append(out.malformed, rec)
				continue
			}
			route := rec.Attr("route")
			sz := out.largest[route]
			if sz == nil {
				sz = &routeSizes{}
				out.largest[route] = sz
			}
			sz.n++
			sz.received = max(sz.received, received)
			sz.decoded = max(sz.decoded, decoded)
			sz.message = max(sz.message, message)
			sz.headers = max(sz.headers, headers)
			out.budgetPeak = max(out.budgetPeak, peak)
		}
		out.refused = append(out.refused, l.Since(0, "otlp request refused", nil)...)
		out.refused = append(out.refused, l.Since(0, "undecodable otlp request", nil)...)
	}
	return out
}

// otlpSizes runs last, over what every service of the run logged: the largest
// OTLP request taken on each route, as received and decoded, and the most of
// the decode budget in use at once (its peak, as each service last logged
// it). It fails if any real-engine request was refused, for any reason, or
// came within otlpMargin of its route's limit, or of the budget.
func (s *state) otlpSizes(t *testing.T) {
	var logs []*LogRecorder
	for _, svc := range s.env.Services() {
		logs = append(logs, svc.Logs)
	}
	sizes := otlpSizesOf(logs...)
	for _, rec := range sizes.refused {
		t.Logf("refused: %s %v", rec.Msg, rec.Attrs)
	}
	Check(t, len(sizes.refused) == 0, "no real-engine OTLP request refused (%d refused)", len(sizes.refused))
	Check(t, len(sizes.malformed) == 0, "every request taken logged its sizes (%d without)", len(sizes.malformed))
	for _, route := range otlpRoutes {
		sz := sizes.largest[route.name]
		if sz == nil {
			// The engines may not send metrics in a run this short; traces
			// and logs carry what the export path needs.
			Check(t, route.name == metrics.OTLPRouteMetrics, "%s: requests taken", route.name)
			continue
		}
		largest := max(sz.received, sz.decoded)
		Check(t, largest*otlpMargin <= route.limit,
			"%s: %d requests, largest %d bytes received, %d decoded; limit %d (%.0fx the largest)",
			route.name, sz.n, sz.received, sz.decoded, route.limit, float64(route.limit)/float64(max(largest, 1)))
		// The headers' reserve (otlpHeaderReserve) is what the service
		// refuses over MaxOTLPHeaderReserve. It is never less than their
		// wire bytes, so the margin bounds those against MaxHeaderBytes too.
		Check(t, sz.headers*otlpMargin <= server.MaxOTLPHeaderReserve,
			"%s: largest request headers reserved at %d bytes; MaxOTLPHeaderReserve %d", route.name, sz.headers, int64(server.MaxOTLPHeaderReserve))
		Check(t, sz.message*otlpMargin <= server.OTLPDecodeBudget,
			"%s: largest decoded message estimated at %d bytes; budget %d", route.name, sz.message, int64(server.OTLPDecodeBudget))
	}
	Check(t, sizes.budgetPeak*otlpMargin <= server.OTLPDecodeBudget,
		"decode budget: at most %d bytes in use at once; budget %d", sizes.budgetPeak, int64(server.OTLPDecodeBudget))
	engineIDs(t, logs)
}

// engineIDs checks the engine cache IDs and sessions real engines sent
// against the service's bound on them (zerobull-consulting/dagger#88): none
// was refused, and the longest of each seen is logged, to measure the bound
// against.
func engineIDs(t *testing.T, logs []*LogRecorder) {
	var refused []LogRecord
	caches, sessions := map[string]bool{}, map[string]bool{}
	longestCache, longestSession := "", ""
	for _, l := range logs {
		refused = append(refused, l.Since(0, server.LogEngineIDsRefused, nil)...)
		for _, rec := range l.Since(0, "cache span", nil) {
			cache := rec.Attr("cache")
			if i := strings.LastIndexByte(cache, '/'); i > 0 {
				cache = cache[:i]
			}
			caches[cache] = true
			if len(cache) > len(longestCache) {
				longestCache = cache
			}
			if session := rec.Attr("session"); session != "" {
				sessions[session] = true
				if len(session) > len(longestSession) {
					longestSession = session
				}
			}
		}
	}
	for _, rec := range refused {
		t.Logf("refused: %s %v", rec.Msg, rec.Attrs)
	}
	Check(t, len(refused) == 0, "no real-engine span, event, wiped cache or hello refused for its cache attribute or session (%d refused)", len(refused))
	Check(t, len(caches) > 0 && len(sessions) > 0, "cache spans seen: %d caches, %d sessions named", len(caches), len(sessions))
	t.Logf("engine IDs: %d cache IDs, longest %d characters (%q); %d sessions, longest %d characters (%q); bound %d",
		len(caches), len(longestCache), longestCache, len(sessions), len(longestSession), longestSession, server.MaxEngineIDLen)
}

// attrInt is a record's integer attribute; a missing or malformed one is an
// error, never 0.
func attrInt(rec LogRecord, key string) (int64, error) {
	v, ok := rec.Attrs[key]
	if !ok {
		return 0, fmt.Errorf("%s: no %s", rec.Msg, key)
	}
	return strconv.ParseInt(v, 10, 64)
}
