package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// flushTimeout bounds the flush scenario's flush: long enough for a loaded
// runner's export, well under the cap.
const flushTimeout = time.Minute

// flush (zerobull-consulting/dagger#136): the sidecar's new stop, on a real
// engine. F runs a build on a service of its own; as soon as the CLI
// returns, the orchestrator flushes F's cache with F's token, by the form
// that needs no cache ID (POST /v1/engines/flush, as the sidecar calls it),
// then stops F's container while the service is still up. The build's last
// retained entry, its last exec, must be in the index when the flush
// returns complete, before the stop, with nothing waited for after it.
func (s *state) flush(t *testing.T) {
	env := s.env
	start := time.Now()
	svc := env.StartService("flush", ServiceOptions{})
	// Not "f": the superseded scenario's engine F has that name.
	f := env.StartEngines(svc, "fl")[0]
	cacheF := f.CacheID()

	n := Nonce()
	query := `{ container { from(address: "alpine:3.22") { withExec(args: ["sh", "-c", "echo flush-` + n + ` > /f"]) { ` +
		`withExec(args: ["sh", "-c", "cat /f /f > /g"]) { file(path: "/g") { contents } } } } } }`
	mark := svc.Logs.Mark()
	_, took := f.Query("flush-f", query)

	// The flush, at once: the orchestrator doesn't wait for anything else.
	flushStart := time.Now()
	rep, code, body := postFlushE2E(t, svc.URL+"/v1/engines/flush?timeout="+flushTimeout.String(), f.Token)
	flushTook := time.Since(flushStart)
	idx := env.Index()

	// The build's last exec: the highest engine number among its executed
	// withExec spans, all in by now (the flush waited for the carrier's).
	var last uint64
	for _, sp := range svc.Logs.CacheSpans(mark, cacheF, "Container.withExec") {
		if sp.Attr("outcome") == "executed" && sp.Attr("retained") == "true" {
			last = max(last, uint64(atoi(sp.Attr("result"))))
		}
	}
	inIndex := indexMaps(idx, cacheF, last)
	entries, err := svc.Entries(f.Conn)
	unstored := 0
	for _, e := range entries {
		if e.Retained && !e.Stored {
			unstored++
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	_, stopErr := Docker(ctx, "stop", "--time", "30", f.Container)
	cancel()
	idxAfter := env.Index()
	afterStop := indexMaps(idxAfter, cacheF, last)

	Check(t, code == http.StatusOK && rep.Outcome == metrics.FlushComplete && rep.CacheID == cacheF && rep.Left == (server.FlushLeft{}),
		"the flush of F's cache by its token completed in %s, %d roots stored during it: %d %s", flushTook.Round(10*time.Millisecond), rep.Stored, code, body)
	Check(t, last != 0 && inIndex, "the build's last retained exec (entry %d) was in the index when the flush returned", last)
	Check(t, err == nil && unstored == 0, "every retained entry of F's was stored when the flush returned (%d not; %v)", unstored, err)
	Check(t, stopErr == nil && afterStop, "F stopped with the service up (%v), and entry %d is still in the index after", stopErr, last)
	env.CheckConsistent(t, "after the flush and F's stop")
	svc.Stop()
	t.Logf("timings: the build %s, the flush %s; the scenario took %s",
		took.Round(10*time.Millisecond), flushTook.Round(10*time.Millisecond), time.Since(start).Round(time.Second))
}

// indexMaps reports whether an index maps engine number n of cacheID.
func indexMaps(idx *metastore.Index, cacheID string, n uint64) bool {
	c := idx.Caches[cacheID]
	if c == nil {
		return false
	}
	_, ok := c.Numbers[n]
	return ok
}

// postFlushE2E posts a flush with token as the HTTP Basic username, bounded
// past the flush's own timeout.
func postFlushE2E(t *testing.T, url, token string) (server.FlushReport, int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout+30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(token, "")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var rep server.FlushReport
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, &rep); err != nil {
			t.Fatalf("flush report %s: %v", data, err)
		}
	}
	return rep, resp.StatusCode, string(data)
}
