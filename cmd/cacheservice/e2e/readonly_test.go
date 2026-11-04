package e2e

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// readonly (#65): a second service, read-only as -export=false makes it,
// merges what the main service stored of A into a fresh engine R, and R
// gets cache hits for A's pipeline. R then runs a pipeline of its own, whose
// entries a writer would export, and nothing new appears in the index or
// the bucket: the read-only service sent the store no write, and the
// listing is unchanged. S3 stays consistent.
func (s *state) readonly(t *testing.T) {
	env := s.env
	start := time.Now()
	// The main service's last writes of the first part (the imports of the
	// merges into A) may still be landing: take the listing once it holds
	// still for 3s. Nothing else writes meanwhile, since no engine of the
	// main service runs a session.
	before := env.Listing()
	stableSince := time.Now()
	env.WaitFor(env.Settings.Timeout, "the bucket to hold still", func() (bool, error) {
		now := env.Listing()
		if !slices.Equal(now, before) {
			before, stableSince = now, time.Now()
		}
		return time.Since(stableSince) >= 3*time.Second, nil
	})
	s3Mark := env.Proxy.Mark()

	ro := env.StartService("readonly", ServiceOptions{ReadOnly: true})
	r := env.StartEngines(ro, "r")[0]
	cacheR := r.CacheID()
	roots := make([]uint64, 0, len(s.indexAfterA.Roots))
	for n := range s.indexAfterA.Roots {
		roots = append(roots, n)
	}
	s.waitMergedAll(ro, r, roots)

	logMark := ro.Logs.Mark()
	res, timeR := r.Query("run-r", pipeline)
	stampR := strings.TrimSpace(Field(res, stampPath))
	var hitR, executedR int
	env.WaitFor(30*time.Second, "R's withExec cache spans", func() (bool, error) {
		hitR, executedR = countOutcomes(ro.Logs.CacheSpans(logMark, cacheR, "Container.withExec"))
		return hitR+executedR >= 2, nil
	})

	// A pipeline of R's own: a writer would export its exec.
	own := `{ container { from(address: "alpine:3.22") { withExec(args: ["sh", "-c", "echo readonly-` + Nonce() + ` > /ro"]) { file(path: "/ro") { contents } } } } }`
	_, timeOwn := r.Query("run-r-own", own)
	// Nothing being written can only be shown by waiting. A writer exports
	// a session once its end and spans are in, within the end grace (2s),
	// or after the quiet period (10s) without its end: 15s is past both.
	time.Sleep(exporter.DefaultQuietPeriod + exporter.DefaultEndGrace + 3*time.Second)

	after := env.Listing()
	var writes []string
	for _, req := range env.Proxy.Since(s3Mark) {
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			writes = append(writes, req.Method+" "+req.Path)
		}
	}
	_, entriesErr := ro.Entries(r.Conn)
	_, gcErr := ro.CollectGarbage(t.Context(), true)

	Check(t, stampR != "" && stampR == s.stampA && executedR == 0 && hitR == 2,
		"cache hit through the read-only service: R's stamps are A's, Container.withExec %d hit, %d executed", hitR, executedR)
	Check(t, len(writes) == 0, "no write reached the store: %d requests through the proxy since the read-only service started, writes %v", len(env.Proxy.Since(s3Mark)), writes)
	Check(t, slices.Equal(before, after), "the bucket is unchanged: %d objects before, %d after", len(before), len(after))
	Check(t, errors.Is(entriesErr, server.ErrReadOnly) && errors.Is(gcErr, server.ErrGCReadOnly),
		"the read-only service tracks nothing and runs no GC: %v; %v", entriesErr, gcErr)
	env.CheckConsistent(t, "after the read-only service")
	ro.Stop()
	t.Logf("timings: R %s (entries merged through the read-only service), R's own pipeline %s; the scenario took %s",
		timeR.Round(10*time.Millisecond), timeOwn.Round(10*time.Millisecond), time.Since(start).Round(time.Second))
}
