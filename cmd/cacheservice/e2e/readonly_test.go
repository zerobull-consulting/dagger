package e2e

import (
	"errors"
	"net/http"
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
// the bucket. S3 stays consistent.
//
// The read-only service has an S3 proxy of its own, which its engine's
// presigned GETs go through too, so what that proxy records is theirs
// alone: the main service and the replica, writers still running, use the
// run's. With the store's read-only user (dev.sh creates one on MinIO), the
// service runs with that key, as a pull-request job would.
func (s *state) readonly(t *testing.T) {
	env := s.env
	start := time.Now()
	proxy := env.NewProxy()
	opts := ServiceOptions{ReadOnly: true, Proxy: proxy}
	key := "the run's key"
	if id, secret, ok := env.ReadOnlyKey(); ok {
		opts.AccessKey, opts.SecretKey = id, secret
		key = "the read-only key " + id
	}
	ro := env.StartService("readonly", opts)
	t.Logf("the read-only service runs with %s", key)
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

	all := proxy.Since(0)
	var writes []string
	for _, req := range all {
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			writes = append(writes, req.Method+" "+req.Path)
		}
	}
	idx := env.Index()
	entriesR := 0
	for _, e := range idx.Entries {
		if e.Source.CacheID == cacheR {
			entriesR++
		}
	}
	_, entriesErr := ro.Entries(r.Conn)
	_, gcErr := ro.CollectGarbage(t.Context(), true)

	Check(t, stampR != "" && stampR == s.stampA && executedR == 0 && hitR == 2,
		"cache hit through the read-only service: R's stamps are A's, Container.withExec %d hit, %d executed", hitR, executedR)
	Check(t, len(all) > 0 && len(writes) == 0, "no write from the read-only service or its engine: %d requests through its proxy, writes %v", len(all), writes)
	Check(t, idx.Caches[cacheR] == nil && entriesR == 0, "nothing new in the index from R: no mapping of its cache (%v), %d entries from it", idx.Caches[cacheR] != nil, entriesR)
	Check(t, errors.Is(entriesErr, server.ErrReadOnly) && errors.Is(gcErr, server.ErrGCReadOnly),
		"the read-only service tracks nothing and runs no GC: %v; %v", entriesErr, gcErr)
	env.CheckConsistent(t, "after the read-only service")
	ro.Stop()
	t.Logf("timings: R %s (entries merged through the read-only service), R's own pipeline %s; the scenario took %s",
		timeR.Round(10*time.Millisecond), timeOwn.Round(10*time.Millisecond), time.Since(start).Round(time.Second))
}
