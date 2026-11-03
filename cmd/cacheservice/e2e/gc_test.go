package e2e

import (
	"context"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Retention and garbage collection (#11), on its own service and namespace
// so the other scenarios keep their data.
const (
	gcNS = "gc"
	// gcMaxAge is the namespace's max age while GA's first build expires,
	// and gcGrace the service's grace period: short, so the scenario
	// waits about gcMaxAge+gcGrace for both.
	gcMaxAge = 10 * time.Second
	gcGrace  = 5 * time.Second
)

// gc: engine GA builds, and the service stores it in namespace gc, whose
// max age is 10s. Once past it and the grace period, GC expires GA's
// entries and deletes their bundles, resolutions and blobs from MinIO.
// Engine GB, started after, gets nothing merged. The max age is then
// raised (the namespaces file, reloaded as on SIGHUP), and GA builds on top
// of its first build: the export uploads the deleted layers again, GB gets
// it merged with the root's expiry capped at the new max age, and hits it.
// S3 stays consistent throughout.
func (s *state) gc(t *testing.T) {
	env := s.env
	start := time.Now()
	tokA, tokB := "e2e-gc-a-"+Nonce(), "e2e-gc-b-"+Nonce()
	tokens := filepath.Join(env.RunDir, "gc-tokens")
	if err := os.WriteFile(tokens, []byte(tokA+" "+gcNS+"\n"+tokB+" "+gcNS+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	limits := filepath.Join(env.RunDir, "gc-namespaces")
	writeLimits := func(line string) {
		t.Helper()
		if err := os.WriteFile(limits, []byte(line+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeLimits(gcNS + " max-age=" + gcMaxAge.String())
	svc := env.StartService("gc", ServiceOptions{TokensFile: tokens, NamespacesFile: limits, GCGrace: gcGrace})
	ga := env.StartEnginesWithTokens(svc, map[string]string{"ga": tokA}, "ga")[0]

	nonce := Nonce()
	base := `withExec(args: ["sh", "-c", "echo gc-` + nonce + ` >/dev/null; head -c 1048576 /dev/urandom > /gc.bin && echo \"gc $(sha256sum /gc.bin | cut -c1-16)\" > /stamp"])`
	first := `{ container { from(address: "alpine:3.22") { ` + base + ` { file(path: "/stamp") { contents } } } } }`
	res, took := ga.Query("gc-run-a", first)
	stamp := strings.TrimSpace(Field(res, "container.from.withExec.file.contents"))
	t.Logf("GA took %s. Stamp: %s", took.Round(10*time.Millisecond), stamp)
	s.waitStored(svc, ga, "GA's entries to be stored in namespace gc")
	idx := env.IndexOf(gcNS)
	bundles := bundlesOf(idx)
	layers := env.ChainLayersIn(gcNS, bundles)
	var recorded int64
	for _, r := range idx.Roots {
		recorded = max(recorded, r.RecordedAtUnix)
	}
	Check(t, len(idx.Roots) > 0 && len(layers) >= 2 && recorded > 0, "GA's build stored: %d roots, %d layers, recorded at %d", len(idx.Roots), len(layers), recorded)
	env.CheckConsistentIn(t, gcNS, "after GA's build")

	// Past the max age and the grace period.
	until := time.Unix(recorded, 0).Add(gcMaxAge + gcGrace + 2*time.Second)
	time.Sleep(time.Until(until))
	// Two runs: the one that expires entries leaves their objects to the
	// next, for import syncs that read the index just before.
	var rep struct {
		expired int
		deleted map[string]int
	}
	for range 2 {
		reps, err := svc.CollectGarbage(context.Background(), false)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range reps {
			if r.Namespace == gcNS {
				rep.expired += r.Expired
				if len(r.Deleted) > 0 {
					rep.deleted = r.Deleted
				}
			}
		}
	}
	idx = env.IndexOf(gcNS)
	var left []string
	for _, d := range layers {
		if env.HasKey(env.BlobKeyIn(gcNS, d)) {
			left = append(left, Short(d.String()))
		}
	}
	Check(t, rep.expired > 0 && len(idx.Entries) == 0 && len(idx.Roots) == 0, "GC expired GA's %d entries: the index has %d entries, %d roots", rep.expired, len(idx.Entries), len(idx.Roots))
	Check(t, len(left) == 0 && len(env.BundleIDsIn(gcNS)) == 0 && rep.deleted["bundle"] == len(bundles) && rep.deleted["resolution"] > 0,
		"GC deleted their %d bundles, %d resolutions and %d blobs from MinIO (blobs left: %v)", rep.deleted["bundle"], rep.deleted["resolution"], rep.deleted["blob"], left)
	env.CheckConsistentIn(t, gcNS, "after GC")
	// GC ran over every namespace of the run's prefix, with this service's
	// short grace period: the run's scope (A's chain, less the layer
	// renew deleted on purpose) and the namespaces scenario's are still
	// whole.
	var mainLeft int
	for _, d := range s.layersA {
		if env.HasKey(env.BlobKey(d)) || env.deleted[d] {
			mainLeft++
		}
	}
	Check(t, mainLeft == len(s.layersA), "GC of gc left the run's scope alone: %d of A's %d layers stored or deleted on purpose", mainLeft, len(s.layersA))
	env.CheckConsistent(t, "in the run's scope after GC")
	for _, ns := range []string{"team-a", "team-b"} {
		if len(env.IndexOf(ns).Entries) > 0 {
			env.CheckConsistentIn(t, ns, "in "+ns+" after GC")
		}
	}

	// GB, started now, gets nothing merged.
	gb := env.StartEnginesWithTokens(svc, map[string]string{"gb": tokB}, "gb")[0]
	imp, err := svc.Import(context.Background(), gb.Conn)
	if err != nil {
		t.Fatal(err)
	}
	Check(t, len(imp.Merged) == 0 && len(s.imports(svc, gb).Merged) == 0, "nothing expired merged into GB")

	// A longer max age, then a build on top of the first: its layers,
	// deleted, are uploaded again; GB gets it merged and hits it.
	writeLimits(gcNS + " max-age=10m")
	if err := svc.ReloadLimits(); err != nil {
		t.Fatal(err)
	}
	s3Mark := env.Proxy.Mark()
	second := `{ container { from(address: "alpine:3.22") { ` + base + ` { withExec(args: ["sh", "-c", "echo again-` + Nonce() + ` > /again"]) { file(path: "/stamp") { contents } } } } } }`
	const secondPath = "container.from.withExec.withExec.file.contents"
	res, took = ga.Query("gc-run-a2", second)
	t.Logf("GA's second build took %s", took.Round(10*time.Millisecond))
	s.waitStored(svc, ga, "GA's second build to be stored")
	puts := env.Proxy.Blobs(s3Mark, http.MethodPut)
	var reuploaded int
	for _, d := range layers {
		if blobsOf(puts, d, func(r S3Request) bool { return r.Status == http.StatusOK }) {
			reuploaded++
		}
	}
	Check(t, reuploaded == len(layers), "the second export uploaded again the %d layers GC deleted (%d)", len(layers), reuploaded)
	env.CheckConsistentIn(t, gcNS, "after the second build")

	idx = env.IndexOf(gcNS)
	roots := slices.Sorted(maps.Keys(idx.Roots))
	s.waitMergedAll(svc, gb, roots)
	capped := true
	for _, m := range s.imports(svc, gb).Merged {
		for r, exp := range m.Retained {
			if want := idx.Roots[r].RecordedAtUnix + 600; exp != want {
				capped = false
				t.Logf("root %d merged with expiry %d, want %d", r, exp, want)
			}
		}
	}
	Check(t, capped, "GB's merges sent each root's expiry capped at the max age (recorded + 10m)")
	logMark := svc.Logs.Mark()
	res, took = gb.Query("gc-run-b", second)
	stampB := strings.TrimSpace(Field(res, secondPath))
	var hit, executed int
	env.WaitFor(30*time.Second, "GB's withExec cache spans", func() (bool, error) {
		hit, executed = countOutcomes(svc.Logs.CacheSpans(logMark, gb.CacheID(), "Container.withExec"))
		return hit+executed >= 1, nil
	})
	Check(t, stampB == stamp && executed == 0 && hit >= 1, "GB hit GA's second build (%d hit, %d executed) in %s", hit, executed, took.Round(10*time.Millisecond))
	env.CheckConsistentIn(t, gcNS, "at the end")
	t.Logf("gc took %s", time.Since(start).Round(time.Second))
}
