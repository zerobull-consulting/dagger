package e2e

import (
	"slices"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// drainTimeout is the drain scenario's deadline: long, as a CI job's, so the
// drain ends because its work is done.
const drainTimeout = 2 * time.Minute

// drain (#63): the service is stopped cleanly (Drain, as SIGTERM does) while
// A's export is in flight, its upload stalled in the S3 proxy. A stays
// connected while the service drains, the upload finishes once S3 reads
// again, and the export is stored before the drain returns. The service
// restarted after it has A's entries without a session naming them again,
// and merges them into B, which gets the exec as a cache hit: the GAP that
// service-restart logs (#45) doesn't apply to a drained stop.
//
// A real engine sends its session's end as soon as the session closes, so
// the export is already running when the stop comes: there is no quiet
// period to cut short. The in-process tests cover that case
// (TestDrainExportsQuietSession, and the binary's
// TestMinIOBinaryDrainsOnSIGTERM).
func (s *state) drain(t *testing.T) {
	env, a, b := s.env, s.a, s.b
	cacheA := a.CacheID()
	stall, query, want, stalled, logMark := s.midUpload(t, "drain-run-a")
	idxBefore, bundlesBefore := env.Index(), env.BundleIDs()

	t.Logf("draining the service with A's upload of %v in flight", shorts(digestStrings(stalled)))
	start := time.Now()
	done := make(chan server.DrainReport, 1)
	go func() { done <- s.main.DrainFor(drainTimeout) }()
	env.WaitFor(30*time.Second, "the drain to see A's export in flight", func() (bool, error) {
		for _, l := range s.main.DrainStatus().Left {
			if l.CacheID == cacheA && l.InFlight > 0 {
				return true, nil
			}
		}
		return false, nil
	})
	// While the upload is stalled: nothing of the export is published, and
	// A is still connected.
	idxMid, bundlesMid := env.Index(), env.BundleIDs()
	_, connected := s.main.EngineInfo(a.Conn)
	env.CheckConsistent(t, "while draining")
	env.Proxy.Remove(stall)
	var rep server.DrainReport
	select {
	case rep = <-done:
	case <-time.After(drainTimeout + 30*time.Second):
		t.Fatal("the drain didn't return")
	}
	took := time.Since(start)
	stored := storedSince(s.main, logMark, cacheA)
	var holding []string
	for _, d := range stalled {
		holding = append(holding, s.bundlesHolding(stored, d)...)
	}
	holding = slices.Compact(sortedCopy(holding))

	Check(t, connected && idxMid.NextNumber == idxBefore.NextNumber && slices.Equal(bundlesMid, bundlesBefore),
		"while the drain waited on the stalled upload, A stayed connected and nothing of the export was published")
	Check(t, rep.Outcome == metrics.DrainComplete && rep.StoredOf(env.Scope, cacheA) > 0 && rep.LeftRoots() == 0,
		"the drain completed in %s: %d of A's roots stored, %d left (deadline %s)", took.Round(10*time.Millisecond), rep.StoredOf(env.Scope, cacheA), rep.LeftRoots(), drainTimeout)
	Check(t, len(stored) > 0 && len(holding) > 0,
		"A's export was stored before the drain returned, with the layer that was in flight (%v in bundles %v)", shorts(digestStrings(stalled)), shorts(holding))
	env.CheckConsistent(t, "after the drain")

	const down = 2 * time.Second
	s.main = env.RestartService(s.main, "main-drained", down)
	a.Attach(s.main)
	b.Attach(s.main)
	// No session names A's entries on the new service: what B is merged
	// comes from the drained export alone.
	s.checkServing(t, s.main, b, bundleIDs(stored), query, want, "drain-run-b")
	t.Logf("ok    the export in flight at the drained stop was stored before it: the GAP of an undrained stop (#45) doesn't apply")
	env.CheckConsistent(t, "after the drained restart")
}
