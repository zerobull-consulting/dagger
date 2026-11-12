package server

// Drain (#63) against MinIO: the fake engine uploads straight to the bucket
// through presigned PUTs. Skips unless CACHESERVICE_S3_ENDPOINT is set;
// `hack/remote-cache/dev.sh test` starts MinIO and runs it.

import (
	"log/slog"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
)

func newMinIODrainHarness(t *testing.T) *exportHarness {
	t.Helper()
	blobs, meta, _ := newMinIOStores(t)
	return newExportHarnessLogged(t, blobs, meta, slog.New(slog.DiscardHandler), nil, nil)
}

// A quiet session, and an upload stalled halfway through its body in the
// engine when the drain starts: both are stored in the bucket before Drain
// returns, with their blobs.
func TestMinIODrain(t *testing.T) {
	t.Run("quiet session", func(t *testing.T) {
		h := newMinIODrainHarness(t)
		digests := h.build()
		h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
		rep := drainFor(h.svc, 30*time.Second)
		if !indexHas(t, h, 10, 11) || rep.Outcome != metrics.DrainComplete || rep.StoredOf(testScope, h.cache.ID) != 2 {
			t.Fatalf("the quiet session wasn't stored by the drain: %+v", rep)
		}
		if !hasAll(t, h.blobs, digests) {
			t.Fatal("the drained export's blobs aren't all in the bucket")
		}
	})
	t.Run("upload stalled", func(t *testing.T) {
		h := newMinIODrainHarness(t)
		digests := h.build()
		release := h.cache.StallUploads()
		defer release()
		h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
		waitUntil(t, 10*time.Second, "an upload to MinIO to stall", func() bool { return h.cache.StalledUploads() > 0 })
		done := startDrain(t, h.svc, time.Minute)
		waitUntil(t, 5*time.Second, "the drain to see the export in flight", func() bool {
			st := h.svc.DrainStatus()
			return len(st.Left) == 1 && st.Left[0].InFlight == 2
		})
		drainRunning(t, done, "with the upload stalled")
		if indexHas(t, h, 10) {
			t.Fatal("published while the upload was stalled")
		}
		release()
		rep := waitDrain(t, done, 30*time.Second)
		if !indexHas(t, h, 10, 11) || rep.Outcome != metrics.DrainComplete || rep.LeftRoots() != 0 {
			t.Fatalf("the stalled upload's export wasn't stored by the drain: %+v", rep)
		}
		if !hasAll(t, h.blobs, digests) {
			t.Fatal("the drained export's blobs aren't all in the bucket")
		}
	})
	t.Run("deadline with the upload stalled", func(t *testing.T) {
		h := newMinIODrainHarness(t)
		h.build()
		release := h.cache.StallUploads()
		defer release()
		h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
		waitUntil(t, 10*time.Second, "an upload to MinIO to stall", func() bool { return h.cache.StalledUploads() > 0 })
		rep := drainFor(h.svc, 500*time.Millisecond)
		if rep.Outcome != metrics.DrainDeadline || rep.LeftRoots() != 2 {
			t.Fatalf("report %+v", rep)
		}
		h.svc.CloseEngines()
		h.cancel()
		h.svc.Wait()
		release()
		if n := indexEntries(t, h.meta); n != 0 {
			t.Fatalf("the index has %d entries of the export the deadline cut off", n)
		}
	})
}
