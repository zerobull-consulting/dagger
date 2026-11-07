package exporter

// Streaming exports (zerobull-consulting/dagger#134) and one export in
// flight per cache (zerobull-consulting/dagger#135), through the paths the
// service runs: Due, DrainDue, Done, Interrupted and Requeue.

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// at is t0 plus d.
func at(d time.Duration) time.Time { return t0.Add(d) }

// dueRoots runs Due at now and settles every batch as stored, returning
// the roots it exported.
func dueRoots(tr *Tracker, now time.Time) []uint64 {
	var out []uint64
	for _, b := range tr.Due(now) {
		out = append(out, b.Request.Roots...)
		tr.Done(b, Result{Stored: b.Request.Roots}, now)
	}
	slices.Sort(out)
	return out
}

// noSession is a call span of a retained entry from no session: nothing
// can speed it up.
func noSession(n uint64, end int64, parts ...string) Observation {
	o := state(n, end, parts...)
	o.Session = ""
	return o
}

// An entry named once is not due before Settle, and is due at Settle.
func TestTrackerSettle(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	if got := dueRoots(tr, at(testSettle-time.Millisecond)); len(got) != 0 {
		t.Fatalf("due before Settle: %v", got)
	}
	if got := dueRoots(tr, at(testSettle)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("at Settle: %v", got)
	}
}

// Each span that names the entry restarts its settle, whether it changes
// the value or not.
func TestTrackerSettleRestarts(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	// Unchanged, then a new part.
	tr.Observe(state(1, 11, fsPart), at(800*time.Millisecond))
	tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s1", Number: 1, Parts: []string{metaPart}}, at(1600*time.Millisecond))
	for _, d := range []time.Duration{testSettle, 1800 * time.Millisecond, 2599 * time.Millisecond} {
		if got := dueRoots(tr, at(d)); len(got) != 0 {
			t.Fatalf("due at %s, though a span named it within Settle: %v", d, got)
		}
	}
	if got := dueRoots(tr, at(2600*time.Millisecond)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("Settle after the last span: %v", got)
	}
}

// An entry named every 500ms isn't due until MaxDelay after it became dirty,
// and is due then. After that export it is dirty again only when its value
// changes, and MaxDelay counts from that change, not from when it was first
// seen.
func TestTrackerMaxDelay(t *testing.T) {
	tr := newTestTracker()
	step := 500 * time.Millisecond
	var exported []time.Duration
	parts := []string{fsPart}
	for d := time.Duration(0); d <= 30*time.Second; d += step {
		o := state(1, int64(d)+1, parts...)
		if d == 15*time.Second {
			// The value changes once, after the first export.
			parts = append(parts, metaPart)
			o.Parts = parts
		}
		tr.Observe(o, at(d))
		if got := dueRoots(tr, at(d)); len(got) > 0 {
			exported = append(exported, d)
		}
	}
	// First change at 0 (first seen), exported at MaxDelay; then not dirty
	// until the change at 15s, exported at 15s+MaxDelay.
	want := []time.Duration{testMaxDelay, 15*time.Second + testMaxDelay}
	if !slices.Equal(exported, want) {
		t.Fatalf("exported at %v, want %v", exported, want)
	}
}

// Spans naming a stored entry without changing its value don't make it due.
func TestTrackerUnchangedAfterStored(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	if got := dueRoots(tr, at(testSettle)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("first export: %v", got)
	}
	for d := 2 * time.Second; d < time.Minute; d += 300 * time.Millisecond {
		tr.Observe(state(1, int64(d), fsPart), at(d))
		if got := dueRoots(tr, at(d)); len(got) != 0 {
			t.Fatalf("exported %v at %s, unchanged", got, d)
		}
	}
	if es := tr.Entries("c"); es[0].Dirty || es[0].DueAt != "" {
		t.Fatalf("entry %+v, want clean and not due", es[0])
	}
}

// A carrier whose spans are all in makes its candidates due at once, within
// Settle; only the retained ones.
func TestTrackerAcceleratorSpansIn(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	notRetained := state(2, 11)
	notRetained.Retained = false
	tr.Observe(notRetained, t0)
	// A lazy-evaluation span adds a part.
	tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s1", Number: 1, Parts: []string{metaPart}}, t0)
	if b := tr.Due(t0); len(b) != 0 {
		t.Fatalf("due before the carrier: %v", roots(b))
	}
	tr.SessionEnded("c", 1, "s1", 3, at(100*time.Millisecond))
	batches := tr.Due(at(100 * time.Millisecond))
	if len(batches) != 1 || !slices.Equal(batches[0].Request.Roots, []uint64{1}) || len(batches[0].Request.Outputs) != 2 {
		t.Fatalf("batches %+v", batches)
	}
	// Dropped once sped up.
	if n := len(tr.caches["c"].sessions); n != 0 {
		t.Fatalf("%d sessions kept after the speed-up", n)
	}
}

// A carrier with spans missing makes nothing due early. The late spans
// arrive, and the entries are due on Settle, or at once if they complete
// the count.
func TestTrackerAcceleratorSpansMissing(t *testing.T) {
	t.Run("never arrive", func(t *testing.T) {
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		tr.SessionEnded("c", 1, "s1", 2, t0)
		if got := dueRoots(tr, at(testSettle-time.Millisecond)); len(got) != 0 {
			t.Fatalf("sped up with a span missing: %v", got)
		}
		if got := dueRoots(tr, at(testSettle)); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("not due on Settle: %v", got)
		}
	})
	t.Run("arrive late", func(t *testing.T) {
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		tr.SessionEnded("c", 1, "s1", 3, t0)
		tr.Observe(state(2, 11, fsPart), at(500*time.Millisecond))
		if got := dueRoots(tr, at(500*time.Millisecond)); len(got) != 0 {
			t.Fatalf("sped up with a span missing: %v", got)
		}
		// The last one completes the count: all due at once.
		tr.Observe(state(3, 12, fsPart), at(600*time.Millisecond))
		if got := dueRoots(tr, at(600*time.Millisecond)); !slices.Equal(got, []uint64{1, 2, 3}) {
			t.Fatalf("once all spans were in: %v", got)
		}
	})
}

// An entry with no carrier, or no session at all, is exported on Settle.
func TestTrackerNoCarrier(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Observe(noSession(2, 10, fsPart), t0)
	if got := dueRoots(tr, at(testSettle)); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("on Settle: %v", got)
	}
}

// A non-retained entry is forgotten after ForgetAfter, whatever Settle and
// MaxDelay are.
func TestTrackerForgetAfter(t *testing.T) {
	for _, cfg := range []TrackerConfig{
		{Settle: time.Millisecond, MaxDelay: 2 * time.Millisecond},
		{Settle: time.Hour, MaxDelay: 2 * time.Hour},
	} {
		cfg.ForgetAfter = 5 * time.Second
		tr := NewTracker(cfg)
		o := state(1, 10)
		o.Retained = false
		tr.Observe(o, t0)
		tr.Due(at(5*time.Second - time.Millisecond))
		if len(tr.Entries("c")) != 1 {
			t.Fatalf("%+v: forgot it before ForgetAfter", cfg)
		}
		tr.Due(at(5 * time.Second))
		if len(tr.Entries("c")) != 0 {
			t.Fatalf("%+v: kept it after ForgetAfter", cfg)
		}
	}
}

// Sessions are dropped once sped up, or ForgetAfter after their last span.
func TestTrackerSessionsForgotten(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Due(at(testForget - time.Millisecond))
	if n := len(tr.caches["c"].sessions); n != 1 {
		t.Fatalf("%d sessions before ForgetAfter", n)
	}
	tr.Due(at(testForget))
	if n := len(tr.caches["c"].sessions); n != 0 {
		t.Fatalf("%d sessions after ForgetAfter", n)
	}
	// A later span starts it again.
	tr.Observe(state(2, 20, fsPart), at(11*time.Second))
	if n := len(tr.caches["c"].sessions); n != 1 {
		t.Fatalf("%d sessions after a new span", n)
	}
}

// An entry first seen unretained, then retained, becomes dirty then, and is
// due on Settle from that span; MaxDelay counts from it.
func TestTrackerRetainedLater(t *testing.T) {
	tr := newTestTracker()
	o := state(1, 10, fsPart)
	o.Retained = false
	for d := time.Duration(0); d < 20*time.Second; d += 500 * time.Millisecond {
		tr.Observe(o, at(d))
		if got := dueRoots(tr, at(d)); len(got) != 0 {
			t.Fatalf("unretained entry exported at %s", d)
		}
	}
	tr.Observe(state(1, 30, fsPart), at(20*time.Second))
	if es := tr.Entries("c"); !es[0].Dirty {
		t.Fatalf("entry %+v, want dirty once retained", es[0])
	}
	if got := dueRoots(tr, at(20*time.Second+testSettle-time.Millisecond)); len(got) != 0 {
		t.Fatalf("due before Settle (MaxDelay counted from first seen?): %v", got)
	}
	if got := dueRoots(tr, at(20*time.Second+testSettle)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("on Settle: %v", got)
	}
}

// A dirty entry pruned or expired before it is due isn't exported, and is
// forgotten.
func TestTrackerDirtyPrunedOrExpired(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	expiring := state(2, 10, fsPart)
	expiring.Expires = at(500 * time.Millisecond).Unix()
	tr.Observe(expiring, t0)
	tr.Pruned("c", 1, []Drop{{Number: 1, DroppedAtUnixNano: 20}})
	if got := exportedWithin(tr, at(testSettle), time.Minute); len(got) != 0 {
		t.Fatalf("exported %v", got)
	}
	if es := tr.Entries("c"); len(es) != 0 {
		t.Fatalf("kept %+v", es)
	}
}

// An entry that changes in flight isn't put in a second batch; done requeues
// it, and the next batch's value covers its current parts.
func TestTrackerChangedInFlight(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.Due(at(testSettle))
	if got := roots(b); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("first export: %v", got)
	}
	tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s1", Number: 1, Parts: []string{metaPart}}, at(1100*time.Millisecond))
	tr.SessionEnded("c", 1, "s1", 2, at(1100*time.Millisecond))
	for d := 1100 * time.Millisecond; d < 5*time.Second; d += 100 * time.Millisecond {
		if got := roots(tr.Due(at(d))); len(got) != 0 {
			t.Fatalf("in a second batch at %s: %v", d, got)
		}
	}
	tr.Done(b[0], Result{Stored: []uint64{1}}, at(5*time.Second))
	if es := tr.Entries("c"); es[0].Stored || !es[0].Dirty {
		t.Fatalf("entry %+v, want dirty, not stored", es[0])
	}
	next := tr.Due(at(5 * time.Second))
	if got := roots(next); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("requeue: %v", got)
	}
	var got []string
	for _, o := range next[0].Request.Outputs {
		got = append(got, string(o.Address.Part))
	}
	if slices.Sort(got); !slices.Equal(got, []string{"execMeta", "fs"}) {
		t.Fatalf("the next export's parts: %v, want both", got)
	}
	tr.Done(next[0], Result{Stored: []uint64{1}}, at(5*time.Second))
	if es := tr.Entries("c"); !es[0].Stored || es[0].Dirty {
		t.Fatalf("entry %+v, want stored", es[0])
	}
}

// An imported entry that hasn't changed is never due. One that gained a part
// is due on Settle from that part's span, though it was first seen long ago.
func TestTrackerImportedStreaming(t *testing.T) {
	tr := newTestTracker()
	tr.Imported("c", 1, []ImportedValue{{Number: 1, Parts: []dagql.PersistedPartAddress{{Part: "fs"}}}})
	for d := time.Duration(0); d < 20*time.Second; d += 500 * time.Millisecond {
		tr.Observe(state(1, int64(d), fsPart), at(d))
		if got := dueRoots(tr, at(d)); len(got) != 0 {
			t.Fatalf("unchanged import exported at %s", d)
		}
	}
	tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s1", Number: 1, Parts: []string{metaPart}}, at(20*time.Second))
	if got := dueRoots(tr, at(20*time.Second+testSettle-time.Millisecond)); len(got) != 0 {
		t.Fatalf("due before Settle: %v", got)
	}
	if got := dueRoots(tr, at(20*time.Second+testSettle)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("a new part: %v", got)
	}
}

// The drain: DrainDue makes every dirty entry worth exporting due at once,
// and DrainStatus counts them. Due itself is unchanged by the drain.
func TestTrackerDrainDirty(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Observe(noSession(2, 10, fsPart), t0)
	stored := state(3, 10, fsPart)
	tr.Observe(stored, t0)
	b := tr.All("c", t0)
	tr.Done(b[0], Result{Stored: []uint64{1, 2, 3}}, t0)
	// 1 and 2 change; 3 doesn't.
	tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s1", Number: 1, Parts: []string{metaPart}}, at(time.Second))
	tr.Observe(Observation{CacheID: "c", Generation: 1, Number: 2, Parts: []string{metaPart}}, at(time.Second))
	tr.Observe(state(3, 11, fsPart), at(time.Second))
	now := at(1100 * time.Millisecond)
	tr.StartDrain(now)
	if got := roots(tr.Due(now)); len(got) != 0 {
		t.Fatalf("Due changed while draining: %v", got)
	}
	st := tr.DrainStatus(now, time.Time{})
	if len(st) != 1 || st[0].Pending != 2 || st[0].Roots() != 2 || st[0].Sessions != 1 {
		t.Fatalf("drain status %+v, want 2 dirty, 1 session", st)
	}
	if got := dueRoots(tr, now); len(got) != 0 {
		t.Fatalf("Due exported %v while draining", got)
	}
	got := tr.DrainDue(now)
	if !slices.Equal(roots(got), []uint64{1, 2}) {
		t.Fatalf("drain due: %v", roots(got))
	}
	// The session hadn't ended: no longer tracked.
	if tr.Observe(state(4, 12, fsPart), now) {
		t.Fatal("took a span of a session the drain already exported")
	}
	tr.Done(got[0], Result{Stored: []uint64{1, 2}}, now)
	if st := tr.DrainStatus(now, time.Time{}); len(st) != 0 {
		t.Fatalf("drain status after: %+v", st)
	}
}

// An ended session whose counted spans are missing: the drain exports what
// it has at once, and still takes its late spans, until Settle passes with
// none of them.
func TestTrackerDrainEndedSessionLateSpans(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.SessionEnded("c", 1, "s1", 2, t0)
	tr.StartDrain(t0)
	got := tr.DrainDue(t0)
	if !slices.Equal(roots(got), []uint64{1}) {
		t.Fatalf("drain due: %v", roots(got))
	}
	tr.Done(got[0], Result{Stored: []uint64{1}}, t0)
	if st := tr.DrainStatus(at(500*time.Millisecond), time.Time{}); len(st) != 1 || st[0].Sessions != 1 || st[0].Roots() != 0 {
		t.Fatalf("drain status %+v, want the session waited for", st)
	}
	if !tr.Observe(state(2, 11, fsPart), at(500*time.Millisecond)) {
		t.Fatal("a span of a tracked session was dropped while draining")
	}
	if got := roots(tr.DrainDue(at(500 * time.Millisecond))); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("its late span: %v", got)
	}

	// Settle bounds the wait.
	tr = newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.SessionEnded("c", 1, "s1", 5, t0)
	tr.StartDrain(t0)
	tr.DrainDue(at(testSettle))
	if st := tr.DrainStatus(at(testSettle), time.Time{}); len(st) != 1 || st[0].Sessions != 0 {
		t.Fatalf("drain status %+v, want no session waited for after Settle", st)
	}
	if tr.Observe(state(2, 11, fsPart), at(testSettle)) {
		t.Fatal("took a span of a session past Settle while draining")
	}
}

// One at a time: entries due while an export of their cache is in flight
// wait for it to end, then go in one batch with anything else due. Batches
// grow with load.
func TestTrackerOneExportPerCache(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(noSession(1, 10, fsPart), t0)
	first := tr.Due(at(testSettle))
	if got := roots(first); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("first: %v", got)
	}
	// N entries become due one by one while it runs.
	for i := uint64(2); i <= 6; i++ {
		d := time.Duration(i) * time.Second
		tr.Observe(noSession(i, int64(i), fsPart), at(d))
		if got := roots(tr.Due(at(d + testSettle))); len(got) != 0 {
			t.Fatalf("a second export of the cache started: %v", got)
		}
		if w := tr.WaitingForSlot(); w != int(i-1) {
			t.Fatalf("waiting %d, want %d", w, i-1)
		}
	}
	tr.Done(first[0], Result{Stored: []uint64{1}}, at(10*time.Second))
	next := tr.Due(at(10 * time.Second))
	if len(next) != 1 || !slices.Equal(next[0].Request.Roots, []uint64{2, 3, 4, 5, 6}) {
		t.Fatalf("next batch %v, want all five", roots(next))
	}
	if w := tr.WaitingForSlot(); w != 0 {
		t.Fatalf("waiting %d after the batch took them", w)
	}
}

// More than maxBatchRoots due: one batch at a time, never two at once.
func TestTrackerSplitBatchesRunInTurn(t *testing.T) {
	tr := newTestTracker()
	for n := uint64(1); n <= 2*maxBatchRoots+5; n++ {
		tr.Observe(noSession(n, 10, fsPart), t0)
	}
	now := at(testSettle)
	var sizes []int
	for range 4 {
		b := tr.Due(now)
		if len(b) > 1 {
			t.Fatalf("%d batches of one cache at once", len(b))
		}
		if len(b) == 0 {
			break
		}
		if again := tr.Due(now); len(again) != 0 {
			t.Fatal("a second batch while the first is in flight")
		}
		sizes = append(sizes, len(b[0].Request.Roots))
		tr.Done(b[0], Result{Stored: b[0].Request.Roots}, now)
	}
	if !slices.Equal(sizes, []int{maxBatchRoots, maxBatchRoots, 5}) {
		t.Fatalf("batch sizes %v", sizes)
	}
}

// The slot is freed by Done, Interrupted and Requeue: the cache isn't left
// stuck.
func TestTrackerSlotFreed(t *testing.T) {
	for name, free := range map[string]func(*Tracker, Batch, time.Time){
		"done": func(tr *Tracker, b Batch, now time.Time) {
			tr.Done(b, Result{Busy: []protocol.SkippedRoot{{Number: 1}}}, now)
		},
		"done failed": func(tr *Tracker, b Batch, now time.Time) { tr.Done(b, Result{}, now) },
		"interrupted": func(tr *Tracker, b Batch, now time.Time) { tr.Interrupted(b, Result{}, now, now) },
		"requeue":     func(tr *Tracker, b Batch, now time.Time) { tr.Requeue(b, now) },
		"wiped cache": func(tr *Tracker, b Batch, now time.Time) {
			// The cache is gone while its export runs: still freed.
			tr.Started("other", 1, "c")
			tr.Observe(noSession(1, 20, fsPart), now)
			tr.Done(b, Result{}, now)
		},
	} {
		t.Run(name, func(t *testing.T) {
			tr := newTestTracker()
			tr.Observe(noSession(1, 10, fsPart), t0)
			b := tr.Due(at(testSettle))
			if len(b) != 1 {
				t.Fatal("no batch")
			}
			free(tr, b[0], at(testSettle))
			if tr.slots["c"] {
				t.Fatal("the slot is still taken")
			}
			if got := exportedWithin(tr, at(testSettle), time.Hour); !slices.Equal(got, []uint64{1}) {
				t.Fatalf("exported %v after, want [1]", got)
			}
		})
	}
}

// Two caches each have a slot, and export at once.
func TestTrackerTwoCachesTwoSlots(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(noSession(1, 10, fsPart), t0)
	other := noSession(1, 10, fsPart)
	other.CacheID = "d"
	tr.Observe(other, t0)
	b := tr.Due(at(testSettle))
	if len(b) != 2 || b[0].CacheID != "c" || b[1].CacheID != "d" {
		t.Fatalf("batches %+v, want one per cache", b)
	}
}

// The explicit hook takes the slot: it waits for the export in flight, Due
// starts nothing for the cache while it waits or runs, and the cache's
// exports go on once it frees the slot.
func TestTrackerAcquireSlot(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(noSession(1, 10, fsPart), t0)
	b := tr.Due(at(testSettle))
	acquired := make(chan func(), 1)
	go func() {
		release, err := tr.AcquireSlot(context.Background(), "c")
		if err != nil {
			t.Error(err)
		}
		acquired <- release
	}()
	waitFor(t, func() bool { tr.mu.Lock(); defer tr.mu.Unlock(); return tr.slotWaiters["c"] == 1 })
	select {
	case <-acquired:
		t.Fatal("took the slot while an export holds it")
	case <-time.After(50 * time.Millisecond):
	}
	tr.Observe(noSession(2, 10, fsPart), at(time.Second))
	tr.Done(b[0], Result{Stored: []uint64{1}}, at(2*time.Second))
	release := <-acquired
	if got := roots(tr.Due(at(5 * time.Second))); len(got) != 0 {
		t.Fatalf("Due started %v while the hook holds the slot", got)
	}
	release()
	release() // twice is harmless
	if got := roots(tr.Due(at(5 * time.Second))); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("after the hook: %v", got)
	}

	// A waiter whose context ends leaves the queue.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := tr.AcquireSlot(ctx, "c"); err == nil {
		t.Fatal("took the slot while an export holds it")
	}
	if tr.slotWaiters["c"] != 0 {
		t.Fatal("a waiter that gave up is still queued")
	}
}

// NextDue tracks the earliest due time, and Notify is called when it comes
// sooner, a carrier speeds entries up, or a slot is freed; not on every span.
func TestTrackerNextDueAndNotify(t *testing.T) {
	var kicks atomic.Int32
	tr := NewTracker(TrackerConfig{Settle: testSettle, MaxDelay: testMaxDelay, Notify: func() { kicks.Add(1) }})
	if !tr.NextDue().IsZero() {
		t.Fatal("next due with nothing tracked")
	}
	tr.Observe(state(1, 10, fsPart), t0)
	if got := tr.NextDue(); !got.Equal(at(testSettle)) || kicks.Load() != 1 {
		t.Fatalf("next %s, %d kicks", got, kicks.Load())
	}
	// Later spans are due later: no kick.
	for i := uint64(2); i < 10; i++ {
		tr.Observe(state(i, 10, fsPart), at(time.Duration(i)*100*time.Millisecond))
	}
	if kicks.Load() != 1 {
		t.Fatalf("%d kicks, want 1", kicks.Load())
	}
	// Due recomputes it without a kick.
	tr.Due(at(500 * time.Millisecond))
	if got := tr.NextDue(); !got.Equal(at(testSettle)) || kicks.Load() != 1 {
		t.Fatalf("after Due: next %s, %d kicks", got, kicks.Load())
	}
	// A carrier with its spans in: due now.
	tr.SessionEnded("c", 1, "s1", 9, at(600*time.Millisecond))
	if got := tr.NextDue(); !got.Equal(at(600*time.Millisecond)) || kicks.Load() != 2 {
		t.Fatalf("after the carrier: next %s, %d kicks", got, kicks.Load())
	}
	b := tr.Due(at(600 * time.Millisecond))
	if !tr.NextDue().IsZero() {
		t.Fatalf("next %s with the only cache's slot taken", tr.NextDue())
	}
	tr.Done(b[0], Result{Stored: b[0].Request.Roots}, at(700*time.Millisecond))
	if kicks.Load() != 3 {
		t.Fatalf("%d kicks after the slot was freed, want 3", kicks.Load())
	}
}

// The debug fields: dirty, and when the entry is due.
func TestTrackerEntryDueAt(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	es := tr.EntriesAt("c", t0)
	if !es[0].Dirty || es[0].DueAt != "2027-01-15T08:00:01.000Z" {
		t.Fatalf("entry %+v", es[0])
	}
	b := tr.Due(at(testSettle))
	if es := tr.EntriesAt("c", at(testSettle)); es[0].Dirty || es[0].DueAt != "" || !es[0].InFlight {
		t.Fatalf("in flight: %+v", es[0])
	}
	tr.Done(b[0], Result{Stored: []uint64{1}}, at(testSettle))
	if es := tr.EntriesAt("c", at(testSettle)); es[0].Dirty || es[0].DueAt != "" || es[0].InFlight {
		t.Fatalf("stored: %+v", es[0])
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

// A retry is a floor: a root waiting out a busy backoff, or given back for
// its engine, isn't due before its retryAt, though its value changed
// meanwhile. Otherwise a busy root, which its engine is still evaluating and
// so keeps naming, would be exported every Settle, and a root of a
// disconnected engine requeued every Settle.
func TestTrackerRetryIsAFloor(t *testing.T) {
	change := func(tr *Tracker, d time.Duration, part string) {
		tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s1", Number: 1, Parts: []string{part}}, at(d))
	}
	t.Run("busy", func(t *testing.T) {
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		b := tr.Due(at(testSettle))
		change(tr, 1100*time.Millisecond, metaPart) // in flight
		// Busy: retryAt = done + BusyBackoff (1s).
		tr.Done(b[0], Result{Busy: []protocol.SkippedRoot{{Number: 1, Reason: protocol.SkipBusy}}}, at(5*time.Second))
		if got := roots(tr.Due(at(5*time.Second + 999*time.Millisecond))); len(got) != 0 {
			t.Fatalf("due before its busy backoff: %v", got)
		}
		if got := roots(tr.Due(at(6 * time.Second))); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("after its backoff: %v", got)
		}
	})
	t.Run("requeued", func(t *testing.T) {
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		b := tr.Due(at(testSettle))
		tr.Requeue(b[0], at(11*time.Second))
		change(tr, 2*time.Second, metaPart)
		for d := 2 * time.Second; d < 11*time.Second; d += 100 * time.Millisecond {
			if got := roots(tr.Due(at(d))); len(got) != 0 {
				t.Fatalf("requeued root due at %s, before its wait: %v", d, got)
			}
		}
		if got := roots(tr.Due(at(11 * time.Second))); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("after its wait: %v", got)
		}
	})
	t.Run("drain", func(t *testing.T) {
		// A retry during the drain waits its backoff too, changed or not.
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		tr.StartDrain(t0)
		b := tr.DrainDue(t0)
		change(tr, 100*time.Millisecond, metaPart)
		tr.Done(b[0], Result{Busy: []protocol.SkippedRoot{{Number: 1, Reason: protocol.SkipBusy}}}, at(200*time.Millisecond))
		if got := roots(tr.DrainDue(at(500 * time.Millisecond))); len(got) != 0 {
			t.Fatalf("drain due before its backoff: %v", got)
		}
		if st := tr.DrainStatus(at(500*time.Millisecond), time.Time{}); len(st) != 1 || st[0].Retrying != 1 || st[0].Pending != 0 {
			t.Fatalf("drain status %+v, want it retrying", st)
		}
	})
}

// A drain waits for an ended session's missing spans for Settle, but at
// most maxDrainSessionWait: a long Settle doesn't hold it idle.
func TestTrackerDrainSessionWaitBounded(t *testing.T) {
	tr := NewTracker(TrackerConfig{Settle: 10 * time.Minute, MaxDelay: 10 * time.Minute})
	tr.Observe(state(1, 10, fsPart), t0)
	tr.SessionEnded("c", 1, "s1", 2, t0)
	tr.StartDrain(t0)
	tr.DrainDue(t0)
	if st := tr.DrainStatus(at(maxDrainSessionWait-time.Millisecond), time.Time{}); len(st) != 1 || st[0].Sessions != 1 {
		t.Fatalf("drain status %+v, want the session waited for", st)
	}
	if st := tr.DrainStatus(at(maxDrainSessionWait), time.Time{}); len(st) != 1 || st[0].Sessions != 0 {
		t.Fatalf("drain status %+v, want no session waited for after %s", st, maxDrainSessionWait)
	}
}

// With Settle longer than ForgetAfter, a session is kept until Settle, so a
// carrier that comes after ForgetAfter still speeds its entries up.
func TestTrackerSessionKeptWhileItCanSpeedUp(t *testing.T) {
	tr := NewTracker(TrackerConfig{Settle: time.Minute, MaxDelay: time.Hour, ForgetAfter: 10 * time.Second})
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Due(at(30 * time.Second))
	tr.SessionEnded("c", 1, "s1", 1, at(30*time.Second))
	if got := roots(tr.Due(at(30 * time.Second))); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("the carrier after ForgetAfter didn't speed it up: %v", got)
	}
}

// Spans of a cache whose slot is taken don't wake the scheduler: it would
// run Due for nothing. Freeing the slot does.
func TestTrackerNoNotifyWhileHeld(t *testing.T) {
	var kicks atomic.Int32
	tr := NewTracker(TrackerConfig{Settle: testSettle, MaxDelay: testMaxDelay, Notify: func() { kicks.Add(1) }})
	tr.Observe(noSession(1, 10, fsPart), t0)
	b := tr.Due(at(testSettle))
	before := kicks.Load()
	for i := uint64(2); i < 100; i++ {
		tr.Observe(noSession(i, 10, fsPart), at(testSettle))
		tr.Due(at(testSettle)) // as the scheduler would, resetting NextDue
	}
	if k := kicks.Load() - before; k != 0 {
		t.Fatalf("%d kicks for spans of a held cache", k)
	}
	tr.Done(b[0], Result{Stored: []uint64{1}}, at(testSettle))
	if kicks.Load() == before {
		t.Fatal("no kick when the slot was freed")
	}
}

// Under a backlog of more than a batch, the longest due go first: a root
// with a high number isn't starved by lower ones that keep coming due.
func TestTrackerBacklogLongestDueFirst(t *testing.T) {
	tr := newTestTracker()
	// 1,000 low numbers due now, and one high number, due since earlier.
	tr.Observe(noSession(99999, 10, fsPart), t0)
	for n := uint64(1); n <= maxBatchRoots; n++ {
		tr.Observe(noSession(n, 10, fsPart), at(time.Second))
	}
	b := tr.Due(at(2 * time.Second))
	if len(b) != 1 || !slices.Contains(b[0].Request.Roots, 99999) {
		t.Fatal("the root due longest wasn't in the first batch")
	}
	if w := tr.WaitingForSlot(); w != 1 {
		t.Fatalf("waiting %d, want 1", w)
	}
}

// An import an engine.prune drops, from an entry a later span retained,
// isn't the stored value any more: it is dirty, the scheduler is woken, and
// it is exported.
func TestTrackerPrunedImportExported(t *testing.T) {
	var kicks atomic.Int32
	tr := NewTracker(TrackerConfig{Settle: testSettle, MaxDelay: testMaxDelay, Notify: func() { kicks.Add(1) }})
	tr.Imported("c", 1, []ImportedValue{{Number: 1, Parts: []dagql.PersistedPartAddress{{Part: "fs"}}}})
	tr.Observe(state(1, 100, fsPart), t0)
	if got := dueRoots(tr, at(time.Minute)); len(got) != 0 {
		t.Fatalf("unchanged import exported: %v", got)
	}
	// Dropped before the span that retained it: still retained.
	before := kicks.Load()
	tr.Pruned("c", 1, []Drop{{Number: 1, DroppedAtUnixNano: 50}})
	if kicks.Load() == before {
		t.Fatal("the scheduler wasn't woken")
	}
	if es := tr.EntriesAt("c", at(time.Minute)); !es[0].Dirty {
		t.Fatalf("entry %+v, want dirty", es[0])
	}
	if got := dueRoots(tr, at(time.Minute)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after its import was dropped: %v", got)
	}
}
