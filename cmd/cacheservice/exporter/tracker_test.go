package exporter

import (
	"slices"
	"testing"
	"time"

	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

var t0 = time.Unix(1_800_000_000, 0)

const (
	fsPart   = `{"part":"fs"}`
	metaPart = `{"part":"execMeta"}`
)

// The test tracker's timing.
const (
	testSettle   = time.Second
	testMaxDelay = 10 * time.Second
	testForget   = 10 * time.Second
)

func newTestTracker() *Tracker {
	return NewTracker(TrackerConfig{
		Settle:         testSettle,
		MaxDelay:       testMaxDelay,
		ForgetAfter:    testForget,
		BusyBackoff:    time.Second,
		MaxBusyBackoff: 8 * time.Second,
		MaxFailures:    3,
	})
}

// state is a call span's observation of a retained entry.
func state(n uint64, end int64, parts ...string) Observation {
	return Observation{
		CacheID: "c", Generation: 1, Session: "s1", Number: n,
		State: true, Type: "Container", HasDeps: true, Parts: parts,
		Retained: true, EndUnixNano: end,
	}
}

func roots(batches []Batch) []uint64 {
	var out []uint64
	for _, b := range batches {
		out = append(out, b.Request.Roots...)
	}
	slices.Sort(out)
	return out
}

func TestTrackerStoredNotExportedAgain(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Done(b[0], Result{Stored: []uint64{1}}, t0)

	// The same value, seen again in a later session.
	tr.Observe(state(1, 20, fsPart), t0)
	tr.SessionEnded("c", 1, "s1", 1, t0)
	if got := roots(tr.Due(t0)); len(got) != 0 {
		t.Fatalf("stored value exported again: %v", got)
	}
	// A new part: exported again.
	tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s2", Number: 1, Parts: []string{metaPart}}, t0)
	if got := roots(tr.All("c", t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("grown parts: %v", got)
	}
}

func TestTrackerPartsGrewDuringExport(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Observe(Observation{CacheID: "c", Generation: 1, Number: 1, Parts: []string{metaPart}}, t0)
	tr.Done(b[0], Result{Stored: []uint64{1}}, t0)
	// Dirty again since the change in flight: due once it settles, not
	// back to back with the export that just ended (#135 point 4).
	if got := roots(tr.Due(t0.Add(testSettle - time.Millisecond))); len(got) != 0 {
		t.Fatalf("re-exported before it settled: %v", got)
	}
	again := tr.Due(t0.Add(testSettle))
	if got := roots(again); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("not re-exported after its parts grew: %v", got)
	}
	// The next export's value covers the new part.
	if len(again[0].Request.Outputs) != 2 {
		t.Fatalf("outputs %+v, want both parts", again[0].Request.Outputs)
	}
}

func TestTrackerReplacements(t *testing.T) {
	tr := newTestTracker()
	o := state(1, 10, fsPart)
	o.Replacements = 2
	tr.Observe(o, t0)
	// An older value adds nothing.
	old := state(1, 11, metaPart)
	old.Replacements = 1
	tr.Observe(old, t0)
	if e := tr.Entries("c"); len(e) != 1 || !slices.Equal(e[0].Parts, []string{fsPart}) {
		t.Fatalf("entries %+v", e)
	}
	b := tr.All("c", t0)
	tr.Done(b[0], Result{Stored: []uint64{1}}, t0)
	// A newer value resets the parts, and is exported again.
	newer := state(1, 12, metaPart)
	newer.Replacements = 3
	tr.Observe(newer, t0)
	e := tr.Entries("c")
	if !slices.Equal(e[0].Parts, []string{metaPart}) || e[0].Stored {
		t.Fatalf("entries %+v", e)
	}
	if got := roots(tr.All("c", t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("new value: %v", got)
	}
}

func TestTrackerGenerationFence(t *testing.T) {
	tr := newTestTracker()
	o := state(1, 10, fsPart)
	o.Generation = 2
	tr.Observe(o, t0)
	old := state(5, 10, fsPart)
	old.Generation = 1
	tr.Observe(old, t0)
	if got := roots(tr.All("c", t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("older generation not fenced: %v", got)
	}
}

// A root the index refused as a stale generation's export (#59) isn't
// exported again by that generation, but is by a newer one, which the index
// accepts. An engine's own refusal stays across generations. Tested through
// Due, as the service runs: All exports whatever is wanted, scheduled or not
// (#129).
func TestTrackerStaleRefusalClearedByNewerGeneration(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Observe(state(2, 10, fsPart), t0)
	now := t0.Add(10 * time.Second)
	b := tr.Due(now)
	if got := roots(b); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("first export: %v", got)
	}
	tr.Done(b[0], Result{Refused: []protocol.SkippedRoot{
		{Number: 1, Reason: protocol.SkipRefused, Message: StaleGenerationMessage},
		{Number: 2, Reason: protocol.SkipRefused, Message: "not exportable"},
	}}, now)
	if got := exportedWithin(tr, now, 2*time.Hour); len(got) != 0 {
		t.Fatalf("refused roots exported again in the same generation: %v", got)
	}
	// A newer generation reports in, naming neither root.
	now = now.Add(2 * time.Hour)
	tr.Started("c", 2, "")
	got := exportedWithin(tr, now, 2*time.Hour)
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after a newer generation: %v, want the stale refusal exported again, once", got)
	}
}

// exportedWithin runs Due every minute for d after now, settling each batch
// as stored, and returns every root it exported, in order of export.
func exportedWithin(tr *Tracker, now time.Time, d time.Duration) []uint64 {
	return exportedBy(tr.Due, tr, now, d)
}

func exportedBy(due func(time.Time) []Batch, tr *Tracker, now time.Time, d time.Duration) []uint64 {
	var out []uint64
	for at := now; !at.After(now.Add(d)); at = at.Add(time.Minute) {
		for _, b := range due(at) {
			out = append(out, b.Request.Roots...)
			tr.Done(b, Result{Stored: b.Request.Roots}, at)
		}
	}
	return out
}

// #129's repro: a stale-generation refusal that settled before the newer
// generation reported in is cleared by it, and Due exports the root again.
func TestTrackerStaleRefusalClearedRescheduled(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Done(b[0], Result{Refused: []protocol.SkippedRoot{
		{Number: 1, Reason: protocol.SkipRefused, Message: StaleGenerationMessage},
	}}, t0)
	tr.Started("c", 2, "")
	for i := 1; i <= 120; i++ {
		if got := roots(tr.Due(t0.Add(time.Duration(i) * time.Minute))); len(got) > 0 {
			if !slices.Equal(got, []uint64{1}) {
				t.Fatalf("Due exported %v, want [1]", got)
			}
			return
		}
	}
	t.Fatal("Due exported nothing in 2 hours, while All would export root 1")
}

// The same, for a service that is draining: DrainStatus counts the root as
// left to export, and DrainDue exports it.
func TestTrackerStaleRefusalClearedDrain(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Done(b[0], Result{Refused: []protocol.SkippedRoot{
		{Number: 1, Reason: protocol.SkipRefused, Message: StaleGenerationMessage},
	}}, t0)
	tr.Started("c", 2, "")
	now := t0.Add(time.Minute)
	tr.StartDrain(now)
	if st := tr.DrainStatus(now, time.Time{}); len(st) != 1 || st[0].Roots() != 1 || st[0].Retrying != 1 {
		t.Fatalf("drain status %+v, want root 1 retrying", st)
	}
	if got := exportedBy(tr.DrainDue, tr, now, time.Hour); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("DrainDue exported %v, want [1] once", got)
	}
	if st := tr.DrainStatus(now.Add(time.Hour), time.Time{}); len(st) != 0 {
		t.Fatalf("drain status after: %+v, want nothing left", st)
	}
}

// A cleared refusal is exported once, and not while an export already holds
// the root, nor once it is no longer worth exporting.
func TestTrackerStaleRefusalClearedNoDouble(t *testing.T) {
	refuse := func(tr *Tracker, at time.Time) {
		b := tr.All("c", at)
		tr.Done(b[0], Result{Refused: []protocol.SkippedRoot{
			{Number: 1, Reason: protocol.SkipRefused, Message: StaleGenerationMessage},
		}}, at)
	}
	t.Run("in flight", func(t *testing.T) {
		// The manual export holds the root when the newer generation
		// reports in: Due doesn't export it alongside, and once that
		// export stores it, nothing more.
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		refuse(tr, t0)
		held, err := tr.Claim("c", Request{Roots: []uint64{1}})
		if err != nil {
			t.Fatal(err)
		}
		tr.Started("c", 2, "")
		if got := roots(tr.Due(t0.Add(time.Minute))); len(got) != 0 {
			t.Fatalf("Due exported %v while an export holds it", got)
		}
		tr.Done(held, Result{Stored: []uint64{1}}, t0.Add(2*time.Minute))
		if got := exportedWithin(tr, t0.Add(2*time.Minute), 2*time.Hour); len(got) != 0 {
			t.Fatalf("exported %v after it was stored", got)
		}
	})
	t.Run("in flight and refused again", func(t *testing.T) {
		// That export is refused as stale too (claimed at generation 1):
		// done drops the refusal and schedules it, once.
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		refuse(tr, t0)
		held, err := tr.Claim("c", Request{Roots: []uint64{1}})
		if err != nil {
			t.Fatal(err)
		}
		tr.Started("c", 2, "")
		tr.Done(held, Result{Refused: []protocol.SkippedRoot{
			{Number: 1, Reason: protocol.SkipRefused, Message: StaleGenerationMessage},
		}}, t0.Add(time.Minute))
		if got := exportedWithin(tr, t0.Add(time.Minute), 2*time.Hour); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("exported %v, want [1] once", got)
		}
	})
	t.Run("named by a session", func(t *testing.T) {
		// A session of the newer generation names it too: one export.
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		refuse(tr, t0)
		newer := state(1, 20, fsPart)
		newer.Generation = 2
		newer.Session = "s2"
		tr.Observe(newer, t0)
		if got := exportedWithin(tr, t0, 2*time.Hour); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("exported %v, want [1] once", got)
		}
	})
	t.Run("pruned", func(t *testing.T) {
		// The newer generation pruned it before the next Due: not wanted.
		tr := newTestTracker()
		tr.Observe(state(1, 10, fsPart), t0)
		refuse(tr, t0)
		tr.Started("c", 2, "")
		tr.Pruned("c", 2, []Drop{{Number: 1, DroppedAtUnixNano: 100}})
		if got := exportedWithin(tr, t0, 2*time.Hour); len(got) != 0 {
			t.Fatalf("exported %v, want nothing: pruned", got)
		}
	})
}

// A stale-generation refusal that lands after a newer generation reported
// in, while the export was in flight, is dropped: the newer generation's
// export of the same value is accepted (#59).
func TestTrackerStaleRefusalInFlightDropped(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	newer := state(1, 20, fsPart)
	newer.Generation = 2
	newer.Session = "s2"
	tr.Observe(newer, t0)
	tr.Done(b[0], Result{Refused: []protocol.SkippedRoot{
		{Number: 1, Reason: protocol.SkipRefused, Message: StaleGenerationMessage},
	}}, t0)
	if e := tr.Entries("c"); len(e) != 1 || e[0].Refused != "" || e[0].Failures != 0 {
		t.Fatalf("entries %+v, want not refused, not failed", e)
	}
	// Through Due, as the service runs (#129): nothing in flight changed
	// the value, so done schedules it at once.
	if got := exportedWithin(tr, t0, time.Hour); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after the in-flight refusal: %v, want it exported again, once", got)
	}
}

func TestTrackerPruneWatermark(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 100, fsPart), t0)
	tr.Pruned("c", 1, []Drop{{Number: 1, DroppedAtUnixNano: 200}})
	if got := roots(tr.All("c", t0)); len(got) != 0 {
		t.Fatalf("pruned entry exported: %v", got)
	}
	// A span that ended before the prune, arriving late, doesn't retain it.
	tr.Observe(state(1, 150, fsPart), t0)
	if got := roots(tr.All("c", t0)); len(got) != 0 {
		t.Fatalf("late span re-retained: %v", got)
	}
	// A span after the prune does.
	tr.Observe(state(1, 300, fsPart), t0)
	if got := roots(tr.All("c", t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("retained again: %v", got)
	}
}

func TestTrackerExpiry(t *testing.T) {
	tr := newTestTracker()
	o := state(1, 10, fsPart)
	o.Expires = t0.Unix()
	tr.Observe(o, t0)
	r := state(2, 10, fsPart)
	r.RetentionExp = t0.Unix() - 1
	tr.Observe(r, t0)
	live := state(3, 10, fsPart)
	live.Expires = t0.Unix() + 60
	tr.Observe(live, t0)
	if got := roots(tr.All("c", t0)); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("expired entries exported: %v", got)
	}
}

func TestTrackerSkippedRoots(t *testing.T) {
	tr := newTestTracker()
	for n := uint64(1); n <= 3; n++ {
		tr.Observe(state(n, 10, fsPart), t0)
	}
	b := tr.All("c", t0)
	tr.Done(b[0], Result{
		Gone:    []uint64{1},
		Busy:    []protocol.SkippedRoot{{Number: 2, Reason: protocol.SkipBusy}},
		Refused: []protocol.SkippedRoot{{Number: 3, Reason: protocol.SkipRefused, Message: "no"}},
	}, t0)

	if e := tr.Entries("c"); len(e) != 2 || e[0].Number != 2 || e[1].Refused != "no" {
		t.Fatalf("entries %+v", e)
	}
	// Busy: due again after the backoff, then backing off further.
	if got := roots(tr.Due(t0.Add(500 * time.Millisecond))); len(got) != 0 {
		t.Fatalf("busy root due early: %v", got)
	}
	b = tr.Due(t0.Add(time.Second))
	if got := roots(b); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("busy root: %v", got)
	}
	tr.Done(b[0], Result{Busy: []protocol.SkippedRoot{{Number: 2, Reason: protocol.SkipBusy}}}, t0.Add(time.Second))
	if got := roots(tr.Due(t0.Add(2 * time.Second))); len(got) != 0 {
		t.Fatalf("second backoff not longer: %v", got)
	}
	if got := roots(tr.Due(t0.Add(3 * time.Second))); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("busy root, second time: %v", got)
	}
	// Refused: not retried for the same value, but for a new one.
	o := state(3, 20, fsPart)
	o.Replacements = 1
	tr.Observe(o, t0)
	if got := roots(tr.All("c", t0)); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("refused root with a new value: %v", got)
	}
}

// A caller that loops flushes doesn't retry a failing root at its own rate:
// a flush cuts short a failure wait only if a flush didn't cause the try
// that started it, so the root waits out the backoff after a second failed
// try, and a flush after its next try, the backoff's own, may cut it short
// again. A busy root, which doesn't count toward MaxFailures, is due at
// every flush (zerobull-consulting/dagger#160).
func TestTrackerLoopedFlushesDontRushFailures(t *testing.T) {
	tr := NewTracker(TrackerConfig{Settle: testSettle, MaxDelay: testMaxDelay, ForgetAfter: testForget,
		BusyBackoff: time.Second, MaxBusyBackoff: 8 * time.Second, MaxFailures: 5})
	tr.Observe(state(1, 10, fsPart), t0)
	// loop flushes 20 times, 10ms apart, answering each try with answer,
	// and returns the tries.
	now := t0
	loop := func(answer func(Batch) Result) int {
		tries := 0
		for range 20 {
			end := tr.StartFlush("c", now)
			for _, b := range tr.Due(now) {
				tries++
				tr.Done(b, answer(b), now)
			}
			end()
			now = now.Add(10 * time.Millisecond)
		}
		return tries
	}
	fail := func(Batch) Result { return Result{Failed: []FailedRoot{{Number: 1, Error: "upload"}}} }
	failures := func() int { return tr.Entries("c")[0].Failures }

	// The first flush's try fails, and the second flush cuts its wait
	// short; that try fails too, and no flush cuts the wait it started.
	if tries := loop(fail); tries != 2 || failures() != 2 {
		t.Fatalf("looped flushes: %d tries, %d failures, want 2 and 2", tries, failures())
	}
	// The backoff's own try (2s after the second): a flush may cut the
	// wait after it short again, once.
	now = now.Add(2 * time.Second)
	if bs := tr.Due(now); len(bs) != 1 {
		t.Fatalf("not due once its backoff passed: %d batches", len(bs))
	} else {
		tr.Done(bs[0], fail(bs[0]), now)
	}
	if tries := loop(fail); tries != 1 || failures() != 4 {
		t.Fatalf("looped flushes after the backoff's try: %d tries, %d failures, want 1 and 4", tries, failures())
	}

	// A busy root is due at every flush.
	tr.Observe(state(2, 10, fsPart), t0)
	busy := func(b Batch) Result {
		var res Result
		for _, n := range b.Request.Roots {
			if n == 2 {
				res.Busy = append(res.Busy, protocol.SkippedRoot{Number: 2, Reason: protocol.SkipBusy})
			} else {
				res.Failed = append(res.Failed, FailedRoot{Number: n, Error: "upload"})
			}
		}
		return res
	}
	if tries := loop(busy); tries != 20 {
		t.Fatalf("looped flushes of a busy root: %d tries, want 20", tries)
	}
	if e := tr.Entries("c"); e[1].Failures != 0 || e[0].Failures != 4 {
		t.Fatalf("entries %+v", e)
	}
}

// A failed root whose wait no flush cuts short (see
// TestTrackerLoopedFlushesDontRushFailures) is due at the next flush once
// its value changed: the failures counted were the old value's.
func TestTrackerFlushRetriesChangedValueAfterFlushFailure(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	fail := func(now time.Time) int {
		end := tr.StartFlush("c", now)
		defer end()
		bs := tr.Due(now)
		for _, b := range bs {
			tr.Done(b, Result{Failed: []FailedRoot{{Number: 1, Error: "upload"}}}, now)
		}
		return len(bs)
	}
	// The first flush's try fails, the second flush cuts its wait short,
	// and that try fails too: the third flush leaves the wait it started.
	if n := fail(t0) + fail(t0); n != 2 {
		t.Fatalf("%d tries, want 2", n)
	}
	if n := fail(t0); n != 0 {
		t.Fatal("the flush cut short a wait a flush's try started")
	}
	o := state(1, 10, fsPart)
	o.Replacements = 1
	tr.Observe(o, t0)
	end := tr.StartFlush("c", t0)
	defer end()
	if got := roots(tr.Due(t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("the changed value isn't due at the flush: %v", got)
	}
}

// A flush's try given back before it ran (Requeue, or Interrupted with the
// root unsettled) is still the flush's when it runs: if it fails, the next
// flush leaves the wait it started.
func TestTrackerFlushTryGivenBackStaysTheFlushs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		giveBack func(*Tracker, Batch, time.Time)
	}{
		{"requeued", func(tr *Tracker, b Batch, now time.Time) { tr.Requeue(b, now.Add(time.Hour)) }},
		{"interrupted", func(tr *Tracker, b Batch, now time.Time) { tr.Interrupted(b, Result{}, now, now.Add(time.Hour)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTestTracker()
			tr.Observe(state(1, 10, fsPart), t0)
			failed := Result{Failed: []FailedRoot{{Number: 1, Error: "upload"}}}
			// A try of its own, failed: a wait no flush caused.
			b := tr.All("c", t0)
			tr.Done(b[0], failed, t0)
			// A flush cuts it short; its try is given back to wait for
			// the engine, which then connects.
			end := tr.StartFlush("c", t0)
			bs := tr.Due(t0)
			if len(bs) != 1 {
				t.Fatalf("not due at the flush: %d batches", len(bs))
			}
			tc.giveBack(tr, bs[0], t0)
			end()
			tr.WakeEngineWait("c", 1, t0)
			bs = tr.Due(t0)
			if len(bs) != 1 {
				t.Fatalf("not due once the engine connected: %d batches", len(bs))
			}
			tr.Done(bs[0], failed, t0)
			end = tr.StartFlush("c", t0)
			defer end()
			if got := roots(tr.Due(t0)); len(got) != 0 {
				t.Fatalf("the flush cut short the wait its own try started: %v", got)
			}
		})
	}
}

func TestTrackerFailuresBackOffThenStop(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	now := t0
	for i := range 3 {
		b := tr.All("c", now)
		if len(b) != 1 {
			t.Fatalf("try %d: no batch", i)
		}
		tr.Done(b[0], Result{Failed: []FailedRoot{{Number: 1, Error: "upload"}}}, now)
		now = now.Add(time.Minute)
	}
	if got := roots(tr.All("c", now)); len(got) != 0 {
		t.Fatalf("still trying after MaxFailures: %v", got)
	}
	// Roots a result doesn't settle, as after an error reply or a timeout,
	// count as failed.
	tr.Observe(state(2, 10, fsPart), t0)
	b := tr.All("c", now)
	tr.Done(b[0], Result{}, now)
	if e := tr.Entries("c"); e[1].Failures != 1 || e[1].RetryAt == "" {
		t.Fatalf("entries %+v", e)
	}
}

func TestTrackerRequeue(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Requeue(b[0], t0.Add(5*time.Second))
	if got := roots(tr.Due(t0.Add(4 * time.Second))); len(got) != 0 {
		t.Fatalf("requeued root due early: %v", got)
	}
	if got := roots(tr.Due(t0.Add(5 * time.Second))); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("requeued: %v", got)
	}
	if e := tr.Entries("c"); e[0].Failures != 0 {
		t.Fatal("requeue counted as a failure")
	}
}

// The hello of the generation a requeued root waits for makes it due at
// once; a backoff, and the hello of an older generation, don't
// (zerobull-consulting/dagger#132).
func TestTrackerConnectedWakesEngineWait(t *testing.T) {
	setup := func() (*Tracker, Batch) {
		tr := newTestTracker()
		o := state(1, 10, fsPart)
		o.Generation = 2
		tr.Observe(o, t0)
		return tr, tr.All("c", t0)[0]
	}
	wait := t0.Add(10 * time.Second)
	now := t0.Add(time.Second)

	t.Run("newer generation's hello", func(t *testing.T) {
		tr, b := setup()
		tr.Requeue(b, wait)
		tr.Connected("c", 2, now)
		if got := roots(tr.Due(now)); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("not due at the hello: %v", got)
		}
	})
	t.Run("older generation's hello", func(t *testing.T) {
		tr, b := setup()
		tr.Requeue(b, wait)
		tr.Connected("c", 1, now)
		if got := roots(tr.Due(now)); len(got) != 0 {
			t.Fatalf("an older generation's hello made it due: %v", got)
		}
	})
	t.Run("backoff", func(t *testing.T) {
		tr, b := setup()
		tr.Done(b, Result{Failed: []FailedRoot{{Number: 1, Error: "upload"}}}, t0)
		retry := tr.Entries("c")[0].RetryAt
		now := t0.Add(time.Millisecond) // inside the backoff
		tr.Connected("c", 2, now)
		if got := tr.Entries("c")[0].RetryAt; got != retry {
			t.Fatalf("a hello moved a backoff: %s, was %s", got, retry)
		}
		if got := roots(tr.Due(now)); len(got) != 0 {
			t.Fatalf("a hello made a backed-off root due: %v", got)
		}
	})
	t.Run("interrupted export", func(t *testing.T) {
		tr, b := setup()
		tr.Interrupted(b, Result{}, t0, wait)
		if got := roots(tr.Due(now)); len(got) != 0 {
			t.Fatalf("due before the hello: %v", got)
		}
		tr.Connected("c", 2, now)
		if got := roots(tr.Due(now)); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("an interrupted root not due at the hello: %v", got)
		}
	})
	t.Run("claimed entry", func(t *testing.T) {
		tr, b := setup()
		tr.Requeue(b, wait)
		tr.Due(wait) // claims it
		tr.Connected("c", 2, wait.Add(time.Second))
		if got := tr.Entries("c")[0].RetryAt; got != "" {
			t.Fatalf("a hello gave a claimed entry a retry time: %s", got)
		}
	})
	t.Run("stale wait", func(t *testing.T) {
		// The entry waited until `wait`, was claimed, failed, and its
		// backoff ends at the same instant: still a backoff.
		tr, b := setup()
		tr.Requeue(b, wait)
		b2 := tr.Due(wait)
		tr.Done(b2[0], Result{Failed: []FailedRoot{{Number: 1, Error: "upload"}}}, wait.Add(-time.Second))
		tr.Connected("c", 2, wait.Add(-500*time.Millisecond))
		if got := roots(tr.Due(wait.Add(-500 * time.Millisecond))); len(got) != 0 {
			t.Fatalf("a hello cut a backoff short: %v", got)
		}
	})
}

func TestTrackerWipedCache(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Started("new", 1, "c")
	if e := tr.Entries("c"); e != nil {
		t.Fatalf("wiped cache still tracked: %+v", e)
	}
}

func TestTrackerForgetsIdleUnretainedEntries(t *testing.T) {
	tr := newTestTracker()
	o := state(1, 10)
	o.Retained = false
	tr.Observe(o, t0)
	tr.Due(t0.Add(5 * time.Second))
	if len(tr.Entries("c")) != 1 {
		t.Fatal("forgot an entry too early")
	}
	tr.Due(t0.Add(10 * time.Second))
	if len(tr.Entries("c")) != 0 {
		t.Fatal("idle entry kept")
	}
}

func TestTrackerBatchesAreBounded(t *testing.T) {
	tr := newTestTracker()
	for n := uint64(1); n <= maxBatchRoots+5; n++ {
		tr.Observe(state(n, 10, fsPart), t0)
	}
	b := tr.All("c", t0)
	if len(b) != 2 || len(b[0].Request.Roots) != maxBatchRoots || len(b[1].Request.Roots) != 5 {
		t.Fatalf("%d batches", len(b))
	}
}

func TestTrackerIgnoresBadParts(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, "not json", `{"part":""}`, fsPart), t0)
	if e := tr.Entries("c"); !slices.Equal(e[0].Parts, []string{fsPart}) {
		t.Fatalf("parts %v", e[0].Parts)
	}
}

func TestTrackerPrunedWhileWaitingIsForgotten(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Done(b[0], Result{Busy: []protocol.SkippedRoot{{Number: 1, Reason: protocol.SkipBusy}}}, t0)
	tr.Pruned("c", 1, []Drop{{Number: 1, DroppedAtUnixNano: 20}})

	if got := roots(tr.Due(t0.Add(time.Second))); len(got) != 0 {
		t.Fatalf("pruned root retried: %v", got)
	}
	if e := tr.Entries("c"); len(e) != 1 || e[0].RetryAt != "" {
		t.Fatalf("pruned root still waiting: %+v", e)
	}
	// The first pass exports its quiet session, which names it, and the next
	// forgets it.
	tr.Due(t0.Add(time.Minute))
	tr.Due(t0.Add(time.Minute))
	if e := tr.Entries("c"); len(e) != 0 {
		t.Fatalf("pruned root kept: %+v", e)
	}
}

// A root whose value changes while its export is in flight, and which the
// engine then refuses for the old value, is exported again for the new one.
func TestTrackerRetriesValueChangedInFlight(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	newer := state(1, 20, fsPart)
	newer.Replacements = 1
	tr.Observe(newer, t0)
	tr.Done(b[0], Result{Refused: []protocol.SkippedRoot{{Number: 1, Reason: protocol.SkipRefused}}}, t0)
	if got := roots(tr.Due(t0)); len(got) != 0 {
		t.Fatalf("new value retried before it settled: %v", got)
	}
	if got := roots(tr.Due(t0.Add(testSettle))); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("new value not retried: %v", got)
	}
}

// A root named by a session while in flight, then stored with the value it
// had, isn't exported again.
func TestTrackerNoRetryWhenStoredCovers(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Observe(state(1, 20, fsPart), t0)
	// The session ends with its spans in: its root is sped up, but in
	// flight.
	tr.SessionEnded("c", 1, "s1", 2, t0)
	if got := roots(tr.Due(t0)); len(got) != 0 {
		t.Fatalf("in-flight root in a second batch: %v", got)
	}
	tr.Done(b[0], Result{Stored: []uint64{1}}, t0)
	if got := exportedWithin(tr, t0, time.Hour); len(got) != 0 {
		t.Fatalf("stored root exported again: %v", got)
	}
}

func TestTrackerClaim(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	b, err := tr.Claim("c", Request{Roots: []uint64{1, 99}})
	if err != nil {
		t.Fatal(err)
	}
	if len(b.values) != 1 {
		t.Fatalf("claimed %d roots, want the tracked one", len(b.values))
	}
	// In flight: neither the scheduler nor another claim takes it.
	if got := roots(tr.All("c", t0)); len(got) != 0 {
		t.Fatalf("claimed root handed out: %v", got)
	}
	if _, err := tr.Claim("c", Request{Roots: []uint64{1}}); err == nil {
		t.Fatal("second claim of an in-flight root accepted")
	}
	tr.Done(b, Result{Stored: []uint64{1}, Gone: []uint64{99}}, t0)
	if e := tr.Entries("c"); len(e) != 1 || !e[0].Stored {
		t.Fatalf("claim's result not recorded: %+v", e)
	}
	// An unknown cache: nothing to claim, nothing to record.
	if b, err := tr.Claim("other", Request{Roots: []uint64{1}}); err != nil || len(b.values) != 0 {
		t.Fatalf("unknown cache: %+v, %v", b, err)
	}
}

// An interrupted export settles what it did, and puts the rest back without
// counting a failure.
func TestTrackerInterrupted(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Observe(state(2, 10, fsPart), t0)
	b := tr.All("c", t0)
	for range 10 {
		tr.Interrupted(b[0], Result{Stored: []uint64{1}}, t0, t0.Add(time.Second))
		b = tr.Due(t0.Add(time.Second))
		if got := roots(b); !slices.Equal(got, []uint64{2}) {
			t.Fatalf("interrupted root: %v", got)
		}
	}
	if e := tr.Entries("c"); !e[0].Stored || e[1].Failures != 0 {
		t.Fatalf("entries %+v", e)
	}
}

// An imported entry is the stored value: not exported while it has the
// merge's replacement count and parts, exported once it changes.
func TestTrackerImportedNotExported(t *testing.T) {
	tr := newTestTracker()
	tr.Imported("c", 1, []ImportedValue{
		{Number: 1, Parts: []dagql.PersistedPartAddress{{Part: "fs"}}},
		{Number: 2, Parts: []dagql.PersistedPartAddress{{Part: "fs"}}},
	})
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Observe(state(2, 10, fsPart, metaPart), t0)
	tr.Observe(state(3, 10, fsPart), t0)
	tr.SessionEnded("c", 1, "s1", 3, t0)
	if got := roots(tr.Due(t0)); !slices.Equal(got, []uint64{2, 3}) {
		t.Fatalf("exported %v, want 2 (a new part) and 3 (not imported)", got)
	}
	if es := tr.Entries("c"); !es[0].Stored {
		t.Fatalf("entries %+v", es)
	}
	// A pruned import is forgotten: its next value is the engine's own.
	tr.Pruned("c", 1, []Drop{{Number: 1, DroppedAtUnixNano: 5}})
	if got := roots(tr.All("c", t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after prune: %v", got)
	}
}

// Intentional: an unchanged import whose spans report a later expiry is not
// exported, as a stored entry isn't (exportedValue compares replacements and
// parts only). An export couldn't change the expiry anyway: the index keeps
// an imported value's record, whose expiry the entry has, unless the export
// holds more parts (see metastore's TestRecordImportKeepsRecordExpiry).
func TestTrackerImportedIgnoresExpiry(t *testing.T) {
	tr := newTestTracker()
	tr.Imported("c", 1, []ImportedValue{{Number: 1, Parts: []dagql.PersistedPartAddress{{Part: "fs"}}}})
	o := state(1, 10, fsPart)
	o.Expires = t0.Add(time.Hour).Unix()
	o.RetentionExp = t0.Add(2 * time.Hour).Unix()
	tr.Observe(o, t0)
	tr.SessionEnded("c", 1, "s1", 1, t0)
	if got := roots(tr.Due(t0)); len(got) != 0 {
		t.Fatalf("exported %v", got)
	}
}

// While draining, the tracker takes no new work: spans and ends of sessions
// it doesn't track, of caches it doesn't know, or of no session.
func TestTrackerDrainDropsNewSessions(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.StartDrain(t0)
	other := state(2, 10, fsPart)
	other.Session = "s2"
	noSession := state(3, 10, fsPart)
	noSession.Session = ""
	newCache := state(4, 10, fsPart)
	newCache.CacheID = "d"
	for _, o := range []Observation{other, noSession, newCache} {
		if tr.Observe(o, t0) {
			t.Fatalf("took %+v while draining", o)
		}
	}
	if tr.SessionEnded("c", 1, "s2", 1, t0) || tr.SessionEnded("d", 1, "s1", 1, t0) {
		t.Fatal("took the end of an untracked session while draining")
	}
	if got := roots(tr.DrainDue(t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("drain due: %v", got)
	}
	// Exported: the session is no longer tracked, and its later spans are
	// dropped too.
	if tr.Observe(state(1, 11, fsPart), t0) {
		t.Fatal("took a span of a session the drain already exported")
	}
}

// StartDrain makes roots waiting for another try due at once; a retry after
// that waits its normal backoff. DrainStatus tells retries due by the
// deadline from those due after it.
func TestTrackerDrainRetries(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.Observe(state(2, 10, fsPart), t0)
	b := tr.All("c", t0)
	tr.Done(b[0], Result{Busy: []protocol.SkippedRoot{{Number: 1}}, Failed: []FailedRoot{{Number: 2}}}, t0)
	tr.Done(tr.All("c", t0.Add(time.Hour))[0], Result{Busy: []protocol.SkippedRoot{{Number: 1}}, Failed: []FailedRoot{{Number: 2}}}, t0)
	// Both wait: 1 busy twice (2s), 2 failed twice (2s).
	if got := roots(tr.Due(t0.Add(time.Second))); len(got) != 0 {
		t.Fatalf("due early: %v", got)
	}
	tr.StartDrain(t0.Add(time.Second))
	got := tr.DrainDue(t0.Add(time.Second))
	if !slices.Equal(roots(got), []uint64{1, 2}) {
		t.Fatalf("retries not due at the drain's start: %v", roots(got))
	}
	// A busy root during the drain waits its backoff (4s now).
	tr.Done(got[0], Result{Busy: []protocol.SkippedRoot{{Number: 1}}, Stored: []uint64{2}}, t0.Add(time.Second))
	if got := roots(tr.DrainDue(t0.Add(4 * time.Second))); len(got) != 0 {
		t.Fatalf("a retry during the drain skipped its backoff: %v", got)
	}
	st := tr.DrainStatus(t0.Add(time.Second), t0.Add(3*time.Second))
	if len(st) != 1 || st[0].Later != 1 || st[0].Retrying != 0 || st[0].Roots() != 1 {
		t.Fatalf("drain status with the retry after the deadline: %+v", st)
	}
	st = tr.DrainStatus(t0.Add(time.Second), time.Time{})
	if len(st) != 1 || st[0].Retrying != 1 {
		t.Fatalf("drain status without a deadline: %+v", st)
	}
	if got := roots(tr.DrainDue(t0.Add(5 * time.Second))); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after its backoff: %v", got)
	}
	if st := tr.DrainStatus(t0.Add(5*time.Second), time.Time{}); len(st) != 1 || st[0].InFlight != 1 {
		t.Fatalf("drain status in flight: %+v", st)
	}
}

// StartDrain pulls retries forward once, when the drain starts: the service
// calls it again for every span request while draining, and a later call
// must not cut a retry's backoff short.
func TestTrackerStartDrainOnce(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.StartDrain(t0)
	b := tr.DrainDue(t0)
	tr.Done(b[0], Result{Busy: []protocol.SkippedRoot{{Number: 1}}}, t0)
	tr.StartDrain(t0.Add(100 * time.Millisecond))
	if got := roots(tr.DrainDue(t0.Add(100 * time.Millisecond))); len(got) != 0 {
		t.Fatalf("a second StartDrain cut the retry's backoff short: %v", got)
	}
}
