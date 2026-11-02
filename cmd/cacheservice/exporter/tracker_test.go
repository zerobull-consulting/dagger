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

func newTestTracker() *Tracker {
	return NewTracker(TrackerConfig{
		QuietPeriod:    10 * time.Second,
		EndGrace:       2 * time.Second,
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

func TestTrackerExportsRetainedAtSessionEnd(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	notRetained := state(2, 11)
	notRetained.Retained = false
	tr.Observe(notRetained, t0)
	// A lazy-evaluation span adds a part.
	tr.Observe(Observation{CacheID: "c", Generation: 1, Session: "s1", Number: 1, Parts: []string{metaPart}}, t0)

	if b := tr.Due(t0); len(b) != 0 {
		t.Fatalf("due before the session ended: %v", roots(b))
	}
	tr.SessionEnded("c", 1, "s1", 3, t0)
	batches := tr.Due(t0)
	if len(batches) != 1 || !slices.Equal(batches[0].Request.Roots, []uint64{1}) {
		t.Fatalf("batches %+v", batches)
	}
	b := batches[0]
	if b.CacheID != "c" || len(b.Request.Outputs) != 2 {
		t.Fatalf("batch %+v", b)
	}
	for _, o := range b.Request.Outputs {
		if o.Number != 1 || (o.Address.Part != "fs" && o.Address.Part != "execMeta") {
			t.Fatalf("output %+v", o)
		}
	}
	// In flight: not handed out again.
	if b := tr.All("c", t0); len(b) != 0 {
		t.Fatalf("in-flight root handed out again: %v", roots(b))
	}
}

func TestTrackerWaitsForAnnouncedSpans(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	tr.SessionEnded("c", 1, "s1", 2, t0)
	if b := tr.Due(t0.Add(time.Second)); len(b) != 0 {
		t.Fatal("due while a span is still coming")
	}
	if got := roots(tr.Due(t0.Add(2 * time.Second))); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after the grace: %v", got)
	}
}

func TestTrackerQuietPeriod(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	if b := tr.Due(t0.Add(9 * time.Second)); len(b) != 0 {
		t.Fatal("due before the quiet period")
	}
	if got := roots(tr.Due(t0.Add(10 * time.Second))); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("after the quiet period: %v", got)
	}
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
	if got := roots(tr.Due(t0)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("not re-exported after its parts grew: %v", got)
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

func TestTrackerQuietSessionRemoved(t *testing.T) {
	tr := newTestTracker()
	tr.Observe(state(1, 10, fsPart), t0)
	if got := roots(tr.Due(t0.Add(10 * time.Second))); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("quiet session: %v", got)
	}
	if n := len(tr.caches["c"].sessions); n != 0 {
		t.Fatalf("%d sessions kept after export", n)
	}
	// A later span starts it again.
	tr.Observe(state(2, 20, fsPart), t0.Add(11*time.Second))
	if n := len(tr.caches["c"].sessions); n != 1 {
		t.Fatalf("%d sessions after a new span", n)
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
	if got := roots(tr.Due(t0)); !slices.Equal(got, []uint64{1}) {
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
	tr.SessionEnded("c", 1, "s1", 2, t0)
	tr.Due(t0) // drops the session: its root is in flight
	tr.Done(b[0], Result{Stored: []uint64{1}}, t0)
	if got := roots(tr.Due(t0)); len(got) != 0 {
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
