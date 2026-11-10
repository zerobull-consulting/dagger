package e2e

import (
	"testing"
	"time"
)

// These test the measurements of the "leads" scenario on records laid out
// as the service logs them, without Docker.

var leadsT0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func spanRec(after time.Duration, cache string, n string, call bool, retained bool, repl string, deps string) LogRecord {
	outcome := ""
	if call {
		outcome = "executed"
	}
	r := "false"
	if retained {
		r = "true"
	}
	return LogRecord{Time: leadsT0.Add(after), Msg: "cache span", Attrs: map[string]string{
		"cache": cache, "result": n, "outcome": outcome, "retained": r, "replacements": repl, "deps": deps,
	}}
}

func storedRec(after time.Duration, cacheID, numbers string) LogRecord {
	return LogRecord{Time: leadsT0.Add(after), Msg: "export stored roots", Attrs: map[string]string{"cacheID": cacheID, "generation": "1", "numbers": numbers}}
}

func TestDepsAfterStoreSeesGrowthAtTheSameCount(t *testing.T) {
	recs := []LogRecord{
		spanRec(0, "c1/1", "7", false, false, "1", "1"),
		spanRec(time.Second, "c1/1", "7", true, true, "1", "2"),
		storedRec(3*time.Second, "c1", "7,9"),
		spanRec(5*time.Second, "c1/1", "7", false, true, "1", "3"), // grew after the store
		spanRec(6*time.Second, "c1/1", "7", false, true, "1", "3"), // not again
	}
	stored, growth, _ := depsAfterStore(recs)
	if stored != 1 || len(growth) != 1 {
		t.Fatalf("stored %d, growth %+v; want 1 stored value and one growth", stored, growth)
	}
	g := growth[0]
	if g.number != 7 || g.before != 2 || g.after != 3 || g.late != 2*time.Second {
		t.Errorf("growth %+v; want entry 7, deps 2 -> 3, 2s after the store", g)
	}
}

func TestDepsAfterStoreIgnoresWhatIsNotGrowth(t *testing.T) {
	recs := []LogRecord{
		spanRec(0, "c1/1", "7", true, true, "2", "3"),
		storedRec(time.Second, "c1", "7"),
		spanRec(2*time.Second, "c1/1", "7", false, true, "2", "1"), // fewer deps: the tracker keeps the longest
		spanRec(3*time.Second, "c1/1", "7", false, true, "1", "9"), // an older value
		spanRec(4*time.Second, "c1/1", "7", false, true, "3", "9"), // a new value, stored for itself
		spanRec(5*time.Second, "c1/1", "8", false, true, "1", "5"), // never stored
		spanRec(6*time.Second, "c1/1", "7", false, true, "3", "9"), // the new value's deps, unchanged
	}
	stored, growth, _ := depsAfterStore(recs)
	if stored != 1 {
		t.Errorf("stored %d values; want 1 (entry 8 was never stored)", stored)
	}
	if len(growth) != 0 {
		t.Errorf("growth %+v; want none", growth)
	}
}

func TestDepsAfterStoreMatchesTheCacheID(t *testing.T) {
	// A store of the same number in another cache says nothing of this one.
	recs := []LogRecord{
		spanRec(0, "c1/1", "7", true, true, "1", "1"),
		storedRec(time.Second, "c2", "7"),
		spanRec(2*time.Second, "c1/1", "7", false, true, "1", "4"),
	}
	if stored, growth, _ := depsAfterStore(recs); stored != 0 || len(growth) != 0 {
		t.Errorf("stored %d, growth %+v; want neither", stored, growth)
	}
}

func TestLazyToCallGaps(t *testing.T) {
	recs := []LogRecord{
		// Entry 7: lazy spans, then its call span 12s after the last.
		spanRec(0, "c1/1", "7", false, false, "1", "0"),
		spanRec(2*time.Second, "c1/1", "7", false, false, "1", "1"),
		spanRec(14*time.Second, "c1/1", "7", true, true, "1", "1"),
		// Later, retained: a lazy span and a call span 30s apart is not a
		// risk to the entry, and counts as a gap of the entry the engine
		// already kept.
		spanRec(20*time.Second, "c1/1", "7", false, true, "1", "1"),
		spanRec(50*time.Second, "c1/1", "7", true, true, "1", "1"),
		// Entry 9: lazy spans, then a call span that says it isn't retained,
		// 40s later: nothing to lose.
		spanRec(0, "c1/1", "9", false, false, "1", "0"),
		spanRec(40*time.Second, "c1/1", "9", true, false, "1", "0"),
		// Entry 8: a call span with no lazy span before it.
		spanRec(60*time.Second, "c1/1", "8", true, true, "1", "0"),
	}
	gaps := lazyToCall(recs)
	if len(gaps) != 3 {
		t.Fatalf("gaps %+v; want 3", gaps)
	}
	var open, kept *lazyGap
	risky := 0
	for i := range gaps {
		switch {
		case gaps[i].atRisk():
			open = &gaps[i]
			risky++
		case gaps[i].number == 7:
			kept = &gaps[i]
		case gaps[i].number != 9 || gaps[i].gap != 40*time.Second || gaps[i].call.retained:
			t.Errorf("unexpected gap %+v", gaps[i])
		}
	}
	if risky != 1 {
		t.Errorf("%d gaps at risk, want 1 (entry 9's call span says it isn't retained)", risky)
	}
	if open == nil || open.gap != 12*time.Second || open.number != 7 {
		t.Errorf("open gap %+v; want entry 7, 12s", open)
	}
	if kept == nil || kept.gap != 30*time.Second {
		t.Errorf("kept gap %+v; want 30s", kept)
	}
}

// A value stored again at a higher replacement count: growth the second
// export covered is not growth after a store, and growth after the second is.
func TestDepsAfterStoreRestored(t *testing.T) {
	recs := []LogRecord{
		spanRec(0, "c1/1", "7", true, true, "1", "1"),
		storedRec(time.Second, "c1", "7"),
		spanRec(2*time.Second, "c1/1", "7", true, true, "2", "1"),
		spanRec(3*time.Second, "c1/1", "7", false, true, "2", "3"), // before the second store
		storedRec(4*time.Second, "c1", "7"),
		spanRec(5*time.Second, "c1/1", "7", false, true, "2", "4"), // after it
	}
	stored, growth, _ := depsAfterStore(recs)
	if stored != 1 || len(growth) != 1 || growth[0].before != 3 || growth[0].after != 4 || growth[0].storedAt != leadsT0.Add(4*time.Second) {
		t.Fatalf("stored %d, growth %+v; want one growth, 3 -> 4 after the second store", stored, growth)
	}
}

// Entry numbers are reused across generations: a store of generation 1 says
// nothing of generation 2's entry.
func TestDepsAfterStoreKeysByGeneration(t *testing.T) {
	recs := []LogRecord{
		spanRec(0, "c1/2", "7", true, true, "1", "1"),
		storedRec(time.Second, "c1", "7"), // generation 1
		spanRec(2*time.Second, "c1/2", "7", false, true, "1", "4"),
	}
	if stored, growth, _ := depsAfterStore(recs); stored != 0 || len(growth) != 0 {
		t.Errorf("stored %d, growth %+v; want neither", stored, growth)
	}
}

func TestRootsByEntryKeepsEveryStoreAndCountsOmitted(t *testing.T) {
	a := storedRec(time.Second, "c1", "7,x,,9")
	a.Attrs["omitted"] = "3"
	stores, omitted := rootsByEntry([]LogRecord{a, storedRec(2*time.Second, "c1", "7")}, "export stored roots")
	if omitted != 3 || len(stores[entryKey{"c1/1", 7}]) != 2 || len(stores[entryKey{"c1/1", 9}]) != 1 || len(stores) != 2 {
		t.Errorf("stores %v, omitted %d", stores, omitted)
	}
}

// spanRecEnd is a span record with an end time.
func spanRecEnd(after time.Duration, end string, cache, n string, call, retained bool, repl, deps string) LogRecord {
	r := spanRec(after, cache, n, call, retained, repl, deps)
	r.Attrs["endUnixNano"] = end
	return r
}

// The tracker ignores a span of a generation older than the newest it has
// seen for the cache, so growth reported by one is not growth.
func TestSpansOfAnOlderGenerationAreIgnored(t *testing.T) {
	recs := []LogRecord{
		spanRec(0, "c1/1", "7", true, true, "1", "1"),
		storedRec(time.Second, "c1", "7"),                          // generation 1
		spanRec(2*time.Second, "c1/2", "8", true, true, "1", "1"),  // generation 2 reports in
		spanRec(3*time.Second, "c1/1", "7", false, true, "1", "5"), // a late span of generation 1
	}
	if got := spansByEntry(recs); len(got[entryKey{"c1/1", 7}]) != 1 {
		t.Errorf("spans of entry 7 of generation 1: %+v; want only the first", got[entryKey{"c1/1", 7}])
	}
	if stored, growth, _ := depsAfterStore(recs); stored != 1 || len(growth) != 0 {
		t.Errorf("stored %d, growth %+v; want no growth from a stale generation's span", stored, growth)
	}
	// Another cache's newer generation says nothing of this one.
	other := []LogRecord{
		spanRec(0, "c2/2", "8", true, true, "1", "1"),
		spanRec(time.Second, "c1/1", "7", true, true, "1", "1"),
	}
	if got := spansByEntry(other); len(got[entryKey{"c1/1", 7}]) != 1 {
		t.Errorf("a span of cache c1 was dropped for cache c2's generation: %+v", got)
	}
}

// The tracker keeps the retained flag of the call span with the latest end
// time: a call span that ends earlier doesn't override it, however late it
// arrives.
func TestRetainedFollowsTheLatestEndTime(t *testing.T) {
	recs := []LogRecord{
		spanRecEnd(0, "100", "c1/1", "7", true, true, "1", "1"),
		spanRecEnd(time.Second, "50", "c1/1", "7", true, false, "1", "1"), // ends earlier, arrives later
		spanRecEnd(2*time.Second, "150", "c1/1", "7", false, true, "1", "1"),
		spanRecEnd(32*time.Second, "200", "c1/1", "7", true, true, "1", "1"),
	}
	for _, g := range lazyToCall(recs) {
		if g.atRisk() {
			t.Errorf("gap %+v at risk: the entry was retained by the call span that ended last", g)
		}
	}
	if gaps := lazyToCall(recs); len(gaps) != 1 || gaps[0].gap != 30*time.Second {
		t.Errorf("gaps %+v; want one of 30s", gaps)
	}
	// With the later-ending span saying not retained, the entry is not kept.
	recs[0] = spanRecEnd(0, "100", "c1/1", "7", true, false, "1", "1")
	recs[1] = spanRecEnd(time.Second, "50", "c1/1", "7", true, true, "1", "1")
	if gaps := lazyToCall(recs); len(gaps) != 1 || !gaps[0].atRisk() {
		t.Errorf("gaps %+v; want one at risk", gaps)
	}
}

func tryRec(after time.Duration, cacheID, numbers string) LogRecord {
	r := storedRec(after, cacheID, numbers)
	r.Msg = "export try roots"
	return r
}

// The engine fixes a bundle's deps when the export try starts, so a span that
// arrives after that and before the store is logged is not in the stored
// value: growth, though it comes before the store.
func TestDepsAfterStoreCountsFromTheStartOfTheTry(t *testing.T) {
	recs := []LogRecord{
		spanRec(0, "c1/1", "7", true, true, "1", "2"),
		tryRec(time.Second, "c1", "7"),
		spanRec(2*time.Second, "c1/1", "7", false, true, "1", "3"), // after the try started, before the store
		storedRec(3*time.Second, "c1", "7"),
	}
	stored, growth, _ := depsAfterStore(recs)
	if stored != 1 || len(growth) != 1 || growth[0].before != 2 || growth[0].after != 3 || growth[0].storedAt != leadsT0.Add(time.Second) {
		t.Fatalf("stored %d, growth %+v; want one growth, 2 -> 3, counted from the try at 1s", stored, growth)
	}
	// A try that did not store the root, before the one that did.
	recs = []LogRecord{
		spanRec(0, "c1/1", "7", true, true, "1", "2"),
		tryRec(time.Second, "c1", "7"), // busy: no store
		spanRec(2*time.Second, "c1/1", "7", false, true, "1", "3"),
		tryRec(3*time.Second, "c1", "7"),
		storedRec(4*time.Second, "c1", "7"),
	}
	if _, growth, _ := depsAfterStore(recs); len(growth) != 0 {
		t.Errorf("growth %+v; the try that stored the value started after the span", growth)
	}
	// Tries of other roots and caches don't count.
	recs = []LogRecord{
		spanRec(0, "c1/1", "7", true, true, "1", "2"),
		tryRec(time.Second, "c1", "8,9"),
		tryRec(time.Second, "c2", "7"),
		spanRec(2*time.Second, "c1/1", "7", false, true, "1", "3"),
		storedRec(3*time.Second, "c1", "7"),
	}
	if _, growth, _ := depsAfterStore(recs); len(growth) != 0 {
		t.Errorf("growth %+v; no try named root 7 of cache c1", growth)
	}
}

func TestQuietGaps(t *testing.T) {
	recs := []LogRecord{
		// Entry 7: two lazy spans 15s apart, then a call span that retains it,
		// then a lazy span 40s later: that gap is of a retained entry.
		spanRecEnd(0, "10", "c1/1", "7", false, false, "1", "0"),
		spanRecEnd(15*time.Second, "20", "c1/1", "7", false, false, "1", "1"),
		spanRecEnd(16*time.Second, "30", "c1/1", "7", true, true, "1", "1"),
		spanRecEnd(56*time.Second, "40", "c1/1", "7", false, true, "1", "1"),
		// Entry 9 is never retained: a call span that says not retained, then
		// another span 2 minutes later.
		spanRecEnd(0, "10", "c1/1", "9", true, false, "1", "0"),
		spanRecEnd(120*time.Second, "20", "c1/1", "9", false, false, "1", "0"),
	}
	gaps := quietGaps(recs)
	if len(gaps) != 3 || gaps[0].number != 9 || gaps[0].gap != 120*time.Second || gaps[1].gap != 15*time.Second || gaps[2].gap != time.Second {
		t.Fatalf("gaps %+v; want entry 9's 120s, then entry 7's 15s and 1s (not its 40s, after it was retained)", gaps)
	}
}

// A call span that says retained but ends earlier than the one the tracker
// took its flag from doesn't make the tracker keep the entry, however late it
// arrives, so a gap before it is not at risk.
func TestAtRiskUsesTheEffectiveRetainedFlag(t *testing.T) {
	recs := []LogRecord{
		spanRecEnd(0, "100", "c1/1", "7", true, false, "1", "1"), // the latest-ending call span: not retained
		spanRecEnd(time.Second, "10", "c1/1", "7", false, false, "1", "1"),
		spanRecEnd(31*time.Second, "50", "c1/1", "7", true, true, "1", "1"), // ends earlier, arrives later
	}
	gaps := lazyToCall(recs)
	if len(gaps) != 1 || gaps[0].gap != 30*time.Second || !gaps[0].unretained || gaps[0].atRisk() {
		t.Fatalf("gaps %+v; want one 30s gap of an unretained entry, not at risk", gaps)
	}
	// The same call span ending later does make the tracker keep it.
	recs[2] = spanRecEnd(31*time.Second, "150", "c1/1", "7", true, true, "1", "1")
	if gaps := lazyToCall(recs); len(gaps) != 1 || !gaps[0].atRisk() {
		t.Errorf("gaps %+v; want one at risk", gaps)
	}
}
