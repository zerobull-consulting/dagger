package e2e

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
)

// The two leads of zerobull-consulting/dagger#154, measured over the "cache
// span" and "export stored roots" records of every service the run started:
//
//  1. A stored value whose deps grow isn't exported again (tracker.go,
//     exportedValue.covers): does a later span report a longer deps list for
//     a value, at the replacement count it was first stored at?
//  2. A lazy-evaluation-only entry is forgotten after ForgetAfter: how long
//     before an entry's call span does its last lazy-evaluation span come,
//     while the engine doesn't yet keep the entry?

// entrySpan is one "cache span" record naming an entry.
type entrySpan struct {
	at   time.Time
	call bool // a call span (it has an outcome); otherwise lazy evaluation
	// retained is the call span's retained attribute.
	retained     bool
	replacements uint64
	deps         int
}

// entryKey names an entry: the span's cache attribute (cache ID and
// generation) and the entry's engine number.
type entryKey struct {
	cache  string
	number uint64
}

// spansByEntry groups the "cache span" records of one service by entry, in
// the order they arrived.
func spansByEntry(recs []LogRecord) map[entryKey][]entrySpan {
	out := map[entryKey][]entrySpan{}
	for _, r := range recs {
		if r.Msg != "cache span" {
			continue
		}
		n, err := strconv.ParseUint(r.Attr("result"), 10, 64)
		if err != nil {
			continue
		}
		repl, _ := strconv.ParseUint(r.Attr("replacements"), 10, 64)
		deps, _ := strconv.Atoi(r.Attr("deps"))
		k := entryKey{r.Attr("cache"), n}
		out[k] = append(out[k], entrySpan{
			at: r.Time, call: r.Attr("outcome") != "", retained: r.Attr("retained") == "true",
			replacements: repl, deps: deps,
		})
	}
	return out
}

// firstStored returns, per cache ID (the cache attribute before its "/"),
// when each root was first stored, from the "export stored roots" records.
func firstStored(recs []LogRecord) map[string]map[uint64]time.Time {
	out := map[string]map[uint64]time.Time{}
	for _, r := range recs {
		if r.Msg != "export stored roots" {
			continue
		}
		id := r.Attr("cacheID")
		for _, s := range strings.Split(r.Attr("numbers"), ",") {
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				continue
			}
			if out[id] == nil {
				out[id] = map[uint64]time.Time{}
			}
			if _, ok := out[id][n]; !ok {
				out[id][n] = r.Time
			}
		}
	}
	return out
}

// depsGrowth is a span that reported a longer deps list than any before it
// for the replacement count a value was stored at, after it was stored.
type depsGrowth struct {
	entryKey
	storedAt time.Time
	// before and after are the longest deps list up to the store, and this
	// span's.
	before, after int
	late          time.Duration
}

// depsAfterStore measures lead 1 for one service: how many values it stored
// (that the spans name), and the spans that reported a longer deps list at
// the replacement count the value had when it was first stored. The tracker
// keeps the highest replacement count seen and the longest deps list for it,
// and ignores a span of an older count, so these do the same.
func depsAfterStore(recs []LogRecord) (stored int, growth []depsGrowth) {
	spans := spansByEntry(recs)
	at := firstStored(recs)
	for k, ss := range spans {
		id, _, _ := strings.Cut(k.cache, "/")
		storedAt, ok := at[id][k.number]
		if !ok {
			continue
		}
		stored++
		var repl uint64
		deps := 0
		for _, s := range ss {
			if s.at.After(storedAt) {
				break
			}
			if s.replacements > repl {
				repl, deps = s.replacements, 0
			}
			if s.replacements == repl {
				deps = max(deps, s.deps)
			}
		}
		for _, s := range ss {
			if !s.at.After(storedAt) {
				continue
			}
			switch {
			case s.replacements > repl:
				// A new value: it is stored for itself.
				repl, deps = s.replacements, s.deps
			case s.replacements == repl && s.deps > deps:
				growth = append(growth, depsGrowth{k, storedAt, deps, s.deps, s.at.Sub(storedAt)})
				deps = s.deps
			}
		}
	}
	slices.SortFunc(growth, func(a, b depsGrowth) int {
		return cmp.Or(cmp.Compare(a.cache, b.cache), cmp.Compare(a.number, b.number), a.storedAt.Compare(b.storedAt))
	})
	return stored, growth
}

// lazyGap is the time from an entry's last lazy-evaluation span to its next
// call span.
type lazyGap struct {
	entryKey
	gap time.Duration
	// unretained is whether the engine didn't yet keep the entry when the
	// lazy span came (no call span before it said retained): only then does a
	// gap past ForgetAfter lose the entry's parts.
	unretained bool
}

// lazyToCall measures lead 2 for one service: for each call span preceded
// by lazy-evaluation spans with no call span between, the gap from the last
// of them.
func lazyToCall(recs []LogRecord) []lazyGap {
	var out []lazyGap
	for k, ss := range spansByEntry(recs) {
		var lastLazy time.Time
		retained := false
		for _, s := range ss {
			if !s.call {
				lastLazy = s.at
				continue
			}
			if !lastLazy.IsZero() {
				out = append(out, lazyGap{k, s.at.Sub(lastLazy), !retained})
				lastLazy = time.Time{}
			}
			retained = s.retained
		}
	}
	return out
}

// leadsBound is how long a lazy-evaluation span may come before its entry's
// call span while the engine doesn't keep the entry: the tracker forgets such
// an entry ForgetAfter after its last span.
const leadsBound = exporter.DefaultForgetAfter

// leads logs both measurements over every service's records, and fails if a
// value's deps grew after it was stored, or a lazy-evaluation span came
// leadsBound or more before the call span of an entry the engine didn't yet
// keep.
func (s *state) leads(t *testing.T) {
	var stored, total int
	var growth []depsGrowth
	var gaps []lazyGap
	for _, svc := range s.env.Services() {
		recs := svc.Logs.Since(0, "", nil)
		n, g := depsAfterStore(recs)
		stored += n
		growth = append(growth, g...)
		gaps = append(gaps, lazyToCall(recs)...)
		for _, r := range recs {
			if r.Msg == "cache span" {
				total++
			}
		}
	}
	reportLeads(t, total, stored, growth, gaps)
}

// reportLeads logs the measurements and checks them.
func reportLeads(t *testing.T, spans, stored int, growth []depsGrowth, gaps []lazyGap) {
	t.Helper()
	t.Logf("lead 1: %d cache spans; %d stored values named by them; %d later spans reported a longer deps list at the stored replacement count", spans, stored, len(growth))
	for _, g := range growth {
		t.Logf("lead 1: entry %d of %s: deps %d -> %d, %s after the value was stored", g.number, g.cache, g.before, g.after, g.late.Round(time.Millisecond))
	}
	Check(t, stored > 0, "lead 1: the run stored values the spans name (%d), so the measurement saw something", stored)
	Check(t, len(growth) == 0, "lead 1: no stored value's deps grew at its replacement count (%d did)", len(growth))

	var maxAll, maxOpen time.Duration
	open := 0
	for _, g := range gaps {
		maxAll = max(maxAll, g.gap)
		if g.unretained {
			open++
			maxOpen = max(maxOpen, g.gap)
		}
	}
	t.Logf("lead 2: %d lazy-evaluation-to-call gaps (%d of entries the engine didn't yet keep); largest %s overall, %s for those; ForgetAfter %s",
		len(gaps), open, maxAll.Round(time.Millisecond), maxOpen.Round(time.Millisecond), leadsBound)
	Check(t, open > 0, "lead 2: the run had lazy-evaluation spans before a call span (%d), so the measurement saw something", open)
	Check(t, maxOpen < leadsBound, "lead 2: the largest gap from a lazy-evaluation span to the call span of an entry the engine didn't yet keep is %s, under ForgetAfter %s", maxOpen.Round(time.Millisecond), leadsBound)
}
