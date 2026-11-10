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
	// end is the span's end time, as the tracker orders state spans by it.
	end int64
	// session and name are for reading a result: which session sent the
	// span, and its span name.
	session, name string
}

// entryKey names an entry: the span's cache attribute (cache ID and
// generation) and the entry's engine number.
type entryKey struct {
	cache  string
	number uint64
}

// spansByEntry groups the "cache span" records of one service by entry, in
// the order they arrived. As the tracker does, it leaves out a span of an
// older generation than the newest seen for its cache so far.
func spansByEntry(recs []LogRecord) map[entryKey][]entrySpan {
	out := map[entryKey][]entrySpan{}
	newest := map[string]uint64{}
	for _, r := range recs {
		if r.Msg != "cache span" {
			continue
		}
		n, err := strconv.ParseUint(r.Attr("result"), 10, 64)
		if err != nil {
			continue
		}
		if id, gen, ok := strings.Cut(r.Attr("cache"), "/"); ok {
			if g, err := strconv.ParseUint(gen, 10, 64); err == nil {
				if g < newest[id] {
					continue
				}
				newest[id] = g
			}
		}
		repl, _ := strconv.ParseUint(r.Attr("replacements"), 10, 64)
		deps, _ := strconv.Atoi(r.Attr("deps"))
		end, _ := strconv.ParseInt(r.Attr("endUnixNano"), 10, 64)
		k := entryKey{r.Attr("cache"), n}
		out[k] = append(out[k], entrySpan{
			at: r.Time, call: r.Attr("outcome") != "", retained: r.Attr("retained") == "true",
			replacements: repl, deps: deps, end: end, session: r.Attr("session"), name: r.Attr("name"),
		})
	}
	return out
}

// storesByEntry returns, per cache attribute as the spans carry it (cache ID
// and generation) and root, when each export stored the root, in order, from
// the "export stored roots" records. omitted counts roots the records left
// out, which the measurement can't see.
func storesByEntry(recs []LogRecord) (stores map[entryKey][]time.Time, omitted int) {
	stores = map[entryKey][]time.Time{}
	for _, r := range recs {
		if r.Msg != "export stored roots" {
			continue
		}
		o, _ := strconv.Atoi(r.Attr("omitted"))
		omitted += o
		cache := r.Attr("cacheID") + "/" + r.Attr("generation")
		for _, s := range strings.Split(r.Attr("numbers"), ",") {
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				continue
			}
			k := entryKey{cache, n}
			stores[k] = append(stores[k], r.Time)
		}
	}
	return stores, omitted
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
// the replacement count the value had when an export stored it, after that
// store and before the entry's next. The tracker keeps the highest
// replacement count seen and the longest deps list for it, and ignores a
// span of an older count, so these do the same. It also returns how many
// roots the records omitted.
func depsAfterStore(recs []LogRecord) (stored int, growth []depsGrowth, omitted int) {
	spans := spansByEntry(recs)
	stores, omitted := storesByEntry(recs)
	for k, ss := range spans {
		times := stores[k]
		if len(times) == 0 {
			continue
		}
		stored++
		var repl, storedRepl uint64
		var lastStore time.Time
		deps, next := 0, 0
		for _, sp := range ss {
			// Stores up to this span: they cover what was seen before it.
			for ; next < len(times) && !times[next].After(sp.at); next++ {
				lastStore, storedRepl = times[next], repl
			}
			switch {
			case sp.replacements > repl:
				// A new value: reset what belonged to the old one.
				repl, deps = sp.replacements, sp.deps
			case sp.replacements == repl && sp.deps > deps:
				if !lastStore.IsZero() && storedRepl == repl {
					growth = append(growth, depsGrowth{k, lastStore, deps, sp.deps, sp.at.Sub(lastStore)})
				}
				deps = sp.deps
			}
		}
	}
	slices.SortFunc(growth, func(a, b depsGrowth) int {
		return cmp.Or(cmp.Compare(a.cache, b.cache), cmp.Compare(a.number, b.number), a.storedAt.Compare(b.storedAt))
	})
	return stored, growth, omitted
}

// lazyGap is the time from an entry's last lazy-evaluation span to its next
// call span.
type lazyGap struct {
	entryKey
	gap        time.Duration
	lazy, call entrySpan
	// unretained is whether the engine didn't yet keep the entry when the
	// lazy span came (no call span before it said retained).
	unretained bool
}

// atRisk is whether the gap can lose the entry's parts: the engine didn't
// keep the entry when the lazy span came, and the call span then says it
// does, so the entry is worth exporting only from there. A call span that
// says the entry isn't retained has nothing to lose.
func (g lazyGap) atRisk() bool { return g.unretained && g.call.retained }

// lazyToCall measures lead 2 for one service: for each call span preceded
// by lazy-evaluation spans with no call span between, the gap from the last
// of them.
func lazyToCall(recs []LogRecord) []lazyGap {
	var out []lazyGap
	for k, ss := range spansByEntry(recs) {
		var lastLazy entrySpan
		// retained is what the call span with the latest end time said, as
		// the tracker keeps it: a call span that ends earlier than one seen
		// doesn't override it, however late it arrives.
		retained := false
		var stateEnd int64
		for _, s := range ss {
			if !s.call {
				lastLazy = s
				continue
			}
			if !lastLazy.at.IsZero() {
				out = append(out, lazyGap{k, s.at.Sub(lastLazy.at), lastLazy, s, !retained})
				lastLazy = entrySpan{}
			}
			if s.end >= stateEnd {
				stateEnd, retained = s.end, s.retained
			}
		}
	}
	slices.SortFunc(out, func(a, b lazyGap) int { return cmp.Or(-cmp.Compare(a.gap, b.gap), cmp.Compare(a.number, b.number)) })
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
	var stored, total, omitted int
	var growth []depsGrowth
	var gaps []lazyGap
	for _, svc := range s.env.Services() {
		recs := svc.Logs.Since(0, "", nil)
		n, g, o := depsAfterStore(recs)
		stored += n
		omitted += o
		growth = append(growth, g...)
		gaps = append(gaps, lazyToCall(recs)...)
		for _, r := range recs {
			if r.Msg == "cache span" {
				total++
			}
		}
	}
	reportLeads(t, total, stored, omitted, growth, gaps)
}

// reportLeads logs the measurements and checks them.
func reportLeads(t *testing.T, spans, stored, omitted int, growth []depsGrowth, gaps []lazyGap) {
	t.Helper()
	t.Logf("lead 1: %d cache spans; %d stored values named by them; %d later spans reported a longer deps list at the stored replacement count", spans, stored, len(growth))
	for _, g := range growth {
		t.Logf("lead 1: entry %d of %s: deps %d -> %d, %s after the value was stored", g.number, g.cache, g.before, g.after, g.late.Round(time.Millisecond))
	}
	Check(t, omitted == 0, "lead 1: every stored root was logged (%d omitted)", omitted)
	Check(t, stored > 0, "lead 1: the run stored values the spans name (%d), so the measurement saw something", stored)
	Check(t, len(growth) == 0, "lead 1: no stored value's deps grew at its replacement count (%d did)", len(growth))

	var maxAll, maxOpen time.Duration
	open := 0
	for _, g := range gaps {
		maxAll = max(maxAll, g.gap)
		if g.atRisk() {
			open++
			maxOpen = max(maxOpen, g.gap)
		}
		if g.gap >= time.Second {
			t.Logf("lead 2: gap %s, entry %d of %s: lazy %q at %s (session %s, retained %v) -> call %q at %s (session %s, retained %v, replacements %d); engine didn't yet keep it: %v",
				g.gap.Round(time.Millisecond), g.number, g.cache,
				g.lazy.name, g.lazy.at.UTC().Format("15:04:05.000"), g.lazy.session, g.lazy.retained,
				g.call.name, g.call.at.UTC().Format("15:04:05.000"), g.call.session, g.call.retained, g.call.replacements, g.unretained)
		}
	}
	t.Logf("lead 2: %d lazy-evaluation-to-call gaps (%d at risk: the engine didn't yet keep the entry, and the call span says it does); largest %s overall, %s for those at risk; ForgetAfter %s",
		len(gaps), open, maxAll.Round(time.Millisecond), maxOpen.Round(time.Millisecond), leadsBound)
	// The gaps at risk are rare (none in some runs), so the run saw
	// something when it has gaps at all.
	Check(t, len(gaps) > 0, "lead 2: the run had lazy-evaluation spans before call spans (%d gaps), so the measurement saw something", len(gaps))
	Check(t, maxOpen < leadsBound, "lead 2: the largest gap from a lazy-evaluation span to the call span that retained an entry the engine didn't yet keep is %s, under ForgetAfter %s", maxOpen.Round(time.Millisecond), leadsBound)
}
