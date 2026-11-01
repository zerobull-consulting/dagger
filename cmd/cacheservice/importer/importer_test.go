package importer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/snapshots"
)

const scope = "team"

// scriptedEngine answers merge and offer as a test scripts them.
type scriptedEngine struct {
	cacheID    string
	generation uint64

	mu      sync.Mutex
	merges  []protocol.Merge
	offers  []protocol.Offer
	onMerge func(protocol.Merge) (any, protocol.Type)
	onOffer func(protocol.Offer) (any, protocol.Type)
	fail    error
}

func (e *scriptedEngine) CacheID() string    { return e.cacheID }
func (e *scriptedEngine) Generation() uint64 { return e.generation }

func (e *scriptedEngine) Request(_ context.Context, typ protocol.Type, body json.RawMessage) (protocol.Envelope, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fail != nil {
		return protocol.Envelope{}, e.fail
	}
	var reply any
	var rtyp protocol.Type
	switch typ {
	case protocol.TypeMerge:
		var m protocol.Merge
		if err := json.Unmarshal(body, &m); err != nil {
			return protocol.Envelope{}, err
		}
		e.merges = append(e.merges, m)
		reply, rtyp = e.onMerge(m)
	case protocol.TypeOffer:
		var o protocol.Offer
		if err := json.Unmarshal(body, &o); err != nil {
			return protocol.Envelope{}, err
		}
		e.offers = append(e.offers, o)
		reply, rtyp = e.onOffer(o)
	default:
		return protocol.Envelope{}, errors.New("unexpected request " + string(typ))
	}
	return protocol.NewEnvelope(1, 1, rtyp, reply)
}

func (e *scriptedEngine) counts() (merges, offers int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.merges), len(e.offers)
}

// landAll answers a merge as an engine that lands value i on number 100+i,
// with its fs part offered, or, withholding, neither offered nor complete.
func landAll(withhold bool) func(protocol.Merge) (any, protocol.Type) {
	return func(m protocol.Merge) (any, protocol.Type) {
		// As the engine does: every value must be in the roots' closure,
		// expired roots included.
		if got := closure(&m.Bundle, m.Bundle.Roots); len(got) != len(m.Bundle.Values) {
			return protocol.Error{Message: "bundle contains rows outside its root closure"}, protocol.TypeError
		}
		merged := protocol.Merged{Generation: 1, EngineTime: time.Now().UnixNano()}
		for _, v := range m.Bundle.Values {
			mv := protocol.MergedValue{Ordinal: v.Ordinal, Number: 100 + uint64(v.Ordinal), Deps: []uint64{}, Complete: []dagql.PersistedPartAddress{}, Offered: []dagql.PersistedPartAddress{}}
			if !withhold {
				mv.Offered = append(mv.Offered, fsPart)
			}
			merged.Values = append(merged.Values, mv)
		}
		for _, r := range m.Bundle.Roots {
			merged.Retained = append(merged.Retained, protocol.RetainedRoot{Ordinal: r.Ordinal, ExpiresAtUnix: r.ExpiresAtUnix})
		}
		return merged, protocol.TypeMerged
	}
}

// answerAll answers every offered part with outcome.
func answerAll(outcome protocol.OfferOutcome) func(protocol.Offer) (any, protocol.Type) {
	return func(o protocol.Offer) (any, protocol.Type) {
		var out protocol.Offered
		for _, item := range o.Items {
			answer := protocol.OfferedItem{Number: item.Number, Replacements: 1}
			for _, p := range item.Offers {
				answer.Parts = append(answer.Parts, protocol.OfferedPart{Address: p.Offer.Address, Outcome: outcome})
			}
			out.Items = append(out.Items, answer)
		}
		return out, protocol.TypeOffered
	}
}

type fixture struct {
	t     *testing.T
	im    *Importer
	meta  *metastore.Store
	blobs *blobstore.MemStore
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	meta, err := metastore.New(metastore.Config{Objects: metastore.NewMemObjects()})
	if err != nil {
		t.Fatal(err)
	}
	blobs := blobstore.NewMemStore("http://blobs.test")
	im, err := New(Config{Blobs: blobs, Meta: meta, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, im: im, meta: meta, blobs: blobs}
}

// store records a bundle as engine cacheID's export.
func (f *fixture) store(b *dagql.ValueBundle, cacheID string) metastore.Recorded {
	f.t.Helper()
	rec, err := f.meta.Record(context.Background(), scope, b, cacheID, 1)
	if err != nil {
		f.t.Fatal(err)
	}
	return rec
}

func TestSyncMergesAndRecords(t *testing.T) {
	f := newFixture(t)
	rec := f.store(chainBundle(10, "base", "exec"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false), onOffer: answerAll(protocol.OfferAccepted)}
	ctx := context.Background()

	res, err := f.im.Sync(ctx, eng)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Merged) != 1 || len(res.Failed) != 0 {
		t.Fatalf("result %+v", res)
	}
	m := res.Merged[0]
	root := rec.Numbers[2]
	if m.Bundle != rec.BundleID || len(m.Roots) != 1 || m.Roots[0] != root || m.Values != 2 {
		t.Fatalf("merged %+v", m)
	}
	if _, ok := m.Retained[root]; !ok {
		t.Fatalf("root %d not retained: %+v", root, m.Retained)
	}
	// What was sent: service numbers and GET addresses.
	sent := eng.merges[0].Bundle
	if sent.Values[1].SenderNumber != root || sent.Outputs[1].Chain.RenewalKey != scope {
		t.Fatalf("sent %+v", sent)
	}
	for _, o := range sent.Outputs {
		for _, l := range o.Chain.Layers {
			if !strings.HasPrefix(o.Chain.Addresses[l.Descriptor.Digest].URL, "http://blobs.test/mem/") {
				t.Fatalf("output %d: addresses %+v", o.Ordinal, o.Chain.Addresses)
			}
		}
	}
	// What the engine kept, by service number.
	st, ok := f.im.State("cache-b")
	if !ok || len(st.Values) != 2 || st.Values[root].Number != 102 || len(st.Values[root].Offered) != 1 {
		t.Fatalf("state %+v", st)
	}
	// Every part was offered by the merge: no offer.
	if _, offers := eng.counts(); offers != 0 {
		t.Fatalf("%d offers", offers)
	}

	// Nothing new: no merge.
	if res, err := f.im.Sync(ctx, eng); err != nil || len(res.Merged) != 0 {
		t.Fatalf("second sync: %+v, %v", res, err)
	}
	if merges, _ := eng.counts(); merges != 1 {
		t.Fatalf("%d merges", merges)
	}

	// A new root in a new bundle: merged.
	f.store(chainBundle(30, "other"), "cache-c")
	if res, err := f.im.Sync(ctx, eng); err != nil || len(res.Merged) != 1 {
		t.Fatalf("third sync: %+v, %v", res, err)
	}

	// A new generation starts over.
	eng.generation = 2
	if res, err := f.im.Sync(ctx, eng); err != nil || len(res.Merged) != 2 {
		t.Fatalf("after restart: %+v, %v", res, err)
	}
}

// The exporting engine is not merged its own export.
func TestSyncSkipsOwnExports(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "base"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-a", generation: 1, onMerge: landAll(false)}
	res, err := f.im.Sync(context.Background(), eng)
	if err != nil || len(res.Merged) != 0 {
		t.Fatalf("%+v, %v", res, err)
	}
	if merges, _ := eng.counts(); merges != 0 {
		t.Fatalf("%d merges", merges)
	}
}

// Parts the merged entries lack are offered, by the engine's numbers, and
// the outcomes are recorded.
func TestSyncOffersMissingParts(t *testing.T) {
	f := newFixture(t)
	rec := f.store(chainBundle(10, "base", "exec"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(true), onOffer: answerAll(protocol.OfferAccepted)}
	res, err := f.im.Sync(context.Background(), eng)
	if err != nil {
		t.Fatal(err)
	}
	if res.Offers.Accepted != 2 {
		t.Fatalf("offers %+v", res.Offers)
	}
	offer := eng.offers[0]
	if len(offer.Items) != 2 {
		t.Fatalf("items %+v", offer.Items)
	}
	for i, item := range offer.Items {
		want := uint64(101 + i)
		if item.Number != want || len(item.Offers) != 1 {
			t.Fatalf("item %d: %+v", i, item)
		}
		p := item.Offers[0]
		if p.CloudNumber != rec.Numbers[dagql.TransferOrdinal(i+1)] || !p.CloudStored || p.Offer.Chain.RenewalKey != scope || len(p.Offer.Chain.Addresses) != i+1 {
			t.Fatalf("item %d: part %+v", i, p)
		}
	}
	st, _ := f.im.State("cache-b")
	if st.Offers.Accepted != 2 || len(st.Values[rec.Numbers[2]].Offered) != 1 || st.Pending != 0 {
		t.Fatalf("state %+v", st)
	}
	// Accepted: not offered again.
	if _, err := f.im.Sync(context.Background(), eng); err != nil {
		t.Fatal(err)
	}
	if _, offers := eng.counts(); offers != 1 {
		t.Fatalf("%d offers", offers)
	}
}

func TestSyncOfferOutcomes(t *testing.T) {
	for _, tc := range []struct {
		outcome protocol.OfferOutcome
		// tries is how many offers are sent over several syncs.
		tries int
	}{
		{protocol.OfferAccepted, 1},
		{protocol.OfferAlreadyComplete, 1},
		{protocol.OfferInvalid, 1},
		{protocol.OfferExecutionStarted, DefaultOfferAttempts},
		{protocol.OfferUnavailable, DefaultOfferAttempts},
	} {
		t.Run(string(tc.outcome), func(t *testing.T) {
			f := newFixture(t)
			f.store(chainBundle(10, "base"), "cache-a")
			eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(true), onOffer: answerAll(tc.outcome)}
			for range DefaultOfferAttempts + 2 {
				if _, err := f.im.Sync(context.Background(), eng); err != nil {
					t.Fatal(err)
				}
			}
			if _, offers := eng.counts(); offers != tc.tries {
				t.Fatalf("%d offers, want %d", offers, tc.tries)
			}
			st, _ := f.im.State("cache-b")
			if st.Pending != 0 {
				t.Fatalf("%d still pending", st.Pending)
			}
			v := st.Values[1]
			switch tc.outcome {
			case protocol.OfferAccepted:
				if len(v.Offered) != 1 {
					t.Fatalf("value %+v", v)
				}
			case protocol.OfferAlreadyComplete:
				if len(v.Complete) != 1 {
					t.Fatalf("value %+v", v)
				}
			}
		})
	}
}

// An offered entry the engine no longer has is forgotten.
func TestSyncOfferGone(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "base"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(true), onOffer: func(o protocol.Offer) (any, protocol.Type) {
		return protocol.Offered{Items: []protocol.OfferedItem{{Number: o.Items[0].Number, Gone: true}}}, protocol.TypeOffered
	}}
	res, err := f.im.Sync(context.Background(), eng)
	if err != nil || res.Offers.Gone != 1 {
		t.Fatalf("%+v, %v", res, err)
	}
	if st, _ := f.im.State("cache-b"); len(st.Values) != 0 {
		t.Fatalf("values %+v", st.Values)
	}
}

// A refused merge is tried again at later syncs, up to MergeAttempts.
func TestSyncMergeFailures(t *testing.T) {
	f := newFixture(t)
	rec := f.store(chainBundle(10, "base"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: func(protocol.Merge) (any, protocol.Type) {
		return protocol.Error{Message: "bundle has no roots"}, protocol.TypeError
	}}
	for range DefaultMergeAttempts + 2 {
		res, err := f.im.Sync(context.Background(), eng)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Failed) > 0 && (res.Failed[0].Bundle != rec.BundleID || !strings.Contains(res.Failed[0].Error, "no roots")) {
			t.Fatalf("failed %+v", res.Failed)
		}
	}
	if merges, _ := eng.counts(); merges != DefaultMergeAttempts {
		t.Fatalf("%d merges, want %d", merges, DefaultMergeAttempts)
	}
	st, _ := f.im.State("cache-b")
	if st.Failures[rec.BundleID+"/cache-a"] != DefaultMergeAttempts {
		t.Fatalf("failures %+v", st.Failures)
	}
}

// A connection failure ends the sync with an error, and nothing is recorded
// as failed.
func TestSyncConnectionFailure(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "base"), "cache-a")
	f.store(chainBundle(20, "other"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, fail: errors.New("connection closed")}
	res, err := f.im.Sync(context.Background(), eng)
	if err == nil || len(res.Failed) != 0 {
		t.Fatalf("%+v, %v", res, err)
	}
	if st, _ := f.im.State("cache-b"); len(st.Failures) != 0 {
		t.Fatalf("failures %+v", st.Failures)
	}
}

// A merge over the message limit fails without being sent.
func TestSyncTooLarge(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "base"), "cache-a")
	f.im.cfg.MaxMessageBytes = 256
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false)}
	res, err := f.im.Sync(context.Background(), eng)
	if err != nil || len(res.Failed) != 1 || !strings.Contains(res.Failed[0].Error, protocol.TooLargeMessage) {
		t.Fatalf("%+v, %v", res, err)
	}
	if merges, _ := eng.counts(); merges != 0 {
		t.Fatalf("%d merges", merges)
	}
}

// Pruned entries are not offered parts, and a wiped cache is forgotten.
func TestPrunedAndForget(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "base", "exec"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(true), onOffer: answerAll(protocol.OfferExecutionStarted)}
	if _, err := f.im.Sync(context.Background(), eng); err != nil {
		t.Fatal(err)
	}
	st, _ := f.im.State("cache-b")
	if st.Pending != 2 {
		t.Fatalf("pending %d", st.Pending)
	}
	f.im.Pruned("cache-b", 1, []uint64{101})
	st, _ = f.im.State("cache-b")
	if st.Pending != 1 || len(st.Values) != 1 {
		t.Fatalf("after prune: %+v", st)
	}
	f.im.Forget("cache-b")
	if _, ok := f.im.State("cache-b"); ok {
		t.Fatal("state kept after forget")
	}
}

func TestRenew(t *testing.T) {
	f := newFixture(t)
	present := f.blobs.Put([]byte("layer"))
	missing := digest.FromString("missing")
	layers := func(ds ...digest.Digest) []snapshots.ExportLayer {
		var out []snapshots.ExportLayer
		for _, d := range ds {
			l := layer("x")
			l.Descriptor.Digest = d
			out = append(out, l)
		}
		return out
	}
	ctx := context.Background()
	got := f.im.Renew(ctx, protocol.Renew{RenewalKey: scope, Layers: layers(present), NeededBlob: present})
	if got.Unavailable || got.Addresses[present].URL != "http://blobs.test/mem/"+present.String() || got.Addresses[present].ExpiresAtUnix == 0 {
		t.Fatalf("renewed %+v", got)
	}
	for name, req := range map[string]protocol.Renew{
		"missing layer": {RenewalKey: scope, Layers: layers(present, missing)},
		"other key":     {RenewalKey: "other", Layers: layers(present)},
		"no layers":     {RenewalKey: scope},
		"bad digest":    {RenewalKey: scope, Layers: layers("sha256:nope")},
	} {
		if got := f.im.Renew(ctx, req); !got.Unavailable || len(got.Addresses) != 0 {
			t.Fatalf("%s: renewed %+v", name, got)
		}
	}
}

func TestNewRefuses(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("no stores accepted")
	}
	meta, _ := metastore.New(metastore.Config{Objects: metastore.NewMemObjects()})
	if _, err := New(Config{Blobs: blobstore.NewMemStore(""), Meta: meta, Scope: "blobs"}); err == nil {
		t.Fatal("bad scope accepted")
	}
}

// A bundle with a dropped root still merges: the engine sees every value in
// its roots' closure, and only the live root is reported.
func TestSyncPartlyDroppedBundle(t *testing.T) {
	f := newFixture(t)
	b := chainBundle(10, "base", "exec")
	b.Values = append(b.Values, dagql.TransferredValue{
		Ordinal: 3, SenderNumber: 50,
		Record: dagql.PersistedRecord{ResultID: 3, Call: &dagql.ResultCall{Field: "lone"}},
	})
	b.Roots = append(b.Roots, dagql.TransferredRoot{Ordinal: 3, ExpiresAtUnix: 10})
	rec := f.store(b, "cache-a")
	if _, err := f.meta.DropExpired(context.Background(), scope, 20); err != nil {
		t.Fatal(err)
	}
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false)}
	res, err := f.im.Sync(context.Background(), eng)
	if err != nil || len(res.Merged) != 1 || len(res.Failed) != 0 {
		t.Fatalf("%+v, %v", res, err)
	}
	m := res.Merged[0]
	if len(m.Retained) != 1 || len(m.Expired) != 0 {
		t.Fatalf("merged %+v", m)
	}
	if _, ok := m.Retained[rec.Numbers[2]]; !ok {
		t.Fatalf("retained %+v", m.Retained)
	}
}

// A merged reply that arrives after the cache was wiped is discarded.
func TestSyncDiscardsReplyAfterReset(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "base"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1}
	eng.onMerge = func(m protocol.Merge) (any, protocol.Type) {
		f.im.Forget("cache-b")
		return landAll(false)(m)
	}
	res, err := f.im.Sync(context.Background(), eng)
	if !errors.Is(err, errStateReset) || len(res.Merged) != 0 {
		t.Fatalf("%+v, %v", res, err)
	}
	if st, ok := f.im.State("cache-b"); ok {
		t.Fatalf("state repopulated: %+v", st)
	}
}

// Offers over the message limit are split, not dropped.
func TestSyncSplitsLargeOffers(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "a", "b", "c", "d"), "cache-a")
	var sizes []int
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: func(m protocol.Merge) (any, protocol.Type) {
		// After the merge, room for some offers but not all four parts.
		f.im.cfg.MaxMessageBytes = 2200
		return landAll(true)(m)
	}, onOffer: func(o protocol.Offer) (any, protocol.Type) {
		data, _ := json.Marshal(o)
		sizes = append(sizes, len(data))
		return answerAll(protocol.OfferAccepted)(o)
	}}
	res, err := f.im.Sync(context.Background(), eng)
	if err != nil || len(res.Merged) != 1 {
		t.Fatalf("%+v, %v", res, err)
	}
	_, offers := eng.counts()
	if offers < 2 || res.Offers.Accepted != 4 {
		t.Fatalf("%d offers (%v bytes), %+v", offers, sizes, res.Offers)
	}
}

// State returns copies: changing them changes nothing the importer keeps.
func TestStateCopies(t *testing.T) {
	f := newFixture(t)
	f.store(chainBundle(10, "base"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false)}
	if _, err := f.im.Sync(context.Background(), eng); err != nil {
		t.Fatal(err)
	}
	st, _ := f.im.State("cache-b")
	st.Values[1].Offered[0] = dagql.PersistedPartAddress{Part: "changed"}
	for n := range st.Merged[0].Retained {
		st.Merged[0].Retained[n] = 99
	}
	again, _ := f.im.State("cache-b")
	if again.Values[1].Offered[0].Part != "fs" || again.Merged[0].Retained[1] == 99 {
		t.Fatalf("state aliased: %+v", again)
	}
}
