package importer

import (
	"context"
	"slices"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/snapshots"
)

var metaPart = dagql.PersistedPartAddress{Part: "execMeta"}

// partKeys are a list of parts by metastore.PartKey.
func partKeys(parts ...dagql.PersistedPartAddress) []string {
	var out []string
	for _, p := range parts {
		out = append(out, metastore.PartKey(p))
	}
	return out
}

// valueBundle is an engine's export of one value, sender 10, as a root,
// with an output of each part, each with a layer of its own.
func valueBundle(parts ...dagql.PersistedPartAddress) *dagql.ValueBundle {
	b := &dagql.ValueBundle{
		Version: metastore.BundleVersion,
		Roots:   []dagql.TransferredRoot{{Ordinal: 1}},
		Values:  []dagql.TransferredValue{{Ordinal: 1, SenderNumber: 10, Record: dagql.PersistedRecord{ResultID: 1, Call: &dagql.ResultCall{Field: "v"}}}},
	}
	for _, p := range parts {
		b.Outputs = append(b.Outputs, dagql.TransferredOutput{
			Ordinal: 1,
			Address: p,
			State:   "completed",
			Value:   &dagql.SnapshotValue{Kind: "directory", Path: "/"},
			Chain:   &dagql.OfferedChain{Layers: []snapshots.ExportLayer{layer(string(p.Part))}},
		})
	}
	return b
}

// landComplete answers a merge as an engine that lands the value on 101
// and holds complete the parts it already had there, as dagql's merge keeps
// an unexpired value it already has: here, fs.
func landComplete(m protocol.Merge) (any, protocol.Type) {
	merged := protocol.Merged{Generation: 1}
	for _, v := range m.Bundle.Values {
		merged.Values = append(merged.Values, protocol.MergedValue{
			Ordinal: v.Ordinal, Number: 100 + uint64(v.Ordinal), Deps: []uint64{},
			Complete: []dagql.PersistedPartAddress{fsPart}, Offered: []dagql.PersistedPartAddress{},
		})
	}
	for _, r := range m.Bundle.Roots {
		merged.Retained = append(merged.Retained, protocol.RetainedRoot{Ordinal: r.Ordinal, ExpiresAtUnix: r.ExpiresAtUnix})
	}
	return merged, protocol.TypeMerged
}

// Check 4 of zerobull-consulting/dagger#133: engine A stores V with fs, and
// engine B, empty, imports it. A then stores V with fs and execMeta, which
// replaces the record (check 1). B's next sync merges the newer record's
// bundle and offers B the part it lacks, execMeta, and only that. B's own
// export of V with both parts then doesn't replace A's record: it holds no
// more.
func TestSyncRicherRecordOffersMissingPart(t *testing.T) {
	ctx := context.Background()
	f, _ := newImportsFixture(t, metastore.NewMemObjects())
	first := f.store(valueBundle(fsPart), "cache-a")
	v := first.Numbers[1]
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landComplete, onOffer: answerAll(protocol.OfferAccepted)}
	if _, err := f.im.Sync(ctx, eng); err != nil {
		t.Fatal(err)
	}
	if merges, offers := eng.counts(); merges != 1 || offers != 0 {
		t.Fatalf("first sync: %d merges, %d offers", merges, offers)
	}

	second := f.store(valueBundle(fsPart, metaPart), "cache-a")
	if second.Numbers[1] != v || second.Index.Entries[v].Bundle != second.BundleID {
		t.Fatalf("A's record with more parts didn't replace the first: %+v", second.Index.Entries[v])
	}
	res, err := f.im.Sync(ctx, eng)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Merged) != 1 || res.Merged[0].Bundle != second.BundleID {
		t.Fatalf("second sync merged %+v, want the newer record's bundle", res.Merged)
	}
	if len(eng.offers) != 1 || len(eng.offers[0].Items) != 1 {
		t.Fatalf("offers %+v", eng.offers)
	}
	item := eng.offers[0].Items[0]
	if item.Number != 101 || len(item.Offers) != 1 || metastore.PartKey(item.Offers[0].Offer.Address) != metastore.PartKey(metaPart) || item.Offers[0].CloudNumber != v {
		t.Fatalf("offered %+v, want execMeta alone to 101", item)
	}
	st, _ := f.im.State("cache-b")
	if got := st.Values[v]; !slices.Equal(partKeys(got.Complete...), partKeys(fsPart)) || !slices.Equal(partKeys(got.Offered...), partKeys(metaPart)) {
		t.Fatalf("B's value %+v", got)
	}

	// B's export of V, now with both parts.
	b := valueBundle(fsPart, metaPart)
	b.Values[0].SenderNumber = 101
	rec, err := f.meta.Record(ctx, scope, b, "cache-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e := rec.Index.Entries[v]; rec.Numbers[1] != v || e.Bundle != second.BundleID || e.Source.CacheID != "cache-a" {
		t.Fatalf("B's export of V with as many parts replaced A's record: %+v (number %d)", e, rec.Numbers[1])
	}
}

// Check 5 of zerobull-consulting/dagger#133, the service's side: B holds V
// as imported with fs only, and the store has V with fs and execMeta. The
// importer offers execMeta; when the engine can't take it (unavailable, or
// invalid), the sync doesn't fail and nothing else is asked of B: B computes
// the part itself, as a build needs it. What B's build does then is the
// engine's (the e2e's parts scenario reads a partial import on a real
// engine).
func TestSyncPartialImportOfferRefused(t *testing.T) {
	for _, outcome := range []protocol.OfferOutcome{protocol.OfferUnavailable, protocol.OfferInvalid} {
		ctx := context.Background()
		f, _ := newImportsFixture(t, metastore.NewMemObjects())
		rec := f.store(valueBundle(fsPart, metaPart), "cache-a")
		eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landComplete, onOffer: answerAll(outcome)}
		for i := 0; i < DefaultOfferAttempts+1; i++ {
			if _, err := f.im.Sync(ctx, eng); err != nil {
				t.Fatalf("%s: sync %d: %v", outcome, i, err)
			}
		}
		merges, offers := eng.counts()
		want := 1
		if outcome == protocol.OfferUnavailable {
			want = DefaultOfferAttempts
		}
		if merges != 1 || offers != want {
			t.Fatalf("%s: %d merges, %d offers, want 1 and %d", outcome, merges, offers, want)
		}
		for _, o := range eng.offers {
			for _, item := range o.Items {
				for _, p := range item.Offers {
					if metastore.PartKey(p.Offer.Address) != metastore.PartKey(metaPart) {
						t.Fatalf("%s: offered %+v, which B has", outcome, p.Offer.Address)
					}
				}
			}
		}
		st, _ := f.im.State("cache-b")
		if got := st.Values[rec.Numbers[1]]; !slices.Equal(partKeys(got.Complete...), partKeys(fsPart)) || len(got.Offered) != 0 || st.Pending != 0 {
			t.Fatalf("%s: B's value %+v, pending %d", outcome, got, st.Pending)
		}
	}
}
