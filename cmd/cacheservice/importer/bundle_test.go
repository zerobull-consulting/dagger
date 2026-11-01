package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/snapshots"
)

var fsPart = dagql.PersistedPartAddress{Part: "fs"}

// testBundleID is a valid bundle ID made from s.
func testBundleID(s string) string {
	return digest.FromString(s).Encoded()
}

func layer(s string) snapshots.ExportLayer {
	data := strings.Repeat(s, 64)
	return snapshots.ExportLayer{Descriptor: ocispecs.Descriptor{
		MediaType: ocispecs.MediaTypeImageLayerGzip,
		Digest:    digest.FromString(data),
		Size:      int64(len(data)),
	}}
}

// chainBundle is an engine's export of a chain of entries, each depending on
// the one before, numbered from first. Each has an fs output with one layer
// of its own on top of the ones before. The last entry is the root.
func chainBundle(first uint64, recipes ...string) *dagql.ValueBundle {
	b := &dagql.ValueBundle{Version: dagql.ValueBundleVersion}
	var layers []snapshots.ExportLayer
	for i, r := range recipes {
		o := dagql.TransferOrdinal(i + 1)
		v := dagql.TransferredValue{
			Ordinal:      o,
			SenderNumber: first + uint64(i),
			Record:       dagql.PersistedRecord{ResultID: uint64(o), Call: &dagql.ResultCall{Field: r}},
		}
		if i > 0 {
			v.DependencyIDs = []uint64{uint64(o - 1)}
		}
		b.Values = append(b.Values, v)
		layers = append(layers, layer(r))
		b.Outputs = append(b.Outputs, dagql.TransferredOutput{
			Ordinal: o,
			Address: fsPart,
			State:   "completed",
			Value:   &dagql.SnapshotValue{Kind: "directory", Path: "/"},
			Chain:   &dagql.OfferedChain{Layers: slices.Clone(layers)},
		})
	}
	b.Roots = []dagql.TransferredRoot{{Ordinal: dagql.TransferOrdinal(len(recipes))}}
	return b
}

func record(t *testing.T, idx *metastore.Index, id string, b *dagql.ValueBundle, cacheID string) map[dagql.TransferOrdinal]uint64 {
	t.Helper()
	numbers, err := idx.Record(id, b, cacheID, 1)
	if err != nil {
		t.Fatal(err)
	}
	return numbers
}

func testSigner(_ context.Context, d digest.Digest) (dagql.BlobAddress, error) {
	return dagql.BlobAddress{URL: "https://store.test/" + d.Encoded(), ExpiresAtUnix: 4000000000}, nil
}

func TestPlan(t *testing.T) {
	idx := metastore.NewIndex()
	a1, a2, b1 := testBundleID("a1"), testBundleID("a2"), testBundleID("b1")
	record(t, idx, a1, chainBundle(10, "base", "exec"), "cache-a")
	record(t, idx, a2, chainBundle(20, "other"), "cache-a")
	record(t, idx, b1, chainBundle(30, "mine"), "cache-b")

	got := Plan(idx, "cache-b", 100)
	want := []Candidate{{Bundle: a1, Roots: []uint64{2}}, {Bundle: a2, Roots: []uint64{3}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plan for cache-b: %+v, want %+v", got, want)
	}
	// An engine's own exports are left out.
	if got := Plan(idx, "cache-a", 100); len(got) != 1 || got[0].Bundle != b1 {
		t.Fatalf("plan for cache-a: %+v", got)
	}
	// Expired roots and entries are left out.
	idx.Roots[2] = metastore.Root{ExpiresAtUnix: 50}
	idx.Entries[3].ExpiresAtUnix = 100
	if got := Plan(idx, "cache-b", 100); len(got) != 0 {
		t.Fatalf("plan with expired roots: %+v", got)
	}
	// Not yet expired.
	if got := Plan(idx, "cache-b", 49); len(got) != 2 {
		t.Fatalf("plan before expiry: %+v", got)
	}
}

// A root whose record moved to a later bundle is merged from that bundle.
func TestPlanFollowsCurrentRecord(t *testing.T) {
	idx := metastore.NewIndex()
	first, second := testBundleID("first"), testBundleID("second")
	record(t, idx, first, chainBundle(10, "base", "exec"), "cache-a")
	// The same root again, in a later export with a dependent.
	record(t, idx, second, chainBundle(10, "base", "exec", "test"), "cache-a")
	got := Plan(idx, "cache-b", 0)
	// Both of the second bundle's roots-so-far: exec stays a root, and
	// test is one too; both records are now in the second bundle.
	want := []Candidate{{Bundle: second, Roots: []uint64{2, 3}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plan: %+v, want %+v", got, want)
	}
}

func TestBuildMerge(t *testing.T) {
	idx := metastore.NewIndex()
	id := testBundleID("a")
	stored := chainBundle(10, "base", "exec")
	numbers := record(t, idx, id, stored, "cache-a")
	idx.Roots[numbers[2]] = metastore.Root{ExpiresAtUnix: 12345}
	before, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}

	mb, err := BuildMerge(context.Background(), idx, id, stored, []uint64{numbers[2]}, testSigner, "team")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(stored)
	if string(before) != string(after) {
		t.Fatal("the stored bundle changed")
	}
	b := mb.Bundle
	// Service numbers as sender numbers.
	for _, v := range b.Values {
		if v.SenderNumber != numbers[v.Ordinal] || mb.Numbers[v.Ordinal] != numbers[v.Ordinal] {
			t.Fatalf("value %d: sender %d, want %d", v.Ordinal, v.SenderNumber, numbers[v.Ordinal])
		}
	}
	// The index's roots and expiries.
	if !reflect.DeepEqual(b.Roots, []dagql.TransferredRoot{{Ordinal: 2, ExpiresAtUnix: 12345}}) {
		t.Fatalf("roots %+v", b.Roots)
	}
	// An address per layer, and the renewal key.
	for _, o := range b.Outputs {
		if o.Chain.RenewalKey != "team" || len(o.Chain.Addresses) != len(o.Chain.Layers) {
			t.Fatalf("output %d: chain %+v", o.Ordinal, o.Chain)
		}
		for _, l := range o.Chain.Layers {
			if addr := o.Chain.Addresses[l.Descriptor.Digest]; addr.URL != "https://store.test/"+l.Descriptor.Digest.Encoded() {
				t.Fatalf("output %d layer %s: address %+v", o.Ordinal, l.Descriptor.Digest, addr)
			}
		}
	}
	// The records are the stored ones.
	if !reflect.DeepEqual(b.Values[1].Record, stored.Values[1].Record) || !slices.Equal(b.Values[1].DependencyIDs, []uint64{1}) {
		t.Fatalf("record changed: %+v", b.Values[1])
	}
}

// A value only a dropped root needed gets a placeholder number no stored
// entry has; one a merged root needs must be stored.
func TestBuildMergeDroppedValues(t *testing.T) {
	idx := metastore.NewIndex()
	id := testBundleID("a")
	// Two roots: exec (2, on base 1) and lone (3).
	stored := chainBundle(10, "base", "exec")
	stored.Values = append(stored.Values, dagql.TransferredValue{
		Ordinal: 3, SenderNumber: 50,
		Record: dagql.PersistedRecord{ResultID: 3, Call: &dagql.ResultCall{Field: "lone"}},
	})
	stored.Roots = append(stored.Roots, dagql.TransferredRoot{Ordinal: 3, ExpiresAtUnix: 10})
	numbers := record(t, idx, id, stored, "cache-a")
	if dropped := idx.DropExpired(20); !slices.Equal(dropped, []uint64{numbers[3]}) {
		t.Fatalf("dropped %v", dropped)
	}
	mb, err := BuildMerge(context.Background(), idx, id, stored, []uint64{numbers[2]}, testSigner, "team")
	if err != nil {
		t.Fatal(err)
	}
	if got := mb.Bundle.Values[2].SenderNumber; got < idx.NextNumber {
		t.Fatalf("placeholder %d is below the next number %d", got, idx.NextNumber)
	}
	if _, ok := mb.Numbers[3]; ok {
		t.Fatal("a dropped value has a service number")
	}

	// Drop base's mapping by hand: exec needs it.
	delete(idx.Caches["cache-a"].Numbers, 10)
	if _, err := BuildMerge(context.Background(), idx, id, stored, []uint64{numbers[2]}, testSigner, "team"); err == nil || !strings.Contains(err.Error(), "no longer stored") {
		t.Fatalf("err %v", err)
	}
}

func TestBuildMergeRefuses(t *testing.T) {
	idx := metastore.NewIndex()
	a, b := testBundleID("a"), testBundleID("b")
	sa, sb := chainBundle(10, "base"), chainBundle(20, "other")
	na := record(t, idx, a, sa, "cache-a")
	nb := record(t, idx, b, sb, "cache-b")
	ctx := context.Background()
	for name, tc := range map[string]struct {
		roots []uint64
		want  string
	}{
		"no roots":       {nil, "no roots"},
		"unknown root":   {[]uint64{99}, "not stored"},
		"another bundle": {[]uint64{nb[1]}, "is in bundle"},
		"ok":             {[]uint64{na[1]}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := BuildMerge(ctx, idx, a, sa, tc.roots, testSigner, "team")
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want %q", err, tc.want)
			}
		})
	}
	failing := func(context.Context, digest.Digest) (dagql.BlobAddress, error) {
		return dagql.BlobAddress{}, fmt.Errorf("no credentials")
	}
	if _, err := BuildMerge(ctx, idx, a, sa, []uint64{na[1]}, failing, "team"); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("err %v", err)
	}
}
