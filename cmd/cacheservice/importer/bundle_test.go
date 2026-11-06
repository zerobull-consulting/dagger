package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
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
	numbers, res, err := idx.RecordResolved(id, b, cacheID, 1)
	if err != nil {
		t.Fatal(err)
	}
	keepResolution(t, res)
	return numbers
}

// testResolutions are the resolutions record made, by ID: what Store.Record
// would have stored.
var testResolutions sync.Map

func keepResolution(t *testing.T, res *metastore.Resolution) {
	t.Helper()
	rid, _, err := metastore.ResolutionID(res)
	if err != nil {
		t.Fatal(err)
	}
	testResolutions.Store(rid, res)
}

// resolutionOf returns the resolution root's record names, as the importer
// reads it for a merge: nil when the root isn't stored or names none.
func resolutionOf(t *testing.T, idx *metastore.Index, root uint64) *metastore.Resolution {
	t.Helper()
	e := idx.Entries[root]
	if e == nil || e.Resolution == "" {
		return nil
	}
	res, ok := testResolutions.Load(e.Resolution)
	if !ok {
		t.Fatalf("root %d names resolution %s, which no record made", root, e.Resolution)
	}
	return res.(*metastore.Resolution)
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
	want := []Candidate{
		{Bundle: a1, Source: "cache-a", Resolution: idx.Entries[2].Resolution, Roots: []uint64{2}},
		{Bundle: a2, Source: "cache-a", Resolution: idx.Entries[3].Resolution, Roots: []uint64{3}},
	}
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
	want := []Candidate{{Bundle: second, Source: "cache-a", Resolution: idx.Entries[3].Resolution, Roots: []uint64{2, 3}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plan: %+v, want %+v", got, want)
	}
}

// Identical exports from two caches share a bundle ID: each cache's roots are
// a candidate of their own, and each merges with its own numbers.
func TestPlanSharedBundle(t *testing.T) {
	idx := metastore.NewIndex()
	id := testBundleID("same")
	stored := chainBundle(10, "base")
	na := record(t, idx, id, stored, "cache-a")
	nb := record(t, idx, id, stored, "cache-b")
	got := Plan(idx, "cache-c", 0)
	want := []Candidate{
		{Bundle: id, Source: "cache-a", Resolution: idx.Entries[na[1]].Resolution, Roots: []uint64{na[1]}},
		{Bundle: id, Source: "cache-b", Resolution: idx.Entries[nb[1]].Resolution, Roots: []uint64{nb[1]}},
	}
	if want[0].Resolution == "" || want[0].Resolution == want[1].Resolution {
		t.Fatalf("the two caches' records name resolutions %q and %q, want one each", want[0].Resolution, want[1].Resolution)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plan: %+v, want %+v", got, want)
	}
	if got[0].Key() == got[1].Key() {
		t.Fatal("candidates share a key")
	}
	for _, c := range got {
		mb, err := BuildMerge(context.Background(), idx, c.Bundle, stored, resolutionOf(t, idx, c.Roots[0]), c.Roots, testSigner, "team")
		if err != nil {
			t.Fatal(err)
		}
		if mb.Bundle.Values[0].SenderNumber != c.Roots[0] {
			t.Fatalf("%s: sender %d, want %d", c.Source, mb.Bundle.Values[0].SenderNumber, c.Roots[0])
		}
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

	mb, err := BuildMerge(context.Background(), idx, id, stored, resolutionOf(t, idx, numbers[2]), []uint64{numbers[2]}, testSigner, "team")
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
	if !reflect.DeepEqual(b.Roots, []dagql.TransferredRoot{{Ordinal: 2, ExpiresAtUnix: 12345}}) || !reflect.DeepEqual(mb.Merged, map[dagql.TransferOrdinal]bool{2: true}) {
		t.Fatalf("roots %+v, merged %v", b.Roots, mb.Merged)
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
	mb, err := BuildMerge(context.Background(), idx, id, stored, resolutionOf(t, idx, numbers[2]), []uint64{numbers[2]}, testSigner, "team")
	if err != nil {
		t.Fatal(err)
	}
	// The dropped root is still sent, expired, so every value is in the
	// roots' closure, as the engine requires; only exec is merged.
	wantRoots := []dagql.TransferredRoot{{Ordinal: 2}, {Ordinal: 3, ExpiresAtUnix: expiredRoot}}
	if !reflect.DeepEqual(mb.Bundle.Roots, wantRoots) || !reflect.DeepEqual(mb.Merged, map[dagql.TransferOrdinal]bool{2: true}) {
		t.Fatalf("roots %+v, merged %v", mb.Bundle.Roots, mb.Merged)
	}
	if got := closure(&mb.Bundle, mb.Bundle.Roots); len(got) != len(mb.Bundle.Values) {
		t.Fatalf("closure %v of %d values", got, len(mb.Bundle.Values))
	}
	if got := mb.Bundle.Values[2].SenderNumber; got < idx.NextNumber {
		t.Fatalf("placeholder %d is below the next number %d", got, idx.NextNumber)
	}
	if _, ok := mb.Numbers[3]; ok {
		t.Fatal("a dropped value has a service number")
	}

	// Drop base's entry by hand, as DropExpired would if it had expired:
	// exec needs it. The resolution still names it, but a merge sends only
	// what the index holds.
	res := resolutionOf(t, idx, numbers[2])
	if n, _ := res.Number(1); n != numbers[1] {
		t.Fatalf("resolution gives base %d, want %d", n, numbers[1])
	}
	delete(idx.Entries, numbers[1])
	if _, err := BuildMerge(context.Background(), idx, id, stored, resolutionOf(t, idx, numbers[2]), []uint64{numbers[2]}, testSigner, "team"); err == nil || !strings.Contains(err.Error(), "no longer stored") {
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
	ra, rb := resolutionOf(t, idx, na[1]), resolutionOf(t, idx, nb[1])
	for name, tc := range map[string]struct {
		roots []uint64
		res   *metastore.Resolution
		want  string
	}{
		"no roots":           {nil, ra, "no roots"},
		"unknown root":       {[]uint64{99}, ra, "not stored"},
		"another bundle":     {[]uint64{nb[1]}, ra, "is in bundle"},
		"another resolution": {[]uint64{na[1]}, rb, "names resolution"},
		"no resolution":      {[]uint64{na[1]}, nil, "none was given"},
		"ok":                 {[]uint64{na[1]}, ra, ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := BuildMerge(ctx, idx, a, sa, tc.res, tc.roots, testSigner, "team")
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
	if _, err := BuildMerge(ctx, idx, a, sa, ra, []uint64{na[1]}, failing, "team"); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("err %v", err)
	}
}

// A live root whose dependency has expired is not planned: merging it would
// hand an engine the expired dependency, as DropExpired would drop the root.
func TestPlanSkipsRootWithExpiredDependency(t *testing.T) {
	idx := metastore.NewIndex()
	id := testBundleID("a")
	numbers := record(t, idx, id, chainBundle(10, "base", "exec", "test"), "cache-a")
	// base (1) expires; exec (2) and test (3), the root, depend on it.
	idx.Entries[numbers[1]].ExpiresAtUnix = 50
	if got := Plan(idx, "cache-b", 49); len(got) != 1 {
		t.Fatalf("before expiry: %+v", got)
	}
	if got := Plan(idx, "cache-b", 50); len(got) != 0 {
		t.Fatalf("after the dependency expired: %+v", got)
	}
}

// movedImportsFixture is #47's case: A exports base and exec; C has them
// merged, on its engine entries 6 and 7, and exports test (8) on them. C's
// bundle names base and exec by 6 and 7, which resolved to A's numbers when
// it was recorded.
func movedImportsFixture(t *testing.T) (idx *metastore.Index, cID string, cBundle *dagql.ValueBundle, want map[dagql.TransferOrdinal]uint64) {
	t.Helper()
	idx = metastore.NewIndex()
	aNumbers := record(t, idx, testBundleID("a"), chainBundle(10, "base", "exec"), "cache-a")
	baseN, execN := aNumbers[1], aNumbers[2]
	idx.RecordImports("cache-c", 1, []metastore.Import{{Number: baseN, Engine: 6}, {Number: execN, Engine: 7}})
	cID = testBundleID("c")
	cBundle = chainBundle(6, "base", "exec", "test")
	cNumbers := record(t, idx, cID, cBundle, "cache-c")
	if cNumbers[1] != baseN || cNumbers[2] != execN {
		t.Fatalf("C's bundle recorded base and exec as %d and %d, want A's %d and %d", cNumbers[1], cNumbers[2], baseN, execN)
	}
	return idx, cID, cBundle, cNumbers
}

// checkMerges checks that C's bundle merges test, with every value on the
// service number Record gave it: base and exec on A's.
func checkMerges(t *testing.T, idx *metastore.Index, cID string, cBundle *dagql.ValueBundle, want map[dagql.TransferOrdinal]uint64, when string) {
	t.Helper()
	testN := want[3]
	plan := Plan(idx, "cache-fresh", 0)
	i := slices.IndexFunc(plan, func(c Candidate) bool { return c.Bundle == cID })
	if i < 0 || !slices.Equal(plan[i].Roots, []uint64{testN}) || plan[i].Source != "cache-c" {
		t.Fatalf("%s: plan %+v, want C's bundle %s with root %d", when, plan, cID, testN)
	}
	mb, err := BuildMerge(context.Background(), idx, cID, cBundle, resolutionOf(t, idx, testN), plan[i].Roots, testSigner, "team")
	if err != nil {
		t.Fatalf("%s: C's bundle doesn't merge: %v", when, err)
	}
	for _, v := range mb.Bundle.Values {
		if v.SenderNumber != want[v.Ordinal] || mb.Numbers[v.Ordinal] != want[v.Ordinal] {
			t.Fatalf("%s: value %d sent as %d (numbers %v), want %d", when, v.Ordinal, v.SenderNumber, mb.Numbers, want[v.Ordinal])
		}
	}
}

// #47, found by the real-engine run (#12): a stored bundle names an imported
// value by the engine number it was imported on. Once the import moved to
// another engine entry (the engine pruned it, restarted, and had it merged
// again), the old number no longer resolved through the cache's imported
// mapping, and the bundle couldn't be merged into any engine again. The
// bundle's resolution, stored when Record recorded it, still gives A's
// numbers.
func TestBundleOfMovedImportsMerges(t *testing.T) {
	idx, cID, cBundle, want := movedImportsFixture(t)
	checkMerges(t, idx, cID, cBundle, want, "before the imports move")

	// C prunes them, restarts, and has them merged again, on 20 and 21.
	idx.RecordImports("cache-c", 2, []metastore.Import{{Number: want[1], Engine: 20}, {Number: want[2], Engine: 21}})
	if _, ok := idx.Caches["cache-c"].Imported[6]; ok {
		t.Fatal("the import didn't move")
	}
	checkMerges(t, idx, cID, cBundle, want, "after the imports moved")
}

// The same after a wipe, which drops the cache's imports altogether.
func TestBundleOfWipedImportsMerges(t *testing.T) {
	idx, cID, cBundle, want := movedImportsFixture(t)
	idx.ForgetImports("cache-c", 1000)
	if c := idx.Caches["cache-c"]; c == nil || len(c.Imported) != 0 {
		t.Fatalf("cache-c after the wipe: %+v", c)
	}
	checkMerges(t, idx, cID, cBundle, want, "after the wipe")
}

// A record stored before resolutions names none, and its bundle resolves
// through the source cache's current mappings, as all bundles did before #47
// was fixed: it merges while the imports stay, and not once they move.
//
// Intended: there is no resolution to read, and the index no longer knows
// where a moved import was, so a backfill at startup could only write what
// the mappings say then: it would save the records whose imports hadn't
// moved yet, at the cost of a pass over every entry and bundle at startup.
// Such a record gets a resolution when its value is exported again, and goes
// when it expires; no deployment held stored bundles outside test runs
// before resolutions were added.
func TestLegacyBundleResolvesThroughCurrentMappings(t *testing.T) {
	idx, cID, cBundle, want := movedImportsFixture(t)
	for _, e := range idx.Entries {
		e.Resolution = ""
	}
	testN := want[3]
	if c := Plan(idx, "cache-fresh", 0); len(c) != 2 || c[1].Bundle != cID || c[1].Resolution != "" {
		t.Fatalf("plan %+v", c)
	}
	mb, err := BuildMerge(context.Background(), idx, cID, cBundle, nil, []uint64{testN}, testSigner, "team")
	if err != nil {
		t.Fatalf("before the imports move: %v", err)
	}
	if mb.Numbers[1] != want[1] || mb.Numbers[2] != want[2] {
		t.Fatalf("numbers %v, want %v", mb.Numbers, want)
	}
	idx.RecordImports("cache-c", 2, []metastore.Import{{Number: want[1], Engine: 20}, {Number: want[2], Engine: 21}})
	if _, err := BuildMerge(context.Background(), idx, cID, cBundle, nil, []uint64{testN}, testSigner, "team"); err == nil || !strings.Contains(err.Error(), "no longer stored") {
		t.Fatalf("after the imports moved: %v, want the pre-resolution failure", err)
	}
}

// A bundle one cache recorded twice, resolved differently, plans a candidate
// per resolution: each root merges with the resolution its record names.
// Here the second record (an earlier state of the same generation, arriving
// late: fewer replacements) keeps the newer records of base and exec, and
// adds lone, dropped since the first. (A late export from an older
// generation can't add lone back: see #59.)
func TestPlanSplitsByResolution(t *testing.T) {
	idx := metastore.NewIndex()
	id := testBundleID("a")
	stored := chainBundle(10, "base", "exec")
	stored.Values = append(stored.Values, dagql.TransferredValue{
		Ordinal: 3, SenderNumber: 50,
		Record: dagql.PersistedRecord{ResultID: 3, Call: &dagql.ResultCall{Field: "lone"}},
	})
	stored.Roots = append(stored.Roots, dagql.TransferredRoot{Ordinal: 3, ExpiresAtUnix: 10})
	stored.Values[0].SenderReplacements = 1
	stored.Values[1].SenderReplacements = 1
	first, res1, err := idx.RecordResolved(id, stored, "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	keepResolution(t, res1)
	idx.DropExpired(20)
	stored.Roots[1].ExpiresAtUnix = 0
	stored.Values[0].SenderReplacements = 0
	stored.Values[1].SenderReplacements = 0
	second, res2, err := idx.RecordResolved(id, stored, "cache-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	keepResolution(t, res2)
	if second[3] == first[3] || second[2] != first[2] {
		t.Fatalf("first %v, second %v", first, second)
	}
	plan := Plan(idx, "cache-b", 30)
	if len(plan) != 2 || plan[0].Resolution == plan[1].Resolution || plan[0].Key() == plan[1].Key() {
		t.Fatalf("plan %+v, want a candidate per resolution", plan)
	}
	for _, c := range plan {
		mb, err := BuildMerge(context.Background(), idx, c.Bundle, stored, resolutionOf(t, idx, c.Roots[0]), c.Roots, testSigner, "team")
		if err != nil {
			t.Fatalf("candidate %+v: %v", c, err)
		}
		for _, r := range c.Roots {
			o := idx.Entries[r].Ordinal
			if mb.Numbers[o] != r {
				t.Fatalf("candidate %+v: root ordinal %d sent as %d", c, o, mb.Numbers[o])
			}
		}
	}
}
