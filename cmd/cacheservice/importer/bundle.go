package importer

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
)

// Candidate is a stored bundle to merge into an engine cache, the engine
// cache that exported the roots, and the service numbers of the roots to
// merge it with.
//
// Bundle IDs are content hashes, so two engine caches that export identical
// bundles share one: the roots of each are a candidate of their own, since
// the bundle's sender numbers are each cache's own.
type Candidate struct {
	Bundle string   `json:"bundle"`
	Source string   `json:"source"`
	Roots  []uint64 `json:"roots"`
}

// Key names the candidate's bundle and source, for what the importer keeps
// per merge.
func (c Candidate) Key() string { return c.Bundle + "/" + c.Source }

// Plan returns the bundles to merge into the engine cache cacheID, from the
// scope's index at nowUnix: one per bundle and source cache that hold the
// current record of at least one live root, with those roots. A root is left
// out when it or its entry has expired, or an entry it depends on has, or
// when it came from cacheID itself, since that cache already has it. Bundles come in order of their lowest root, so older
// entries are merged first.
func Plan(idx *metastore.Index, cacheID string, nowUnix int64) []Candidate {
	type key struct{ bundle, source string }
	byBundle := map[key][]uint64{}
	dead := deadEntries(idx, nowUnix)
	for n, root := range idx.Roots {
		e := idx.Entries[n]
		if e == nil || expired(root.ExpiresAtUnix, nowUnix) || dead(n) {
			continue
		}
		if e.Source.CacheID == cacheID {
			continue
		}
		k := key{e.Bundle, e.Source.CacheID}
		byBundle[k] = append(byBundle[k], n)
	}
	out := make([]Candidate, 0, len(byBundle))
	for k, roots := range byBundle {
		slices.Sort(roots)
		out = append(out, Candidate{Bundle: k.bundle, Source: k.source, Roots: roots})
	}
	slices.SortFunc(out, func(a, b Candidate) int {
		return cmp.Or(cmp.Compare(a.Roots[0], b.Roots[0]), cmp.Compare(a.Bundle, b.Bundle), cmp.Compare(a.Source, b.Source))
	})
	return out
}

// deadEntries returns whether an entry is dead at nowUnix: missing, expired,
// or depending, transitively, on an entry that is, as metastore's
// DropExpired judges them. A root over a dead entry is not merged, even
// before DropExpired has run, so an engine never gets an expired dependency.
func deadEntries(idx *metastore.Index, nowUnix int64) func(uint64) bool {
	memo := map[uint64]bool{}
	visiting := map[uint64]bool{}
	var dead func(uint64) bool
	dead = func(n uint64) bool {
		if d, ok := memo[n]; ok {
			return d
		}
		e := idx.Entries[n]
		if e == nil || expired(e.ExpiresAtUnix, nowUnix) {
			memo[n] = true
			return true
		}
		if visiting[n] {
			// A cycle: decided by the rest of it.
			return false
		}
		visiting[n] = true
		d := false
		for _, dep := range e.Deps {
			if dead(dep) {
				d = true
				break
			}
		}
		delete(visiting, n)
		memo[n] = d
		return d
	}
	return dead
}

func expired(expiresAtUnix, nowUnix int64) bool {
	return expiresAtUnix != 0 && expiresAtUnix <= nowUnix
}

// Signer returns a download address for a blob.
type Signer func(ctx context.Context, dgst digest.Digest) (dagql.BlobAddress, error)

// StoreSigner signs presigned GETs from a blob store.
func StoreSigner(store blobstore.Store) Signer {
	return func(ctx context.Context, dgst digest.Digest) (dagql.BlobAddress, error) {
		addr, err := store.PresignGet(ctx, dgst)
		if err != nil {
			return dagql.BlobAddress{}, err
		}
		var exp int64
		if !addr.ExpiresAt.IsZero() {
			exp = addr.ExpiresAt.Unix()
		}
		return dagql.BlobAddress{URL: addr.URL, ExpiresAtUnix: exp}, nil
	}
}

// MergeBundle is a stored bundle rewritten for one merge.
type MergeBundle struct {
	// Bundle is what the merge sends.
	Bundle dagql.ValueBundle
	// Numbers are the values' service numbers, by ordinal. A value no
	// merged root needs, whose entry the index no longer has, is absent.
	Numbers map[dagql.TransferOrdinal]uint64
	// Merged are the ordinals of the merged roots. The bundle's other roots
	// are sent expired.
	Merged map[dagql.TransferOrdinal]bool
}

// expiredRoot is the expiry a root not being merged is sent with: long past,
// so the engine validates the bundle with it and merges only what the
// merged roots need.
const expiredRoot = 1

// BuildMerge rewrites stored bundle bundleID for a merge of the given roots,
// by service number, as the index idx describes them:
//
//   - each value's SenderNumber becomes its service number, which the engine
//     keeps as its Cloud holding's number;
//   - the given roots are merged, each with the index's expiry for it;
//   - each output's chain gets a download address per layer from sign, and
//     renewalKey, which the engine names when it asks for fresh addresses.
//
// Records name each other by ordinal, so values are neither dropped nor
// renumbered. The engine refuses a bundle with a value outside its roots'
// closure, so the stored bundle's other roots are sent too, expired
// (expiredRoot): the engine validates the bundle with them, then skips them
// with whatever only they need. A value that only those need, and whose
// entry the index has since dropped, has no service number: it gets a
// placeholder at or above the index's next number, which no stored entry
// has, and is never merged. A value a given root needs must still be stored.
//
// The stored bundle is not changed.
func BuildMerge(ctx context.Context, idx *metastore.Index, bundleID string, stored *dagql.ValueBundle, roots []uint64, sign Signer, renewalKey string) (MergeBundle, error) {
	if err := metastore.ValidateBundle(stored); err != nil {
		return MergeBundle{}, fmt.Errorf("bundle %s: %w", bundleID, err)
	}
	if len(roots) == 0 {
		return MergeBundle{}, errors.New("no roots")
	}
	values := make(map[dagql.TransferOrdinal]*dagql.TransferredValue, len(stored.Values))
	for i := range stored.Values {
		values[stored.Values[i].Ordinal] = &stored.Values[i]
	}

	// Every root's current record must be in this bundle, from one cache.
	var source string
	rootOrdinals := make([]dagql.TransferredRoot, 0, len(roots))
	for _, n := range roots {
		e := idx.Entries[n]
		if e == nil {
			return MergeBundle{}, fmt.Errorf("root %d is not stored", n)
		}
		if e.Bundle != bundleID {
			return MergeBundle{}, fmt.Errorf("root %d's record is in bundle %s, not %s", n, e.Bundle, bundleID)
		}
		if values[e.Ordinal] == nil {
			return MergeBundle{}, fmt.Errorf("root %d: ordinal %d is not in the bundle", n, e.Ordinal)
		}
		if source == "" {
			source = e.Source.CacheID
		} else if source != e.Source.CacheID {
			return MergeBundle{}, fmt.Errorf("roots from caches %q and %q in one bundle", source, e.Source.CacheID)
		}
		rootOrdinals = append(rootOrdinals, dagql.TransferredRoot{Ordinal: e.Ordinal, ExpiresAtUnix: idx.Roots[n].ExpiresAtUnix})
	}
	slices.SortFunc(rootOrdinals, func(a, b dagql.TransferredRoot) int { return cmp.Compare(a.Ordinal, b.Ordinal) })
	mapping := idx.Caches[source]
	if mapping == nil {
		return MergeBundle{}, fmt.Errorf("no mapping for cache %q", source)
	}

	numbers := make(map[dagql.TransferOrdinal]uint64, len(stored.Values))
	for _, v := range stored.Values {
		// A value the source cache had imported names the stored entry it
		// came from.
		m, ok := mapping.Lookup(v.SenderNumber)
		if !ok {
			continue
		}
		if _, stored := idx.Entries[m.Number]; stored {
			numbers[v.Ordinal] = m.Number
		}
	}
	for _, o := range closure(stored, rootOrdinals) {
		if _, ok := numbers[o]; !ok {
			return MergeBundle{}, fmt.Errorf("value %d, which a root needs, is no longer stored", o)
		}
	}

	merged := make(map[dagql.TransferOrdinal]bool, len(rootOrdinals))
	for _, r := range rootOrdinals {
		merged[r.Ordinal] = true
	}
	allRoots := slices.Clone(rootOrdinals)
	for _, r := range stored.Roots {
		if !merged[r.Ordinal] {
			allRoots = append(allRoots, dagql.TransferredRoot{Ordinal: r.Ordinal, ExpiresAtUnix: expiredRoot})
			merged[r.Ordinal] = false
		}
	}
	for o, m := range merged {
		if !m {
			delete(merged, o)
		}
	}
	slices.SortFunc(allRoots, func(a, b dagql.TransferredRoot) int { return cmp.Compare(a.Ordinal, b.Ordinal) })
	out := dagql.ValueBundle{
		Version: stored.Version,
		Roots:   allRoots,
		Values:  make([]dagql.TransferredValue, len(stored.Values)),
	}
	placeholder := idx.NextNumber
	for i, v := range stored.Values {
		if n, ok := numbers[v.Ordinal]; ok {
			v.SenderNumber = n
		} else {
			v.SenderNumber = placeholder
			placeholder++
		}
		out.Values[i] = v
	}

	addresses := map[digest.Digest]dagql.BlobAddress{}
	for _, o := range stored.Outputs {
		if o.Chain != nil {
			chain := dagql.OfferedChain{
				Layers:     slices.Clone(o.Chain.Layers),
				Addresses:  make(map[digest.Digest]dagql.BlobAddress, len(o.Chain.Layers)),
				RenewalKey: renewalKey,
			}
			for _, l := range o.Chain.Layers {
				d := l.Descriptor.Digest
				addr, ok := addresses[d]
				if !ok {
					var err error
					addr, err = sign(ctx, d)
					if err != nil {
						return MergeBundle{}, fmt.Errorf("sign %s: %w", d, err)
					}
					addresses[d] = addr
				}
				chain.Addresses[d] = addr
			}
			o.Chain = &chain
		}
		out.Outputs = append(out.Outputs, o)
	}
	return MergeBundle{Bundle: out, Numbers: numbers, Merged: merged}, nil
}

// closure returns the ordinals the roots keep alive, as dagql's merge walks
// them: their dependencies and the owners of their pending offers and of
// their outputs, transitively.
func closure(bundle *dagql.ValueBundle, roots []dagql.TransferredRoot) []dagql.TransferOrdinal {
	values := make(map[dagql.TransferOrdinal]*dagql.TransferredValue, len(bundle.Values))
	for i := range bundle.Values {
		values[bundle.Values[i].Ordinal] = &bundle.Values[i]
	}
	outputOwners := map[dagql.TransferOrdinal][]uint64{}
	for _, o := range bundle.Outputs {
		if o.Owner != nil {
			outputOwners[o.Ordinal] = append(outputOwners[o.Ordinal], o.Owner.DependencyIDs...)
		}
	}
	seen := map[dagql.TransferOrdinal]bool{}
	var out []dagql.TransferOrdinal
	var visit func(dagql.TransferOrdinal)
	visit = func(o dagql.TransferOrdinal) {
		if seen[o] {
			return
		}
		seen[o] = true
		out = append(out, o)
		v := values[o]
		if v == nil {
			return
		}
		deps := slices.Clone(v.DependencyIDs)
		for _, offer := range v.Record.Envelope.PendingOffers {
			deps = append(deps, offer.Owner.DependencyIDs...)
		}
		deps = append(deps, outputOwners[o]...)
		for _, d := range deps {
			visit(dagql.TransferOrdinal(d))
		}
	}
	for _, r := range roots {
		visit(r.Ordinal)
	}
	slices.Sort(out)
	return out
}

// addressKey names a part address, for comparing addresses.
func addressKey(a dagql.PersistedPartAddress) string {
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Sprintf("%v", a)
	}
	return string(data)
}
