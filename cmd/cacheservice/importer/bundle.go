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

// Candidate is a stored bundle to merge into an engine cache, and the service
// numbers of the roots to merge it with.
type Candidate struct {
	Bundle string   `json:"bundle"`
	Roots  []uint64 `json:"roots"`
}

// Plan returns the bundles to merge into the engine cache cacheID, from the
// scope's index at nowUnix: one per bundle that holds the current record of
// at least one live root, with those roots. A root is left out when it or
// its entry has expired, or when it came from cacheID itself, since that
// cache already has it. Bundles come in order of their lowest root, so older
// entries are merged first.
func Plan(idx *metastore.Index, cacheID string, nowUnix int64) []Candidate {
	byBundle := map[string][]uint64{}
	for n, root := range idx.Roots {
		e := idx.Entries[n]
		if e == nil || expired(root.ExpiresAtUnix, nowUnix) || expired(e.ExpiresAtUnix, nowUnix) {
			continue
		}
		if e.Source.CacheID == cacheID {
			continue
		}
		byBundle[e.Bundle] = append(byBundle[e.Bundle], n)
	}
	out := make([]Candidate, 0, len(byBundle))
	for id, roots := range byBundle {
		slices.Sort(roots)
		out = append(out, Candidate{Bundle: id, Roots: roots})
	}
	slices.SortFunc(out, func(a, b Candidate) int {
		return cmp.Or(cmp.Compare(a.Roots[0], b.Roots[0]), cmp.Compare(a.Bundle, b.Bundle))
	})
	return out
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
}

// BuildMerge rewrites stored bundle bundleID for a merge of the given roots,
// by service number, as the index idx describes them:
//
//   - each value's SenderNumber becomes its service number, which the engine
//     keeps as its Cloud holding's number;
//   - the roots are the given ones, each with the index's expiry for it;
//   - each output's chain gets a download address per layer from sign, and
//     renewalKey, which the engine names when it asks for fresh addresses.
//
// Records name each other by ordinal, so values are neither dropped nor
// renumbered. A value that only roots left out need, and whose entry the
// index has since dropped, has no service number: it gets a placeholder at
// or above the index's next number, which no stored entry has, and the
// engine merges only what the given roots need. A value a given root needs
// must still be stored.
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
		m, ok := mapping.Numbers[v.SenderNumber]
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

	out := dagql.ValueBundle{
		Version: stored.Version,
		Roots:   rootOrdinals,
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
	return MergeBundle{Bundle: out, Numbers: numbers}, nil
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
