package e2e

// #47 on real engines: a bundle holding an engine's imports stays mergeable
// once those imports move (prune, then restart) and after a service restart.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
)

// heldBundle is a stored bundle one cache exported that holds entries the
// cache had imported: C's verify exec's, which names A's entries by the
// engine entries they were merged onto.
type heldBundle struct {
	ID, Resolution string
	// Roots are the service numbers of the cache's own live roots in it.
	Roots []uint64
	// Imported are the service numbers of other caches' entries it names.
	Imported []uint64
}

func (h heldBundle) String() string {
	return fmt.Sprintf("%s (roots %v, imports %v)", Short(h.ID), h.Roots, h.Imported)
}

// heldImports are cacheID's stored bundles that hold entries it imported,
// read from the index and their stored resolutions.
func (s *state) heldImports(idx *metastore.Index, cacheID string) []heldBundle {
	type key struct{ bundle, res string }
	roots := map[key][]uint64{}
	for n, e := range idx.Entries {
		if e.Source.CacheID != cacheID {
			continue
		}
		k := key{e.Bundle, e.Resolution}
		if _, ok := roots[k]; !ok {
			roots[k] = nil
		}
		if _, ok := idx.Roots[n]; ok {
			roots[k] = append(roots[k], n)
		}
	}
	var out []heldBundle
	for k, rs := range roots {
		if k.res == "" || len(rs) == 0 {
			continue
		}
		h := heldBundle{ID: k.bundle, Resolution: k.res, Roots: sortedCopy(rs)}
		for _, n := range s.env.Resolution(k.res).Numbers {
			if e, ok := idx.Entries[n]; ok && e.Source.CacheID != cacheID {
				h.Imported = append(h.Imported, n)
			}
		}
		if len(h.Imported) > 0 {
			slices.Sort(h.Imported)
			out = append(out, h)
		}
	}
	slices.SortFunc(out, func(a, b heldBundle) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// movedImports counts the values of h whose engine number no longer
// resolves, through the cache's current mappings, to the stored entry h's
// resolution names: what made such a bundle unmergeable before #47.
func (s *state) movedImports(idx *metastore.Index, cacheID string, h heldBundle) int {
	res := s.env.Resolution(h.Resolution)
	moved := 0
	for _, v := range s.env.Bundle(h.ID).Values {
		want, _ := res.Number(v.Ordinal)
		if !slices.Contains(h.Imported, want) {
			continue
		}
		if m, ok := idx.Caches[cacheID].Lookup(v.SenderNumber); !ok || m.Number != want {
			moved++
		}
	}
	return moved
}

// checkHeldMergeable checks, from the index as it is, that a fresh engine's
// plan holds each of held, and that each builds a merge with its stored
// resolution; and reports whether resolving it through the cache's current
// mappings, as before #47 was fixed, would have failed.
func (s *state) checkHeldMergeable(t *testing.T, held []heldBundle, when string) (wouldHaveFailed []string) {
	t.Helper()
	idx := s.env.Index()
	noSign := func(context.Context, digest.Digest) (dagql.BlobAddress, error) { return dagql.BlobAddress{}, nil }
	plan := importer.Plan(idx, "fresh-engine", time.Now().Unix())
	for _, h := range held {
		i := slices.IndexFunc(plan, func(c importer.Candidate) bool { return c.Bundle == h.ID && c.Resolution == h.Resolution })
		if i < 0 {
			Check(t, false, "%s: a fresh engine's plan holds bundle %s; plan: %+v", when, h, plan)
			continue
		}
		c := plan[i]
		stored := s.env.Bundle(c.Bundle)
		_, err := importer.BuildMerge(context.Background(), idx, c.Bundle, stored, s.env.Resolution(c.Resolution), c.Roots, noSign, s.env.Scope)
		Check(t, err == nil, "%s: bundle %s builds a merge with its stored resolution (error: %v)", when, h, err)
		if _, err := importer.BuildMerge(context.Background(), idx, c.Bundle, stored, nil, c.Roots, noSign, s.env.Scope); err != nil {
			wouldHaveFailed = append(wouldHaveFailed, fmt.Sprintf("%s: %v", Short(h.ID), err))
		}
	}
	return wouldHaveFailed
}

// waitHeldMerged waits until svc's importer has merged each of held into
// en, and checks that en kept their roots and the imported entries they
// name.
func (s *state) waitHeldMerged(t *testing.T, svc *Service, en *Engine, held []heldBundle, when string) {
	t.Helper()
	var st importer.State
	s.env.WaitFor(s.env.Settings.Timeout, fmt.Sprintf("%s: merges into %s of C's bundles holding its imports %v", when, en.Name, held), func() (bool, error) {
		var err error
		if st, err = svc.Imports(en.Conn); err != nil {
			return false, err
		}
		for _, h := range held {
			if !slices.ContainsFunc(st.Merged, func(m importer.MergedBundle) bool { return m.Bundle == h.ID }) {
				return false, fmt.Errorf("bundle %s not merged yet (failures %v)", Short(h.ID), st.Failures)
			}
		}
		return true, nil
	})
	for _, h := range held {
		var missing []uint64
		for _, n := range append(slices.Clone(h.Roots), h.Imported...) {
			if v, ok := st.Values[n]; !ok || v.Number == 0 {
				missing = append(missing, n)
			}
		}
		Check(t, len(missing) == 0, "%s: %s holds C's entries from bundle %s: its roots and the imported entries they build on (missing: %v)", when, en.Name, h, missing)
	}
}

// staleMergeFailures are the merges, in every service of the run, that
// failed because a value a root needs was no longer stored: #47's failure.
func staleMergeFailures(env *Env) []string {
	var out []string
	for _, svc := range env.Services() {
		for _, r := range svc.Logs.Since(0, "import: merge failed", func(r LogRecord) bool {
			return strings.Contains(r.Attr("error"), "is no longer stored")
		}) {
			out = append(out, fmt.Sprintf("%s: bundle %s into %s: %s", svc.Name, Short(r.Attr("bundle")), r.Attr("cacheID"), r.Attr("error")))
		}
	}
	return out
}
