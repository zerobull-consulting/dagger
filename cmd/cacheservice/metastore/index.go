package metastore

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/dagger/dagger/dagql"
)

// IndexVersion is the index format this package writes. It refuses to read a
// later one, rather than drop fields it doesn't know.
const IndexVersion = 1

// Index is everything a scope stores, apart from the bundles' contents.
//
// # Service numbers
//
// The service names each entry it stores with its own number, the way an
// engine names its entries: the number in a merge's SenderNumber, an offer's
// CloudNumber, and an engine's Cloud holding (dagql/cache_cloud_holding.go).
// Engines keep these numbers across their own restarts, and re-key a holding
// when a message names another number for the same entry. Numbers that
// changed on every service restart would make every engine re-key every
// holding, so service numbers live in the index, not in memory:
//
//   - NextNumber is the next number to hand out. Numbers start at 1 and only
//     go up, so a number is never reused, even after its entry is dropped.
//   - A number is handed out inside an index update, and the update's
//     conditional write fails if another writer handed one out first. Two
//     writers never hand out the same number.
//
// # Engine numbers
//
// Caches maps each engine cache, by cacheID, to the service numbers of its
// entries. An engine keeps its cacheID and its entry numbers across a clean
// restart, and bumps its generation; an unclean restart gets a new cacheID.
// So a mapping is keyed by cacheID and engine number, and remembers the
// latest generation it was recorded in. Exporting the same engine entry
// again updates its service entry, instead of storing a second one.
//
// # Imported entries
//
// An engine that had a stored entry merged into it holds the value on an
// entry of its own. Its Imported mappings name those entries: engine number
// to the service number of the stored entry the value came from. The
// importer records them after each merge. A later export of such an entry,
// as a root or in another root's closure, reuses the stored entry instead of
// storing the same value again under a new number. An import doesn't change
// the stored entry's source: only a record that adds parts to it does (see
// Record).
type Index struct {
	Version    int    `json:"version"`
	NextNumber uint64 `json:"nextNumber"`
	// Entries are the stored entries, by service number.
	Entries map[uint64]*Entry `json:"entries"`
	// Roots are the entries the scope retains, by service number. Every
	// other entry is kept because a root depends on it.
	Roots map[uint64]Root `json:"roots"`
	// Caches maps engine entry numbers to service numbers, by cacheID.
	Caches map[string]*EngineCache `json:"caches"`
}

// Entry is one stored entry: where its record is, and what it depends on.
type Entry struct {
	// Bundle is the ID of the bundle holding the entry's record, and
	// Ordinal the record's ordinal in it.
	Bundle  string                `json:"bundle"`
	Ordinal dagql.TransferOrdinal `json:"ordinal"`
	// Source is the engine entry the record was exported from.
	Source Source `json:"source"`
	// Replacements is the engine entry's replacement count when its
	// record was read. A record with fewer replacements is older, and
	// doesn't replace this one.
	Replacements uint64 `json:"replacements,omitempty"`
	// Deps are the service numbers of the entries this one keeps alive: its
	// direct dependencies, and the owners of its pending offers and of its
	// outputs, as dagql's importer walks them.
	Deps []uint64 `json:"deps,omitempty"`
	// ExpiresAtUnix is when the entry expires (0: never).
	ExpiresAtUnix int64 `json:"expiresAtUnix,omitempty"`
	// Parts are the parts the record's bundle holds outputs of, by
	// PartKey, sorted. A record of an imported value replaces this one
	// only if it holds more.
	Parts []string `json:"parts,omitempty"`
}

// Source is an engine entry: its cache, the cache's generation when the
// record was exported, and its number there.
type Source struct {
	CacheID    string `json:"cacheID"`
	Generation uint64 `json:"generation"`
	Number     uint64 `json:"number"`
}

// Root is a retained entry, until ExpiresAtUnix (0: never).
type Root struct {
	ExpiresAtUnix int64 `json:"expiresAtUnix,omitempty"`
}

// EngineCache maps one engine cache's entry numbers to service numbers.
type EngineCache struct {
	// Generation is the latest generation the cache was recorded in.
	Generation uint64 `json:"generation"`
	// Numbers are the service numbers of the entries the cache exported,
	// by engine entry number: each is the source of its entry.
	Numbers map[uint64]Mapping `json:"numbers"`
	// Imported are the service numbers of the stored entries merged into
	// the cache, by the engine entry each landed on. Their source is
	// another cache. A stored entry is imported on at most one engine
	// entry per cache, and an engine entry is either exported or imported.
	Imported map[uint64]Mapping `json:"imported,omitempty"`
}

// Lookup returns the service number an engine entry maps to, exported or
// imported.
func (c *EngineCache) Lookup(engineNum uint64) (Mapping, bool) {
	if c == nil {
		return Mapping{}, false
	}
	if m, ok := c.Numbers[engineNum]; ok {
		return m, true
	}
	m, ok := c.Imported[engineNum]
	return m, ok
}

// PartKey names a part address, for comparing parts: its JSON.
func PartKey(a dagql.PersistedPartAddress) string {
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Sprintf("%+v", a)
	}
	return string(data)
}

// Mapping is the service number of one engine entry, and the latest
// generation it was recorded in.
type Mapping struct {
	Number     uint64 `json:"number"`
	Generation uint64 `json:"generation"`
}

// NewIndex returns an empty index.
func NewIndex() *Index {
	return &Index{
		Version:    IndexVersion,
		NextNumber: 1,
		Entries:    map[uint64]*Entry{},
		Roots:      map[uint64]Root{},
		Caches:     map[string]*EngineCache{},
	}
}

func decodeIndex(body []byte) (*Index, error) {
	var idx Index
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if idx.Version != IndexVersion {
		return nil, fmt.Errorf("index version %d, this service reads %d", idx.Version, IndexVersion)
	}
	if idx.Entries == nil {
		idx.Entries = map[uint64]*Entry{}
	}
	if idx.Roots == nil {
		idx.Roots = map[uint64]Root{}
	}
	if idx.Caches == nil {
		idx.Caches = map[string]*EngineCache{}
	}
	for id, c := range idx.Caches {
		if c == nil {
			return nil, fmt.Errorf("cache %q: null", id)
		}
		if c.Numbers == nil {
			c.Numbers = map[uint64]Mapping{}
		}
	}
	if err := idx.Validate(); err != nil {
		return nil, err
	}
	return &idx, nil
}

// Validate checks that the index refers only to what it has: every number is
// below NextNumber, and every dependency, root and mapping names a stored
// entry.
func (idx *Index) Validate() error {
	if idx.Version != IndexVersion {
		return fmt.Errorf("version %d, want %d", idx.Version, IndexVersion)
	}
	if idx.NextNumber == 0 {
		return errors.New("next number is 0")
	}
	valid := func(n uint64) bool { return n != 0 && n < idx.NextNumber }
	for n, e := range idx.Entries {
		if !valid(n) {
			return fmt.Errorf("entry %d: number out of range (next %d)", n, idx.NextNumber)
		}
		if e == nil {
			return fmt.Errorf("entry %d: null", n)
		}
		if !bundleIDPattern.MatchString(e.Bundle) {
			return fmt.Errorf("entry %d: invalid bundle ID %q", n, e.Bundle)
		}
		if e.Ordinal == 0 {
			return fmt.Errorf("entry %d: no ordinal", n)
		}
		for _, d := range e.Deps {
			if _, ok := idx.Entries[d]; !ok {
				return fmt.Errorf("entry %d: dependency %d is not stored", n, d)
			}
		}
	}
	for n := range idx.Roots {
		if _, ok := idx.Entries[n]; !ok {
			return fmt.Errorf("root %d is not stored", n)
		}
	}
	for id, c := range idx.Caches {
		if id == "" {
			return errors.New("a cache with no cacheID")
		}
		for engineNum, m := range c.Numbers {
			e, ok := idx.Entries[m.Number]
			if !ok {
				return fmt.Errorf("cache %q: engine entry %d maps to %d, which is not stored", id, engineNum, m.Number)
			}
			if e.Source.CacheID != id || e.Source.Number != engineNum {
				return fmt.Errorf("cache %q: engine entry %d maps to %d, which came from cache %q entry %d", id, engineNum, m.Number, e.Source.CacheID, e.Source.Number)
			}
		}
		// Imported: a stored entry from another cache, at most once, on
		// an engine entry the cache didn't export.
		targets := make(map[uint64]uint64, len(c.Imported))
		for engineNum, m := range c.Imported {
			e, ok := idx.Entries[m.Number]
			if !ok {
				return fmt.Errorf("cache %q: imported engine entry %d maps to %d, which is not stored", id, engineNum, m.Number)
			}
			if e.Source.CacheID == id {
				return fmt.Errorf("cache %q: imported engine entry %d maps to %d, which the cache exported itself", id, engineNum, m.Number)
			}
			if _, ok := c.Numbers[engineNum]; ok {
				return fmt.Errorf("cache %q: engine entry %d is both exported and imported", id, engineNum)
			}
			if prev, ok := targets[m.Number]; ok {
				return fmt.Errorf("cache %q: engine entries %d and %d both import %d", id, min(prev, engineNum), max(prev, engineNum), m.Number)
			}
			targets[m.Number] = engineNum
		}
	}
	// And the other way: every entry is its source's mapping.
	for n, e := range idx.Entries {
		if e.Source.CacheID == "" || e.Source.Number == 0 {
			return fmt.Errorf("entry %d: no source", n)
		}
		c := idx.Caches[e.Source.CacheID]
		if c == nil || c.Numbers[e.Source.Number].Number != n {
			return fmt.Errorf("entry %d: cache %q doesn't map engine entry %d to it", n, e.Source.CacheID, e.Source.Number)
		}
	}
	return nil
}

// allocate hands out the next service number.
func (idx *Index) allocate() uint64 {
	n := idx.NextNumber
	idx.NextNumber++
	return n
}

// ValidateBundle checks what Record relies on in a bundle: it has roots,
// its ordinals run from 1 to the number of values, each value names its
// sender's number, and every dependency, root and output names one of its
// values. It doesn't check the records themselves.
func ValidateBundle(bundle *dagql.ValueBundle) error {
	if bundle == nil {
		return errors.New("nil bundle")
	}
	if bundle.Version != dagql.ValueBundleVersion {
		return fmt.Errorf("unsupported bundle version %d, want %d", bundle.Version, dagql.ValueBundleVersion)
	}
	if len(bundle.Roots) == 0 {
		return errors.New("bundle has no roots")
	}
	n := uint64(len(bundle.Values))
	in := func(o dagql.TransferOrdinal) bool { return o != 0 && uint64(o) <= n }
	seen := make(map[dagql.TransferOrdinal]bool, n)
	senders := make(map[uint64]dagql.TransferOrdinal, n)
	for _, v := range bundle.Values {
		if !in(v.Ordinal) {
			return fmt.Errorf("value ordinal %d out of range [1, %d]", v.Ordinal, n)
		}
		if seen[v.Ordinal] {
			return fmt.Errorf("duplicate value ordinal %d", v.Ordinal)
		}
		seen[v.Ordinal] = true
		if v.SenderNumber == 0 {
			return fmt.Errorf("value %d: no sender number", v.Ordinal)
		}
		if prev, ok := senders[v.SenderNumber]; ok {
			return fmt.Errorf("values %d and %d have the same sender number %d", prev, v.Ordinal, v.SenderNumber)
		}
		senders[v.SenderNumber] = v.Ordinal
	}
	for _, v := range bundle.Values {
		for _, d := range retentionDeps(bundle, v) {
			if !in(dagql.TransferOrdinal(d)) {
				return fmt.Errorf("value %d: dependency %d is not in the bundle", v.Ordinal, d)
			}
			if dagql.TransferOrdinal(d) == v.Ordinal {
				return fmt.Errorf("value %d depends on itself", v.Ordinal)
			}
		}
	}
	for _, r := range bundle.Roots {
		if !in(r.Ordinal) {
			return fmt.Errorf("root %d is not in the bundle", r.Ordinal)
		}
	}
	for _, o := range bundle.Outputs {
		if !in(o.Ordinal) {
			return fmt.Errorf("output %d is not in the bundle", o.Ordinal)
		}
	}
	return nil
}

// retentionDeps returns the ordinals a value keeps alive, as dagql's importer
// walks them: its dependencies, the owners of its record's pending offers,
// and the owners of the bundle's outputs for it. Unsorted, with repeats.
func retentionDeps(bundle *dagql.ValueBundle, v dagql.TransferredValue) []uint64 {
	deps := slices.Clone(v.DependencyIDs)
	for _, offer := range v.Record.Envelope.PendingOffers {
		deps = append(deps, offer.Owner.DependencyIDs...)
	}
	for _, o := range bundle.Outputs {
		if o.Ordinal == v.Ordinal && o.Owner != nil {
			deps = append(deps, o.Owner.DependencyIDs...)
		}
	}
	return deps
}

// Record adds a bundle's values to the index, stored in bundle bundleID and
// exported from engine cache cacheID at generation. It returns each value's
// service number, by ordinal.
//
// A value whose engine entry already maps to a stored entry keeps that
// service number. Its record replaces the stored one, unless the stored one
// is newer: from a later generation, or from the same generation with more
// replacements. A value whose engine entry is imported (see RecordImports)
// takes the imported entry's number, so the same value isn't stored twice.
// Its record replaces the stored one only if the bundle holds every part the
// stored record does, and more; the entry's source then becomes this cache,
// and the old source's mapping becomes an import. Generations and
// replacement counts of two caches don't compare, so nothing else decides
// between them. Any other value gets a new service number. A root's expiry
// is the later of the stored one and the bundle's (0, never, is the latest).
func (idx *Index) Record(bundleID string, bundle *dagql.ValueBundle, cacheID string, generation uint64) (map[dagql.TransferOrdinal]uint64, error) {
	if !bundleIDPattern.MatchString(bundleID) {
		return nil, fmt.Errorf("invalid bundle ID %q", bundleID)
	}
	if cacheID == "" {
		return nil, errors.New("no cacheID")
	}
	if err := ValidateBundle(bundle); err != nil {
		return nil, err
	}
	cache := idx.Caches[cacheID]
	if cache == nil {
		cache = &EngineCache{Numbers: map[uint64]Mapping{}}
		idx.Caches[cacheID] = cache
	}
	cache.Generation = max(cache.Generation, generation)

	// Ordinal order, so numbers are handed out in a stable order.
	values := slices.Clone(bundle.Values)
	slices.SortFunc(values, func(a, b dagql.TransferredValue) int { return cmp.Compare(a.Ordinal, b.Ordinal) })

	numbers := make(map[dagql.TransferOrdinal]uint64, len(values))
	imported := map[dagql.TransferOrdinal]bool{}
	for _, v := range values {
		if m, ok := cache.Numbers[v.SenderNumber]; ok {
			if _, stored := idx.Entries[m.Number]; stored {
				numbers[v.Ordinal] = m.Number
				continue
			}
		}
		if m, ok := cache.Imported[v.SenderNumber]; ok {
			if _, stored := idx.Entries[m.Number]; stored {
				numbers[v.Ordinal] = m.Number
				imported[v.Ordinal] = true
				continue
			}
		}
		numbers[v.Ordinal] = idx.allocate()
	}
	parts := bundleParts(bundle)
	for _, v := range values {
		n := numbers[v.Ordinal]
		if imported[v.Ordinal] {
			m := cache.Imported[v.SenderNumber]
			m.Generation = max(m.Generation, generation)
			cache.Imported[v.SenderNumber] = m
			old := idx.Entries[n]
			if !morePartsThan(parts[v.Ordinal], old.Parts) {
				continue
			}
			// This record holds more: it becomes the entry's, and this
			// cache its source.
			if src := idx.Caches[old.Source.CacheID]; src != nil {
				if sm, ok := src.Numbers[old.Source.Number]; ok && sm.Number == n {
					delete(src.Numbers, old.Source.Number)
					if src.Imported == nil {
						src.Imported = map[uint64]Mapping{}
					}
					src.Imported[old.Source.Number] = sm
				}
			}
			delete(cache.Imported, v.SenderNumber)
			cache.Numbers[v.SenderNumber] = m
		} else {
			cache.Numbers[v.SenderNumber] = Mapping{
				Number:     n,
				Generation: max(generation, cache.Numbers[v.SenderNumber].Generation),
			}
			if old, ok := idx.Entries[n]; ok && newerThan(old, generation, v.SenderReplacements) {
				continue
			}
		}
		vdeps := retentionDeps(bundle, v)
		deps := make([]uint64, 0, len(vdeps))
		for _, d := range vdeps {
			deps = append(deps, numbers[dagql.TransferOrdinal(d)])
		}
		slices.Sort(deps)
		deps = slices.Compact(deps)
		idx.Entries[n] = &Entry{
			Bundle:        bundleID,
			Ordinal:       v.Ordinal,
			Source:        Source{CacheID: cacheID, Generation: generation, Number: v.SenderNumber},
			Replacements:  v.SenderReplacements,
			Deps:          deps,
			ExpiresAtUnix: v.ExpiresAtUnix,
			Parts:         parts[v.Ordinal],
		}
	}
	for _, r := range bundle.Roots {
		n := numbers[r.Ordinal]
		old, ok := idx.Roots[n]
		if ok && laterExpiry(old.ExpiresAtUnix, r.ExpiresAtUnix) {
			continue
		}
		idx.Roots[n] = Root{ExpiresAtUnix: r.ExpiresAtUnix}
	}
	return numbers, nil
}

// bundleParts returns the parts each value has outputs of in a bundle, by
// ordinal, as sorted PartKeys.
func bundleParts(bundle *dagql.ValueBundle) map[dagql.TransferOrdinal][]string {
	out := map[dagql.TransferOrdinal][]string{}
	for _, o := range bundle.Outputs {
		out[o.Ordinal] = append(out[o.Ordinal], PartKey(o.Address))
	}
	for o, keys := range out {
		slices.Sort(keys)
		out[o] = slices.Compact(keys)
	}
	return out
}

// morePartsThan reports whether parts holds every one of old, and more.
func morePartsThan(parts, old []string) bool {
	if len(parts) <= len(old) {
		return false
	}
	for _, k := range old {
		if _, found := slices.BinarySearch(parts, k); !found {
			return false
		}
	}
	return true
}

// Import is one stored entry merged into an engine cache: the service
// number of the stored entry, and the engine entry it landed on.
type Import struct {
	Number uint64 `json:"number"`
	Engine uint64 `json:"engine"`
}

// RecordImports records that stored entries were merged into the engine
// cache cacheID at generation, and landed on the given engine entries, and
// returns how many it recorded. It skips an import of an entry no longer
// stored, of an entry the cache exported itself, and onto an engine entry
// the cache exported: that entry is the cache's own, and keeps its own
// service number. A stored entry imported onto another engine entry than
// before, as when the engine replaced the one it landed on, moves there.
func (idx *Index) RecordImports(cacheID string, generation uint64, imports []Import) int {
	if cacheID == "" {
		return 0
	}
	cache := idx.Caches[cacheID]
	recorded := 0
	for _, im := range imports {
		e, ok := idx.Entries[im.Number]
		if !ok || im.Engine == 0 || e.Source.CacheID == cacheID {
			continue
		}
		if cache == nil {
			cache = &EngineCache{Numbers: map[uint64]Mapping{}}
			idx.Caches[cacheID] = cache
		}
		if _, own := cache.Numbers[im.Engine]; own {
			continue
		}
		if cache.Imported == nil {
			cache.Imported = map[uint64]Mapping{}
		}
		for engineNum, m := range cache.Imported {
			if m.Number == im.Number && engineNum != im.Engine {
				delete(cache.Imported, engineNum)
			}
		}
		gen := generation
		if m, ok := cache.Imported[im.Engine]; ok && m.Number == im.Number {
			gen = max(gen, m.Generation)
		}
		cache.Imported[im.Engine] = Mapping{Number: im.Number, Generation: gen}
		cache.Generation = max(cache.Generation, generation)
		recorded++
	}
	return recorded
}

// ForgetImports drops the imported mappings of an engine cache, as when the
// cache was wiped: its engine entries are gone. The entries it exported
// stay, since other engines may still import them.
func (idx *Index) ForgetImports(cacheID string) {
	c := idx.Caches[cacheID]
	if c == nil {
		return
	}
	c.Imported = nil
	if len(c.Numbers) == 0 {
		delete(idx.Caches, cacheID)
	}
}

// newerThan reports whether a stored entry describes a later state of its
// engine entry than a record read at generation with replacements: one from
// a later generation, or from the same one with more replacements. A record
// from an earlier generation, arriving late, never replaces a later one.
func newerThan(e *Entry, generation, replacements uint64) bool {
	if e.Source.Generation != generation {
		return e.Source.Generation > generation
	}
	return e.Replacements > replacements
}

// laterExpiry reports whether expiry a is at or after b, where 0 is never.
func laterExpiry(a, b int64) bool {
	if a == 0 {
		return true
	}
	if b == 0 {
		return false
	}
	return a >= b
}

// expired reports whether expiresAtUnix (0: never) is at or before now.
func expired(expiresAtUnix, nowUnix int64) bool {
	return expiresAtUnix != 0 && expiresAtUnix <= nowUnix
}

// DropExpired drops what has expired at nowUnix and returns the dropped
// entries' service numbers, in order. It drops expired roots; then expired
// entries and, transitively, every entry that depends on one; then every
// entry no remaining root depends on. Mappings to dropped entries, exported
// or imported, go too.
// Their numbers are not handed out again.
//
// Bundles stay in the store, even when no entry refers to them any more.
func (idx *Index) DropExpired(nowUnix int64) []uint64 {
	for n, r := range idx.Roots {
		if expired(r.ExpiresAtUnix, nowUnix) {
			delete(idx.Roots, n)
		}
	}
	// dead: expired, or depends on an entry that is.
	dead := map[uint64]bool{}
	var isDead func(n uint64, visiting map[uint64]bool) bool
	isDead = func(n uint64, visiting map[uint64]bool) bool {
		if d, ok := dead[n]; ok {
			return d
		}
		e, ok := idx.Entries[n]
		if !ok || expired(e.ExpiresAtUnix, nowUnix) {
			dead[n] = true
			return true
		}
		if visiting[n] {
			// A cycle; Validate doesn't forbid one. Decided by the rest.
			return false
		}
		visiting[n] = true
		d := false
		for _, dep := range e.Deps {
			if isDead(dep, visiting) {
				d = true
				break
			}
		}
		delete(visiting, n)
		dead[n] = d
		return d
	}
	// live: reachable from a root that isn't dead.
	live := map[uint64]bool{}
	var mark func(uint64)
	mark = func(n uint64) {
		if live[n] {
			return
		}
		live[n] = true
		for _, dep := range idx.Entries[n].Deps {
			mark(dep)
		}
	}
	for n := range idx.Roots {
		if isDead(n, map[uint64]bool{}) {
			delete(idx.Roots, n)
			continue
		}
		mark(n)
	}
	var dropped []uint64
	for n := range idx.Entries {
		if !live[n] {
			dropped = append(dropped, n)
			delete(idx.Entries, n)
		}
	}
	slices.Sort(dropped)
	for _, c := range idx.Caches {
		for engineNum, m := range c.Numbers {
			if _, ok := idx.Entries[m.Number]; !ok {
				delete(c.Numbers, engineNum)
			}
		}
		for engineNum, m := range c.Imported {
			if _, ok := idx.Entries[m.Number]; !ok {
				delete(c.Imported, engineNum)
			}
		}
	}
	return dropped
}

// Recorded is what Store.Record stored.
type Recorded struct {
	// BundleID is the bundle's ID.
	BundleID string
	// Numbers are the values' service numbers, by ordinal.
	Numbers map[dagql.TransferOrdinal]uint64
	// Index is the index as written.
	Index *Index
}

// Record stores an exported bundle and adds its values to the scope's index,
// as Index.Record does, in one atomic index update. The bundle is stored
// first, so the index never names a bundle that isn't there.
func (s *Store) Record(ctx context.Context, scope string, bundle *dagql.ValueBundle, cacheID string, generation uint64) (Recorded, error) {
	if err := ValidateBundle(bundle); err != nil {
		return Recorded{}, fmt.Errorf("record bundle: %w", err)
	}
	if cacheID == "" {
		return Recorded{}, errors.New("record bundle: no cacheID")
	}
	id, err := s.PutBundle(ctx, scope, bundle)
	if err != nil {
		return Recorded{}, err
	}
	var numbers map[dagql.TransferOrdinal]uint64
	idx, err := s.UpdateIndex(ctx, scope, func(idx *Index) error {
		var err error
		numbers, err = idx.Record(id, bundle, cacheID, generation)
		return err
	})
	if err != nil {
		return Recorded{}, fmt.Errorf("record bundle %s: %w", id, err)
	}
	return Recorded{BundleID: id, Numbers: numbers, Index: idx}, nil
}

// DropExpired drops what has expired at nowUnix from the scope's index, as
// Index.DropExpired does, in one atomic index update, and returns the dropped
// service numbers.
func (s *Store) DropExpired(ctx context.Context, scope string, nowUnix int64) ([]uint64, error) {
	var dropped []uint64
	_, err := s.UpdateIndex(ctx, scope, func(idx *Index) error {
		dropped = idx.DropExpired(nowUnix)
		return nil
	})
	return dropped, err
}

// RecordImports records stored entries merged into an engine cache, as
// Index.RecordImports does, in one atomic index update, and returns how many
// it recorded.
func (s *Store) RecordImports(ctx context.Context, scope, cacheID string, generation uint64, imports []Import) (int, error) {
	if cacheID == "" {
		return 0, errors.New("record imports: no cacheID")
	}
	if len(imports) == 0 {
		return 0, nil
	}
	var n int
	_, err := s.UpdateIndex(ctx, scope, func(idx *Index) error {
		n = idx.RecordImports(cacheID, generation, imports)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("record imports into %q: %w", cacheID, err)
	}
	return n, nil
}

// ForgetImports drops an engine cache's imported mappings from the scope's
// index, as Index.ForgetImports does, in one atomic index update.
func (s *Store) ForgetImports(ctx context.Context, scope, cacheID string) error {
	_, err := s.UpdateIndex(ctx, scope, func(idx *Index) error {
		idx.ForgetImports(cacheID)
		return nil
	})
	return err
}
