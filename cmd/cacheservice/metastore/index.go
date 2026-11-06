package metastore

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dagger/dagger/dagql"
)

// IndexVersion is the index format this package writes. It refuses to read a
// later one, rather than drop fields it doesn't know.
//
// Version 2 added Entry.Resolution (#47). A version 1 index reads as one
// whose records name no resolution, and is written back as version 2. A
// service that reads only version 1 refuses a version 2 index, rather than
// rewrite it without the resolutions: so during a rolling upgrade the older
// replicas stop updating the scope instead of erasing what the newer ones
// recorded.
//
// Version 3 added Root.RecordedAtUnix and Index.GC (#11). Versions 1 and 2
// read as an index whose roots have no recording time yet (garbage
// collection stamps them when it first runs, so they get a whole max age
// from then) and that no collection has touched. A service that reads only
// version 2 refuses a version 3 index: it doesn't check GC's deletions when
// it records an export, so it could name an object GC is deleting.
//
// Version 4 added EngineCache.UpdatedAtUnix (#59), required on every cache
// record: a record with no mappings left is kept until CacheRecordRetention
// after it, so that it still knows the cache's latest generation, and then
// dropped. Versions 1 to 3 read with every cache record stamped with the
// reader's clock. A service that reads only version 3 refuses a version 4
// index: it would write it back without the times, and drop no record.
const IndexVersion = 4

// CacheRecordRetention is how long an engine cache's record stays in the
// index once it has no mappings left, after it last changed
// (EngineCache.UpdatedAtUnix). Its Generation is what keeps a late export
// from an older generation of the cache from adding back a value GC
// collected (see Index.RecordResolvedAt); a late export after this is
// recorded as new. Each record costs about a hundred bytes of index.
const CacheRecordRetention = 7 * 24 * time.Hour

// oldestIndexVersion is the oldest index format this package reads.
const oldestIndexVersion = 1

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
	// GC is what garbage collection keeps here: the objects it is deleting
	// or deleted lately, which an export may no longer name (see
	// Collection). Nil until it first deletes something.
	GC *Collection `json:"gc,omitempty"`
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
	// Resolution is the ID of the stored Resolution of the export that
	// wrote this record: the service number of each of the bundle's values
	// when it was recorded, which a merge of the bundle sends. Empty for a
	// record stored before resolutions were: the bundle is then resolved
	// through the source cache's current mappings (see BuildMerge in
	// package importer).
	Resolution string `json:"resolution,omitempty"`
}

// Source is an engine entry: its cache, the cache's generation when the
// record was exported, and its number there.
type Source struct {
	CacheID    string `json:"cacheID"`
	Generation uint64 `json:"generation"`
	Number     uint64 `json:"number"`
}

// Root is a retained entry, until ExpiresAtUnix (0: never), and, under a
// namespace's max age, until that long after RecordedAtUnix (see
// RootExpiry).
type Root struct {
	ExpiresAtUnix int64 `json:"expiresAtUnix,omitempty"`
	// RecordedAtUnix is when an export last wrote the root's record (see
	// Store.Record): the time its max age counts from. 0 for a root
	// recorded before version 3, until GC stamps it (StampRoots).
	RecordedAtUnix int64 `json:"recordedAtUnix,omitempty"`
}

// EngineCache maps one engine cache's entry numbers to service numbers.
//
// The record outlives its mappings: once GC has dropped them all, it is kept
// until CacheRecordRetention after UpdatedAtUnix, for its Generation.
type EngineCache struct {
	// Generation is the latest generation the cache was recorded in.
	Generation uint64 `json:"generation"`
	// UpdatedAtUnix is when the index last created the record, or added,
	// updated or dropped one of its mappings. Required (never 0).
	UpdatedAtUnix int64 `json:"updatedAtUnix"`
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

// decodeIndex decodes and validates an index, read at nowUnix: the time an
// index from before version 4 stamps its cache records with.
func decodeIndex(body []byte, nowUnix int64) (*Index, error) {
	var idx Index
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if idx.Version < oldestIndexVersion || idx.Version > IndexVersion {
		return nil, fmt.Errorf("index version %d, this service reads %d to %d", idx.Version, oldestIndexVersion, IndexVersion)
	}
	// Version 1 has no resolutions; its records read as naming none.
	// Before version 4, cache records have no update time: they get
	// nowUnix, so an empty one is kept a whole CacheRecordRetention.
	stamp := idx.Version < 4
	idx.Version = IndexVersion
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
		if stamp {
			c.UpdatedAtUnix = nowUnix
		}
	}
	if err := idx.Validate(); err != nil {
		return nil, err
	}
	return &idx, nil
}

// Clone returns a deep copy of the index.
func (idx *Index) Clone() *Index {
	body, err := json.Marshal(idx)
	if err != nil {
		panic(fmt.Sprintf("encode index: %v", err))
	}
	var c Index
	if err := json.Unmarshal(body, &c); err != nil {
		panic(fmt.Sprintf("decode index: %v", err))
	}
	if c.Entries == nil {
		c.Entries = map[uint64]*Entry{}
	}
	if c.Roots == nil {
		c.Roots = map[uint64]Root{}
	}
	if c.Caches == nil {
		c.Caches = map[string]*EngineCache{}
	}
	for _, ec := range c.Caches {
		if ec.Numbers == nil {
			ec.Numbers = map[uint64]Mapping{}
		}
	}
	return &c
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
		if e.Resolution != "" && !bundleIDPattern.MatchString(e.Resolution) {
			return fmt.Errorf("entry %d: invalid resolution ID %q", n, e.Resolution)
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
		if c.UpdatedAtUnix <= 0 {
			return fmt.Errorf("cache %q: no update time", id)
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
	if err := idx.GC.validate(); err != nil {
		return err
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

// BundleVersion is the only dagql.ValueBundle version the service stores: the
// version the engine it serves writes and imports. Under the lockstep policy
// (zerobull-consulting/dagger#117) that engine is this module's dagql, and
// TestBundleVersionMatchesEngineCapture pins the two together.
const BundleVersion = 3

// ValidateBundle checks what Record relies on in a bundle: it has roots,
// its ordinals run from 1 to the number of values, each value names its
// sender's number, and every dependency, root and output names one of its
// values. It doesn't check the records themselves.
func ValidateBundle(bundle *dagql.ValueBundle) error {
	if bundle == nil {
		return errors.New("nil bundle")
	}
	if bundle.Version != BundleVersion {
		return fmt.Errorf("unsupported bundle version %d, want %d", bundle.Version, BundleVersion)
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
// replacements; or unless it is the same value (as many replacements, which
// engines keep across a clean restart) and the bundle lacks a part the
// stored record holds, as when the value is a dependency of the bundle's
// roots, which carries no outputs (#133). A value whose engine entry is
// imported (see RecordImportsAt)
// takes the imported entry's number, so the same value isn't stored twice.
// Its record replaces the stored one only if the bundle holds every part the
// stored record does, and more; the entry's source then becomes this cache,
// and the old source's mapping becomes an import. Generations and
// replacement counts of two caches don't compare, so nothing else decides
// between them. Any other value gets a new service number. A root's expiry
// is the later of the stored one and the bundle's (0, never, is the latest).
//
// Each record it writes names the bundle's resolution: the service numbers
// it returns. RecordResolvedAt returns that resolution too; Store.Record
// stores it before the index that names it.
func (idx *Index) Record(bundleID string, bundle *dagql.ValueBundle, cacheID string, generation uint64, nowUnix int64) (map[dagql.TransferOrdinal]uint64, error) {
	numbers, _, err := idx.RecordResolvedAt(bundleID, bundle, cacheID, generation, nowUnix)
	return numbers, err
}

// RecordResolvedAt is Record at nowUnix, and also returns the bundle's
// resolution, which the records it wrote name by its ID. It stamps the
// bundle's roots with nowUnix as their recording time (Root.RecordedAtUnix),
// which a namespace's max age counts from, when this export wrote the
// root's record: a root new to the index with its record, a later state of
// the value from its own engine, or an imported value's record with more
// parts (see Record). So does a root of its own engine's value whose stored
// record this export kept because that record holds a part the bundle lacks
// (#133): the engine exported the value as a root, and the stored record is
// of that same value.
//
// A root whose record this export did not write, or keep that way, keeps
// its time, or none (a root from before version 3, which GC stamps), and is
// not added when new: a late export from an older generation, or an import
// exported back unchanged.
// So merging stored entries into engines never keeps them alive by itself:
// only an export of the value from its own engine does. The cache's record is stamped with nowUnix too
// (EngineCache.UpdatedAtUnix). nowUnix must be positive: the index is
// stamped with the caller's clock only, so its logic follows an injected
// one (#111).
//
// A late export from a generation older than the cache's latest recorded
// one (EngineCache.Generation) adds nothing the index has no mapping for:
// if any of its values has no stored entry, exported or imported, the whole
// record is refused with ErrStaleGeneration, and the index is left as it
// was. The newer generation either never exported that value, or exported
// it and GC collected it; either way the stale record would be a new entry
// and root with a fresh recording time, reviving what the namespace's max
// age expired (#59). The cache's record outlives its mappings for this (see
// CacheRecordRetention). A bundle is a closure, so it is refused whole rather
// than recorded in part: the values that are mapped keep their stored
// records. A stale export whose values are all mapped is recorded as
// before, and newerThan keeps it from replacing a later record, but it adds
// no root: a mapped value with no root had its root expired, and a stale
// record would revive it, whether through an exported mapping or an import
// it takes over with more parts. A root that is still there is refreshed
// as before. The cache's own generation and newer ones are never refused.
func (idx *Index) RecordResolvedAt(bundleID string, bundle *dagql.ValueBundle, cacheID string, generation uint64, nowUnix int64) (map[dagql.TransferOrdinal]uint64, *Resolution, error) {
	if !bundleIDPattern.MatchString(bundleID) {
		return nil, nil, fmt.Errorf("invalid bundle ID %q", bundleID)
	}
	if cacheID == "" {
		return nil, nil, errors.New("no cacheID")
	}
	if err := ValidateBundle(bundle); err != nil {
		return nil, nil, err
	}
	if nowUnix <= 0 {
		return nil, nil, fmt.Errorf("no recording time (%d)", nowUnix)
	}
	// A late export from an older generation than the latest recorded may
	// add no value the index has no mapping for (see above). Refused before
	// anything is changed: no number is handed out.
	if err := idx.CheckGeneration(bundle, cacheID, generation); err != nil {
		return nil, nil, err
	}
	cache := idx.Caches[cacheID]
	if cache == nil {
		cache = &EngineCache{Numbers: map[uint64]Mapping{}}
		idx.Caches[cacheID] = cache
	}
	stale := generation < cache.Generation
	cache.Generation = max(cache.Generation, generation)

	// Ordinal order, so numbers are handed out in a stable order.
	values := slices.Clone(bundle.Values)
	slices.SortFunc(values, func(a, b dagql.TransferredValue) int { return cmp.Compare(a.Ordinal, b.Ordinal) })

	numbers := make(map[dagql.TransferOrdinal]uint64, len(values))
	imported := map[dagql.TransferOrdinal]bool{}
	for _, v := range values {
		if n, imp, ok := idx.mapped(cache, v.SenderNumber); ok {
			numbers[v.Ordinal] = n
			imported[v.Ordinal] = imp
			continue
		}
		// Not stale: CheckGeneration refused a stale one with any value
		// unmapped.
		numbers[v.Ordinal] = idx.allocate()
	}
	// Every value is mapped, by this record or before it.
	cache.UpdatedAtUnix = max(cache.UpdatedAtUnix, nowUnix)
	res, err := NewResolution(bundleID, cacheID, numbers)
	if err != nil {
		return nil, nil, err
	}
	resID, _, err := ResolutionID(res)
	if err != nil {
		return nil, nil, err
	}
	parts := bundleParts(bundle)
	written := map[dagql.TransferOrdinal]bool{}
	// current are the values whose stored record this export kept because
	// the stored record is of the same value and holds a part the bundle
	// lacks: for their roots, as if written.
	current := map[dagql.TransferOrdinal]bool{}
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
					src.UpdatedAtUnix = max(src.UpdatedAtUnix, nowUnix)
				}
			}
			delete(cache.Imported, v.SenderNumber)
			cache.Numbers[v.SenderNumber] = m
		} else {
			cache.Numbers[v.SenderNumber] = Mapping{
				Number:     n,
				Generation: max(generation, cache.Numbers[v.SenderNumber].Generation),
			}
			if old, ok := idx.Entries[n]; ok {
				if newerThan(old, generation, v.SenderReplacements) {
					continue
				}
				if old.Replacements == v.SenderReplacements && !holdsParts(parts[v.Ordinal], old.Parts) {
					// The same value, recorded without a part the stored
					// record holds: as a dependency in another root's
					// closure, which carries no outputs, or by an earlier,
					// slower export finishing late (#133). The stored
					// record stays, so its parts aren't lost; the value's
					// root, if this bundle has it, is refreshed as one
					// whose record was written.
					current[v.Ordinal] = true
					continue
				}
			}
		}
		vdeps := retentionDeps(bundle, v)
		deps := make([]uint64, 0, len(vdeps))
		for _, d := range vdeps {
			deps = append(deps, numbers[dagql.TransferOrdinal(d)])
		}
		slices.Sort(deps)
		deps = slices.Compact(deps)
		written[v.Ordinal] = true
		idx.Entries[n] = &Entry{
			Bundle:        bundleID,
			Ordinal:       v.Ordinal,
			Source:        Source{CacheID: cacheID, Generation: generation, Number: v.SenderNumber},
			Replacements:  v.SenderReplacements,
			Deps:          deps,
			ExpiresAtUnix: v.ExpiresAtUnix,
			Parts:         parts[v.Ordinal],
			Resolution:    resID,
		}
	}
	for _, r := range bundle.Roots {
		n := numbers[r.Ordinal]
		root, existed := idx.Roots[n]
		recorded := written[r.Ordinal] || current[r.Ordinal]
		if !existed && (stale || !recorded) {
			// A root is added only with its record. This export didn't
			// write it (a late export from an older generation, or an
			// import exported back unchanged): the stored entry stays,
			// kept alive by whatever keeps it now, and no root, with a
			// fresh recording time, revives a value whose own root
			// expired. Merges never keep data alive by themselves. Nor
			// does a stale generation's export, even when it writes the
			// record (a later state than the stored one, or an import's
			// with more parts): the root it would add expired (#59).
			continue
		}
		if !existed || !laterExpiry(root.ExpiresAtUnix, r.ExpiresAtUnix) {
			root.ExpiresAtUnix = r.ExpiresAtUnix
		}
		// Only with its record: a root this export added, or whose record
		// it wrote. A root from before version 3 (no time) that this
		// export didn't write keeps none; GC stamps it.
		if recorded {
			root.RecordedAtUnix = max(root.RecordedAtUnix, nowUnix)
		}
		idx.Roots[n] = root
	}
	return numbers, res, nil
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
	return len(parts) > len(old) && holdsParts(parts, old)
}

// holdsParts reports whether parts holds every one of old. Both are sorted
// PartKeys, as bundleParts returns them.
func holdsParts(parts, old []string) bool {
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

// mapped returns the service number of a cache's engine entry, and whether
// the mapping is an import, when the index maps it to a stored entry.
func (idx *Index) mapped(cache *EngineCache, engineNum uint64) (n uint64, imported, ok bool) {
	if m, ok := cache.Numbers[engineNum]; ok {
		if _, stored := idx.Entries[m.Number]; stored {
			return m.Number, false, true
		}
	}
	if m, ok := cache.Imported[engineNum]; ok {
		if _, stored := idx.Entries[m.Number]; stored {
			return m.Number, true, true
		}
	}
	return 0, false, false
}

func staleGenerationError(cacheID string, engineNum, generation, recorded uint64) error {
	return fmt.Errorf("cache %q engine entry %d, exported at generation %d, after generation %d was recorded: %w",
		cacheID, engineNum, generation, recorded, ErrStaleGeneration)
}

// CheckGeneration returns an ErrStaleGeneration error when RecordResolvedAt
// would refuse bundle from cacheID at generation on this index: the
// generation is older than the cache's recorded one, and a value of the
// bundle has no stored entry. It changes nothing. An exporter checks a
// snapshot with it before uploading the bundle's blobs (#111). RecordResolvedAt
// runs it on the index it writes, which stays the check that decides, since
// a newer generation can be recorded in between.
//
// An invalid bundle is not refused here: RecordResolvedAt fails it as
// invalid, which is retried, rather than refused, which isn't.
func (idx *Index) CheckGeneration(bundle *dagql.ValueBundle, cacheID string, generation uint64) error {
	cache := idx.Caches[cacheID]
	if cache == nil || generation >= cache.Generation || ValidateBundle(bundle) != nil {
		return nil
	}
	for _, v := range bundle.Values {
		if _, _, ok := idx.mapped(cache, v.SenderNumber); !ok {
			return staleGenerationError(cacheID, v.SenderNumber, generation, cache.Generation)
		}
	}
	return nil
}

// RecordImportsAt records that stored entries were merged into the engine
// cache cacheID at generation, and landed on the given engine entries, and
// returns the imports it recorded that the index still holds when it
// returns, in order: a later import in the same call may replace an earlier
// one. It skips an import of an entry no longer
// stored, of an entry the cache exported itself, and onto an engine entry
// the cache exported: that entry is the cache's own, and keeps its own
// service number. A stored entry imported onto another engine entry than
// before, as when the engine replaced the one it landed on, moves there.
// It stamps the cache's record (EngineCache.UpdatedAtUnix) with nowUnix,
// which must be positive (see RecordResolvedAt): with none, it records
// nothing.
func (idx *Index) RecordImportsAt(cacheID string, generation uint64, imports []Import, nowUnix int64) []Import {
	if cacheID == "" || nowUnix <= 0 {
		return nil
	}
	cache := idx.Caches[cacheID]
	var recorded []Import
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
		cache.UpdatedAtUnix = max(cache.UpdatedAtUnix, nowUnix)
		recorded = append(recorded, im)
	}
	// Only what the index holds now: a later import in the same call can
	// replace an earlier one, onto the same engine entry or by moving the
	// stored entry elsewhere.
	kept := recorded[:0]
	seen := map[Import]bool{}
	for _, im := range recorded {
		if m, ok := cache.Imported[im.Engine]; ok && m.Number == im.Number && !seen[im] {
			seen[im] = true
			kept = append(kept, im)
		}
	}
	return kept
}

// ForgetImports drops the imported mappings of an engine cache, as when the
// cache was wiped: its engine entries are gone. The entries it exported
// stay, since other engines may still import them. The cache's record stays
// too, even with no mappings left, for its generation: Expire drops it
// CacheRecordRetention after nowUnix, unless it changes meanwhile.
func (idx *Index) ForgetImports(cacheID string, nowUnix int64) {
	c := idx.Caches[cacheID]
	if c == nil || len(c.Imported) == 0 {
		return
	}
	c.Imported = nil
	c.UpdatedAtUnix = max(c.UpdatedAtUnix, nowUnix)
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
// Bundles, resolutions and blobs stay in the store: garbage collection
// (package gc) deletes those no entry names any more.
func (idx *Index) DropExpired(nowUnix int64) []uint64 {
	return idx.Expire(nowUnix, 0)
}

// Expire is DropExpired under a namespace's max age: a root also expires
// maxAge after it was recorded (see RootExpiry). maxAge 0 is no max age.
//
// A cache whose mappings it drops is stamped nowUnix (EngineCache.
// UpdatedAtUnix), and a cache with no mappings is dropped once that is
// CacheRecordRetention ago: until then, its Generation refuses a late
// export from an older generation (see RecordResolvedAt).
func (idx *Index) Expire(nowUnix int64, maxAge time.Duration) []uint64 {
	for n, r := range idx.Roots {
		if expired(CapExpiry(r, maxAge), nowUnix) {
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
	retention := int64(CacheRecordRetention / time.Second)
	for id, c := range idx.Caches {
		changed := false
		for engineNum, m := range c.Numbers {
			if _, ok := idx.Entries[m.Number]; !ok {
				delete(c.Numbers, engineNum)
				changed = true
			}
		}
		for engineNum, m := range c.Imported {
			if _, ok := idx.Entries[m.Number]; !ok {
				delete(c.Imported, engineNum)
				changed = true
			}
		}
		if changed {
			c.UpdatedAtUnix = max(c.UpdatedAtUnix, nowUnix)
		}
		if len(c.Numbers) == 0 && len(c.Imported) == 0 && c.UpdatedAtUnix <= nowUnix-retention {
			delete(idx.Caches, id)
		}
	}
	return dropped
}

// Recorded is what Store.Record stored.
type Recorded struct {
	// BundleID is the bundle's ID.
	BundleID string
	// ResolutionID is the ID of the bundle's stored resolution, which the
	// records it wrote name.
	ResolutionID string
	// Numbers are the values' service numbers, by ordinal.
	Numbers map[dagql.TransferOrdinal]uint64
	// Index is the index as written.
	Index *Index
}

// Record stores an exported bundle and adds its values to the scope's index,
// as Index.Record does, in one atomic index update.
//
// Writes go in an order where every partial state is harmless, so a reader
// never sees the index name an object that isn't stored:
//
//  1. The bundle.
//  2. Its resolution (see Resolution), once the update has resolved the
//     values against the index it read, and before it writes the index.
//     If the conditional index write then loses, the update resolves again
//     against the newer index, and stores that resolution, if it differs,
//     before trying again.
//  3. The index.
//
// A service that stops after 1 or 2 leaves a bundle or resolution no index
// names: nothing reads it, and the export is not recorded (as before
// resolutions, an export the service didn't record is exported again by a
// later session). A resolution of a losing attempt is left the same way.
//
// Roots it writes are stamped with the store's clock (see
// Index.RecordResolvedAt). Record refuses to name an object GC is deleting
// (ErrCollected); an export uses RecordFenced, which also refuses one GC
// deleted after the export checked it.
func (s *Store) Record(ctx context.Context, scope string, bundle *dagql.ValueBundle, cacheID string, generation uint64) (Recorded, error) {
	return s.RecordFenced(ctx, scope, bundle, cacheID, generation, noFence)
}

// RecordFenced is Record for an export that read GC's epoch (Store.ReadEpoch)
// at fence.Started, before it checked any blob, bundle or resolution. It
// refuses, with ErrCollected and without writing the index, to name an
// object GC is deleting, or deleted in a round after fence.Epoch, and a fence
// older than MaxFenceAge (see Collection).
func (s *Store) RecordFenced(ctx context.Context, scope string, bundle *dagql.ValueBundle, cacheID string, generation uint64, fence Fence) (Recorded, error) {
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
	var (
		numbers map[dagql.TransferOrdinal]uint64
		res     *Resolution
		resID   string
		stored  = map[string]bool{}
	)
	var named []string
	for _, d := range BundleBlobs(bundle) {
		named = append(named, BlobRelKey(d))
	}
	idx, err := s.updateIndex(ctx, scope, func(idx *Index) error {
		now := s.now()
		if !fence.Started.IsZero() && now.Sub(fence.Started) > MaxFenceAge {
			return fmt.Errorf("the export started %s ago, over %s: %w", now.Sub(fence.Started).Round(time.Second), MaxFenceAge, ErrCollected)
		}
		var err error
		numbers, res, err = idx.RecordResolvedAt(id, bundle, cacheID, generation, now.Unix())
		if err != nil {
			return err
		}
		rid, _, err := ResolutionID(res)
		if err != nil {
			return err
		}
		return idx.checkFence(append(named[:len(named):len(named)], BundleRelKey(id), ResolutionRelKey(rid)), fence)
	}, func(*Index) error {
		rid, _, err := ResolutionID(res)
		if err != nil {
			return err
		}
		resID = rid
		if stored[rid] {
			return nil
		}
		if _, err := s.PutResolution(ctx, scope, res); err != nil {
			return err
		}
		stored[rid] = true
		return nil
	})
	if err != nil {
		return Recorded{}, fmt.Errorf("record bundle %s: %w", id, err)
	}
	return Recorded{BundleID: id, ResolutionID: resID, Numbers: numbers, Index: idx}, nil
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
// Index.RecordImportsAt does, in one atomic index update, and returns the
// imports it recorded.
func (s *Store) RecordImports(ctx context.Context, scope, cacheID string, generation uint64, imports []Import) ([]Import, error) {
	if cacheID == "" {
		return nil, errors.New("record imports: no cacheID")
	}
	if len(imports) == 0 {
		return nil, nil
	}
	var recorded []Import
	_, err := s.UpdateIndex(ctx, scope, func(idx *Index) error {
		// Read on each try, as RecordFenced does: a retry stamps its own
		// time.
		now := s.now().Unix()
		if now <= 0 {
			return fmt.Errorf("the store's clock reads %d", now)
		}
		recorded = idx.RecordImportsAt(cacheID, generation, imports, now)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("record imports into %q: %w", cacheID, err)
	}
	return recorded, nil
}

// ForgetImports drops an engine cache's imported mappings from the scope's
// index, as Index.ForgetImports does, in one atomic index update.
func (s *Store) ForgetImports(ctx context.Context, scope, cacheID string) error {
	_, err := s.UpdateIndex(ctx, scope, func(idx *Index) error {
		idx.ForgetImports(cacheID, s.now().Unix())
		return nil
	})
	return err
}
