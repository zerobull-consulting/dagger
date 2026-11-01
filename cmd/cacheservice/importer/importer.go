// Package importer is the service's side of the import path: it makes an
// engine reuse cache entries that other engines exported, with merge, merged,
// offer and offered.
//
// One sync of an engine cache goes:
//
//  1. Plan: read the scope's index and pick the stored bundles that hold the
//     current record of a live root, leaving out the engine's own exports.
//     A bundle already merged into this engine cache, at this generation,
//     with every root it needs now, is not merged again.
//  2. Merge: rewrite each bundle for the engine (BuildMerge), with service
//     numbers as SenderNumbers and a presigned GET per layer, and send it in
//     merge. The engine answers merged with the entry each record landed on,
//     at what replacement count and expiry, with its complete and offered
//     parts, and which roots it retained or skipped as expired. The importer
//     keeps that, by service number, to know what the engine kept.
//  3. Offer: a stored part the merged entry has neither complete nor offered
//     is offered to that entry, by the engine's number. accepted and
//     alreadyComplete are recorded; executionStarted and unavailable are
//     tried again at the next sync, up to OfferAttempts times; invalid is
//     not tried again; a gone entry is forgotten.
//
// When the engine reads an offered part after its addresses expired, it
// sends renew; Renew answers with fresh presigned GETs, or unavailable when
// a layer is missing from the blob store.
package importer

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// Defaults for Config.
const (
	// DefaultMergeAttempts is how many times one bundle is merged into one
	// engine cache while the merge fails, before the importer gives up on
	// it until the engine restarts.
	DefaultMergeAttempts = 3
	// DefaultOfferAttempts is how many times one part is offered while the
	// engine answers executionStarted or unavailable.
	DefaultOfferAttempts = 3
	// DefaultRequestTimeout bounds the wait for merged and offered.
	DefaultRequestTimeout = 5 * time.Minute
	// maxCachedBundles bounds the stored bundles kept in memory.
	maxCachedBundles = 256
)

// Engine is a connected engine, as the importer sees it.
type Engine interface {
	// CacheID and Generation are the engine cache's identity, from hello.
	CacheID() string
	Generation() uint64
	// Request sends a request and returns the engine's reply, which may be
	// an error reply. It fails when ctx or the connection ends first.
	Request(ctx context.Context, typ protocol.Type, body json.RawMessage) (protocol.Envelope, error)
}

// Config configures an Importer.
type Config struct {
	// Blobs signs the layers' download addresses, and Meta reads the index
	// and bundles.
	Blobs blobstore.Store
	Meta  *metastore.Store
	// Scope is the index the imported values are read from. It is also the
	// renewal key of every offered chain.
	Scope string
	// Logger receives progress and failures. Nil discards.
	Logger *slog.Logger
	// MergeAttempts, OfferAttempts and RequestTimeout default to the
	// Default* values when zero.
	MergeAttempts  int
	OfferAttempts  int
	RequestTimeout time.Duration
	// MaxMessageBytes is the largest request the importer sends. Zero
	// means protocol.MaxMessageBytes. Tests lower it.
	MaxMessageBytes int
	// Now is the clock. Nil means time.Now.
	Now func() time.Time
}

// Importer merges stored bundles into engines, and offers them stored parts.
type Importer struct {
	cfg  Config
	log  *slog.Logger
	sign Signer

	mu      sync.Mutex
	caches  map[string]*cacheState
	bundles map[string]*dagql.ValueBundle
}

// New returns an Importer for cfg.
func New(cfg Config) (*Importer, error) {
	if cfg.Blobs == nil || cfg.Meta == nil {
		return nil, errors.New("importer: needs a blob store and a metadata store")
	}
	if err := metastore.ValidateScope(cfg.Scope); err != nil {
		return nil, fmt.Errorf("importer: %w", err)
	}
	if cfg.MergeAttempts <= 0 {
		cfg.MergeAttempts = DefaultMergeAttempts
	}
	if cfg.OfferAttempts <= 0 {
		cfg.OfferAttempts = DefaultOfferAttempts
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = protocol.MaxMessageBytes
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Importer{
		cfg:     cfg,
		log:     log,
		sign:    StoreSigner(cfg.Blobs),
		caches:  map[string]*cacheState{},
		bundles: map[string]*dagql.ValueBundle{},
	}, nil
}

// cacheState is what the importer knows of one engine cache, at one
// generation.
type cacheState struct {
	// sync serializes syncs of the cache, from any of its connections.
	sync sync.Mutex

	// The fields below are guarded by Importer.mu.
	generation uint64
	// merged are the bundles merged, by Candidate.Key.
	merged map[string]*MergedBundle
	// failures counts failed merges, by Candidate.Key.
	failures map[string]int
	// values are what the engine kept, by service number.
	values map[uint64]*Value
	// pending are offers to try again.
	pending []pendingOffer
	// offers counts the offers' outcomes.
	offers OfferCounts
}

// MergedBundle is one merge the engine committed.
type MergedBundle struct {
	Bundle string `json:"bundle"`
	// Source is the engine cache that exported the merged roots.
	Source string `json:"source"`
	// Roots are the merged roots, by service number.
	Roots []uint64 `json:"roots"`
	// Retained are the roots the engine retains, by service number, with
	// the retention's expiry (0: never).
	Retained map[uint64]int64 `json:"retained"`
	// Expired are the roots the engine skipped as expired.
	Expired []uint64 `json:"expired,omitempty"`
	// Values counts the records merged.
	Values int `json:"values"`
	// Generation and EngineTime are the engine's, at the commit.
	Generation uint64    `json:"generation"`
	EngineTime time.Time `json:"engineTime"`
}

// Value is the engine entry a service entry landed on, as the engine last
// reported it.
type Value struct {
	Number        uint64   `json:"number"`
	Replacements  uint64   `json:"replacements"`
	ExpiresAtUnix int64    `json:"expiresAtUnix,omitempty"`
	Deps          []uint64 `json:"deps,omitempty"`
	// Complete and Offered are the entry's parts, by address key.
	Complete []dagql.PersistedPartAddress `json:"complete,omitempty"`
	Offered  []dagql.PersistedPartAddress `json:"offered,omitempty"`
}

func (v *Value) has(a dagql.PersistedPartAddress) bool {
	key := addressKey(a)
	for _, c := range v.Complete {
		if addressKey(c) == key {
			return true
		}
	}
	for _, c := range v.Offered {
		if addressKey(c) == key {
			return true
		}
	}
	return false
}

func addAddress(list []dagql.PersistedPartAddress, a dagql.PersistedPartAddress) []dagql.PersistedPartAddress {
	key := addressKey(a)
	for _, c := range list {
		if addressKey(c) == key {
			return list
		}
	}
	return append(list, a)
}

// OfferCounts counts offered parts by outcome.
type OfferCounts struct {
	Accepted         int `json:"accepted,omitempty"`
	AlreadyComplete  int `json:"alreadyComplete,omitempty"`
	ExecutionStarted int `json:"executionStarted,omitempty"`
	Unavailable      int `json:"unavailable,omitempty"`
	Invalid          int `json:"invalid,omitempty"`
	// Gone counts offered entries the engine no longer had.
	Gone int `json:"gone,omitempty"`
}

func (c *OfferCounts) add(o OfferCounts) {
	c.Accepted += o.Accepted
	c.AlreadyComplete += o.AlreadyComplete
	c.ExecutionStarted += o.ExecutionStarted
	c.Unavailable += o.Unavailable
	c.Invalid += o.Invalid
	c.Gone += o.Gone
}

// pendingOffer is one part to offer: to the engine entry Number, the part of
// the service entry the offer names.
type pendingOffer struct {
	number   uint64
	part     protocol.CloudPart
	attempts int
}

// Result is what one Sync did.
type Result struct {
	// Merged are the merges the engine committed.
	Merged []MergedBundle `json:"merged,omitempty"`
	// Failed are the bundles that could not be merged, with why.
	Failed []FailedBundle `json:"failed,omitempty"`
	// Offers counts the outcomes of the offers sent.
	Offers OfferCounts `json:"offers"`
}

// FailedBundle is a bundle that could not be merged, and why.
type FailedBundle struct {
	Bundle string `json:"bundle"`
	Error  string `json:"error"`
}

// stateLocked returns the state of a cache at generation, reset when the
// cache has restarted into it, or nil when generation is older than the
// state's: a stale connection must not reset a newer generation's state.
// Requires mu. To check a state is still current without changing it, use
// currentLocked.
func (im *Importer) stateLocked(cacheID string, generation uint64) *cacheState {
	st := im.caches[cacheID]
	if st == nil {
		st = &cacheState{}
		im.caches[cacheID] = st
	}
	if st.merged != nil && st.generation > generation {
		// A connection of an older generation: its cache restarted since,
		// and the newer generation's state stays.
		return nil
	}
	if st.generation != generation || st.merged == nil {
		st.generation = generation
		st.merged = map[string]*MergedBundle{}
		st.failures = map[string]int{}
		st.values = map[uint64]*Value{}
		st.pending = nil
		st.offers = OfferCounts{}
	}
	return st
}

// Forget drops what the importer knows of an engine cache, as when its cache
// was wiped.
func (im *Importer) Forget(cacheID string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	delete(im.caches, cacheID)
}

// Pruned forgets the engine entries a cache dropped, so they are not
// offered parts.
func (im *Importer) Pruned(cacheID string, generation uint64, numbers []uint64) {
	im.mu.Lock()
	defer im.mu.Unlock()
	st := im.caches[cacheID]
	if st == nil || st.generation != generation {
		return
	}
	dropped := make(map[uint64]bool, len(numbers))
	for _, n := range numbers {
		dropped[n] = true
	}
	for s, v := range st.values {
		if dropped[v.Number] {
			delete(st.values, s)
		}
	}
	st.pending = slices.DeleteFunc(st.pending, func(p pendingOffer) bool { return dropped[p.number] })
}

// Sync merges into eng's cache every stored bundle it lacks, and offers it
// the stored parts its merged entries lack. It returns an error, with the
// result so far, when ctx ends, the connection fails, or the index can't be
// read. A bundle that can't be merged for a reason of its own is in the
// result's Failed.
func (im *Importer) Sync(ctx context.Context, eng Engine) (Result, error) {
	var res Result
	cacheID, generation := eng.CacheID(), eng.Generation()
	im.mu.Lock()
	st := im.stateLocked(cacheID, generation)
	im.mu.Unlock()
	if st == nil {
		return res, errStateReset
	}
	st.sync.Lock()
	defer st.sync.Unlock()

	idx, err := im.cfg.Meta.Index(ctx, im.cfg.Scope)
	if err != nil {
		return res, fmt.Errorf("read index: %w", err)
	}
	for _, c := range Plan(idx, cacheID, im.cfg.Now().Unix()) {
		im.mu.Lock()
		if !im.currentLocked(eng, st) {
			im.mu.Unlock()
			return res, errStateReset
		}
		done := st.merged[c.Key()]
		failures := st.failures[c.Key()]
		im.mu.Unlock()
		if done != nil && subset(c.Roots, done.Roots) {
			continue
		}
		if failures >= im.cfg.MergeAttempts {
			continue
		}
		merged, offers, err := im.merge(ctx, eng, st, idx, c)
		if err != nil {
			if ctx.Err() != nil {
				return res, context.Cause(ctx)
			}
			var connErr *connectionError
			if errors.As(err, &connErr) || errors.Is(err, errStateReset) {
				return res, err
			}
			im.log.Warn("import: merge failed", "cacheID", cacheID, "bundle", c.Bundle, "roots", len(c.Roots), "error", err)
			res.Failed = append(res.Failed, FailedBundle{Bundle: c.Bundle, Error: err.Error()})
			im.mu.Lock()
			st.failures[c.Key()]++
			im.mu.Unlock()
			continue
		}
		res.Merged = append(res.Merged, merged)
		im.mu.Lock()
		st.pending = append(st.pending, offers...)
		im.mu.Unlock()
	}

	counts, err := im.offerPending(ctx, eng, st)
	res.Offers.add(counts)
	if err != nil {
		return res, err
	}
	return res, nil
}

// subset reports whether every number in a is in b.
func subset(a, b []uint64) bool {
	for _, n := range a {
		if !slices.Contains(b, n) {
			return false
		}
	}
	return true
}

// errStateReset is a sync whose engine cache state was reset or replaced
// while it ran, as when the cache was wiped or a newer generation connected:
// what it learned is discarded.
var errStateReset = errors.New("engine cache state reset during sync")

// current reports whether st is still the state of eng's cache. Requires mu.
func (im *Importer) currentLocked(eng Engine, st *cacheState) bool {
	return im.caches[eng.CacheID()] == st && st.generation == eng.Generation()
}

// connectionError is a request that failed for the connection, not the
// request: the sync stops.
type connectionError struct{ err error }

func (e *connectionError) Error() string { return e.err.Error() }
func (e *connectionError) Unwrap() error { return e.err }

// bundle reads a stored bundle, through a small in-memory cache: bundles
// never change.
func (im *Importer) bundle(ctx context.Context, id string) (*dagql.ValueBundle, error) {
	im.mu.Lock()
	b := im.bundles[id]
	im.mu.Unlock()
	if b != nil {
		return b, nil
	}
	b, err := im.cfg.Meta.GetBundle(ctx, im.cfg.Scope, id)
	if err != nil {
		return nil, err
	}
	im.mu.Lock()
	if len(im.bundles) >= maxCachedBundles {
		clear(im.bundles)
	}
	im.bundles[id] = b
	im.mu.Unlock()
	return b, nil
}

// merge merges one bundle, records what the engine kept, and returns the
// offers of the stored parts its entries lack.
func (im *Importer) merge(ctx context.Context, eng Engine, st *cacheState, idx *metastore.Index, c Candidate) (MergedBundle, []pendingOffer, error) {
	stored, err := im.bundle(ctx, c.Bundle)
	if err != nil {
		return MergedBundle{}, nil, fmt.Errorf("read bundle: %w", err)
	}
	mb, err := BuildMerge(ctx, idx, c.Bundle, stored, c.Roots, im.sign, im.cfg.Scope)
	if err != nil {
		return MergedBundle{}, nil, err
	}
	body, err := im.encode(protocol.Merge{Bundle: mb.Bundle})
	if err != nil {
		return MergedBundle{}, nil, err
	}
	rctx, cancel := context.WithTimeout(ctx, im.cfg.RequestTimeout)
	defer cancel()
	env, err := eng.Request(rctx, protocol.TypeMerge, body)
	if err != nil {
		if ctx.Err() == nil && rctx.Err() != nil {
			return MergedBundle{}, nil, fmt.Errorf("merge: no reply in %s", im.cfg.RequestTimeout)
		}
		return MergedBundle{}, nil, &connectionError{fmt.Errorf("merge: %w", err)}
	}
	if err := replyError(env); err != nil {
		return MergedBundle{}, nil, fmt.Errorf("merge: %w", err)
	}
	if env.Type != protocol.TypeMerged {
		return MergedBundle{}, nil, fmt.Errorf("merge answered with %s", env.Type)
	}
	var reply protocol.Merged
	if err := env.Decode(&reply); err != nil {
		return MergedBundle{}, nil, err
	}
	return im.applyMerged(eng, st, c, mb, reply)
}

// applyMerged records a merged reply in st, and returns the offers of the
// parts the merged entries lack. A reply that arrives after st was reset or
// replaced is discarded with errStateReset, so it can't repopulate the state
// of a wiped cache or of another generation.
func (im *Importer) applyMerged(eng Engine, st *cacheState, c Candidate, mb MergeBundle, reply protocol.Merged) (MergedBundle, []pendingOffer, error) {
	byOrdinal := make(map[dagql.TransferOrdinal]*protocol.MergedValue, len(reply.Values))
	for i := range reply.Values {
		v := &reply.Values[i]
		if v.Ordinal == 0 || uint64(v.Ordinal) > uint64(len(mb.Bundle.Values)) {
			return MergedBundle{}, nil, fmt.Errorf("merged: value at unknown ordinal %d", v.Ordinal)
		}
		if v.Number == 0 {
			return MergedBundle{}, nil, fmt.Errorf("merged: value %d landed on no entry", v.Ordinal)
		}
		byOrdinal[v.Ordinal] = v
	}
	// Only the merged roots: the bundle's other roots are sent expired.
	rootNumber := map[dagql.TransferOrdinal]uint64{}
	for o := range mb.Merged {
		rootNumber[o] = mb.Numbers[o]
	}
	out := MergedBundle{
		Bundle:     c.Bundle,
		Source:     c.Source,
		Roots:      slices.Clone(c.Roots),
		Retained:   map[uint64]int64{},
		Values:     len(reply.Values),
		Generation: reply.Generation,
		EngineTime: time.Unix(0, reply.EngineTime).UTC(),
	}
	for _, r := range reply.Retained {
		if n, ok := rootNumber[r.Ordinal]; ok {
			out.Retained[n] = r.ExpiresAtUnix
		}
	}
	for _, s := range reply.Skipped {
		if n, ok := rootNumber[s.Ordinal]; ok {
			out.Expired = append(out.Expired, n)
		}
	}
	slices.Sort(out.Expired)

	values := map[uint64]*Value{}
	for o, v := range byOrdinal {
		n, ok := mb.Numbers[o]
		if !ok {
			continue
		}
		values[n] = &Value{
			Number:        v.Number,
			Replacements:  v.Replacements,
			ExpiresAtUnix: v.ExpiresAtUnix,
			Deps:          slices.Clone(v.Deps),
			Complete:      slices.Clone(v.Complete),
			Offered:       slices.Clone(v.Offered),
		}
	}

	// The stored parts the merged entries lack.
	var offers []pendingOffer
	expiries := map[dagql.TransferOrdinal]int64{}
	for _, v := range mb.Bundle.Values {
		expiries[v.Ordinal] = v.ExpiresAtUnix
	}
	for _, o := range mb.Bundle.Outputs {
		if o.Chain == nil || o.Value == nil || len(o.Address.OutputPath) != 0 {
			continue
		}
		service, ok := mb.Numbers[o.Ordinal]
		target := values[service]
		if !ok || target == nil || target.has(o.Address) {
			continue
		}
		var owner dagql.PersistedOfferOwner
		if o.Owner != nil {
			missing := false
			for _, d := range o.Owner.DependencyIDs {
				mv := byOrdinal[dagql.TransferOrdinal(d)]
				if mv == nil {
					missing = true
					break
				}
				owner.DependencyIDs = append(owner.DependencyIDs, mv.Number)
			}
			if missing {
				continue
			}
		}
		offers = append(offers, pendingOffer{
			number: target.Number,
			part: protocol.CloudPart{
				Offer: dagql.PersistedPartOffer{
					Address: o.Address,
					Value:   *o.Value,
					Chain:   *o.Chain,
					Owner:   owner,
				},
				CloudNumber:        service,
				CloudExpiresAtUnix: expiries[o.Ordinal],
				CloudStored:        true,
			},
		})
	}

	im.mu.Lock()
	if !im.currentLocked(eng, st) {
		im.mu.Unlock()
		return MergedBundle{}, nil, errStateReset
	}
	st.merged[c.Key()] = &out
	delete(st.failures, c.Key())
	for n, v := range values {
		st.values[n] = v
	}
	im.mu.Unlock()
	im.log.Info("import: merged", "cacheID", eng.CacheID(), "bundle", c.Bundle,
		"roots", len(c.Roots), "values", len(reply.Values), "retained", len(out.Retained),
		"expired", len(out.Expired), "offers", len(offers))
	return out, offers, nil
}

// offerPending sends the pending offers, one item per engine entry, and
// records their outcomes. Offers that may succeed later stay pending, up to
// OfferAttempts tries.
func (im *Importer) offerPending(ctx context.Context, eng Engine, st *cacheState) (OfferCounts, error) {
	var counts OfferCounts
	im.mu.Lock()
	pending := st.pending
	st.pending = nil
	im.mu.Unlock()
	if len(pending) == 0 {
		return counts, nil
	}
	requeue := func(ps []pendingOffer) {
		im.mu.Lock()
		if im.currentLocked(eng, st) {
			st.pending = append(st.pending, ps...)
		}
		im.mu.Unlock()
	}

	// Fresh addresses, since a retry may come after they expired.
	for i := range pending {
		chain := &pending[i].part.Offer.Chain
		addrs := make(map[digest.Digest]dagql.BlobAddress, len(chain.Layers))
		for _, l := range chain.Layers {
			addr, err := im.sign(ctx, l.Descriptor.Digest)
			if err != nil {
				requeue(pending)
				return counts, fmt.Errorf("sign %s: %w", l.Descriptor.Digest, err)
			}
			addrs[l.Descriptor.Digest] = addr
		}
		chain.Addresses = addrs
	}

	return im.sendOffers(ctx, eng, st, pending, requeue)
}

// sendOffers sends pending offers in one offer request, one item per engine
// entry, and records their outcomes. A request over the message limit is
// split in two, by items, or by parts when it has one item; a single part
// too large to send is dropped.
func (im *Importer) sendOffers(ctx context.Context, eng Engine, st *cacheState, pending []pendingOffer, requeue func([]pendingOffer)) (OfferCounts, error) {
	var counts OfferCounts
	var order []uint64
	items := map[uint64][]int{}
	for i, p := range pending {
		if _, ok := items[p.number]; !ok {
			order = append(order, p.number)
		}
		items[p.number] = append(items[p.number], i)
	}
	req := protocol.Offer{Items: make([]protocol.OfferItem, 0, len(order))}
	for _, n := range order {
		item := protocol.OfferItem{Number: n}
		for _, i := range items[n] {
			item.Offers = append(item.Offers, pending[i].part)
		}
		req.Items = append(req.Items, item)
	}
	body, err := im.encode(req)
	if err != nil {
		if len(pending) == 1 {
			im.log.Warn("import: offer of one part too large; dropped", "number", pending[0].number, "error", err)
			return counts, nil
		}
		// Split by items, keeping each item whole, or, with one item,
		// by parts.
		var first, rest []pendingOffer
		if len(order) > 1 {
			half := map[uint64]bool{}
			for _, n := range order[:len(order)/2] {
				half[n] = true
			}
			for _, p := range pending {
				if half[p.number] {
					first = append(first, p)
				} else {
					rest = append(rest, p)
				}
			}
		} else {
			first, rest = pending[:len(pending)/2], pending[len(pending)/2:]
		}
		for i, part := range [][]pendingOffer{first, rest} {
			c, err := im.sendOffers(ctx, eng, st, part, requeue)
			counts.add(c)
			if err != nil {
				if i == 0 {
					requeue(rest)
				}
				return counts, err
			}
		}
		return counts, nil
	}
	rctx, cancel := context.WithTimeout(ctx, im.cfg.RequestTimeout)
	defer cancel()
	env, err := eng.Request(rctx, protocol.TypeOffer, body)
	if err != nil {
		requeue(pending)
		if ctx.Err() == nil && rctx.Err() != nil {
			return counts, fmt.Errorf("offer: no reply in %s", im.cfg.RequestTimeout)
		}
		return counts, &connectionError{fmt.Errorf("offer: %w", err)}
	}
	var offered protocol.Offered
	err = replyError(env)
	if err == nil && env.Type != protocol.TypeOffered {
		err = fmt.Errorf("offer answered with %s", env.Type)
	}
	if err == nil {
		err = env.Decode(&offered)
	}
	if err == nil && len(offered.Items) != len(req.Items) {
		err = fmt.Errorf("offered: %d items for %d", len(offered.Items), len(req.Items))
	}
	if err != nil {
		retry := bumpAttempts(pending, im.cfg.OfferAttempts)
		requeue(retry)
		return counts, fmt.Errorf("offer: %w", err)
	}

	var retry []pendingOffer
	im.mu.Lock()
	live := im.currentLocked(eng, st)
	for i, answer := range offered.Items {
		n := req.Items[i].Number
		sent := items[n]
		if answer.Gone {
			counts.Gone++
			if live {
				for s, v := range st.values {
					if v.Number == n {
						delete(st.values, s)
					}
				}
			}
			continue
		}
		outcomes := map[string]protocol.OfferedPart{}
		for _, part := range answer.Parts {
			outcomes[addressKey(part.Address)] = part
		}
		for _, j := range sent {
			p := pending[j]
			part, ok := outcomes[addressKey(p.part.Offer.Address)]
			if !ok {
				part.Outcome = protocol.OfferUnavailable
			}
			target := st.values[p.part.CloudNumber]
			switch part.Outcome {
			case protocol.OfferAccepted:
				counts.Accepted++
				if live && target != nil && target.Number == n {
					target.Offered = addAddress(target.Offered, p.part.Offer.Address)
				}
			case protocol.OfferAlreadyComplete:
				counts.AlreadyComplete++
				if live && target != nil && target.Number == n {
					target.Complete = addAddress(target.Complete, p.part.Offer.Address)
				}
			case protocol.OfferExecutionStarted:
				counts.ExecutionStarted++
				retry = append(retry, p)
			case protocol.OfferInvalid:
				counts.Invalid++
				im.log.Warn("import: offer invalid", "cacheID", eng.CacheID(), "number", n, "address", addressKey(p.part.Offer.Address), "message", part.Message)
			default:
				counts.Unavailable++
				retry = append(retry, p)
			}
		}
		if live && answer.Replacements != 0 {
			for _, v := range st.values {
				if v.Number == n && answer.Replacements >= v.Replacements {
					v.Replacements = answer.Replacements
					if answer.Deps != nil {
						v.Deps = slices.Clone(answer.Deps)
					}
				}
			}
		}
	}
	if live {
		st.offers.add(counts)
	}
	im.mu.Unlock()
	requeue(bumpAttempts(retry, im.cfg.OfferAttempts))
	im.log.Info("import: offered", "cacheID", eng.CacheID(), "items", len(req.Items),
		"accepted", counts.Accepted, "alreadyComplete", counts.AlreadyComplete,
		"executionStarted", counts.ExecutionStarted, "unavailable", counts.Unavailable,
		"invalid", counts.Invalid, "gone", counts.Gone)
	return counts, nil
}

// bumpAttempts counts one more try of each offer, and keeps those with tries
// left.
func bumpAttempts(ps []pendingOffer, limit int) []pendingOffer {
	var out []pendingOffer
	for _, p := range ps {
		p.attempts++
		if p.attempts < limit {
			out = append(out, p)
		}
	}
	return out
}

// Renew answers an engine's renew: a fresh download address for every layer,
// or unavailable when the renewal key isn't this scope's, a layer's digest is
// invalid, or a layer is missing from the blob store.
func (im *Importer) Renew(ctx context.Context, req protocol.Renew) protocol.Renewed {
	unavailable := protocol.Renewed{Unavailable: true}
	if req.RenewalKey != im.cfg.Scope || len(req.Layers) == 0 {
		return unavailable
	}
	addrs := make(map[digest.Digest]dagql.BlobAddress, len(req.Layers))
	for _, l := range req.Layers {
		d := l.Descriptor.Digest
		if err := d.Validate(); err != nil {
			return unavailable
		}
		if _, ok := addrs[d]; ok {
			continue
		}
		has, err := im.cfg.Blobs.Has(ctx, d)
		if err != nil || !has {
			im.log.Warn("import: renew of a missing layer", "digest", d, "error", err)
			return unavailable
		}
		addr, err := im.sign(ctx, d)
		if err != nil {
			return unavailable
		}
		addrs[d] = addr
	}
	return protocol.Renewed{Addresses: addrs}
}

// State is what the importer knows of one engine cache.
type State struct {
	CacheID    string         `json:"cacheID"`
	Generation uint64         `json:"generation"`
	Merged     []MergedBundle `json:"merged"`
	// Failures counts failed merges, by bundle ID and source cache.
	Failures map[string]int   `json:"failures,omitempty"`
	Values   map[uint64]Value `json:"values"`
	Pending  int              `json:"pending"`
	Offers   OfferCounts      `json:"offers"`
}

// State returns what the importer knows of cacheID, and false when it knows
// nothing.
func (im *Importer) State(cacheID string) (State, bool) {
	im.mu.Lock()
	defer im.mu.Unlock()
	st := im.caches[cacheID]
	if st == nil || st.merged == nil {
		return State{}, false
	}
	out := State{
		CacheID:    cacheID,
		Generation: st.generation,
		Merged:     make([]MergedBundle, 0, len(st.merged)),
		Failures:   maps.Clone(st.failures),
		Values:     make(map[uint64]Value, len(st.values)),
		Pending:    len(st.pending),
		Offers:     st.offers,
	}
	for _, m := range st.merged {
		cp := *m
		cp.Roots = slices.Clone(m.Roots)
		cp.Retained = maps.Clone(m.Retained)
		cp.Expired = slices.Clone(m.Expired)
		out.Merged = append(out.Merged, cp)
	}
	slices.SortFunc(out.Merged, func(a, b MergedBundle) int {
		return cmp.Or(cmp.Compare(a.Bundle, b.Bundle), cmp.Compare(a.Source, b.Source))
	})
	// Copies: offers append to the live slices.
	for n, v := range st.values {
		cp := *v
		cp.Deps = slices.Clone(v.Deps)
		cp.Complete = slices.Clone(v.Complete)
		cp.Offered = slices.Clone(v.Offered)
		out.Values[n] = cp
	}
	return out, true
}

// encode encodes a request body, refusing one whose envelope would be over
// the message limit.
func (im *Importer) encode(body any) (json.RawMessage, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	const envelopeOverhead = 128
	if len(data)+envelopeOverhead > im.cfg.MaxMessageBytes {
		return nil, fmt.Errorf("%s: %d bytes is over the %d byte message limit", protocol.TooLargeMessage, len(data), im.cfg.MaxMessageBytes)
	}
	return data, nil
}

// replyError is the error of an error reply, nil for any other.
func replyError(env protocol.Envelope) error {
	if env.Type != protocol.TypeError {
		return nil
	}
	var e protocol.Error
	if err := env.Decode(&e); err != nil {
		return err
	}
	return errors.New(e.Message)
}
