// Package fakeengine is an engine cache for the service's tests: entries by
// number, each with dependencies and the layer blobs of its "fs" part, that
// answers exports the way the engine does. Adapter serves it to a real
// remotecache.Run, so tests can drive the engine's own side of the protocol.
package fakeengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/snapshots"
)

// layerCreatedAt is every fake layer's creation time.
var layerCreatedAt = time.Date(2026, 11, 2, 7, 52, 43, 412316627, time.UTC)

// FSPart is the part address the fake's layers belong to.
var FSPart = dagql.PersistedPartAddress{Part: "fs"}

// Entry is one cache entry.
type Entry struct {
	Number       uint64
	Replacements uint64
	Deps         []uint64
	// Layers are the blobs of the entry's fs part, base first.
	Layers [][]byte
	// Busy and Refused make the engine skip the entry as a root.
	Busy    bool
	Refused string
	// Recipe names what the entry computes, as a recipe digest does: a
	// merged record lands on the entry of the same recipe. Exported records
	// carry it as their call's field.
	Recipe string
	// CloudNumber is the service's number for the entry, from a merge or an
	// offer (0: none).
	CloudNumber uint64
	// Offer is the fs part offered by the service, not yet pulled.
	Offer *dagql.PersistedPartOffer
}

// Cache is a fake engine cache.
type Cache struct {
	ID         string
	Generation uint64
	// MaxRoots, when set, makes an export of more roots fail as too large.
	MaxRoots int
	// NoMergeOffers makes merges place no offers, as when the engine's
	// part is being computed, so only an offer can place them.
	NoMergeOffers bool
	// CreatedAtAnnotation makes exports annotate a layer the cache exported
	// before with buildkit/createdat, as the real engine does: a layer it
	// compresses for an export has no creation time in its metadata yet, and
	// every later export of it reads one back.
	CreatedAtAnnotation bool
	// ChainPrefixes makes exports send an output whose layers are the first
	// of another output's chain as a prefix of that chain, with no chain of
	// its own, as the engine does since v1.0.0-beta.26.
	ChainPrefixes bool
	// ComputedLayers makes exports mark the layers the cache hadn't
	// exported before as computed by the export, as the engine does.
	ComputedLayers bool

	mu      sync.Mutex
	entries map[uint64]*Entry
	exports int
	// inFlight and peak count exports whose chains are held.
	inFlight, peak int
	// hold, when set, blocks each export until it is closed.
	hold chan struct{}
	// stall, when set, blocks each upload halfway through its body until
	// it is closed; stalled counts the uploads it blocked.
	stall   chan struct{}
	stalled int
	// next is the next number a merge hands out.
	next uint64
	// merges counts the merges committed.
	merges int
	// offerHook, when set, decides an offer's outcome before the fake does.
	offerHook func(number uint64, offer protocol.CloudPart) protocol.OfferOutcome

	// compressed are the layers an export has compressed.
	compressed map[digest.Digest]bool
	// corrupt counts the uploads of each blob still to corrupt (-1: all).
	corrupt map[digest.Digest]int

	renewals chan *dagql.RenewalRequest
	renewed  map[dagql.RenewalRequestID]chan dagql.RenewalReply
	renewSeq uint64

	// landed are the entries a merge landed on, with their replacement
	// count then: while a connection's known rows hold one, unchanged, an
	// export sends it as a held row.
	landed map[uint64]uint64
	// heldExports counts the exports that sent a held row.
	heldExports int
	// settle is the engine's side of settling (SetSettleState), and
	// settleChanged closes when it next changes; settled are the Settled
	// calls.
	settle        remotecache.SettleState
	settleChanged chan struct{}
	settled       []uint64
	// misses are the lookup misses to send (Miss).
	misses chan []dagql.RemoteMiss
}

// New returns an empty cache.
func New(id string, generation uint64) *Cache {
	return &Cache{
		ID:         id,
		Generation: generation,
		entries:    map[uint64]*Entry{},
		next:       1000,
		compressed: map[digest.Digest]bool{},
		corrupt:    map[digest.Digest]int{},
		renewals:   make(chan *dagql.RenewalRequest),
		renewed:    map[dagql.RenewalRequestID]chan dagql.RenewalReply{},
		landed:     map[uint64]uint64{},
		misses:     make(chan []dagql.RemoteMiss, 16),
	}
}

// SetSettleState sets the engine's side of settling with the service, which
// the connection acts on: an open for a new period, claims, and a settle
// once no session is live.
func (c *Cache) SetSettleState(st remotecache.SettleState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.settle = st
	c.notifySettleLocked()
}

func (c *Cache) notifySettleLocked() {
	if c.settleChanged != nil {
		close(c.settleChanged)
		c.settleChanged = nil
	}
}

// SettledPeriods are the periods of the service's Settled replies, in order.
func (c *Cache) SettledPeriods() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.settled)
}

// Miss queues lookup misses, which a connection with Lookups on sends.
func (c *Cache) Miss(misses ...dagql.RemoteMiss) {
	c.misses <- misses
}

// HeldExports is how many exports sent a held row.
func (c *Cache) HeldExports() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.heldExports
}

// Add adds or replaces an entry.
func (c *Cache) Add(e Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := e
	c.entries[e.Number] = &cp
}

// Remove drops an entry, as a prune would.
func (c *Cache) Remove(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, n)
}

// Exports returns how many exports were captured.
func (c *Cache) Exports() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exports
}

// Peak returns the most exports whose chains were held at once.
func (c *Cache) Peak() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peak
}

// InFlight returns how many exports have their chains held now.
func (c *Cache) InFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inFlight
}

// Hold makes exports wait, holding their chains, until the returned function
// is called.
func (c *Cache) Hold() (release func()) {
	ch := make(chan struct{})
	c.mu.Lock()
	c.hold = ch
	c.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

// StallUploads makes each upload from now on send half its body, then wait,
// as an upload to a store that stopped reading would, until the returned
// function is called or the upload is given up. It stalls the upload in the
// engine, so it works against any store, a real bucket included.
func (c *Cache) StallUploads() (release func()) {
	ch := make(chan struct{})
	c.mu.Lock()
	c.stall = ch
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			if c.stall == ch {
				c.stall = nil
			}
			c.mu.Unlock()
			close(ch)
		})
	}
}

// StalledUploads is how many uploads StallUploads has blocked so far.
func (c *Cache) StalledUploads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stalled
}

// Capture is one export's bundle and the blobs it can upload.
type Capture struct {
	Bundle  *dagql.ValueBundle
	Skipped []protocol.SkippedRoot
	Blobs   map[digest.Digest][]byte
	// Rows are the rows the bundle carries, in full and held.
	Rows dagql.ExportedRows
}

// ErrTooLarge is the export of more than MaxRoots roots.
var ErrTooLarge = errors.New(protocol.TooLargeMessage)

// Capture captures an export: the surviving roots' closure, with the
// requested outputs inside it.
func (c *Cache) Capture(req protocol.Export) (Capture, error) {
	return c.CaptureKnown(req, nil)
}

// knows reports whether known holds row n. KnownRows exports no query, so it
// forgets the row and looks at what that did; a row it held is put back.
func knows(known *dagql.KnownRows, n uint64, rev dagql.RowRevision) bool {
	if known == nil {
		return false
	}
	before := known.Len()
	known.NotMerged(dagql.ExportedRows{Held: []uint64{n}})
	if known.Len() == before {
		return false
	}
	known.Merged(dagql.ExportedRows{Full: map[uint64]dagql.RowRevision{n: rev}})
	return true
}

// CaptureKnown is Capture for a connection whose service holds the known
// rows: as the engine does since v1.0.0-beta.26, it sends each entry a merge
// of the service's landed on, and that hasn't changed since, as a held row,
// with no record and nothing below it.
func (c *Cache) CaptureKnown(req protocol.Export, known *dagql.KnownRows) (Capture, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MaxRoots > 0 && len(req.Roots) > c.MaxRoots {
		return Capture{}, ErrTooLarge
	}
	var capt Capture
	var roots []uint64
	for _, n := range req.Roots {
		e := c.entries[n]
		switch {
		case e == nil:
			capt.Skipped = append(capt.Skipped, protocol.SkippedRoot{Number: n, Reason: protocol.SkipGone})
		case e.Busy:
			capt.Skipped = append(capt.Skipped, protocol.SkippedRoot{Number: n, Reason: protocol.SkipBusy, Message: "busy"})
		case e.Refused != "":
			capt.Skipped = append(capt.Skipped, protocol.SkippedRoot{Number: n, Reason: protocol.SkipRefused, Message: e.Refused})
		default:
			roots = append(roots, n)
		}
	}
	if len(roots) == 0 {
		return capt, nil
	}
	c.exports++
	// The closure, dependencies first.
	var order []uint64
	seen := map[uint64]bool{}
	held := map[uint64]bool{}
	var visit func(uint64) error
	visit = func(n uint64) error {
		if seen[n] {
			return nil
		}
		seen[n] = true
		e := c.entries[n]
		if e == nil {
			return fmt.Errorf("entry %d: dependency missing", n)
		}
		if rev, ok := c.landed[n]; ok && rev == e.Replacements && knows(known, n, dagql.RowRevision{Replacements: rev}) {
			// A held row stands for its closure.
			held[n] = true
			order = append(order, n)
			return nil
		}
		for _, d := range e.Deps {
			if err := visit(d); err != nil {
				return err
			}
		}
		order = append(order, n)
		return nil
	}
	for _, n := range roots {
		if err := visit(n); err != nil {
			return Capture{}, err
		}
	}
	ordinal := map[uint64]dagql.TransferOrdinal{}
	for i, n := range order {
		ordinal[n] = dagql.TransferOrdinal(i + 1)
	}
	b := &dagql.ValueBundle{Version: metastore.BundleVersion}
	capt.Rows.Full = map[uint64]dagql.RowRevision{}
	for _, n := range order {
		e := c.entries[n]
		if held[n] {
			capt.Rows.Held = append(capt.Rows.Held, n)
			b.Values = append(b.Values, dagql.TransferredValue{
				Ordinal:            ordinal[n],
				SenderNumber:       n,
				SenderReplacements: e.Replacements,
				Record:             dagql.PersistedRecord{ResultID: uint64(ordinal[n])},
				Held:               digest.FromString(e.Recipe),
			})
			continue
		}
		capt.Rows.Full[n] = dagql.RowRevision{Replacements: e.Replacements}
		v := dagql.TransferredValue{
			Ordinal:            ordinal[n],
			SenderNumber:       n,
			SenderReplacements: e.Replacements,
			Record:             dagql.PersistedRecord{ResultID: n},
		}
		if e.Recipe != "" {
			v.Record.Call = &dagql.ResultCall{Field: e.Recipe}
		}
		// As the engine does, a part offered and not yet pulled is exported
		// in the record, as the offer it holds, addresses and all.
		if e.Offer != nil {
			v.Record.Envelope.PendingOffers = []dagql.PersistedPartOffer{*e.Offer}
		}
		for _, d := range e.Deps {
			v.DependencyIDs = append(v.DependencyIDs, uint64(ordinal[d]))
		}
		b.Values = append(b.Values, v)
	}
	for _, n := range roots {
		b.Roots = append(b.Roots, dagql.TransferredRoot{Ordinal: ordinal[n]})
	}
	capt.Blobs = map[digest.Digest][]byte{}
	if len(capt.Rows.Held) > 0 {
		c.heldExports++
	}
	for _, o := range req.Outputs {
		e := c.entries[o.Number]
		if !seen[o.Number] || held[o.Number] || o.Address.Part != FSPart.Part || len(e.Layers) == 0 {
			continue
		}
		chain := &dagql.OfferedChain{}
		for _, l := range e.Layers {
			d := digest.FromBytes(l)
			capt.Blobs[d] = l
			created := layerCreatedAt
			layer := snapshots.ExportLayer{CreatedAt: &created, Descriptor: ocispecs.Descriptor{
				MediaType: ocispecs.MediaTypeImageLayerGzip,
				Digest:    d,
				Size:      int64(len(l)),
			}}
			if c.CreatedAtAnnotation && c.compressed[d] {
				layer.Descriptor.Annotations = map[string]string{"buildkit/createdat": created.Format(time.RFC3339Nano)}
			}
			layer.Computed = c.ComputedLayers && !c.compressed[d]
			c.compressed[d] = true
			chain.Layers = append(chain.Layers, layer)
		}
		b.Outputs = append(b.Outputs, dagql.TransferredOutput{
			Ordinal: ordinal[o.Number],
			Address: o.Address,
			State:   "completed",
			Value:   &dagql.SnapshotValue{Kind: "directory", Path: "/"},
			Chain:   chain,
		})
	}
	if c.ChainPrefixes {
		prefixChains(b)
	}
	capt.Bundle = b
	return capt, nil
}

// prefixChains sends each output whose layers are the first of a longer
// output's chain as a prefix of the longest such chain.
func prefixChains(b *dagql.ValueBundle) {
	for i := range b.Outputs {
		short := b.Outputs[i].Chain
		if short == nil {
			continue
		}
		best := -1
		for j := range b.Outputs {
			long := b.Outputs[j].Chain
			if i == j || long == nil || len(long.Layers) <= len(short.Layers) {
				continue
			}
			same := true
			for k, l := range short.Layers {
				if long.Layers[k].Descriptor.Digest != l.Descriptor.Digest {
					same = false
					break
				}
			}
			if same && (best < 0 || len(long.Layers) > len(b.Outputs[best].Chain.Layers)) {
				best = j
			}
		}
		if best >= 0 {
			b.Outputs[i].Prefix = &dagql.ChainPrefix{Ordinal: b.Outputs[best].Ordinal, Address: b.Outputs[best].Address, Layers: len(short.Layers)}
		}
	}
	for i := range b.Outputs {
		if b.Outputs[i].Prefix != nil {
			b.Outputs[i].Chain = nil
		}
	}
}

// held counts an export whose chains are held, and waits for Hold's release.
func (c *Cache) held(ctx context.Context) (func(), error) {
	c.mu.Lock()
	c.inFlight++
	c.peak = max(c.peak, c.inFlight)
	hold := c.hold
	c.mu.Unlock()
	done := func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			done()
			return nil, ctx.Err()
		}
	}
	return done, nil
}

// Adapter serves the cache to remotecache.Run.
type Adapter struct {
	Cache *Cache
}

var _ remotecache.Adapter = Adapter{}

func (a Adapter) CacheIdentity() (string, uint64) { return a.Cache.ID, a.Cache.Generation }

func (a Adapter) Export(ctx context.Context, req protocol.Export, known *dagql.KnownRows, consume func(context.Context, remotecache.Export) error) error {
	capt, err := a.Cache.CaptureKnown(req, known)
	if err != nil {
		return err
	}
	if capt.Bundle == nil {
		return consume(ctx, remotecache.Export{Skipped: capt.Skipped})
	}
	done, err := a.Cache.held(ctx)
	if err != nil {
		return err
	}
	defer done()
	return consume(ctx, remotecache.Export{Bundle: capt.Bundle, Skipped: capt.Skipped, Blobs: blobSource{cache: a.Cache, blobs: capt.Blobs}, Rows: capt.Rows})
}

// Merge merges the bundle, and the rows it landed on join known: the
// service holds the records it sent.
func (a Adapter) Merge(_ context.Context, _ uint64, req protocol.Merge, known *dagql.KnownRows) (protocol.Merged, error) {
	merged, err := a.Cache.Merge(req.Bundle)
	if err != nil {
		return merged, err
	}
	rows := dagql.ExportedRows{Full: map[uint64]dagql.RowRevision{}}
	a.Cache.mu.Lock()
	for _, v := range merged.Values {
		a.Cache.landed[v.Number] = v.Replacements
		rows.Full[v.Number] = dagql.RowRevision{Replacements: v.Replacements}
	}
	a.Cache.mu.Unlock()
	if known != nil {
		known.Merged(rows)
	}
	return merged, nil
}

func (a Adapter) StartLookups() {}
func (a Adapter) StopLookups()  {}

// TakeLookupMisses waits for the misses of the next Miss call.
func (a Adapter) TakeLookupMisses(ctx context.Context, _ int, _ time.Duration) ([]dagql.RemoteMiss, uint64, error) {
	select {
	case misses := <-a.Cache.misses:
		return misses, 0, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (a Adapter) SettleState() (remotecache.SettleState, <-chan struct{}) {
	a.Cache.mu.Lock()
	defer a.Cache.mu.Unlock()
	if a.Cache.settleChanged == nil {
		a.Cache.settleChanged = make(chan struct{})
	}
	st := a.Cache.settle
	st.Sessions = slices.Clone(st.Sessions)
	st.Claims = slices.Clone(st.Claims)
	return st, a.Cache.settleChanged
}

// Settled records the service's Settled, as the engine does: the period is
// settled and its sessions covered, unless the state moved on.
func (a Adapter) Settled(period uint64, _ []protocol.SettleSession) {
	a.Cache.mu.Lock()
	defer a.Cache.mu.Unlock()
	a.Cache.settled = append(a.Cache.settled, period)
	if a.Cache.settle.Period == period && !a.Cache.settle.Live {
		a.Cache.settle.Settled = true
		a.Cache.settle.Sessions = nil
		a.Cache.notifySettleLocked()
	}
}

func (a Adapter) OfferParts(_ context.Context, req protocol.Offer) (protocol.Offered, error) {
	return a.Cache.OfferParts(req), nil
}

func (a Adapter) TakeRenewalRequest(ctx context.Context) (*dagql.RenewalRequest, error) {
	select {
	case req := <-a.Cache.renewals:
		return req, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a Adapter) ReplyRenewal(reply dagql.RenewalReply) dagql.RenewalReplyDisposition {
	a.Cache.mu.Lock()
	ch := a.Cache.renewed[reply.ID]
	delete(a.Cache.renewed, reply.ID)
	a.Cache.mu.Unlock()
	if ch == nil {
		return dagql.RenewalReplyDiscarded
	}
	ch <- reply
	return dagql.RenewalReplyAccepted
}

type blobSource struct {
	cache *Cache
	blobs map[digest.Digest][]byte
}

func (b blobSource) ReadBlob(ctx context.Context, d digest.Digest) (io.ReadCloser, int64, error) {
	data, ok := b.blobs[d]
	if !ok {
		return nil, 0, fmt.Errorf("blob %s is not in the export's chains", d)
	}
	data = b.cache.UploadBody(d, data)
	b.cache.mu.Lock()
	stall := b.cache.stall
	b.cache.mu.Unlock()
	if stall == nil || len(data) < 2 {
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
	}
	half := len(data) / 2
	body := io.MultiReader(bytes.NewReader(data[:half]), &stallReader{ctx: ctx, cache: b.cache, release: stall}, bytes.NewReader(data[half:]))
	return io.NopCloser(body), int64(len(data)), nil
}

// stallReader blocks its first read until release is closed or ctx ends,
// then reads nothing more.
type stallReader struct {
	ctx     context.Context
	cache   *Cache
	release chan struct{}
}

func (r *stallReader) Read([]byte) (int, error) {
	r.cache.mu.Lock()
	r.cache.stalled++
	r.cache.mu.Unlock()
	select {
	case <-r.release:
		return 0, io.EOF
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	}
}

// CorruptUploads makes the next n uploads of blob d send a body of the
// right size with other content, as a buggy or compromised engine could; n
// < 0 corrupts every one from now on, and 0 none. The engine still reports
// each upload done: S3 takes the body, since a presigned PUT bounds only its
// size.
func (c *Cache) CorruptUploads(d digest.Digest, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.corrupt[d] = n
}

// UploadBody is the body an upload of blob d sends: data, or a corrupted
// copy of the same size (see CorruptUploads).
func (c *Cache) UploadBody(d digest.Digest, data []byte) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.corrupt[d]
	if n == 0 || len(data) == 0 {
		return data
	}
	if n > 0 {
		c.corrupt[d] = n - 1
	}
	out := bytes.Clone(data)
	out[0] ^= 0xff
	return out
}

// SortedDigests returns a capture's blob digests, sorted.
func (c Capture) SortedDigests() []digest.Digest {
	out := make([]digest.Digest, 0, len(c.Blobs))
	for d := range c.Blobs {
		out = append(out, d)
	}
	slices.Sort(out)
	return out
}
