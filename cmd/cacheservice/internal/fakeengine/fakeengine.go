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

	"github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
	"github.com/dagger/dagger/engine/snapshots"
)

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
}

// Cache is a fake engine cache.
type Cache struct {
	ID         string
	Generation uint64
	// MaxRoots, when set, makes an export of more roots fail as too large.
	MaxRoots int

	mu      sync.Mutex
	entries map[uint64]*Entry
	exports int
	// inFlight and peak count exports whose chains are held.
	inFlight, peak int
	// hold, when set, blocks each export until it is closed.
	hold chan struct{}
}

// New returns an empty cache.
func New(id string, generation uint64) *Cache {
	return &Cache{ID: id, Generation: generation, entries: map[uint64]*Entry{}}
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

// Capture is one export's bundle and the blobs it can upload.
type Capture struct {
	Bundle  *dagql.ValueBundle
	Skipped []protocol.SkippedRoot
	Blobs   map[digest.Digest][]byte
}

// ErrTooLarge is the export of more than MaxRoots roots.
var ErrTooLarge = errors.New(protocol.TooLargeMessage)

// Capture captures an export: the surviving roots' closure, with the
// requested outputs inside it.
func (c *Cache) Capture(req protocol.Export) (Capture, error) {
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
	b := &dagql.ValueBundle{Version: dagql.ValueBundleVersion}
	for _, n := range order {
		e := c.entries[n]
		v := dagql.TransferredValue{
			Ordinal:            ordinal[n],
			SenderNumber:       n,
			SenderReplacements: e.Replacements,
			Record:             dagql.PersistedRecord{ResultID: n},
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
	for _, o := range req.Outputs {
		e := c.entries[o.Number]
		if !seen[o.Number] || o.Address.Part != FSPart.Part || len(e.Layers) == 0 {
			continue
		}
		chain := &dagql.OfferedChain{}
		for _, l := range e.Layers {
			d := digest.FromBytes(l)
			capt.Blobs[d] = l
			chain.Layers = append(chain.Layers, snapshots.ExportLayer{Descriptor: ocispecs.Descriptor{
				MediaType: ocispecs.MediaTypeImageLayerGzip,
				Digest:    d,
				Size:      int64(len(l)),
			}})
		}
		b.Outputs = append(b.Outputs, dagql.TransferredOutput{
			Ordinal: ordinal[o.Number],
			Address: o.Address,
			State:   "completed",
			Value:   &dagql.SnapshotValue{Kind: "directory", Path: "/"},
			Chain:   chain,
		})
	}
	capt.Bundle = b
	return capt, nil
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

func (a Adapter) Export(ctx context.Context, req protocol.Export, consume func(context.Context, remotecache.Export) error) error {
	capt, err := a.Cache.Capture(req)
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
	return consume(ctx, remotecache.Export{Bundle: capt.Bundle, Skipped: capt.Skipped, Blobs: blobSource(capt.Blobs)})
}

func (Adapter) Merge(context.Context, protocol.Merge) (protocol.Merged, error) {
	return protocol.Merged{}, errors.New("merge: not supported by the fake")
}

func (Adapter) OfferParts(context.Context, protocol.Offer) (protocol.Offered, error) {
	return protocol.Offered{}, errors.New("offer: not supported by the fake")
}

func (Adapter) TakeRenewalRequest(ctx context.Context) (*dagql.RenewalRequest, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (Adapter) ReplyRenewal(dagql.RenewalReply) dagql.RenewalReplyDisposition {
	return dagql.RenewalReplyDiscarded
}

type blobSource map[digest.Digest][]byte

func (b blobSource) ReadBlob(_ context.Context, d digest.Digest) (io.ReadCloser, int64, error) {
	data, ok := b[d]
	if !ok {
		return nil, 0, fmt.Errorf("blob %s is not in the export's chains", d)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
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
