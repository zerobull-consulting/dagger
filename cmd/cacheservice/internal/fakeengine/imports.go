package fakeengine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// The import side of the fake: Merge and OfferParts as the engine answers
// them, Pull to read an offered part's layers from their download addresses,
// as a cache hit on an imported entry does, and Renew to ask the service for
// fresh addresses through the engine's own session.
//
// A record lands on the entry of its recipe, the call field Capture writes.
// An entry with layers has its fs part complete; one without takes the
// offered part.

// Lookup returns the entry of a recipe, as a cache lookup would.
func (c *Cache) Lookup(recipe string) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.byRecipeLocked(recipe)
	if e == nil {
		return Entry{}, false
	}
	return copyEntry(e), true
}

// Entry returns an entry by number.
func (c *Cache) Entry(n uint64) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[n]
	if e == nil {
		return Entry{}, false
	}
	return copyEntry(e), true
}

// Merges returns how many merges were committed.
func (c *Cache) Merges() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.merges
}

// OnOffer makes hook decide offers' outcomes first; an empty outcome leaves
// the offer to the fake.
func (c *Cache) OnOffer(hook func(number uint64, offer protocol.CloudPart) protocol.OfferOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offerHook = hook
}

func copyEntry(e *Entry) Entry {
	cp := *e
	cp.Deps = slices.Clone(e.Deps)
	cp.Layers = slices.Clone(e.Layers)
	return cp
}

func (c *Cache) byRecipeLocked(recipe string) *Entry {
	if recipe == "" {
		return nil
	}
	var best *Entry
	for _, e := range c.entries {
		if e.Recipe == recipe && (best == nil || e.Number < best.Number) {
			best = e
		}
	}
	return best
}

// Merge merges a bundle: every value the roots need lands on its recipe's
// entry, or a new one. Entries with layers keep them; the others take the
// bundle's fs outputs as offers.
func (c *Cache) Merge(b dagql.ValueBundle) (protocol.Merged, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(b.Roots) == 0 {
		return protocol.Merged{}, errors.New("bundle has no roots")
	}
	values := map[dagql.TransferOrdinal]*dagql.TransferredValue{}
	senders := map[uint64]bool{}
	for i := range b.Values {
		v := &b.Values[i]
		if v.SenderNumber == 0 || senders[v.SenderNumber] {
			return protocol.Merged{}, fmt.Errorf("value %d: bad sender number %d", v.Ordinal, v.SenderNumber)
		}
		senders[v.SenderNumber] = true
		values[v.Ordinal] = v
	}
	outputs := map[dagql.TransferOrdinal]*dagql.TransferredOutput{}
	for i := range b.Outputs {
		o := &b.Outputs[i]
		if o.Address.Part == FSPart.Part && o.Chain != nil {
			outputs[o.Ordinal] = o
		}
	}
	now := time.Now()
	landed := map[dagql.TransferOrdinal]*Entry{}
	var order []dagql.TransferOrdinal
	var land func(dagql.TransferOrdinal) error
	land = func(o dagql.TransferOrdinal) error {
		if landed[o] != nil {
			return nil
		}
		v := values[o]
		if v == nil {
			return fmt.Errorf("ordinal %d is not in the bundle", o)
		}
		if v.Record.Call == nil || v.Record.Call.Field == "" {
			return fmt.Errorf("value %d has no recipe", o)
		}
		var deps []uint64
		for _, d := range v.DependencyIDs {
			if err := land(dagql.TransferOrdinal(d)); err != nil {
				return err
			}
			deps = append(deps, landed[dagql.TransferOrdinal(d)].Number)
		}
		e := c.byRecipeLocked(v.Record.Call.Field)
		if e == nil {
			e = &Entry{Number: c.next, Recipe: v.Record.Call.Field, Deps: deps}
			c.next++
			c.entries[e.Number] = e
		}
		e.CloudNumber = v.SenderNumber
		if out := outputs[o]; out != nil && len(e.Layers) == 0 && !c.NoMergeOffers {
			offer := dagql.PersistedPartOffer{Address: out.Address, Value: *out.Value, Chain: *out.Chain}
			e.Offer = &offer
		}
		landed[o] = e
		order = append(order, o)
		return nil
	}
	for _, r := range b.Roots {
		if r.ExpiresAtUnix != 0 && r.ExpiresAtUnix <= now.Unix() {
			continue
		}
		if err := land(r.Ordinal); err != nil {
			return protocol.Merged{}, err
		}
	}
	c.merges++
	merged := protocol.Merged{Generation: c.Generation, EngineTime: now.UnixNano(), Retained: []protocol.RetainedRoot{}}
	slices.Sort(order)
	for _, o := range order {
		e := landed[o]
		mv := protocol.MergedValue{Ordinal: o, Number: e.Number, Replacements: e.Replacements, Deps: slices.Clone(e.Deps), Complete: []dagql.PersistedPartAddress{}, Offered: []dagql.PersistedPartAddress{}}
		if mv.Deps == nil {
			mv.Deps = []uint64{}
		}
		switch {
		case len(e.Layers) > 0:
			mv.Complete = append(mv.Complete, FSPart)
		case e.Offer != nil:
			mv.Offered = append(mv.Offered, FSPart)
		}
		merged.Values = append(merged.Values, mv)
	}
	for _, r := range b.Roots {
		if r.ExpiresAtUnix != 0 && r.ExpiresAtUnix <= now.Unix() {
			merged.Skipped = append(merged.Skipped, protocol.SkippedValue{Ordinal: r.Ordinal, Reason: protocol.SkipExpired})
			continue
		}
		merged.Retained = append(merged.Retained, protocol.RetainedRoot{Ordinal: r.Ordinal, ExpiresAtUnix: r.ExpiresAtUnix})
	}
	return merged, nil
}

// DropOffer forgets an entry's offered part, as when the engine refused it
// at merge time, so a later offer can place it.
func (c *Cache) DropOffer(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[n]; e != nil {
		e.Offer = nil
	}
}

// OfferParts places offered fs parts on entries by number.
func (c *Cache) OfferParts(req protocol.Offer) protocol.Offered {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := protocol.Offered{Items: make([]protocol.OfferedItem, 0, len(req.Items))}
	for _, item := range req.Items {
		e := c.entries[item.Number]
		if e == nil {
			out.Items = append(out.Items, protocol.OfferedItem{Number: item.Number, Gone: true})
			continue
		}
		answer := protocol.OfferedItem{Number: item.Number, Replacements: e.Replacements, Deps: slices.Clone(e.Deps)}
		for _, offer := range item.Offers {
			part := protocol.OfferedPart{Address: offer.Offer.Address}
			if c.offerHook != nil {
				part.Outcome = c.offerHook(item.Number, offer)
			}
			switch {
			case part.Outcome != "":
			case offer.Offer.Address.Part != FSPart.Part || offer.CloudNumber == 0:
				part.Outcome, part.Message = protocol.OfferInvalid, "not an fs part"
			case len(e.Layers) > 0:
				part.Outcome = protocol.OfferAlreadyComplete
				e.CloudNumber = offer.CloudNumber
			default:
				cp := offer.Offer
				e.Offer = &cp
				e.CloudNumber = offer.CloudNumber
				part.Outcome = protocol.OfferAccepted
			}
			answer.Parts = append(answer.Parts, part)
		}
		out.Items = append(out.Items, answer)
	}
	return out
}

// ErrNoOffer is a pull of an entry with neither layers nor an offer.
var ErrNoOffer = errors.New("entry has no layers and no offer")

// Pull reads entry n's fs part: its own layers, or its offered layers from
// their download addresses, each checked against its digest and size. An
// offered part pulled becomes the entry's own. It reports whether the layers
// were downloaded.
func (c *Cache) Pull(ctx context.Context, client *http.Client, n uint64) ([][]byte, bool, error) {
	c.mu.Lock()
	e := c.entries[n]
	if e == nil {
		c.mu.Unlock()
		return nil, false, fmt.Errorf("entry %d is gone", n)
	}
	if len(e.Layers) > 0 {
		layers := slices.Clone(e.Layers)
		c.mu.Unlock()
		return layers, false, nil
	}
	if e.Offer == nil {
		c.mu.Unlock()
		return nil, false, ErrNoOffer
	}
	chain := e.Offer.Chain
	c.mu.Unlock()
	if client == nil {
		client = http.DefaultClient
	}
	var layers [][]byte
	for _, l := range chain.Layers {
		addr, ok := chain.Addresses[l.Descriptor.Digest]
		if !ok {
			return nil, false, fmt.Errorf("layer %s has no address", l.Descriptor.Digest)
		}
		data, err := get(ctx, client, addr.URL)
		if err != nil {
			return nil, false, fmt.Errorf("layer %s: %w", l.Descriptor.Digest, err)
		}
		if int64(len(data)) != l.Descriptor.Size || digest.FromBytes(data) != l.Descriptor.Digest {
			return nil, false, fmt.Errorf("layer %s: content doesn't match its descriptor", l.Descriptor.Digest)
		}
		layers = append(layers, data)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[n]; e != nil {
		e.Layers = slices.Clone(layers)
		e.Offer = nil
	}
	return layers, true, nil
}

func get(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET: status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// Renew asks the service, through the engine's session, for fresh addresses
// of entry n's offered layers, as a read whose addresses expired does, and
// stores them in the offer. It reports false when the service answered
// unavailable.
func (c *Cache) Renew(ctx context.Context, n uint64) (bool, error) {
	c.mu.Lock()
	e := c.entries[n]
	if e == nil || e.Offer == nil {
		c.mu.Unlock()
		return false, ErrNoOffer
	}
	chain := e.Offer.Chain
	c.renewSeq++
	req := &dagql.RenewalRequest{
		ID:         dagql.RenewalRequestID{Sequence: c.renewSeq},
		RenewalKey: chain.RenewalKey,
		Layers:     slices.Clone(chain.Layers),
		NeededBlob: chain.Layers[0].Descriptor.Digest,
		Deadline:   time.Now().Add(time.Minute),
	}
	reply := make(chan dagql.RenewalReply, 1)
	c.renewed[req.ID] = reply
	c.mu.Unlock()
	done := make(chan struct{})
	defer close(done)
	req.Done = done
	select {
	case c.renewals <- req:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case r := <-reply:
		if r.Unavailable {
			return false, nil
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if e := c.entries[n]; e != nil && e.Offer != nil {
			e.Offer.Chain.Addresses = r.Addresses
		}
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
