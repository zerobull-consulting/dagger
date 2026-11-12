package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/dagql"
)

// This file holds what garbage collection (package gc) needs of the index:
// roots' max age, and the deletion markers that keep an export from naming
// an object GC is deleting.

// CapExpiry returns when a root expires under a namespace's max age: its own
// expiry, or maxAge after it was recorded, whichever comes first (0:
// never). A root with no recording time yet, and any root when maxAge is 0,
// keeps its own expiry. Times are whole seconds: a max age with a fraction
// of a second counts as the next whole second, so a root is never cut
// short of it by more than the recording time's own rounding.
func CapExpiry(r Root, maxAge time.Duration) int64 {
	if maxAge <= 0 || r.RecordedAtUnix == 0 {
		return r.ExpiresAtUnix
	}
	secs := int64(maxAge / time.Second)
	if maxAge%time.Second != 0 {
		secs++
	}
	aged := r.RecordedAtUnix + max(secs, 1)
	if aged < r.RecordedAtUnix { // overflow: no cap
		return r.ExpiresAtUnix
	}
	if r.ExpiresAtUnix == 0 || aged < r.ExpiresAtUnix {
		return aged
	}
	return r.ExpiresAtUnix
}

// RootExpiry returns when root n expires under maxAge (see CapExpiry), or 0
// when it isn't a root.
func (idx *Index) RootExpiry(n uint64, maxAge time.Duration) int64 {
	r, ok := idx.Roots[n]
	if !ok {
		return 0
	}
	return CapExpiry(r, maxAge)
}

// ApplyMaxAge caps every root's expiry at maxAge after it was recorded, in
// this copy of the index. It is for a snapshot that is read, not written
// back: the import path applies it, so it neither merges a root past its
// max age nor sends engines a later expiry than that. Written back, it would
// keep the cap after the max age was raised.
func (idx *Index) ApplyMaxAge(maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	for n, r := range idx.Roots {
		r.ExpiresAtUnix = CapExpiry(r, maxAge)
		idx.Roots[n] = r
	}
}

// StampRoots gives every root with no recording time nowUnix, and returns
// how many it stamped: roots recorded before version 3 get a whole max age
// from the first collection that sees them.
func (idx *Index) StampRoots(nowUnix int64) int {
	stamped := 0
	for n, r := range idx.Roots {
		if r.RecordedAtUnix == 0 {
			r.RecordedAtUnix = nowUnix
			idx.Roots[n] = r
			stamped++
		}
	}
	return stamped
}

// Collection is what garbage collection keeps in the index: the objects it
// is deleting, or deleted lately, by key relative to the scope
// ("blobs/<alg>/<hex>", "bundles/<id>.json", "resolutions/<id>.json").
//
// Objects are content-addressed, so an export can name one that GC decided
// to delete because nothing named it: an export whose blobs the store
// already has (Has, before the upload), or a bundle or resolution stored by
// an earlier export. Index writes are the only point both sides serialize
// on, so GC claims each object in an index update before deleting it, and
// an export's index write (Store.RecordFenced) refuses to name an object:
//
//   - that is being deleted (Deletion.Epoch is 0); or
//   - whose deletion finished in a round after the export's Fence: the
//     export checked the object before it was deleted, so it may be gone.
//
// An export the fence refuses fails with ErrCollected, and is tried again
// from the start: its new checks see what is stored then.
type Collection struct {
	// Epoch counts the deletion rounds that finished. Each round's
	// deletions carry the epoch it reached, and GC writes the latest to
	// the scope's epoch object (Store.WriteEpoch) after the index, for
	// exports to read before they check anything (Store.ReadEpoch).
	Epoch uint64 `json:"epoch,omitempty"`
	// Deletions are the objects being deleted or deleted lately, by key
	// relative to the scope. A finished deletion is kept for
	// DeletionRetention, longer than any export may take (MaxFenceAge).
	Deletions map[string]Deletion `json:"deletions,omitempty"`
}

// Deletion is one object GC claimed.
type Deletion struct {
	// Run is the GC run that claimed it, and StartedUnix when.
	Run         string `json:"run"`
	StartedUnix int64  `json:"started"`
	// Epoch is the epoch of the round that deleted it, and DoneUnix when
	// that round finished; both 0 while it is being deleted.
	Epoch    uint64 `json:"epoch,omitempty"`
	DoneUnix int64  `json:"done,omitempty"`
}

// InProgress reports whether the deletion hasn't finished.
func (d Deletion) InProgress() bool { return d.Epoch == 0 }

const (
	// MaxFenceAge bounds an export, from its fence (read before its first
	// check of the store) to its index write: RecordFenced refuses a fence
	// older than this. An export waits up to an hour for its uploads
	// (exporter.DefaultUploadTimeout), so this leaves it two more.
	MaxFenceAge = 3 * time.Hour
	// DeletionRetention is how long a finished deletion stays in the
	// index. It is longer than MaxFenceAge, so no export that checked an
	// object before it was deleted can still record it once its marker is
	// gone, with an hour to spare for clocks of different replicas.
	DeletionRetention = MaxFenceAge + time.Hour
)

// ErrCollected is an export refused because an object it names is being
// deleted by GC, or was deleted after the export checked it, or because the
// export took longer than MaxFenceAge. Try the export again from the start.
var ErrCollected = errors.New("the export names an object garbage collection is deleting or deleted after the export checked it")

// ErrStaleGeneration is an export refused because it came from an older
// generation of its engine cache than the index has recorded, and would add
// a value the index has no entry for (see Index.RecordResolvedAt). Trying it
// again can't succeed: the engine's generation doesn't change.
var ErrStaleGeneration = errors.New("a late export from an older generation of the engine cache names a value the index doesn't hold")

// Fence is what an export knew of GC's deletions when it started: the
// epoch it read (Store.ReadEpoch) before checking any object, and when.
type Fence struct {
	Epoch   uint64
	Started time.Time
}

// noFence is Record's fence: it refuses only objects being deleted.
var noFence = Fence{Epoch: math.MaxUint64}

// ValidateCollectable reports whether key, relative to a scope, names an
// object GC may delete: a blob's digest key, a bundle or a resolution.
func ValidateCollectable(key string) error {
	switch k, _ := layout.Classify(key); k {
	case layout.KindBlob, layout.KindBundle, layout.KindResolution:
		return nil
	}
	return fmt.Errorf("not a collectable key: %q", key)
}

func (c *Collection) validate() error {
	if c == nil {
		return nil
	}
	for k, d := range c.Deletions {
		if err := ValidateCollectable(k); err != nil {
			return fmt.Errorf("gc deletion: %w", err)
		}
		if d.Epoch > c.Epoch {
			return fmt.Errorf("gc deletion %s: epoch %d is after the collection's %d", k, d.Epoch, c.Epoch)
		}
	}
	return nil
}

// Keys of a scope's objects, relative to the scope.

// BundleRelKey is a bundle's key relative to its scope.
func BundleRelKey(id string) string { return layout.BundleRel(id) }

// ResolutionRelKey is a resolution's key relative to its scope.
func ResolutionRelKey(id string) string { return layout.ResolutionRel(id) }

// BlobRelKey is a blob's digest key relative to its namespace, as the blob
// store lays it out: blobs/<algorithm>/<hex>.
func BlobRelKey(d digest.Digest) string { return layout.BlobRel(d) }

// BundleBlobs returns the blobs a bundle names, each once, sorted: every
// layer of its outputs' chains and of its records' pending offers. Those
// are the blobs a merge of the bundle signs GETs for, so GC keeps every one
// of them while an entry names the bundle. Invalid digests are left out.
func BundleBlobs(bundle *dagql.ValueBundle) []digest.Digest {
	seen := map[digest.Digest]bool{}
	add := func(c *dagql.OfferedChain) {
		if c == nil {
			return
		}
		for _, l := range c.Layers {
			if d := l.Descriptor.Digest; d.Validate() == nil {
				seen[d] = true
			}
		}
	}
	for _, o := range bundle.Outputs {
		add(o.Chain)
	}
	for i := range bundle.Values {
		for j := range bundle.Values[i].Record.Envelope.PendingOffers {
			add(&bundle.Values[i].Record.Envelope.PendingOffers[j].Chain)
		}
	}
	out := make([]digest.Digest, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	slices.Sort(out)
	return out
}

// checkFence refuses keys GC is deleting, or deleted in a round after
// fence.Epoch.
func (idx *Index) checkFence(keys []string, fence Fence) error {
	if idx.GC == nil {
		return nil
	}
	for _, k := range keys {
		d, ok := idx.GC.Deletions[k]
		if !ok {
			continue
		}
		if d.InProgress() {
			return fmt.Errorf("%s is being deleted: %w", k, ErrCollected)
		}
		if d.Epoch > fence.Epoch {
			return fmt.Errorf("%s was deleted (round %d) after the export checked it (round %d): %w", k, d.Epoch, fence.Epoch, ErrCollected)
		}
	}
	return nil
}

// epochKey is the scope's epoch object: <prefix>/ns/<scope>/gc-epoch.json.
func (s *Store) epochKey(scope string) (string, error) {
	return s.scopeKey(scope, layout.EpochName)
}

// EpochKey returns the key of a scope's epoch object.
func (s *Store) EpochKey(scope string) (string, error) { return s.epochKey(scope) }

type epochObject struct {
	Epoch uint64 `json:"epoch"`
}

// ReadEpoch returns the epoch GC last published for the scope: 0 when it
// has published none. An export reads it before it checks any object, and
// records with it (Fence). The object is tiny, unlike the index; it may lag
// the index's epoch (GC writes it after), which only makes exports refuse
// more, never less.
func (s *Store) ReadEpoch(ctx context.Context, scope string) (uint64, error) {
	key, err := s.epochKey(scope)
	if err != nil {
		return 0, err
	}
	body, _, err := s.objects.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read gc epoch: %w", err)
	}
	var e epochObject
	if err := json.Unmarshal(body, &e); err != nil {
		return 0, fmt.Errorf("read gc epoch %s: %w", key, err)
	}
	return e.Epoch, nil
}

// WriteEpoch publishes the scope's epoch, after the index update that
// reached it. It only ever raises the published epoch: a conditional write
// (If-Match the epoch object read, or If-None-Match: * when there is none),
// so a replica that publishes late never lowers what another published.
// An epoch that lags the index's only makes exports refuse more, never
// less; GC republishes the index's epoch at each run when it lags.
func (s *Store) WriteEpoch(ctx context.Context, scope string, epoch uint64) error {
	key, err := s.epochKey(scope)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(epochObject{Epoch: epoch})
	for attempt := 1; ; attempt++ {
		cur, etag, err := s.objects.Get(ctx, key)
		cond := Condition{IfNoneMatch: true}
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return fmt.Errorf("write gc epoch: %w", err)
		default:
			var e epochObject
			if json.Unmarshal(cur, &e) == nil && e.Epoch >= epoch {
				return nil
			}
			cond = Condition{IfMatch: etag}
		}
		_, err = s.objects.Put(ctx, key, body, cond)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			return fmt.Errorf("write gc epoch: %w", err)
		}
		if attempt >= s.maxAttempts {
			return fmt.Errorf("write gc epoch: %w", ErrContention)
		}
		if err := s.sleep(ctx, backoff(attempt)); err != nil {
			return err
		}
	}
}

// ScopePrefix returns the prefix of every key of a scope:
// <prefix>/ns/<scope>/.
func (s *Store) ScopePrefix(scope string) (string, error) {
	return s.scopeKey(scope, "")
}

// Prefix returns the store's prefix, without slashes: "" when the service
// owns the whole bucket.
func (s *Store) Prefix() string { return s.prefix }

// RelKey returns key relative to its scope's prefix, and false when key
// isn't under it.
func (s *Store) RelKey(scope, key string) (string, bool) {
	p, err := s.ScopePrefix(scope)
	if err != nil {
		return "", false
	}
	return strings.CutPrefix(key, p)
}
