package metastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dagger/dagger/dagql"
)

// ResolutionVersion is the resolution format this package writes. It refuses
// to read another one.
const ResolutionVersion = 1

// Resolution is how one export resolved a stored bundle's values to service
// numbers, when Record recorded it: the service number of each value, by
// ordinal. It is what a merge sends as each value's SenderNumber.
//
// A bundle names its values by the exporting cache's engine numbers
// (SenderNumber). Those of the values the cache had imported resolve through
// its imported mapping (EngineCache.Imported), which moves when the cache has
// the same stored entry merged onto another engine entry, and is dropped when
// the cache is wiped. Resolving a stored bundle through the mapping at merge
// time would then fail (#47). Bundles never change, so how they resolved when
// recorded doesn't either: Record writes it once, and the importer merges
// with it.
//
// A resolution is stored as <prefix>/<scope>/resolutions/<id>.json, where id
// is the SHA-256 of its JSON: written once, never changed, like a bundle. It
// names its bundle and cache because a bundle two caches exported resolves to
// different service numbers, so each gets a resolution of its own; two
// exports that resolve a bundle the same way share one. Each entry Record
// writes names its resolution (Entry.Resolution).
//
// A resolution only says what a value's number was. Whether that entry is
// still stored is the index's to say: a merge sends only the numbers the
// index still holds, so a resolution neither keeps an entry alive nor names
// one DropExpired dropped. Service numbers are never reused, so a dropped
// number can't come to name another entry.
type Resolution struct {
	Version int `json:"version"`
	// Bundle is the ID of the bundle resolved.
	Bundle string `json:"bundle"`
	// CacheID is the engine cache that exported it.
	CacheID string `json:"cacheID"`
	// Numbers are the service numbers of the bundle's values: Numbers[i]
	// is the value at ordinal i+1's.
	Numbers []uint64 `json:"numbers"`
}

// NewResolution returns the resolution of a bundle of len(numbers) values,
// exported from cacheID, whose values got the given service numbers, by
// ordinal. Ordinals must run from 1 to the number of values.
func NewResolution(bundleID, cacheID string, numbers map[dagql.TransferOrdinal]uint64) (*Resolution, error) {
	r := &Resolution{
		Version: ResolutionVersion,
		Bundle:  bundleID,
		CacheID: cacheID,
		Numbers: make([]uint64, len(numbers)),
	}
	for o, n := range numbers {
		if o == 0 || int(o) > len(numbers) {
			return nil, fmt.Errorf("resolution: ordinal %d out of range [1, %d]", o, len(numbers))
		}
		r.Numbers[o-1] = n
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	return r, nil
}

// Number returns the service number of the value at ordinal o.
func (r *Resolution) Number(o dagql.TransferOrdinal) (uint64, bool) {
	if r == nil || o == 0 || int(o) > len(r.Numbers) {
		return 0, false
	}
	return r.Numbers[o-1], true
}

func (r *Resolution) validate() error {
	if r.Version != ResolutionVersion {
		return fmt.Errorf("resolution version %d, this service reads %d", r.Version, ResolutionVersion)
	}
	if !bundleIDPattern.MatchString(r.Bundle) {
		return fmt.Errorf("resolution: invalid bundle ID %q", r.Bundle)
	}
	if r.CacheID == "" {
		return errors.New("resolution: no cacheID")
	}
	if len(r.Numbers) == 0 {
		return errors.New("resolution: no values")
	}
	seen := make(map[uint64]bool, len(r.Numbers))
	for i, n := range r.Numbers {
		if n == 0 {
			return fmt.Errorf("resolution: value %d has no number", i+1)
		}
		if seen[n] {
			return fmt.Errorf("resolution: number %d appears twice", n)
		}
		seen[n] = true
	}
	return nil
}

// Check reports whether r resolves bundle bundleID, exported from cacheID:
// it names them, and has a number for each of the bundle's values.
func (r *Resolution) Check(bundleID, cacheID string, bundle *dagql.ValueBundle) error {
	if r.Bundle != bundleID || r.CacheID != cacheID {
		return fmt.Errorf("resolution of bundle %s from %q, not of bundle %s from %q", r.Bundle, r.CacheID, bundleID, cacheID)
	}
	if len(r.Numbers) != len(bundle.Values) {
		return fmt.Errorf("resolution of %d values, for a bundle of %d", len(r.Numbers), len(bundle.Values))
	}
	return nil
}

// ResolutionID returns the ID a resolution is stored under, the SHA-256 of
// its JSON, and the JSON.
func ResolutionID(r *Resolution) (string, []byte, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return "", nil, fmt.Errorf("encode resolution: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), body, nil
}

// ResolutionKey returns the key of a resolution:
// <prefix>/<scope>/resolutions/<id>.json.
func (s *Store) ResolutionKey(scope, id string) (string, error) {
	if !bundleIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid resolution ID %q", id)
	}
	return s.scopeKey(scope, "resolutions/"+id+".json")
}

// PutResolution stores a resolution and returns its ID. Storing one that is
// already stored writes nothing and returns the same ID; a stored object at
// its key whose content doesn't match is ErrCorruptBundle.
func (s *Store) PutResolution(ctx context.Context, scope string, r *Resolution) (string, error) {
	if err := r.validate(); err != nil {
		return "", fmt.Errorf("put resolution: %w", err)
	}
	id, body, err := ResolutionID(r)
	if err != nil {
		return "", err
	}
	key, err := s.ResolutionKey(scope, id)
	if err != nil {
		return "", err
	}
	if err := s.putImmutable(ctx, key, body); err != nil {
		return "", fmt.Errorf("put resolution %s: %w", id, err)
	}
	return id, nil
}

// GetResolution returns a stored resolution, or an error wrapping
// ErrNotFound.
func (s *Store) GetResolution(ctx context.Context, scope, id string) (*Resolution, error) {
	key, err := s.ResolutionKey(scope, id)
	if err != nil {
		return nil, err
	}
	body, _, err := s.objects.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get resolution: %w", err)
	}
	if sum := sha256.Sum256(body); hex.EncodeToString(sum[:]) != id {
		return nil, fmt.Errorf("get resolution %s: %w", id, ErrCorruptBundle)
	}
	var r Resolution
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("decode resolution %s: %w", id, err)
	}
	if err := r.validate(); err != nil {
		return nil, fmt.Errorf("resolution %s: %w", id, err)
	}
	return &r, nil
}

// putImmutable writes a content-addressed object once: If-None-Match: *,
// and, when the key exists, a check that what is there is body.
func (s *Store) putImmutable(ctx context.Context, key string, body []byte) error {
	for attempt := 1; ; attempt++ {
		_, err := s.objects.Put(ctx, key, body, Condition{IfNoneMatch: true})
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			return err
		}
		// The key exists, or a write of it is in flight (S3's 409).
		stored, _, err := s.objects.Get(ctx, key)
		if err == nil {
			if !bytes.Equal(stored, body) {
				return ErrCorruptBundle
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if attempt >= s.maxAttempts {
			return ErrContention
		}
		if err := s.sleep(ctx, backoff(attempt)); err != nil {
			return err
		}
	}
}
