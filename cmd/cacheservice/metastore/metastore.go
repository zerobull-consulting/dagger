// Package metastore stores the remote cache's metadata in an object store,
// next to the blobs, with no separate database.
//
// Each scope (a team, a tenant) has:
//
//   - bundles: <prefix>/<scope>/bundles/<id>.json, one per exported
//     dagql.ValueBundle. A bundle's ID is the SHA-256 of its JSON, so a bundle
//     is written once and never changes.
//   - an index: <prefix>/<scope>/index.json, one object naming every entry
//     the scope stores, its roots, expiries and dependencies, and which
//     engine entry numbers map to which service numbers. See Index.
//
// Every index update is a read, a change, and a conditional write: If-Match
// the ETag read, or If-None-Match: * when there was no index yet. A writer
// that loses reads again and retries, so concurrent writers, in one process
// or many, never lose each other's updates. All state is in the object store,
// so a restarted service picks up where it stopped.
package metastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"

	"github.com/dagger/dagger/dagql"
)

const (
	// DefaultMaxAttempts is how many times UpdateIndex tries a write before
	// it gives up with ErrContention.
	DefaultMaxAttempts = 50
	// minBackoff and maxBackoff bound the wait between attempts, which
	// doubles from minBackoff, with full jitter.
	minBackoff = 5 * time.Millisecond
	maxBackoff = time.Second
)

// ErrContention is an index update that lost its conditional write
// MaxAttempts times in a row.
var ErrContention = errors.New("index update lost to concurrent writers too many times")

// ErrCorruptBundle is a stored bundle whose content doesn't hash to its ID.
var ErrCorruptBundle = errors.New("bundle content doesn't match its ID")

// Config configures a Store.
type Config struct {
	// Objects holds the bundles and indexes.
	Objects Objects
	// Prefix is prepended to every key, with a slash. Empty stores scopes
	// at the top of the store.
	Prefix string
	// MaxAttempts bounds the writes UpdateIndex tries. Zero means
	// DefaultMaxAttempts.
	MaxAttempts int
}

// Store keeps bundles and indexes in Objects. It holds no state of its own,
// so any number of Stores, in any number of processes, can share Objects.
type Store struct {
	objects     Objects
	prefix      string
	maxAttempts int
	// sleep waits between attempts; tests replace it.
	sleep func(context.Context, time.Duration) error
}

// New returns a Store for cfg. It makes no requests.
func New(cfg Config) (*Store, error) {
	if cfg.Objects == nil {
		return nil, errors.New("metadata store: no object store")
	}
	if cfg.MaxAttempts < 0 {
		return nil, fmt.Errorf("metadata store: max attempts %d is negative", cfg.MaxAttempts)
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	return &Store{
		objects:     cfg.Objects,
		prefix:      strings.Trim(cfg.Prefix, "/"),
		maxAttempts: cfg.MaxAttempts,
		sleep:       sleepCtx,
	}, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// scopePattern is what a scope may be: one key segment, with nothing a path
// could misread.
var scopePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// reservedScope is the blob store's key segment under a shared prefix.
const reservedScope = "blobs"

// ValidateScope reports whether scope can name a scope's keys.
func ValidateScope(scope string) error {
	if !scopePattern.MatchString(scope) || strings.Contains(scope, "..") {
		return fmt.Errorf("invalid scope %q: want 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit, without '..'", scope)
	}
	if scope == reservedScope {
		return fmt.Errorf("invalid scope %q: the blob store uses it", scope)
	}
	return nil
}

// bundleIDPattern is a bundle ID: a SHA-256, in lowercase hex.
var bundleIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (s *Store) scopeKey(scope, rest string) (string, error) {
	if err := ValidateScope(scope); err != nil {
		return "", err
	}
	key := scope + "/" + rest
	if s.prefix != "" {
		key = s.prefix + "/" + key
	}
	return key, nil
}

// IndexKey returns the key of a scope's index: <prefix>/<scope>/index.json.
func (s *Store) IndexKey(scope string) (string, error) {
	return s.scopeKey(scope, "index.json")
}

// BundleKey returns the key of a bundle: <prefix>/<scope>/bundles/<id>.json.
func (s *Store) BundleKey(scope, id string) (string, error) {
	if !bundleIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid bundle ID %q", id)
	}
	return s.scopeKey(scope, "bundles/"+id+".json")
}

// BundleID returns the ID a bundle is stored under: the SHA-256 of its JSON.
func BundleID(bundle *dagql.ValueBundle) (string, []byte, error) {
	body, err := json.Marshal(bundle)
	if err != nil {
		return "", nil, fmt.Errorf("encode bundle: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), body, nil
}

// PutBundle stores a bundle and returns its ID. Storing a bundle that is
// already stored writes nothing and returns the same ID.
func (s *Store) PutBundle(ctx context.Context, scope string, bundle *dagql.ValueBundle) (string, error) {
	if bundle == nil {
		return "", errors.New("put bundle: nil bundle")
	}
	id, body, err := BundleID(bundle)
	if err != nil {
		return "", err
	}
	key, err := s.BundleKey(scope, id)
	if err != nil {
		return "", err
	}
	for attempt := 1; ; attempt++ {
		_, err = s.objects.Put(ctx, key, body, Condition{IfNoneMatch: true})
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			return "", fmt.Errorf("put bundle: %w", err)
		}
		// The key exists, or a write of it is in flight (S3's 409). Its
		// content is the same either way; only check that it landed.
		_, _, err = s.objects.Get(ctx, key)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return "", fmt.Errorf("put bundle: %w", err)
		}
		if attempt >= s.maxAttempts {
			return "", fmt.Errorf("put bundle %s: %w", id, ErrContention)
		}
		if err := s.sleep(ctx, backoff(attempt)); err != nil {
			return "", err
		}
	}
}

// GetBundle returns a stored bundle, or an error wrapping ErrNotFound.
func (s *Store) GetBundle(ctx context.Context, scope, id string) (*dagql.ValueBundle, error) {
	key, err := s.BundleKey(scope, id)
	if err != nil {
		return nil, err
	}
	body, _, err := s.objects.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get bundle: %w", err)
	}
	if sum := sha256.Sum256(body); hex.EncodeToString(sum[:]) != id {
		return nil, fmt.Errorf("get bundle %s: %w", id, ErrCorruptBundle)
	}
	var bundle dagql.ValueBundle
	if err := json.Unmarshal(body, &bundle); err != nil {
		return nil, fmt.Errorf("decode bundle %s: %w", id, err)
	}
	return &bundle, nil
}

// Index returns a scope's index as last written: an empty one when the scope
// has none yet. It is a snapshot; change the index with UpdateIndex.
func (s *Store) Index(ctx context.Context, scope string) (*Index, error) {
	idx, _, err := s.readIndex(ctx, scope)
	return idx, err
}

// readIndex returns the index and its ETag, or an empty index and "" when the
// scope has none.
func (s *Store) readIndex(ctx context.Context, scope string) (*Index, string, error) {
	key, err := s.IndexKey(scope)
	if err != nil {
		return nil, "", err
	}
	body, etag, err := s.objects.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return NewIndex(), "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("read index: %w", err)
	}
	idx, err := decodeIndex(body)
	if err != nil {
		return nil, "", fmt.Errorf("read index %s: %w", key, err)
	}
	if etag == "" {
		return nil, "", fmt.Errorf("read index %s: the store returned no ETag, so it can't be updated safely", key)
	}
	return idx, etag, nil
}

// UpdateIndex changes a scope's index atomically and returns the index it
// wrote. It reads the index, calls change on it, and writes it back only if
// no other writer wrote in between. If one did, it waits, reads the new
// index, and calls change again, up to MaxAttempts times; then it returns
// ErrContention.
//
// change may run several times, each on a fresh copy of the index, so it
// must decide only from the index it is given. An error from change ends the
// update with that error and writes nothing.
func (s *Store) UpdateIndex(ctx context.Context, scope string, change func(*Index) error) (*Index, error) {
	key, err := s.IndexKey(scope)
	if err != nil {
		return nil, err
	}
	for attempt := 1; ; attempt++ {
		idx, etag, err := s.readIndex(ctx, scope)
		if err != nil {
			return nil, err
		}
		if err := change(idx); err != nil {
			return nil, err
		}
		if err := idx.Validate(); err != nil {
			return nil, fmt.Errorf("update index %s: the change left it invalid: %w", key, err)
		}
		body, err := json.Marshal(idx)
		if err != nil {
			return nil, fmt.Errorf("encode index: %w", err)
		}
		cond := Condition{IfMatch: etag}
		if etag == "" {
			cond = Condition{IfNoneMatch: true}
		}
		_, err = s.objects.Put(ctx, key, body, cond)
		if err == nil {
			return idx, nil
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			return nil, fmt.Errorf("update index: %w", err)
		}
		if attempt >= s.maxAttempts {
			return nil, fmt.Errorf("update index %s after %d attempts: %w", key, attempt, ErrContention)
		}
		if err := s.sleep(ctx, backoff(attempt)); err != nil {
			return nil, err
		}
	}
}

// backoff is the wait after a lost attempt: a random duration up to
// minBackoff doubled per attempt, capped at maxBackoff.
func backoff(attempt int) time.Duration {
	ceil := maxBackoff
	if attempt < 20 {
		ceil = min(maxBackoff, minBackoff<<(attempt-1))
	}
	return time.Duration(rand.Int64N(int64(ceil))) + 1
}

// VerifyConditionalWrites checks that the object store honors the
// conditional writes UpdateIndex relies on. A store that ignored them would
// let concurrent writers overwrite each other's index updates. It writes
// <prefix>/<scope>/conditional-write-check/<nonce>, and deletes it again.
func (s *Store) VerifyConditionalWrites(ctx context.Context, scope string) (err error) {
	key, err := s.scopeKey(scope, fmt.Sprintf("conditional-write-check/%016x", rand.Uint64()))
	if err != nil {
		return err
	}
	defer func() {
		// Also after a failed check, which may have written the probe.
		if derr := s.objects.Delete(context.WithoutCancel(ctx), key); derr != nil && err == nil {
			err = fmt.Errorf("conditional write check: delete probe: %w", derr)
		}
	}()
	first, err := s.objects.Put(ctx, key, []byte(`{"check":1}`), Condition{IfNoneMatch: true})
	if err != nil {
		return fmt.Errorf("conditional write check: create: %w", err)
	}
	if _, err := s.objects.Put(ctx, key, []byte(`{"check":2}`), Condition{IfNoneMatch: true}); !errors.Is(err, ErrPreconditionFailed) {
		return fmt.Errorf("conditional write check: If-None-Match: * on an existing object: got %v, want a failed precondition", err)
	}
	second, err := s.objects.Put(ctx, key, []byte(`{"check":3}`), Condition{IfMatch: first})
	if err != nil {
		return fmt.Errorf("conditional write check: If-Match the current ETag: %w", err)
	}
	if second == first {
		return errors.New("conditional write check: the ETag didn't change when the object did")
	}
	if _, err := s.objects.Put(ctx, key, []byte(`{"check":4}`), Condition{IfMatch: first}); !errors.Is(err, ErrPreconditionFailed) {
		return fmt.Errorf("conditional write check: If-Match a stale ETag: got %v, want a failed precondition", err)
	}
	return nil
}
