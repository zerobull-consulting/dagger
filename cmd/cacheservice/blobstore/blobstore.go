// Package blobstore stores the remote cache's layer blobs, keyed by digest.
//
// Engines upload and download blobs directly. The service hands the engine a
// presigned PUT for each blob it needs, and a presigned GET for each blob it
// offers, so the engine never holds the store's credentials and its uploads
// and downloads never pass through the service. The one read through the
// service is a service-hashed promotion (see S3Store.Promote).
//
// An upload doesn't go to the blob's digest key. A presigned PUT bounds the
// body's size, not its content, so it goes to a staging key of its own,
// under a Stage that one upload request names. Once the engine reports it
// done, Promote checks the staged object's content against the digest and
// only then copies it to the digest key. Has and PresignGet look at digest
// keys alone, so a stored blob is a verified one; nothing staged is ever
// reported, offered or served.
package blobstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
	"golang.org/x/sync/errgroup"
)

// Store holds blobs keyed by digest.
type Store interface {
	// Has reports whether the blob is stored at its digest key: verified.
	Has(ctx context.Context, dgst digest.Digest) (bool, error)
	// PresignPut returns an address that accepts a PUT of exactly size
	// bytes, staged under stage: not at the digest key, and not reported by
	// Has, until Promote verifies it. The PUT must carry Content-Length and
	// needs no other headers. size must be positive.
	PresignPut(ctx context.Context, stage Stage, dgst digest.Digest, size int64) (Address, error)
	// PresignGet returns an address that serves the blob at its digest key
	// to a plain GET.
	PresignGet(ctx context.Context, dgst digest.Digest) (Address, error)
	// Promote verifies the blob staged under stage against dgst and size,
	// and copies it to the digest key only if it matches, without
	// overwriting a blob already there. A staged object it decided on
	// (promoted, already stored, or a mismatch) is deleted. Its outcome says
	// what it found; an error is a failure of the store, after which the
	// staged object, if any, is left to the bucket's lifecycle rule.
	Promote(ctx context.Context, stage Stage, dgst digest.Digest, size int64) (Promotion, error)
}

// Stage names the staging area of one upload request: its blobs are PUT
// under it, and promoted from it. It is random, so two uploads of one blob,
// by two exports, never write the same staging key, and a staging key can't
// be guessed from a digest.
type Stage string

// stageBytes is how much randomness a Stage has.
const stageBytes = 16

// NewStage returns a new random Stage.
func NewStage() Stage {
	b := make([]byte, stageBytes)
	_, _ = rand.Read(b) // never fails (crypto/rand)
	return Stage(hex.EncodeToString(b))
}

// Validate reports whether s could have come from NewStage.
func (s Stage) Validate() error {
	if len(s) != 2*stageBytes {
		return fmt.Errorf("invalid stage %q", string(s))
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("invalid stage %q", string(s))
		}
	}
	return nil
}

// Outcome is what Promote found.
type Outcome string

const (
	// Promoted verified the staged blob and copied it to its digest key.
	Promoted Outcome = "promoted"
	// Existing found the blob at its digest key already: stored by an
	// earlier upload, or by another export racing this one. Nothing was
	// written to the digest key.
	Existing Outcome = "existing"
	// Mismatch found staged content that doesn't hash to the digest, or
	// isn't of the declared size. Nothing was written to the digest key.
	Mismatch Outcome = "mismatch"
	// Missing found nothing staged: the engine reported a blob done that it
	// never uploaded.
	Missing Outcome = "missing"
)

// Who hashed a promoted blob.
const (
	// HashedByStore is a hash the store computed as it copied the blob
	// (S3's checksum of an UploadPartCopy).
	HashedByStore = "store"
	// HashedByService is a hash the service computed by reading the blob.
	HashedByService = "service"
)

// Promotion is what one Promote did.
type Promotion struct {
	Outcome Outcome
	// HashedBy is who hashed the staged blob (HashedByStore or
	// HashedByService), and Bytes how many bytes were hashed; "" and 0 when
	// nothing was hashed (Existing, Missing).
	HashedBy string
	Bytes    int64
	// Duration is how long Promote took, waiting for a slot included.
	Duration time.Duration
}

// Stored reports whether the blob is at its digest key after the promotion.
func (p Promotion) Stored() bool { return p.Outcome == Promoted || p.Outcome == Existing }

// Namespacer is a Store that can keep each namespace's blobs apart. The
// service stores every namespace's blobs under its own keys (see the README,
// "Auth and namespaces"), so a blob uploaded by one namespace is never served
// to, or counted as stored for, another.
type Namespacer interface {
	// Namespace returns the store of one namespace's blobs. It makes no
	// requests.
	Namespace(ns string) (Store, error)
}

// ValidateNamespace reports whether ns can name a namespace's keys: one key
// segment. The service checks namespaces against metastore.ValidateScope,
// which is stricter.
func ValidateNamespace(ns string) error {
	if ns == "" || ns == "." || ns == ".." || strings.ContainsAny(ns, "/\\") {
		return fmt.Errorf("invalid blob namespace %q", ns)
	}
	return nil
}

// Address is a presigned URL and the time it stops working.
type Address struct {
	URL       string
	ExpiresAt time.Time
}

// Blob is a blob the service may need, with its size in bytes.
type Blob struct {
	Digest digest.Digest
	Size   int64
}

// maxConcurrentChecks bounds the Has calls UploadURLs makes at once.
const maxConcurrentChecks = 16

// UploadURLs returns a presigned PUT for each blob the store doesn't have yet,
// keyed by digest: the URLs of an upload request, staged under stage. Blobs
// the store has already (at their digest keys: verified) are left out, so
// they are not requested again. A blob listed twice is checked once; listing
// it with two sizes, or with a size that isn't positive, is an error.
func UploadURLs(ctx context.Context, store Store, stage Stage, blobs []Blob) (map[digest.Digest]string, error) {
	if err := stage.Validate(); err != nil {
		return nil, err
	}
	sizes := make(map[digest.Digest]int64, len(blobs))
	for _, b := range blobs {
		if err := b.Digest.Validate(); err != nil {
			return nil, fmt.Errorf("blob %q: %w", b.Digest, err)
		}
		if b.Size <= 0 {
			return nil, fmt.Errorf("blob %s: size %d is not positive", b.Digest, b.Size)
		}
		if prev, ok := sizes[b.Digest]; ok && prev != b.Size {
			return nil, fmt.Errorf("blob %s listed with sizes %d and %d", b.Digest, prev, b.Size)
		}
		sizes[b.Digest] = b.Size
	}
	// Sorted, so errors and calls come in a stable order.
	unique := make([]digest.Digest, 0, len(sizes))
	for d := range sizes {
		unique = append(unique, d)
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })

	// At most maxConcurrentChecks goroutines exist at once: Go blocks until
	// one finishes. The first error cancels the rest and is the one returned.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentChecks)
	var (
		mu   sync.Mutex
		urls = make(map[digest.Digest]string)
	)
	for _, d := range unique {
		if gctx.Err() != nil {
			break
		}
		g.Go(func() error {
			url, err := uploadURL(gctx, store, stage, d, sizes[d])
			if err != nil {
				return fmt.Errorf("blob %s: %w", d, err)
			}
			if url != "" {
				mu.Lock()
				urls[d] = url
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	// Set when the caller's context ended before any check failed.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return urls, nil
}

func uploadURL(ctx context.Context, store Store, stage Stage, d digest.Digest, size int64) (string, error) {
	has, err := store.Has(ctx, d)
	if err != nil {
		return "", fmt.Errorf("check: %w", err)
	}
	if has {
		return "", nil
	}
	addr, err := store.PresignPut(ctx, stage, d, size)
	if err != nil {
		return "", fmt.Errorf("presign PUT: %w", err)
	}
	return addr.URL, nil
}

// maxConcurrentPromotions bounds the Promote calls PromoteAll makes at once.
// A store may bound them further across calls (S3Config.MaxPromotions).
const maxConcurrentPromotions = 8

// PromoteResult is the promotion of one blob by PromoteAll.
type PromoteResult struct {
	Blob
	Promotion
	// Err is the store's failure; Promotion is then empty.
	Err error
}

// PromoteAll promotes each blob staged under stage, at most
// maxConcurrentPromotions at a time, and returns their results in blobs'
// order. A blob listed twice is promoted once, and reported for each.
func PromoteAll(ctx context.Context, store Store, stage Stage, blobs []Blob) []PromoteResult {
	out := make([]PromoteResult, len(blobs))
	first := map[digest.Digest]int{}
	var wg sync.WaitGroup
	slots := make(chan struct{}, maxConcurrentPromotions)
	for i, b := range blobs {
		out[i].Blob = b
		if _, dup := first[b.Digest]; dup {
			continue
		}
		first[b.Digest] = i
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			out[i].Err = context.Cause(ctx)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			out[i].Promotion, out[i].Err = store.Promote(ctx, stage, b.Digest, b.Size)
		}()
	}
	wg.Wait()
	for i, b := range blobs {
		if j := first[b.Digest]; j != i {
			out[i].Promotion, out[i].Err = out[j].Promotion, out[j].Err
		}
	}
	return out
}
