// Package blobstore stores the remote cache's layer blobs, keyed by digest.
//
// Blobs never pass through the service. The service hands the engine a
// presigned PUT for each blob it needs, and a presigned GET for each blob it
// offers, so the engine never holds the store's credentials.
package blobstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

// Store holds blobs keyed by digest.
type Store interface {
	// Has reports whether the blob is stored.
	Has(ctx context.Context, dgst digest.Digest) (bool, error)
	// PresignPut returns an address that accepts a PUT of exactly size
	// bytes, stored under dgst. The PUT must carry Content-Length and needs
	// no other headers. size must be positive.
	PresignPut(ctx context.Context, dgst digest.Digest, size int64) (Address, error)
	// PresignGet returns an address that serves the blob to a plain GET.
	PresignGet(ctx context.Context, dgst digest.Digest) (Address, error)
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
// keyed by digest: the URLs of an upload request. Blobs the store has already
// are left out, so they are not requested again. A blob listed twice is
// checked once; listing it with two sizes, or with a size that isn't
// positive, is an error.
func UploadURLs(ctx context.Context, store Store, blobs []Blob) (map[digest.Digest]string, error) {
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu   sync.Mutex
		urls = make(map[digest.Digest]string)
		errs = make([]error, len(unique))
		wg   sync.WaitGroup
		sem  = make(chan struct{}, maxConcurrentChecks)
	)
	for i, d := range unique {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = fmt.Errorf("blob %s: %w", d, ctx.Err())
				return
			}
			defer func() { <-sem }()
			url, err := uploadURL(ctx, store, d, sizes[d])
			if err != nil {
				errs[i] = fmt.Errorf("blob %s: %w", d, err)
				cancel()
				return
			}
			if url != "" {
				mu.Lock()
				urls[d] = url
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// Report the errors that caused the failure, not the cancellations they
	// caused in other checks.
	var firstErrs []error
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			firstErrs = append(firstErrs, err)
		}
	}
	if len(firstErrs) == 0 {
		for _, err := range errs {
			if err != nil {
				firstErrs = append(firstErrs, err)
			}
		}
	}
	if len(firstErrs) > 0 {
		return nil, errors.Join(firstErrs...)
	}
	return urls, nil
}

func uploadURL(ctx context.Context, store Store, d digest.Digest, size int64) (string, error) {
	has, err := store.Has(ctx, d)
	if err != nil {
		return "", fmt.Errorf("check: %w", err)
	}
	if has {
		return "", nil
	}
	addr, err := store.PresignPut(ctx, d, size)
	if err != nil {
		return "", fmt.Errorf("presign PUT: %w", err)
	}
	return addr.URL, nil
}
