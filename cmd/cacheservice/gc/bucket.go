package gc

import (
	"context"
	"errors"
	"strings"
	"time"
)

// Bucket is what garbage collection needs of the object store, beyond the
// metadata store: listing, batch deletes, incomplete multipart uploads, and
// the bucket's lifecycle rules.
type Bucket interface {
	// ListPrefixes returns the common prefixes one level under prefix
	// (which ends with "/"), each ending with "/".
	ListPrefixes(ctx context.Context, prefix string) ([]string, error)
	// List calls fn with each page of the objects under prefix, in key
	// order. An error from fn ends the listing with that error.
	List(ctx context.Context, prefix string, fn func([]Object) error) error
	// Delete deletes up to MaxDeleteBatch objects, and returns the keys it
	// failed to delete. A missing object is not a failure. An error is a
	// request that failed as a whole: any of its keys may be deleted or
	// not.
	Delete(ctx context.Context, keys []string) (failed []string, err error)
	// ListUploads returns the incomplete multipart uploads under prefix.
	ListUploads(ctx context.Context, prefix string) ([]Upload, error)
	// AbortUpload aborts one. An upload already gone is not an error.
	AbortUpload(ctx context.Context, u Upload) error
	// ExpirationPrefixes returns the key prefixes the bucket's enabled
	// lifecycle rules expire objects under, or ErrRulesUnknown when they
	// can't be read (no permission, or a store without lifecycle rules).
	ExpirationPrefixes(ctx context.Context) ([]string, error)
}

// MaxDeleteBatch is the most keys one Delete takes: S3's DeleteObjects limit.
const MaxDeleteBatch = 1000

// ErrRulesUnknown is a bucket whose lifecycle rules can't be read.
var ErrRulesUnknown = errors.New("the bucket's lifecycle rules can't be read")

// Object is an object as a listing shows it.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

// Upload is an incomplete multipart upload.
type Upload struct {
	Key       string
	ID        string
	Initiated time.Time
}

// stagingCovered reports whether one of the rules' prefixes covers every
// key under a namespace's staging prefix (nsPrefix + "staging/") and is
// within it: a broader rule, over the whole namespace or prefix, expires
// live data too, and doesn't count (see liveRules).
func stagingCovered(nsPrefix string, prefixes []string) bool {
	staging := nsPrefix + "staging/"
	for _, r := range prefixes {
		if strings.HasPrefix(staging, r) && strings.HasPrefix(r, nsPrefix+"staging") {
			return true
		}
	}
	return false
}

// liveRules returns the rules' prefixes that expire keys of a namespace's
// live data, by age: its blobs, bundles, resolutions or index. Such a rule
// deletes what the index names, whatever GC decides.
func liveRules(nsPrefix string, prefixes []string) []string {
	var out []string
	for _, r := range prefixes {
		for _, live := range []string{"blobs/", "bundles/", "resolutions/", "index.json"} {
			k := nsPrefix + live
			if strings.HasPrefix(k, r) || strings.HasPrefix(r, k) {
				out = append(out, r)
				break
			}
		}
	}
	return out
}
