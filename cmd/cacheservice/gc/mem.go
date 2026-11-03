package gc

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// MemBucket is a Bucket over the in-memory stores tests use: a MemStore's
// blobs, at the keys the S3 store would give them under Prefix, and a
// MemObjects' bundles, resolutions and indexes (whose Store must have the
// same Prefix). Uploads and lifecycle rules are what tests set.
type MemBucket struct {
	Prefix  string
	Blobs   *blobstore.MemStore
	Objects *metastore.MemObjects

	mu      sync.Mutex
	uploads []Upload
	rules   []string
	// rulesErr, when set, is what ExpirationPrefixes returns.
	rulesErr error
	// deletes counts Delete calls; beforeDelete runs before each.
	deletes      int
	beforeDelete func(keys []string)
}

var _ Bucket = (*MemBucket)(nil)

// AddUpload adds an incomplete multipart upload.
func (b *MemBucket) AddUpload(u Upload) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.uploads = append(b.uploads, u)
}

// SetRules sets the prefixes lifecycle rules expire, or the error reading
// them gives.
func (b *MemBucket) SetRules(prefixes []string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rules, b.rulesErr = prefixes, err
}

// BeforeDelete sets a function that runs before each Delete, with its keys.
func (b *MemBucket) BeforeDelete(f func(keys []string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.beforeDelete = f
}

// Deletes returns how many Delete calls were made.
func (b *MemBucket) Deletes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deletes
}

func (b *MemBucket) top() string {
	if b.Prefix == "" {
		return ""
	}
	return strings.Trim(b.Prefix, "/") + "/"
}

// all returns every object, sorted by key.
func (b *MemBucket) all() []Object {
	var out []Object
	for _, o := range b.Blobs.Objects() {
		key := b.top() + o.NS + "/" + metastore.BlobRelKey(o.Digest)
		if o.Stage != "" {
			key = b.top() + o.NS + "/staging/" + string(o.Stage) + "/" + o.Digest.Algorithm().String() + "/" + o.Digest.Encoded()
		}
		out = append(out, Object{Key: key, Size: o.Size, Modified: o.Modified})
	}
	for _, o := range b.Objects.List(b.top()) {
		out = append(out, Object{Key: o.Key, Size: o.Size, Modified: o.Modified})
	}
	slices.SortFunc(out, func(a, b Object) int { return strings.Compare(a.Key, b.Key) })
	return out
}

// ListPrefixes returns the prefixes one level under prefix.
func (b *MemBucket) ListPrefixes(_ context.Context, prefix string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, o := range b.all() {
		rest, ok := strings.CutPrefix(o.Key, prefix)
		if !ok {
			continue
		}
		if dir, _, found := strings.Cut(rest, "/"); found && !seen[dir] {
			seen[dir] = true
			out = append(out, prefix+dir+"/")
		}
	}
	return out, nil
}

// List lists the objects under prefix in pages of 3, so callers see pages.
func (b *MemBucket) List(_ context.Context, prefix string, fn func([]Object) error) error {
	var page []Object
	for _, o := range b.all() {
		if !strings.HasPrefix(o.Key, prefix) {
			continue
		}
		page = append(page, o)
		if len(page) == 3 {
			if err := fn(page); err != nil {
				return err
			}
			page = nil
		}
	}
	if len(page) > 0 {
		return fn(page)
	}
	return nil
}

// Delete deletes keys from whichever store holds them.
func (b *MemBucket) Delete(ctx context.Context, keys []string) error {
	if len(keys) > MaxDeleteBatch {
		return fmt.Errorf("delete: %d keys, over %d", len(keys), MaxDeleteBatch)
	}
	b.mu.Lock()
	b.deletes++
	hook := b.beforeDelete
	b.mu.Unlock()
	if hook != nil {
		hook(keys)
	}
	for _, key := range keys {
		rest, ok := strings.CutPrefix(key, b.top())
		if !ok {
			return fmt.Errorf("delete %s: not under the prefix", key)
		}
		ns, rel, _ := strings.Cut(rest, "/")
		switch {
		case strings.HasPrefix(rel, "blobs/"):
			d, err := relDigest(strings.TrimPrefix(rel, "blobs/"))
			if err != nil {
				return err
			}
			b.Blobs.DeleteObject(ns, "", d)
		case strings.HasPrefix(rel, "staging/"):
			stage, dpath, _ := strings.Cut(strings.TrimPrefix(rel, "staging/"), "/")
			d, err := relDigest(dpath)
			if err != nil {
				return err
			}
			b.Blobs.DeleteObject(ns, blobstore.Stage(stage), d)
		default:
			if err := b.Objects.Delete(ctx, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// relDigest parses <alg>/<hex>.
func relDigest(p string) (digest.Digest, error) {
	alg, hex, _ := strings.Cut(p, "/")
	d := digest.NewDigestFromEncoded(digest.Algorithm(alg), hex)
	if err := d.Validate(); err != nil {
		return "", err
	}
	return d, nil
}

// ListUploads returns the uploads added under prefix.
func (b *MemBucket) ListUploads(_ context.Context, prefix string) ([]Upload, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Upload
	for _, u := range b.uploads {
		if strings.HasPrefix(u.Key, prefix) {
			out = append(out, u)
		}
	}
	return out, nil
}

// AbortUpload removes an upload.
func (b *MemBucket) AbortUpload(_ context.Context, u Upload) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.uploads = slices.DeleteFunc(b.uploads, func(x Upload) bool { return x.Key == u.Key && x.ID == u.ID })
	return nil
}

// Uploads returns the uploads not aborted.
func (b *MemBucket) Uploads() []Upload {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.uploads)
}

// ExpirationPrefixes returns what SetRules set.
func (b *MemBucket) ExpirationPrefixes(context.Context) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rulesErr != nil {
		return nil, b.rulesErr
	}
	return slices.Clone(b.rules), nil
}

// Now is a settable clock for tests, safe for concurrent use.
type Now struct {
	mu sync.Mutex
	t  time.Time
}

// NewNow returns a clock at t.
func NewNow(t time.Time) *Now { return &Now{t: t} }

// Now returns the clock's time.
func (n *Now) Now() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.t
}

// Add moves the clock forward by d.
func (n *Now) Add(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.t = n.t.Add(d)
}
