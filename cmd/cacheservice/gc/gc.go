// Package gc is the remote cache's garbage collection (#11): per namespace,
// it expires the index's roots and entries, by their own expiry, the
// namespace's max age and its max size, then deletes what no live entry
// names any more (bundles, resolutions, blobs), staged uploads older than an
// export's upload timeout, and stale incomplete multipart uploads.
//
// Every delete is the service's own decision, from expiry, size or
// reachability in the index. Nothing an engine reports (engine.prune, an
// offer's gone, a failed renew) and no change of tokens deletes anything.
//
// # Safety
//
// The index is the only thing an export and GC serialize on: both change it
// with conditional writes (metastore.Store.UpdateIndex). Objects are
// content-addressed, so an export may name an object GC found unreferenced:
// a blob it found stored (Has) before uploading anything, or a bundle or
// resolution an earlier export stored. So GC deletes in three steps:
//
//  1. Claim: in one index update, it marks the objects it will delete
//     (metastore.Collection), re-checking against that index that nothing
//     names them. An export recorded before the claim is seen; one after is
//     refused (metastore.ErrCollected) while the claim stands.
//  2. Delete them, within DeleteDeadline of the claim.
//  3. Finish: in another update, it marks them deleted in a new epoch,
//     which it then publishes in the scope's epoch object. An export read
//     the epoch before checking anything (its Fence), so it is refused if
//     it names an object deleted in a later epoch: it checked the object
//     before it was deleted.
//
// A refused export is tried again from the start, and uploads what is gone.
// So the index never names an object that isn't stored, whatever GC runs at
// the same time, in this process or another replica: there is no election.
// Deletes are idempotent, and a claim left by a run that stopped is taken
// over by a later run, of any replica, once it is stale.
//
// Objects younger than Grace aren't claimed at all: an export in flight
// (between its blob promotions and its index write) isn't refused for
// them. Correctness doesn't rest on the grace period; the fence ensures it.
package gc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
)

// Defaults for Config.
const (
	// DefaultInterval is the time between runs, jittered by ±25%.
	DefaultInterval = time.Hour
	// DefaultGrace is how old an unreferenced bundle, resolution or blob
	// must be to be claimed: longer than an export takes from its first
	// promotion to its index write, which waits up to its upload timeout
	// (an hour) for the engine.
	DefaultGrace = 2 * time.Hour
	// DefaultStagingAge is how old a staged upload, or an incomplete
	// multipart upload, must be to be deleted: an export's upload timeout.
	// A staged object is never read as cache, so deleting it is always
	// safe: at worst a promotion still copying it fails, and its export
	// asks for the blob again.
	DefaultStagingAge = exporter.DefaultUploadTimeout
	// DefaultMaxDeletes bounds the objects one run claims per namespace,
	// so the claims in the index stay small (about 100 bytes each); a
	// larger backlog drains over later runs.
	DefaultMaxDeletes = 5000
	// DefaultDeleteDeadline bounds a run's deletes after its claim. A run
	// doesn't start a delete after it; a later run takes over its claims
	// once they are DeleteDeadline plus staleSlack old.
	DefaultDeleteDeadline = 10 * time.Minute
	// staleSlack is the margin before a claim counts as stale, for clocks
	// of different replicas.
	staleSlack = 5 * time.Minute
	// maxCachedBundles bounds the bundles' blob lists kept in memory.
	maxCachedBundles = 65536
	// finishTimeout bounds the finishing index update, which runs even
	// after the run's context ends, so the claims don't stay stale.
	finishTimeout = time.Minute
)

// Config configures a Collector.
type Config struct {
	// Bucket lists and deletes; Meta reads and updates the indexes, under
	// the same prefix as Bucket's keys.
	Bucket Bucket
	Meta   *metastore.Store
	// Limits returns a namespace's limits; nil is no limits.
	Limits func(ns string) Limits
	// Grace, StagingAge, MaxDeletes and DeleteDeadline default to the
	// Default* values when zero.
	Grace          time.Duration
	StagingAge     time.Duration
	MaxDeletes     int
	DeleteDeadline time.Duration
	// DryRun deletes nothing and changes no index: it logs, and reports,
	// what a run would expire and delete.
	DryRun bool
	// Logger receives what each run does. Nil discards.
	Logger *slog.Logger
	// Metrics records what is stored and deleted. Nil records nothing.
	Metrics *metrics.Metrics
	// Now is the clock for ages and expiries. Nil means time.Now. Deadlines
	// use the monotonic clock whatever it is.
	Now func() time.Time
}

// Collector runs garbage collection.
type Collector struct {
	cfg Config
	log *slog.Logger

	// run serializes this collector's runs.
	run sync.Mutex

	mu sync.Mutex
	// bundles caches the blob keys each bundle names, by scope/id.
	// Bundles never change, so an entry never goes stale.
	bundles map[string][]string
}

// New returns a Collector for cfg.
func New(cfg Config) (*Collector, error) {
	if cfg.Bucket == nil || cfg.Meta == nil {
		return nil, errors.New("gc: needs a bucket and a metadata store")
	}
	if cfg.Grace < 0 || cfg.StagingAge < 0 || cfg.MaxDeletes < 0 || cfg.DeleteDeadline < 0 {
		return nil, errors.New("gc: negative setting")
	}
	if cfg.Grace == 0 {
		cfg.Grace = DefaultGrace
	}
	if cfg.StagingAge == 0 {
		cfg.StagingAge = DefaultStagingAge
	}
	if cfg.MaxDeletes == 0 {
		cfg.MaxDeletes = DefaultMaxDeletes
	}
	if cfg.DeleteDeadline == 0 {
		cfg.DeleteDeadline = DefaultDeleteDeadline
	}
	if cfg.Limits == nil {
		cfg.Limits = func(string) Limits { return Limits{} }
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Collector{cfg: cfg, log: log, bundles: map[string][]string{}}, nil
}

// Report is what one run did in one namespace.
type Report struct {
	Namespace string `json:"namespace"`
	DryRun    bool   `json:"dryRun,omitempty"`
	// Stamped are the roots given a recording time; Expired the entries
	// dropped by their expiry or the max age; SizeRoots and SizeExpired
	// the roots expired, and entries dropped, for the max size.
	Stamped     int `json:"stamped,omitempty"`
	Expired     int `json:"expired,omitempty"`
	SizeRoots   int `json:"sizeRoots,omitempty"`
	SizeExpired int `json:"sizeExpired,omitempty"`
	// LiveBytes is what the namespace keeps after expiry: its index, and
	// the bundles, resolutions and blobs its entries name.
	LiveBytes int64 `json:"liveBytes"`
	// StoredBytes and StoredObjects are what the listing found, by kind.
	StoredBytes   map[string]int64 `json:"storedBytes"`
	StoredObjects map[string]int64 `json:"storedObjects"`
	// Deleted and DeletedBytes are what was deleted, by kind.
	Deleted      map[string]int   `json:"deleted,omitempty"`
	DeletedBytes map[string]int64 `json:"deletedBytes,omitempty"`
	// WouldDelete are the keys a dry run would delete.
	WouldDelete []string `json:"wouldDelete,omitempty"`
	// StagingRule is whether a lifecycle rule expires the namespace's
	// staging prefix: present, missing or unknown.
	StagingRule string `json:"stagingRule"`
	// Skipped says why the reachability sweep (bundles, resolutions,
	// blobs) didn't run, if it didn't.
	Skipped string `json:"skipped,omitempty"`
	// Errors are what failed; the run went on where it safely could.
	Errors []string `json:"errors,omitempty"`
}

func (r *Report) deleted(kind string, n int, bytes int64) {
	if n == 0 {
		return
	}
	if r.Deleted == nil {
		r.Deleted, r.DeletedBytes = map[string]int{}, map[string]int64{}
	}
	r.Deleted[kind] += n
	r.DeletedBytes[kind] += bytes
}

func (r *Report) fail(err error) { r.Errors = append(r.Errors, err.Error()) }

// Loop runs every interval, jittered by ±25%, until ctx ends. The first run
// comes after one jittered interval too, so replicas started together
// spread out.
func (c *Collector) Loop(ctx context.Context, interval time.Duration) {
	for {
		wait := time.Duration(float64(interval) * (0.75 + mrand.Float64()/2))
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		if _, err := c.Run(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("gc run failed", "error", err)
		}
	}
}

// Namespaces lists the namespaces in the bucket: the prefixes under the
// store's prefix that are valid scopes. It skips `blobs` (where blobs were
// kept before namespaces) and anything else that isn't a namespace's name.
// It reads the bucket, not the tokens, so namespaces whose tokens were all
// removed are collected too.
func (c *Collector) Namespaces(ctx context.Context) ([]string, error) {
	top := c.top()
	prefixes, err := c.cfg.Bucket.ListPrefixes(ctx, top)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, p := range prefixes {
		name := strings.TrimSuffix(strings.TrimPrefix(p, top), "/")
		if metastore.ValidateScope(name) != nil {
			c.log.Debug("gc: not a namespace; skipped", "prefix", p)
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func (c *Collector) top() string {
	if p := c.cfg.Meta.Prefix(); p != "" {
		return p + "/"
	}
	return ""
}

// Run collects every namespace in the bucket, one after the other, and
// returns their reports. A namespace that fails doesn't stop the others;
// the error joins their failures.
func (c *Collector) Run(ctx context.Context) ([]Report, error) {
	c.run.Lock()
	defer c.run.Unlock()
	names, err := c.Namespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("gc: list namespaces: %w", err)
	}
	rules, rerr := c.cfg.Bucket.ExpirationPrefixes(ctx)
	if rerr != nil {
		c.log.Info("gc: can't read the bucket's lifecycle rules, so it expires every namespace's staged uploads itself", "error", rerr)
		rules = nil
	}
	var reports []Report
	var errs []error
	for _, ns := range names {
		if ctx.Err() != nil {
			return reports, context.Cause(ctx)
		}
		rep := c.collect(ctx, ns, rules, rerr == nil)
		reports = append(reports, rep)
		if len(rep.Errors) > 0 {
			errs = append(errs, fmt.Errorf("namespace %s: %s", ns, strings.Join(rep.Errors, "; ")))
		}
	}
	return reports, errors.Join(errs...)
}

// RunNamespace collects one namespace.
func (c *Collector) RunNamespace(ctx context.Context, ns string) (Report, error) {
	if err := metastore.ValidateScope(ns); err != nil {
		return Report{}, err
	}
	c.run.Lock()
	defer c.run.Unlock()
	rules, rerr := c.cfg.Bucket.ExpirationPrefixes(ctx)
	rep := c.collect(ctx, ns, rules, rerr == nil)
	if len(rep.Errors) > 0 {
		return rep, errors.New(strings.Join(rep.Errors, "; "))
	}
	return rep, nil
}

// listing is a namespace's objects, by kind, keyed relative to the
// namespace.
type listing struct {
	bundles     map[string]Object // by bundle ID
	resolutions map[string]Object // by resolution ID
	blobs       map[string]Object // by relative key
	staging     []Object          // full keys
	hasIndex    bool
	indexBytes  int64
	bytes       map[string]int64
	objects     map[string]int64
}

var (
	bundleRel     = regexp.MustCompile(`^bundles/([0-9a-f]{64})\.json$`)
	resolutionRel = regexp.MustCompile(`^resolutions/([0-9a-f]{64})\.json$`)
	stagingRel    = regexp.MustCompile(`^staging/[0-9a-f]{32}/`)
)

func (c *Collector) list(ctx context.Context, ns string) (*listing, error) {
	prefix, err := c.cfg.Meta.ScopePrefix(ns)
	if err != nil {
		return nil, err
	}
	l := &listing{
		bundles: map[string]Object{}, resolutions: map[string]Object{}, blobs: map[string]Object{},
		bytes: map[string]int64{}, objects: map[string]int64{},
	}
	err = c.cfg.Bucket.List(ctx, prefix, func(objs []Object) error {
		for _, o := range objs {
			rel, ok := strings.CutPrefix(o.Key, prefix)
			if !ok {
				continue
			}
			kind := metrics.KindOther
			switch {
			case rel == "index.json":
				kind = metrics.KindIndex
				l.hasIndex, l.indexBytes = true, o.Size
			case bundleRel.MatchString(rel):
				kind = metrics.KindBundle
				l.bundles[bundleRel.FindStringSubmatch(rel)[1]] = o
			case resolutionRel.MatchString(rel):
				kind = metrics.KindResolution
				l.resolutions[resolutionRel.FindStringSubmatch(rel)[1]] = o
			case strings.HasPrefix(rel, "blobs/"):
				if d, err := relDigest(strings.TrimPrefix(rel, "blobs/")); err == nil && metastore.BlobRelKey(d) == rel {
					kind = metrics.KindBlob
					l.blobs[rel] = o
				}
			case stagingRel.MatchString(rel):
				kind = metrics.KindStaging
				l.staging = append(l.staging, o)
			}
			l.bytes[kind] += o.Size
			l.objects[kind]++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return l, nil
}

// refs is what an index names: bundles and resolutions by ID, blobs by
// relative key, and each bundle's blobs.
type refs struct {
	bundles     map[string]bool
	resolutions map[string]bool
	blobs       map[string]bool
	byBundle    map[string][]string
}

// has reports whether a relative key is named.
func (r *refs) has(key string) bool {
	switch {
	case bundleRel.MatchString(key):
		return r.bundles[bundleRel.FindStringSubmatch(key)[1]]
	case resolutionRel.MatchString(key):
		return r.resolutions[resolutionRel.FindStringSubmatch(key)[1]]
	default:
		return r.blobs[key]
	}
}

// references reads what idx names, reading the bundles it hasn't seen yet.
// A bundle the index names that can't be read fails it: GC can't know its
// blobs, so it must not delete any.
func (c *Collector) references(ctx context.Context, ns string, idx *metastore.Index) (*refs, error) {
	r := &refs{bundles: map[string]bool{}, resolutions: map[string]bool{}, blobs: map[string]bool{}, byBundle: map[string][]string{}}
	for _, e := range idx.Entries {
		r.bundles[e.Bundle] = true
		if e.Resolution != "" {
			r.resolutions[e.Resolution] = true
		}
	}
	for id := range r.bundles {
		keys, err := c.bundleBlobs(ctx, ns, id)
		if err != nil {
			return nil, fmt.Errorf("bundle %s, which the index names: %w", id, err)
		}
		r.byBundle[id] = keys
		for _, k := range keys {
			r.blobs[k] = true
		}
	}
	return r, nil
}

func (c *Collector) bundleBlobs(ctx context.Context, ns, id string) ([]string, error) {
	ck := ns + "/" + id
	c.mu.Lock()
	keys, ok := c.bundles[ck]
	c.mu.Unlock()
	if ok {
		return keys, nil
	}
	b, err := c.cfg.Meta.GetBundle(ctx, ns, id)
	if err != nil {
		return nil, err
	}
	for _, d := range metastore.BundleBlobs(b) {
		keys = append(keys, metastore.BlobRelKey(d))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.bundles) >= maxCachedBundles {
		// Drop about half, at random (map order).
		n := 0
		for k := range c.bundles {
			delete(c.bundles, k)
			if n++; n >= maxCachedBundles/2 {
				break
			}
		}
	}
	c.bundles[ck] = keys
	return keys, nil
}

// collect runs one namespace: expire, list, size, sweep, staging, uploads.
func (c *Collector) collect(ctx context.Context, ns string, rules []string, rulesKnown bool) Report {
	start := time.Now()
	ctx = metrics.WithCaller(ctx, metrics.CallerGC)
	rep := Report{Namespace: ns, DryRun: c.cfg.DryRun}
	log := c.log.With("namespace", ns)
	outcome := metrics.GCOK
	defer func() {
		if c.cfg.DryRun {
			outcome = metrics.GCDryRun
		} else if len(rep.Errors) > 0 {
			outcome = metrics.GCError
		}
		c.cfg.Metrics.GCRun(ns, outcome, time.Since(start))
		log.Info("gc run", "dryRun", c.cfg.DryRun, "expired", rep.Expired, "sizeRoots", rep.SizeRoots, "sizeExpired", rep.SizeExpired,
			"liveBytes", rep.LiveBytes, "deleted", rep.Deleted, "deletedBytes", rep.DeletedBytes, "wouldDelete", len(rep.WouldDelete),
			"stagingRule", rep.StagingRule, "skipped", rep.Skipped, "errors", len(rep.Errors), "duration", time.Since(start).Round(time.Millisecond))
	}()
	lim := c.cfg.Limits(ns)
	now := c.cfg.Now()

	// 1. Expire by expiry and max age; stamp roots with no recording time.
	idx, err := c.expire(ctx, ns, lim, now, &rep)
	if err != nil {
		rep.fail(fmt.Errorf("expire: %w", err))
		rep.Skipped = "the index couldn't be read or updated"
		return rep
	}

	// 2. List what is stored.
	l, err := c.list(ctx, ns)
	if err != nil {
		rep.fail(fmt.Errorf("list: %w", err))
		rep.Skipped = "the namespace couldn't be listed"
		return rep
	}
	rep.StoredBytes, rep.StoredObjects = l.bytes, l.objects
	c.cfg.Metrics.GCStored(ns, l.bytes, l.objects)

	// 3. Bundles, resolutions and blobs, only where an index is: without
	// one, GC can't tell an object in use from one that isn't.
	if !l.hasIndex {
		rep.Skipped = "no index"
	} else {
		c.sweep(ctx, ns, idx, l, lim, now, &rep, log)
	}

	// 4. Staged uploads, unless a lifecycle rule expires them.
	c.sweepStaging(ctx, ns, l, rules, rulesKnown, now, &rep, log)
	// 5. Incomplete multipart uploads.
	c.abortUploads(ctx, ns, now, &rep, log)
	return rep
}

// expire stamps unstamped roots and drops what expired, in one index
// update, and only when there is something to change: GC writes no index
// that didn't need it, and never creates one. It also drops finished
// deletions older than metastore.DeletionRetention. A dry run changes a
// copy.
func (c *Collector) expire(ctx context.Context, ns string, lim Limits, now time.Time, rep *Report) (*metastore.Index, error) {
	snap, err := c.cfg.Meta.Index(ctx, ns)
	if err != nil {
		return nil, err
	}
	nowUnix := now.Unix()
	apply := func(idx *metastore.Index) (stamped, dropped, pruned int) {
		stamped = idx.StampRoots(nowUnix)
		dropped = len(idx.Expire(nowUnix, lim.MaxAge))
		pruned = pruneDeletions(idx, now)
		return
	}
	trial := snap.Clone()
	s, d, p := apply(trial)
	if s+d+p == 0 {
		return snap, nil
	}
	if c.cfg.DryRun {
		rep.Stamped, rep.Expired = s, d
		return trial, nil
	}
	idx, err := c.cfg.Meta.UpdateIndex(ctx, ns, func(idx *metastore.Index) error {
		rep.Stamped, rep.Expired, _ = apply(idx)
		return nil
	})
	if err != nil {
		rep.Stamped, rep.Expired = 0, 0
		return nil, err
	}
	c.cfg.Metrics.GCExpired(ns, metrics.ExpiredByExpiry, rep.Expired)
	return idx, nil
}

// pruneDeletions drops finished deletions older than DeletionRetention.
func pruneDeletions(idx *metastore.Index, now time.Time) int {
	if idx.GC == nil {
		return 0
	}
	cutoff := now.Add(-metastore.DeletionRetention).Unix()
	n := 0
	for k, d := range idx.GC.Deletions {
		if !d.InProgress() && d.DoneUnix < cutoff {
			delete(idx.GC.Deletions, k)
			n++
		}
	}
	return n
}

// sweep enforces the max size, then claims, deletes and finishes the
// bundles, resolutions and blobs no entry names.
func (c *Collector) sweep(ctx context.Context, ns string, idx *metastore.Index, l *listing, lim Limits, now time.Time, rep *Report, log *slog.Logger) {
	r, err := c.references(ctx, ns, idx)
	if err != nil {
		rep.fail(err)
		rep.Skipped = "a bundle the index names couldn't be read"
		return
	}
	rep.LiveBytes = liveBytes(idx, nil, r, l)
	if lim.MaxBytes > 0 && rep.LiveBytes > lim.MaxBytes {
		idx, err = c.shrink(ctx, ns, idx, r, l, lim, now, rep, log)
		if err != nil {
			rep.fail(fmt.Errorf("max size: %w", err))
			rep.Skipped = "the max size couldn't be applied"
			return
		}
		if r, err = c.references(ctx, ns, idx); err != nil {
			rep.fail(err)
			rep.Skipped = "a bundle the index names couldn't be read"
			return
		}
		rep.LiveBytes = liveBytes(idx, nil, r, l)
	}

	cands := c.candidates(idx, r, l, now)
	if c.cfg.DryRun {
		for _, k := range cands {
			log.Info("gc dry run: would delete", "key", k)
		}
		rep.WouldDelete = append(rep.WouldDelete, cands...)
		return
	}
	if len(cands) == 0 && !hasStale(idx, now, c.cfg.DeleteDeadline) {
		return
	}
	c.claimAndDelete(ctx, ns, cands, l, now, rep, log)
}

// candidates are the unreferenced objects older than Grace, oldest first,
// that no run has claimed, at most MaxDeletes.
func (c *Collector) candidates(idx *metastore.Index, r *refs, l *listing, now time.Time) []string {
	cutoff := now.Add(-c.cfg.Grace)
	type cand struct {
		key string
		mod time.Time
	}
	var all []cand
	// Claimed by a run now: that run deletes it. An object deleted before
	// (a finished deletion) can be claimed again: it was stored again
	// since, and the new claim only makes exports refuse more.
	marked := func(k string) bool {
		if idx.GC == nil {
			return false
		}
		d, ok := idx.GC.Deletions[k]
		return ok && d.InProgress()
	}
	add := func(k string, o Object) {
		if !r.has(k) && o.Modified.Before(cutoff) && !marked(k) {
			all = append(all, cand{k, o.Modified})
		}
	}
	for id, o := range l.bundles {
		add(metastore.BundleRelKey(id), o)
	}
	for id, o := range l.resolutions {
		add(metastore.ResolutionRelKey(id), o)
	}
	for k, o := range l.blobs {
		add(k, o)
	}
	slices.SortFunc(all, func(a, b cand) int {
		if c := a.mod.Compare(b.mod); c != 0 {
			return c
		}
		return strings.Compare(a.key, b.key)
	})
	out := make([]string, 0, min(len(all), c.cfg.MaxDeletes))
	for _, a := range all[:min(len(all), c.cfg.MaxDeletes)] {
		out = append(out, a.key)
	}
	return out
}

// stale reports whether a claim was left by a run that stopped.
func stale(d metastore.Deletion, now time.Time, deadline time.Duration) bool {
	return d.InProgress() && now.Unix()-d.StartedUnix > int64((deadline+staleSlack)/time.Second)
}

func hasStale(idx *metastore.Index, now time.Time, deadline time.Duration) bool {
	if idx.GC == nil {
		return false
	}
	for _, d := range idx.GC.Deletions {
		if stale(d, now, deadline) {
			return true
		}
	}
	return false
}

// claimAndDelete claims cands (and stale claims), deletes them, and
// finishes. See the package doc.
func (c *Collector) claimAndDelete(ctx context.Context, ns string, cands []string, l *listing, now time.Time, rep *Report, log *slog.Logger) {
	run := newRunID()
	nowUnix := now.Unix()
	var claimed []string
	_, err := c.cfg.Meta.UpdateIndex(ctx, ns, func(idx *metastore.Index) error {
		claimed = claimed[:0]
		// Against this index: what it names now, recorded since.
		r, err := c.references(ctx, ns, idx)
		if err != nil {
			return err
		}
		if idx.GC == nil {
			idx.GC = &metastore.Collection{}
		}
		if idx.GC.Deletions == nil {
			idx.GC.Deletions = map[string]metastore.Deletion{}
		}
		for k, d := range idx.GC.Deletions {
			if !stale(d, now, c.cfg.DeleteDeadline) {
				continue
			}
			if r.has(k) {
				// Can't be (exports are refused while claimed), but
				// never delete what the index names.
				delete(idx.GC.Deletions, k)
				continue
			}
			idx.GC.Deletions[k] = metastore.Deletion{Run: run, StartedUnix: nowUnix}
			claimed = append(claimed, k)
		}
		for _, k := range cands {
			if d, ok := idx.GC.Deletions[k]; (ok && d.InProgress()) || r.has(k) {
				continue
			}
			idx.GC.Deletions[k] = metastore.Deletion{Run: run, StartedUnix: nowUnix}
			claimed = append(claimed, k)
		}
		return nil
	})
	if err != nil {
		rep.fail(fmt.Errorf("claim: %w", err))
		return
	}
	if len(claimed) == 0 {
		return
	}
	slices.Sort(claimed)
	prefix, _ := c.cfg.Meta.ScopePrefix(ns)

	// Delete, within the deadline (monotonic, from the claim).
	deadline := time.Now().Add(c.cfg.DeleteDeadline)
	attempted := map[string]bool{}
	for i := 0; i < len(claimed); i += MaxDeleteBatch {
		batch := claimed[i:min(i+MaxDeleteBatch, len(claimed))]
		if time.Now().After(deadline) {
			rep.fail(fmt.Errorf("deadline passed with %d claimed objects not deleted; their claims are released", len(claimed)-i))
			break
		}
		if ctx.Err() != nil {
			rep.fail(context.Cause(ctx))
			break
		}
		keys := make([]string, len(batch))
		for j, k := range batch {
			keys[j] = prefix + k
			// Before the call: one that fails may still have deleted.
			attempted[k] = true
		}
		if err := c.cfg.Bucket.Delete(ctx, keys); err != nil {
			rep.fail(fmt.Errorf("delete: %w", err))
			break
		}
		for _, k := range batch {
			kind, size := l.kindSize(k)
			rep.deleted(kind, 1, size)
			c.cfg.Metrics.GCDeleted(ns, kind, 1, size)
			log.Debug("gc deleted", "key", k)
		}
	}

	// Finish: what was attempted is deleted as of a new epoch; the rest is
	// released. It runs even if ctx ended, so the claims don't go stale.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
	defer cancel()
	var epoch uint64
	_, err = c.cfg.Meta.UpdateIndex(fctx, ns, func(idx *metastore.Index) error {
		epoch = 0
		if idx.GC == nil {
			return nil
		}
		next := idx.GC.Epoch + 1
		done := c.cfg.Now().Unix()
		for k, d := range idx.GC.Deletions {
			if d.Run != run || !d.InProgress() {
				continue
			}
			if attempted[k] {
				idx.GC.Deletions[k] = metastore.Deletion{Run: run, StartedUnix: d.StartedUnix, Epoch: next, DoneUnix: done}
				epoch = next
			} else {
				delete(idx.GC.Deletions, k)
			}
		}
		if epoch != 0 {
			idx.GC.Epoch = epoch
		}
		return nil
	})
	if err != nil {
		rep.fail(fmt.Errorf("finish: %w; a later run takes the claims over", err))
		return
	}
	if epoch != 0 {
		if err := c.cfg.Meta.WriteEpoch(fctx, ns, epoch); err != nil {
			// Exports read an older epoch, and refuse more until the
			// next run publishes one.
			rep.fail(err)
		}
	}
}

// kindSize is a relative key's kind and listed size.
func (l *listing) kindSize(k string) (string, int64) {
	switch {
	case bundleRel.MatchString(k):
		return metrics.KindBundle, l.bundles[bundleRel.FindStringSubmatch(k)[1]].Size
	case resolutionRel.MatchString(k):
		return metrics.KindResolution, l.resolutions[resolutionRel.FindStringSubmatch(k)[1]].Size
	default:
		return metrics.KindBlob, l.blobs[k].Size
	}
}

// liveBytes is what the index keeps: its own size, and the listed sizes of
// the bundles, resolutions and blobs its entries reachable from the roots
// not in removed name.
func liveBytes(idx *metastore.Index, removed map[uint64]bool, r *refs, l *listing) int64 {
	live := map[uint64]bool{}
	var stack []uint64
	for n := range idx.Roots {
		if !removed[n] {
			stack = append(stack, n)
		}
	}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		e := idx.Entries[n]
		if live[n] || e == nil {
			continue
		}
		live[n] = true
		stack = append(stack, e.Deps...)
	}
	bundles, resolutions, blobs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	total := l.indexBytes
	for n := range live {
		e := idx.Entries[n]
		if !bundles[e.Bundle] {
			bundles[e.Bundle] = true
			total += l.bundles[e.Bundle].Size
			for _, k := range r.byBundle[e.Bundle] {
				if !blobs[k] {
					blobs[k] = true
					total += l.blobs[k].Size
				}
			}
		}
		if e.Resolution != "" && !resolutions[e.Resolution] {
			resolutions[e.Resolution] = true
			total += l.resolutions[e.Resolution].Size
		}
	}
	return total
}

// shrink expires the oldest roots, by recording time, then number, until
// what the namespace keeps is under its max size, and returns the index as
// written. It finds how many with a binary search over liveBytes, so it
// walks the index about log2(roots) times. A root an export recorded again
// since the snapshot is kept.
func (c *Collector) shrink(ctx context.Context, ns string, idx *metastore.Index, r *refs, l *listing, lim Limits, now time.Time, rep *Report, log *slog.Logger) (*metastore.Index, error) {
	nowUnix := now.Unix()
	type root struct {
		n  uint64
		at int64
	}
	roots := make([]root, 0, len(idx.Roots))
	for n, rt := range idx.Roots {
		at := rt.RecordedAtUnix
		if at == 0 {
			at = nowUnix
		}
		roots = append(roots, root{n, at})
	}
	slices.SortFunc(roots, func(a, b root) int {
		if a.at != b.at {
			if a.at < b.at {
				return -1
			}
			return 1
		}
		if a.n < b.n {
			return -1
		}
		return 1
	})
	removedFirst := func(k int) map[uint64]bool {
		m := make(map[uint64]bool, k)
		for _, rt := range roots[:k] {
			m[rt.n] = true
		}
		return m
	}
	// The fewest oldest roots whose removal brings it under.
	k := sort.Search(len(roots), func(k int) bool {
		return liveBytes(idx, removedFirst(k), r, l) <= lim.MaxBytes
	})
	if k == 0 {
		return idx, nil
	}
	chosen := roots[:k]
	log.Info("gc: over the max size; expiring the oldest roots", "liveBytes", rep.LiveBytes, "maxBytes", lim.MaxBytes, "roots", k, "of", len(roots))
	drop := func(x *metastore.Index) (int, int) {
		dropped := 0
		for _, rt := range chosen {
			cur, ok := x.Roots[rt.n]
			if !ok {
				continue
			}
			if cur.RecordedAtUnix != 0 && cur.RecordedAtUnix > rt.at {
				continue // recorded again since: younger now
			}
			delete(x.Roots, rt.n)
			dropped++
		}
		return dropped, len(x.Expire(nowUnix, lim.MaxAge))
	}
	if c.cfg.DryRun {
		trial := idx.Clone()
		rep.SizeRoots, rep.SizeExpired = drop(trial)
		return trial, nil
	}
	written, err := c.cfg.Meta.UpdateIndex(ctx, ns, func(x *metastore.Index) error {
		rep.SizeRoots, rep.SizeExpired = drop(x)
		return nil
	})
	if err != nil {
		rep.SizeRoots, rep.SizeExpired = 0, 0
		return nil, err
	}
	c.cfg.Metrics.GCExpired(ns, metrics.ExpiredBySize, rep.SizeExpired)
	return written, nil
}

// sweepStaging deletes staged uploads older than StagingAge, unless a
// lifecycle rule expires the namespace's staging prefix. It reports a
// namespace without one.
func (c *Collector) sweepStaging(ctx context.Context, ns string, l *listing, rules []string, rulesKnown bool, now time.Time, rep *Report, log *slog.Logger) {
	prefix, _ := c.cfg.Meta.ScopePrefix(ns)
	switch {
	case !rulesKnown:
		rep.StagingRule = "unknown"
	case coveredBy(prefix+"staging/", rules):
		rep.StagingRule = "present"
		c.cfg.Metrics.GCStagingRule(ns, false)
		return
	default:
		rep.StagingRule = "missing"
		c.cfg.Metrics.GCStagingRule(ns, true)
		if len(l.staging) > 0 {
			log.Warn("gc: no lifecycle rule expires this namespace's staged uploads; GC expires them itself (see README, Lifecycle rule for staging)", "prefix", prefix+"staging/")
		}
	}
	cutoff := now.Add(-c.cfg.StagingAge)
	var old []Object
	for _, o := range l.staging {
		if o.Modified.Before(cutoff) {
			old = append(old, o)
		}
	}
	if c.cfg.DryRun {
		for _, o := range old {
			log.Info("gc dry run: would delete", "key", o.Key)
			rep.WouldDelete = append(rep.WouldDelete, strings.TrimPrefix(o.Key, prefix))
		}
		return
	}
	for i := 0; i < len(old); i += MaxDeleteBatch {
		batch := old[i:min(i+MaxDeleteBatch, len(old))]
		keys := make([]string, len(batch))
		var bytes int64
		for j, o := range batch {
			keys[j] = o.Key
			bytes += o.Size
		}
		if err := c.cfg.Bucket.Delete(ctx, keys); err != nil {
			rep.fail(fmt.Errorf("delete staged uploads: %w", err))
			return
		}
		rep.deleted(metrics.KindStaging, len(batch), bytes)
		c.cfg.Metrics.GCDeleted(ns, metrics.KindStaging, len(batch), bytes)
	}
}

// abortUploads aborts the namespace's incomplete multipart uploads older than
// StagingAge: promotions that a stopped service left.
func (c *Collector) abortUploads(ctx context.Context, ns string, now time.Time, rep *Report, log *slog.Logger) {
	prefix, _ := c.cfg.Meta.ScopePrefix(ns)
	uploads, err := c.cfg.Bucket.ListUploads(ctx, prefix)
	if err != nil {
		rep.fail(fmt.Errorf("list uploads: %w", err))
		return
	}
	cutoff := now.Add(-c.cfg.StagingAge)
	n := 0
	for _, u := range uploads {
		if !u.Initiated.Before(cutoff) {
			continue
		}
		if c.cfg.DryRun {
			log.Info("gc dry run: would abort upload", "key", u.Key)
			rep.WouldDelete = append(rep.WouldDelete, "upload:"+strings.TrimPrefix(u.Key, prefix))
			continue
		}
		if err := c.cfg.Bucket.AbortUpload(ctx, u); err != nil {
			rep.fail(err)
			continue
		}
		n++
	}
	rep.deleted(metrics.KindUpload, n, 0)
	c.cfg.Metrics.GCDeleted(ns, metrics.KindUpload, n, 0)
}

func newRunID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
