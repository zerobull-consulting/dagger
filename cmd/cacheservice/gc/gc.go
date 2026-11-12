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
	"maps"
	mrand "math/rand/v2"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
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
	// multipart upload, must be to be deleted: an export's upload timeout,
	// plus an hour for its promotions. An export waits up to the upload
	// timeout for the engine's uploads, then promotes them, which takes
	// minutes for a large blob the service hashes; a staged upload deleted
	// before its promotion is uploaded again. Deleting one is always safe:
	// a staged object is never read as cache.
	DefaultStagingAge = exporter.DefaultUploadTimeout + time.Hour
	// DefaultMaxDeletes bounds the objects one run claims per namespace,
	// so the claims in the index stay small (about 100 bytes each); a
	// larger backlog drains over later runs.
	DefaultMaxDeletes = 5000
	// DefaultDeleteDeadline bounds a run's deletes after its claim. A run
	// doesn't start a delete after it, and cancels a delete still in flight
	// at it.
	DefaultDeleteDeadline = 10 * time.Minute
	// MaxDeleteDeadline is the longest DeleteDeadline a collector takes.
	// Every replica takes a claim over once it is MaxDeleteDeadline plus
	// staleSlack old, whatever its own deadline: a staleness bound that
	// followed each replica's own setting would let one with a shorter
	// deadline take over, and finish, claims another is still deleting.
	MaxDeleteDeadline = 10 * time.Minute
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

	// run serializes this collector's runs, and those of its DryRunner.
	run *sync.Mutex

	mu *sync.Mutex
	// bundles caches the blob keys each bundle names, by scope/id.
	// Bundles never change, so an entry never goes stale.
	bundles map[string][]string
	// listed are the namespaces the last Run found: their gauges are set.
	// At most the bucket's namespaces.
	listed map[string]bool
}

// ValidateSettings refuses the settings New refuses whatever the stores are:
// a negative Grace, StagingAge, MaxDeletes or DeleteDeadline. It needs no
// bucket, so a command can check its flags before it opens any store, and
// can't disagree with New: New calls it.
func (c Config) ValidateSettings() error {
	if c.Grace < 0 || c.StagingAge < 0 || c.MaxDeletes < 0 || c.DeleteDeadline < 0 {
		return errors.New("gc: negative setting")
	}
	return nil
}

// New returns a Collector for cfg.
func New(cfg Config) (*Collector, error) {
	if cfg.Bucket == nil || cfg.Meta == nil {
		return nil, errors.New("gc: needs a bucket and a metadata store")
	}
	if err := cfg.ValidateSettings(); err != nil {
		return nil, err
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
	if cfg.DeleteDeadline > MaxDeleteDeadline {
		return nil, fmt.Errorf("gc: delete deadline %s is over %s, after which other replicas take a run's claims over", cfg.DeleteDeadline, MaxDeleteDeadline)
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
	return &Collector{cfg: cfg, log: log, run: &sync.Mutex{}, mu: &sync.Mutex{}, bundles: map[string][]string{}}, nil
}

// DryRunner returns a collector of the same configuration that deletes
// nothing (Config.DryRun), and runs one at a time with this one: a dry run
// asked for while a real run is under way waits for it, rather than report
// a state half way through it. It shares this one's bundle cache.
func (c *Collector) DryRunner() *Collector {
	d := &Collector{cfg: c.cfg, log: c.log, run: c.run, mu: c.mu, bundles: c.bundles}
	d.cfg.DryRun = true
	return d
}

// Report is what one run did in one namespace.
type Report struct {
	Namespace string `json:"namespace"`
	DryRun    bool   `json:"dryRun,omitempty"`
	// Stamped are the roots given a recording time; Expired the entries
	// dropped by their expiry or the max age; SizeRoots and SizeExpired
	// the roots expired, and entries dropped, for the max size.
	Stamped int `json:"stamped,omitempty"`
	Expired int `json:"expired,omitempty"`
	// Caches are the engine cache records dropped: no mappings left for
	// metastore.CacheRecordRetention.
	Caches      int `json:"caches,omitempty"`
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
	// LiveRules are lifecycle rules' prefixes that expire the namespace's
	// live data (blobs, bundles, resolutions, index) by age: a
	// misconfiguration that deletes what the index names.
	LiveRules []string `json:"liveRules,omitempty"`
	// Unexpected are some of the keys of no known shape GC found (at most
	// MaxUnexpectedLogged; uploads prefixed "upload:"), and
	// UnexpectedCount how many there were: ownership violations, for which
	// GC changed nothing in the namespace, or, for RootScope, anywhere.
	Unexpected      []string `json:"unexpected,omitempty"`
	UnexpectedCount int      `json:"unexpectedCount,omitempty"`
	// Skipped says why the reachability sweep (bundles, resolutions,
	// blobs), or the whole namespace, wasn't collected, if it wasn't.
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

// Namespaces lists the namespaces in the bucket: the valid names under
// <prefix>/ns/, the only place namespaces live. It reads the bucket, not the
// tokens, so namespaces whose tokens were all removed are collected too.
//
// It is what a run finds (survey), without the run.
func (c *Collector) Namespaces(ctx context.Context) ([]string, error) {
	s, err := c.survey(ctx)
	if err != nil {
		return nil, err
	}
	return s.namespaces, nil
}

// Run collects every namespace in the bucket, one after the other, and
// returns their reports. It runs only under an ownership marker of this
// layout, and only when nothing outside the namespaces is of an unknown
// shape (see survey); otherwise it does nothing, and returns ErrNotOwned.
// A namespace that fails doesn't stop the others; the error joins their
// failures.
func (c *Collector) Run(ctx context.Context) ([]Report, error) {
	c.run.Lock()
	defer c.run.Unlock()
	s, rules, rerr, err := c.prepare(ctx)
	if err != nil {
		return nil, err
	}
	if s.root.n > 0 {
		rep, err := c.refuseRoot(s)
		return []Report{rep}, err
	}
	var reports []Report
	var errs []error
	found := make(map[string]bool, len(s.namespaces))
	for _, ns := range s.namespaces {
		found[ns] = true
	}
	for _, ns := range s.namespaces {
		if ctx.Err() != nil {
			return reports, context.Cause(ctx)
		}
		rep := c.collect(ctx, ns, rules, rerr == nil, s.uploads[ns], s.badUploads[ns])
		reports = append(reports, rep)
		if len(rep.Errors) > 0 {
			errs = append(errs, fmt.Errorf("namespace %s: %s", ns, strings.Join(rep.Errors, "; ")))
		}
	}
	// Uploads in a namespace with no objects: it has no index, and is
	// left alone. Those of no known shape are still violations.
	for _, ns := range slices.Sorted(maps.Keys(s.uploads)) {
		if !slices.Contains(s.namespaces, ns) {
			c.log.Info("gc: incomplete multipart uploads in a namespace with no index; left alone", "namespace", ns, "uploads", len(s.uploads[ns]))
		}
	}
	for _, ns := range slices.Sorted(maps.Keys(s.badUploads)) {
		if slices.Contains(s.namespaces, ns) {
			continue
		}
		rep, err := c.badUploadsOnly(ns, s.badUploads[ns])
		reports = append(reports, rep)
		errs = append(errs, fmt.Errorf("namespace %s: %w", ns, err))
		found[ns] = true
	}
	// The gauges of namespaces gone from the bucket go too.
	for ns := range c.listed {
		if !found[ns] {
			c.cfg.Metrics.GCForget(ns)
		}
	}
	c.listed = found
	return reports, errors.Join(errs...)
}

// badUploadsOnly reports a namespace with no objects whose uploads include
// some of no known shape: a violation, as in a namespace with objects. They
// are never aborted, and nor is anything else there (it has no index).
func (c *Collector) badUploadsOnly(ns string, bad *unexpected) (Report, error) {
	c.cfg.Metrics.GCUnexpected(ns, bad.n)
	c.log.Error("gc: multipart uploads of no known shape in a namespace with no index; never aborted. "+
		"Nothing else may write under the service's prefix (see README, Bucket ownership)", "namespace", ns, "count", bad.n, "keys", bad.keys)
	rep := Report{Namespace: ns, DryRun: c.cfg.DryRun, Unexpected: bad.keys, UnexpectedCount: bad.n, Skipped: "no index"}
	err := fmt.Errorf("%d multipart uploads of no known shape, such as %s", bad.n, bad.keys[0])
	rep.fail(err)
	return rep, err
}

// prepare is what every run does before collecting: it checks the marker,
// surveys the prefix, and reads the lifecycle rules.
func (c *Collector) prepare(ctx context.Context) (*survey, []string, error, error) {
	if err := c.checkMarker(ctx); err != nil {
		c.log.Error("gc: does nothing", "error", err)
		return nil, nil, nil, err
	}
	s, err := c.survey(ctx)
	if err != nil {
		c.log.Error("gc: does nothing", "error", err)
		return nil, nil, nil, fmt.Errorf("gc: %w", err)
	}
	if s.root.n == 0 {
		c.cfg.Metrics.GCUnexpected(RootScope, 0)
	}
	rules, rerr := c.cfg.Bucket.ExpirationPrefixes(ctx)
	if rerr != nil {
		c.log.Info("gc: can't read the bucket's lifecycle rules, so it expires every namespace's staged uploads itself", "error", rerr)
		rules = nil
	}
	return s, rules, rerr, nil
}

// RunNamespace collects one namespace, under the same checks as Run: the
// marker, and nothing of unknown shape outside the namespaces.
func (c *Collector) RunNamespace(ctx context.Context, ns string) (Report, error) {
	if err := metastore.ValidateScope(ns); err != nil {
		return Report{}, err
	}
	c.run.Lock()
	defer c.run.Unlock()
	s, rules, rerr, err := c.prepare(ctx)
	if err != nil {
		return Report{}, err
	}
	if s.root.n > 0 {
		return c.refuseRoot(s)
	}
	if !slices.Contains(s.namespaces, ns) {
		if bad := s.badUploads[ns]; bad != nil {
			return c.badUploadsOnly(ns, bad)
		}
		return Report{Namespace: ns, DryRun: c.cfg.DryRun, Skipped: "no index"}, nil
	}
	rep := c.collect(ctx, ns, rules, rerr == nil, s.uploads[ns], s.badUploads[ns])
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
	// bytes and objects count the keys of known shapes, by kind.
	bytes   map[string]int64
	objects map[string]int64
	// unexpected are the keys of no known shape: ownership violations.
	unexpected unexpected
}

// metricKinds are the metrics' kinds of the layout's shapes. The epoch
// object and the conditional-write probe are "other".
var metricKinds = map[layout.Kind]string{
	layout.KindIndex:      metrics.KindIndex,
	layout.KindBundle:     metrics.KindBundle,
	layout.KindResolution: metrics.KindResolution,
	layout.KindBlob:       metrics.KindBlob,
	layout.KindStaging:    metrics.KindStaging,
	layout.KindEpoch:      metrics.KindOther,
	layout.KindProbe:      metrics.KindOther,
}

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
			k, id := layout.Classify(rel)
			switch k {
			case layout.KindUnknown:
				l.unexpected.add(o.Key)
				continue
			case layout.KindIndex:
				l.hasIndex, l.indexBytes = true, o.Size
			case layout.KindBundle:
				l.bundles[id] = o
			case layout.KindResolution:
				l.resolutions[id] = o
			case layout.KindBlob:
				l.blobs[rel] = o
			case layout.KindStaging:
				l.staging = append(l.staging, o)
			}
			l.bytes[metricKinds[k]] += o.Size
			l.objects[metricKinds[k]]++
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
	switch k, id := layout.Classify(key); k {
	case layout.KindBundle:
		return r.bundles[id]
	case layout.KindResolution:
		return r.resolutions[id]
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

// collect runs one namespace: read the index, list, check ownership,
// expire, size, sweep, staging, uploads.
//
// The namespace is left alone, with nothing written, deleted or aborted in
// it, when it has no index (an export that never recorded, or a prefix
// under ns/ that isn't the service's), or when any of its keys or uploads is
// of no known shape: an ownership violation.
func (c *Collector) collect(ctx context.Context, ns string, rules []string, rulesKnown bool, uploads []Upload, badUploads *unexpected) Report {
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
			"stagingRule", rep.StagingRule, "unexpected", rep.UnexpectedCount, "skipped", rep.Skipped, "errors", len(rep.Errors),
			"duration", time.Since(start).Round(time.Millisecond))
	}()
	lim := c.cfg.Limits(ns)
	now := c.cfg.Now()

	// 1. Read the index, before the listing: everything it names was
	// stored before the listing began, so must be in it (see sweep).
	snap, existed, ierr := c.cfg.Meta.IndexSnapshot(ctx, ns)

	// 2. List what is stored.
	l, err := c.list(ctx, ns)
	if err != nil {
		rep.fail(fmt.Errorf("list: %w", err))
		rep.Skipped = "the namespace couldn't be listed"
		return rep
	}

	// What the listing found of known shapes, whatever happens next: a
	// namespace skipped for a violation or a missing index still reports
	// what it holds.
	rep.StoredBytes, rep.StoredObjects = l.bytes, l.objects
	c.cfg.Metrics.GCStored(ns, l.bytes, l.objects)

	// 3. Ownership: every key and upload must be of a known shape.
	u := l.unexpected
	if badUploads != nil {
		u.n += badUploads.n
		u.keys = append(u.keys, badUploads.keys...)[:min(len(u.keys)+len(badUploads.keys), MaxUnexpectedLogged)]
	}
	c.cfg.Metrics.GCUnexpected(ns, u.n)
	if u.n > 0 {
		rep.Unexpected, rep.UnexpectedCount = u.keys, u.n
		rep.Skipped = "keys of no known shape in the namespace"
		rep.fail(fmt.Errorf("%d keys of no known shape, such as %s: the namespace isn't the service's alone; nothing in it is changed this run", u.n, u.keys[0]))
		log.Error("gc: keys of no known shape in the namespace: nothing in it is deleted, aborted or changed this run. "+
			"Nothing else may write under the service's prefix (see README, Bucket ownership)", "count", u.n, "keys", u.keys)
		return rep
	}
	if !l.hasIndex {
		rep.Skipped = "no index"
		log.Info("gc: a namespace without an index; left alone")
		return rep
	}
	if ierr == nil && !existed {
		// The first index was written between the read and the listing:
		// the snapshot, empty, names nothing the new index does. The next
		// run collects the namespace.
		rep.Skipped = "the index appeared during the run"
		log.Info("gc: the namespace's first index was written during the run; left alone until the next")
		return rep
	}

	// 4. Expire by expiry and max age; stamp roots with no recording time.
	var prev, idx *metastore.Index
	if ierr == nil {
		prev, idx, err = c.expire(ctx, ns, snap, lim, now, &rep)
	} else {
		err = ierr
	}
	if err != nil {
		// The bundles, resolutions and blobs need the index; staged and
		// multipart uploads don't, and are still collected.
		rep.fail(fmt.Errorf("expire: %w", err))
		rep.Skipped = "the index couldn't be read or updated"
	}

	// The published epoch must not lag the index's: a finish whose publish
	// failed, or a replica that published late, is caught up here.
	if !c.cfg.DryRun && idx != nil {
		c.publishEpoch(ctx, ns, idx, &rep)
	}

	// 5. Bundles, resolutions and blobs, only where the index was read:
	// without it, GC can't tell an object in use from one that isn't.
	if idx != nil {
		c.sweep(ctx, ns, prev, idx, l, lim, now, &rep, log)
	}

	// 6. Staged uploads, unless a lifecycle rule expires them.
	c.sweepStaging(ctx, ns, l, rules, rulesKnown, now, &rep, log)
	// 7. Incomplete multipart uploads.
	c.abortUploads(ctx, ns, uploads, now, &rep, log)
	return rep
}

// expire stamps unstamped roots and drops what expired, in one index
// update, and only when there is something to change: GC writes no index
// that didn't need it, and never creates one. It also drops finished
// deletions older than metastore.DeletionRetention. A dry run changes a
// copy. It returns snap, the index read before the listing, and the index
// after.
func (c *Collector) expire(ctx context.Context, ns string, snap *metastore.Index, lim Limits, now time.Time, rep *Report) (*metastore.Index, *metastore.Index, error) {
	nowUnix := now.Unix()
	// caches counts the cache records Expire drops: an index update of its
	// own, when nothing else changes (#59).
	apply := func(idx *metastore.Index) (stamped, dropped, caches, pruned int) {
		stamped = idx.StampRoots(nowUnix)
		before := len(idx.Caches)
		dropped = len(idx.Expire(nowUnix, lim.MaxAge))
		caches = before - len(idx.Caches)
		pruned = pruneDeletions(idx, now)
		return
	}
	trial := snap.Clone()
	s, d, k, p := apply(trial)
	if s+d+k+p == 0 {
		return snap, snap, nil
	}
	if c.cfg.DryRun {
		rep.Stamped, rep.Expired, rep.Caches = s, d, k
		return snap, trial, nil
	}
	idx, err := c.cfg.Meta.UpdateIndex(ctx, ns, func(idx *metastore.Index) error {
		rep.Stamped, rep.Expired, rep.Caches, _ = apply(idx)
		return nil
	})
	if err != nil {
		rep.Stamped, rep.Expired, rep.Caches = 0, 0, 0
		return nil, nil, err
	}
	c.cfg.Metrics.GCExpired(ns, metrics.ExpiredByExpiry, rep.Expired)
	return snap, idx, nil
}

// publishEpoch publishes the index's epoch when the epoch object lags it.
// A lagging epoch is safe, but refuses exports that name an object deleted
// in the epochs it lags, until their markers are pruned.
func (c *Collector) publishEpoch(ctx context.Context, ns string, idx *metastore.Index, rep *Report) {
	if idx.GC == nil || idx.GC.Epoch == 0 {
		return
	}
	published, err := c.cfg.Meta.ReadEpoch(ctx, ns)
	if err == nil && published >= idx.GC.Epoch {
		return
	}
	if err := c.cfg.Meta.WriteEpoch(ctx, ns, idx.GC.Epoch); err != nil {
		rep.fail(fmt.Errorf("publish epoch %d: %w", idx.GC.Epoch, err))
	}
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
//
// What the index named when this run started (prev) isn't deleted by this
// run, even when this run's expiry left nothing naming it: an import sync
// that read the index just before may still be merging it, with GETs of
// its blobs. It is deleted by the next run, an interval later.
func (c *Collector) sweep(ctx context.Context, ns string, prev, idx *metastore.Index, l *listing, lim Limits, now time.Time, rep *Report, log *slog.Logger) {
	r, err := c.references(ctx, ns, idx)
	if err != nil {
		rep.fail(err)
		rep.Skipped = "a bundle the index names couldn't be read"
		return
	}
	// Every bundle and resolution the index read before the listing (prev)
	// names must be in the listing: the blob lists are cached across runs,
	// so a bundle gone since (deleted by hand, say) would otherwise go
	// unnoticed, and its blobs be swept. The index after expiry may name
	// more, recorded since the listing; those aren't in it, and aren't
	// candidates either.
	named := r
	if prev != idx {
		if named, err = c.references(ctx, ns, prev); err != nil {
			rep.fail(err)
			rep.Skipped = "a bundle the index names couldn't be read"
			return
		}
	}
	if missing := missingNamed(named, l); missing != "" {
		rep.fail(fmt.Errorf("%s, which the index names, is not in the listing", missing))
		rep.Skipped = "an object the index names is missing"
		return
	}
	rep.LiveBytes = liveBytes(idx, nil, r, l)
	if lim.MaxBytes > 0 && rep.LiveBytes > lim.MaxBytes {
		shrunk, err := c.shrink(ctx, ns, idx, r, l, lim, now, rep, log)
		if errors.Is(err, errIndexOverMax) {
			// Nothing expired; the rest of the sweep still runs.
			rep.fail(fmt.Errorf("max size: %w", err))
			shrunk, err = idx, nil
		}
		idx = shrunk
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

	keep := r
	if prev != idx || rep.SizeRoots > 0 {
		if keep, err = c.references(ctx, ns, prev); err != nil {
			rep.fail(err)
			rep.Skipped = "a bundle the index names couldn't be read"
			return
		}
	}
	cands := c.candidates(idx, keep, l, now)
	if c.cfg.DryRun {
		// What a real run would claim: stale claims it takes over first,
		// then new candidates, MaxDeletes in all (see claimAndDelete).
		would := staleKeys(idx, now)
		for _, k := range cands {
			if !slices.Contains(would, k) {
				would = append(would, k)
			}
		}
		would = would[:min(len(would), c.cfg.MaxDeletes)]
		for _, k := range would {
			log.Info("gc dry run: would delete", "key", k)
		}
		rep.WouldDelete = append(rep.WouldDelete, would...)
		return
	}
	if len(cands) == 0 && !hasStale(idx, now) {
		return
	}
	c.claimAndDelete(ctx, ns, cands, l, now, rep, log)
}

// candidates are the objects r doesn't name, older than Grace, oldest
// first, that no run has claimed, at most MaxDeletes.
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

// stale reports whether a claim was left by a run that stopped: one older
// than any run, of any replica, may still delete under it.
func stale(d metastore.Deletion, now time.Time) bool {
	return d.InProgress() && now.Unix()-d.StartedUnix > int64((MaxDeleteDeadline+staleSlack)/time.Second)
}

// staleKeys are the keys of the stale claims, sorted.
func staleKeys(idx *metastore.Index, now time.Time) []string {
	if idx.GC == nil {
		return nil
	}
	var keys []string
	for k, d := range idx.GC.Deletions {
		if stale(d, now) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

func hasStale(idx *metastore.Index, now time.Time) bool {
	if idx.GC == nil {
		return false
	}
	for _, d := range idx.GC.Deletions {
		if stale(d, now) {
			return true
		}
	}
	return false
}

// claimAndDelete claims cands (and stale claims), deletes them, and
// finishes. See the package doc.
func (c *Collector) claimAndDelete(ctx context.Context, ns string, cands []string, l *listing, now time.Time, rep *Report, log *slog.Logger) {
	run := newRunID()
	var claimed []string
	var claimedAt time.Time
	_, err := c.cfg.Meta.UpdateIndex(ctx, ns, func(idx *metastore.Index) error {
		claimed = claimed[:0]
		// The claim's own time, not the run's start: a claim stamped with
		// the start of a long run would look stale, to other replicas,
		// while this run may still delete under it.
		claimNow := c.cfg.Now()
		nowUnix := claimNow.Unix()
		// The delete deadline counts from this same instant, on the
		// monotonic clock: the claim's own update may take a while (lost
		// conditional writes, a slow store), and a deadline counted from
		// after it would let deletes run past the time other replicas
		// take this claim over (StartedUnix + MaxDeleteDeadline + slack).
		claimedAt = time.Now()
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
		// Stale claims first, since exports naming them are refused until
		// they are finished; then new candidates; MaxDeletes in all. The
		// rest wait for later runs.
		for _, k := range staleKeys(idx, claimNow) {
			if len(claimed) >= c.cfg.MaxDeletes {
				break
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
			if len(claimed) >= c.cfg.MaxDeletes {
				break
			}
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

	// Delete, within the deadline (monotonic, from the claim's stamp): no
	// batch starts after it, and one in flight is canceled at it. A claim
	// whose update took past it deletes nothing, and is released.
	deadline := claimedAt.Add(c.cfg.DeleteDeadline)
	dctx, cancelDeletes := context.WithDeadline(ctx, deadline)
	defer cancelDeletes()
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
			// Before the call: one that fails may still have deleted
			// some of its keys (DeleteObjects answers per key), so each
			// attempted key is finished as deleted. That is the safe
			// side: an export that checked a key before must be refused.
			// A key that survived costs one refused export, which is
			// tried again, finds it, and records; and nothing names it,
			// so a later run claims it again.
			attempted[k] = true
		}
		failed, err := c.cfg.Bucket.Delete(dctx, keys)
		if err != nil {
			rep.fail(fmt.Errorf("delete: %w", err))
			break
		}
		if len(failed) > 0 {
			// The others are deleted; the next batches still run.
			rep.fail(fmt.Errorf("delete: %d of %d keys failed, such as %s", len(failed), len(keys), failed[0]))
		}
		gone := failedSet(failed)
		for _, k := range batch {
			if gone[prefix+k] {
				continue
			}
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

// failedSet is a set of keys a Delete reported failing.
func failedSet(keys []string) map[string]bool {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	return m
}

// sameRecord reports whether two entries are the same, in every field that
// decides what they keep alive and until when: an entry changed in any of
// them since GC's snapshot was recorded again, and is kept.
func sameRecord(a, b metastore.Entry) bool {
	return a.Bundle == b.Bundle && a.Ordinal == b.Ordinal && a.Resolution == b.Resolution &&
		a.Source == b.Source && a.Replacements == b.Replacements &&
		a.ExpiresAtUnix == b.ExpiresAtUnix && slices.Equal(a.Deps, b.Deps) && slices.Equal(a.Parts, b.Parts)
}

// kindSize is a relative key's kind and listed size.
func (l *listing) kindSize(k string) (string, int64) {
	switch kind, id := layout.Classify(k); kind {
	case layout.KindBundle:
		return metrics.KindBundle, l.bundles[id].Size
	case layout.KindResolution:
		return metrics.KindResolution, l.resolutions[id].Size
	default:
		return metrics.KindBlob, l.blobs[k].Size
	}
}

// missingNamed returns a bundle or resolution r names that the listing
// doesn't hold, by relative key, or "".
func missingNamed(r *refs, l *listing) string {
	for _, id := range slices.Sorted(maps.Keys(r.bundles)) {
		if _, ok := l.bundles[id]; !ok {
			return metastore.BundleRelKey(id)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(r.resolutions)) {
		if _, ok := l.resolutions[id]; !ok {
			return metastore.ResolutionRelKey(id)
		}
	}
	return ""
}

// errIndexOverMax is a namespace whose index alone is over its max size:
// expiring roots can't bring it under.
var errIndexOverMax = errors.New("the max size can't be reached by expiring roots")

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
		// rec is the root's entry's record when GC read the index.
		rec metastore.Entry
	}
	roots := make([]root, 0, len(idx.Roots))
	for n, rt := range idx.Roots {
		at := rt.RecordedAtUnix
		if at == 0 {
			at = nowUnix
		}
		var rec metastore.Entry
		if e := idx.Entries[n]; e != nil {
			rec = *e
		}
		roots = append(roots, root{n: n, at: at, rec: rec})
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
	// Expiring roots can't shrink the index itself: when it alone is over
	// the limit, no number of roots would do, and the search would expire
	// every one of them for nothing.
	if l.indexBytes >= lim.MaxBytes {
		return idx, fmt.Errorf("%w: the index alone is %d bytes, at or over the max size of %d; no root expired; raise max-bytes", errIndexOverMax, l.indexBytes, lim.MaxBytes)
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
			// Recorded again within the same second: its record changed.
			// (An identical record written again that second can't be
			// told apart, and goes: it was the oldest.)
			if e := x.Entries[rt.n]; e == nil || !sameRecord(*e, rt.rec) {
				continue
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
	if live := liveRules(prefix, rules); len(live) > 0 {
		rep.LiveRules = live
		log.Warn("gc: a lifecycle rule expires this namespace's live data by age: it deletes blobs, bundles or the index that entries still name; limit it to the staging prefix", "rules", live, "staging", prefix+"staging/")
	}
	switch {
	case !rulesKnown:
		rep.StagingRule = "unknown"
		c.cfg.Metrics.GCStagingRuleUnknown(ns)
	case stagingCovered(prefix, rules):
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
		failed, err := c.cfg.Bucket.Delete(ctx, keys)
		if err != nil {
			rep.fail(fmt.Errorf("delete staged uploads: %w", err))
			return
		}
		if len(failed) > 0 {
			rep.fail(fmt.Errorf("delete staged uploads: %d of %d keys failed, such as %s", len(failed), len(keys), failed[0]))
			miss := failedSet(failed)
			for _, o := range batch {
				if miss[o.Key] {
					bytes -= o.Size
				}
			}
		}
		n := len(batch) - len(failed)
		rep.deleted(metrics.KindStaging, n, bytes)
		c.cfg.Metrics.GCDeleted(ns, metrics.KindStaging, n, bytes)
	}
}

// abortUploads aborts the namespace's incomplete multipart uploads older than
// StagingAge: promotions that a stopped service left.
//
// Every upload here is at a blob key of the namespace, or at a staging key
// (a large service-hashed promotion's private copy; survey): one of another
// shape is a violation, and its namespace isn't collected.
func (c *Collector) abortUploads(ctx context.Context, ns string, uploads []Upload, now time.Time, rep *Report, log *slog.Logger) {
	prefix, _ := c.cfg.Meta.ScopePrefix(ns)
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
