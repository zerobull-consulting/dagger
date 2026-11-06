// Package exporter is the service's side of pushing an engine's cache entries
// to the store: export, exported, upload, uploaded, then publish.
//
// One export of some roots goes:
//
//  1. The service sends export with the roots and the parts to upload. The
//     engine answers exported: a bundle, if any root survived, and the roots
//     it skipped, as gone, busy or refused. It holds the bundle's chains
//     under the reply's exportID until upload.
//  2. If the scope's index already refuses the bundle as a late export from
//     an older generation of the cache (metastore.Index.CheckGeneration),
//     the service releases the chains and settles the roots as refused,
//     uploading nothing. Otherwise it works out which layer blobs the
//     bundle's outputs need, drops those the blob store already has, and
//     sends upload with a presigned PUT for each of the rest, to staging
//     keys under a new blobstore.Stage of this try's own. An empty map
//     needs nothing, and releases the chains.
//  3. The engine PUTs each blob and answers uploaded: which blobs are done,
//     and which failed.
//  4. The service verifies each blob the engine reported done, and promotes
//     it to its digest key only if its content hashes to the digest
//     (blobstore.Promote). A blob that doesn't is never stored, and counts
//     as failed.
//  5. Only once every blob is stored (verified) does the service publish the
//     bundle and record its values in the scope's index. If a blob failed,
//     the export is tried again from step 1, since the engine released the
//     chains with its answer. Blobs stored by the earlier try are not asked
//     for again; a blob that failed verification is.
//
// The engine works on at most remotecache.ExportsInFlight exports at once,
// and queues the rest, so the service sends no more than that to one engine.
// The engine uploads at most remotecache.UploadConcurrency blobs at once
// across its exports, so an upload's reply can take a while. Neither side
// accepts a message over protocol.MaxMessageBytes: an export whose reply
// would be larger is answered with protocol.TooLargeMessage, and an upload
// whose request would be. Either way, the roots are split in two and each
// half is exported on its own.
//
// The import path sends a stored bundle whole, with a presigned GET added
// per layer of each output's chain, and can't split it: records name each
// other by ordinal. So a bundle is stored only if its merge would fit in a
// message too (importer.MergeSize, against Config.MaxMergeBytes); an export
// whose bundle wouldn't is split the same way, and a root whose bundle alone
// wouldn't fails.
package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// Defaults for Config.
const (
	// DefaultUploadAttempts is how many times one export is tried while
	// blobs fail to upload.
	DefaultUploadAttempts = 3
	// DefaultRetryBackoff is the wait before the second try; it doubles
	// for each later one.
	DefaultRetryBackoff = time.Second
	// DefaultExportTimeout bounds the wait for exported.
	DefaultExportTimeout = 5 * time.Minute
	// DefaultUploadTimeout bounds the wait for uploaded. The engine uploads
	// UploadConcurrency blobs at a time, across all its exports.
	DefaultUploadTimeout = time.Hour
)

// Engine is a connected engine, as the exporter sees it.
type Engine interface {
	// CacheID and Generation are the engine cache's identity, from hello.
	CacheID() string
	Generation() uint64
	// Request sends a request and returns the engine's reply, which may be
	// an error reply. It fails when ctx or the connection ends first.
	Request(ctx context.Context, typ protocol.Type, body json.RawMessage) (protocol.Envelope, error)
}

// Config configures an Exporter.
type Config struct {
	// Blobs stores layer blobs, and Meta bundles and the index.
	Blobs blobstore.Store
	Meta  *metastore.Store
	// Scope is the index the exported values are recorded in.
	Scope string
	// Logger receives progress and failures. Nil discards.
	Logger *slog.Logger
	// Metrics records each blob's promotion, under Scope. Nil records
	// nothing.
	Metrics *metrics.Metrics
	// UploadAttempts, RetryBackoff, ExportTimeout and UploadTimeout default
	// to the Default* values when zero.
	UploadAttempts int
	RetryBackoff   time.Duration
	ExportTimeout  time.Duration
	UploadTimeout  time.Duration
	// MaxMessageBytes is the largest request the exporter sends. Zero means
	// protocol.MaxMessageBytes. Tests lower it.
	MaxMessageBytes int
	// MaxMergeBytes is the largest merge request the import path sends: its
	// own MaxMessageBytes. A bundle whose merge would be larger is not
	// stored (see importer.MergeSize). Zero means protocol.MaxMessageBytes.
	MaxMergeBytes int
}

// Exporter exports entries from engines to the store.
type Exporter struct {
	cfg Config
	log *slog.Logger
	// sleep waits between tries; tests replace it.
	sleep func(context.Context, time.Duration) error

	mu sync.Mutex
	// slots bound the exports in flight per engine.
	slots map[Engine]chan struct{}
	// addressBytes is the size of a presigned GET from the blob store, as
	// a merge carries it; 0 until measured.
	addressBytes int
	// epoch is the last fence read, when epochOK (see fence).
	epoch   metastore.Fence
	epochOK bool
}

const (
	// epochTTL is how long a fence read is used for later tries.
	epochTTL = 10 * time.Second
	// maxCollectedTries bounds the tries of an export that GC's fence
	// keeps refusing.
	maxCollectedTries = 3
)

// New returns an Exporter for cfg.
func New(cfg Config) (*Exporter, error) {
	if cfg.Blobs == nil || cfg.Meta == nil {
		return nil, errors.New("exporter: needs a blob store and a metadata store")
	}
	if err := metastore.ValidateScope(cfg.Scope); err != nil {
		return nil, fmt.Errorf("exporter: %w", err)
	}
	if cfg.UploadAttempts <= 0 {
		cfg.UploadAttempts = DefaultUploadAttempts
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = DefaultRetryBackoff
	}
	if cfg.ExportTimeout <= 0 {
		cfg.ExportTimeout = DefaultExportTimeout
	}
	if cfg.UploadTimeout <= 0 {
		cfg.UploadTimeout = DefaultUploadTimeout
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = protocol.MaxMessageBytes
	}
	if cfg.MaxMergeBytes <= 0 {
		cfg.MaxMergeBytes = protocol.MaxMessageBytes
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Exporter{cfg: cfg, log: log, sleep: sleepCtx, slots: map[Engine]chan struct{}{}}, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

// Forget drops what the exporter keeps for an engine whose connection ended.
func (x *Exporter) Forget(eng Engine) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.slots, eng)
}

func (x *Exporter) engineSlots(eng Engine) chan struct{} {
	x.mu.Lock()
	defer x.mu.Unlock()
	s := x.slots[eng]
	if s == nil {
		s = make(chan struct{}, remotecache.ExportsInFlight)
		x.slots[eng] = s
	}
	return s
}

// Request is what to export: roots by engine number, and the parts of any
// entries in their closure to upload.
type Request struct {
	Roots   []uint64                `json:"roots"`
	Outputs []protocol.ExportOutput `json:"outputs,omitempty"`
}

// Result is what one Export did, by engine root number.
type Result struct {
	// Stored roots are in a published bundle, with every blob their
	// outputs need.
	Stored []uint64 `json:"stored,omitempty"`
	// Gone, Busy and Refused are the roots the engine skipped. Refused
	// also holds the roots the index refused because the export came
	// from an older generation of the cache than it recorded
	// (StaleGenerationMessage): neither is exported again unchanged.
	Gone    []uint64               `json:"gone,omitempty"`
	Busy    []protocol.SkippedRoot `json:"busy,omitempty"`
	Refused []protocol.SkippedRoot `json:"refused,omitempty"`
	// Failed roots were exported, but could not be stored: their blobs kept
	// failing to upload, the store failed, or a root alone was too large
	// for a message. Each has its error.
	Failed []FailedRoot `json:"failed,omitempty"`
	// Bundles are the IDs of the published bundles.
	Bundles []string `json:"bundles,omitempty"`
	// Uploaded are the blobs the engine uploaded and the service verified
	// and stored, and Present those it was not asked for because the store
	// had them.
	Uploaded []digest.Digest `json:"uploaded,omitempty"`
	Present  []digest.Digest `json:"present,omitempty"`
	// Rejected are the blobs the engine reported uploaded that were not
	// stored: their content didn't match the digest, or nothing was
	// uploaded. Each try that rejected one is counted.
	Rejected []digest.Digest `json:"rejected,omitempty"`
}

// StaleGenerationMessage is the message of a root in Result.Refused that the
// index refused, not the engine: a late export from an older generation of
// the cache than the index recorded, naming a value it no longer holds (see
// metastore.ErrStaleGeneration).
const StaleGenerationMessage = "not recorded: exported by an older generation of the cache than the index's"

// FailedRoot is a root that could not be stored, and why.
type FailedRoot struct {
	Number uint64 `json:"number"`
	Error  string `json:"error"`
}

// Merge adds o to r.
func (r *Result) Merge(o Result) {
	r.Stored = append(r.Stored, o.Stored...)
	r.Gone = append(r.Gone, o.Gone...)
	r.Busy = append(r.Busy, o.Busy...)
	r.Refused = append(r.Refused, o.Refused...)
	r.Failed = append(r.Failed, o.Failed...)
	r.Bundles = append(r.Bundles, o.Bundles...)
	r.Uploaded = append(r.Uploaded, o.Uploaded...)
	r.Present = append(r.Present, o.Present...)
	r.Rejected = append(r.Rejected, o.Rejected...)
}

func (r *Result) fail(roots []uint64, err error) {
	for _, n := range roots {
		r.Failed = append(r.Failed, FailedRoot{Number: n, Error: err.Error()})
	}
}

// errTooLarge is a request, or the engine's reply to it, over the message
// limit.
var errTooLarge = errors.New(protocol.TooLargeMessage)

// bundleTooLargeError is an exported bundle whose merge would be over the
// import path's message limit. It is an errTooLarge: the roots are split.
type bundleTooLargeError struct{ size, limit int }

func (e *bundleTooLargeError) Error() string {
	return fmt.Sprintf("bundle too large to import: its merge would be up to %d bytes, over the %d byte message limit", e.size, e.limit)
}

func (e *bundleTooLargeError) Is(target error) bool { return target == errTooLarge }

// checkMergeSize returns a bundleTooLargeError when the merge the import
// path would make of bundle, as stored, could be over MaxMergeBytes.
func (x *Exporter) checkMergeSize(ctx context.Context, bundle *dagql.ValueBundle) error {
	data, err := json.Marshal(bundle)
	if err != nil {
		return err
	}
	size := importer.MergeSize(bundle, len(data), x.addressSize(ctx), x.cfg.Scope)
	if size > x.cfg.MaxMergeBytes {
		return &bundleTooLargeError{size: size, limit: x.cfg.MaxMergeBytes}
	}
	return nil
}

// addressSize is the size of one presigned GET from the blob store, as a
// merge carries it, measured once.
func (x *Exporter) addressSize(ctx context.Context) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.addressBytes == 0 {
		n, err := importer.AddressBytes(ctx, importer.StoreSigner(x.cfg.Blobs))
		if err != nil {
			// Measured again next time.
			x.log.Warn("export: can't sign a GET to size merges; assuming the default", "error", err)
			return importer.DefaultAddressBytes
		}
		x.addressBytes = n
	}
	return x.addressBytes
}

// Export exports req's roots from eng, uploads the blobs the store lacks,
// and publishes what was stored. It returns an error, with the result so
// far, when the export as a whole fails: ctx ends, the connection fails, the
// engine answers export or upload with an error reply, or a wait for its
// answer passes ExportTimeout or UploadTimeout. A root that could not be
// stored for a reason of its own is in the result's Failed instead, with the
// reason.
func (x *Exporter) Export(ctx context.Context, eng Engine, req Request) (Result, error) {
	roots := slices.Clone(req.Roots)
	slices.Sort(roots)
	roots = slices.Compact(roots)
	if len(roots) == 0 {
		return Result{}, nil
	}
	return x.export(ctx, eng, roots, req.Outputs)
}

// export exports roots, splitting them in two while the export is too large.
func (x *Exporter) export(ctx context.Context, eng Engine, roots []uint64, outputs []protocol.ExportOutput) (Result, error) {
	res, err := x.exportWithRetries(ctx, eng, roots, outputs)
	if !errors.Is(err, errTooLarge) {
		return res, err
	}
	if len(roots) == 1 {
		var big *bundleTooLargeError
		if errors.As(err, &big) {
			x.log.Warn("export: one root's bundle is too large to import", "root", roots[0], "mergeBytes", big.size, "limit", big.limit)
			res.fail(roots, big)
			return res, nil
		}
		x.log.Warn("export: one root is too large for a message", "root", roots[0])
		res.fail(roots, fmt.Errorf("export of this root alone is over the %d byte message limit", x.cfg.MaxMessageBytes))
		return res, nil
	}
	half := len(roots) / 2
	x.log.Info("export: too large; splitting", "roots", len(roots))
	var all Result
	all.Merge(res)
	for _, part := range [][]uint64{roots[:half], roots[half:]} {
		r, err := x.export(ctx, eng, part, outputs)
		all.Merge(r)
		if err != nil {
			return all, err
		}
	}
	return all, nil
}

// exportWithRetries exports roots, and tries again while blobs fail to
// upload, up to UploadAttempts tries, and while GC's fence refuses the
// index write (metastore.ErrCollected), up to maxCollectedTries more: a
// refusal is GC racing the export, not a failed upload, and is counted and
// reported apart.
func (x *Exporter) exportWithRetries(ctx context.Context, eng Engine, roots []uint64, outputs []protocol.ExportOutput) (Result, error) {
	var all Result
	wait := x.cfg.RetryBackoff
	uploads, collections := 0, 0
	for {
		res, retry, collected, err := x.exportOnce(ctx, eng, roots, outputs)
		all.Merge(res)
		if err != nil || len(retry) == 0 {
			return all, err
		}
		if collected {
			collections++
			if collections >= maxCollectedTries {
				all.fail(retry, fmt.Errorf("garbage collection deleted what the export named %d times in a row", collections))
				return all, nil
			}
			x.log.Info("export: trying again after garbage collection", "roots", len(retry), "try", collections)
		} else {
			uploads++
			if uploads >= x.cfg.UploadAttempts {
				all.fail(retry, fmt.Errorf("blobs failed to upload %d times", uploads))
				return all, nil
			}
			x.log.Info("export: blobs failed to upload; trying again", "roots", len(retry), "attempt", uploads, "wait", wait)
		}
		if err := x.sleep(ctx, wait); err != nil {
			return all, err
		}
		wait *= 2
		roots = retry
	}
}

// exportOnce runs one export, upload and publish. It returns the roots to
// try again because blobs failed to upload, or, with collected, because
// GC's fence refused the index write.
func (x *Exporter) exportOnce(ctx context.Context, eng Engine, roots []uint64, outputs []protocol.ExportOutput) (res Result, retry []uint64, collected bool, _ error) {
	slots := x.engineSlots(eng)
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return res, nil, false, context.Cause(ctx)
	}
	defer func() { <-slots }()

	exported, err := x.requestExport(ctx, eng, roots, outputs)
	if err != nil {
		return res, nil, false, err
	}
	for _, s := range exported.Skipped {
		switch s.Reason {
		case protocol.SkipGone:
			res.Gone = append(res.Gone, s.Number)
		case protocol.SkipBusy:
			res.Busy = append(res.Busy, s)
		default:
			res.Refused = append(res.Refused, s)
		}
	}
	if exported.Bundle == nil {
		return res, nil, false, nil
	}
	survivors := survivingRoots(roots, exported.Skipped)

	// A stale generation's export the index would refuse uploads nothing
	// (#111): refused here, before any blob is asked for. Only a shortcut:
	// the check in the index update below decides, since a newer
	// generation can be recorded in between.
	if err := x.checkGeneration(ctx, eng, exported.Bundle); err != nil {
		if rerr := x.release(ctx, eng, exported.ExportID); rerr != nil {
			return res, nil, false, rerr
		}
		x.refuseStale(eng, survivors, err, "before upload", &res)
		return res, nil, false, nil
	}

	stripVolatile(exported.Bundle)
	if err := x.checkMergeSize(ctx, exported.Bundle); err != nil {
		// Not stored: release the chains, and let the caller split the
		// roots.
		if rerr := x.release(ctx, eng, exported.ExportID); rerr != nil {
			return res, nil, false, rerr
		}
		return Result{}, nil, false, err
	}

	blobs, err := neededBlobs(exported.Bundle)
	if err != nil {
		err = fmt.Errorf("invalid bundle: %w", err)
	}
	// Each try uploads under a stage of its own: a blob it stages is never
	// at the same key as another try's or another export's.
	stage := blobstore.NewStage()
	// GC's epoch, read before any check of the store, fences this try's
	// index write against objects GC deletes after the checks (see
	// metastore.Collection).
	fence := x.fence(ctx)
	var urls map[digest.Digest]string
	if err == nil {
		urls, err = blobstore.UploadURLs(ctx, x.cfg.Blobs, stage, blobs)
		if err != nil {
			err = fmt.Errorf("blob store: %w", err)
		}
	}
	if err == nil {
		err = x.checkOffered(ctx, exported.Bundle, blobs)
	}
	if err != nil {
		// Release the chains; the export isn't stored.
		if rerr := x.release(ctx, eng, exported.ExportID); rerr != nil {
			return res, nil, false, rerr
		}
		if ctx.Err() != nil {
			return res, nil, false, context.Cause(ctx)
		}
		res.fail(survivors, err)
		return res, nil, false, nil
	}
	for _, b := range blobs {
		if _, asked := urls[b.Digest]; !asked {
			res.Present = append(res.Present, b.Digest)
		}
	}

	uploaded, err := x.requestUpload(ctx, eng, exported.ExportID, urls)
	if errors.Is(err, errTooLarge) {
		// Our own upload request is too large to send: release the chains
		// and let the caller split the roots.
		if rerr := x.release(ctx, eng, exported.ExportID); rerr != nil {
			return res, nil, false, rerr
		}
		return Result{}, nil, false, errTooLarge
	}
	if err != nil {
		return res, nil, false, err
	}
	for _, f := range uploaded.Failed {
		x.log.Warn("export: blob upload failed", "digest", f.Digest, "error", f.Message)
	}
	done, err := x.promote(ctx, eng, stage, blobs, urls, uploaded.Done, &res)
	if err != nil {
		return res, nil, false, err
	}
	var missing int
	for d := range urls {
		if !done[d] {
			missing++
		}
	}
	if missing > 0 {
		return res, survivors, false, nil
	}

	rec, err := x.cfg.Meta.RecordFenced(ctx, x.cfg.Scope, exported.Bundle, eng.CacheID(), eng.Generation(), fence)
	if err != nil {
		if ctx.Err() != nil {
			return res, nil, false, context.Cause(ctx)
		}
		if errors.Is(err, metastore.ErrCollected) {
			// GC deleted, or is deleting, an object this try found
			// stored. Try again from the start: its checks will see
			// what is stored then, and ask for what is gone.
			x.cfg.Metrics.ExportCollected(x.cfg.Scope)
			x.log.Info("export: garbage collection raced it; trying again", "cacheID", eng.CacheID(), "roots", len(survivors), "error", err)
			x.forgetEpoch()
			return res, survivors, true, nil
		}
		if errors.Is(err, metastore.ErrStaleGeneration) {
			// A newer generation was recorded after the early check
			// (checkGeneration), or that check couldn't read the index.
			x.refuseStale(eng, survivors, err, "index update", &res)
			return res, nil, false, nil
		}
		res.fail(survivors, fmt.Errorf("publish: %w", err))
		return res, nil, false, nil
	}
	res.Stored = append(res.Stored, survivors...)
	res.Bundles = append(res.Bundles, rec.BundleID)
	x.log.Info("export stored", "cacheID", eng.CacheID(), "roots", len(survivors),
		"values", len(exported.Bundle.Values), "bundle", rec.BundleID,
		"uploaded", len(res.Uploaded), "present", len(res.Present))
	return res, nil, false, nil
}

// checkGeneration returns the index's ErrStaleGeneration refusal of the
// bundle, as the scope's index reads now (metastore.Index.CheckGeneration),
// or nil. A failed read returns nil: the export goes on, and the index
// update's own check decides.
//
// It reads the index on every try. Kept generations would save the read,
// but a stale export reaches the index only from another replica's or an
// earlier service's engine connection (this service's tracker fences older
// generations), and what this service kept is what those writes outdate.
func (x *Exporter) checkGeneration(ctx context.Context, eng Engine, bundle *dagql.ValueBundle) error {
	idx, err := x.cfg.Meta.Index(ctx, x.cfg.Scope)
	if err != nil {
		if ctx.Err() == nil {
			x.log.Warn("export: reading the index to check the generation failed; checking at the index update", "error", err)
		}
		return nil
	}
	return idx.CheckGeneration(bundle, eng.CacheID(), eng.Generation())
}

// refuseStale settles roots as refused, for a late export from an older
// generation of the cache than the index recorded, of a value the index
// doesn't hold (#59). Its generation never changes, so trying again can't
// help: the tracker doesn't export this state of them again, until a newer
// generation of the cache reports in. check names the check that refused it.
func (x *Exporter) refuseStale(eng Engine, roots []uint64, err error, check string, res *Result) {
	x.log.Info("export: older generation than the index's, not recorded", "cacheID", eng.CacheID(), "generation", eng.Generation(), "roots", len(roots), "check", check, "error", err)
	for _, n := range roots {
		res.Refused = append(res.Refused, protocol.SkippedRoot{Number: n, Reason: protocol.SkipRefused, Message: StaleGenerationMessage})
	}
}

// checkOffered checks, after the fence, that every layer the bundle names
// but doesn't upload is stored: the layers of its records' pending offers,
// which the service offered the engine and the engine hasn't pulled, so it
// can't upload them. The fence protects only what an export checked after
// reading it; without this check, a layer GC deleted before the export
// started would pass the fence, and the index would name a bundle whose
// offered layer is gone. Such an export fails: it can't be stored whole.
func (x *Exporter) checkOffered(ctx context.Context, bundle *dagql.ValueBundle, uploads []blobstore.Blob) error {
	uploaded := make(map[digest.Digest]bool, len(uploads))
	for _, b := range uploads {
		uploaded[b.Digest] = true
	}
	for _, d := range metastore.BundleBlobs(bundle) {
		if uploaded[d] {
			continue
		}
		has, err := x.cfg.Blobs.Has(ctx, d)
		if err != nil {
			return fmt.Errorf("blob store: %w", err)
		}
		if !has {
			return fmt.Errorf("a pending offer's layer %s is no longer stored, and the engine can't upload it", d)
		}
	}
	return nil
}

// fence reads GC's epoch for the scope, now. A failed read fences at epoch
// 0, which refuses every object GC deleted lately: an export that names one
// is tried again.
//
// The fence is read at most once per epochTTL, and kept: a fence read
// earlier is still read before this try's checks, so it is as safe, and at
// worst older, which only refuses more. A refused export forgets it
// (forgetEpoch), so its next try reads a fresh one.
func (x *Exporter) fence(ctx context.Context) metastore.Fence {
	x.mu.Lock()
	f, ok := x.epoch, x.epochOK
	x.mu.Unlock()
	if ok && x.cfg.Meta.Now().Sub(f.Started) < epochTTL {
		return f
	}
	started := x.cfg.Meta.Now()
	epoch, err := x.cfg.Meta.ReadEpoch(ctx, x.cfg.Scope)
	if err != nil {
		x.log.Warn("export: reading GC's epoch failed; fencing at 0", "error", err)
		return metastore.Fence{Epoch: 0, Started: started}
	}
	f = metastore.Fence{Epoch: epoch, Started: started}
	x.mu.Lock()
	x.epoch, x.epochOK = f, true
	x.mu.Unlock()
	return f
}

// forgetEpoch drops the kept fence.
func (x *Exporter) forgetEpoch() {
	x.mu.Lock()
	x.epochOK = false
	x.mu.Unlock()
}

// promote verifies and promotes the blobs the engine reported done, of those
// it was asked for, and returns those now stored. A blob whose content
// doesn't match its digest, or that wasn't uploaded at all, isn't stored and
// is in res.Rejected; one the store failed to promote isn't either, and is
// asked for again on the next try like a failed upload. It fails only when
// ctx ends: the staged blobs not yet promoted are then left to the bucket's
// lifecycle rule, and the next export asks for them again.
func (x *Exporter) promote(ctx context.Context, eng Engine, stage blobstore.Stage, blobs []blobstore.Blob, urls map[digest.Digest]string, reported []digest.Digest, res *Result) (map[digest.Digest]bool, error) {
	sizes := make(map[digest.Digest]int64, len(blobs))
	for _, b := range blobs {
		sizes[b.Digest] = b.Size
	}
	var todo []blobstore.Blob
	seen := make(map[digest.Digest]bool, len(reported))
	for _, d := range reported {
		if _, asked := urls[d]; asked && !seen[d] {
			seen[d] = true
			todo = append(todo, blobstore.Blob{Digest: d, Size: sizes[d]})
		}
	}
	done := make(map[digest.Digest]bool, len(todo))
	for _, r := range blobstore.PromoteAll(ctx, x.cfg.Blobs, stage, todo) {
		if r.Err != nil {
			if ctx.Err() != nil {
				return done, context.Cause(ctx)
			}
			x.cfg.Metrics.BlobPromotion(x.cfg.Scope, metrics.PromotionError, "", 0, r.Duration)
			x.log.Warn("export: blob promotion failed; asking for it again", "digest", r.Digest, "error", r.Err)
			continue
		}
		x.cfg.Metrics.BlobPromotion(x.cfg.Scope, string(r.Outcome), r.HashedBy, r.Bytes, r.Duration)
		if r.Stored() {
			done[r.Digest] = true
			res.Uploaded = append(res.Uploaded, r.Digest)
			continue
		}
		res.Rejected = append(res.Rejected, r.Digest)
		x.log.Warn("export: uploaded blob not stored; asking for it again", "cacheID", eng.CacheID(),
			"digest", r.Digest, "size", r.Size, "outcome", string(r.Outcome), "hashedBy", r.HashedBy)
	}
	return done, nil
}

// survivingRoots are the roots not skipped.
func survivingRoots(roots []uint64, skipped []protocol.SkippedRoot) []uint64 {
	gone := make(map[uint64]bool, len(skipped))
	for _, s := range skipped {
		gone[s.Number] = true
	}
	var out []uint64
	for _, n := range roots {
		if !gone[n] {
			out = append(out, n)
		}
	}
	return out
}

func (x *Exporter) requestExport(ctx context.Context, eng Engine, roots []uint64, outputs []protocol.ExportOutput) (protocol.Exported, error) {
	body, err := x.encode(protocol.Export{Roots: roots, Outputs: outputs})
	if err != nil {
		return protocol.Exported{}, err
	}
	rctx, cancel := context.WithTimeout(ctx, x.cfg.ExportTimeout)
	defer cancel()
	env, err := eng.Request(rctx, protocol.TypeExport, body)
	if err != nil {
		return protocol.Exported{}, fmt.Errorf("export: %w", err)
	}
	if err := replyError(env); err != nil {
		return protocol.Exported{}, fmt.Errorf("export: %w", err)
	}
	if env.Type != protocol.TypeExported {
		return protocol.Exported{}, fmt.Errorf("export answered with %s", env.Type)
	}
	var exported protocol.Exported
	if err := env.Decode(&exported); err != nil {
		return protocol.Exported{}, err
	}
	if exported.Bundle != nil && exported.ExportID == "" {
		return protocol.Exported{}, errors.New("export: a bundle with no export ID")
	}
	return exported, nil
}

func (x *Exporter) requestUpload(ctx context.Context, eng Engine, exportID string, urls map[digest.Digest]string) (protocol.Uploaded, error) {
	if urls == nil {
		urls = map[digest.Digest]string{}
	}
	body, err := x.encode(protocol.Upload{ExportID: exportID, URLs: urls})
	if err != nil {
		return protocol.Uploaded{}, err
	}
	rctx, cancel := context.WithTimeout(ctx, x.cfg.UploadTimeout)
	defer cancel()
	env, err := eng.Request(rctx, protocol.TypeUpload, body)
	if err != nil {
		return protocol.Uploaded{}, fmt.Errorf("upload: %w", err)
	}
	if err := replyError(env); err != nil {
		return protocol.Uploaded{}, fmt.Errorf("upload: %w", err)
	}
	if env.Type != protocol.TypeUploaded {
		return protocol.Uploaded{}, fmt.Errorf("upload answered with %s", env.Type)
	}
	var uploaded protocol.Uploaded
	if err := env.Decode(&uploaded); err != nil {
		return protocol.Uploaded{}, err
	}
	return uploaded, nil
}

// release sends an empty upload, which releases the export's chains.
func (x *Exporter) release(ctx context.Context, eng Engine, exportID string) error {
	_, err := x.requestUpload(ctx, eng, exportID, nil)
	if err != nil && ctx.Err() == nil {
		// The chains are released anyway when the connection ends.
		x.log.Warn("export: release failed", "exportID", exportID, "error", err)
		return nil
	}
	return err
}

// encode encodes a request body, refusing one whose envelope would be over
// the message limit.
func (x *Exporter) encode(body any) (json.RawMessage, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	// The envelope adds its ID, type and field names.
	const envelopeOverhead = 128
	if len(data)+envelopeOverhead > x.cfg.MaxMessageBytes {
		return nil, errTooLarge
	}
	return data, nil
}

// replyError is the error of an error reply, nil for any other. An error
// reply saying the reply was too large is errTooLarge.
func replyError(env protocol.Envelope) error {
	if env.Type != protocol.TypeError {
		return nil
	}
	var e protocol.Error
	if err := env.Decode(&e); err != nil {
		return err
	}
	if e.Message == protocol.TooLargeMessage {
		return errTooLarge
	}
	return errors.New(e.Message)
}

// volatileLayerAnnotations are layer descriptor annotations an engine reports
// for some exports of a chain and not others.
//
// buildkit/createdat is the layer's creation time, which ExportLayer.CreatedAt
// carries too. The engine reads it from the snapshot's metadata when it builds
// the descriptor, and a layer it has just compressed for the export has none
// yet: a real engine's first export of an exec's layers leaves it out, and
// every later export has it (seen on the real-engine run, #30). It is internal
// to the engine, which strips it from the images it exports too.
var volatileLayerAnnotations = []string{"buildkit/createdat"}

// stripVolatile removes from an exported bundle what belongs to one export,
// or one engine's session, rather than to the values, before it is stored:
//
//   - the download addresses of every chain. An engine that merged a bundle
//     from this service holds the merge's offers, with presigned GETs, in its
//     records' pending offers until it pulls them, and exports them as they
//     are. Stored, they would be handed to every engine that merges the
//     bundle, as GETs that work until they expire but that this service never
//     renewed, and that read the store under whatever scope signed them (seen
//     on the real-engine run, #31). Without them, an engine that wants such a
//     part renews it through its own session, with the chain's renewal key,
//     and the service signs GETs for the blobs it still has. The Cloud's own
//     merge drops the pending offers altogether; they are kept here, without
//     addresses, as they may name the values an offer's owner needs.
//   - volatileLayerAnnotations.
//
// Either would also make each export of the same values a different bundle,
// which moves them in the index and is merged again into every engine.
func stripVolatile(bundle *dagql.ValueBundle) {
	strip := func(c *dagql.OfferedChain) {
		c.Addresses = nil
		for i := range c.Layers {
			ann := c.Layers[i].Descriptor.Annotations
			for _, k := range volatileLayerAnnotations {
				delete(ann, k)
			}
			if ann != nil && len(ann) == 0 {
				c.Layers[i].Descriptor.Annotations = nil
			}
		}
	}
	for i := range bundle.Values {
		offers := bundle.Values[i].Record.Envelope.PendingOffers
		for j := range offers {
			strip(&offers[j].Chain)
		}
	}
	for _, o := range bundle.Outputs {
		if o.Chain != nil {
			strip(o.Chain)
		}
	}
}

// neededBlobs are the layer blobs a bundle's outputs need, each once.
func neededBlobs(bundle *dagql.ValueBundle) ([]blobstore.Blob, error) {
	seen := map[digest.Digest]int64{}
	var blobs []blobstore.Blob
	for _, o := range bundle.Outputs {
		if o.Chain == nil {
			continue
		}
		for _, l := range o.Chain.Layers {
			d, size := l.Descriptor.Digest, l.Descriptor.Size
			if prev, ok := seen[d]; ok {
				if prev != size {
					return nil, fmt.Errorf("blob %s listed with sizes %d and %d", d, prev, size)
				}
				continue
			}
			seen[d] = size
			blobs = append(blobs, blobstore.Blob{Digest: d, Size: size})
		}
	}
	return blobs, nil
}
