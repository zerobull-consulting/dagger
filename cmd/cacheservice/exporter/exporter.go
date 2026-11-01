// Package exporter is the service's side of pushing an engine's cache entries
// to the store: export, exported, upload, uploaded, then publish.
//
// One export of some roots goes:
//
//  1. The service sends export with the roots and the parts to upload. The
//     engine answers exported: a bundle, if any root survived, and the roots
//     it skipped, as gone, busy or refused. It holds the bundle's chains
//     under the reply's exportID until upload.
//  2. The service works out which layer blobs the bundle's outputs need,
//     drops those the blob store already has, and sends upload with a
//     presigned PUT for each of the rest. An empty map needs nothing, and
//     releases the chains.
//  3. The engine PUTs each blob and answers uploaded: which blobs are done,
//     and which failed.
//  4. Only once every blob is stored does the service publish the bundle and
//     record its values in the scope's index. If a blob failed, the export
//     is tried again from step 1, since the engine released the chains with
//     its answer. Blobs stored by the earlier try are not asked for again.
//
// The engine works on at most remotecache.ExportsInFlight exports at once,
// and queues the rest, so the service sends no more than that to one engine.
// The engine uploads at most remotecache.UploadConcurrency blobs at once
// across its exports, so an upload's reply can take a while. Neither side
// accepts a message over protocol.MaxMessageBytes: an export whose reply
// would be larger is answered with protocol.TooLargeMessage, and an upload
// whose request would be. Either way, the roots are split in two and each
// half is exported on its own.
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
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
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
	// UploadAttempts, RetryBackoff, ExportTimeout and UploadTimeout default
	// to the Default* values when zero.
	UploadAttempts int
	RetryBackoff   time.Duration
	ExportTimeout  time.Duration
	UploadTimeout  time.Duration
	// MaxMessageBytes is the largest request the exporter sends. Zero means
	// protocol.MaxMessageBytes. Tests lower it.
	MaxMessageBytes int
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
}

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
	// Gone, Busy and Refused are the roots the engine skipped.
	Gone    []uint64               `json:"gone,omitempty"`
	Busy    []protocol.SkippedRoot `json:"busy,omitempty"`
	Refused []protocol.SkippedRoot `json:"refused,omitempty"`
	// Failed roots were exported, but could not be stored: their blobs kept
	// failing to upload, the store failed, or a root alone was too large
	// for a message. Each has its error.
	Failed []FailedRoot `json:"failed,omitempty"`
	// Bundles are the IDs of the published bundles.
	Bundles []string `json:"bundles,omitempty"`
	// Uploaded are the blobs the engine uploaded, and Present those it was
	// not asked for because the store had them.
	Uploaded []digest.Digest `json:"uploaded,omitempty"`
	Present  []digest.Digest `json:"present,omitempty"`
}

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
}

func (r *Result) fail(roots []uint64, err error) {
	for _, n := range roots {
		r.Failed = append(r.Failed, FailedRoot{Number: n, Error: err.Error()})
	}
}

// errTooLarge is a request, or the engine's reply to it, over the message
// limit.
var errTooLarge = errors.New(protocol.TooLargeMessage)

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
// upload, up to UploadAttempts tries.
func (x *Exporter) exportWithRetries(ctx context.Context, eng Engine, roots []uint64, outputs []protocol.ExportOutput) (Result, error) {
	var all Result
	wait := x.cfg.RetryBackoff
	for attempt := 1; ; attempt++ {
		res, retry, err := x.exportOnce(ctx, eng, roots, outputs)
		all.Merge(res)
		if err != nil || len(retry) == 0 {
			return all, err
		}
		if attempt >= x.cfg.UploadAttempts {
			all.fail(retry, fmt.Errorf("blobs failed to upload %d times", attempt))
			return all, nil
		}
		x.log.Info("export: blobs failed to upload; trying again", "roots", len(retry), "attempt", attempt, "wait", wait)
		if err := x.sleep(ctx, wait); err != nil {
			return all, err
		}
		wait *= 2
		roots = retry
	}
}

// exportOnce runs one export, upload and publish. It returns the roots to
// try again because blobs failed to upload.
func (x *Exporter) exportOnce(ctx context.Context, eng Engine, roots []uint64, outputs []protocol.ExportOutput) (res Result, retry []uint64, _ error) {
	slots := x.engineSlots(eng)
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return res, nil, context.Cause(ctx)
	}
	defer func() { <-slots }()

	exported, err := x.requestExport(ctx, eng, roots, outputs)
	if err != nil {
		return res, nil, err
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
		return res, nil, nil
	}
	survivors := survivingRoots(roots, exported.Skipped)

	blobs, err := neededBlobs(exported.Bundle)
	if err != nil {
		err = fmt.Errorf("invalid bundle: %w", err)
	}
	var urls map[digest.Digest]string
	if err == nil {
		urls, err = blobstore.UploadURLs(ctx, x.cfg.Blobs, blobs)
		if err != nil {
			err = fmt.Errorf("blob store: %w", err)
		}
	}
	if err != nil {
		// Release the chains; the export isn't stored.
		if rerr := x.release(ctx, eng, exported.ExportID); rerr != nil {
			return res, nil, rerr
		}
		if ctx.Err() != nil {
			return res, nil, context.Cause(ctx)
		}
		res.fail(survivors, err)
		return res, nil, nil
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
			return res, nil, rerr
		}
		return Result{}, nil, errTooLarge
	}
	if err != nil {
		return res, nil, err
	}
	done := make(map[digest.Digest]bool, len(uploaded.Done))
	for _, d := range uploaded.Done {
		if _, asked := urls[d]; asked {
			done[d] = true
			res.Uploaded = append(res.Uploaded, d)
		}
	}
	var missing int
	for d := range urls {
		if !done[d] {
			missing++
		}
	}
	for _, f := range uploaded.Failed {
		x.log.Warn("export: blob upload failed", "digest", f.Digest, "error", f.Message)
	}
	if missing > 0 {
		return res, survivors, nil
	}

	rec, err := x.cfg.Meta.Record(ctx, x.cfg.Scope, exported.Bundle, eng.CacheID(), eng.Generation())
	if err != nil {
		if ctx.Err() != nil {
			return res, nil, context.Cause(ctx)
		}
		res.fail(survivors, fmt.Errorf("publish: %w", err))
		return res, nil, nil
	}
	res.Stored = append(res.Stored, survivors...)
	res.Bundles = append(res.Bundles, rec.BundleID)
	x.log.Info("export stored", "cacheID", eng.CacheID(), "roots", len(survivors),
		"values", len(exported.Bundle.Values), "bundle", rec.BundleID,
		"uploaded", len(res.Uploaded), "present", len(res.Present))
	return res, nil, nil
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
