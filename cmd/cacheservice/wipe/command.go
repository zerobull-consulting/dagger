// Package wipe is `cacheservice wipe`: it empties a store's prefix, of any
// layout (layout 1 has no ownership marker), so that the next writer claims
// it afresh. It is the migration path between layouts (README.md, "Changing
// layouts").
//
// A dry run, the default, only reports what the prefix holds. A wipe needs
// -dry-run=false and -confirm=<bucket>/<prefix>, and deletes, in order:
//
//  1. the ownership marker, so that a wipe cut off halfway leaves keys and
//     no marker, which every instance refuses (ownership.ErrNotOwned);
//  2. the objects, PageSize per request, each listed under the prefix;
//  3. the incomplete multipart uploads under the prefix;
//
// then lists the prefix again, and exits 0 only if it is empty. It never
// reads engine data, and deletes only keys a listing under the prefix
// returned (or the marker's own key).
package wipe

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/lifecycle"
)

// Exit codes, as lifecycle's.
const (
	// ExitOK: a dry run reported, or a wipe left the prefix empty.
	ExitOK = 0
	// ExitError: a request or a key's delete failed; what was done so far
	// is reported.
	ExitError = 1
	// ExitUsage: bad flags, an empty prefix, or -confirm missing or wrong;
	// nothing was read or deleted.
	ExitUsage = 2
	// ExitNotEmpty: every request succeeded, but the final listing found
	// keys or uploads under the prefix: a writer was still running.
	ExitNotEmpty = 3
)

// DefaultTimeout bounds a whole run. A wipe lists and deletes 1,000 keys a
// request pair, so this is room for a few million keys.
const DefaultTimeout = 30 * time.Minute

// MaxPages bounds each listing loop: PageSize keys or uploads a page, so
// 100 million of either.
const MaxPages = 100_000

// MaxShown is how many keys and uploads a report lists.
const MaxShown = 20

// MaxMarkerBytes is the most of the marker read: the service writes a few
// hundred bytes.
const MaxMarkerBytes = 64 << 10

// Options are a run's settings, from the flags and CACHESERVICE_S3_*.
type Options struct {
	Bucket, Prefix string
	// DryRun only reports.
	DryRun bool
	// Confirm is -confirm: <bucket>/<prefix> (lifecycle.ConfirmValue).
	Confirm string
}

const usage = `usage: cacheservice wipe [-dry-run=false -confirm=<bucket>/<prefix>] [flags]

Empties the prefix CACHESERVICE_S3_PREFIX of the bucket CACHESERVICE_S3_BUCKET,
of any layout. By default a dry run: it reports the objects, their bytes, the
incomplete multipart uploads and the ownership marker's layout, and deletes
nothing. With -dry-run=false and -confirm repeating the bucket and the prefix,
it deletes the marker, then the objects, then aborts the uploads, then lists
the prefix again. Stop every writer on the prefix first. An empty prefix (the
whole bucket) isn't supported.

Exit codes: 0 ok (a wipe: the prefix is empty), 1 error, 2 usage or
confirmation, 3 the prefix isn't empty after the wipe (a writer is running).

flags:
`

// Main runs the command with args (after "wipe"), and returns its exit
// code. It never prints the credentials.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer, open Opener) int {
	fs := flag.NewFlagSet("cacheservice wipe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}
	var o Options
	var timeout time.Duration
	fs.BoolVar(&o.DryRun, "dry-run", true, "only report what the prefix holds; false: delete it all, which needs -confirm")
	fs.StringVar(&o.Confirm, "confirm", "", "the bucket and prefix it wipes, as <bucket>/<prefix>")
	fs.DurationVar(&timeout, "timeout", DefaultTimeout, "bound on the whole run")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	fail := func(code int, format string, a ...any) int {
		fmt.Fprintf(stderr, "cacheservice wipe: "+format+"\n", a...)
		return code
	}
	if fs.NArg() > 0 {
		return fail(ExitUsage, "unexpected arguments %q", fs.Args())
	}
	if timeout <= 0 {
		return fail(ExitUsage, "-timeout must be positive")
	}
	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		return fail(ExitUsage, "%v", err)
	}
	if cfg.Bucket == "" {
		return fail(ExitUsage, "%s is not set", blobstore.EnvBucket)
	}
	o.Bucket, o.Prefix = cfg.Bucket, cfg.Prefix
	if msg := problem(o); msg != "" {
		return fail(ExitUsage, "%s; nothing read or deleted", msg)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	st, err := open(ctx, cfg)
	if err != nil {
		return fail(ExitError, "%v", err)
	}
	return Run(ctx, o, st, stdout, stderr)
}

// problem is why o can't run, or "": checked before anything is read.
func problem(o Options) string {
	p := strings.Trim(o.Prefix, "/")
	if p == "" {
		return fmt.Sprintf("%s is %q: wiping a whole bucket isn't supported; set the prefix to wipe", blobstore.EnvPrefix, o.Prefix)
	}
	if slices.ContainsFunc(strings.Split(p, "/"), func(s string) bool { return s == "" || s == "." || s == ".." }) {
		return fmt.Sprintf("%s is %q: it can't contain empty, . or .. segments", blobstore.EnvPrefix, o.Prefix)
	}
	want := lifecycle.ConfirmValue(o.Bucket, o.Prefix)
	switch {
	case o.Confirm == want:
		return ""
	case o.Confirm == "" && o.DryRun:
		return ""
	case o.Confirm == "":
		return fmt.Sprintf("-dry-run=false needs -confirm=%s, repeating the bucket and prefix it wipes", want)
	}
	return fmt.Sprintf("-confirm=%s doesn't match the bucket and prefix it would wipe, %s", o.Confirm, want)
}

// Summary is what a listing found under the prefix.
type Summary struct {
	Objects int
	Bytes   int64
	Keys    []string // the first MaxShown
	Uploads int
	Upload  []string // the first MaxShown upload keys
}

// Empty reports whether nothing is under the prefix.
func (s Summary) Empty() bool { return s.Objects == 0 && s.Uploads == 0 }

// Run runs a dry run or a wipe of o's prefix against st, printing its
// report to out and why it failed to errOut.
func Run(ctx context.Context, o Options, st Store, out, errOut io.Writer) int {
	fail := func(code int, format string, a ...any) int {
		fmt.Fprintf(errOut, "cacheservice wipe: "+format+"\n", a...)
		return code
	}
	// Again, for Run's other callers: before anything is read.
	if msg := problem(o); msg != "" {
		return fail(ExitUsage, "%s; nothing read or deleted", msg)
	}
	root := layout.Root(o.Prefix)
	fmt.Fprintf(out, "bucket %s, prefix %q\n", o.Bucket, root)

	markerKey := layout.MarkerKey(o.Prefix)
	marker, merr := readMarker(ctx, st, markerKey)
	switch {
	case merr == nil:
		fmt.Fprintf(out, "ownership marker: %s, %s\n", markerKey, marker)
	case errors.Is(merr, ErrNotFound):
		fmt.Fprintf(out, "ownership marker: none at %s (layout 1, or not the service's)\n", markerKey)
	default:
		if !o.DryRun {
			// The order (the marker first) is what makes a cut-off
			// wipe safe: without knowing the marker is gone, stop.
			return fail(ExitError, "can't read the ownership marker %s: %v; nothing deleted", markerKey, merr)
		}
		fmt.Fprintf(out, "ownership marker: can't read %s: %v\n", markerKey, merr)
	}

	if o.DryRun {
		s, err := survey(ctx, st, root)
		if err != nil {
			return fail(ExitError, "%v", err)
		}
		printSummary(out, s)
		if s.Empty() {
			fmt.Fprintln(out, "dry run: the prefix is empty; nothing to wipe")
			return ExitOK
		}
		fmt.Fprintf(out, "dry run: nothing deleted. To wipe it, stop every writer on the prefix, then run, with the same CACHESERVICE_S3_* settings:\n  cacheservice wipe -dry-run=false -confirm=%s\n",
			lifecycle.ConfirmValue(o.Bucket, o.Prefix))
		return ExitOK
	}

	var failures []Failure
	// 1. The marker.
	if merr == nil {
		failed, err := st.Delete(ctx, []string{markerKey})
		if err == nil && len(failed) > 0 {
			err = errors.New(failed[0].Reason)
		}
		if err != nil {
			return fail(ExitError, "delete the ownership marker %s: %v; nothing else deleted", markerKey, err)
		}
		fmt.Fprintf(out, "deleted the ownership marker %s\n", markerKey)
	}

	// 2. The objects.
	var deleted int
	var bytes int64
	report := func() {
		fmt.Fprintf(out, "deleted %d objects, %d bytes; %d failed\n", deleted, bytes, len(failures))
	}
	startAfter := ""
	for page := 0; ; page++ {
		if page == MaxPages {
			report()
			return fail(ExitError, "more than %d pages of objects under the prefix: stopped", MaxPages)
		}
		objs, more, err := st.List(ctx, root, startAfter)
		if err == nil {
			err = under(root, objKeys(objs))
		}
		if err != nil {
			report()
			return fail(ExitError, "%v", err)
		}
		if len(objs) == 0 {
			break
		}
		keys := objKeys(objs)
		failed, err := st.Delete(ctx, keys)
		if err != nil {
			report()
			return fail(ExitError, "%v", err)
		}
		failures = append(failures, failed...)
		bad := make(map[string]bool, len(failed))
		for _, f := range failed {
			bad[f.Key] = true
		}
		for _, ob := range objs {
			if !bad[ob.Key] {
				deleted++
				bytes += ob.Size
			}
		}
		startAfter = keys[len(keys)-1]
		if !more {
			break
		}
	}
	report()

	// 3. The incomplete multipart uploads.
	var aborted, abortFailures int
	keyMarker, idMarker := "", ""
	for page := 0; ; page++ {
		if page == MaxPages {
			return fail(ExitError, "more than %d pages of multipart uploads: stopped", MaxPages)
		}
		ups, nk, ni, err := st.ListUploads(ctx, root, keyMarker, idMarker)
		if err == nil {
			err = under(root, upKeys(ups))
		}
		if err != nil {
			return fail(ExitError, "aborted %d uploads: %v", aborted, err)
		}
		for _, u := range ups {
			if err := st.Abort(ctx, u); err != nil {
				if ctx.Err() != nil {
					return fail(ExitError, "aborted %d uploads: %v", aborted, context.Cause(ctx))
				}
				abortFailures++
				failures = append(failures, Failure{Key: "upload:" + u.Key, Reason: err.Error()})
				continue
			}
			aborted++
		}
		if nk == "" && ni == "" {
			break
		}
		keyMarker, idMarker = nk, ni
	}
	fmt.Fprintf(out, "aborted %d incomplete multipart uploads; %d failed\n", aborted, abortFailures)
	for i, f := range failures {
		if i == MaxShown {
			fmt.Fprintf(out, "  ... and %d more failures\n", len(failures)-MaxShown)
			break
		}
		fmt.Fprintf(out, "  FAILED %q: %s\n", f.Key, f.Reason)
	}

	// 4. List again.
	s, err := survey(ctx, st, root)
	if err != nil {
		return fail(ExitError, "can't list the prefix after the wipe: %v", err)
	}
	if !s.Empty() {
		fmt.Fprintln(out, "after the wipe, the prefix still holds:")
		printSummary(out, s)
	}
	switch {
	case len(failures) > 0:
		return fail(ExitError, "%d deletes or aborts failed; the prefix isn't empty", len(failures))
	case !s.Empty():
		return fail(ExitNotEmpty, "the prefix isn't empty after the wipe: is a writer still running on it? Stop it, and wipe again")
	}
	fmt.Fprintln(out, "wipe: the prefix is empty")
	return ExitOK
}

// markerInfo is what the report says of a marker.
type markerInfo struct {
	layout int
	ok     bool
}

func (m markerInfo) String() string {
	if !m.ok {
		return "not a marker the service wrote (no layout version)"
	}
	return fmt.Sprintf("layout %d", m.layout)
}

// readMarker reads the marker's layout version, and nothing else of it.
func readMarker(ctx context.Context, st Store, key string) (markerInfo, error) {
	body, err := st.Get(ctx, key)
	if err != nil {
		return markerInfo{}, err
	}
	var m struct {
		Layout *int `json:"layout"`
	}
	if len(body) > MaxMarkerBytes || json.Unmarshal(body, &m) != nil || m.Layout == nil {
		return markerInfo{}, nil
	}
	return markerInfo{layout: *m.Layout, ok: true}, nil
}

// survey lists everything under root.
func survey(ctx context.Context, st Store, root string) (Summary, error) {
	var s Summary
	startAfter := ""
	for page := 0; ; page++ {
		if page == MaxPages {
			return s, fmt.Errorf("more than %d pages of objects under the prefix: stopped", MaxPages)
		}
		objs, more, err := st.List(ctx, root, startAfter)
		if err == nil {
			err = under(root, objKeys(objs))
		}
		if err != nil {
			return s, err
		}
		for _, ob := range objs {
			s.Objects++
			s.Bytes += ob.Size
			if len(s.Keys) < MaxShown {
				s.Keys = append(s.Keys, ob.Key)
			}
		}
		if len(objs) == 0 || !more {
			break
		}
		startAfter = objs[len(objs)-1].Key
	}
	keyMarker, idMarker := "", ""
	for page := 0; ; page++ {
		if page == MaxPages {
			return s, fmt.Errorf("more than %d pages of multipart uploads: stopped", MaxPages)
		}
		ups, nk, ni, err := st.ListUploads(ctx, root, keyMarker, idMarker)
		if err == nil {
			err = under(root, upKeys(ups))
		}
		if err != nil {
			return s, err
		}
		for _, u := range ups {
			s.Uploads++
			if len(s.Upload) < MaxShown {
				s.Upload = append(s.Upload, u.Key)
			}
		}
		if nk == "" && ni == "" {
			break
		}
		keyMarker, idMarker = nk, ni
	}
	return s, nil
}

// under refuses keys a listing returned outside root: the store's answer,
// checked before any of it is deleted.
func under(root string, keys []string) error {
	for _, k := range keys {
		if !strings.HasPrefix(k, root) {
			return fmt.Errorf("the store listed %q, which isn't under %q: stopped, nothing more deleted", k, root)
		}
	}
	return nil
}

func objKeys(objs []Object) []string {
	keys := make([]string, len(objs))
	for i, o := range objs {
		keys[i] = o.Key
	}
	return keys
}

func upKeys(ups []Upload) []string {
	keys := make([]string, len(ups))
	for i, u := range ups {
		keys[i] = u.Key
	}
	return keys
}

func printSummary(out io.Writer, s Summary) {
	fmt.Fprintf(out, "objects: %d, %d bytes\n", s.Objects, s.Bytes)
	for _, k := range s.Keys {
		fmt.Fprintf(out, "  %q\n", k)
	}
	if s.Objects > len(s.Keys) {
		fmt.Fprintf(out, "  ... and %d more\n", s.Objects-len(s.Keys))
	}
	fmt.Fprintf(out, "incomplete multipart uploads: %d\n", s.Uploads)
	for _, k := range s.Upload {
		fmt.Fprintf(out, "  %q\n", k)
	}
	if s.Uploads > len(s.Upload) {
		fmt.Fprintf(out, "  ... and %d more\n", s.Uploads-len(s.Upload))
	}
}
