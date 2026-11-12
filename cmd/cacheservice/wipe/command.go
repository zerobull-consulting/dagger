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
//     a marker it can't read stops it;
//  2. the objects, PageSize per request, each listed under the prefix;
//  3. the incomplete multipart uploads under the prefix;
//
// before each request of 2 and 3 it checks again that no ancestor prefix and
// not the prefix itself has a marker: one that appeared is a writer's, and
// stops the wipe with what it wrote left alone;
//
// then lists the prefix again, and exits 0 only if it is empty. It never
// reads engine data, and deletes only keys a listing under the prefix
// returned (or the marker's own key). It refuses a prefix inside another
// store's (an ancestor prefix holds a marker), whose wipe would delete part
// of that store.
package wipe

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/lifecycle"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// Exit codes, as lifecycle's.
const (
	// ExitOK: a dry run reported, or a wipe left the prefix empty.
	ExitOK = 0
	// ExitError: a request or a key's delete failed, or the prefix is
	// refused (inside another store's, or its marker unreadable), or a
	// marker appeared during the wipe (a writer claimed the prefix or an
	// ancestor) or can't be read; what was done so far is reported.
	ExitError = 1
	// ExitUsage: bad flags, an empty prefix, or -confirm missing or wrong;
	// nothing was read or deleted.
	ExitUsage = 2
	// ExitNotEmpty: every request succeeded, but the final listing found
	// keys or uploads under the prefix: a writer was still running.
	ExitNotEmpty = 3
)

// DefaultTimeout bounds a whole run. A wipe lists and deletes 1,000 keys a
// request pair, one pair at a time, so this is room for about one to two
// million keys; a larger prefix takes another run, or a longer -timeout.
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
the prefix again, and stops if a marker appears meanwhile. Stop every writer
on the prefix first. An empty prefix (the whole bucket) isn't supported, nor
is a prefix inside another store's.

Exit codes: 0 ok (a wipe: the prefix is empty), 1 error, refusal, or a marker
that appeared during the wipe, 2 usage or confirmation, 3 the prefix isn't
empty after the wipe (a writer is running).

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
		fmt.Fprintf(stderr, "cacheservice wipe: %s\n", printable(fmt.Sprintf(format, a...)))
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
	// Nested counts the ownership markers under the prefix other than its
	// own: stores nested in it, which a wipe deletes too. NestedKeys are
	// the first MaxShown.
	Nested     int
	NestedKeys []string
}

// Empty reports whether nothing is under the prefix.
func (s Summary) Empty() bool { return s.Objects == 0 && s.Uploads == 0 }

// Run runs a dry run or a wipe of o's prefix against st, printing its
// report to out and why it failed to errOut.
func Run(ctx context.Context, o Options, st Store, out, errOut io.Writer) int {
	// What the store answers (errors, keys) is printed escaped: it can't
	// write control characters to the terminal.
	fail := func(code int, format string, a ...any) int {
		fmt.Fprintf(errOut, "cacheservice wipe: %s\n", printable(fmt.Sprintf(format, a...)))
		return code
	}
	// Again, for Run's other callers: before anything is read.
	if msg := problem(o); msg != "" {
		return fail(ExitUsage, "%s; nothing read or deleted", msg)
	}
	root := layout.Root(o.Prefix)
	fmt.Fprintf(out, "bucket %s, prefix %q\n", o.Bucket, root)

	if err := checkAncestors(ctx, st, o.Prefix); err != nil {
		return fail(ExitError, "%v; nothing deleted", err)
	}
	markerKey := layout.MarkerKey(o.Prefix)
	marker, err := readMarker(ctx, st, o.Prefix)
	if err != nil {
		// The order (the marker first) is what makes a cut-off wipe
		// safe: without knowing whether there is one, stop.
		return fail(ExitError, "can't read the ownership marker %s: %v; nothing deleted", markerKey, err)
	}
	fmt.Fprintf(out, "ownership marker: %s\n", printable(marker.desc))

	if o.DryRun {
		s, err := survey(ctx, st, root)
		if err != nil {
			return fail(ExitError, "%v", err)
		}
		printSummary(out, s)
		if s.Nested > 0 {
			fmt.Fprintf(out, "WARNING: %d ownership markers under the prefix, of stores nested in it, which the wipe deletes too:\n", s.Nested)
			printKeys(out, s.NestedKeys, s.Nested)
		}
		switch v, err := st.Versioning(ctx); {
		case err != nil:
			fmt.Fprintf(out, "bucket versioning: can't read it: %s\n", printable(err.Error()))
		case v != "":
			fmt.Fprintf(out, "WARNING: the bucket's versioning is %s: a wipe refuses a versioned bucket\n", printable(v))
		default:
			fmt.Fprintln(out, "bucket versioning: never enabled")
		}
		if s.Empty() {
			fmt.Fprintln(out, "dry run: the prefix is empty; nothing to wipe")
			return ExitOK
		}
		fmt.Fprintf(out, "dry run: nothing deleted. To wipe it, stop every writer on the prefix, then run, with the same CACHESERVICE_S3_* settings:\n  cacheservice wipe -dry-run=false -confirm=%s\n",
			lifecycle.ConfirmValue(o.Bucket, o.Prefix))
		return ExitOK
	}

	// Before anything is deleted: list it all, read-only. A listing the key
	// isn't allowed (the bucket's uploads, on some stores) refuses now, not
	// halfway through; and the stores nested in the prefix are named.
	pre, err := survey(ctx, st, root)
	if err != nil {
		return fail(ExitError, "can't list the prefix: %v; nothing deleted", err)
	}
	if pre.Nested > 0 {
		fmt.Fprintf(out, "WARNING: %d ownership markers under the prefix, of stores nested in it, which this wipe deletes too:\n", pre.Nested)
		printKeys(out, pre.NestedKeys, pre.Nested)
	}
	// A versioned bucket keeps the noncurrent versions of what a delete
	// without a version ID removes, which the listings don't show: the
	// wipe would report an empty prefix and leave it all. A key that may
	// not read the setting gets a warning; any other error refuses, since
	// the store didn't say.
	switch v, err := st.Versioning(ctx); {
	case errors.Is(err, metastore.ErrDenied):
		fmt.Fprintln(out, "WARNING: the key may not read the bucket's versioning: if the bucket is versioned, this wipe leaves the noncurrent versions of all it deletes")
	case err != nil:
		return fail(ExitError, "%v; nothing deleted", err)
	case v != "":
		return fail(ExitError, "the bucket's versioning is %s: a wipe deletes only current versions and leaves the noncurrent ones, so it refuses a versioned bucket; nothing deleted", v)
	}

	var (
		deleted, aborted int
		bytes            int64
		failures         failureLog
		// countLow: a delete request failed, so deleted and bytes may be
		// low (Store.Delete reports only failures).
		countLow bool
	)
	// progress reports what was done, and the failures (up to MaxShown),
	// on every way out after the marker.
	progress := func() {
		atLeast := ""
		if countLow {
			// A page's delete request failed: the keys it deleted one by
			// one before that are not in the count.
			atLeast = "at least "
		}
		fmt.Fprintf(out, "deleted %s%d objects, %s%d bytes; aborted %d incomplete multipart uploads; %d failed\n", atLeast, deleted, atLeast, bytes, aborted, failures.n)
		for _, f := range failures.first {
			fmt.Fprintf(out, "  FAILED %q: %s\n", f.Key, printable(f.Reason))
		}
		if failures.n > len(failures.first) {
			fmt.Fprintf(out, "  ... and %d more failures\n", failures.n-len(failures.first))
		}
	}
	stop := func(format string, a ...any) int {
		progress()
		return fail(ExitError, format, a...)
	}

	// 1. The marker.
	if marker.present {
		failed, err := st.Delete(ctx, []string{markerKey})
		if err == nil && len(failed) > 0 {
			err = errors.New(failed[0].Reason)
		}
		if err != nil {
			return fail(ExitError, "delete the ownership marker %s: %v; nothing else deleted", markerKey, err)
		}
		fmt.Fprintf(out, "deleted the ownership marker %s\n", printable(markerKey))
	}

	// 2. The objects.
	// recheck stops the wipe if, since it started, an ancestor prefix was
	// claimed (that store's keys may be under this prefix: wiping <a>/ns
	// under a new store at <a>), or the prefix's own marker is back (a
	// writer claimed the emptied prefix: its marker and first exports are
	// not the old store's). Before each destructive request. It reads each
	// ancestor's marker, then this prefix's: one GET more than the ancestor
	// check alone, per page. A marker it can't read stops the wipe too: when
	// it can't tell, it doesn't delete.
	recheck := func() error {
		if err := checkAncestors(ctx, st, o.Prefix); err != nil {
			return fmt.Errorf("stopped before the next delete: %w", err)
		}
		switch m, err := readMarker(ctx, st, o.Prefix); {
		case err != nil:
			return fmt.Errorf("stopped before the next delete: can't read the ownership marker %s: %w", markerKey, err)
		case m.present && marker.present:
			return fmt.Errorf("stopped before the next delete: an ownership marker, %s, is back after the wipe deleted the old one: a writer claimed the prefix during the wipe; its marker and anything written after it were left alone", markerKey)
		case m.present:
			return fmt.Errorf("stopped before the next delete: an ownership marker, %s, appeared (the prefix had none): a writer claimed the prefix during the wipe; its marker and anything written after it were left alone", markerKey)
		}
		return nil
	}
	err = walkObjects(ctx, st, root, func(objs []Object) error {
		if err := recheck(); err != nil {
			return err
		}
		// The failures it collected count even when the request failed.
		keyFailures, err := st.Delete(ctx, objKeys(objs))
		failures.add(keyFailures...)
		if err != nil {
			countLow = true
			return err
		}
		bad := make(map[string]bool, len(keyFailures))
		for _, f := range keyFailures {
			bad[f.Key] = true
		}
		for _, ob := range objs {
			if !bad[ob.Key] {
				deleted++
				bytes += ob.Size
			}
		}
		return nil
	})
	if err != nil {
		return stop("%v", err)
	}

	// 3. The incomplete multipart uploads.
	if err := recheck(); err != nil {
		return stop("%v", err)
	}
	err = walkUploads(ctx, st, root, func(ups []Upload) error {
		// Before each page of uploads: a writer's new ones are not the old
		// store's.
		if err := recheck(); err != nil {
			return err
		}
		for _, u := range ups {
			if err := st.Abort(ctx, u); err != nil {
				if ctx.Err() != nil {
					return context.Cause(ctx)
				}
				failures.add(Failure{Key: "upload:" + u.Key, Reason: "upload ID " + strconv.Quote(u.ID) + ": " + err.Error()})
				continue
			}
			aborted++
		}
		return nil
	})
	if err != nil {
		return stop("%v", err)
	}
	progress()

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
	case failures.n > 0 && s.Empty():
		return fail(ExitError, "%d deletes or aborts failed, though the prefix is empty now", failures.n)
	case failures.n > 0:
		return fail(ExitError, "%d deletes or aborts failed; the prefix isn't empty", failures.n)
	case !s.Empty():
		return fail(ExitNotEmpty, "the prefix isn't empty after the wipe: is a writer still running on it? Stop it, and wipe again")
	}
	fmt.Fprintln(out, "wipe: the prefix is empty")
	return ExitOK
}

// failureLog counts every failure, and keeps the first MaxShown for the
// report: a key denied every delete can't grow it without bound.
type failureLog struct {
	n     int
	first []Failure
}

func (l *failureLog) add(fs ...Failure) {
	l.n += len(fs)
	l.first = append(l.first, fs[:min(len(fs), MaxShown-len(l.first))]...)
}

// checkAncestors refuses a prefix inside another store's: an ancestor
// prefix (the bucket's top included) holding an ownership marker owns
// everything under it, so this wipe would delete part of that store. It is
// the service's own check (ownership.CheckAncestors), with what to do
// instead of this wipe.
func checkAncestors(ctx context.Context, st Store, prefix string) error {
	err := ownership.CheckAncestors(ctx, storeGetter{st}, prefix)
	var nested *ownership.ErrNestedPrefix
	if errors.As(err, &nested) {
		instead := "empty the bucket in the provider's console" // an empty prefix isn't supported
		if nested.Ancestor != "" {
			instead = "wipe prefix " + nested.Ancestor + "/ itself"
		}
		return fmt.Errorf("%w; wiping it would delete part of that store, so instead, %s", err, instead)
	}
	return err
}

// storeGetter is st as ownership reads it.
type storeGetter struct{ st Store }

func (g storeGetter) Get(ctx context.Context, key string) ([]byte, string, error) {
	b, err := g.st.Get(ctx, key)
	return b, "", err
}

// markerState is what the report says of the prefix's marker.
type markerState struct {
	present bool
	desc    string
}

// readMarker reads the prefix's marker, and gives ownership.ReadMarker's
// verdict on it: an error only when it can't be read.
func readMarker(ctx context.Context, st Store, prefix string) (markerState, error) {
	key := layout.MarkerKey(prefix)
	g := &getter{st: st}
	m, err := ownership.ReadMarker(ctx, g, prefix)
	var other *ownership.ErrOtherLayout
	switch {
	case err == nil:
		return markerState{true, fmt.Sprintf("%s, layout %d, the service's", key, m.Layout)}, nil
	case errors.Is(err, ownership.ErrNoMarker):
		return markerState{false, "none at " + key + " (layout 1, or not the service's)"}, nil
	case g.err != nil:
		return markerState{}, g.err
	case errors.As(err, &other):
		return markerState{true, fmt.Sprintf("%s, layout %d: another layout's, which this service refuses", key, other.Found)}, nil
	}
	// ErrBadMarker, or not JSON: every instance refuses it.
	return markerState{true, fmt.Sprintf("%s, not one the service wrote, which every instance refuses: %v", key, err)}, nil
}

// getter is st as ownership.ReadMarker reads it, keeping the store's error
// apart from the verdict on the content.
type getter struct {
	st  Store
	err error
}

func (g *getter) Get(ctx context.Context, key string) ([]byte, string, error) {
	b, err := g.st.Get(ctx, key)
	if err != nil && !errors.Is(err, metastore.ErrNotFound) {
		g.err = err
	}
	return b, "", err
}

// survey lists everything under root.
func survey(ctx context.Context, st Store, root string) (Summary, error) {
	var s Summary
	own := root + layout.MarkerName
	err := walkObjects(ctx, st, root, func(objs []Object) error {
		for _, ob := range objs {
			s.Objects++
			s.Bytes += ob.Size
			if len(s.Keys) < MaxShown {
				s.Keys = append(s.Keys, ob.Key)
			}
			if ob.Key != own && strings.HasSuffix(ob.Key, "/"+layout.MarkerName) {
				s.Nested++
				if len(s.NestedKeys) < MaxShown {
					s.NestedKeys = append(s.NestedKeys, ob.Key)
				}
			}
		}
		return nil
	})
	if err != nil {
		return s, err
	}
	err = walkUploads(ctx, st, root, func(ups []Upload) error {
		for _, u := range ups {
			s.Uploads++
			if len(s.Upload) < MaxShown {
				s.Upload = append(s.Upload, u.Key)
			}
		}
		return nil
	})
	return s, err
}

// walkObjects calls fn with each page of the objects under root, in key
// order, at most MaxPages of them. A page is listed after the last key of
// the one before, so a page fn deleted isn't listed again.
func walkObjects(ctx context.Context, st Store, root string, fn func([]Object) error) error {
	startAfter := ""
	for page := 0; page < MaxPages; page++ {
		objs, more, err := st.List(ctx, root, startAfter)
		if err == nil {
			err = under(root, objKeys(objs))
		}
		if err != nil {
			return err
		}
		if len(objs) == 0 {
			return nil
		}
		if err := fn(objs); err != nil {
			return err
		}
		if !more {
			return nil
		}
		startAfter = objs[len(objs)-1].Key
	}
	return fmt.Errorf("more than %d pages of objects under the prefix: stopped", MaxPages)
}

// walkUploads calls fn with each page of the incomplete multipart uploads
// under root. On a store that may list them only by exact key, unless the
// listing by prefix found one strictly below root (so it lists by prefix),
// it lists the bucket's and keeps those under root, as gc does. Such a
// store's listing by prefix may return only the uploads at exactly root,
// which the bucket's listing then skips: fn sees each upload once.
func walkUploads(ctx context.Context, st Store, root string, fn func([]Upload) error) error {
	below, atRoot, err := walkUploadListing(ctx, st, root, root, false, fn)
	if err != nil || below > 0 || !st.UploadsByExactKey() {
		return err
	}
	_, _, err = walkUploadListing(ctx, st, "", root, atRoot > 0, fn)
	return err
}

// walkUploadListing pages through the uploads the store lists for prefix
// ("": the bucket's), at most MaxPages, and calls fn with those under root,
// leaving out those at exactly root with skipRoot. Any other upload the
// store lists is left out, and never aborted. It returns how many it passed
// to fn strictly below root, and at exactly root.
func walkUploadListing(ctx context.Context, st Store, prefix, root string, skipRoot bool, fn func([]Upload) error) (below, atRoot int, err error) {
	keyMarker, idMarker := "", ""
	for page := 0; page < MaxPages; page++ {
		ups, nk, ni, err := st.ListUploads(ctx, prefix, keyMarker, idMarker)
		if err != nil {
			return below, atRoot, err
		}
		ups = slices.DeleteFunc(ups, func(u Upload) bool { return !strings.HasPrefix(u.Key, root) })
		if skipRoot {
			ups = slices.DeleteFunc(ups, func(u Upload) bool { return u.Key == root })
		}
		for _, u := range ups {
			if u.Key == root {
				atRoot++
			} else {
				below++
			}
		}
		if len(ups) > 0 {
			if err := fn(ups); err != nil {
				return below, atRoot, err
			}
		}
		if nk == "" && ni == "" {
			return below, atRoot, nil
		}
		keyMarker, idMarker = nk, ni
	}
	return below, atRoot, fmt.Errorf("more than %d pages of multipart uploads: stopped", MaxPages)
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
	printKeys(out, s.Keys, s.Objects)
	fmt.Fprintf(out, "incomplete multipart uploads: %d\n", s.Uploads)
	printKeys(out, s.Upload, s.Uploads)
}

// printable is s with every character that isn't printable escaped, as %q
// escapes it: a string from the store can't write control characters to the
// terminal.
func printable(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r != utf8.RuneError && unicode.IsPrint(r) {
			b.WriteRune(r)
			continue
		}
		q := strconv.QuoteRune(r)
		b.WriteString(q[1 : len(q)-1])
	}
	return b.String()
}

// printKeys prints keys quoted, so that a key can't write control
// characters to the terminal, and how many of total it left out.
func printKeys(out io.Writer, keys []string, total int) {
	for _, k := range keys {
		fmt.Fprintf(out, "  %q\n", k)
	}
	if total > len(keys) {
		fmt.Fprintf(out, "  ... and %d more\n", total-len(keys))
	}
}
