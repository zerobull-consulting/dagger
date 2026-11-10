// Package gccmd is `cacheservice gc`: it runs one garbage collection over
// every namespace under the prefix, and exits. It is what collects a bucket
// whose writers run with -gc-mode=batch, from a Kubernetes CronJob, a systemd
// timer or a CI schedule (README.md, "The gc command").
//
// It lives apart from package gc, the collector, which it runs (gc.New, then
// Collector.Run once), as `wipe` and `lifecycle` do, and which can't import
// lifecycle's ConfirmValue. The flags that set the limits and the collector
// are package gc's (gc.RegisterFlags), the service's own.
//
// Without -confirm it is a dry run: it reports what a run would expire and
// delete, and changes nothing. With -confirm=<bucket>/<prefix> it collects,
// as POST /debug/gc does, but only with limits given explicitly
// (-namespaces-file, or both -max-age and -max-bytes; 0 is no limit): a job
// that deletes shouldn't do so under limits nobody chose.
//
// The report, a JSON array of gc.Report, goes to stdout and nothing else
// does; logs and errors go to stderr.
package gccmd

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/lifecycle"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// Exit codes.
const (
	// ExitOK: a clean run, or a dry run that ran cleanly.
	ExitOK = 0
	// ExitError: a namespace reported errors, the run failed, the stores
	// couldn't be opened, or a signal ended the run. The report so far is
	// still printed.
	ExitError = 1
	// ExitUsage: bad flags, a -confirm that is wrong, or a confirmed run
	// without explicit limits; nothing was read or deleted.
	ExitUsage = 2
	// ExitNotOwned: the run refused to do anything (gc.ErrNotOwned): no
	// ownership marker, another layout, or keys of no known shape at the
	// root.
	ExitNotOwned = 3
)

// Stores are what a run collects: the bucket it lists and deletes in, and the
// metadata store of the same bucket and prefix.
type Stores struct {
	Bucket gc.Bucket
	Meta   *metastore.Store
}

// Opener makes the Stores for an S3 configuration.
type Opener func(ctx context.Context, cfg blobstore.S3Config) (Stores, error)

// OpenS3 is the Opener of the real command: the service's own S3 client, one
// for the bucket and the metadata, as the service shares it (server.EnableS3).
func OpenS3(ctx context.Context, cfg blobstore.S3Config) (Stores, error) {
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		return Stores{}, err
	}
	meta, err := metastore.NewS3WithClient(ctx, cfg, client)
	if err != nil {
		return Stores{}, err
	}
	// A custom endpoint is an S3-compatible store, which may list multipart
	// uploads only by exact key (MinIO); AWS S3 lists them by prefix.
	return Stores{
		Bucket: &gc.S3Bucket{Client: client, Bucket: cfg.Bucket, FilterAllUploads: cfg.Endpoint != ""},
		Meta:   meta,
	}, nil
}

const usage = `usage: cacheservice gc [-confirm=<bucket>/<prefix>] [flags]

Runs one garbage collection over every namespace under the prefix
CACHESERVICE_S3_PREFIX of the bucket CACHESERVICE_S3_BUCKET, and exits. By
default a dry run: it reports what it would expire and delete, and changes
nothing. With -confirm repeating the bucket and the prefix, it collects, as
POST /debug/gc does; a confirmed run needs -namespaces-file, or both -max-age
and -max-bytes (0 is no limit). The report, a JSON array, goes to stdout; logs
go to stderr.

On SIGTERM or an interrupt the run ends, and finishes the claims it made
(within a minute) so none is left in progress.

Exit codes: 0 ok (a clean run or dry run), 1 a namespace reported errors or
the run failed or was interrupted, 2 bad flags or -confirm, 3 the run refused
to do anything: the prefix's ownership can't be confirmed (no marker, another
layout, keys of no known shape).

flags:
`

// Main runs the command with args (after "gc"), and returns its exit code.
// It ends the run on SIGTERM or SIGINT, and never prints the credentials.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer, open Opener) int {
	fs := flag.NewFlagSet("cacheservice gc", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}
	fl := gc.RegisterFlags(fs, false)
	var confirm string
	fs.StringVar(&confirm, "confirm", "", "the bucket and prefix it collects, as <bucket>/<prefix>; without it, a dry run")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	fail := func(code int, format string, a ...any) int {
		fmt.Fprintf(stderr, "cacheservice gc: %s\n", printable(fmt.Sprintf(format, a...)))
		return code
	}
	if fs.NArg() > 0 {
		return fail(ExitUsage, "unexpected arguments %q", fs.Args())
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		return fail(ExitUsage, "%v", err)
	}
	if cfg.Bucket == "" {
		return fail(ExitUsage, "%s is not set", blobstore.EnvBucket)
	}
	want := lifecycle.ConfirmValue(cfg.Bucket, cfg.Prefix)
	if confirm != "" && confirm != want {
		return fail(ExitUsage, "-confirm=%s doesn't match the bucket and prefix it would collect, %s; nothing read or deleted", confirm, want)
	}
	confirmed := confirm != ""
	limitsGiven := fl.NamespacesFile != "" || (set["max-age"] && set["max-bytes"])
	if confirmed && !limitsGiven {
		return fail(ExitUsage, "-confirm without explicit limits: a confirmed run deletes, and mustn't do so under limits nobody chose; give -namespaces-file, or both -max-age and -max-bytes (0 is no limit)%s; nothing read or deleted", missingLimits(set))
	}
	defaults, err := fl.Limits()
	if err != nil {
		return fail(ExitUsage, "%v", err)
	}
	var limits *gc.LimitsFile
	if fl.NamespacesFile != "" {
		limits, err = gc.LoadLimitsFile(fl.NamespacesFile, defaults)
	} else {
		limits, err = gc.ParseLimits(nil, defaults)
	}
	if err != nil {
		return fail(ExitUsage, "%v", err)
	}
	c := fl.Collector()
	if c.Grace < 0 || c.StagingAge < 0 || c.MaxDeletes < 0 {
		return fail(ExitUsage, "-gc-grace, -gc-staging-age and -gc-max-deletes must not be negative")
	}

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	if !confirmed {
		logger.Info("dry run: nothing will be changed; to collect, run again with -confirm=" + want)
		if !limitsGiven {
			logger.Warn("dry run without limits: no -namespaces-file, and -max-age and -max-bytes not both given, so no max age or max size is applied (only each entry's own expiry); a confirmed run needs them" + missingLimits(set))
		}
	}

	// The first SIGTERM or interrupt ends the run's context; the collector
	// still finishes the claims it made (gc: finishTimeout). A second is
	// caught too, so the finish isn't cut off; the process manager's own
	// kill is the last resort.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := open(ctx, cfg)
	if err != nil {
		return fail(ExitError, "%v", err)
	}
	c.Bucket, c.Meta = st.Bucket, st.Meta
	c.Limits = limits.For
	c.Logger = logger.With("component", "gc")
	c.DryRun = !confirmed
	collector, err := gc.New(c)
	if err != nil {
		return fail(ExitUsage, "%v", err)
	}
	logger.Info("collecting", "bucket", cfg.Bucket, "prefix", cfg.Prefix, "dryRun", !confirmed)
	reports, runErr := collector.Run(ctx)
	if reports == nil {
		reports = []gc.Report{} // [], not null
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(reports); err != nil {
		return fail(ExitError, "write the report: %v", err)
	}

	switch {
	case ctx.Err() != nil:
		return fail(ExitError, "interrupted: %v", context.Cause(ctx))
	case errors.Is(runErr, gc.ErrNotOwned):
		return fail(ExitNotOwned, "%v", runErr)
	case runErr != nil:
		return fail(ExitError, "%v", runErr)
	}
	// Run joins each namespace's errors into its error; a report with some
	// and no error would be a bug, but the exit code doesn't rest on it.
	for _, r := range reports {
		if len(r.Errors) > 0 {
			return fail(ExitError, "namespace %s: %s", r.Namespace, strings.Join(r.Errors, "; "))
		}
	}
	return ExitOK
}

// missingLimits names the limit flags a command line left out, for a message
// about explicit limits: "" when neither -max-age nor -max-bytes is missing.
func missingLimits(set map[string]bool) string {
	var missing []string
	for _, f := range []string{"-max-age", "-max-bytes"} {
		if !set[f[1:]] {
			missing = append(missing, f)
		}
	}
	if len(missing) == 0 {
		return ""
	}
	return " (not given: " + strings.Join(missing, ", ") + ")"
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
