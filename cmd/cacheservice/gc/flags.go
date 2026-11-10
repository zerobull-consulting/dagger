package gc

import (
	"flag"
	"fmt"
	"time"
)

// Flags are the limit and collector settings the service and `cacheservice gc`
// both take, under the same names, defaults and rules: RegisterFlags puts
// them on a flag set, and the methods read them, so the two can't drift.
type Flags struct {
	// NamespacesFile, MaxAge and MaxBytes are -namespaces-file, -max-age and
	// -max-bytes, as typed: Limits parses them.
	NamespacesFile string
	MaxAge         string
	MaxBytes       string
	// DryRun is -gc-dry-run, registered only for the service
	// (RegisterFlags's service).
	DryRun bool
	// Grace, StagingAge and MaxDeletes are -gc-grace, -gc-staging-age and
	// -gc-max-deletes.
	Grace      time.Duration
	StagingAge time.Duration
	MaxDeletes int
}

// RegisterFlags registers -namespaces-file, -max-age, -max-bytes, -gc-grace,
// -gc-staging-age and -gc-max-deletes on fs, and returns what they set. With
// service, it registers -gc-dry-run too, and the service's help text for
// -namespaces-file (which SIGHUP reloads); `cacheservice gc` is a one-shot
// job that takes neither: no -confirm is its dry run.
func RegisterFlags(fs *flag.FlagSet, service bool) *Flags {
	f := &Flags{}
	nsHelp := "file of per-namespace limits, one '<namespace> max-age=<duration> max-bytes=<size>' per line, '*' for the defaults; SIGHUP reloads it (empty: -max-age and -max-bytes for every namespace)"
	if !service {
		nsHelp = "file of per-namespace limits, one '<namespace> max-age=<duration> max-bytes=<size>' per line, '*' for the defaults; read once (empty: -max-age and -max-bytes for every namespace)"
	}
	fs.StringVar(&f.NamespacesFile, "namespaces-file", "", nsHelp)
	fs.StringVar(&f.MaxAge, "max-age", "0", "default max age of a namespace's roots, from when an export last recorded them, such as 720h (0: no limit)")
	fs.StringVar(&f.MaxBytes, "max-bytes", "0", "default max size of what a namespace keeps, such as 500GiB (0: no limit)")
	if service {
		fs.BoolVar(&f.DryRun, "gc-dry-run", false, "garbage collection logs what it would expire and delete, and changes nothing")
	}
	fs.DurationVar(&f.Grace, "gc-grace", DefaultGrace, "how old an unreferenced bundle, resolution or blob must be before garbage collection deletes it")
	fs.DurationVar(&f.StagingAge, "gc-staging-age", DefaultStagingAge, "how old a staged upload or incomplete multipart upload must be before garbage collection deletes it")
	fs.IntVar(&f.MaxDeletes, "gc-max-deletes", DefaultMaxDeletes, "most bundles, resolutions and blobs one garbage collection run deletes per namespace")
	return f
}

// Limits reads -max-age and -max-bytes: the limits of every namespace the
// namespaces file doesn't set.
func (f *Flags) Limits() (Limits, error) {
	age, err := ParseAge(f.MaxAge)
	if err != nil {
		return Limits{}, fmt.Errorf("-max-age: %w", err)
	}
	size, err := ParseSize(f.MaxBytes)
	if err != nil {
		return Limits{}, fmt.Errorf("-max-bytes: %w", err)
	}
	return Limits{MaxAge: age, MaxBytes: size}, nil
}

// Collector is the part of a Config the flags set: the caller adds the
// Bucket, the Meta store, the Limits and the rest.
func (f *Flags) Collector() Config {
	return Config{
		DryRun:     f.DryRun,
		Grace:      f.Grace,
		StagingAge: f.StagingAge,
		MaxDeletes: f.MaxDeletes,
	}
}
