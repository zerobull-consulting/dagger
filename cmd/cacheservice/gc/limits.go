package gc

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// Limits are a namespace's retention limits. Zero is no limit.
type Limits struct {
	// MaxAge expires a root this long after an export last wrote its
	// record (metastore.Root.RecordedAtUnix), and with it the entries only
	// it keeps.
	MaxAge time.Duration
	// MaxBytes bounds what the namespace keeps: its index, and the
	// bundles, resolutions and blobs its live entries name. Over it, GC
	// expires the oldest roots first until it is under.
	MaxBytes int64
}

// LimitsFile is the namespaces file: per-namespace limits, one namespace per
// line, as `<namespace> key=value ...`. The keys are max-age (a Go
// duration, such as 720h) and max-bytes (a size, such as 500GiB); 0 or none
// is no limit. A `*` line sets the defaults for namespaces with no line of
// their own, over the defaults the file was parsed with (the -max-age and
// -max-bytes flags); a namespace's line sets only the keys it names, and
// takes the others from the defaults. Blank lines are ignored, and a #
// that starts a field begins a comment, to the end of its line:
//
//	# the defaults, then two namespaces
//	*        max-age=720h
//	team-a   max-age=168h max-bytes=500GiB   # CI images
//	team-b   max-bytes=2TiB
//
// A namespace that is in no line, and one with no tokens, is collected with
// the defaults: the file never decides that a namespace's data goes.
type LimitsFile struct {
	defaults Limits
	byNS     map[string]Limits
}

// NewLimits returns limits with only defaults.
func NewLimits(defaults Limits) *LimitsFile {
	return &LimitsFile{defaults: defaults, byNS: map[string]Limits{}}
}

// LoadLimitsFile reads a namespaces file.
func LoadLimitsFile(path string, defaults Limits) (*LimitsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("namespaces file: %w", err)
	}
	return ParseLimits(data, defaults)
}

// ParseLimits reads the content of a namespaces file.
func ParseLimits(data []byte, defaults Limits) (*LimitsFile, error) {
	if err := defaults.validate(); err != nil {
		return nil, fmt.Errorf("namespaces file: defaults: %w", err)
	}
	type line struct {
		n    int
		ns   string
		keys map[string]string
	}
	var lines []line
	seen := map[string]int{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	for n := 1; sc.Scan(); n++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		// A comment may end a line too: from a field that starts with #.
		for i, f := range fields {
			if strings.HasPrefix(f, "#") {
				fields = fields[:i]
				break
			}
		}
		ns := fields[0]
		if ns != "*" {
			if err := metastore.ValidateScope(ns); err != nil {
				return nil, fmt.Errorf("namespaces file: line %d: %w", n, err)
			}
		}
		if prev, ok := seen[ns]; ok {
			return nil, fmt.Errorf("namespaces file: line %d: %s again (line %d)", n, ns, prev)
		}
		seen[ns] = n
		keys := map[string]string{}
		for _, f := range fields[1:] {
			k, v, ok := strings.Cut(f, "=")
			if !ok || v == "" {
				return nil, fmt.Errorf("namespaces file: line %d: %q: want key=value", n, f)
			}
			if k != "max-age" && k != "max-bytes" {
				return nil, fmt.Errorf("namespaces file: line %d: unknown key %q (want max-age or max-bytes)", n, k)
			}
			if _, dup := keys[k]; dup {
				return nil, fmt.Errorf("namespaces file: line %d: %s twice", n, k)
			}
			keys[k] = v
		}
		lines = append(lines, line{n: n, ns: ns, keys: keys})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("namespaces file: %w", err)
	}
	apply := func(l Limits, keys map[string]string, n int) (Limits, error) {
		if v, ok := keys["max-age"]; ok {
			d, err := ParseAge(v)
			if err != nil {
				return l, fmt.Errorf("namespaces file: line %d: max-age: %w", n, err)
			}
			l.MaxAge = d
		}
		if v, ok := keys["max-bytes"]; ok {
			b, err := ParseSize(v)
			if err != nil {
				return l, fmt.Errorf("namespaces file: line %d: max-bytes: %w", n, err)
			}
			l.MaxBytes = b
		}
		return l, nil
	}
	f := &LimitsFile{defaults: defaults, byNS: map[string]Limits{}}
	for _, l := range lines {
		if l.ns == "*" {
			d, err := apply(defaults, l.keys, l.n)
			if err != nil {
				return nil, err
			}
			f.defaults = d
		}
	}
	for _, l := range lines {
		if l.ns == "*" {
			continue
		}
		lim, err := apply(f.defaults, l.keys, l.n)
		if err != nil {
			return nil, err
		}
		f.byNS[l.ns] = lim
	}
	return f, nil
}

// For returns a namespace's limits: its line's, or the defaults.
func (f *LimitsFile) For(ns string) Limits {
	if f == nil {
		return Limits{}
	}
	if l, ok := f.byNS[ns]; ok {
		return l
	}
	return f.defaults
}

// Defaults returns the limits of a namespace with no line.
func (f *LimitsFile) Defaults() Limits {
	if f == nil {
		return Limits{}
	}
	return f.defaults
}

// Namespaces returns how many namespaces have a line.
func (f *LimitsFile) Namespaces() int {
	if f == nil {
		return 0
	}
	return len(f.byNS)
}

func (l Limits) validate() error {
	if l.MaxAge < 0 {
		return fmt.Errorf("max age %s is negative", l.MaxAge)
	}
	if l.MaxAge > 0 && l.MaxAge < time.Second {
		return fmt.Errorf("max age %s is under a second", l.MaxAge)
	}
	if l.MaxBytes < 0 {
		return fmt.Errorf("max bytes %d is negative", l.MaxBytes)
	}
	return nil
}

// ParseAge parses a max age: a Go duration of at least a second, or 0 or
// none for no limit.
func ParseAge(s string) (time.Duration, error) {
	if s == "0" || s == "none" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	if d < time.Second {
		return 0, fmt.Errorf("%s: want at least 1s, or 0 for no limit", s)
	}
	return d, nil
}

// sizeUnits are the suffixes ParseSize takes, longest first.
var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3},
	{"B", 1},
}

// ParseSize parses a size in bytes: a whole number with an optional unit
// (B, KB, MB, GB, TB in powers of 1000; KiB, MiB, GiB, TiB in powers of
// 1024), or 0 or none for no limit.
func ParseSize(s string) (int64, error) {
	if s == "0" || s == "none" {
		return 0, nil
	}
	num, mult := s, int64(1)
	for _, u := range sizeUnits {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			num, mult = n, u.mult
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q: want a positive whole number of bytes, such as 500GiB", s)
	}
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("%q: too large", s)
	}
	return n * mult, nil
}
