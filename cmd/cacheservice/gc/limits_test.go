package gc

import (
	"strings"
	"testing"
	"time"
)

func TestParseLimits(t *testing.T) {
	flags := Limits{MaxAge: 48 * time.Hour, MaxBytes: 1 << 30}
	f, err := ParseLimits([]byte(`
# defaults over the flags' max age; max-bytes stays the flags'
*        max-age=720h
team-a   max-age=168h max-bytes=500GiB
team-b   max-bytes=2TB
team-c   max-age=none max-bytes=0
`), flags)
	if err != nil {
		t.Fatal(err)
	}
	for ns, want := range map[string]Limits{
		"team-a":  {MaxAge: 168 * time.Hour, MaxBytes: 500 << 30},
		"team-b":  {MaxAge: 720 * time.Hour, MaxBytes: 2e12},
		"team-c":  {},
		"unknown": {MaxAge: 720 * time.Hour, MaxBytes: 1 << 30},
	} {
		if got := f.For(ns); got != want {
			t.Errorf("%s: %+v, want %+v", ns, got, want)
		}
	}
	if f.Namespaces() != 3 {
		t.Errorf("namespaces %d", f.Namespaces())
	}
	// Without a file: the flags.
	if got := NewLimits(flags).For("x"); got != flags {
		t.Errorf("no file: %+v", got)
	}
	var none *LimitsFile
	if got := none.For("x"); got != (Limits{}) {
		t.Errorf("nil: %+v", got)
	}
}

func TestParseLimitsRefuses(t *testing.T) {
	for _, tc := range []struct{ body, want string }{
		{"team max-age=1h\nteam max-bytes=1", "again"},
		{"team max-age", "key=value"},
		{"team size=1", "unknown key"},
		{"team max-age=1h max-age=2h", "twice"},
		{"team max-age=10ms", "at least 1s"},
		{"team max-age=-1h", "at least 1s"},
		{"team max-bytes=-5", "positive"},
		{"team max-bytes=1.5GiB", "positive"},
		{"team max-bytes=99999999999TiB", "too large"},
		{"../x max-age=1h", "invalid scope"},
	} {
		if _, err := ParseLimits([]byte(tc.body), Limits{}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: %v, want %q", tc.body, err, tc.want)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"1": 1, "10B": 10, "2KB": 2000, "2KiB": 2048, "3MiB": 3 << 20, "4GB": 4e9, "5TiB": 5 << 40, "none": 0, "0": 0} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("%s: %d, %v; want %d", in, got, err, want)
		}
	}
}

// A comment may end a line, as well as fill one.
func TestParseLimitsTrailingComment(t *testing.T) {
	f, err := ParseLimits([]byte("team-a max-age=1h  # CI images\n* max-bytes=1GiB #defaults\n"), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.For("team-a"); got.MaxAge != time.Hour || got.MaxBytes != 1<<30 {
		t.Fatalf("team-a: %+v", got)
	}
}
