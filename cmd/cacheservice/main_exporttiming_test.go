package main

import (
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
)

// -export-settle and -export-max-delay, and the deprecated -export-quiet and
// -export-end-grace (zerobull-consulting/dagger#134).
func TestExportTiming(t *testing.T) {
	def := func() (time.Duration, time.Duration) { return exporter.DefaultSettle, exporter.DefaultMaxDelay }
	for _, tc := range []struct {
		name                     string
		settle, maxDelay         time.Duration
		quiet, endGrace          time.Duration
		set                      []string
		wantSettle, wantMaxDelay time.Duration
		wantWarnings             []string
		wantErr                  string
	}{
		{name: "defaults", wantSettle: exporter.DefaultSettle, wantMaxDelay: exporter.DefaultMaxDelay},
		{name: "explicit", settle: 2 * time.Second, maxDelay: 30 * time.Second, set: []string{"export-settle", "export-max-delay"},
			wantSettle: 2 * time.Second, wantMaxDelay: 30 * time.Second},
		{name: "quiet holds exports back", quiet: 10 * time.Minute, set: []string{"export-quiet"},
			wantSettle: 10 * time.Minute, wantMaxDelay: 10 * time.Minute, wantWarnings: []string{"-export-quiet is deprecated"}},
		{name: "quiet shorter than the max delay", quiet: 100 * time.Millisecond, set: []string{"export-quiet"},
			wantSettle: 100 * time.Millisecond, wantMaxDelay: exporter.DefaultMaxDelay, wantWarnings: []string{"-export-quiet is deprecated"}},
		{name: "quiet with an explicit max delay", quiet: 10 * time.Minute, maxDelay: 5 * time.Second, set: []string{"export-quiet", "export-max-delay"},
			wantSettle: 10 * time.Minute, wantMaxDelay: 5 * time.Second, wantWarnings: []string{"-export-quiet is deprecated"}},
		{name: "quiet with an explicit settle", settle: 2 * time.Second, quiet: 10 * time.Minute, set: []string{"export-quiet", "export-settle"},
			wantSettle: 2 * time.Second, wantMaxDelay: exporter.DefaultMaxDelay, wantWarnings: []string{"ignored: -export-settle is set"}},
		{name: "end grace ignored", endGrace: 100 * time.Millisecond, set: []string{"export-end-grace"},
			wantSettle: exporter.DefaultSettle, wantMaxDelay: exporter.DefaultMaxDelay, wantWarnings: []string{"-export-end-grace is deprecated, ignored"}},
		{name: "zero settle", settle: 0, set: []string{"export-settle"}, wantErr: "-export-settle must be positive"},
		{name: "negative max delay", maxDelay: -time.Second, set: []string{"export-max-delay"}, wantErr: "-export-max-delay must be positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settle, maxDelay := def()
			set := map[string]bool{}
			for _, s := range tc.set {
				set[s] = true
				switch s {
				case "export-settle":
					settle = tc.settle
				case "export-max-delay":
					maxDelay = tc.maxDelay
				}
			}
			cfg, warnings, err := exportTiming(settle, maxDelay, tc.quiet, tc.endGrace, set)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("error %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Settle != tc.wantSettle || cfg.MaxDelay != tc.wantMaxDelay {
				t.Fatalf("settle %s, max delay %s; want %s, %s", cfg.Settle, cfg.MaxDelay, tc.wantSettle, tc.wantMaxDelay)
			}
			if len(warnings) != len(tc.wantWarnings) {
				t.Fatalf("warnings %q, want %q", warnings, tc.wantWarnings)
			}
			for i, w := range tc.wantWarnings {
				if !strings.Contains(warnings[i], w) {
					t.Fatalf("warning %q, want %q in it", warnings[i], w)
				}
			}
		})
	}
}
