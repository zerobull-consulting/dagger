package main

import (
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// -gc-mode and -gc-interval: the mode each flag combination gives, and the
// ones that are refused, with the flags the message names.
func TestResolveGCMode(t *testing.T) {
	const def = "service"
	for _, tc := range []struct {
		name     string
		mode     string
		interval time.Duration
		noExport bool
		set      []string
		want     server.GCMode
		wantErr  []string
		wantNot  []string
	}{
		{name: "defaults", mode: def, interval: gc.DefaultInterval, want: server.GCModeService},
		{name: "service, explicit", mode: "service", interval: gc.DefaultInterval, set: []string{"gc-mode"}, want: server.GCModeService},
		{name: "service, explicit interval", mode: def, interval: time.Minute, set: []string{"gc-interval"}, want: server.GCModeService},
		{name: "batch", mode: "batch", interval: gc.DefaultInterval, set: []string{"gc-mode"}, want: server.GCModeBatch},
		{name: "off", mode: "off", interval: gc.DefaultInterval, set: []string{"gc-mode"}, want: server.GCModeOff},

		{name: "interval 0", mode: def, set: []string{"gc-interval"}, wantErr: []string{"-gc-interval=0", "-gc-mode=batch", "-gc-mode=off"}},
		{name: "read-only, interval 0", mode: def, noExport: true, set: []string{"gc-interval"}, wantErr: []string{"-gc-interval=0", "-export=false", "drop -gc-interval", "-gc-mode=off"}, wantNot: []string{"-gc-mode=batch"}},
		{name: "interval 0 with batch", mode: "batch", set: []string{"gc-mode", "gc-interval"}, wantErr: []string{"-gc-interval=0", "-gc-mode"}},
		{name: "negative interval", mode: def, interval: -time.Second, set: []string{"gc-interval"}, wantErr: []string{"-gc-interval=-1s", "negative"}},
		{name: "interval with batch", mode: "batch", interval: time.Hour, set: []string{"gc-mode", "gc-interval"}, wantErr: []string{"-gc-interval=1h0m0s", "-gc-mode=batch"}},
		{name: "interval with off", mode: "off", interval: time.Hour, set: []string{"gc-mode", "gc-interval"}, wantErr: []string{"-gc-interval=1h0m0s", "-gc-mode=off"}},
		{name: "unknown mode", mode: "daily", interval: gc.DefaultInterval, set: []string{"gc-mode"}, wantErr: []string{`-gc-mode="daily"`, "service", "batch", "off"}},
		{name: "empty mode", mode: "", interval: gc.DefaultInterval, set: []string{"gc-mode"}, wantErr: []string{`-gc-mode=""`}},

		{name: "read-only is off", mode: def, interval: gc.DefaultInterval, noExport: true, want: server.GCModeOff},
		{name: "read-only, explicit off", mode: "off", interval: gc.DefaultInterval, noExport: true, set: []string{"gc-mode"}, want: server.GCModeOff},
		{name: "read-only, explicit service", mode: "service", interval: gc.DefaultInterval, noExport: true, set: []string{"gc-mode"}, wantErr: []string{"-gc-mode=service", "-export=false"}},
		{name: "read-only, explicit batch", mode: "batch", interval: gc.DefaultInterval, noExport: true, set: []string{"gc-mode"}, wantErr: []string{"-gc-mode=batch", "-export=false"}},
		{name: "read-only, explicit interval", mode: def, interval: time.Hour, noExport: true, set: []string{"gc-interval"}, wantErr: []string{"-gc-interval=1h0m0s", "-gc-mode=off", "-export=false"}},
		{name: "read-only, off and interval", mode: "off", interval: time.Hour, noExport: true, set: []string{"gc-mode", "gc-interval"}, wantErr: []string{"-gc-interval=1h0m0s", "-gc-mode=off"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := map[string]bool{}
			for _, f := range tc.set {
				set[f] = true
			}
			got, err := resolveGCMode(tc.mode, tc.interval, !tc.noExport, set)
			if len(tc.wantErr) == 0 {
				if err != nil || got != tc.want {
					t.Fatalf("got %q, %v; want %q", got, err, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("taken as %q", got)
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q doesn't name %s", err, want)
				}
			}
			for _, not := range tc.wantNot {
				if strings.Contains(err.Error(), not) {
					t.Errorf("error %q names %s", err, not)
				}
			}
		})
	}
}
