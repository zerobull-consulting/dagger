package exporter

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// An export that stores roots logs which, at debug level: the e2e reads when
// each value was first stored (zerobull-consulting/dagger#154).
func TestExportLogsStoredRoots(t *testing.T) {
	e := newTestEnv(t, nil)
	var buf bytes.Buffer
	x, err := New(Config{Blobs: e.blobs, Meta: e.meta, Scope: "team", Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		t.Fatal(err)
	}
	e.addBuild()
	res, err := x.Export(context.Background(), e.eng, Request{Roots: []uint64{10, 11}, Outputs: fsOutputs(10, 11)})
	if err != nil || len(res.Stored) != 2 {
		t.Fatalf("export: %+v, %v", res, err)
	}
	if out := buf.String(); !strings.Contains(out, `msg="export try roots"`) || !strings.Contains(out, `msg="export stored roots"`) || !strings.Contains(out, "numbers=10,11") || !strings.Contains(out, "generation=") || !strings.Contains(out, "omitted=0") {
		t.Errorf("log lacks the stored roots:\n%s", out)
	}
}

func TestLogStoredRootsBounded(t *testing.T) {
	var buf bytes.Buffer
	x := &Exporter{log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	roots := make([]uint64, 3*maxLoggedRoots)
	for i := range roots {
		roots[i] = uint64(i + 1)
	}
	x.logRoots("export stored roots", "c", 4, roots)
	if n := strings.Count(buf.String(), ",") + 1; n != maxLoggedRoots {
		t.Errorf("%d roots logged, want %d", n, maxLoggedRoots)
	}
	if out := buf.String(); !strings.Contains(out, "omitted=2000") || !strings.Contains(out, "generation=4") {
		t.Errorf("the record doesn't say what it omitted, or the generation:\n%.200s", out)
	}
}

// With debug logging off, nothing is logged and nothing is built for the
// record.
func TestLogStoredRootsSkippedWhenDebugOff(t *testing.T) {
	var buf bytes.Buffer
	x := &Exporter{log: slog.New(slog.NewTextHandler(&buf, nil))}
	roots := make([]uint64, maxLoggedRoots)
	for i := range roots {
		roots[i] = uint64(i + 1)
	}
	if n := testing.AllocsPerRun(10, func() { x.logRoots("export stored roots", "c", 1, roots) }); n != 0 {
		t.Errorf("%v allocations with debug logging off, want 0", n)
	}
	if buf.Len() != 0 {
		t.Errorf("logged at info level: %.100s", &buf)
	}
}
