package main

// The binary's stop bound and exit codes (zerobull-consulting/dagger#77
// item 5): the whole stop, from the first signal through the drain to exit,
// takes at most -drain-timeout plus shutdownGrace; a signal during the
// shutdown exits at once; and the exit code says whether the stop was clean.
// Like main_test.go, these run the built command against MinIO, and skip
// unless CACHESERVICE_S3_ENDPOINT is set.

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// waitExitCode waits for the process to exit, fails the test unless it did
// within timeout, and returns its exit code and how long the wait took.
func (b *binaryTest) waitExitCode(timeout time.Duration) (int, time.Duration) {
	b.t.Helper()
	start := time.Now()
	select {
	case <-b.exited:
	case <-time.After(timeout):
		b.t.Fatalf("the service didn't exit within %s", timeout)
	}
	took := time.Since(start)
	if b.err == nil {
		return 0, took
	}
	var exit *exec.ExitError
	if !errors.As(b.err, &exit) {
		b.t.Fatalf("the service ended with %v", b.err)
	}
	return exit.ExitCode(), took
}

// held runs the binary with the test hook that makes its stop wait for work
// that ignores cancellation and never ends, as GC's finish update or a
// promotion's cleanup would against a store that doesn't answer.
func held() binaryOpts {
	return binaryOpts{env: []string{stopTestHoldEnv + "=1"}}
}

func (b *binaryTest) logHas(t *testing.T, wants ...string) {
	t.Helper()
	logs := b.logs.String()
	for _, want := range wants {
		if !strings.Contains(logs, want) {
			t.Errorf("the log has no %q", want)
		}
	}
}

// A signal during the shutdown exits at once, with status 1, and logs what
// was still running: the shutdown no longer has to run out its bound.
func TestMinIOBinarySignalDuringShutdownExitsAtOnce(t *testing.T) {
	b := startBinaryWith(t, held(), "-drain-timeout", "0")
	b.signal()
	// The engine's connection closed: the shutdown is past CloseEngines,
	// and waits for the held work.
	b.waitLog(10*time.Second, `msg="engine disconnected"`)
	b.signal()
	code, took := b.waitExitCode(shutdownGrace)
	if code != 1 {
		t.Fatalf("exit %d after a signal during the shutdown, want 1", code)
	}
	if took > shutdownGrace/2 {
		t.Fatalf("the service took %s to exit after a signal during the shutdown", took)
	}
	b.logHas(t, `msg="signal during the shutdown: exiting at once" signal=terminated`, "held=1")
}

// Work that ignores cancellation can't hold the stop past its bound: the
// drain runs to its deadline, the shutdown waits shutdownGrace for the held
// work, and the process exits. A stop cut off by its bound exits 1, not 0,
// though nothing failed as such: it is how a process manager, and these
// tests, tell a stop that left work running from a clean one.
func TestMinIOBinaryStopBoundedExitsOne(t *testing.T) {
	const drainTimeout = time.Second
	b := startBinaryWith(t, held(), "-drain-timeout", drainTimeout.String())
	b.build()
	release := b.cache.Hold()
	defer release()
	b.postSpans("session-1", true)
	b.waitFor(10*time.Second, "the export to be in flight", func() bool { return b.cache.InFlight() == 1 })
	start := time.Now()
	b.signal()
	code, _ := b.waitExitCode(drainTimeout + shutdownGrace + 10*time.Second)
	took := time.Since(start)
	if code != 1 {
		t.Fatalf("exit %d after the stop's bound cut it off, want 1", code)
	}
	// The documented bound: -drain-timeout plus shutdownGrace.
	if bound := drainTimeout + shutdownGrace; took > bound {
		t.Fatalf("the stop took %s, past -drain-timeout plus the shutdown's bound (%s)", took, bound)
	}
	if took < drainTimeout+shutdownGrace/2 {
		t.Fatalf("the stop took %s: it didn't wait for the held work", took)
	}
	b.logHas(t, `msg="drain: done" outcome=deadline`, "the shutdown's bound passed with work still running", "held=1")
}

// A clean stop exits 0, within the bound, and logs that it stopped.
func TestMinIOBinaryCleanStopExitsZero(t *testing.T) {
	b := startBinary(t, "-drain-timeout", "0")
	b.signal()
	code, took := b.waitExitCode(2 * shutdownGrace)
	if code != 0 {
		t.Fatalf("exit %d after a clean stop, want 0", code)
	}
	if took > shutdownGrace {
		t.Fatalf("the clean stop took %s, past the shutdown's bound %s", took, shutdownGrace)
	}
	b.logHas(t, `msg=stopped took=`)
	if strings.Contains(b.logs.String(), "level=ERROR") {
		t.Errorf("a clean stop logged an error")
	}
}
