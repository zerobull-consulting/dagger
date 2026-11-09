package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// heldBucket holds GC's first listing until its context ends, as a long GC
// run would.
type heldBucket struct {
	gc.Bucket
	held chan struct{}
	once sync.Once
}

func (b *heldBucket) ListLevel(ctx context.Context, prefix string) ([]string, []gc.Object, error) {
	b.once.Do(func() { close(b.held) })
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

// The stop ends the service's paths before it waits for the HTTP servers'
// requests, as when the signal canceled their context: a POST /debug/gc run
// under way ends at once, rather than holding the stop for the grace period.
// That is the stop with -drain-timeout 0, and after a drain.
func TestShutdownCancelsBeforeWaitingForRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc := server.New(slog.New(slog.DiscardHandler), "")
	objs := metastore.NewMemObjects()
	meta, err := metastore.New(metastore.Config{Objects: objs})
	if err != nil {
		t.Fatal(err)
	}
	// GC runs only under the ownership marker.
	if _, _, err := ownership.Check(ctx, objs, "", true, time.Now()); err != nil {
		t.Fatal(err)
	}
	bucket := &heldBucket{Bucket: &gc.MemBucket{Blobs: blobstore.NewMemStore(""), Objects: objs}, held: make(chan struct{})}
	if err := svc.EnableGC(ctx, server.GCConfig{Collector: gc.Config{Bucket: bucket, Meta: meta}}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: svc.DebugHandler()}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}()
	go func() {
		resp, err := http.Post("http://"+ln.Addr().String()+"/debug/gc", "", nil)
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-bucket.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the GC run didn't start")
	}

	const grace = 3 * time.Second
	start := time.Now()
	shutdown(svc, []*http.Server{srv}, nil, cancel, grace)
	if took := time.Since(start); took >= grace/2 {
		t.Fatalf("the stop took %s: it waited for the GC request before ending the service's paths", took)
	}
}

// A request still running when the shutdown's bound passes makes the stop
// unclean: shutdown reports it, so the process exits 1, not 0. The service
// itself has nothing left to wait for here, so only the HTTP server's
// shutdown can tell.
func TestShutdownReportsRequestsStillRunning(t *testing.T) {
	svc := server.New(slog.New(slog.DiscardHandler), "")
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	mux := http.NewServeMux()
	// Ignores its request's context, as a handler stuck on a store would.
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String() + "/slow")
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the request didn't reach its handler")
	}
	const grace = 300 * time.Millisecond
	err = shutdown(svc, []*http.Server{srv}, nil, func() {}, grace)
	if err == nil || !strings.Contains(err.Error(), "requests still running") {
		t.Fatalf("shutdown with a request still running reported %v", err)
	}
	if n := strings.Count(err.Error(), errShutdownGrace.Error()); n != 1 {
		t.Fatalf("the bound is named %d times in %q, want once", n, err)
	}
}

// A server that failed during the drain or the shutdown, after the first
// signal, makes the stop unclean: its error, left in errs, gives exit 1
// even when the stop itself ended within its bound.
func TestExitCodeTakesServerErrorsDuringTheStop(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	errs := make(chan error, 2)
	if code := exitCode(true, errs, logger); code != 0 {
		t.Fatalf("a clean stop with no server error: exit %d, want 0", code)
	}
	if code := exitCode(false, errs, logger); code != 1 {
		t.Fatalf("an unclean stop: exit %d, want 1", code)
	}
	errs <- errors.New("accept: too many open files")
	if code := exitCode(true, errs, logger); code != 1 {
		t.Fatalf("a clean stop after a server failed during it: exit %d, want 1", code)
	}
}

// A server whose Serve fails as the stop ends is counted: shutdown waits for
// the servers' goroutines (within its bound) before exitCode reads errs, so
// the error a goroutine sends after srv.Shutdown returned is in errs, and the
// stop exits 1. Without the wait, exitCode read errs first and exited 0
// (https://github.com/zerobull-consulting/dagger/issues/80).
func TestShutdownWaitsForServerErrorsBeforeTheExitCode(t *testing.T) {
	svc := server.New(slog.New(slog.DiscardHandler), "")
	errs := make(chan error, 1)
	var serving sync.WaitGroup
	serving.Add(1)
	go func() {
		defer serving.Done()
		// Fails a moment after the shutdown began, as an accept error
		// racing the stop's end.
		time.Sleep(200 * time.Millisecond)
		errs <- errors.New("accept: too many open files")
	}()
	done := make(chan struct{})
	go func() { serving.Wait(); close(done) }()
	if err := shutdown(svc, nil, done, func() {}, 5*time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if code := exitCode(true, errs, slog.New(slog.DiscardHandler)); code != 1 {
		t.Fatalf("exit %d: the late server error was missed, want 1", code)
	}
}

// A server goroutine still running at the shutdown's bound makes the stop
// unclean, and the bound is not exceeded waiting for it.
func TestShutdownReportsServersStillServing(t *testing.T) {
	svc := server.New(slog.New(slog.DiscardHandler), "")
	never := make(chan struct{})
	const grace = 300 * time.Millisecond
	start := time.Now()
	err := shutdown(svc, nil, never, func() {}, grace)
	if err == nil || !strings.Contains(err.Error(), "servers still serving") || !errors.Is(err, errShutdownGrace) {
		t.Fatalf("shutdown with a server still serving reported %v", err)
	}
	if took := time.Since(start); took > grace+2*time.Second {
		t.Fatalf("shutdown took %s, past its bound %s", took, grace)
	}
}

// The log of an unclean stop names its cause: the bound only when it passed,
// not for an error that comes well before it, such as the engine connections
// still closing after the close bound (https://github.com/zerobull-consulting/dagger/issues/81).
func TestLogUncleanStopNamesTheCause(t *testing.T) {
	svc := server.New(slog.New(slog.DiscardHandler), "")
	log := func(err error) string {
		var buf strings.Builder
		logUncleanStop(slog.New(slog.NewTextHandler(&buf, nil)), svc, err, 5*time.Second)
		return buf.String()
	}
	engines := log(errors.New("2 engine connections still closing after they were dropped"))
	if !strings.Contains(engines, "engine connections still closing") || strings.Contains(engines, "bound passed") || strings.Contains(engines, "bound=") {
		t.Errorf("the engine-close error was logged as %q: want its text as the reason and no bound", engines)
	}
	if !strings.Contains(engines, "stopped uncleanly") {
		t.Errorf("the engine-close error was logged as %q: want \"stopped uncleanly\"", engines)
	}
	// Everything else was waited for, so the line must not say it wasn't.
	if strings.Contains(engines, "without waiting") {
		t.Errorf("the engine-close error was logged as %q: it claims not to have waited", engines)
	}
	// The bound passing, from a real shutdown with a request held past it.
	srv := &http.Server{Handler: http.NotFoundHandler()}
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) { close(entered); <-release })
	srv.Handler = mux
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	go func() {
		if resp, err := http.Get("http://" + ln.Addr().String() + "/slow"); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the request didn't reach its handler")
	}
	err = shutdown(svc, []*http.Server{srv}, nil, func() {}, 300*time.Millisecond)
	if err == nil {
		t.Fatal("shutdown with a request held past the bound reported nothing")
	}
	bound := log(err)
	if !strings.Contains(bound, "the shutdown's bound passed") || !strings.Contains(bound, "requests still running") || !strings.Contains(bound, "without waiting") {
		t.Errorf("the bound passing was logged as %q", bound)
	}
}

// Servers that had returned by the time the bound passed are not reported as
// still serving: with both the bound and serversDone ready, the select must
// not pick the bound at random.
func TestShutdownDoesNotBlameServersThatReturned(t *testing.T) {
	svc := server.New(slog.New(slog.DiscardHandler), "")
	done := make(chan struct{})
	close(done)
	for i := 0; i < 200; i++ {
		err := shutdown(svc, nil, done, func() {}, time.Nanosecond)
		if err != nil && strings.Contains(err.Error(), "servers still serving") {
			t.Fatalf("run %d: servers that had returned were reported as still serving: %v", i, err)
		}
	}
}
