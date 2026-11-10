package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/cmd/cacheservice/server"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
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
	// msgOf is the logged message alone, so a check on the wording can't be
	// satisfied or tripped by the error's text or the attributes.
	msgOf := func(err error) string {
		var msg string
		h := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.MessageKey {
				msg = a.Value.String()
			}
			return a
		}})
		logUncleanStop(slog.New(h), svc, err, 5*time.Second)
		return msg
	}
	engines := log(errors.New("2 engine connections still closing after they were dropped"))
	if !strings.Contains(engines, "engine connections still closing") || strings.Contains(engines, "bound passed") || strings.Contains(engines, "bound=") {
		t.Errorf("the engine-close error was logged as %q: want its text as the reason and no bound", engines)
	}
	if !strings.Contains(engines, "stopped uncleanly") {
		t.Errorf("the engine-close error was logged as %q: want \"stopped uncleanly\"", engines)
	}
	// Everything else was waited for, so the line must not say it wasn't.
	if msg := msgOf(errors.New("2 engine connections still closing after they were dropped")); strings.Contains(msg, "without waiting") {
		t.Errorf("the engine-close error was logged with message %q: it claims not to have waited", msg)
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
	if !strings.Contains(bound, "the shutdown's bound passed") || !strings.Contains(bound, "requests still running") {
		t.Errorf("the bound passing was logged as %q", bound)
	}
	if msg := msgOf(err); !strings.Contains(msg, "exiting without waiting") {
		t.Errorf("the bound passing was logged with message %q: want it to say it exits without waiting", msg)
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

// heldReads holds every index read, once armed, until its context ends, as
// a store that doesn't answer would.
type heldReads struct {
	metastore.Objects
	armed atomic.Bool
	held  chan struct{}
	once  sync.Once
}

func (o *heldReads) Get(ctx context.Context, key string) ([]byte, string, error) {
	if o.armed.Load() {
		o.once.Do(func() { close(o.held) })
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	return o.Objects.Get(ctx, key)
}

// The stop cancels an explicit export (POST /debug/engines/{conn}/export)
// blocked in an index read, and waits for it: the stop is clean and quick,
// rather than held for the grace period by the debug server's request.
// zerobull-consulting/dagger#202.
func TestShutdownCancelsExplicitExport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc := server.New(slog.New(slog.DiscardHandler), "")
	blobs := blobstore.NewMemStore("")
	blobSrv := httptest.NewServer(blobs)
	defer blobSrv.Close()
	blobs.SetBaseURL(blobSrv.URL)
	objs := &heldReads{Objects: metastore.NewMemObjects(), held: make(chan struct{})}
	meta, err := metastore.New(metastore.Config{Objects: objs})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.EnableExport(ctx, server.ExportConfig{
		Exporter: exporter.Config{Blobs: blobs, Meta: meta, Scope: "test", RetryBackoff: time.Millisecond},
		Tracker:  exporter.TrackerConfig{Settle: time.Minute, MaxDelay: time.Minute},
	}); err != nil {
		t.Fatal(err)
	}
	var servers []*http.Server
	serve := func(h http.Handler) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: h}
		servers = append(servers, srv)
		go func() { _ = srv.Serve(ln) }()
		return "http://" + ln.Addr().String()
	}
	engineURL, debugURL := serve(svc.Handler()), serve(svc.DebugHandler())
	cache := fakeengine.New("cache-a", 1)
	cache.Add(fakeengine.Entry{Number: 99, Layers: [][]byte{[]byte(strings.Repeat("explicit", 64))}})
	engineCtx, stopEngine := context.WithCancel(context.Background())
	engineDone := make(chan error, 1)
	go func() {
		engineDone <- remotecache.Run(engineCtx, fakeengine.Adapter{Cache: cache}, remotecache.Config{
			URL: engineURL, Token: "engine-token", EngineName: "e", EngineVersion: testEngineVersion,
			Logger: slog.New(slog.DiscardHandler),
		})
	}()
	defer func() {
		stopEngine()
		<-engineDone
	}()
	waitFor(t, 5*time.Second, "the engine to connect", func() bool { return len(svc.Engines()) == 1 })
	conn := svc.Engines()[0].Conn

	body, err := json.Marshal(exporter.Request{
		Roots:   []uint64{99},
		Outputs: []protocol.ExportOutput{{Number: 99, Address: fakeengine.FSPart}},
	})
	if err != nil {
		t.Fatal(err)
	}
	objs.armed.Store(true)
	answered := make(chan int, 1)
	go func() {
		resp, err := http.Post(debugURL+"/debug/engines/"+strconv.FormatUint(conn, 10)+"/export", "application/json", bytes.NewReader(body))
		if err != nil {
			answered <- 0
			return
		}
		resp.Body.Close()
		answered <- resp.StatusCode
	}()
	select {
	case <-objs.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the explicit export didn't reach its index read")
	}

	const grace = 3 * time.Second
	start := time.Now()
	err = shutdown(svc, servers, nil, cancel, grace)
	if took := time.Since(start); took >= grace/2 {
		t.Fatalf("the stop took %s: it didn't cancel the explicit export", took)
	}
	if err != nil {
		t.Fatalf("the stop with an explicit export in flight: %v", err)
	}
	if r := svc.Running(); r.Explicit != 0 {
		t.Fatalf("the stop returned with the explicit export still running: %+v", r)
	}
	select {
	case code := <-answered:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("the explicit export the stop canceled answered %d, want 503", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the explicit export didn't answer")
	}
}
