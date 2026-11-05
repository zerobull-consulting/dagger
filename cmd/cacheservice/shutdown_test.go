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
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// heldBucket holds GC's first listing until its context ends, as a long GC
// run would.
type heldBucket struct {
	gc.Bucket
	held chan struct{}
	once sync.Once
}

func (b *heldBucket) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	b.once.Do(func() { close(b.held) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// The stop ends the service's paths before it waits for the HTTP servers'
// requests, as when the signal canceled their context: a POST /debug/gc run
// under way ends at once, rather than holding the stop for the grace period.
// That is the stop with -drain-timeout 0, and after a drain.
func TestShutdownCancelsBeforeWaitingForRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc := server.New(slog.New(slog.DiscardHandler), "")
	meta, err := metastore.New(metastore.Config{Objects: metastore.NewMemObjects()})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &heldBucket{Bucket: &gc.MemBucket{Blobs: blobstore.NewMemStore(""), Objects: metastore.NewMemObjects()}, held: make(chan struct{})}
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
	shutdown(svc, []*http.Server{srv}, cancel, grace)
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
	err = shutdown(svc, []*http.Server{srv}, func() {}, grace)
	if err == nil || !strings.Contains(err.Error(), "requests still running") {
		t.Fatalf("shutdown with a request still running reported %v", err)
	}
}
