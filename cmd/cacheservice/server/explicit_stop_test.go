package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// stopHoldReads holds the first index read made with a context marked by
// holdKey until that context ends, as a store read does, and then holds its
// unwinding until released: Wait must wait for it.
type stopHoldReads struct {
	metastore.Objects
	held, unwinding, released chan struct{}
	once                      sync.Once
}

func newStopHoldReads() *stopHoldReads {
	return &stopHoldReads{Objects: metastore.NewMemObjects(), held: make(chan struct{}), unwinding: make(chan struct{}), released: make(chan struct{})}
}

func (o *stopHoldReads) Get(ctx context.Context, key string) ([]byte, string, error) {
	if ctx.Value(holdKey{}) != nil {
		first := false
		o.once.Do(func() { first = true })
		if first {
			close(o.held)
			<-ctx.Done()
			close(o.unwinding)
			<-o.released
			return nil, "", ctx.Err()
		}
	}
	return o.Objects.Get(ctx, key)
}

// newStopHoldHarness is an export harness whose index reads hold reads
// marked with holdKey; release is called at cleanup too.
func newStopHoldHarness(t *testing.T) (h *exportHarness, hold *stopHoldReads, release func()) {
	t.Helper()
	blobs, _ := newMemStores(t)
	hold = newStopHoldReads()
	meta, err := metastore.New(metastore.Config{Objects: hold})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { close(hold.released) }) }
	h = newExportHarness(t, blobs, meta, nil)
	// Registered after the harness's cleanup, so it runs before it: the
	// harness's Wait needs the hold released.
	t.Cleanup(release)
	return h, hold, release
}

// An explicit export or import (POST /debug/engines/{conn}/export, or
// .../import) blocked in a store read when the stop starts is canceled by
// the paths' context ending, with ErrStopping (503 on the debug server),
// and Wait returns only once it ended. One asked for once Wait began is
// refused, 503, even before the paths' context ended.
// zerobull-consulting/dagger#202.
func TestStopCancelsAndWaitsForExplicitHooks(t *testing.T) {
	for _, hook := range []string{"export", "import"} {
		t.Run(hook, func(t *testing.T) {
			h, hold, release := newStopHoldHarness(t)
			ran := make(chan error, 1)
			ctx := context.WithValue(context.Background(), holdKey{}, true)
			go func() {
				var err error
				if hook == "export" {
					h.cache.Add(fakeengine.Entry{Number: 99, Layers: [][]byte{testLayer("explicit")}})
					_, err = h.svc.Export(ctx, h.engine.id, exporter.Request{
						Roots:   []uint64{99},
						Outputs: []protocol.ExportOutput{{Number: 99, Address: fakeengine.FSPart}},
					})
				} else {
					_, err = h.svc.Import(ctx, h.engine.id)
				}
				ran <- err
			}()
			select {
			case <-hold.held:
			case <-time.After(5 * time.Second):
				t.Fatalf("the explicit %s didn't reach its index read", hook)
			}
			// Wait begins first, while the paths still run: it waits for
			// the hook, and admits none from then on.
			waited := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				waited <- h.svc.WaitContext(ctx)
			}()
			waitUntil(t, 5*time.Second, "Wait to begin", func() bool {
				h.svc.drain.mu.Lock()
				defer h.svc.drain.mu.Unlock()
				return h.svc.drain.explicitClosed
			})
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Post(h.debug.URL+"/debug/engines/"+strconv.FormatUint(h.engine.id, 10)+"/"+hook, "application/json", strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("an explicit %s asked for once Wait began: %s, want 503", hook, resp.Status)
			}
			// Then the paths' context ends.
			h.cancelPaths()
			select {
			case <-hold.unwinding:
			case <-time.After(5 * time.Second):
				t.Fatalf("the paths' context ending didn't cancel the explicit %s", hook)
			}
			// Shows Wait doesn't return while the hook unwinds: it would
			// return within a few milliseconds if it didn't wait for it.
			select {
			case err := <-waited:
				t.Fatalf("Wait returned (%v) while the explicit %s was still running", err, hook)
			case <-time.After(200 * time.Millisecond):
			}
			if r := h.svc.Running(); r.Explicit != 1 {
				t.Fatalf("running %+v: the explicit %s isn't counted", r, hook)
			}
			release()
			select {
			case err := <-waited:
				if err != nil {
					t.Fatalf("Wait: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("Wait didn't return once the explicit %s ended", hook)
			}
			if err := <-ran; !errors.Is(err, ErrStopping) {
				t.Fatalf("the explicit %s the stop canceled returned %v, want ErrStopping (503)", hook, err)
			}
		})
	}
}

// Once the paths' context ended, before Wait began, an explicit hook is
// refused rather than run on a context already canceled.
func TestExplicitHooksRefusedOnceThePathsStopped(t *testing.T) {
	blobs, meta := newMemStores(t)
	h := newExportHarness(t, blobs, meta, nil)
	h.cancelPaths()
	if _, err := h.svc.Export(context.Background(), h.engine.id, exporter.Request{}); !errors.Is(err, ErrStopping) {
		t.Fatalf("an explicit export after the paths stopped: %v", err)
	}
	if _, err := h.svc.Import(context.Background(), h.engine.id); !errors.Is(err, ErrStopping) {
		t.Fatalf("an explicit import after the paths stopped: %v", err)
	}
}

// An import sync whose context is done doesn't wait for the engine cache's
// sync lock (importer.Importer.Sync) while another sync holds it, as a sync
// loop's sync or an explicit import admitted during another one could: it
// returns the context's cause, and the held import finishes normally.
func TestImportSyncCanceledDoesNotWaitForSyncLock(t *testing.T) {
	h, done, release := newHeldImportHarness(t)
	drained := false
	defer func() {
		release()
		if !drained {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	}()
	c, err := h.svc.conn(h.engine.id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	synced := make(chan error, 1)
	go func() {
		_, err := h.svc.syncMeasured(ctx, c)
		synced <- err
	}()
	select {
	case err := <-synced:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the canceled sync returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the sync with a canceled context waited for the held import's sync lock")
	}
	select {
	case err := <-done:
		drained = true
		t.Fatalf("the held import returned early: %v", err)
	default:
	}
	release()
	select {
	case err := <-done:
		drained = true
		if err != nil {
			t.Fatalf("the held import: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the held import didn't finish once released")
	}
}
