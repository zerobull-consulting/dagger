package importer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// gateObjects counts the index reads inside a sync, how many are inside at
// once, and holds the first until released.
type gateObjects struct {
	metastore.Objects
	inside, maxInside, reads atomic.Int32
	held                     chan struct{} // closed when the first read is held
	released                 chan struct{}
	holdFirst                bool
	once                     sync.Once
}

func (o *gateObjects) Get(ctx context.Context, key string) ([]byte, string, error) {
	n := o.inside.Add(1)
	defer o.inside.Add(-1)
	for {
		m := o.maxInside.Load()
		if n <= m || o.maxInside.CompareAndSwap(m, n) {
			break
		}
	}
	o.reads.Add(1)
	if o.holdFirst {
		o.once.Do(func() {
			close(o.held)
			<-o.released // deliberately ignores ctx: the held sync is the lock's holder
		})
	}
	time.Sleep(time.Millisecond)
	return o.Objects.Get(ctx, key)
}

func newGatedImporter(t *testing.T, hold bool) (*Importer, *gateObjects) {
	t.Helper()
	g := &gateObjects{Objects: metastore.NewMemObjects(), held: make(chan struct{}), released: make(chan struct{}), holdFirst: hold}
	meta, err := metastore.New(metastore.Config{Objects: g})
	if err != nil {
		t.Fatal(err)
	}
	im, err := New(Config{Blobs: blobstore.NewMemStore("http://blobs.test"), Meta: meta, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	return im, g
}

func syncAsync(im *Importer, ctx context.Context, eng Engine) <-chan error {
	ch := make(chan error, 1)
	go func() {
		_, err := im.Sync(ctx, eng)
		ch <- err
	}()
	return ch
}

func within(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out: %s", what)
		return nil
	}
}

func holdFirstSync(t *testing.T, im *Importer, g *gateObjects, eng Engine) <-chan error {
	t.Helper()
	first := syncAsync(im, context.Background(), eng)
	select {
	case <-g.held:
	case <-time.After(5 * time.Second):
		t.Fatal("the first sync didn't reach its index read")
	}
	return first
}

// A sync whose context is already done returns its cause at once while
// another holds the lock, and without taking a free lock either.
func TestSyncLockCanceledBeforeWaiting(t *testing.T) {
	im, g := newGatedImporter(t, true)
	eng := &scriptedEngine{cacheID: "c1", generation: 1}
	first := holdFirstSync(t, im, g, eng)
	cause := errors.New("stopping")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	if err := within(t, syncAsync(im, ctx, eng), "canceled sync waited for the lock"); !errors.Is(err, cause) {
		t.Fatalf("got %v, want the cause", err)
	}
	close(g.released)
	if err := within(t, first, "held sync"); err != nil {
		t.Fatal(err)
	}
	// Free lock, done context: no read happens.
	reads := g.reads.Load()
	if err := within(t, syncAsync(im, ctx, eng), "sync on a free lock"); !errors.Is(err, cause) {
		t.Fatalf("got %v, want the cause", err)
	}
	if g.reads.Load() != reads {
		t.Fatal("a sync with a done context ran")
	}
}

// A sync canceled while it waits returns without waiting for the holder, and
// the lock still works afterwards.
func TestSyncLockCanceledWhileWaiting(t *testing.T) {
	im, g := newGatedImporter(t, true)
	eng := &scriptedEngine{cacheID: "c1", generation: 1}
	first := holdFirstSync(t, im, g, eng)
	ctx, cancel := context.WithCancel(context.Background())
	waiter := syncAsync(im, ctx, eng)
	select {
	case err := <-waiter:
		t.Fatalf("returned before cancel: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	if err := within(t, waiter, "the waiter after cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	// The holder is still held; the lock is intact.
	select {
	case err := <-first:
		t.Fatalf("holder returned early: %v", err)
	default:
	}
	close(g.released)
	if err := within(t, first, "held sync"); err != nil {
		t.Fatal(err)
	}
	reads := g.reads.Load()
	if err := within(t, syncAsync(im, context.Background(), eng), "next sync"); err != nil {
		t.Fatal(err)
	}
	if g.reads.Load() == reads {
		t.Fatal("the next sync didn't run")
	}
}

// Syncs with live contexts wait for each other and never overlap.
func TestSyncLockSerializes(t *testing.T) {
	im, g := newGatedImporter(t, false)
	eng := &scriptedEngine{cacheID: "c1", generation: 1}
	const n = 8
	var chans []<-chan error
	for range n {
		chans = append(chans, syncAsync(im, context.Background(), eng))
	}
	for _, ch := range chans {
		if err := within(t, ch, "a sync"); err != nil {
			t.Fatal(err)
		}
	}
	if got := g.reads.Load(); got < n {
		t.Fatalf("%d index reads for %d syncs", got, n)
	}
	if got := g.maxInside.Load(); got != 1 {
		t.Fatalf("%d syncs inside at once, want 1", got)
	}
}

// A state made after a forget or a new generation has a usable lock.
func TestSyncLockOfEveryStateConstructor(t *testing.T) {
	im, _ := newGatedImporter(t, false)
	for _, eng := range []*scriptedEngine{
		{cacheID: "c1", generation: 1},
		{cacheID: "c1", generation: 2},
	} {
		if err := within(t, syncAsync(im, context.Background(), eng), "sync"); err != nil {
			t.Fatal(err)
		}
	}
	im.Forget("c1")
	if err := within(t, syncAsync(im, context.Background(), &scriptedEngine{cacheID: "c1", generation: 1}), "sync after forget"); err != nil {
		t.Fatal(err)
	}
}
