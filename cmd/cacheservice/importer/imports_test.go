package importer

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// importedCalls records Config.Imported's calls.
type importedCalls struct {
	mu    sync.Mutex
	calls []map[uint64]Value
}

func (c *importedCalls) hook(cacheID string, _ uint64, values map[uint64]Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, values)
}

func (c *importedCalls) all() []map[uint64]Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

func newImportsFixture(t *testing.T, objects metastore.Objects) (*fixture, *importedCalls) {
	t.Helper()
	meta, err := metastore.New(metastore.Config{Objects: objects})
	if err != nil {
		t.Fatal(err)
	}
	blobs := blobstore.NewMemStore("http://blobs.test")
	calls := &importedCalls{}
	im, err := New(Config{Blobs: blobs, Meta: meta, Scope: scope, Imported: calls.hook})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, im: im, meta: meta, blobs: blobs}, calls
}

// A merge records where each value landed as the cache's import, and tells
// the export path; parts the offers place later are told too.
func TestSyncRecordsImports(t *testing.T) {
	f, calls := newImportsFixture(t, metastore.NewMemObjects())
	rec := f.store(chainBundle(10, "base", "exec"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(true), onOffer: answerAll(protocol.OfferAccepted)}
	if _, err := f.im.Sync(context.Background(), eng); err != nil {
		t.Fatal(err)
	}
	idx, err := f.meta.Index(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	imported := idx.Caches["cache-b"].Imported
	if len(imported) != 2 || imported[101].Number != rec.Numbers[1] || imported[102].Number != rec.Numbers[2] {
		t.Fatalf("imports %+v", imported)
	}
	got := calls.all()
	if len(got) != 2 {
		t.Fatalf("%d calls, want the merge's and the offers'", len(got))
	}
	if v := got[0][rec.Numbers[2]]; v.Number != 102 || len(v.Offered) != 0 {
		t.Fatalf("merge's call %+v", got[0])
	}
	if v := got[1][rec.Numbers[2]]; v.Number != 102 || len(v.Offered) != 1 || len(got[1]) != 2 {
		t.Fatalf("offers' call %+v", got[1])
	}
}

// A value that lands on an engine entry the cache exported itself is not
// its import, and the export path isn't told it is.
func TestSyncImportsSkipOwnEntries(t *testing.T) {
	f, calls := newImportsFixture(t, metastore.NewMemObjects())
	rec := f.store(chainBundle(10, "base", "exec"), "cache-a")
	// cache-b exported engine entry 101, where the merge lands "base".
	f.store(chainBundle(101, "base"), "cache-b")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false), onOffer: answerAll(protocol.OfferAccepted)}
	if _, err := f.im.Sync(context.Background(), eng); err != nil {
		t.Fatal(err)
	}
	idx, err := f.meta.Index(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if imported := idx.Caches["cache-b"].Imported; len(imported) != 1 || imported[102].Number != rec.Numbers[2] {
		t.Fatalf("imports %+v", imported)
	}
	got := calls.all()
	if len(got) != 1 || len(got[0]) != 1 || got[0][rec.Numbers[2]].Number != 102 {
		t.Fatalf("calls %+v", got)
	}
}

// onIndexPut calls hook before the first index write that holds imports.
type onIndexPut struct {
	metastore.Objects
	once sync.Once
	hook func()
}

func (o *onIndexPut) Put(ctx context.Context, key string, body []byte, cond metastore.Condition) (string, error) {
	if strings.HasSuffix(key, "/index.json") && strings.Contains(string(body), `"imported"`) {
		o.once.Do(o.hook)
	}
	return o.Objects.Put(ctx, key, body, cond)
}

// A wipe that forgets the cache while its imports are being written leaves
// none in the index.
func TestSyncImportsRacingWipe(t *testing.T) {
	objects := &onIndexPut{Objects: metastore.NewMemObjects()}
	f, calls := newImportsFixture(t, objects)
	// The wipe, as the service runs it: forget, then drop the imports,
	// which finds none yet.
	objects.hook = func() {
		f.im.Forget("cache-b")
	}
	f.store(chainBundle(10, "base"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false)}
	if _, err := f.im.Sync(context.Background(), eng); !errors.Is(err, errStateReset) {
		t.Fatalf("sync: %v", err)
	}
	idx, err := f.meta.Index(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if c := idx.Caches["cache-b"]; c != nil {
		t.Fatalf("a wiped cache's imports stayed: %+v", c)
	}
	if len(calls.all()) != 0 {
		t.Fatalf("export path told %+v", calls.all())
	}
}

// A new generation of the cache, connecting while the imports are written,
// keeps them: its entries are the same.
func TestSyncImportsRacingNewGeneration(t *testing.T) {
	objects := &onIndexPut{Objects: metastore.NewMemObjects()}
	f, _ := newImportsFixture(t, objects)
	objects.hook = func() {
		f.im.mu.Lock()
		f.im.stateLocked("cache-b", 2)
		f.im.mu.Unlock()
	}
	f.store(chainBundle(10, "base"), "cache-a")
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false)}
	if _, err := f.im.Sync(context.Background(), eng); !errors.Is(err, errStateReset) {
		t.Fatalf("sync: %v", err)
	}
	idx, err := f.meta.Index(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if c := idx.Caches["cache-b"]; c == nil || len(c.Imported) != 1 {
		t.Fatalf("imports %+v", c)
	}
}
