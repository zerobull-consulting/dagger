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
	"github.com/dagger/dagger/dagql"
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
	if c := idx.Caches["cache-b"]; c != nil && len(c.Imported) != 0 {
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

// failIndexPuts fails index writes that hold imports while n > 0.
type failIndexPuts struct {
	metastore.Objects
	mu sync.Mutex
	n  int
}

func (o *failIndexPuts) Put(ctx context.Context, key string, body []byte, cond metastore.Condition) (string, error) {
	if strings.HasSuffix(key, "/index.json") && strings.Contains(string(body), `"imported"`) {
		o.mu.Lock()
		fail := o.n > 0
		o.n--
		o.mu.Unlock()
		if fail {
			return "", errors.New("store unavailable")
		}
	}
	return o.Objects.Put(ctx, key, body, cond)
}

// A merge whose imports can't be recorded stays committed, since the engine
// has it, and is not sent again: only the index write is retried, at each
// sync, however many times it fails, and the export path is told once it
// lands.
func TestSyncRetriesImportsNotMerge(t *testing.T) {
	objects := &failIndexPuts{Objects: metastore.NewMemObjects()}
	f, calls := newImportsFixture(t, objects)
	f.store(chainBundle(10, "base"), "cache-a")
	objects.n = DefaultMergeAttempts + 1
	eng := &scriptedEngine{cacheID: "cache-b", generation: 1, onMerge: landAll(false)}
	ctx := context.Background()
	for i := range DefaultMergeAttempts + 1 {
		res, err := f.im.Sync(ctx, eng)
		if err != nil || len(res.Failed) != 0 {
			t.Fatalf("sync %d: %+v, %v", i, res, err)
		}
		if i == 0 && len(res.Merged) != 1 {
			t.Fatalf("sync 0: %+v", res)
		}
		if len(calls.all()) != 0 {
			t.Fatalf("sync %d: export path told before the imports were recorded", i)
		}
	}
	if merges, _ := eng.counts(); merges != 1 {
		t.Fatalf("%d merges: the merge was sent again", merges)
	}
	if _, err := f.im.Sync(ctx, eng); err != nil {
		t.Fatal(err)
	}
	idx, err := f.meta.Index(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if c := idx.Caches["cache-b"]; c == nil || len(c.Imported) != 1 {
		t.Fatalf("imports %+v", c)
	}
	if got := calls.all(); len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("export path told %+v", got)
	}
	if merges, _ := eng.counts(); merges != 1 {
		t.Fatalf("%d merges", merges)
	}
	if st, _ := f.im.State("cache-b"); len(st.Merged) != 1 || len(st.Failures) != 0 {
		t.Fatalf("state %+v", st)
	}
}

// #47, through Sync: C has A's entries merged, exports a bundle built on
// them, and then its imports move (it pruned them, restarted, and had them
// merged again) or are dropped (it was wiped). A fresh engine still gets C's
// bundle merged, with base and exec on A's service numbers.
func TestSyncMergesBundleOfMovedImports(t *testing.T) {
	for name, change := range map[string]func(*fixture, map[dagql.TransferOrdinal]uint64) error{
		"moved": func(f *fixture, a map[dagql.TransferOrdinal]uint64) error {
			_, err := f.meta.RecordImports(context.Background(), scope, "cache-c", 2, []metastore.Import{{Number: a[1], Engine: 201}, {Number: a[2], Engine: 202}})
			return err
		},
		"wiped": func(f *fixture, _ map[dagql.TransferOrdinal]uint64) error {
			return f.meta.ForgetImports(context.Background(), scope, "cache-c")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, _ := newImportsFixture(t, metastore.NewMemObjects())
			ctx := context.Background()
			a := f.store(chainBundle(10, "base", "exec"), "cache-a")
			c := &scriptedEngine{cacheID: "cache-c", generation: 1, onMerge: landAll(true), onOffer: answerAll(protocol.OfferAccepted)}
			if _, err := f.im.Sync(ctx, c); err != nil {
				t.Fatal(err)
			}
			// C's export names base and exec by the entries they landed
			// on, 101 and 102, and builds test on them.
			cRec := f.store(chainBundle(101, "base", "exec", "test"), "cache-c")
			if cRec.Numbers[1] != a.Numbers[1] || cRec.Numbers[2] != a.Numbers[2] {
				t.Fatalf("C's export recorded as %v, want base and exec on A's %v", cRec.Numbers, a.Numbers)
			}
			if err := change(f, a.Numbers); err != nil {
				t.Fatal(err)
			}

			fresh := &scriptedEngine{cacheID: "cache-fresh", generation: 1, onMerge: landAll(true), onOffer: answerAll(protocol.OfferAccepted)}
			res, err := f.im.Sync(ctx, fresh)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Failed) != 0 {
				t.Fatalf("merges failed: %+v", res.Failed)
			}
			fresh.mu.Lock()
			merges := slices.Clone(fresh.merges)
			fresh.mu.Unlock()
			var got []uint64
			for _, m := range merges {
				if len(m.Bundle.Values) == 3 {
					for _, v := range m.Bundle.Values {
						got = append(got, v.SenderNumber)
					}
				}
			}
			want := []uint64{cRec.Numbers[1], cRec.Numbers[2], cRec.Numbers[3]}
			if !slices.Equal(got, want) {
				t.Fatalf("C's bundle merged with numbers %v (merges %d), want %v", got, len(merges), want)
			}
		})
	}
}
