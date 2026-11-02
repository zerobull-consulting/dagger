package blobstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
)

// fakeStore is a Store in memory that records its calls.
type fakeStore struct {
	mu      sync.Mutex
	have    map[digest.Digest]bool
	hasErr  map[digest.Digest]error
	putErr  error
	hasHits map[digest.Digest]int
	puts    map[digest.Digest]int64
}

func newFakeStore(have ...digest.Digest) *fakeStore {
	f := &fakeStore{
		have:    map[digest.Digest]bool{},
		hasErr:  map[digest.Digest]error{},
		hasHits: map[digest.Digest]int{},
		puts:    map[digest.Digest]int64{},
	}
	for _, d := range have {
		f.have[d] = true
	}
	return f
}

func (f *fakeStore) Has(ctx context.Context, d digest.Digest) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hasHits[d]++
	if err := f.hasErr[d]; err != nil {
		return false, err
	}
	return f.have[d], ctx.Err()
}

func (f *fakeStore) PresignPut(_ context.Context, _ Stage, d digest.Digest, size int64) (Address, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return Address{}, f.putErr
	}
	f.puts[d] = size
	return Address{URL: "https://put.example/" + d.Encoded(), ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func (f *fakeStore) PresignGet(_ context.Context, d digest.Digest) (Address, error) {
	return Address{URL: "https://get.example/" + d.Encoded()}, nil
}

func (f *fakeStore) Promote(_ context.Context, _ Stage, d digest.Digest, _ int64) (Promotion, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.have[d] = true
	return Promotion{Outcome: Promoted}, nil
}

// testStage is a valid Stage.
const testStage = Stage("0123456789abcdef0123456789abcdef")

func TestUploadURLsSkipsStoredBlobs(t *testing.T) {
	stored := digest.FromString("stored")
	missing := digest.FromString("missing")
	store := newFakeStore(stored)

	urls, err := UploadURLs(context.Background(), store, testStage, []Blob{
		{Digest: stored, Size: 6},
		{Digest: missing, Size: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 1 || urls[missing] != "https://put.example/"+missing.Encoded() {
		t.Fatalf("urls = %v, want only %s", urls, missing)
	}
	if _, ok := store.puts[stored]; ok {
		t.Errorf("presigned a PUT for %s, which the store has", stored)
	}
	if store.puts[missing] != 7 {
		t.Errorf("PUT for %s signed for %d bytes, want 7", missing, store.puts[missing])
	}
}

func TestUploadURLsAllStored(t *testing.T) {
	a, b := digest.FromString("a"), digest.FromString("b")
	store := newFakeStore(a, b)
	urls, err := UploadURLs(context.Background(), store, testStage, []Blob{{Digest: a, Size: 1}, {Digest: b, Size: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 0 {
		t.Fatalf("urls = %v, want none", urls)
	}
	if len(store.puts) != 0 {
		t.Fatalf("presigned PUTs %v, want none", store.puts)
	}
}

func TestUploadURLsEmpty(t *testing.T) {
	urls, err := UploadURLs(context.Background(), newFakeStore(), testStage, nil)
	if err != nil {
		t.Fatal(err)
	}
	if urls == nil || len(urls) != 0 {
		t.Fatalf("urls = %#v, want an empty, non-nil map", urls)
	}
}

func TestUploadURLsDeduplicates(t *testing.T) {
	d := digest.FromString("twice")
	store := newFakeStore()
	urls, err := UploadURLs(context.Background(), store, testStage, []Blob{{Digest: d, Size: 5}, {Digest: d, Size: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 1 {
		t.Fatalf("urls = %v, want one", urls)
	}
	if store.hasHits[d] != 1 {
		t.Fatalf("checked %s %d times, want once", d, store.hasHits[d])
	}
}

func TestUploadURLsRejectsBadInput(t *testing.T) {
	d := digest.FromString("x")
	for _, tc := range []struct {
		name  string
		blobs []Blob
		want  string
	}{
		{"bad digest", []Blob{{Digest: "sha256:nothex", Size: 1}}, "blob"},
		{"negative size", []Blob{{Digest: d, Size: -1}}, "not positive"},
		{"empty blob", []Blob{{Digest: digest.FromString(""), Size: 0}}, "not positive"},
		{"conflicting sizes", []Blob{{Digest: d, Size: 1}, {Digest: d, Size: 2}}, "sizes 1 and 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			_, err := UploadURLs(context.Background(), store, testStage, tc.blobs)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
			if len(store.hasHits) != 0 {
				t.Fatalf("checked the store before validating: %v", store.hasHits)
			}
		})
	}
}

func TestUploadURLsHasError(t *testing.T) {
	bad := digest.FromString("bad")
	store := newFakeStore()
	boom := errors.New("boom")
	store.hasErr[bad] = boom
	_, err := UploadURLs(context.Background(), store, testStage, []Blob{
		{Digest: bad, Size: 1},
		{Digest: digest.FromString("ok"), Size: 1},
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if !strings.Contains(err.Error(), bad.String()) {
		t.Fatalf("err = %v, want it to name %s", err, bad)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, reports the cancellation it caused", err)
	}
}

func TestUploadURLsPresignError(t *testing.T) {
	store := newFakeStore()
	store.putErr = errors.New("no signing")
	_, err := UploadURLs(context.Background(), store, testStage, []Blob{{Digest: digest.FromString("a"), Size: 1}})
	if !errors.Is(err, store.putErr) || !strings.Contains(err.Error(), "presign PUT") {
		t.Fatalf("err = %v, want the presign error", err)
	}
}

func TestUploadURLsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := UploadURLs(ctx, newFakeStore(), testStage, []Blob{{Digest: digest.FromString("a"), Size: 1}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// fakeStore.Has with a gauge: how many calls run at once, at most.
type gaugedStore struct {
	*fakeStore
	mu        sync.Mutex
	running   int
	maxActive int
}

func (g *gaugedStore) Has(ctx context.Context, d digest.Digest) (bool, error) {
	g.mu.Lock()
	g.running++
	g.maxActive = max(g.maxActive, g.running)
	g.mu.Unlock()
	time.Sleep(time.Millisecond)
	defer func() {
		g.mu.Lock()
		g.running--
		g.mu.Unlock()
	}()
	return g.fakeStore.Has(ctx, d)
}

// Checks run at most maxConcurrentChecks at a time, however many blobs.
func TestUploadURLsBoundsConcurrency(t *testing.T) {
	store := &gaugedStore{fakeStore: newFakeStore()}
	var blobs []Blob
	for i := range 10 * maxConcurrentChecks {
		blobs = append(blobs, Blob{Digest: digest.FromString(strings.Repeat("y", i+1)), Size: 1})
	}
	if _, err := UploadURLs(context.Background(), store, testStage, blobs); err != nil {
		t.Fatal(err)
	}
	if store.maxActive > maxConcurrentChecks {
		t.Fatalf("%d checks ran at once, want at most %d", store.maxActive, maxConcurrentChecks)
	}
}

// More blobs than checks run at once: every one is still checked.
func TestUploadURLsManyBlobs(t *testing.T) {
	store := newFakeStore()
	var blobs []Blob
	for i := range 3 * maxConcurrentChecks {
		d := digest.FromString(strings.Repeat("x", i+1))
		if i%2 == 0 {
			store.have[d] = true
		}
		blobs = append(blobs, Blob{Digest: d, Size: int64(i + 1)})
	}
	urls, err := UploadURLs(context.Background(), store, testStage, blobs)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != len(blobs)/2 {
		t.Fatalf("got %d URLs, want %d", len(urls), len(blobs)/2)
	}
	for _, b := range blobs {
		if _, ok := urls[b.Digest]; ok == store.have[b.Digest] {
			t.Errorf("%s: URL %v, stored %v", b.Digest, ok, store.have[b.Digest])
		}
	}
}

// NewStage returns valid, distinct stages; Validate refuses anything else,
// so a staging key never takes an arbitrary string.
func TestStage(t *testing.T) {
	a, b := NewStage(), NewStage()
	if a == b {
		t.Fatalf("two stages %s", a)
	}
	for _, s := range []Stage{a, b, testStage} {
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []Stage{"", "x", "../../blobs/sha256/abc", Stage(strings.Repeat("A", 32)), Stage(strings.Repeat("a", 31)), Stage(strings.Repeat("a", 33)), Stage(strings.Repeat("a", 31) + "/")} {
		if bad.Validate() == nil {
			t.Errorf("stage %q accepted", bad)
		}
	}
	if _, err := UploadURLs(context.Background(), newFakeStore(), "bad", []Blob{{Digest: digest.FromString("a"), Size: 1}}); err == nil {
		t.Fatal("UploadURLs took a bad stage")
	}
}

// countingStore counts its promotions and the most in flight at once.
type countingStore struct {
	*fakeStore
	mu             sync.Mutex
	calls          map[digest.Digest]int
	inFlight, peak int
}

func (c *countingStore) Promote(ctx context.Context, s Stage, d digest.Digest, size int64) (Promotion, error) {
	c.mu.Lock()
	c.calls[d]++
	c.inFlight++
	c.peak = max(c.peak, c.inFlight)
	c.mu.Unlock()
	time.Sleep(time.Millisecond)
	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}()
	if d == digest.FromString("fails") {
		return Promotion{}, errors.New("store down")
	}
	return c.fakeStore.Promote(ctx, s, d, size)
}

// PromoteAll promotes each blob once, at most maxConcurrentPromotions at a
// time, and reports each listing in order, errors included.
func TestPromoteAll(t *testing.T) {
	store := &countingStore{fakeStore: newFakeStore(), calls: map[digest.Digest]int{}}
	var blobs []Blob
	for i := range 50 {
		blobs = append(blobs, Blob{Digest: digest.FromString(strings.Repeat("x", i+1)), Size: 1})
	}
	blobs = append(blobs, blobs[3], Blob{Digest: digest.FromString("fails"), Size: 1})
	res := PromoteAll(context.Background(), store, testStage, blobs)
	if len(res) != len(blobs) {
		t.Fatalf("%d results for %d blobs", len(res), len(blobs))
	}
	for i, r := range res {
		if r.Digest != blobs[i].Digest {
			t.Fatalf("result %d is of %s, want %s", i, r.Digest, blobs[i].Digest)
		}
		failing := r.Digest == digest.FromString("fails")
		if failing != (r.Err != nil) || !failing && !r.Stored() {
			t.Fatalf("result %d: %+v", i, r)
		}
	}
	if store.calls[blobs[3].Digest] != 1 {
		t.Fatalf("a blob listed twice was promoted %d times", store.calls[blobs[3].Digest])
	}
	if store.peak > maxConcurrentPromotions || store.peak < 2 {
		t.Fatalf("peak %d promotions at once, want 2..%d", store.peak, maxConcurrentPromotions)
	}
}
