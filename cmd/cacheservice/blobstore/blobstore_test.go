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

func (f *fakeStore) PresignPut(_ context.Context, d digest.Digest, size int64) (Address, error) {
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

func TestUploadURLsSkipsStoredBlobs(t *testing.T) {
	stored := digest.FromString("stored")
	missing := digest.FromString("missing")
	store := newFakeStore(stored)

	urls, err := UploadURLs(context.Background(), store, []Blob{
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
	urls, err := UploadURLs(context.Background(), store, []Blob{{Digest: a, Size: 1}, {Digest: b, Size: 1}})
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
	urls, err := UploadURLs(context.Background(), newFakeStore(), nil)
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
	urls, err := UploadURLs(context.Background(), store, []Blob{{Digest: d, Size: 5}, {Digest: d, Size: 5}})
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
			_, err := UploadURLs(context.Background(), store, tc.blobs)
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
	_, err := UploadURLs(context.Background(), store, []Blob{
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
	_, err := UploadURLs(context.Background(), store, []Blob{{Digest: digest.FromString("a"), Size: 1}})
	if !errors.Is(err, store.putErr) || !strings.Contains(err.Error(), "presign PUT") {
		t.Fatalf("err = %v, want the presign error", err)
	}
}

func TestUploadURLsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := UploadURLs(ctx, newFakeStore(), []Blob{{Digest: digest.FromString("a"), Size: 1}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
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
	urls, err := UploadURLs(context.Background(), store, blobs)
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
