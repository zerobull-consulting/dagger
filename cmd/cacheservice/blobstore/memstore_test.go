package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/opencontainers/go-digest"
)

func memPut(t *testing.T, url string, body []byte, size int64) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	req.ContentLength = size
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestMemStore(t *testing.T) {
	m := NewMemStore("")
	srv := httptest.NewServer(m)
	defer srv.Close()
	m.SetBaseURL(srv.URL)
	ctx := context.Background()
	blob := []byte("layer bytes")
	d := digest.FromBytes(blob)

	if _, err := m.PresignPut(ctx, d, 0); !errors.Is(err, ErrNotPositiveSize) {
		t.Fatalf("size 0: %v", err)
	}
	put, err := m.PresignPut(ctx, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	// A body of another size, or content, is refused.
	if code := memPut(t, put.URL, blob[:3], 3); code != http.StatusForbidden {
		t.Fatalf("short body: %d", code)
	}
	if code := memPut(t, put.URL, bytes.Repeat([]byte("x"), len(blob)), int64(len(blob))); code != http.StatusBadRequest {
		t.Fatalf("wrong content: %d", code)
	}
	// An injected failure, then success.
	m.FailPuts(d, 1)
	if code := memPut(t, put.URL, blob, int64(len(blob))); code != http.StatusServiceUnavailable {
		t.Fatalf("injected failure: %d", code)
	}
	if ok, _ := m.Has(ctx, d); ok {
		t.Fatal("stored after a failed PUT")
	}
	if code := memPut(t, put.URL, blob, int64(len(blob))); code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	if ok, _ := m.Has(ctx, d); !ok || m.Len() != 1 || m.Puts() != 1 {
		t.Fatalf("has %v, len %d, puts %d", ok, m.Len(), m.Puts())
	}
	get, err := m.PresignGet(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(get.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, blob) {
		t.Fatalf("got %q", got)
	}
	// UploadURLs skips what it has.
	other := m.Put([]byte("other"))
	urls, err := UploadURLs(ctx, m, []Blob{{Digest: d, Size: int64(len(blob))}, {Digest: digest.FromString("new"), Size: 3}, {Digest: other, Size: 5}})
	if err != nil || len(urls) != 1 {
		t.Fatalf("urls %v, err %v", urls, err)
	}
	if _, err := m.Has(ctx, "bad"); err == nil {
		t.Fatal("bad digest accepted")
	}
}

// A namespace's view keeps its blobs apart: a blob PUT through one view is
// stored, counted and served in that namespace only, and its addresses name
// the namespace.
func TestMemStoreNamespaces(t *testing.T) {
	m := NewMemStore("")
	srv := httptest.NewServer(m)
	defer srv.Close()
	m.SetBaseURL(srv.URL)
	ctx := context.Background()
	a, err := m.In("team-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Namespace("team-b")
	if err != nil {
		t.Fatal(err)
	}
	blob := []byte("layer bytes")
	d := digest.FromBytes(blob)
	put, err := a.PresignPut(ctx, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if code := memPut(t, put.URL, blob, int64(len(blob))); code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	if ok, _ := a.Has(ctx, d); !ok || a.Len() != 1 {
		t.Fatal("not stored in team-a")
	}
	for name, s := range map[string]Store{"team-b": b, "the store": m} {
		if ok, _ := s.Has(ctx, d); ok {
			t.Fatalf("stored in %s too", name)
		}
	}
	if m.Len() != 0 {
		t.Fatal("counted at the top")
	}
	getB, _ := b.PresignGet(ctx, d)
	resp, err := http.Get(getB.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("team-b's GET of team-a's blob: %d", resp.StatusCode)
	}
	getA, _ := a.PresignGet(ctx, d)
	if want := srv.URL + "/mem/team-a/" + d.String(); getA.URL != want {
		t.Fatalf("address %s, want %s", getA.URL, want)
	}
	for _, bad := range []string{"", "a/b", "..", "."} {
		if _, err := m.Namespace(bad); err == nil {
			t.Errorf("namespace %q accepted", bad)
		}
	}
}
