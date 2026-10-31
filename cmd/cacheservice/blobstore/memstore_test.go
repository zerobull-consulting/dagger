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
