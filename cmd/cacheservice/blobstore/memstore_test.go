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
	stage := NewStage()

	if _, err := m.PresignPut(ctx, stage, d, 0); !errors.Is(err, ErrNotPositiveSize) {
		t.Fatalf("size 0: %v", err)
	}
	if _, err := m.PresignPut(ctx, "bad", d, 1); err == nil {
		t.Fatal("bad stage accepted")
	}
	put, err := m.PresignPut(ctx, stage, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if want := srv.URL + "/mem/staging/" + string(stage) + "/" + d.String() + "?size=11"; put.URL != want {
		t.Fatalf("PUT address %s, want %s", put.URL, want)
	}
	// A body of another size is refused.
	if code := memPut(t, put.URL, blob[:3], 3); code != http.StatusForbidden {
		t.Fatalf("short body: %d", code)
	}
	// An injected failure, then success.
	m.FailPuts(d, 1)
	if code := memPut(t, put.URL, blob, int64(len(blob))); code != http.StatusServiceUnavailable {
		t.Fatalf("injected failure: %d", code)
	}
	if code := memPut(t, put.URL, blob, int64(len(blob))); code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	// Staged, not stored, and not served, until promoted.
	if ok, _ := m.Has(ctx, d); ok || m.Len() != 0 || len(m.Staged()) != 1 || m.Puts() != 1 {
		t.Fatalf("before promotion: has %v, len %d, staged %d, puts %d", ok, m.Len(), len(m.Staged()), m.Puts())
	}
	if code := memGet(t, srv.URL+"/mem/staging/"+string(stage)+"/"+d.String()); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET of a staged blob: %d", code)
	}
	if code := memPut(t, srv.URL+"/mem/"+d.String()+"?size=11", blob, int64(len(blob))); code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT to a digest key: %d", code)
	}
	p, err := m.Promote(ctx, stage, d, int64(len(blob)))
	if err != nil || p.Outcome != Promoted || p.HashedBy != HashedByService || p.Bytes != int64(len(blob)) {
		t.Fatalf("promote: %+v, %v", p, err)
	}
	if ok, _ := m.Has(ctx, d); !ok || m.Len() != 1 || len(m.Staged()) != 0 {
		t.Fatalf("after promotion: has %v, len %d, staged %d", ok, m.Len(), len(m.Staged()))
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
	// Nothing staged is Missing; a second copy finds the blob Existing.
	if p, err := m.Promote(ctx, NewStage(), d, int64(len(blob))); err != nil || p.Outcome != Missing {
		t.Fatalf("promote of nothing staged: %+v, %v", p, err)
	}
	again := NewStage()
	put, _ = m.PresignPut(ctx, again, d, int64(len(blob)))
	memPut(t, put.URL, blob, int64(len(blob)))
	if p, err := m.Promote(ctx, again, d, int64(len(blob))); err != nil || p.Outcome != Existing || len(m.Staged()) != 0 {
		t.Fatalf("second promotion: %+v, %v, %d staged", p, err, len(m.Staged()))
	}
	// UploadURLs skips what it has.
	other := m.Put([]byte("other"))
	urls, err := UploadURLs(ctx, m, NewStage(), []Blob{{Digest: d, Size: int64(len(blob))}, {Digest: digest.FromString("new"), Size: 3}, {Digest: other, Size: 5}})
	if err != nil || len(urls) != 1 {
		t.Fatalf("urls %v, err %v", urls, err)
	}
	if _, err := m.Has(ctx, "bad"); err == nil {
		t.Fatal("bad digest accepted")
	}
}

// A same-size body with the wrong content is staged like any other, but its
// promotion is a mismatch: it never reaches the digest key, is dropped, and
// the blob is asked for again.
func TestMemStorePromoteMismatch(t *testing.T) {
	m := NewMemStore("")
	srv := httptest.NewServer(m)
	defer srv.Close()
	m.SetBaseURL(srv.URL)
	ctx := context.Background()
	blob := []byte("layer bytes")
	d := digest.FromBytes(blob)
	stage := NewStage()
	put, _ := m.PresignPut(ctx, stage, d, int64(len(blob)))
	if code := memPut(t, put.URL, bytes.Repeat([]byte("x"), len(blob)), int64(len(blob))); code != http.StatusOK {
		t.Fatalf("PUT of the wrong content: %d (the store checks sizes only)", code)
	}
	p, err := m.Promote(ctx, stage, d, int64(len(blob)))
	if err != nil || p.Outcome != Mismatch || p.Stored() {
		t.Fatalf("promote: %+v, %v", p, err)
	}
	if ok, _ := m.Has(ctx, d); ok || len(m.Staged()) != 0 {
		t.Fatalf("after the mismatch: has %v, staged %d", ok, len(m.Staged()))
	}
	urls, err := UploadURLs(ctx, m, NewStage(), []Blob{{Digest: d, Size: int64(len(blob))}})
	if err != nil || len(urls) != 1 {
		t.Fatalf("not asked for again: %v, %v", urls, err)
	}
}

func memGet(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
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
	stage := NewStage()
	put, err := a.PresignPut(ctx, stage, d, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if code := memPut(t, put.URL, blob, int64(len(blob))); code != http.StatusOK {
		t.Fatalf("put: %d", code)
	}
	// Staged in team-a: another namespace's promotion finds nothing.
	if p, err := b.Promote(ctx, stage, d, int64(len(blob))); err != nil || p.Outcome != Missing {
		t.Fatalf("team-b promoted team-a's upload: %+v, %v", p, err)
	}
	if p, err := a.Promote(ctx, stage, d, int64(len(blob))); err != nil || p.Outcome != Promoted {
		t.Fatalf("promote: %+v, %v", p, err)
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
	// A namespace named staging is a namespace like any other.
	st, _ := m.In("staging")
	put, _ = st.PresignPut(ctx, stage, d, int64(len(blob)))
	if code := memPut(t, put.URL, blob, int64(len(blob))); code != http.StatusOK {
		t.Fatalf("put in namespace staging: %d", code)
	}
	if p, err := st.Promote(ctx, stage, d, int64(len(blob))); err != nil || p.Outcome != Promoted {
		t.Fatalf("promote in namespace staging: %+v, %v", p, err)
	}
	getS, _ := st.PresignGet(ctx, d)
	if code := memGet(t, getS.URL); code != http.StatusOK || m.Len() != 0 {
		t.Fatalf("GET in namespace staging: %d; stored at the top: %d", code, m.Len())
	}
	for _, bad := range []string{"", "a/b", "..", "."} {
		if _, err := m.Namespace(bad); err == nil {
			t.Errorf("namespace %q accepted", bad)
		}
	}
}
