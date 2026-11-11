package blobstore

import (
	"context"
	"crypto/md5" //nolint:gosec // what the fake's ETags are
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
)

// copyStore is a fake S3 that, like DigitalOcean Spaces, ignores
// CopySourceIfMatch and returns no checksums. overwrite replaces the staged
// object right after the service has read it.
type copyStore struct {
	mu        sync.Mutex
	body      []byte
	overwrite []byte
	// etag makes the object's and the parts' ETags something else than their
	// MD5s, as AWS S3 does with SSE-KMS.
	etag      func(data []byte) string
	completed int
	aborted   int
	copies    int
	// noCopyResult answers part copies with 200 and no CopyPartResult.
	noCopyResult bool
	// checksum answers part copies with the data's SHA-256, as a store
	// that hashes does.
	checksum bool
}

func (c *copyStore) etagOf(data []byte) string {
	if c.etag != nil {
		return c.etag(data)
	}
	sum := md5.Sum(data) //nolint:gosec // see the import
	return hex.EncodeToString(sum[:])
}

func (c *copyStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	staged := strings.Contains(r.URL.Path, "/staging/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodHead && staged:
		w.Header().Set("ETag", `"`+c.etagOf(c.body)+`"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(c.body)))
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusNotFound)
	case r.Method == http.MethodGet && staged:
		_, _ = w.Write(c.body)
		if c.overwrite != nil {
			c.body = c.overwrite
		}
	case r.Method == http.MethodPost && q.Has("uploads"):
		_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		c.copies++
		data := c.body
		if rng := r.Header.Get("X-Amz-Copy-Source-Range"); rng != "" {
			var from, to int
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &from, &to); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			data = c.body[from : to+1]
		}
		if c.noCopyResult {
			return
		}
		if c.checksum {
			sum := sha256.Sum256(data)
			_, _ = fmt.Fprintf(w, `<CopyPartResult><ETag>"%s"</ETag><ChecksumSHA256>%s</ChecksumSHA256></CopyPartResult>`, c.etagOf(data), base64.StdEncoding.EncodeToString(sum[:]))
			return
		}
		_, _ = fmt.Fprintf(w, `<CopyPartResult><ETag>"%s"</ETag></CopyPartResult>`, c.etagOf(data))
	case r.Method == http.MethodPost && q.Has("uploadId"):
		c.completed++
		_, _ = io.WriteString(w, `<CompleteMultipartUploadResult/>`)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		c.aborted++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// Without CopySourceIfMatch to rely on, a staged object overwritten between
// the service's hash and the store's copy must not reach the digest key: the
// part's ETag shows the copy isn't what was hashed. A store whose ETags aren't
// MD5s can't be checked this way, and must not be failed by it.
func TestPromoteChecksCopiedParts(t *testing.T) {
	good := []byte(strings.Repeat("0123456789", 3))
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xff
	d := digest.FromBytes(good)
	notMD5 := func(data []byte) string { return fmt.Sprintf("kms-%d", len(data)) }

	for _, tc := range []struct {
		name          string
		overwrite     []byte
		etag          func([]byte) string
		partSize      int64
		wantOutcome   Outcome
		wantCompleted int
		wantAborted   int
	}{
		{name: "one part", wantOutcome: Promoted, wantCompleted: 1},
		{name: "many parts", partSize: 10, wantOutcome: Promoted, wantCompleted: 1},
		{name: "one part overwritten", overwrite: bad, wantOutcome: Mismatch, wantAborted: 1},
		{name: "last of many parts overwritten", overwrite: bad, partSize: 10, wantOutcome: Mismatch, wantAborted: 1},
		{name: "ETags aren't MD5s", etag: notMD5, wantOutcome: Promoted, wantCompleted: 1},
		{name: "many parts, ETags aren't MD5s", etag: notMD5, partSize: 10, wantOutcome: Promoted, wantCompleted: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &copyStore{body: good, overwrite: tc.overwrite, etag: tc.etag}
			srv := httptest.NewServer(f)
			defer srv.Close()
			cfg := testConfig()
			cfg.Endpoint = srv.URL
			cfg.HashInService = true
			s := newTestStore(t, cfg)
			if tc.partSize > 0 {
				s.maxPartCopy, s.copyPartSize = tc.partSize, tc.partSize
			}
			p, err := s.Promote(context.Background(), NewStage(), d, int64(len(good)))
			if err != nil {
				t.Fatal(err)
			}
			if p.Outcome != tc.wantOutcome || p.HashedBy != HashedByService {
				t.Fatalf("promote: %+v, want %s hashed by the service", p, tc.wantOutcome)
			}
			if f.completed != tc.wantCompleted || f.aborted != tc.wantAborted {
				t.Fatalf("%d uploads completed, %d aborted; want %d and %d", f.completed, f.aborted, tc.wantCompleted, tc.wantAborted)
			}
		})
	}
}

// A store answering UploadPartCopy without a CopyPartResult
// (zerobull-consulting/dagger#77, item 6) fails the promotion with an error,
// in both paths, instead of panicking on the missing ETag; the upload is
// aborted and nothing is completed. The store path doesn't take it for a
// store that doesn't hash: that would switch every later promotion to the
// service.
func TestPromoteNoCopyPartResult(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 3))
	d := digest.FromBytes(body)
	for _, tc := range []struct {
		name          string
		hashInService bool
		partSize      int64
	}{
		{name: "store hashed"},
		{name: "service hashed", hashInService: true},
		{name: "service hashed, many parts", hashInService: true, partSize: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &copyStore{body: body, noCopyResult: true}
			srv := httptest.NewServer(f)
			defer srv.Close()
			cfg := testConfig()
			cfg.Endpoint = srv.URL
			cfg.HashInService = tc.hashInService
			s := newTestStore(t, cfg)
			if tc.partSize > 0 {
				s.maxPartCopy, s.copyPartSize = tc.partSize, tc.partSize
			}
			p, err := s.Promote(context.Background(), NewStage(), d, int64(len(body)))
			if !errors.Is(err, errNoCopyResult) {
				t.Fatalf("promote: %+v, %v; want errNoCopyResult", p, err)
			}
			if f.completed != 0 || f.aborted != 1 || f.copies != 1 {
				t.Fatalf("%d copies, %d completed, %d aborted; want 1, 0 and 1", f.copies, f.completed, f.aborted)
			}
			if s.noStoreHash.Load() {
				t.Fatal("a missing CopyPartResult switched the store to hashing in the service")
			}
		})
	}
	// The fake's checksums work: the same store, answering with a result,
	// promotes through the store path.
	f := &copyStore{body: body, checksum: true}
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := testConfig()
	cfg.Endpoint = srv.URL
	p, err := newTestStore(t, cfg).Promote(context.Background(), NewStage(), d, int64(len(body)))
	if err != nil || p.Outcome != Promoted || p.HashedBy != HashedByStore {
		t.Fatalf("promote with a result: %+v, %v; want promoted, hashed by the store", p, err)
	}
}

// Known gap, pinned on purpose: MD5 isn't collision resistant. On a store that
// ignores CopySourceIfMatch, someone holding the presigned PUT who prepared
// two bodies with one MD5, and overwrote the staged object between the hash
// and the copy, gets the second body promoted, unverified. Simulated here with
// a store whose ETags are all the first body's MD5. When promotion stops
// relying on MD5 there, this test fails: replace it with one of the fix.
func TestKnownGapPromoteSameMD5Overwrite(t *testing.T) {
	good := []byte(strings.Repeat("0123456789", 3))
	bad := append([]byte(nil), good...)
	bad[0] ^= 0xff
	sum := md5.Sum(good) //nolint:gosec // see the import
	f := &copyStore{body: good, overwrite: bad, etag: func([]byte) string { return hex.EncodeToString(sum[:]) }}
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := testConfig()
	cfg.Endpoint = srv.URL
	cfg.HashInService = true
	s := newTestStore(t, cfg)
	p, err := s.Promote(context.Background(), NewStage(), digest.FromBytes(good), int64(len(good)))
	if err != nil {
		t.Fatal(err)
	}
	if p.Outcome != Promoted || f.completed != 1 {
		t.Fatalf("promote: %+v, %d completed; the gap is closed, so replace this test", p, f.completed)
	}
}

// Known gap, pinned on purpose: a store whose ETags aren't MD5s can't have its
// copies checked, so only CopySourceIfMatch protects them. One that also
// ignored that condition would have an overwritten staged object promoted,
// unverified. No store is known to do both: Spaces ignores the condition, but
// its ETags are MD5s. When promotion stops relying on either, this test fails:
// replace it with one of the fix.
func TestKnownGapPromoteNonMD5ETagsIgnoredCondition(t *testing.T) {
	good := []byte(strings.Repeat("0123456789", 3))
	bad := append([]byte(nil), good...)
	bad[0] ^= 0xff
	f := &copyStore{body: good, overwrite: bad, etag: func(data []byte) string { return fmt.Sprintf("kms-%x", md5.Sum(data)) }} //nolint:gosec // see the import
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := testConfig()
	cfg.Endpoint = srv.URL
	cfg.HashInService = true
	s := newTestStore(t, cfg)
	p, err := s.Promote(context.Background(), NewStage(), digest.FromBytes(good), int64(len(good)))
	if err != nil {
		t.Fatal(err)
	}
	if p.Outcome != Promoted || f.completed != 1 {
		t.Fatalf("promote: %+v, %d completed; the gap is closed, so replace this test", p, f.completed)
	}
}

func TestPartMD5s(t *testing.T) {
	sum := func(s string) string {
		b := md5.Sum([]byte(s)) //nolint:gosec // see the import
		return hex.EncodeToString(b[:])
	}
	w := &partMD5s{size: 4}
	for _, chunk := range []string{"ab", "cdefghij", "k"} {
		if n, err := w.Write([]byte(chunk)); err != nil || n != len(chunk) {
			t.Fatalf("Write(%q) = %d, %v", chunk, n, err)
		}
	}
	got := w.sums()
	want := []string{sum("abcd"), sum("efgh"), sum("ijk")}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sums %v, want %v", got, want)
	}
	exact := &partMD5s{size: 2}
	_, _ = exact.Write([]byte("abcd"))
	if got := exact.sums(); len(got) != 2 || got[1] != sum("cd") {
		t.Fatalf("exact parts: %v", got)
	}
}
