package blobstore

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // what the fake's ETags are
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
)

// copyStore is a fake S3 of keyed objects that, like DigitalOcean Spaces,
// ignores CopySourceIfMatch and returns no checksums (unless checksum). Its
// keys are relative to the bucket "cache" (testConfig).
type copyStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	uploads map[string]map[int32][]byte
	// nextUpload numbers the multipart uploads.
	nextUpload int
	// staged is the staged object's key; overwrite replaces its content
	// once something has read it (a GET or a copy from it), as someone
	// holding its presigned PUT might.
	staged    string
	overwrite []byte
	// etag makes the objects' and the parts' ETags something else than
	// their MD5s, as AWS S3 does with SSE-KMS.
	etag      func(data []byte) string
	completed int
	aborted   int
	// partCopies counts UploadPartCopy requests; stagedGets the GETs of
	// the staged object.
	partCopies int
	stagedGets int
	// noCopyResult answers part copies with 200 and no CopyPartResult.
	noCopyResult bool
	// checksum answers part copies with the data's SHA-256, as a store
	// that hashes does.
	checksum bool
}

func newCopyStore(t *testing.T, stage Stage, d digest.Digest, body []byte) *copyStore {
	t.Helper()
	staged, err := newTestStore(t, testConfig()).StagingKey(stage, d)
	if err != nil {
		t.Fatal(err)
	}
	return &copyStore{objects: map[string][]byte{staged: body}, uploads: map[string]map[int32][]byte{}, staged: staged}
}

func (c *copyStore) etagOf(data []byte) string {
	if c.etag != nil {
		return c.etag(data)
	}
	sum := md5.Sum(data) //nolint:gosec // see the import
	return hex.EncodeToString(sum[:])
}

// read is what reading key returns now, and swaps the staged object for
// overwrite once it has been read.
func (c *copyStore) read(key string) ([]byte, bool) {
	data, ok := c.objects[key]
	if ok && key == c.staged && c.overwrite != nil {
		c.objects[key] = c.overwrite
	}
	return data, ok
}

func (c *copyStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/cache/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodHead:
		data, ok := c.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"`+c.etagOf(data)+`"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	case r.Method == http.MethodGet:
		if key == c.staged {
			c.stagedGets++
		}
		data, ok := c.read(key)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(data)
	case r.Method == http.MethodPost && q.Has("uploads"):
		c.nextUpload++
		id := fmt.Sprintf("u%d", c.nextUpload)
		c.uploads[id] = map[int32][]byte{}
		_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		src, err := url.PathUnescape(r.Header.Get("X-Amz-Copy-Source"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		data, ok := c.read(strings.TrimPrefix(src, "cache/"))
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !q.Has("uploadId") {
			c.objects[key] = data
			_, _ = fmt.Fprintf(w, `<CopyObjectResult><ETag>"%s"</ETag></CopyObjectResult>`, c.etagOf(data))
			return
		}
		c.partCopies++
		if rng := r.Header.Get("X-Amz-Copy-Source-Range"); rng != "" {
			var from, to int
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &from, &to); err != nil || to >= len(data) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			data = data[from : to+1]
		}
		var num int32
		_, _ = fmt.Sscan(q.Get("partNumber"), &num)
		c.uploads[q.Get("uploadId")][num] = data
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
		parts := c.uploads[q.Get("uploadId")]
		nums := slices.Sorted(maps.Keys(parts))
		var all []byte
		for _, n := range nums {
			all = append(all, parts[n]...)
		}
		c.objects[key] = all
		delete(c.uploads, q.Get("uploadId"))
		c.completed++
		_, _ = io.WriteString(w, `<CompleteMultipartUploadResult/>`)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		delete(c.uploads, q.Get("uploadId"))
		c.aborted++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete:
		delete(c.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// keys are the fake's object keys, sorted.
func (c *copyStore) keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := slices.Collect(maps.Keys(c.objects))
	sort.Strings(k)
	return k
}

// serviceHashedStore is a store on the server at endpoint that hashes in the
// service, with part copies of partSize when it is set.
func serviceHashedStore(t *testing.T, endpoint string, partSize int64) *S3Store {
	t.Helper()
	cfg := testConfig()
	cfg.Endpoint = endpoint
	cfg.HashInService = true
	s := newTestStore(t, cfg)
	if partSize > 0 {
		s.maxPartCopy, s.copyPartSize = partSize, partSize
	}
	return s
}

// A service-hashed promotion hashes a private copy of the staged object and
// promotes that copy, so a staged object overwritten after it was read never
// reaches the digest key: the digest key holds what was hashed, or nothing.
// It relies on no store condition (the fake ignores CopySourceIfMatch) and
// on no ETag, MD5 or not (zerobull-consulting/dagger#67). The service never
// reads the staged object itself, and leaves no private copy behind.
func TestPromoteHashesPrivateCopy(t *testing.T) {
	good := []byte(strings.Repeat("0123456789", 3))
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xff
	d := digest.FromBytes(good)
	goodMD5 := md5.Sum(good) //nolint:gosec // see the import
	etags := map[string]func([]byte) string{
		"MD5 ETags": nil,
		// AWS S3 with SSE-KMS.
		"ETags aren't MD5s": func(data []byte) string { return fmt.Sprintf("kms-%x", md5.Sum(data)) }, //nolint:gosec // see the import
		// Every body has the good one's MD5: a same-MD5 pair.
		"every ETag the same MD5": func([]byte) string { return hex.EncodeToString(goodMD5[:]) },
	}
	for _, etagName := range slices.Sorted(maps.Keys(etags)) {
		for _, tc := range []struct {
			name        string
			first       []byte
			overwrite   []byte
			partSize    int64
			wantOutcome Outcome
		}{
			{name: "one part", first: good, wantOutcome: Promoted},
			{name: "many parts", first: good, partSize: 10, wantOutcome: Promoted},
			// Overwritten once the private copy has read it: the copy
			// is the good content, and promoted.
			{name: "one part, overwritten after the copy", first: good, overwrite: bad, wantOutcome: Promoted},
			// Overwritten after the private copy's first part: the copy
			// is a mix, which doesn't hash to the digest.
			{name: "many parts, overwritten during the copy", first: good, overwrite: bad, partSize: 10, wantOutcome: Mismatch},
			// Wrong from the start, then overwritten with the right
			// content: the copy is wrong, and stays so.
			{name: "one part, wrong then right", first: bad, overwrite: good, wantOutcome: Mismatch},
		} {
			t.Run(etagName+"/"+tc.name, func(t *testing.T) {
				stage := NewStage()
				f := newCopyStore(t, stage, d, tc.first)
				f.overwrite, f.etag = tc.overwrite, etags[etagName]
				srv := httptest.NewServer(f)
				defer srv.Close()
				s := serviceHashedStore(t, srv.URL, tc.partSize)

				p, err := s.Promote(context.Background(), stage, d, int64(len(good)))
				if err != nil {
					t.Fatal(err)
				}
				if p.Outcome != tc.wantOutcome || p.HashedBy != HashedByService {
					t.Fatalf("promote: %+v, want %s hashed by the service", p, tc.wantOutcome)
				}
				dst, _ := s.Key(d)
				var want []string
				if tc.wantOutcome == Promoted {
					want = []string{dst}
					if got := f.objects[dst]; !bytes.Equal(got, good) {
						t.Fatalf("the digest key holds %q, want %q", got, good)
					}
				}
				// The staged object and the private copy are gone.
				if got := f.keys(); !slices.Equal(got, want) {
					t.Fatalf("keys left %v, want %v", got, want)
				}
				if f.stagedGets != 0 {
					t.Fatalf("the service read the staged object %d times; it reads only its private copy", f.stagedGets)
				}
				if len(f.uploads) != 0 {
					t.Fatalf("multipart uploads left: %v", slices.Collect(maps.Keys(f.uploads)))
				}
			})
		}
	}
}

// A same-MD5 pair swapped into the staged object after the hash, on a store
// that ignores CopySourceIfMatch, used to be promoted unverified
// (TestKnownGapPromoteSameMD5Overwrite, zerobull-consulting/dagger#67). Now
// the swap after the service's read leaves the private copy, which is what
// was hashed and what is promoted, untouched.
func TestPromoteSameMD5Overwrite(t *testing.T) {
	good := []byte(strings.Repeat("0123456789", 3))
	bad := append([]byte(nil), good...)
	bad[0] ^= 0xff
	sum := md5.Sum(good) //nolint:gosec // see the import
	d := digest.FromBytes(good)
	stage := NewStage()
	f := newCopyStore(t, stage, d, good)
	f.overwrite, f.etag = bad, func([]byte) string { return hex.EncodeToString(sum[:]) }
	srv := httptest.NewServer(f)
	defer srv.Close()
	s := serviceHashedStore(t, srv.URL, 0)
	p, err := s.Promote(context.Background(), stage, d, int64(len(good)))
	if err != nil {
		t.Fatal(err)
	}
	dst, _ := s.Key(d)
	if p.Outcome != Promoted || !bytes.Equal(f.objects[dst], good) {
		t.Fatalf("promote: %+v; the digest key holds %q, want %q", p, f.objects[dst], good)
	}
	if staged := f.objects[f.staged]; staged != nil {
		t.Fatalf("the staged object is left: %q", staged)
	}
}

// A store whose ETags aren't MD5s and that ignores CopySourceIfMatch used to
// have an overwritten staged object promoted unverified
// (TestKnownGapPromoteNonMD5ETagsIgnoredCondition,
// zerobull-consulting/dagger#67). Now the content promoted is the private
// copy's, which the service hashed, whatever the staged object holds since.
func TestPromoteNonMD5ETagsIgnoredCondition(t *testing.T) {
	good := []byte(strings.Repeat("0123456789", 3))
	bad := append([]byte(nil), good...)
	bad[0] ^= 0xff
	d := digest.FromBytes(good)
	for _, partSize := range []int64{0, 10} {
		t.Run(fmt.Sprintf("part size %d", partSize), func(t *testing.T) {
			stage := NewStage()
			f := newCopyStore(t, stage, d, good)
			f.etag = func(data []byte) string { return fmt.Sprintf("kms-%x", md5.Sum(data)) } //nolint:gosec // see the import
			// Overwrite the staged object when the service starts the copy
			// to the digest key: after the hash, before any byte is copied
			// there.
			dst, _ := newTestStore(t, testConfig()).Key(d)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Query().Has("uploads") && strings.HasSuffix(r.URL.Path, dst) {
					f.mu.Lock()
					f.objects[f.staged] = bad
					f.mu.Unlock()
				}
				f.ServeHTTP(w, r)
			}))
			defer srv.Close()
			s := serviceHashedStore(t, srv.URL, partSize)
			p, err := s.Promote(context.Background(), stage, d, int64(len(good)))
			if err != nil {
				t.Fatal(err)
			}
			// dst is s.Key(d): the key depends on the namespace, not the endpoint.
			if p.Outcome != Promoted || !bytes.Equal(f.objects[dst], good) {
				t.Fatalf("promote: %+v; the digest key holds %q, want %q", p, f.objects[dst], good)
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
		// The private copy is a CopyObject; the copy to the digest key's
		// first part fails.
		{name: "service hashed", hashInService: true},
		// The private copy's first part fails.
		{name: "service hashed, many parts", hashInService: true, partSize: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stage := NewStage()
			f := newCopyStore(t, stage, d, body)
			f.noCopyResult = true
			srv := httptest.NewServer(f)
			defer srv.Close()
			cfg := testConfig()
			cfg.Endpoint = srv.URL
			cfg.HashInService = tc.hashInService
			s := newTestStore(t, cfg)
			if tc.partSize > 0 {
				s.maxPartCopy, s.copyPartSize = tc.partSize, tc.partSize
			}
			p, err := s.Promote(context.Background(), stage, d, int64(len(body)))
			if !errors.Is(err, errNoCopyResult) {
				t.Fatalf("promote: %+v, %v; want errNoCopyResult", p, err)
			}
			if f.completed != 0 || f.aborted != 1 || f.partCopies != 1 {
				t.Fatalf("%d part copies, %d completed, %d aborted; want 1, 0 and 1", f.partCopies, f.completed, f.aborted)
			}
			if s.noStoreHash.Load() {
				t.Fatal("a missing CopyPartResult switched the store to hashing in the service")
			}
			// Only the staged object is left: a store error leaves it to
			// the lifecycle rule, and the private copy is deleted.
			if got := f.keys(); !slices.Equal(got, []string{f.staged}) {
				t.Fatalf("keys left %v", got)
			}
		})
	}
	// The fake's checksums work: the same store, answering with a result,
	// promotes through the store path.
	stage := NewStage()
	f := newCopyStore(t, stage, d, body)
	f.checksum = true
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := testConfig()
	cfg.Endpoint = srv.URL
	s := newTestStore(t, cfg)
	p, err := s.Promote(context.Background(), stage, d, int64(len(body)))
	if err != nil || p.Outcome != Promoted || p.HashedBy != HashedByStore {
		t.Fatalf("promote with a result: %+v, %v; want promoted, hashed by the store", p, err)
	}
}
