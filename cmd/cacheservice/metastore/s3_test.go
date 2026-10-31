package metastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
)

// fakeS3 serves GetObject and PutObject for one bucket from MemObjects,
// honoring If-Match and If-None-Match the way S3 does. A key in conflict
// answers PUTs with 409, as AWS S3 does while another conditional write of
// the key is in flight.
type fakeS3 struct {
	mem      *MemObjects
	mu       sync.Mutex
	conflict map[string]int
	headers  []http.Header
}

func newFakeS3() *fakeS3 { return &fakeS3{mem: NewMemObjects(), conflict: map[string]int{}} }

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const bucket = "/cache/"
	if !strings.HasPrefix(r.URL.Path, bucket) {
		http.Error(w, "no such bucket", http.StatusNotFound)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, bucket)
	switch r.Method {
	case http.MethodGet:
		body, etag, err := f.mem.Get(r.Context(), key)
		if errors.Is(err, ErrNotFound) {
			s3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = w.Write(body)
	case http.MethodPut:
		f.mu.Lock()
		f.headers = append(f.headers, r.Header.Clone())
		if f.conflict[key] > 0 {
			f.conflict[key]--
			f.mu.Unlock()
			s3Error(w, http.StatusConflict, "ConditionalRequestConflict")
			return
		}
		f.mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		cond := Condition{IfNoneMatch: r.Header.Get("If-None-Match") == "*", IfMatch: r.Header.Get("If-Match")}
		etag, err := f.mem.Put(r.Context(), key, body, cond)
		if errors.Is(err, ErrPreconditionFailed) {
			s3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		w.Header().Set("ETag", etag)
	case http.MethodDelete:
		_ = f.mem.Delete(r.Context(), key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func newFakeS3Store(t *testing.T, f *fakeS3) *Store {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	s, err := NewS3(context.Background(), blobstore.S3Config{
		Endpoint:    srv.URL,
		Region:      "us-east-1",
		Bucket:      "cache",
		Prefix:      "team",
		PathStyle:   true,
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNewS3Validates(t *testing.T) {
	ctx := context.Background()
	if _, err := NewS3(ctx, blobstore.S3Config{Region: "us-east-1"}); err == nil {
		t.Error("no bucket: no error")
	}
	if _, err := NewS3(ctx, blobstore.S3Config{Bucket: "b"}); err == nil {
		t.Error("no region: no error")
	}
}

func TestS3ObjectsErrors(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3()
	s := newFakeS3Store(t, f)
	o := s.objects

	if _, _, err := o.Get(ctx, "team/missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: %v, want ErrNotFound", err)
	}
	etag, err := o.Put(ctx, "team/k", []byte("1"), Condition{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Put(ctx, "team/k", []byte("2"), Condition{IfNoneMatch: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("412: %v, want ErrPreconditionFailed", err)
	}
	f.conflict["team/k"] = 1
	if _, err := o.Put(ctx, "team/k", []byte("2"), Condition{IfMatch: etag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("409: %v, want ErrPreconditionFailed", err)
	}
	if _, err := o.Put(ctx, "team/k", []byte("2"), Condition{IfMatch: etag}); err != nil {
		t.Fatalf("If-Match after the conflict: %v", err)
	}
	// Each conditional PUT sent the matching header, and only it.
	var sent []string
	for _, h := range f.headers {
		sent = append(sent, fmt.Sprintf("none=%q match=%q", h.Get("If-None-Match"), h.Get("If-Match")))
	}
	want := []string{
		`none="*" match=""`,
		`none="*" match=""`,
		fmt.Sprintf(`none="" match=%q`, etag),
		fmt.Sprintf(`none="" match=%q`, etag),
	}
	if strings.Join(sent, "\n") != strings.Join(want, "\n") {
		t.Fatalf("headers sent:\n%s\nwant:\n%s", strings.Join(sent, "\n"), strings.Join(want, "\n"))
	}
}

func TestS3StoreOverTheWire(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3()
	s := newFakeS3Store(t, f)
	s.sleep = func(context.Context, time.Duration) error { return nil }

	if err := s.VerifyConditionalWrites(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if keys := f.mem.Keys(); len(keys) != 0 {
		t.Fatalf("the check left %v behind", keys)
	}
	r1, err := s.Record(ctx, "s1", chain(10), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	// AWS's 409 on the index is retried like a 412.
	f.conflict["team/s1/index.json"] = 2
	r2, err := s.Record(ctx, "s1", chain(20), "cache-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Numbers[1] != 4 || len(r2.Index.Entries) != 6 {
		t.Fatalf("second record: numbers %v, %d entries", r2.Numbers, len(r2.Index.Entries))
	}
	// A bundle PUT that conflicts, and then finds its key, succeeds.
	f.conflict["team/s1/bundles/"+r1.BundleID+".json"] = 1
	if id, err := s.PutBundle(ctx, "s1", chain(10)); err != nil || id != r1.BundleID {
		t.Fatalf("put an existing bundle: %s, %v", id, err)
	}
	// A bundle PUT that conflicts, and doesn't find its key, tries again.
	b := chain(30)
	id, _, _ := BundleID(b)
	f.conflict["team/s1/bundles/"+id+".json"] = 2
	if got, err := s.PutBundle(ctx, "s1", b); err != nil || got != id {
		t.Fatalf("put a conflicting bundle: %s, %v", got, err)
	}
	if _, err := s.GetBundle(ctx, "s1", id); err != nil {
		t.Fatal(err)
	}
}
