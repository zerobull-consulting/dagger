package wipe

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
)

// stubS3 is the bucket's delete API: a DeleteObject of a key in denyOne
// answers 403, and a DeleteObjects (a POST with ?delete) answers 403 when
// denyBatch, else deletes everything but the keys in batchErrors, which it
// reports in the response.
type stubS3 struct {
	mu          sync.Mutex
	denyOne     map[string]bool
	denyBatch   bool
	batchErrors map[string]bool
	oneKeys     []string
	// onOne, when set, runs for each DeleteObject, without the lock.
	onOne     func(key string, r *http.Request)
	batchKeys []string
}

func (s *stubS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete && s.onOne != nil {
		s.onOne(strings.TrimPrefix(r.URL.Path, "/bkt/"), r)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deny := func() {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>no</Message></Error>`)
	}
	switch {
	case r.Method == http.MethodDelete:
		key := strings.TrimPrefix(r.URL.Path, "/bkt/")
		s.oneKeys = append(s.oneKeys, key)
		if s.denyOne[key] {
			deny()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
		if s.denyBatch {
			deny()
			return
		}
		body, _ := io.ReadAll(r.Body)
		var errs strings.Builder
		for _, part := range strings.Split(string(body), "<Key>")[1:] {
			key := part[:strings.Index(part, "</Key>")]
			s.batchKeys = append(s.batchKeys, key)
			if s.batchErrors[key] {
				errs.WriteString(`<Error><Key>` + key + `</Key><Code>AccessDenied</Code><Message>no</Message></Error>`)
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult>`+errs.String()+`</DeleteResult>`)
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusNotImplemented)
	}
}

func newStubStore(t *testing.T, s *stubS3) *S3Store {
	t.Helper()
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	client, err := blobstore.NewS3Client(context.Background(), blobstore.S3Config{
		Endpoint:    srv.URL,
		Region:      "us-east-1",
		Bucket:      "bkt",
		PathStyle:   true,
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &S3Store{Client: client, Bucket: "bkt"}
}

// A page mixing a key XML can't carry (deleted on its own) with ordinary
// keys (one DeleteObjects): when the single-key delete fails and then the
// batch request fails, Delete returns the single-key failure with the
// error, and not nil.
func TestS3DeleteBatchFailureKeepsKeyFailures(t *testing.T) {
	stub := &stubS3{denyOne: map[string]bool{"cache/\x01a": true}, denyBatch: true}
	st := newStubStore(t, stub)
	failed, err := st.Delete(context.Background(), []string{"cache/ok1", "cache/\x01a", "cache/ok2"})
	if err == nil || !strings.Contains(err.Error(), "delete 2 objects") {
		t.Fatalf("error %v, want the batch request's", err)
	}
	if len(failed) != 1 || failed[0].Key != "cache/\x01a" || !strings.Contains(failed[0].Reason, "AccessDenied") {
		t.Fatalf("failures %+v, want the single-key one", failed)
	}
	if len(stub.oneKeys) != 1 {
		t.Fatalf("single-key requests %v", stub.oneKeys)
	}
}

// With both requests answering, the failures are the single-key one and the
// batch's per-key errors, and there is no error.
func TestS3DeleteCollectsBothKindsOfFailure(t *testing.T) {
	stub := &stubS3{denyOne: map[string]bool{"cache/\x01a": true}, batchErrors: map[string]bool{"cache/ok2": true}}
	st := newStubStore(t, stub)
	failed, err := st.Delete(context.Background(), []string{"cache/ok1", "cache/\x01a", "cache/ok2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 2 || failed[0].Key != "cache/\x01a" || failed[1].Key != "cache/ok2" {
		t.Fatalf("failures %+v", failed)
	}
}

// The context ending during a later single-key delete: the earlier
// single-key failure still comes back with the error.
func TestS3DeleteContextEndKeepsKeyFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stub := &stubS3{denyOne: map[string]bool{"cache/\x01a": true}, onOne: func(key string, r *http.Request) {
		if key == "cache/\x02b" {
			cancel()
			<-r.Context().Done()
		}
	}}
	st := newStubStore(t, stub)
	failed, err := st.Delete(ctx, []string{"cache/\x01a", "cache/\x02b", "cache/ok"})
	if err == nil {
		t.Fatal("no error")
	}
	if len(failed) != 1 || failed[0].Key != "cache/\x01a" {
		t.Fatalf("failures %+v, want the first single-key one", failed)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.batchKeys) != 0 {
		t.Fatalf("a batch request went out after the context ended: %v", stub.batchKeys)
	}
}
