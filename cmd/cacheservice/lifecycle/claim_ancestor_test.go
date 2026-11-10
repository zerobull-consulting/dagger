package lifecycle

import (
	"bytes"
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
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// ancestorS3 is a bucket, over the wire, holding nothing under cache/team/,
// whose answer to a GET of the bucket's top marker (cacheservice.json, an
// ancestor of cache/team) is set by the test: a status and code, or, with
// hang, no answer until the request ends. It answers the listings a claim
// makes with nothing, and records the PUTs.
type ancestorS3 struct {
	status int
	code   string
	hang   bool

	mu           sync.Mutex
	ancestorGets int
	puts         []string
}

func (f *ancestorS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const bucket = "/bkt"
	if !strings.HasPrefix(r.URL.Path, bucket) {
		s3Err(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, bucket), "/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && key == "" && q.Has("uploads"):
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>bkt</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
	case r.Method == http.MethodGet && key == "":
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`)
	case r.Method == http.MethodGet && key == "cacheservice.json":
		f.mu.Lock()
		f.ancestorGets++
		f.mu.Unlock()
		if f.hang {
			<-r.Context().Done()
			return
		}
		s3Err(w, f.status, f.code)
	case r.Method == http.MethodGet:
		s3Err(w, http.StatusNotFound, "NoSuchKey")
	case r.Method == http.MethodPut:
		_, _ = io.Copy(io.Discard, r.Body)
		f.mu.Lock()
		f.puts = append(f.puts, key)
		f.mu.Unlock()
		w.Header().Set("ETag", `"e"`)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func s3Err(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func ancestorStore(t *testing.T, f *ancestorS3) *S3Store {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	client, err := blobstore.NewS3Client(context.Background(), blobstore.S3Config{
		Endpoint:    srv.URL,
		Region:      "us-east-1",
		PathStyle:   true,
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &S3Store{Client: client, Bucket: "bkt"}
}

// claim of cache/team, with the S3 client the command uses, refuses when
// the bucket's top marker (an ancestor's) can't be read for a 5xx or a
// timeout: exit 1, the ancestor's key named, nothing written. The SDK's
// own retries run first (the 5xx is asked again). A 404 or a 403 there
// is skipped, and the claim goes ahead.
func TestClaimAncestorMarkerErrors(t *testing.T) {
	claim := func(t *testing.T, f *ancestorS3, timeout time.Duration) (int, string) {
		t.Helper()
		o := opts("claim")
		o.Prefix, o.Confirm = "cache/team", "bkt/cache/team"
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		var out bytes.Buffer
		code := Run(ctx, o, ancestorStore(t, f), &out, &out)
		return code, out.String()
	}
	t.Run("5xx", func(t *testing.T) {
		f := &ancestorS3{status: http.StatusInternalServerError, code: "InternalError"}
		code, out := claim(t, f, time.Minute)
		if code != ExitError || len(f.puts) != 0 || !strings.Contains(out, "can't read cacheservice.json") || !strings.Contains(out, "nothing claimed") {
			t.Fatalf("%d, puts %v\n%s", code, f.puts, out)
		}
		if f.ancestorGets < 2 {
			t.Fatalf("the ancestor's marker was asked for %d times: the SDK didn't retry", f.ancestorGets)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		f := &ancestorS3{hang: true}
		code, out := claim(t, f, 2*time.Second)
		if code != ExitError || len(f.puts) != 0 || !strings.Contains(out, "can't read cacheservice.json") || !strings.Contains(out, "nothing claimed") {
			t.Fatalf("%d, puts %v\n%s", code, f.puts, out)
		}
	})
	for _, tc := range []struct {
		status int
		code   string
	}{{http.StatusNotFound, "NoSuchKey"}, {http.StatusForbidden, "AccessDenied"}} {
		t.Run(tc.code, func(t *testing.T) {
			f := &ancestorS3{status: tc.status, code: tc.code}
			code, out := claim(t, f, time.Minute)
			if code != ExitOK || len(f.puts) != 1 || f.puts[0] != "cache/team/cacheservice.json" {
				t.Fatalf("%d, puts %v\n%s", code, f.puts, out)
			}
		})
	}
}

// The refusal unwraps to the store's error, and is an ErrAncestorUnread.
func TestClaimAncestorErrorType(t *testing.T) {
	f := &ancestorS3{status: http.StatusServiceUnavailable, code: "SlowDown"}
	_, _, err := ancestorStore(t, f).Claim(context.Background(), "cache/team")
	var unread *ownership.ErrAncestorUnread
	if !errors.As(err, &unread) || unread.Key != "cacheservice.json" || errorCode(err) != "SlowDown" {
		t.Fatalf("%v; want ErrAncestorUnread of cacheservice.json, wrapping SlowDown", err)
	}
}
