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

	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3util"
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

// counts is how many times the ancestor's marker was asked for, and the
// keys PUT, read under the lock: a hung request's handler may still run.
func (f *ancestorS3) counts() (int, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ancestorGets, append([]string(nil), f.puts...)
}

func s3Err(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

// ancestorStore is an S3Store on f with the client the command uses, but
// for its retries, which wait for nothing (still 3 attempts), and an HTTP
// client that gives up on a request after 200ms, as a store that
// doesn't answer times out.
func ancestorStore(t *testing.T, f *ancestorS3) *S3Store {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	client, err := blobstore.NewS3Client(context.Background(), blobstore.S3Config{
		Endpoint:    srv.URL,
		Region:      "us-east-1",
		PathStyle:   true,
		Credentials: credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
		HTTPClient:  &http.Client{Timeout: 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	noWait := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 3
		o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		o.RateLimiter = ratelimit.None
	})
	client = s3.New(client.Options(), func(o *s3.Options) { o.Retryer = noWait })
	return &S3Store{Client: client, Bucket: "bkt"}
}

// claimAncestor runs claim of cache/team on f.
func claimAncestor(t *testing.T, f *ancestorS3) (int, string) {
	t.Helper()
	o := opts("claim")
	o.Prefix, o.Confirm = "cache/team", "bkt/cache/team"
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out bytes.Buffer
	code := Run(ctx, o, ancestorStore(t, f), &out, &out)
	return code, out.String()
}

// claim of cache/team, with the S3 client the command uses, refuses when
// the bucket's top marker (an ancestor's) can't be read for a 5xx or a
// request that times out: exit 1, the ancestor's key named, nothing
// written. The SDK's own retries run first (each is asked 3 times). A 404
// or a 403 there is skipped, and the claim goes ahead.
func TestClaimAncestorMarkerErrors(t *testing.T) {
	for name, f := range map[string]*ancestorS3{
		"5xx":     {status: http.StatusInternalServerError, code: "InternalError"},
		"timeout": {hang: true},
	} {
		t.Run(name, func(t *testing.T) {
			code, out := claimAncestor(t, f)
			gets, puts := f.counts()
			if code != ExitError || len(puts) != 0 || !strings.Contains(out, "can't read cacheservice.json") || !strings.Contains(out, "nothing claimed") {
				t.Fatalf("%d, puts %v\n%s", code, puts, out)
			}
			if gets != 3 {
				t.Fatalf("the ancestor's marker was asked for %d times, want 3: the SDK's retries", gets)
			}
		})
	}
	for _, tc := range []struct {
		status int
		code   string
	}{{http.StatusNotFound, "NoSuchKey"}, {http.StatusForbidden, "AccessDenied"}} {
		t.Run(tc.code, func(t *testing.T) {
			f := &ancestorS3{status: tc.status, code: tc.code}
			code, out := claimAncestor(t, f)
			if _, puts := f.counts(); code != ExitOK || len(puts) != 1 || puts[0] != "cache/team/cacheservice.json" {
				t.Fatalf("%d, puts %v\n%s", code, puts, out)
			}
		})
	}
}

// Known gap: a 403 is "access denied", whatever its code, so an ancestor's
// marker that exists but that a store answers with a 403 of another code
// (InvalidObjectState here) is skipped, and the nested claim goes ahead.
// This pins today's behaviour (see the README, "Bucket ownership").
func TestClaimAncestorInvalidObjectStateSkippedKnownGap(t *testing.T) {
	f := &ancestorS3{status: http.StatusForbidden, code: "InvalidObjectState"}
	code, out := claimAncestor(t, f)
	if _, puts := f.counts(); code != ExitOK || len(puts) != 1 || puts[0] != "cache/team/cacheservice.json" {
		t.Fatalf("%d, puts %v\n%s", code, puts, out)
	}
}

// The refusal unwraps to the store's error, and is an ErrAncestorUnread.
func TestClaimAncestorErrorType(t *testing.T) {
	f := &ancestorS3{status: http.StatusServiceUnavailable, code: "SlowDown"}
	_, _, err := ancestorStore(t, f).Claim(context.Background(), "cache/team")
	var unread *ownership.ErrAncestorUnread
	if !errors.As(err, &unread) || unread.Key != "cacheservice.json" || s3util.ErrorCode(err) != "SlowDown" {
		t.Fatalf("%v; want ErrAncestorUnread of cacheservice.json, wrapping SlowDown", err)
	}
}
