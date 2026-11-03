package s3test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestCleanPrefix(t *testing.T) {
	for _, bad := range []string{"", "/", "//", " ", " / ", "a/../b", ".."} {
		if p, err := CleanPrefix(bad); err == nil {
			t.Errorf("CleanPrefix(%q) = %q, want an error", bad, p)
		}
	}
	for in, want := range map[string]string{"test": "test", "/test/": "test", "a/b/": "a/b"} {
		if got, err := CleanPrefix(in); err != nil || got != want {
			t.Errorf("CleanPrefix(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestRunRootRefusesEmptyPrefix(t *testing.T) {
	t.Setenv(EnvPrefix, "/")
	fatal := &recordingTB{TB: t}
	func() {
		defer func() { _ = recover() }()
		RunRoot(fatal)
	}()
	if !fatal.failed {
		t.Fatal("RunRoot took the prefix /")
	}

	t.Setenv(EnvPrefix, "test")
	first := RunRoot(t)
	if !strings.HasPrefix(first, "test/") || first == "test/" || RunRoot(t) != first {
		t.Fatalf("RunRoot = %q, then %q; want one root under test/", first, RunRoot(t))
	}
}

// recordingTB records a Fatal instead of failing the real test.
type recordingTB struct {
	testing.TB
	failed bool
}

func (r *recordingTB) Fatal(...any) {
	r.failed = true
	panic("fatal")
}

// fakeStore answers everything with an empty success, and records what
// reached it.
type fakeStore struct {
	reached []string
}

func (f *fakeStore) Do(r *http.Request) (*http.Response, error) {
	f.reached = append(f.reached, r.Method+" "+r.Host+r.URL.Path)
	body := ""
	switch {
	case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
		body = `<DeleteResult/>`
	case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
		body = `<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>`
	case r.Method == http.MethodPost:
		body = `<CompleteMultipartUploadResult/>`
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    r,
	}, nil
}

const (
	testBucket = "bkt"
	testRoot   = "test/run-1"
)

// The guard sees the requests the SDK really makes, for both ways of
// addressing the bucket: every way of changing a key outside the root, or the
// bucket, is refused before it reaches the store, and the same ways of
// changing keys under the root go through.
func TestGuardWithSDK(t *testing.T) {
	ctx := context.Background()
	inRoot := testRoot + "/a/b"
	sibling := "test/run-10/a" // shares the root's first characters
	copySource := aws.String(testBucket + "/" + inRoot)

	ops := func(c *s3.Client, key string) map[string]func() error {
		k, b := aws.String(key), aws.String(testBucket)
		return map[string]func() error{
			"PutObject": func() error {
				_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: k, Body: bytes.NewReader([]byte("x"))})
				return err
			},
			"DeleteObject": func() error {
				_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k})
				return err
			},
			"CopyObject": func() error {
				_, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: k, CopySource: copySource})
				return err
			},
			"CreateMultipartUpload": func() error {
				_, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: k})
				return err
			},
			"UploadPartCopy": func() error {
				_, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: b, Key: k, UploadId: aws.String("u"), PartNumber: aws.Int32(1), CopySource: copySource})
				return err
			},
			"CompleteMultipartUpload": func() error {
				_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: b, Key: k, UploadId: aws.String("u")})
				return err
			},
			"AbortMultipartUpload": func() error {
				_, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: b, Key: k, UploadId: aws.String("u")})
				return err
			},
			"DeleteObjects": func() error {
				_, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b, Delete: &types.Delete{Objects: []types.ObjectIdentifier{
					{Key: aws.String(inRoot)},
					{Key: k},
				}}})
				return err
			},
		}
	}

	for _, pathStyle := range []bool{true, false} {
		name := "virtual-hosted"
		if pathStyle {
			name = "path-style"
		}
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			g := NewGuard(testBucket, testRoot, false).WithClient(store)
			c := s3.New(s3.Options{
				Region:       "us-east-1",
				BaseEndpoint: aws.String("https://s3.example.test"),
				UsePathStyle: pathStyle,
				Credentials:  credentials.NewStaticCredentialsProvider("id", "secret", ""),
				HTTPClient:   g,
				// The service's settings (blobstore.NewS3Client).
				RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
				ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
			})
			for _, outside := range []string{"elsewhere/key", sibling, testRoot, "key", testRoot + "/../x"} {
				for op, call := range ops(c, outside) {
					if err := call(); err == nil || !strings.Contains(err.Error(), "s3test: refused") {
						t.Errorf("%s of %q: %v, want it refused", op, outside, err)
					}
				}
			}
			if len(store.reached) != 0 {
				t.Fatalf("refused requests reached the store: %v", store.reached)
			}

			allowed := ops(c, inRoot)
			for op, call := range allowed {
				if err := call(); err != nil {
					t.Errorf("%s under the root: %v", op, err)
				}
			}
			if len(store.reached) != len(allowed) {
				t.Fatalf("%d requests reached the store, want %d: %v", len(store.reached), len(allowed), store.reached)
			}
			if touched := g.Touched(); len(touched) == 0 || slices.ContainsFunc(touched, func(k string) bool { return !strings.HasPrefix(k, testRoot+"/") }) {
				t.Fatalf("touched %v, want keys under %s/ only", touched, testRoot)
			}
		})
	}
}

// Bucket configuration: only a disposable bucket's lifecycle can change, and
// nothing else about any bucket can.
func TestGuardBucketConfiguration(t *testing.T) {
	ctx := context.Background()
	b := aws.String(testBucket)
	ops := map[string]func(c *s3.Client) error{
		"PutBucketLifecycleConfiguration": func(c *s3.Client) error {
			_, err := c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: b, LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
				ID: aws.String("r"), Status: types.ExpirationStatusEnabled,
				Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("x/")},
				Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
			}}}})
			return err
		},
		"DeleteBucketLifecycle": func(c *s3.Client) error {
			_, err := c.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: b})
			return err
		},
	}
	never := map[string]func(c *s3.Client) error{
		"DeleteBucket": func(c *s3.Client) error {
			_, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: b})
			return err
		},
		"CreateBucket": func(c *s3.Client) error {
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b})
			return err
		},
		"PutBucketPolicy": func(c *s3.Client) error {
			_, err := c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: b, Policy: aws.String("{}")})
			return err
		},
		"PutBucketVersioning": func(c *s3.Client) error {
			_, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
			return err
		},
		"PutBucketCors": func(c *s3.Client) error {
			_, err := c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: b, CORSConfiguration: &types.CORSConfiguration{CORSRules: []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}}})
			return err
		},
	}
	client := func(g *Guard) *s3.Client {
		return s3.New(s3.Options{
			Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true,
			Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""), HTTPClient: g,
		})
	}

	for _, disposable := range []bool{false, true} {
		store := &fakeStore{}
		c := client(NewGuard(testBucket, testRoot, disposable).WithClient(store))
		for op, call := range ops {
			err := call(c)
			if disposable && err != nil {
				t.Errorf("%s on a disposable bucket: %v", op, err)
			}
			if !disposable && (err == nil || !strings.Contains(err.Error(), "s3test: refused")) {
				t.Errorf("%s on a shared bucket: %v, want it refused", op, err)
			}
		}
		if !disposable && len(store.reached) != 0 {
			t.Errorf("lifecycle requests reached a shared bucket: %v", store.reached)
		}
		store.reached = nil
		for op, call := range never {
			if err := call(c); err == nil || !strings.Contains(err.Error(), "s3test: refused") {
				t.Errorf("%s (disposable %v): %v, want it refused", op, disposable, err)
			}
		}
		if len(store.reached) != 0 {
			t.Errorf("bucket changes reached the store (disposable %v): %v", disposable, store.reached)
		}
	}
}

// Another bucket's keys are refused even under a key that looks like the
// root; reads anywhere go through.
func TestGuardOtherBucketAndReads(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	g := NewGuard(testBucket, testRoot, false).WithClient(store)
	c := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""), HTTPClient: g,
	})
	_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("other"), Key: aws.String(testRoot + "/k"), Body: bytes.NewReader([]byte("x"))})
	if err == nil || !strings.Contains(err.Error(), "s3test: refused") {
		t.Fatalf("PUT to another bucket: %v", err)
	}
	if len(store.reached) != 0 || len(g.Refused()) != 1 {
		t.Fatalf("reached %v, refused %v", store.reached, g.Refused())
	}
	if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(testBucket), Key: aws.String("elsewhere")}); err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	if len(store.reached) != 1 {
		t.Fatalf("the read didn't reach the store: %v", store.reached)
	}
}

// Bound to its endpoint, the guard takes only that host and the bucket's name
// under it. A request for another bucket's host is refused even when its key
// starts with this bucket's name, which would otherwise look path-style.
func TestGuardBoundToTheEndpoint(t *testing.T) {
	g := NewGuard(testBucket, testRoot, false).WithEndpoint("https://s3.example.test")
	for _, tc := range []struct {
		url string
		ok  bool
	}{
		{"https://s3.example.test/bkt/" + testRoot + "/k", true},
		{"https://bkt.s3.example.test/" + testRoot + "/k", true},
		{"https://other.s3.example.test/bkt/" + testRoot + "/k", false},
		{"https://elsewhere.test/bkt/" + testRoot + "/k", false},
		{"https://bkt.elsewhere.test/" + testRoot + "/k", false},
		{"https://bkt.s3.example.test.evil.test/" + testRoot + "/k", false},
	} {
		req, err := http.NewRequest(http.MethodPut, tc.url, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Check(req); (err == nil) != tc.ok {
			t.Errorf("PUT %s: %v, want allowed %v", tc.url, err, tc.ok)
		}
	}
}

// CheckURL looks at presigned URLs, which don't go through a Guard.
func TestCheckURLRefusesOutsideTheRoot(t *testing.T) {
	t.Setenv(EnvPrefix, "test")
	t.Setenv(EnvEndpoint, "https://s3.example.test")
	root := RunRoot(t)
	CheckURL(t, http.MethodPut, "https://s3.example.test/bkt/"+root+"/k?X-Amz-Signature=x", "bkt")
	CheckURL(t, http.MethodPut, "https://bkt.s3.example.test/"+root+"/k?X-Amz-Signature=x", "bkt")
	fatal := &recordingTB{TB: t}
	func() {
		defer func() { _ = recover() }()
		CheckURL(fatal, http.MethodPut, "https://s3.example.test/bkt/test/other/k", "bkt")
	}()
	if !fatal.failed {
		t.Fatal("CheckURL took a key outside the root")
	}
	fatal = &recordingTB{TB: t}
	func() {
		defer func() { _ = recover() }()
		CheckURL(fatal, http.MethodPut, "https://other.s3.example.test/bkt/"+root+"/k", "bkt")
	}()
	if !fatal.failed {
		t.Fatal("CheckURL took another host")
	}
}

// DeletePrefix refuses to empty the whole bucket.
func TestDeletePrefixRefusesTheBucket(t *testing.T) {
	for _, p := range []string{"", "/", "//"} {
		if err := DeletePrefix(context.Background(), nil, testBucket, p); err == nil {
			t.Errorf("DeletePrefix(%q) took it", p)
		}
	}
}

// versionedStore is a fake versioned bucket: a plain delete adds a delete
// marker, and only a delete by version removes anything.
type versionedStore struct {
	mu       sync.Mutex
	versions map[string][]string // key -> version IDs
	markers  map[string][]string
	next     int
}

func (v *versionedStore) Do(r *http.Request) (*http.Response, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	q := r.URL.Query()
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"+testBucket), "/")
	body := ""
	status := http.StatusOK
	switch {
	case r.Method == http.MethodGet && q.Has("versions"):
		var b strings.Builder
		b.WriteString("<ListVersionsResult>")
		for k, ids := range v.versions {
			if strings.HasPrefix(k, q.Get("prefix")) {
				for _, id := range ids {
					b.WriteString("<Version><Key>" + k + "</Key><VersionId>" + id + "</VersionId></Version>")
				}
			}
		}
		for k, ids := range v.markers {
			if strings.HasPrefix(k, q.Get("prefix")) {
				for _, id := range ids {
					b.WriteString("<DeleteMarker><Key>" + k + "</Key><VersionId>" + id + "</VersionId></DeleteMarker>")
				}
			}
		}
		b.WriteString("</ListVersionsResult>")
		body = b.String()
	case r.Method == http.MethodGet && q.Has("uploads"):
		body = "<ListMultipartUploadsResult/>"
	case r.Method == http.MethodGet:
		// Listing objects hides anything behind a delete marker.
		var b strings.Builder
		b.WriteString("<ListBucketResult>")
		for k := range v.versions {
			if strings.HasPrefix(k, q.Get("prefix")) && len(v.markers[k]) == 0 {
				b.WriteString("<Contents><Key>" + k + "</Key></Contents>")
			}
		}
		b.WriteString("</ListBucketResult>")
		body = b.String()
	case r.Method == http.MethodDelete && q.Get("versionId") != "":
		id := q.Get("versionId")
		v.versions[key] = slices.DeleteFunc(v.versions[key], func(s string) bool { return s == id })
		v.markers[key] = slices.DeleteFunc(v.markers[key], func(s string) bool { return s == id })
		status = http.StatusNoContent
	case r.Method == http.MethodDelete:
		v.next++
		v.markers[key] = append(v.markers[key], fmt.Sprintf("marker-%d", v.next))
		status = http.StatusNoContent
	default:
		status = http.StatusNotImplemented
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

// On a versioned bucket, cleaning up removes the versions and the delete
// markers, not just what listing objects shows, and leaves other keys alone.
func TestDeletePrefixVersionedBucket(t *testing.T) {
	inside := testRoot + "/t/a"
	outside := testRoot + "/t-sibling/a"
	store := &versionedStore{
		versions: map[string][]string{inside: {"v1", "v2"}, outside: {"v9"}},
		markers:  map[string][]string{testRoot + "/t/gone": {"m1"}},
	}
	c := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
		HTTPClient:  NewGuard(testBucket, testRoot, false).WithClient(store),
	})
	if err := DeletePrefix(context.Background(), c, testBucket, testRoot+"/t"); err != nil {
		t.Fatal(err)
	}
	if left := store.versions[inside]; len(left) != 0 {
		t.Errorf("versions left under the prefix: %v", left)
	}
	for k, ids := range store.markers {
		if strings.HasPrefix(k, testRoot+"/t/") && len(ids) != 0 {
			t.Errorf("delete markers left at %s: %v", k, ids)
		}
	}
	if got := store.versions[outside]; len(got) != 1 {
		t.Errorf("the sibling's versions: %v", got)
	}
}

// stuckUploads is a store whose aborts answer success and do nothing.
type stuckUploads struct{ key string }

func (u stuckUploads) Do(r *http.Request) (*http.Response, error) {
	q := r.URL.Query()
	body := "<ListBucketResult/>"
	switch {
	case q.Has("uploads"):
		body = "<ListMultipartUploadsResult><Upload><Key>" + u.key + "</Key><UploadId>u1</UploadId></Upload></ListMultipartUploadsResult>"
	case q.Has("versions"):
		body = "<ListVersionsResult/>"
	case r.Method == http.MethodDelete:
		return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody, Request: r}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

// An upload still there after its abort is left behind, and billed: cleaning
// up says so.
func TestDeletePrefixReportsUploadsLeft(t *testing.T) {
	c := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
		HTTPClient:  NewGuard(testBucket, testRoot, false).WithClient(stuckUploads{key: testRoot + "/t/upload"}),
	})
	err := DeletePrefix(context.Background(), c, testBucket, testRoot+"/t")
	if err == nil || !strings.Contains(err.Error(), "uploads left") {
		t.Fatalf("DeletePrefix: %v, want it to report the upload left", err)
	}
}

// External mode without a bucket would otherwise skip every real-store test,
// and pass having tested nothing.
func TestDevShExternalNeedsItsBucket(t *testing.T) {
	script := filepath.Join("..", "..", "..", "..", "hack", "remote-cache", "dev.sh")
	full := map[string]string{
		"CACHESERVICE_S3_ENDPOINT": "https://s3.example.test",
		"CACHESERVICE_S3_BUCKET":   "bkt",
		"CACHESERVICE_S3_PREFIX":   "test",
	}
	for _, command := range []string{"test", "e2e"} {
		for missing := range full {
			t.Run(command+" without "+missing, func(t *testing.T) {
				cmd := exec.Command("bash", script, command)
				cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CACHESERVICE_TEST_EXTERNAL=1"}
				for name, value := range full {
					if name != missing {
						cmd.Env = append(cmd.Env, name+"="+value)
					}
				}
				out, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(string(out), missing) {
					t.Fatalf("%v\n%s", err, out)
				}
			})
		}
	}
}
