package main

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
)

// lifecycleWrite is a call that changes a bucket's lifecycle configuration,
// in Go (the SDK) or a shell (the AWS CLI, MinIO's mc).
var lifecycleWrite = regexp.MustCompile(`PutBucketLifecycle|DeleteBucketLifecycle|put-bucket-lifecycle|delete-bucket-lifecycle|mc ilm`)

// Neither the service nor the sidecar action ever writes lifecycle rules:
// only the lifecycle command does (package lifecycle), run by an operator
// with the admin key. A source check of every non-test file, so it runs
// without a store; TestMinIOBinaryNeverWritesLifecycle checks the running
// service's requests.
func TestServiceNeverWritesLifecycleSource(t *testing.T) {
	var found []string
	check := func(root string, skip func(rel string) bool) {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			if d.IsDir() {
				if skip(rel) {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, "_test.sh") || strings.HasSuffix(path, ".md") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if m := lifecycleWrite.Find(b); m != nil {
				found = append(found, path+": "+string(m))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// The lifecycle command writes them by design; the test helpers
	// (internal/s3test, internal/fakeengine) aren't in the service.
	check(".", func(rel string) bool { return rel == "lifecycle" || rel == "internal" })
	check(filepath.Join("..", "..", ".github", "actions", "remote-cache"), func(string) bool { return false })
	check(filepath.Join("..", "..", ".github", "actions", "remote-cache-sidecar"), func(string) bool { return false })
	if len(found) > 0 {
		t.Fatalf("a lifecycle write outside the lifecycle command: %v", found)
	}
}

// The running service never writes lifecycle rules: a writer that exports
// and runs GC reaches the store through a proxy that refuses any change of
// the bucket's lifecycle configuration (as s3test.Guard does without
// CACHESERVICE_TEST_DISPOSABLE_BUCKET). The proxy sees GC read the rules, so
// the service's S3 client did go through it.
func TestMinIOBinaryNeverWritesLifecycle(t *testing.T) {
	endpoint := os.Getenv(blobstore.EnvEndpoint)
	if endpoint == "" {
		t.Skip("CACHESERVICE_S3_ENDPOINT is not set; start MinIO with hack/remote-cache/dev.sh up")
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var reads, writes []string
	// The Host stays the proxy's, which the service signed for: MinIO checks
	// the signature against it (as the e2e's proxy, e2e.StartS3ProxyWith).
	rp := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		r.Out.Host = r.In.Host
	}}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("lifecycle") {
			mu.Lock()
			if r.Method == http.MethodGet {
				reads = append(reads, r.Method+" "+r.URL.String())
			} else {
				writes = append(writes, r.Method+" "+r.URL.String())
				mu.Unlock()
				http.Error(w, "the service may not change lifecycle rules", http.StatusForbidden)
				return
			}
			mu.Unlock()
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)

	b := startBinaryWith(t, binaryOpts{env: []string{blobstore.EnvEndpoint + "=" + proxy.URL}}, "-export-end-grace", "100ms")
	b.waitLog(5*time.Second, "prefix claimed")
	b.build()
	b.postSpans("session-1", true)
	b.waitFor(20*time.Second, "the export to be stored", b.stored)
	if code, body := b.post("/debug/gc"); code != http.StatusOK {
		t.Fatalf("POST /debug/gc: %d %s", code, body)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(writes) > 0 {
		t.Fatalf("the service changed the bucket's lifecycle configuration: %v", writes)
	}
	if len(reads) == 0 {
		t.Fatal("the proxy saw no read of the lifecycle rules: GC didn't go through it")
	}
}

// The built binary dispatches `lifecycle` to the command, with its exit
// codes; a wrong -confirm exits 2 before any request (the endpoint here
// answers nothing).
func TestBinaryLifecycleExitCodes(t *testing.T) {
	bin := buildBinary(t)
	env := append(os.Environ(),
		blobstore.EnvEndpoint+"=http://127.0.0.1:1",
		blobstore.EnvRegion+"=us-east-1",
		blobstore.EnvBucket+"=bkt",
		blobstore.EnvPrefix+"=cache",
		blobstore.EnvAccessKeyID+"=id",
		blobstore.EnvSecretAccessKey+"=a-secret-never-printed",
	)
	for _, tc := range []struct {
		args []string
		want int
	}{
		{[]string{"lifecycle", "apply", "-confirm=bkt/other"}, 2},
		{[]string{"lifecycle", "apply"}, 2},
		{[]string{"lifecycle", "bogus"}, 2},
		{[]string{"lifecycle", "plan", "-timeout", "2s"}, 1}, // nothing answers
	} {
		cmd := exec.Command(bin, tc.args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if code != tc.want {
			t.Errorf("%q: exit %d, want %d\n%s", tc.args, code, tc.want, out)
		}
		if strings.Contains(string(out), "a-secret-never-printed") {
			t.Errorf("%q printed the secret key", tc.args)
		}
	}
}
