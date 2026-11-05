package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
)

// newScope is a fresh prefix of the test's own, deleted when it ends.
func newScope(t *testing.T) *s3test.Scope {
	t.Helper()
	if os.Getenv(blobstore.EnvEndpoint) == "" {
		t.Skip("CACHESERVICE_S3_ENDPOINT is not set; start MinIO with hack/remote-cache/dev.sh up")
	}
	scope := s3test.NewScope(t, envOr("CACHESERVICE_S3_BUCKET", "dagger-cache"))
	scope.DeleteOnCleanup(t, rootClient(scope.Guard))
	return scope
}

// putUnder writes key, relative to the scope's prefix, with the root user.
func putUnder(t *testing.T, scope *s3test.Scope, key, body string) {
	t.Helper()
	_, err := rootClient(scope.Guard).PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(envOr("CACHESERVICE_S3_BUCKET", "dagger-cache")),
		Key:    aws.String(scope.Prefix + "/" + key),
		Body:   bytes.NewReader([]byte(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// waitExit1 waits for a service that must refuse to start: exit 1, with
// every one of wants in its log.
func (b *binaryTest) waitExit1(wants ...string) {
	b.t.Helper()
	select {
	case <-b.exited:
	case <-time.After(30 * time.Second):
		b.t.Fatal("the service didn't exit")
	}
	var exit *exec.ExitError
	if !errors.As(b.err, &exit) || exit.ExitCode() != 1 {
		b.t.Fatalf("exit %v, want 1", b.err)
	}
	b.logHas(b.t, wants...)
}

// A writer on an empty prefix creates the marker, of layout 2, and starts;
// what it stores is under ns/.
func TestMinIOBinaryWriterClaimsEmptyPrefix(t *testing.T) {
	b := startBinary(t, "-export-end-grace", "100ms")
	b.waitLog(5*time.Second, "prefix claimed")
	b.build()
	b.postSpans("session-1", true)
	b.waitFor(20*time.Second, "the export to be stored", b.stored)
	b.signal()
	b.waitExit(30 * time.Second)
	var marker, other []string
	for _, l := range b.listing() {
		key := strings.TrimPrefix(strings.Fields(l)[0], b.scope.Prefix+"/")
		switch {
		case key == "cacheservice.json":
			marker = append(marker, key)
		case !strings.HasPrefix(key, "ns/"):
			other = append(other, key)
		}
	}
	if len(marker) != 1 || len(other) != 0 {
		t.Fatalf("marker %v, outside ns/ %v", marker, other)
	}
	out, err := rootClient(b.scope.Guard).GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(envOr("CACHESERVICE_S3_BUCKET", "dagger-cache")),
		Key:    aws.String(b.scope.Prefix + "/cacheservice.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	var m struct {
		Layout         int       `json:"layout"`
		Created        time.Time `json:"created"`
		ServiceVersion string    `json:"serviceVersion"`
	}
	if err := json.NewDecoder(out.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Layout != 2 || m.Created.IsZero() || m.ServiceVersion == "" {
		t.Fatalf("marker %+v", m)
	}
}

// Two writers started at once on an empty prefix both start, with one
// marker between them.
func TestMinIOBinaryWritersRaceOnEmptyPrefix(t *testing.T) {
	scope := newScope(t)
	// Started back to back without waiting (noEngine), so their startup
	// checks overlap; then each is waited for on the test's goroutine,
	// since startBinaryWith and waitLog may fail the test.
	bins := make([]*binaryTest, 3)
	for i := range bins {
		bins[i] = startBinaryWith(t, binaryOpts{scope: scope, noEngine: true})
	}
	for _, b := range bins {
		b.waitLog(30*time.Second, "msg=listening")
	}
	claimed := 0
	for _, b := range bins {
		if strings.Contains(b.logs.String(), "prefix claimed") {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("%d writers claimed the prefix, want 1", claimed)
	}
	for _, b := range bins {
		b.signal()
		b.waitExit(30 * time.Second)
	}
	if l := bins[0].listing(); len(l) != 1 || !strings.HasPrefix(l[0], scope.Prefix+"/cacheservice.json ") {
		t.Fatalf("listing %v, want only the marker", l)
	}
}

// A prefix that isn't the service's, or of another layout, is refused by
// both kinds of instance: exit 1, the keys or versions in the log, and the
// bucket listing exactly as before. The old layout (default/index.json,
// blobs/sha256/...) is refused on purpose: there is no migration, and a
// cache is rebuilt by wiping the prefix (README, "Changing layouts").
func TestMinIOBinaryRefusesPrefixNotOwned(t *testing.T) {
	hex := strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name  string
		keys  map[string]string
		wants []string
	}{
		{name: "another layout", keys: map[string]string{"cacheservice.json": `{"layout":7}`},
			wants: []string{"layout version 7", "this service uses layout 2", "Wipe the prefix"}},
		{name: "another application", keys: map[string]string{"backups/db.tar": "x", "reports/a.csv": "y"},
			wants: []string{"backups/db.tar", "reports/a.csv", "a prefix of its own"}},
		{name: "the old layout", keys: map[string]string{"default/index.json": "{}", "blobs/sha256/" + hex: "x"},
			wants: []string{"default/index.json", "blobs/sha256/" + hex, "no ownership marker"}},
	} {
		for _, readOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, read-only %v", tc.name, readOnly), func(t *testing.T) {
				scope := newScope(t)
				for k, v := range tc.keys {
					putUnder(t, scope, k, v)
				}
				before := listScope(t, scope)
				var args []string
				if readOnly {
					args = []string{"-export=false"}
				}
				b := startBinaryWith(t, binaryOpts{scope: scope, noEngine: true}, args...)
				b.waitExit1(append([]string{"ownership check"}, tc.wants...)...)
				if after := listScope(t, scope); !slices.Equal(before, after) {
					t.Fatalf("the bucket changed:\nbefore %v\nafter  %v", before, after)
				}
			})
		}
	}
}

// A read-only instance on an empty prefix starts, with the read-only key,
// says no store exists yet, merges nothing, and writes nothing: the prefix
// is still empty after it stopped.
func TestMinIOBinaryReadOnlyEmptyPrefix(t *testing.T) {
	id, secret := readOnlyKey(t)
	b := startBinaryWith(t, binaryOpts{accessKey: id, secretKey: secret}, "-export=false")
	b.logHas(t, "no store exists under the prefix yet")
	code, body := b.post(fmt.Sprintf("/debug/engines/%d/import", b.conn()))
	if code != 200 {
		t.Fatalf("import: %d %q", code, body)
	}
	if n := b.cache.Merges(); n != 0 {
		t.Fatalf("%d merges", n)
	}
	b.signal()
	b.waitExit(30 * time.Second)
	if l := b.listing(); len(l) != 0 {
		t.Fatalf("listing %v, want empty", l)
	}
}

func listScope(t *testing.T, scope *s3test.Scope) []string {
	t.Helper()
	return (&binaryTest{t: t, scope: scope}).listing()
}

// A prefix whose only content is another writer's multipart upload, in
// progress, is refused by a writer: exit 1 naming the upload, and no marker
// written. The object listing alone would have seen an empty prefix.
func TestMinIOBinaryWriterRefusesPrefixWithUpload(t *testing.T) {
	scope := newScope(t)
	client := rootClient(scope.Guard)
	bucket := envOr("CACHESERVICE_S3_BUCKET", "dagger-cache")
	key := scope.Prefix + "/backups/db.tar"
	up, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: up.UploadId})
	})
	b := startBinaryWith(t, binaryOpts{scope: scope, noEngine: true})
	b.waitExit1("ownership check", "upload:"+key)
	if l := listScope(t, scope); len(l) != 0 {
		t.Fatalf("listing %v, want empty", l)
	}
}
