package main

// -export=false, a read-only instance (zerobull-consulting/dagger#65): the
// flag handling in main, and the service with a read-only key against MinIO.
// The flag tests run without a store: what they check happens before
// anything connects to the bucket. The MinIO tests skip unless
// CACHESERVICE_S3_ENDPOINT is set, and the read-only key's unless
// CACHESERVICE_TEST_READONLY_ACCESS_KEY_ID is too; `hack/remote-cache/dev.sh
// test` starts MinIO, creates the read-only user, and runs them.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// The combinations -export=false can't honor.
func TestCheckReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name             string
		exports, imports bool
		noStore          bool
		gcInterval       time.Duration
		set              []string
		wantErr          []string
	}{
		{name: "defaults", exports: true, imports: true, gcInterval: gc.DefaultInterval},
		{name: "import off", exports: true, imports: false, gcInterval: gc.DefaultInterval},
		{name: "export off, default gc interval", imports: true, gcInterval: gc.DefaultInterval},
		{name: "export off, explicit gc interval 0", imports: true, set: []string{"gc-interval"}},
		{name: "export off, explicit gc interval", imports: true, gcInterval: time.Hour, set: []string{"gc-interval"}, wantErr: []string{"-gc-interval=1h0m0s", "-export=false"}},
		{name: "both off", wantErr: []string{"-export=false", "-import=false"}},
		{name: "both off, explicit gc interval", gcInterval: time.Hour, set: []string{"gc-interval"}, wantErr: []string{"-export=false", "-import=false"}},
		{name: "export off, no bucket", imports: true, noStore: true, gcInterval: gc.DefaultInterval, wantErr: []string{"-export=false", blobstore.EnvBucket}},
		{name: "defaults, no bucket", exports: true, imports: true, noStore: true, gcInterval: gc.DefaultInterval},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := map[string]bool{}
			for _, f := range tc.set {
				set[f] = true
			}
			err := checkReadOnly(tc.exports, tc.imports, !tc.noStore, tc.gcInterval, set)
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("taken")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q doesn't name %s", err, want)
				}
			}
		})
	}
}

// countingStore is a TCP listener posing as the store's endpoint, that
// counts the connections it gets.
func countingStore(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var n atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			c.Close()
		}
	}()
	return "http://" + ln.Addr().String(), &n
}

// The startup errors of -export=false exit 2, name both flags, and come
// before anything connects to the bucket: the store's endpoint gets no
// connection.
func TestBinaryReadOnlyStartupErrors(t *testing.T) {
	bin := buildBinary(t)
	for _, tc := range []struct {
		name     string
		args     []string
		want     []string
		noBucket bool
	}{
		{"import off too", []string{"-export=false", "-import=false"}, []string{"-export=false", "-import=false"}, false},
		{"explicit gc interval", []string{"-gc-interval=1h", "-export=false"}, []string{"-gc-interval=1h0m0s", "-export=false"}, false},
		{"no bucket", []string{"-export=false"}, []string{"-export=false", blobstore.EnvBucket}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, conns := countingStore(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, append([]string{"-listen", anyPort, "-debug-listen", ""}, tc.args...)...)
			var environ []string
			for _, kv := range os.Environ() {
				if !strings.HasPrefix(kv, "CACHESERVICE_S3_") {
					environ = append(environ, kv)
				}
			}
			bucket := "dagger-cache"
			if tc.noBucket {
				bucket = ""
			}
			cmd.Env = append(environ,
				blobstore.EnvEndpoint+"="+endpoint,
				blobstore.EnvBucket+"="+bucket,
				blobstore.EnvPrefix+"=test",
				blobstore.EnvRegion+"=us-east-1",
				"CACHESERVICE_S3_ACCESS_KEY_ID=x",
				"CACHESERVICE_S3_SECRET_ACCESS_KEY=y",
			)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 2 {
				t.Fatalf("exit %v, want 2; log:\n%s", err, out)
			}
			for _, want := range tc.want {
				if !strings.Contains(string(out), want) {
					t.Errorf("the error doesn't name %s:\n%s", want, out)
				}
			}
			if n := conns.Load(); n != 0 {
				t.Errorf("the store got %d connections before the service exited", n)
			}
		})
	}
}

// -export=false with the default -gc-interval starts, with GC off: the
// default alone isn't an error. With the root key here; the read-only key's
// test is TestMinIOBinaryReadOnlyKey.
func TestMinIOBinaryReadOnlyDefaultGCInterval(t *testing.T) {
	b := startBinaryWith(t, binaryOpts{gcDefault: true}, "-export=false")
	b.waitLog(10*time.Second, "msg=mode export=off import=on gc=off readOnly=true")
	for _, want := range []string{"msg=\"gc off\"", "conditional write check skipped"} {
		if !strings.Contains(b.logs.String(), want) {
			t.Errorf("the log has no %q", want)
		}
	}
	if code, body := b.post("/debug/gc"); code != http.StatusConflict || !strings.Contains(body, "read-only (-export=false)") {
		t.Errorf("POST /debug/gc: %d %q, want 409, read-only", code, body)
	}
}

// The defaults, and -import=false alone, start as before: export path on,
// with the conditional write check and scheduled GC.
func TestMinIOBinaryDefaultsStartAsBefore(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		mode string
	}{
		{"defaults", nil, "msg=mode export=on import=on gc=on readOnly=false"},
		{"import off", []string{"-import=false"}, "msg=mode export=on import=off gc=on readOnly=false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := startBinaryWith(t, binaryOpts{gcDefault: true}, tc.args...)
			b.waitLog(10*time.Second, tc.mode)
			logs := b.logs.String()
			for _, want := range []string{"msg=\"export path on\"", "msg=\"gc on\" interval=" + gc.DefaultInterval.String()} {
				if !strings.Contains(logs, want) {
					t.Errorf("the log has no %q", want)
				}
			}
			if strings.Contains(logs, "conditional write check skipped") {
				t.Error("the conditional write check was skipped")
			}
		})
	}
}

// readOnlyKey is the MinIO user dev.sh creates, whose policy allows only
// s3:GetObject and s3:ListBucket under the test prefix.
func readOnlyKey(t *testing.T) (string, string) {
	t.Helper()
	id, secret := os.Getenv("CACHESERVICE_TEST_READONLY_ACCESS_KEY_ID"), os.Getenv("CACHESERVICE_TEST_READONLY_SECRET_ACCESS_KEY")
	if os.Getenv(blobstore.EnvEndpoint) == "" {
		t.Skip("CACHESERVICE_S3_ENDPOINT is not set; start MinIO with hack/remote-cache/dev.sh up")
	}
	if id == "" || secret == "" {
		t.Skip("CACHESERVICE_TEST_READONLY_ACCESS_KEY_ID is not set: hack/remote-cache/dev.sh test creates a read-only MinIO user")
	}
	return id, secret
}

// listing is every object under the scope's prefix, with its size and ETag.
func (b *binaryTest) listing() []string {
	b.t.Helper()
	client := rootClient(b.scope.Guard)
	var out []string
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(envOr(blobstore.EnvBucket, "dagger-cache")),
		Prefix: aws.String(b.scope.Prefix + "/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			b.t.Fatal(err)
		}
		for _, o := range page.Contents {
			out = append(out, fmt.Sprintf("%s %d %s", aws.ToString(o.Key), aws.ToInt64(o.Size), aws.ToString(o.ETag)))
		}
	}
	slices.Sort(out)
	return out
}

func (b *binaryTest) post(path string) (int, string) {
	b.t.Helper()
	resp, err := http.Post(b.debug+path, "application/json", nil)
	if err != nil {
		b.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// conn is the connection number of the service's one engine.
func (b *binaryTest) conn() uint64 {
	b.t.Helper()
	var engines []server.EngineInfo
	if !b.getJSON("/debug/engines", &engines) || len(engines) != 1 {
		b.t.Fatalf("engines: %+v", engines)
	}
	return engines[0].Conn
}

// The read-only key against MinIO: a writer stores A's build, and a service
// with -export=false and a key that can only read merges it into a fresh
// engine B, offers its parts and renews their addresses. B's session then
// ends and its quiet period passes, and nothing in the bucket changed. This
// is the test that proves the README's permissions list for a read-only key.
func TestMinIOBinaryReadOnlyKey(t *testing.T) {
	id, secret := readOnlyKey(t)
	writer := startBinary(t, "-export-end-grace", "100ms")
	writer.build()
	writer.postSpans("session-1", true)
	writer.waitFor(20*time.Second, "the writer's export to be stored", writer.stored)
	// Stopped, after its exports are stored, so nothing else writes.
	writer.signal()
	writer.waitExit(30 * time.Second)
	before := writer.listing()

	b := fakeengine.New("cache-b", 1)
	ro := startBinaryWith(t, binaryOpts{scope: writer.scope, accessKey: id, secretKey: secret, gcDefault: true, cache: b},
		"-export=false", "-export-quiet", "100ms", "-export-end-grace", "100ms")
	ro.waitLog(10*time.Second, "msg=mode export=off import=on gc=off readOnly=true")
	conn := ro.conn()
	ro.waitFor(20*time.Second, "the import path to merge A's build", func() bool {
		var st importer.State
		return ro.getJSON("/debug/engines/"+strconv.FormatUint(conn, 10)+"/imports", &st) && len(st.Merged) == 1
	})
	var e fakeengine.Entry
	ro.waitFor(10*time.Second, "an offer of withExec(make)", func() bool {
		var ok bool
		e, ok = b.Lookup("withExec(make)")
		return ok && e.Offer != nil
	})
	if ok, err := b.Renew(context.Background(), e.Number); err != nil || !ok {
		t.Fatalf("renew with the read-only key: %v, %v", ok, err)
	}
	if _, downloaded, err := b.Pull(context.Background(), nil, e.Number); err != nil || !downloaded {
		t.Fatalf("pull with renewed addresses: downloaded %v, %v", downloaded, err)
	}

	// B's session: received and dropped. Nothing happening can only be
	// shown by waiting: 2s is 20 times the quiet period and end grace
	// (100ms), past which a writer would have exported it.
	ro.postSpans("session-b", true)
	time.Sleep(2 * time.Second)
	if code, body := ro.post("/debug/engines/" + strconv.FormatUint(conn, 10) + "/export"); code != http.StatusConflict || !strings.Contains(body, "read-only (-export=false)") {
		t.Errorf("export hook: %d %q, want 409, read-only", code, body)
	}
	ro.signal()
	ro.waitExit(30 * time.Second)

	if after := writer.listing(); !slices.Equal(before, after) {
		t.Errorf("the bucket changed:\nbefore %v\nafter  %v", before, after)
	}
	logs := ro.logs.String()
	for _, bad := range []string{"AccessDenied", "level=ERROR", "recording imports failed"} {
		if strings.Contains(logs, bad) {
			t.Errorf("the read-only service's log has %q", bad)
		}
	}
	if !strings.Contains(logs, "drain: nothing to export (read-only: -export=false)") {
		t.Error("the drain didn't say why it had nothing to export")
	}
}

// The same read-only key with the export path on (the default) fails at
// startup, with AccessDenied, and exits 1: on an empty prefix at the
// ownership check (its listing of multipart uploads, before it would
// create the marker), and on a prefix a writer claimed at the conditional
// write check. Intended: it's what stops a pull-request job
// holding a read-only key, but missing -export=false, from silently running
// as a writer whose every export fails.
func TestMinIOBinaryReadOnlyKeyRefusedAsWriter(t *testing.T) {
	id, secret := readOnlyKey(t)
	b := startBinaryWith(t, binaryOpts{accessKey: id, secretKey: secret, noEngine: true})
	b.waitExit1("level=ERROR msg=\"export path\"", "ownership check: list multipart uploads", "AccessDenied")

	writer := startBinary(t)
	writer.signal()
	writer.waitExit(30 * time.Second)
	before := writer.listing()
	b = startBinaryWith(t, binaryOpts{scope: writer.scope, accessKey: id, secretKey: secret, noEngine: true})
	b.waitExit1("level=ERROR msg=\"export path\"", "conditional write check", "AccessDenied")
	if after := writer.listing(); !slices.Equal(before, after) {
		t.Fatalf("the bucket changed:\nbefore %v\nafter  %v", before, after)
	}
}

// The read-only key on a namespace nothing was stored in yet, as on a
// repository's first pull request: the import path reads an index that
// doesn't exist, and takes it as empty only if the store answers 404. AWS
// S3 answers 404 for a missing key only to a key that may list the bucket,
// and 403 otherwise, which is why the README's read-only key has
// s3:ListBucket as well as s3:GetObject. MinIO answers 404 either way (checked
// by hand with a GetObject-only user), so this pins the outcome, not the
// need for s3:ListBucket.
func TestMinIOBinaryReadOnlyKeyEmptyNamespace(t *testing.T) {
	id, secret := readOnlyKey(t)
	b := startBinaryWith(t, binaryOpts{accessKey: id, secretKey: secret, gcDefault: true}, "-export=false")
	conn := b.conn()
	code, body := b.post("/debug/engines/" + strconv.FormatUint(conn, 10) + "/import")
	if code != http.StatusOK {
		t.Fatalf("import with no index: %d %q", code, body)
	}
	if logs := b.logs.String(); strings.Contains(logs, "AccessDenied") {
		t.Fatal("the read-only key was denied reading a missing index")
	}
}
