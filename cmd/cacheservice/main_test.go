package main

// The binary's stop (#63): SIGTERM drains before the process exits, and a
// second SIGTERM ends the drain at once. In-process tests (server's
// TestDrain*) cover the drain itself; these cover the signal wiring in main,
// which they don't reach. They build the command and run it against MinIO,
// with a fake engine behind the engine's own remotecache.Run. They skip
// unless CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh test`
// starts MinIO and runs them.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/server"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/telemetryattrs"
)

// binaryTest is the built command running against MinIO, under the test's
// own prefix, with a fake engine connected.
type binaryTest struct {
	t     *testing.T
	cmd   *exec.Cmd
	logs  *syncBuffer
	url   string
	debug string
	meta  *metastore.Store
	cache *fakeengine.Cache
	// exited is closed once the process ended; err is then its result.
	exited chan struct{}
	err    error
}

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// buildBinary builds cmd/cacheservice once per test run.
func buildBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "cacheservice-bin-")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "cacheservice")
		out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput()
		if err != nil {
			buildErr = errors.New(string(out))
		}
	})
	if buildErr != nil {
		t.Fatalf("build cmd/cacheservice: %v", buildErr)
	}
	return binPath
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// startBinary runs the command with args, on MinIO under the test's own
// prefix, and connects a fake engine to it.
func startBinary(t *testing.T, args ...string) *binaryTest {
	t.Helper()
	endpoint := os.Getenv(blobstore.EnvEndpoint)
	if endpoint == "" {
		t.Skip("CACHESERVICE_S3_ENDPOINT is not set; start MinIO with hack/remote-cache/dev.sh up")
	}
	bin := buildBinary(t)
	bucket := envOr(blobstore.EnvBucket, "dagger-cache")
	scope := s3test.NewScope(t, bucket)
	client := s3.New(s3.Options{
		HTTPClient:   scope.Guard,
		BaseEndpoint: aws.String(endpoint),
		Region:       envOr(blobstore.EnvRegion, "us-east-1"),
		Credentials: credentials.NewStaticCredentialsProvider(
			envOr("CACHESERVICE_S3_ACCESS_KEY_ID", "dagger-dev"),
			envOr("CACHESERVICE_S3_SECRET_ACCESS_KEY", "dagger-dev-secret"), ""),
		UsePathStyle:               envOr(blobstore.EnvPathStyle, "true") == "true",
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	})
	scope.DeleteOnCleanup(t, client)

	listen, debug := freeAddr(t), freeAddr(t)
	cmd := exec.Command(bin, append([]string{"-listen", listen, "-debug-listen", debug, "-gc-interval", "0"}, args...)...)
	cmd.Env = append(os.Environ(),
		blobstore.EnvBucket+"="+bucket,
		blobstore.EnvPrefix+"="+scope.Prefix,
		blobstore.EnvRegion+"="+envOr(blobstore.EnvRegion, "us-east-1"),
		blobstore.EnvPathStyle+"="+envOr(blobstore.EnvPathStyle, "true"),
		"CACHESERVICE_S3_ACCESS_KEY_ID="+envOr("CACHESERVICE_S3_ACCESS_KEY_ID", "dagger-dev"),
		"CACHESERVICE_S3_SECRET_ACCESS_KEY="+envOr("CACHESERVICE_S3_SECRET_ACCESS_KEY", "dagger-dev-secret"),
	)
	b := &binaryTest{t: t, cmd: cmd, logs: &syncBuffer{}, url: "http://" + listen, debug: "http://" + debug, exited: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = b.logs, b.logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		b.err = cmd.Wait()
		close(b.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-b.exited:
		default:
			_ = cmd.Process.Kill()
			<-b.exited
		}
		if t.Failed() {
			t.Logf("the service's log:\n%s", b.logs.String())
		}
	})

	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bucket, cfg.Prefix, cfg.HTTPClient = bucket, scope.Prefix, scope.Guard
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		cfg.PathStyle = true
	}
	if b.meta, err = metastore.NewS3(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}

	b.waitLog(30*time.Second, `msg=listening addr=`+listen)
	b.cache = fakeengine.New("cache-a", 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: b.cache}, remotecache.Config{URL: b.url, Token: "engine-token", EngineName: "engine-a"})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	b.waitFor(10*time.Second, "the engine to connect", func() bool {
		var engines []server.EngineInfo
		return b.getJSON("/debug/engines", &engines) && len(engines) == 1
	})
	return b
}

func (b *binaryTest) waitFor(timeout time.Duration, what string, cond func() bool) {
	b.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			b.t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (b *binaryTest) waitLog(timeout time.Duration, substr string) {
	b.t.Helper()
	b.waitFor(timeout, "the log line "+substr, func() bool { return strings.Contains(b.logs.String(), substr) })
}

func (b *binaryTest) getJSON(path string, v any) bool {
	resp, err := http.Get(b.debug + path)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(v) == nil
}

// build adds a base image (10) and an exec on it (11) to the engine.
func (b *binaryTest) build() {
	base, exec := bytes.Repeat([]byte("base"), 512), bytes.Repeat([]byte("exec"), 512)
	b.cache.Add(fakeengine.Entry{Number: 10, Layers: [][]byte{base}})
	b.cache.Add(fakeengine.Entry{Number: 11, Deps: []uint64{10}, Layers: [][]byte{base, exec}})
}

// postSpans posts a session's cache spans of 10 and 11, with its end when
// ended, as the engine forwards them.
func (b *binaryTest) postSpans(session string, ended bool) {
	b.t.Helper()
	kv := func(k, v string) *commonpb.KeyValue {
		return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
	}
	strs := func(k string, vs ...string) *commonpb.KeyValue {
		var values []*commonpb.AnyValue
		for _, v := range vs {
			values = append(values, &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}})
		}
		return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: values}}}}
	}
	span := func(n uint64, deps ...string) *tracepb.Span {
		return &tracepb.Span{Name: "call", EndTimeUnixNano: uint64(time.Now().UnixNano()), Attributes: []*commonpb.KeyValue{
			kv(telemetryattrs.CacheResultIDAttr, strconv.FormatUint(n, 10)),
			kv(telemetryattrs.CacheOutcomeAttr, "executed"),
			kv(telemetryattrs.CacheTypeAttr, "Container"),
			kv(telemetryattrs.CacheRetainedAttr, "true"),
			strs(telemetryattrs.CacheDepsAttr, deps...),
			strs(telemetryattrs.CachePartsAttr, `{"part":"fs"}`),
		}}
	}
	spans := []*tracepb.Span{span(10), span(11, "10")}
	if ended {
		spans = append(spans, &tracepb.Span{Name: "session complete", Attributes: []*commonpb.KeyValue{kv(telemetryattrs.CacheSessionSpansAttr, "2")}})
	}
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			kv(telemetryattrs.EngineCacheAttr, b.cache.ID+"/"+strconv.FormatUint(b.cache.Generation, 10)),
			kv(telemetryattrs.EngineSessionAttr, session),
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
	data, err := proto.Marshal(req)
	if err != nil {
		b.t.Fatal(err)
	}
	hreq, err := http.NewRequest(http.MethodPost, b.url+"/v1/traces", bytes.NewReader(data))
	if err != nil {
		b.t.Fatal(err)
	}
	hreq.Header.Set("Content-Type", "application/x-protobuf")
	hreq.SetBasicAuth("engine-token", "")
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		b.t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.t.Fatalf("POST /v1/traces: %d", resp.StatusCode)
	}
}

// stored reports whether the index names entries 10 and 11 of the engine.
func (b *binaryTest) stored() bool {
	b.t.Helper()
	idx, err := b.meta.Index(context.Background(), server.DefaultNamespace)
	if err != nil {
		b.t.Fatal(err)
	}
	c := idx.Caches[b.cache.ID]
	if c == nil {
		return false
	}
	_, ok10 := c.Numbers[10]
	_, ok11 := c.Numbers[11]
	return ok10 && ok11
}

func (b *binaryTest) signal() {
	b.t.Helper()
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		b.t.Fatal(err)
	}
}

// waitExit waits for the process to exit, and fails the test unless it did
// within timeout, with status 0.
func (b *binaryTest) waitExit(timeout time.Duration) time.Duration {
	b.t.Helper()
	start := time.Now()
	select {
	case <-b.exited:
	case <-time.After(timeout):
		b.t.Fatalf("the service didn't exit within %s", timeout)
	}
	if b.err != nil {
		b.t.Fatalf("the service exited with %v", b.err)
	}
	return time.Since(start)
}

// SIGTERM during a session's quiet period: the drain exports it at once, the
// export is stored, and the process exits 0, well within -drain-timeout and
// long before the quiet period would have passed.
func TestMinIOBinaryDrainsOnSIGTERM(t *testing.T) {
	const drainTimeout = 30 * time.Second
	b := startBinary(t, "-export-quiet", "10m", "-drain-timeout", drainTimeout.String())
	b.build()
	b.postSpans("session-1", false)
	b.waitLog(10*time.Second, "otlp traces")
	if b.stored() {
		t.Fatal("exported before the quiet period passed: the test can't tell the drain did it")
	}
	b.signal()
	took := b.waitExit(drainTimeout)
	if !b.stored() {
		t.Fatalf("the quiet session's export wasn't stored before the process exited (%s)", took)
	}
	logs := b.logs.String()
	for _, want := range []string{"msg=stopping signal=terminated", "msg=\"drain: started\"", "msg=\"drain: stored\" cacheID=cache-a roots=2", "msg=\"drain: done\" outcome=complete"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log has no %q", want)
		}
	}
	t.Logf("drained and exited in %s", took)
}

// A second SIGTERM ends the drain at once: the process exits 0 without
// waiting for the export in flight, which isn't stored.
func TestMinIOBinarySecondSIGTERMStopsAtOnce(t *testing.T) {
	b := startBinary(t, "-drain-timeout", "10m")
	b.build()
	release := b.cache.Hold()
	defer release()
	b.postSpans("session-1", true)
	b.waitFor(10*time.Second, "the export to be in flight", func() bool { return b.cache.InFlight() == 1 })
	b.signal()
	b.waitLog(10*time.Second, `msg="drain: started" namespace=default cacheID=cache-a connected=true sessions=0 pending=0 inFlight=2`)
	select {
	case <-b.exited:
		t.Fatal("the service exited on the first SIGTERM with an export in flight and a 10m drain")
	default:
	}
	b.signal()
	took := b.waitExit(10 * time.Second)
	if took > 5*time.Second {
		t.Fatalf("the service took %s to exit after the second SIGTERM", took)
	}
	release()
	if b.stored() {
		t.Fatal("the export the second signal cut off was stored")
	}
	for _, want := range []string{"second signal", "msg=\"drain: done\" outcome=canceled"} {
		if !strings.Contains(b.logs.String(), want) {
			t.Errorf("the log has no %q", want)
		}
	}
}

// -drain-timeout 0 is today's stop: no drain, so the quiet session isn't
// stored (the known gap, #45, for an undrained stop).
func TestMinIOBinaryNoDrain(t *testing.T) {
	b := startBinary(t, "-export-quiet", "10m", "-drain-timeout", "0")
	b.build()
	b.postSpans("session-1", false)
	b.waitLog(10*time.Second, "otlp traces")
	b.signal()
	b.waitExit(10 * time.Second)
	if b.stored() {
		t.Fatal("stored with -drain-timeout 0: the stop drained")
	}
	if strings.Contains(b.logs.String(), "drain:") {
		t.Fatal("the stop with -drain-timeout 0 logged a drain")
	}
}

// syncBuffer is a bytes.Buffer safe for the process's writes and the test's
// reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
