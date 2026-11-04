// Package e2e runs the remote cache end to end with real engines: the cache
// service in this process, engines built from this repo in Docker
// containers, the dagger CLI against them, and MinIO behind a recording
// proxy. See README.md in this directory.
package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
)

// EnvEnable turns the end-to-end tests on: they need Docker, MinIO and a
// few minutes, so they skip unless it is set to 1.
const EnvEnable = "CACHESERVICE_E2E"

// Settings are the run's knobs, from E2E_* variables.
type Settings struct {
	// Dir holds the binaries (Dir/bin) and each run's logs and results.
	Dir string
	// BaseImage is the release engine image the binaries are copied over.
	BaseImage string
	// Image is the engine image's tag.
	Image string
	// SkipBuild reuses the binaries in Dir/bin.
	SkipBuild bool
	// CAFile is a CA bundle the engines should trust, for hosts behind a
	// TLS-intercepting proxy.
	CAFile string
	// Timeout bounds each wait for an export or a merge.
	Timeout time.Duration
	// GetExpiry is how long the replica's presigned GETs last.
	GetExpiry time.Duration
	// ImportPoll is how often the replica looks for entries to merge.
	ImportPoll time.Duration
	// TTL is the cache TTL of the superseded scenario's function.
	TTL time.Duration
	// Keep leaves the engines and the run's objects in the bucket when the
	// test ends.
	Keep bool
}

// SettingsFromEnv reads the settings, with their defaults.
func SettingsFromEnv(t *testing.T) Settings {
	t.Helper()
	str := func(name, def string) string {
		if v := os.Getenv(name); v != "" {
			return v
		}
		return def
	}
	secs := func(name string, def int) time.Duration {
		v := str(name, strconv.Itoa(def))
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("%s=%q: want a number of seconds", name, v)
		}
		return time.Duration(n) * time.Second
	}
	return Settings{
		Dir:        str("E2E_DIR", filepath.Join(os.TempDir(), "dagger-remote-cache-e2e")),
		BaseImage:  str("E2E_BASE_IMAGE", "registry.dagger.io/engine:v1.0.0-beta.22"),
		Image:      str("E2E_IMAGE", "dagger-remote-cache-e2e-engine"),
		SkipBuild:  os.Getenv("E2E_SKIP_BUILD") == "1",
		CAFile:     os.Getenv("E2E_CA_FILE"),
		Timeout:    secs("E2E_TIMEOUT", 180),
		GetExpiry:  secs("E2E_GET_EXPIRY", 5),
		ImportPoll: secs("E2E_IMPORT_POLL", 1),
		TTL:        secs("E2E_TTL", 10),
		Keep:       os.Getenv("E2E_KEEP") == "1",
	}
}

// Env is one end-to-end run: its own bucket prefix, engine image and
// directory, and what it started.
type Env struct {
	// T is the test running now: the run's own test, or a scenario's
	// subtest while Run runs it. Failures go to it.
	T        *testing.T
	Settings Settings
	// RunID names this run's containers, volumes and bucket prefix.
	RunID string
	// RunDir holds this run's logs and results.
	RunDir string
	// Gateway is the Docker bridge gateway: engine containers reach the
	// services and the S3 proxy there.
	Gateway string
	// Scope is the scope the services record entries in.
	Scope string

	// S3Config is the store's, under this run's prefix: S3 and Meta read the
	// bucket with it directly, past the proxy, so what they read isn't
	// recorded as the engines' traffic.
	S3Config blobstore.S3Config
	S3       *s3.Client
	Meta     *metastore.Store
	// Proxy is between MinIO and the services and engines.
	Proxy *S3Proxy

	bin string
	// deleted are the blobs scenarios deleted from MinIO on purpose.
	deleted map[digest.Digest]bool
	// root is the run's own test: what the run starts is cleaned up when it
	// ends, not when a scenario does, since later scenarios reuse it.
	root *testing.T

	mu sync.Mutex
	// services are every service the run started (Services).
	services []*Service
}

// New starts a run: it skips unless EnvEnable is set, builds the engine image
// unless told not to, and starts the S3 proxy.
func New(t *testing.T) *Env {
	t.Helper()
	if os.Getenv(EnvEnable) != "1" {
		t.Skipf("%s is not 1; run with hack/remote-cache/dev.sh e2e", EnvEnable)
	}
	if os.Getenv(blobstore.EnvEndpoint) == "" {
		t.Fatalf("%s is not set: start MinIO with hack/remote-cache/dev.sh up and eval \"$(hack/remote-cache/dev.sh env)\"", blobstore.EnvEndpoint)
	}
	settings := SettingsFromEnv(t)
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	e := &Env{
		T:        t,
		root:     t,
		Settings: settings,
		RunID:    time.Now().UTC().Format("20060102-150405") + "-" + hex.EncodeToString(suffix),
		Scope:    "e2e",
		bin:      filepath.Join(settings.Dir, "bin"),
		deleted:  map[digest.Digest]bool{},
	}
	e.RunDir = filepath.Join(settings.Dir, "runs", e.RunID)
	if err := os.MkdirAll(e.RunDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Logf("run %s: logs and results in %s", e.RunID, e.RunDir)

	gw, err := Docker(context.Background(), "network", "inspect", "bridge", "-f", "{{(index .IPAM.Config 0).Gateway}}")
	if err != nil {
		t.Fatalf("Docker bridge gateway: %v", err)
	}
	e.Gateway = strings.TrimSpace(gw)

	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		cfg.PathStyle = true
	}
	// The run's own prefix: no reset of the bucket, and runs don't see each
	// other's entries. Everything the run does is kept under it (s3test), on
	// its direct client and in the proxy.
	base, err := s3test.CleanPrefix(cfg.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Prefix = base + "/e2e/" + e.RunID
	direct := s3test.NewGuard(cfg.Bucket, cfg.Prefix, false).WithEndpoint(cfg.Endpoint)
	cfg.HTTPClient = direct
	e.S3Config = cfg
	ctx := context.Background()
	if e.S3, err = blobstore.NewS3Client(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if e.Meta, err = metastore.NewS3(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if !settings.Keep {
		t.Cleanup(e.deletePrefix)
	}
	// Not bound to the endpoint: requests arrive at the proxy's own address.
	guard := s3test.NewGuard(cfg.Bucket, cfg.Prefix, false)
	opts := ProxyOptions{Guard: guard}
	if ResignFor(cfg.Endpoint, cfg.PathStyle) {
		opts.Upstream = newUpstream(cfg, e.S3)
		t.Logf("the S3 proxy signs requests again for the store (HTTPS or virtual-hosted endpoint)")
	}
	e.Proxy = StartS3ProxyWith(t, e.Gateway, cfg.Endpoint, opts)
	t.Cleanup(func() {
		for _, r := range append(guard.Refused(), direct.Refused()...) {
			t.Errorf("a request outside the run's prefix was refused: %s", r)
		}
	})

	e.build()
	return e
}

// ResignFor reports whether the proxy has to sign again for a store: one that
// routes on its host name. E2E_S3_RESIGN=1 does it for any store, to run that
// path against MinIO.
func ResignFor(endpoint string, pathStyle bool) bool {
	return os.Getenv("E2E_S3_RESIGN") == "1" || strings.HasPrefix(endpoint, "https://") || !pathStyle
}

// Run runs a scenario as a subtest of the run's test, with e.T set to it.
// Scenarios run one after another, and share what earlier ones started.
func (e *Env) Run(name string, f func(t *testing.T)) bool {
	return e.root.Run(name, func(t *testing.T) {
		prev := e.T
		e.T = t
		defer func() { e.T = prev }()
		f(t)
	})
}

// ctx is a context for one step, bounded by the run's timeout.
func (e *Env) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), e.Settings.Timeout)
}

// build builds the engine, init-session and the CLI from this repo, and the
// engine image that copies them over the base image.
func (e *Env) build() {
	t := e.T
	t.Helper()
	if err := os.MkdirAll(e.bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if !e.Settings.SkipBuild {
		start := time.Now()
		root := repoRoot(t)
		for _, b := range []struct {
			out  string
			pkg  string
			tags string
		}{
			{"dagger-engine", "./cmd/engine", "dfexcludepatterns,dfparents"},
			{"dagger-init-session", "./cmd/init-session", ""},
			{"dagger", "./cmd/dagger", ""},
		} {
			args := []string{"build", "-o", filepath.Join(e.bin, b.out)}
			if b.tags != "" {
				args = append(args, "-tags", b.tags)
			}
			cmd := exec.Command("go", append(args, b.pkg)...)
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("go build %s: %v\n%s", b.pkg, err, out)
			}
		}
		t.Logf("built the engine, init-session and the CLI in %s", time.Since(start).Round(time.Second))
	}

	// GC is off: on small disks the engine prunes every entry right after
	// the session. docker.io pulls go through mirror.gcr.io, as in the
	// release image's own engine.json.
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(e.bin, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("engine.json", `{"gc": {"enabled": false}, "registries": {"docker.io": {"mirrors": ["mirror.gcr.io"]}}}`+"\n")
	dockerfile := "FROM " + e.Settings.BaseImage + "\n" +
		"COPY dagger-engine dagger-init-session dagger /usr/local/bin/\n" +
		"COPY engine.json /etc/dagger/engine.json\n"
	if e.Settings.CAFile != "" {
		ca, err := os.ReadFile(e.Settings.CAFile)
		if err != nil {
			t.Fatal(err)
		}
		write("extra-ca.crt", string(ca))
		dockerfile += "COPY extra-ca.crt /usr/local/share/ca-certificates/extra-ca.crt\n" +
			"RUN cat /usr/local/share/ca-certificates/extra-ca.crt >>/etc/ssl/certs/ca-certificates.crt\n"
	} else {
		_ = os.Remove(filepath.Join(e.bin, "extra-ca.crt"))
	}
	write("Dockerfile", dockerfile)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if _, err := Docker(ctx, "build", "-q", "-t", e.Settings.Image, e.bin); err != nil {
		t.Fatalf("engine image: %v", err)
	}
	t.Logf("built %s from %s in %s", e.Settings.Image, e.Settings.BaseImage, time.Since(start).Round(time.Second))
}

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" || mod == os.DevNull {
		t.Fatal("not in a Go module")
	}
	return filepath.Dir(mod)
}

// Save writes v as JSON to the run's directory, for looking at after the
// run.
func (e *Env) Save(name string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		e.T.Errorf("save %s: %v", name, err)
		return
	}
	if err := os.WriteFile(filepath.Join(e.RunDir, name+".json"), data, 0o644); err != nil {
		e.T.Errorf("save %s: %v", name, err)
	}
}

// Index reads the scope's index from MinIO.
func (e *Env) Index() *metastore.Index {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	idx, err := e.Meta.Index(ctx, e.Scope)
	if err != nil {
		e.T.Fatalf("read the index: %v", err)
	}
	return idx
}

// IndexOf reads a namespace's index.
func (e *Env) IndexOf(ns string) *metastore.Index {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	idx, err := e.Meta.Index(ctx, ns)
	if err != nil {
		e.T.Fatalf("index of %s: %v", ns, err)
	}
	return idx
}

// Bundle reads a stored bundle from MinIO.
func (e *Env) Bundle(id string) *dagql.ValueBundle {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	b, err := e.Meta.GetBundle(ctx, e.Scope, id)
	if err != nil {
		e.T.Fatalf("read bundle %s: %v", id, err)
	}
	return b
}

// Resolution reads a stored resolution from MinIO.
func (e *Env) Resolution(id string) *metastore.Resolution {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	r, err := e.Meta.GetResolution(ctx, e.Scope, id)
	if err != nil {
		e.T.Fatalf("read resolution %s: %v", id, err)
	}
	return r
}

// BundleIDs lists the stored bundles, sorted.
func (e *Env) BundleIDs() []string {
	e.T.Helper()
	return e.BundleIDsIn(e.Scope)
}

// BundleIDsIn lists a namespace's stored bundles, sorted.
func (e *Env) BundleIDsIn(ns string) []string {
	e.T.Helper()
	prefix := e.S3Config.Prefix + "/" + ns + "/bundles/"
	var ids []string
	for _, key := range e.listKeys(prefix) {
		ids = append(ids, strings.TrimSuffix(strings.TrimPrefix(key, prefix), ".json"))
	}
	slices.Sort(ids)
	return ids
}

// Listing is every object under the run's prefix, as "<key> <size> <ETag>",
// sorted: two listings are equal only if nothing was written or deleted in
// between.
func (e *Env) Listing() []string {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var out []string
	pages := s3.NewListObjectsV2Paginator(e.S3, &s3.ListObjectsV2Input{Bucket: aws.String(e.S3Config.Bucket), Prefix: aws.String(e.S3Config.Prefix + "/")})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			e.T.Fatalf("list %s: %v", e.S3Config.Prefix, err)
		}
		for _, o := range page.Contents {
			out = append(out, fmt.Sprintf("%s %d %s", aws.ToString(o.Key), aws.ToInt64(o.Size), aws.ToString(o.ETag)))
		}
	}
	slices.Sort(out)
	return out
}

func (e *Env) listKeys(prefix string) []string {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var keys []string
	pages := s3.NewListObjectsV2Paginator(e.S3, &s3.ListObjectsV2Input{Bucket: aws.String(e.S3Config.Bucket), Prefix: aws.String(prefix)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			e.T.Fatalf("list %s: %v", prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys
}

// BlobKey is the object key of a layer blob of the run's scope, the
// namespace of a service without tokens.
func (e *Env) BlobKey(d digest.Digest) string {
	return e.BlobKeyIn(e.Scope, d)
}

// BlobKeyIn is the object key of a layer blob of a namespace.
func (e *Env) BlobKeyIn(ns string, d digest.Digest) string {
	return e.S3Config.Prefix + "/" + ns + "/blobs/" + d.Algorithm().String() + "/" + d.Encoded()
}

// HasKey reports whether an object is stored.
func (e *Env) HasKey(key string) bool {
	e.T.Helper()
	return len(e.listKeys(key)) > 0
}

// DeleteBlob deletes a layer blob from MinIO, and checks it is gone.
func (e *Env) DeleteBlob(d digest.Digest) {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	key := e.BlobKey(d)
	e.deleted[d] = true
	if _, err := e.S3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(e.S3Config.Bucket), Key: aws.String(key)}); err != nil {
		e.T.Fatalf("delete %s: %v", key, err)
	}
	if _, err := e.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(e.S3Config.Bucket), Key: aws.String(key)}); err == nil {
		e.T.Fatalf("%s is still in MinIO after its delete", key)
	}
}

// CheckConsistent checks, reading MinIO directly, that what the store
// publishes is whole (#12): the index names no bundle or resolution that
// isn't stored, and no stored bundle names a layer blob that isn't, besides
// those a scenario deleted on purpose (DeleteBlob). Each failure says what
// is missing.
func (e *Env) CheckConsistent(t *testing.T, when string) {
	t.Helper()
	e.CheckConsistentIn(t, e.Scope, when)
}

// CheckConsistentIn is CheckConsistent for a namespace's objects.
func (e *Env) CheckConsistentIn(t *testing.T, ns, when string) {
	t.Helper()
	idx := e.IndexOf(ns)
	stored := map[string]bool{}
	ids := e.BundleIDsIn(ns)
	for _, id := range ids {
		stored[id] = true
	}
	resPrefix := e.S3Config.Prefix + "/" + ns + "/resolutions/"
	resolutions := map[string]bool{}
	for _, key := range e.listKeys(resPrefix) {
		resolutions[strings.TrimSuffix(strings.TrimPrefix(key, resPrefix), ".json")] = true
	}
	var missingBundles, missingResolutions []string
	for n, entry := range idx.Entries {
		if !stored[entry.Bundle] {
			missingBundles = append(missingBundles, fmt.Sprintf("entry %d's bundle %s", n, Short(entry.Bundle)))
		}
		// Strict on purpose: every run starts on an empty prefix, so each
		// entry was recorded by this service, which always names a stored
		// resolution. Records from before resolutions (Entry.Resolution
		// empty), which the importer still merges, can't occur here.
		if entry.Resolution == "" || !resolutions[entry.Resolution] {
			missingResolutions = append(missingResolutions, fmt.Sprintf("entry %d's resolution %q", n, Short(entry.Resolution)))
		}
	}
	slices.Sort(missingBundles)
	slices.Sort(missingResolutions)
	blobs := map[string]bool{}
	for _, key := range e.listKeys(e.S3Config.Prefix + "/" + ns + "/blobs/") {
		blobs[key] = true
	}
	named := map[digest.Digest]bool{}
	var missingBlobs []string
	deleted := 0
	ctx, cancel := e.ctx()
	defer cancel()
	for _, id := range ids {
		b, err := e.Meta.GetBundle(ctx, ns, id)
		if err != nil {
			t.Fatalf("bundle %s of %s: %v", id, ns, err)
		}
		var chains []*dagql.OfferedChain
		for _, o := range b.Outputs {
			chains = append(chains, o.Chain)
		}
		for _, v := range b.Values {
			for i := range v.Record.Envelope.PendingOffers {
				chains = append(chains, &v.Record.Envelope.PendingOffers[i].Chain)
			}
		}
		for _, c := range chains {
			if c == nil {
				continue
			}
			for _, l := range c.Layers {
				d := l.Descriptor.Digest
				if named[d] {
					continue
				}
				named[d] = true
				switch {
				case blobs[e.BlobKeyIn(ns, d)]:
				case ns == e.Scope && e.deleted[d]:
					deleted++
				default:
					missingBlobs = append(missingBlobs, fmt.Sprintf("%s (in bundle %s)", Short(d.String()), Short(id)))
				}
			}
		}
	}
	Check(t, len(missingBundles) == 0, "S3 consistent %s: the index's %d entries are all in its stored bundles (%d stored; missing: %v)",
		when, len(idx.Entries), len(ids), missingBundles)
	Check(t, len(missingResolutions) == 0, "S3 consistent %s: the index's %d entries each name a stored resolution (%d stored; missing: %v)",
		when, len(idx.Entries), len(resolutions), missingResolutions)
	Check(t, len(missingBlobs) == 0, "S3 consistent %s: the %d stored bundles name %d layer blobs, all stored (missing: %v; %d deleted on purpose)",
		when, len(ids), len(named), missingBlobs, deleted)
}

func (e *Env) deletePrefix() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := s3test.DeletePrefix(ctx, e.S3, e.S3Config.Bucket, e.S3Config.Prefix); err != nil {
		e.root.Errorf("deleting the run's objects: %v", err)
	}
}

// WaitFor calls cond every half second until it returns true, and fails the
// test when timeout passes first. cond's error, when not nil, is kept to
// say what it last saw.
func (e *Env) WaitFor(timeout time.Duration, what string, cond func() (bool, error)) {
	e.T.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for {
		ok, err := cond()
		if ok {
			return
		}
		last = err
		if time.Now().After(deadline) {
			if last != nil {
				e.T.Fatalf("timed out after %s waiting for %s: %v", timeout, what, last)
			}
			e.T.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Check reports one check of a scenario: it logs it, and fails the test
// without stopping it when ok is false.
func Check(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if ok {
		t.Logf("ok    "+format, args...)
	} else {
		t.Errorf("FAIL  "+format, args...)
	}
}

// Short is the start of a digest's or an ID's hex, for messages.
func Short(s string) string {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[i+1:]
	}
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// Nonce is a fresh random string, for queries that must never be cached.
func Nonce() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
