package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// runGC runs `cacheservice gc args` as a process, and returns its exit
// code, stdout and stderr.
func runGC(t *testing.T, bin string, env []string, args ...string) (int, string, string) {
	t.Helper()
	cmd := exec.Command(bin, append([]string{"gc"}, args...)...)
	cmd.Env = env
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	return code, out.String(), errOut.String()
}

// stdoutReports fails unless stdout is one JSON array of reports.
func stdoutReports(t *testing.T, stdout string) []gc.Report {
	t.Helper()
	var reps []gc.Report
	dec := json.NewDecoder(strings.NewReader(stdout))
	if err := dec.Decode(&reps); err != nil || reps == nil {
		t.Fatalf("stdout isn't a JSON array: %v\n%q", err, stdout)
	}
	if dec.More() {
		t.Fatalf("stdout holds more than the array: %q", stdout)
	}
	return reps
}

// The built binary dispatches `gc` to the command, with its exit codes and
// its JSON on stdout. The endpoint here answers nothing, so the run can't
// read the ownership marker, and refuses (3) with an empty report; a wrong
// -confirm, no limits with -confirm, and bad flags exit 2 before any request.
func TestBinaryGCExitCodes(t *testing.T) {
	bin := buildBinary(t)
	env := append(os.Environ(),
		blobstore.EnvEndpoint+"=http://127.0.0.1:1",
		blobstore.EnvRegion+"=us-east-1",
		blobstore.EnvBucket+"=bkt",
		blobstore.EnvPrefix+"=cache",
		blobstore.EnvAccessKeyID+"=id",
		blobstore.EnvSecretAccessKey+"=a-secret-never-printed",
		blobstore.EnvPathStyle+"=true",
	)
	for _, tc := range []struct {
		args     []string
		want     int
		wantJSON bool // stdout is the report array; otherwise empty
	}{
		{[]string{"-confirm=bkt/other", "-max-age=0", "-max-bytes=0"}, 2, false},
		{[]string{"-confirm=bkt/cache"}, 2, false},
		{[]string{"-confirm=bkt/cache", "-max-age=0"}, 2, false},
		{[]string{"-bogus"}, 2, false},
		{[]string{"-gc-mode=batch"}, 2, false},
		{[]string{"extra"}, 2, false},
		{[]string{}, 3, true},                                                   // dry run: no marker can be read
		{[]string{"-confirm=bkt/cache", "-max-age=0", "-max-bytes=0"}, 3, true}, // confirmed: the same
	} {
		code, out, errOut := runGC(t, bin, env, tc.args...)
		if code != tc.want {
			t.Errorf("%q: exit %d, want %d\nstdout: %s\nstderr: %s", tc.args, code, tc.want, out, errOut)
		}
		if tc.wantJSON {
			stdoutReports(t, out)
		} else if out != "" {
			t.Errorf("%q: stdout %q, want none", tc.args, out)
		}
		if strings.Contains(out+errOut, "a-secret-never-printed") {
			t.Errorf("%q printed the secret key", tc.args)
		}
	}
}

// The built binary against MinIO: a dry run and a confirmed run of a bucket
// with expired entries exit 0 and report them as JSON on stdout, and the
// confirmed one leaves no claim in progress.
func TestMinIOBinaryGC(t *testing.T) {
	endpoint := os.Getenv(blobstore.EnvEndpoint)
	if endpoint == "" {
		t.Skip("CACHESERVICE_S3_ENDPOINT is not set; start MinIO with hack/remote-cache/dev.sh up")
	}
	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Bucket == "" {
		cfg.Bucket = "dagger-cache"
	}
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		cfg.PathStyle = true
	}
	scope := s3test.NewScope(t, cfg.Bucket)
	cfg.Prefix = scope.Prefix
	cfg.HTTPClient = scope.Guard
	ctx := context.Background()
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope.DeleteOnCleanup(t, client)
	meta, err := metastore.NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ownership.Check(ctx, meta.Objects().(ownership.Store), cfg.Prefix, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	// One namespace with an entry, to expire.
	c := fakeengine.New("a", 1)
	c.Add(fakeengine.Entry{Number: 1, Recipe: "base", Layers: [][]byte{[]byte(strings.Repeat("layer", 100))}})
	capt, err := c.Capture(protocol.Export{Roots: []uint64{1}, Outputs: []protocol.ExportOutput{{Number: 1, Address: fakeengine.FSPart}}})
	if err != nil {
		t.Fatal(err)
	}
	for d, data := range capt.Blobs {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &cfg.Bucket, Key: aws.String(cfg.Prefix + "/ns/team/" + metastore.BlobRelKey(d)), Body: bytes.NewReader(data)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := meta.Record(ctx, "team", capt.Bundle, "a", 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)

	bin := buildBinary(t)
	env := append(os.Environ(), blobstore.EnvBucket+"="+cfg.Bucket, blobstore.EnvPrefix+"="+cfg.Prefix, blobstore.EnvRegion+"="+cfg.Region)
	if os.Getenv(blobstore.EnvPathStyle) == "" {
		env = append(env, blobstore.EnvPathStyle+"=true")
	}
	limits := []string{"-max-age=1s", "-max-bytes=0"}
	code, out, errOut := runGC(t, bin, env, limits...)
	if code != 0 {
		t.Fatalf("dry run: exit %d\n%s", code, errOut)
	}
	reps := stdoutReports(t, out)
	if len(reps) != 1 || reps[0].Namespace != "team" || !reps[0].DryRun || reps[0].Expired != 1 {
		t.Fatalf("dry run reports: %+v", reps)
	}
	code, out, errOut = runGC(t, bin, env, append([]string{"-confirm=" + cfg.Bucket + "/" + cfg.Prefix}, limits...)...)
	if code != 0 {
		t.Fatalf("confirmed run: exit %d\n%s", code, errOut)
	}
	reps = stdoutReports(t, out)
	if len(reps) != 1 || reps[0].DryRun || reps[0].Expired != 1 || len(reps[0].Errors) != 0 {
		t.Fatalf("confirmed run reports: %+v", reps)
	}
	idx, err := meta.Index(ctx, "team")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 0 {
		t.Fatalf("the entry wasn't expired: %+v", idx.Entries)
	}
}
