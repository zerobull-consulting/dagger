package wipe

// The wipe command against a real S3-compatible store: the MinIO dev
// environment in hack/remote-cache, or any bucket the tests may use. These
// skip unless CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh
// test` starts MinIO and runs them.
//
// Everything happens under the test's own prefix (s3test.NewScope), whose
// guard refuses any change outside the run's root. The wiped prefix is
// <scope>/cache; the scope's own top (<scope>/top.json) stands for the
// bucket's top, which a test may not write, and <scope>/cache2/ and
// <scope>/cachex for sibling prefixes.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/internal/s3test"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

type minioEnv struct {
	t      *testing.T
	cfg    blobstore.S3Config // the scope's: Prefix is the scope's prefix
	client *s3.Client
	guard  *s3test.Guard
	// target is the prefix wiped: <scope>/cache.
	target string
}

func newMinIOEnv(t *testing.T) *minioEnv {
	t.Helper()
	if os.Getenv(blobstore.EnvEndpoint) == "" {
		t.Skipf("%s is not set; start MinIO with hack/remote-cache/dev.sh up", blobstore.EnvEndpoint)
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
	client, err := blobstore.NewS3Client(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope.DeleteOnCleanup(t, client)
	e := &minioEnv{t: t, cfg: cfg, client: client, guard: scope.Guard, target: scope.Prefix + "/cache"}
	t.Setenv(blobstore.EnvBucket, cfg.Bucket)
	t.Setenv(blobstore.EnvPrefix, e.target)
	return e
}

func (e *minioEnv) put(key string, size int) {
	e.t.Helper()
	k := e.cfg.Prefix + "/" + key
	if _, err := e.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &e.cfg.Bucket, Key: &k, Body: bytes.NewReader(bytes.Repeat([]byte("x"), size)),
	}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *minioEnv) startUpload(key string) {
	e.t.Helper()
	k := e.cfg.Prefix + "/" + key
	if _, err := e.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: &e.cfg.Bucket, Key: &k}); err != nil {
		e.t.Fatal(err)
	}
}

// listing is every key and upload under the scope, sorted: the full
// listing compared before and after.
func (e *minioEnv) listing() (keys, uploads []string) {
	e.t.Helper()
	ctx := context.Background()
	under := e.cfg.Prefix + "/"
	pages := s3.NewListObjectsV2Paginator(e.client, &s3.ListObjectsV2Input{Bucket: &e.cfg.Bucket, Prefix: &under})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			e.t.Fatal(err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	// MinIO lists uploads only by exact key: list the bucket's, and filter.
	in := &s3.ListMultipartUploadsInput{Bucket: &e.cfg.Bucket}
	for range 1000 {
		out, err := e.client.ListMultipartUploads(ctx, in)
		if err != nil {
			e.t.Fatal(err)
		}
		for _, u := range out.Uploads {
			if k := aws.ToString(u.Key); strings.HasPrefix(k, under) {
				uploads = append(uploads, k+"#"+aws.ToString(u.UploadId))
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		in.KeyMarker, in.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
	slices.Sort(keys)
	slices.Sort(uploads)
	return keys, uploads
}

// seed writes a store of the layout under the target (1: no marker), and
// keys and an upload outside it.
func (e *minioEnv) seed(version int) {
	e.t.Helper()
	if version == 2 {
		objs := &metastore.S3Objects{Client: e.client, Bucket: e.cfg.Bucket, FilterAllUploads: true}
		if st, _, err := ownership.Check(context.Background(), objs, e.target, true, time.Now()); err != nil || st != ownership.Claimed {
			e.t.Fatalf("claim: %v %v", st, err)
		}
	}
	for i := range 5 {
		e.put(fmt.Sprintf("cache/ns/main/blobs/sha256/%02x", i), 100)
	}
	e.put("cache/ns/main/index.json", 7)
	e.startUpload("cache/ns/main/staging/up")
	for _, k := range []string{"top.json", "cachex", "cache2/ns/main/index.json", "cache2/cacheservice.json"} {
		e.put(k, 3)
	}
	e.startUpload("cache2/ns/main/staging/up")
}

// main runs the command as the binary does, with the test's client.
func (e *minioEnv) main(args ...string) (int, string) {
	e.t.Helper()
	var out bytes.Buffer
	code := Main(context.Background(), args, &out, &out, e.opener(nil))
	return code, out.String()
}

func (e *minioEnv) opener(wrap func(Store) Store) Opener {
	return func(ctx context.Context, cfg blobstore.S3Config) (Store, error) {
		cfg.HTTPClient = e.cfg.HTTPClient
		cfg.Region, cfg.PathStyle = e.cfg.Region, e.cfg.PathStyle
		st, err := OpenS3(ctx, cfg)
		if err != nil || wrap == nil {
			return st, err
		}
		return wrap(st), nil
	}
}

func (e *minioEnv) outsideTarget(keys []string) []string {
	return slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return strings.HasPrefix(k, e.target+"/") })
}

func TestMinIODryRunDeletesNothing(t *testing.T) {
	e := newMinIOEnv(t)
	e.seed(2)
	keys, ups := e.listing()
	seeded := len(e.guard.Touched())
	code, out := e.main()
	if code != ExitOK {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if k, u := e.listing(); !slices.Equal(k, keys) || !slices.Equal(u, ups) {
		t.Fatalf("a dry run changed the bucket:\n%v\n%v", k, u)
	}
	if touched := e.guard.Touched()[seeded:]; len(touched) > 0 {
		t.Fatalf("a dry run changed %v", touched)
	}
	for _, want := range []string{"layout 2", "objects: 7, ", "incomplete multipart uploads: 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestMinIOWipe(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint("layout", version), func(t *testing.T) {
			e := newMinIOEnv(t)
			e.seed(version)
			keys, ups := e.listing()
			seeded := len(e.guard.Touched())
			code, out := e.main("-dry-run=false", "-confirm="+e.cfg.Bucket+"/"+e.target)
			if code != ExitOK {
				t.Fatalf("exit %d\n%s", code, out)
			}
			k, u := e.listing()
			if want := e.outsideTarget(keys); !slices.Equal(k, want) {
				t.Errorf("keys after the wipe:\n%v\nwant\n%v", k, want)
			}
			if want := e.outsideTarget(ups); !slices.Equal(u, want) {
				t.Errorf("uploads after the wipe:\n%v\nwant\n%v", u, want)
			}
			for _, k := range e.guard.Touched()[seeded:] {
				if !strings.HasPrefix(k, e.target+"/") {
					t.Errorf("changed %q, outside the wiped prefix", k)
				}
			}
			wants := []string{"deleted 6 objects, 507 bytes; aborted 1 incomplete multipart uploads; 0 failed", "wipe: the prefix is empty"}
			if version == 2 {
				wants = append(wants, "deleted the ownership marker")
			}
			for _, want := range wants {
				if !strings.Contains(out, want) {
					t.Errorf("output lacks %q:\n%s", want, out)
				}
			}
			// The emptied prefix is claimable again, as a new store.
			objs := &metastore.S3Objects{Client: e.client, Bucket: e.cfg.Bucket, FilterAllUploads: true}
			if st, _, err := ownership.Check(context.Background(), objs, e.target, true, time.Now()); err != nil || st != ownership.Claimed {
				t.Fatalf("claim after the wipe: %v %v", st, err)
			}
		})
	}
}

// failObjects fails every delete but the marker's: a wipe cut off right
// after it.
type failObjects struct{ Store }

func (f failObjects) Delete(ctx context.Context, keys []string) ([]Failure, error) {
	if len(keys) == 1 && strings.HasSuffix(keys[0], "/cacheservice.json") {
		return f.Store.Delete(ctx, keys)
	}
	return nil, errors.New("cut off")
}

// A wipe cut off after the marker's delete leaves a prefix that the
// service's startup check refuses, for a writer and a read-only instance.
func TestMinIOInterruptedWipeRefused(t *testing.T) {
	e := newMinIOEnv(t)
	e.seed(2)
	var out bytes.Buffer
	code := Main(context.Background(), []string{"-dry-run=false", "-confirm=" + e.cfg.Bucket + "/" + e.target}, &out, &out,
		e.opener(func(s Store) Store { return failObjects{s} }))
	if code != ExitError {
		t.Fatalf("exit %d\n%s", code, out.String())
	}
	objs := &metastore.S3Objects{Client: e.client, Bucket: e.cfg.Bucket, FilterAllUploads: true}
	if _, err := ownership.ReadMarker(context.Background(), objs, e.target); !errors.Is(err, ownership.ErrNoMarker) {
		t.Fatalf("the marker: %v", err)
	}
	for _, writer := range []bool{true, false} {
		_, _, err := ownership.Check(context.Background(), objs, e.target, writer, time.Now())
		var notOwned *ownership.ErrNotOwned
		if !errors.As(err, &notOwned) {
			t.Fatalf("writer %v: the startup check gave %v, want ErrNotOwned", writer, err)
		}
	}
}

// A wrong -confirm deletes nothing on a real store either.
func TestMinIOWrongConfirm(t *testing.T) {
	e := newMinIOEnv(t)
	e.seed(1)
	keys, ups := e.listing()
	if code, out := e.main("-dry-run=false", "-confirm="+e.cfg.Bucket+"/"+e.cfg.Prefix); code != ExitUsage {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if k, u := e.listing(); !slices.Equal(k, keys) || !slices.Equal(u, ups) {
		t.Fatal("the bucket changed")
	}
}

// More than one page of objects: the batches and the StartAfter paging
// against a real listing.
func TestMinIOWipeManyPages(t *testing.T) {
	e := newMinIOEnv(t)
	for i := range PageSize + 3 {
		e.put(fmt.Sprintf("cache/k%05d", i), 1)
	}
	e.put("cache2/x", 1)
	code, out := e.main("-dry-run=false", "-confirm="+e.cfg.Bucket+"/"+e.target)
	if code != ExitOK || !strings.Contains(out, fmt.Sprintf("deleted %d objects", PageSize+3)) {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if k, _ := e.listing(); !slices.Equal(k, []string{e.cfg.Prefix + "/cache2/x"}) {
		t.Fatalf("left %v", k)
	}
}

// A prefix inside another store's is refused on a real store, by a dry run
// and a wipe: the scope's top holds a marker, and the target is under it.
func TestMinIOAncestorMarkerRefused(t *testing.T) {
	e := newMinIOEnv(t)
	e.seed(1)
	e.put("cacheservice.json", 3)
	keys, ups := e.listing()
	for _, args := range [][]string{nil, {"-dry-run=false", "-confirm=" + e.cfg.Bucket + "/" + e.target}} {
		code, out := e.main(args...)
		if code != ExitError || !strings.Contains(out, "holds an ownership marker ("+e.cfg.Prefix+"/cacheservice.json)") {
			t.Fatalf("%q: exit %d\n%s", args, code, out)
		}
		if k, u := e.listing(); !slices.Equal(k, keys) || !slices.Equal(u, ups) {
			t.Fatalf("%q: the bucket changed", args)
		}
	}
}

// A dry run on a real store warns about a store nested in the prefix.
func TestMinIODryRunWarnsNestedMarkers(t *testing.T) {
	e := newMinIOEnv(t)
	e.seed(2)
	e.put("cache/team/cacheservice.json", 3)
	code, out := e.main()
	if code != ExitOK || !strings.Contains(out, "WARNING: 1 ownership markers under the prefix") || !strings.Contains(out, e.target+"/team/cacheservice.json") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}
