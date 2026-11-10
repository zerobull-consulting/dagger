package wipe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// fakeStore is a Store in memory, recording every request.
type fakeStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	uploads []Upload
	// pageSize is the most a listing returns (PageSize when 0).
	pageSize int
	// calls are the requests, in order: "list <prefix>", "delete <keys>",
	// "abort <key>", "get <key>", "uploads <prefix>".
	calls []string
	// deleted and aborted are every key deleted and upload aborted.
	deleted, aborted []string
	// Hooks, when set: their error fails the request; deleteFailures are
	// keys the store reports failing.
	getErr         func(key string) error
	deleteErr      func(keys []string) error
	afterDelete    func(f *fakeStore)
	deleteFailures map[string]bool
	abortErr       func(u Upload) error
	// extraListed is a key a listing returns whatever the prefix.
	extraListed string
	// exactKey lists uploads as MinIO does: by prefix, only an upload at
	// exactly that key.
	exactKey bool
	// listErr, when set, fails the listing of objects after startAfter.
	listErr func(startAfter string) error
	// uploadsErr, when set, fails every listing of uploads.
	uploadsErr error
	// extraUpload, when set, is an upload every listing of uploads
	// returns, whatever the prefix: a store that ignores it.
	extraUpload *Upload
	// versioning and versioningErr are what Versioning answers.
	versioning    string
	versioningErr error
}

func (f *fakeStore) Versioning(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "versioning")
	return f.versioning, f.versioningErr
}

func (f *fakeStore) UploadsByExactKey() bool { return f.exactKey }

func newFake() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}}
}

func (f *fakeStore) put(key string, size int) { f.objects[key] = bytes.Repeat([]byte("x"), size) }

func (f *fakeStore) putMarker(prefix string, version int) {
	b, _ := json.Marshal(ownership.Marker{Layout: version, Created: time.Now().UTC(), ServiceVersion: "test"})
	f.objects[layout.MarkerKey(prefix)] = b
}

func (f *fakeStore) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.objects))
}

func (f *fakeStore) uploadKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, u := range f.uploads {
		out = append(out, u.Key+"#"+u.ID)
	}
	slices.Sort(out)
	return out
}

func (f *fakeStore) List(_ context.Context, prefix, startAfter string) ([]Object, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "list "+prefix)
	if f.listErr != nil {
		if err := f.listErr(startAfter); err != nil {
			return nil, false, err
		}
	}
	n := f.pageSize
	if n == 0 {
		n = PageSize
	}
	var out []Object
	for _, k := range slices.Sorted(maps.Keys(f.objects)) {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			out = append(out, Object{Key: k, Size: int64(len(f.objects[k]))})
		}
	}
	if f.extraListed != "" {
		out = append([]Object{{Key: f.extraListed}}, out...)
	}
	if len(out) > n {
		return out[:n], true, nil
	}
	return out, false, nil
}

func (f *fakeStore) ListUploads(_ context.Context, prefix, keyMarker, idMarker string) ([]Upload, string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "uploads "+prefix)
	if f.uploadsErr != nil {
		return nil, "", "", f.uploadsErr
	}
	n := f.pageSize
	if n == 0 {
		n = PageSize
	}
	ups := slices.Clone(f.uploads)
	slices.SortFunc(ups, func(a, b Upload) int { return strings.Compare(a.Key+"\x00"+a.ID, b.Key+"\x00"+b.ID) })
	var out []Upload
	for _, u := range ups {
		match := strings.HasPrefix(u.Key, prefix)
		if f.exactKey && prefix != "" {
			match = u.Key == prefix
		}
		if match && u.Key+"\x00"+u.ID > keyMarker+"\x00"+idMarker {
			out = append(out, u)
		}
	}
	if f.extraUpload != nil {
		out = append([]Upload{*f.extraUpload}, out...)
	}
	if len(out) > n {
		last := out[n-1]
		return out[:n], last.Key, last.ID, nil
	}
	return out, "", "", nil
}

func (f *fakeStore) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "get "+key)
	if f.getErr != nil {
		if err := f.getErr(key); err != nil {
			return nil, err
		}
	}
	b, ok := f.objects[key]
	if !ok {
		return nil, metastore.ErrNotFound
	}
	return b, nil
}

func (f *fakeStore) Delete(_ context.Context, keys []string) ([]Failure, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "delete "+strings.Join(keys, ","))
	if len(keys) > PageSize {
		f.mu.Unlock()
		return nil, fmt.Errorf("delete: %d keys, over %d", len(keys), PageSize)
	}
	if f.deleteErr != nil {
		if err := f.deleteErr(keys); err != nil {
			f.mu.Unlock()
			return nil, err
		}
	}
	var failed []Failure
	for _, k := range keys {
		if f.deleteFailures[k] {
			failed = append(failed, Failure{Key: k, Reason: "AccessDenied: no"})
			continue
		}
		delete(f.objects, k)
		f.deleted = append(f.deleted, k)
	}
	hook := f.afterDelete
	f.mu.Unlock()
	if hook != nil {
		hook(f)
	}
	return failed, nil
}

func (f *fakeStore) Abort(_ context.Context, u Upload) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "abort "+u.Key)
	if f.abortErr != nil {
		if err := f.abortErr(u); err != nil {
			return err
		}
	}
	f.uploads = slices.DeleteFunc(f.uploads, func(x Upload) bool { return x == u })
	f.aborted = append(f.aborted, u.Key)
	return nil
}

// Objects outside the prefix "cache": a sibling prefix, one that shares its
// name's start, the bucket's top, and a deeper "cache" elsewhere.
var outside = []string{"cache2/ns/main/index.json", "cache2/cacheservice.json", "cachex", "top.json", "other/cache/blobs/x"}

// seed fills f with a store of the version (1: no marker) under "cache",
// and keys and uploads outside it.
func seed(f *fakeStore, version int) {
	if version >= 2 {
		f.putMarker("cache", version)
	}
	for i := range 25 {
		f.put(fmt.Sprintf("cache/ns/main/blobs/sha256/%02x", i), 100)
	}
	f.put("cache/ns/main/index.json", 7)
	for _, k := range outside {
		f.put(k, 3)
	}
	f.uploads = []Upload{{Key: "cache/ns/main/staging/a", ID: "1"}, {Key: "cache/ns/main/staging/a", ID: "2"}, {Key: "cache2/staging/b", ID: "3"}, {Key: "top-upload", ID: "4"}}
}

func run(t *testing.T, f *fakeStore, o Options) (int, string) {
	t.Helper()
	var out bytes.Buffer
	if o.Bucket == "" {
		o.Bucket = "bkt"
	}
	code := Run(context.Background(), o, f, &out, &out)
	return code, out.String()
}

// checkOutsideUntouched compares the keys and uploads outside the prefix
// with what they were.
func checkOutsideUntouched(t *testing.T, f *fakeStore, beforeKeys, beforeUploads []string) {
	t.Helper()
	outsideOnly := func(keys []string) []string {
		return slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return strings.HasPrefix(k, "cache/") })
	}
	if got, want := outsideOnly(f.keys()), outsideOnly(beforeKeys); !slices.Equal(got, want) {
		t.Errorf("keys outside the prefix changed: %v, want %v", got, want)
	}
	if got, want := outsideOnly(f.uploadKeys()), outsideOnly(beforeUploads); !slices.Equal(got, want) {
		t.Errorf("uploads outside the prefix changed: %v, want %v", got, want)
	}
	for _, k := range append(slices.Clone(f.deleted), f.aborted...) {
		if !strings.HasPrefix(k, "cache/") {
			t.Errorf("deleted or aborted %q, outside the prefix", k)
		}
	}
}

func TestDryRunDeletesNothingAndReports(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run(fmt.Sprint("layout", version), func(t *testing.T) {
			f := newFake()
			seed(f, version)
			keys, ups := f.keys(), f.uploadKeys()
			code, out := run(t, f, Options{Prefix: "cache", DryRun: true})
			if code != ExitOK {
				t.Fatalf("exit %d\n%s", code, out)
			}
			if !slices.Equal(f.keys(), keys) || !slices.Equal(f.uploadKeys(), ups) {
				t.Fatal("a dry run changed the store")
			}
			for _, c := range f.calls {
				if strings.HasPrefix(c, "delete") || strings.HasPrefix(c, "abort") {
					t.Fatalf("a dry run made %s", c)
				}
			}
			objects, bytes := 26, 25*100+7
			if version >= 2 {
				objects++
				bytes += len(f.objects["cache/cacheservice.json"])
			}
			for _, want := range []string{
				fmt.Sprintf("objects: %d, %d bytes", objects, bytes),
				"incomplete multipart uploads: 2",
				fmt.Sprintf("... and %d more", objects-MaxShown),
				`"cache/ns/main/blobs/sha256/00"`,
				"-confirm=bkt/cache",
				"nothing deleted",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("output lacks %q:\n%s", want, out)
				}
			}
			if n := strings.Count(out, `  "cache/ns/main/blobs/`) + strings.Count(out, `  "cache/cacheservice.json"`); n != MaxShown {
				t.Errorf("listed %d keys, want %d:\n%s", n, MaxShown, out)
			}
			wantMarker := "ownership marker: none at cache/cacheservice.json"
			if version >= 2 {
				wantMarker = fmt.Sprintf("ownership marker: cache/cacheservice.json, layout %d", version)
			}
			if !strings.Contains(out, wantMarker) {
				t.Errorf("output lacks %q:\n%s", wantMarker, out)
			}
			for _, k := range outside {
				if strings.Contains(out, fmt.Sprintf("%q", k)) {
					t.Errorf("output names %q, outside the prefix:\n%s", k, out)
				}
			}
		})
	}
}

func TestDryRunEmptyPrefix(t *testing.T) {
	f := newFake()
	f.put("cache2/x", 1)
	code, out := run(t, f, Options{Prefix: "cache", DryRun: true})
	if code != ExitOK || !strings.Contains(out, "the prefix is empty") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}

func TestWipeEmptiesPrefix(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, pageSize := range []int{0, 4} {
			t.Run(fmt.Sprintf("layout%d/page%d", version, pageSize), func(t *testing.T) {
				f := newFake()
				f.pageSize = pageSize
				seed(f, version)
				keys, ups := f.keys(), f.uploadKeys()
				code, out := run(t, f, Options{Prefix: "/cache/", Confirm: "bkt/cache"})
				if code != ExitOK {
					t.Fatalf("exit %d\n%s", code, out)
				}
				for _, k := range f.keys() {
					if strings.HasPrefix(k, "cache/") {
						t.Errorf("%s is left", k)
					}
				}
				for _, u := range f.uploadKeys() {
					if strings.HasPrefix(u, "cache/") {
						t.Errorf("upload %s is left", u)
					}
				}
				checkOutsideUntouched(t, f, keys, ups)
				var dels []string
				for _, c := range f.calls {
					if strings.HasPrefix(c, "delete ") {
						dels = append(dels, c)
					}
				}
				if version >= 2 && (len(dels) == 0 || dels[0] != "delete cache/cacheservice.json") {
					t.Errorf("the first delete is %v, want the marker's alone", dels)
				}
				if version == 1 && strings.Contains(out, "deleted the ownership marker") {
					t.Errorf("reports deleting a marker on layout 1:\n%s", out)
				}
				want := fmt.Sprintf("deleted %d objects, %d bytes; aborted 2 incomplete multipart uploads; 0 failed", 26, 25*100+7)
				for _, s := range []string{want, "wipe: the prefix is empty"} {
					if !strings.Contains(out, s) {
						t.Errorf("output lacks %q:\n%s", s, out)
					}
				}
				// Uploads go after every object.
				lastDelete, firstAbort := -1, -1
				for i, c := range f.calls {
					if strings.HasPrefix(c, "delete ") {
						lastDelete = i
					}
					if strings.HasPrefix(c, "abort ") && firstAbort < 0 {
						firstAbort = i
					}
				}
				if firstAbort < lastDelete {
					t.Errorf("an upload was aborted before the last delete: %v", f.calls)
				}
			})
		}
	}
}

// More than one delete's worth: each delete takes at most PageSize keys.
func TestWipeBatches(t *testing.T) {
	f := newFake()
	for i := range 2*PageSize + 5 {
		f.put(fmt.Sprintf("cache/k%05d", i), 1)
	}
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitOK {
		t.Fatalf("exit %d\n%s", code, out)
	}
	var n int
	for _, c := range f.calls {
		if strings.HasPrefix(c, "delete ") {
			n++
		}
	}
	if n != 3 || len(f.objects) != 0 {
		t.Fatalf("%d deletes, %d objects left", n, len(f.objects))
	}
}

func TestUsageRefusedBeforeAnyRequest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
		args   []string
	}{
		{"wrong confirm", "cache", []string{"-dry-run=false", "-confirm=bkt/other"}},
		{"wrong bucket", "cache", []string{"-dry-run=false", "-confirm=other/cache"}},
		{"confirm of a parent", "cache/team", []string{"-dry-run=false", "-confirm=bkt/cache"}},
		{"no confirm", "cache", []string{"-dry-run=false"}},
		{"wrong confirm, dry run", "cache", []string{"-confirm=bkt/other"}},
		{"empty prefix", "", []string{"-dry-run=false", "-confirm=bkt/"}},
		{"empty prefix, dry run", "", nil},
		{"slash prefix", "/", []string{"-dry-run=false", "-confirm=bkt/"}},
		{"dot-dot prefix", "cache/..", []string{"-dry-run=false", "-confirm=bkt/cache/.."}},
		{"empty segment", "cache//x", []string{"-dry-run=false", "-confirm=bkt/cache//x"}},
		{"argument", "cache", []string{"now"}},
		{"bad flag", "cache", []string{"-whole-bucket"}},
		{"zero timeout", "cache", []string{"-timeout=0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(blobstore.EnvBucket, "bkt")
			t.Setenv(blobstore.EnvPrefix, tc.prefix)
			f := newFake()
			seed(f, 2)
			f.put("x", 1)
			keys := f.keys()
			opened := false
			open := func(context.Context, blobstore.S3Config) (Store, error) { opened = true; return f, nil }
			var out bytes.Buffer
			if code := Main(context.Background(), tc.args, &out, &out, open); code != ExitUsage {
				t.Fatalf("exit %d, want %d\n%s", code, ExitUsage, out.String())
			}
			if strings.Trim(tc.prefix, "/") == "" && !strings.Contains(out.String(), "wiping a whole bucket isn't supported") {
				t.Errorf("an empty prefix's refusal doesn't say why:\n%s", out.String())
			}
			if opened || len(f.calls) > 0 || !slices.Equal(f.keys(), keys) {
				t.Fatalf("opened %v, requests %v", opened, f.calls)
			}
		})
	}
}

// Run checks the confirmation and prefix too, for its other callers.
func TestRunRefusesWithoutConfirm(t *testing.T) {
	f := newFake()
	seed(f, 2)
	for _, o := range []Options{{Prefix: "cache"}, {Prefix: "cache", Confirm: "bkt/x"}, {Prefix: "", Confirm: "bkt/"}} {
		if code, out := run(t, f, o); code != ExitUsage || len(f.calls) > 0 {
			t.Fatalf("%+v: exit %d, requests %v\n%s", o, code, f.calls, out)
		}
	}
}

func TestMainWipes(t *testing.T) {
	t.Setenv(blobstore.EnvBucket, "bkt")
	t.Setenv(blobstore.EnvPrefix, "cache")
	f := newFake()
	seed(f, 2)
	open := func(context.Context, blobstore.S3Config) (Store, error) { return f, nil }
	var out bytes.Buffer
	if code := Main(context.Background(), []string{"-dry-run=false", "-confirm=bkt/cache"}, &out, &out, open); code != ExitOK {
		t.Fatalf("exit %d\n%s", code, out.String())
	}
	if f.objects["cache/cacheservice.json"] != nil {
		t.Fatal("the marker is left")
	}
}

// A wipe cut off after the marker's delete leaves keys and no marker: every
// instance, a writer or read-only, refuses the prefix.
func TestInterruptedWipeLeavesRefusedPrefix(t *testing.T) {
	f := newFake()
	seed(f, 2)
	f.deleteErr = func(keys []string) error {
		if len(keys) == 1 && keys[0] == "cache/cacheservice.json" {
			return nil
		}
		return errors.New("connection reset")
	}
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitError {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if _, ok := f.objects["cache/cacheservice.json"]; ok {
		t.Fatal("the marker is still there")
	}
	for _, writer := range []bool{true, false} {
		_, _, err := ownership.Check(context.Background(), ownershipStore{f}, "cache", writer, time.Now())
		var notOwned *ownership.ErrNotOwned
		if !errors.As(err, &notOwned) {
			t.Fatalf("writer %v: the startup check gave %v, want ErrNotOwned", writer, err)
		}
	}
}

// ownershipStore is f as the startup check sees it.
type ownershipStore struct{ f *fakeStore }

func (o ownershipStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	b, err := o.f.Get(ctx, key)
	return b, "etag", err
}

func (o ownershipStore) Put(context.Context, string, []byte, metastore.Condition) (string, error) {
	return "", errors.New("the test's store takes no writes")
}

func (o ownershipStore) ListKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	objs, _, err := o.f.List(ctx, prefix, "")
	keys := objKeys(objs)
	return keys[:min(len(keys), max)], err
}

func (o ownershipStore) ListUploadKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	ups, _, _, err := o.f.ListUploads(ctx, prefix, "", "")
	keys := upKeys(ups)
	return keys[:min(len(keys), max)], err
}

// The marker's delete failing stops the wipe before anything else.
func TestMarkerDeleteFailureStops(t *testing.T) {
	for name, f := range map[string]*fakeStore{
		"request": func() *fakeStore {
			f := newFake()
			f.deleteErr = func([]string) error { return errors.New("503") }
			return f
		}(),
		"per key": func() *fakeStore {
			f := newFake()
			f.deleteFailures = map[string]bool{"cache/cacheservice.json": true}
			return f
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			seed(f, 2)
			keys, ups := f.keys(), f.uploadKeys()
			code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
			if code != ExitError || !strings.Contains(out, "nothing else deleted") {
				t.Fatalf("exit %d\n%s", code, out)
			}
			if !slices.Equal(f.keys(), keys) || !slices.Equal(f.uploadKeys(), ups) {
				t.Fatal("the store changed")
			}
		})
	}
}

// A marker it can't read is an error, for a wipe and a dry run: nothing is
// listed or deleted.
func TestMarkerUnreadable(t *testing.T) {
	for _, dry := range []bool{false, true} {
		f := newFake()
		seed(f, 2)
		f.getErr = func(key string) error {
			if key == "cache/cacheservice.json" {
				return errors.New("503 SlowDown")
			}
			return nil
		}
		keys := f.keys()
		code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache", DryRun: dry})
		if code != ExitError || !slices.Equal(f.keys(), keys) || !strings.Contains(out, "can't read the ownership marker cache/cacheservice.json") {
			t.Fatalf("dry run %v: exit %d\n%s", dry, code, out)
		}
		for _, c := range f.calls {
			if !strings.HasPrefix(c, "get ") {
				t.Fatalf("dry run %v: made %s", dry, c)
			}
		}
	}
}

// A marker the service didn't write is reported with ownership's verdict,
// and wiped like any key.
func TestMarkerNotTheService(t *testing.T) {
	for name, body := range map[string]string{
		"not JSON":        "<html>",
		"lacks its times": `{"layout":2}`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake()
			f.objects["cache/cacheservice.json"] = []byte(body)
			code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
			if code != ExitOK || !strings.Contains(out, "not one the service wrote, which every instance refuses") || len(f.objects) != 0 {
				t.Fatalf("exit %d, %v left\n%s", code, f.keys(), out)
			}
		})
	}
}

// A writer still running leaves keys behind: exit 3, which says so.
func TestWriterDuringWipeExits3(t *testing.T) {
	f := newFake()
	seed(f, 1)
	once := sync.Once{}
	f.afterDelete = func(f *fakeStore) {
		once.Do(func() {
			f.mu.Lock()
			f.put("cache/ns/main/blobs/sha256/00", 5) // before the next page's start
			f.mu.Unlock()
		})
	}
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitNotEmpty || !strings.Contains(out, "is a writer still running") || !strings.Contains(out, "objects: 1, 5 bytes") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}

// A key the store reports failing to delete, or an upload it won't abort:
// exit 1, naming it.
func TestFailuresExit1(t *testing.T) {
	f := newFake()
	seed(f, 2)
	f.deleteFailures = map[string]bool{"cache/ns/main/index.json": true}
	f.abortErr = func(u Upload) error {
		if u.ID == "2" {
			return errors.New("500")
		}
		return nil
	}
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitError {
		t.Fatalf("exit %d\n%s", code, out)
	}
	for _, s := range []string{`FAILED "cache/ns/main/index.json"`, `FAILED "upload:cache/ns/main/staging/a": upload ID "2": 500`, "deleted 25 objects, 2500 bytes; aborted 1 incomplete multipart uploads; 2 failed", "the prefix isn't empty"} {
		if !strings.Contains(out, s) {
			t.Errorf("output lacks %q:\n%s", s, out)
		}
	}
	if left := f.keys(); !slices.Contains(left, "cache/ns/main/index.json") {
		t.Errorf("left %v", left)
	}
}

// A listing returning a key outside the prefix stops the wipe before that
// page's delete: only listed keys under the prefix are ever deleted.
func TestKeyOutsidePrefixFromStoreRefused(t *testing.T) {
	f := newFake()
	seed(f, 1)
	f.extraListed = "cache2/ns/main/index.json"
	keys, ups := f.keys(), f.uploadKeys()
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitError || !strings.Contains(out, "isn't under") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if !slices.Equal(f.keys(), keys) || !slices.Equal(f.uploadKeys(), ups) {
		t.Fatalf("the store changed: %v", f.calls)
	}
}

// Keys are printed quoted: a key can't write control characters to the
// terminal.
func TestKeysPrintedQuoted(t *testing.T) {
	f := newFake()
	f.put("cache/\x1b[2Jevil", 1)
	_, out := run(t, f, Options{Prefix: "cache", DryRun: true})
	if strings.Contains(out, "\x1b") {
		t.Fatalf("printed a raw escape:\n%q", out)
	}
}

// The run's -timeout ends a wipe stuck on the store: exit 1.
func TestTimeoutStops(t *testing.T) {
	t.Setenv(blobstore.EnvBucket, "bkt")
	t.Setenv(blobstore.EnvPrefix, "cache")
	open := func(context.Context, blobstore.S3Config) (Store, error) { return blockingStore{}, nil }
	var out bytes.Buffer
	start := time.Now()
	if code := Main(context.Background(), []string{"-timeout=50ms"}, &out, &out, open); code != ExitError {
		t.Fatalf("exit %d\n%s", code, out.String())
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the timeout didn't stop it")
	}
}

type blockingStore struct{ Store }

func (blockingStore) Get(ctx context.Context, _ string) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingStore) List(ctx context.Context, _, _ string) ([]Object, bool, error) {
	<-ctx.Done()
	return nil, false, ctx.Err()
}

// A prefix inside another store's (an ancestor holds a marker, of any
// content) is refused, by a dry run and a wipe, naming the ancestor's
// marker: nothing is listed or deleted.
func TestAncestorMarkerRefused(t *testing.T) {
	for _, tc := range []struct{ prefix, ancestor, confirm string }{
		{"cache/team", "cache/cacheservice.json", "bkt/cache/team"},
		{"cache", "cacheservice.json", "bkt/cache"},
		{"a/b/c", "a/cacheservice.json", "bkt/a/b/c"},
	} {
		for _, dry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/dry%v", tc.prefix, dry), func(t *testing.T) {
				f := newFake()
				f.put(tc.ancestor, 1)
				f.put(tc.prefix+"/ns/main/index.json", 1)
				f.uploads = []Upload{{Key: tc.prefix + "/ns/main/staging/x", ID: "1"}}
				keys, ups := f.keys(), f.uploadKeys()
				code, out := run(t, f, Options{Prefix: tc.prefix, Confirm: tc.confirm, DryRun: dry})
				if code != ExitError || !strings.Contains(out, "holds an ownership marker ("+tc.ancestor+")") {
					t.Fatalf("exit %d\n%s", code, out)
				}
				// The hint is one the operator can follow: this command
				// doesn't wipe a bucket's top.
				if tc.ancestor == "cacheservice.json" && (!strings.Contains(out, "empty the bucket in the provider's console") || strings.Contains(out, "wipe the bucket's top")) {
					t.Errorf("the bucket's top's hint:\n%s", out)
				}
				if !slices.Equal(f.keys(), keys) || !slices.Equal(f.uploadKeys(), ups) {
					t.Fatal("the store changed")
				}
				for _, c := range f.calls {
					if !strings.HasPrefix(c, "get ") {
						t.Fatalf("made %s", c)
					}
				}
			})
		}
	}
}

// An ancestor's marker position the key may not read (403) is skipped, as
// ownership's check does; any other error refuses.
func TestAncestorMarkerErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want int
	}{
		"denied":   {fmt.Errorf("get: %w: 403", metastore.ErrDenied), ExitOK},
		"5xx":      {errors.New("503 SlowDown"), ExitError},
		"timeout":  {context.DeadlineExceeded, ExitError},
		"notfound": {metastore.ErrNotFound, ExitOK},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake()
			seed(f, 2)
			f.getErr = func(key string) error {
				if key == "cacheservice.json" {
					return tc.err
				}
				return nil
			}
			keys := f.keys()
			code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
			if code != tc.want {
				t.Fatalf("exit %d, want %d\n%s", code, tc.want, out)
			}
			if tc.want == ExitError && (!slices.Equal(f.keys(), keys) || !strings.Contains(out, "can't read cacheservice.json, an ancestor's ownership marker position")) {
				t.Fatalf("changed the store, or doesn't say why:\n%s", out)
			}
		})
	}
}

// A dry run warns about the ownership markers under the prefix: stores
// nested in it, which the wipe deletes too.
func TestDryRunWarnsNestedMarkers(t *testing.T) {
	f := newFake()
	seed(f, 2)
	f.put("cache/team/cacheservice.json", 1)
	f.put("cache/xcacheservice.json", 1)
	_, out := run(t, f, Options{Prefix: "cache", DryRun: true})
	if !strings.Contains(out, "WARNING: 1 ownership markers under the prefix") || !strings.Contains(out, `  "cache/team/cacheservice.json"`) {
		t.Fatalf("no warning:\n%s", out)
	}
	f = newFake()
	seed(f, 2)
	if _, out := run(t, f, Options{Prefix: "cache", DryRun: true}); strings.Contains(out, "WARNING") {
		t.Fatalf("a warning with only the prefix's own marker:\n%s", out)
	}
}

// On a store that lists uploads only by exact key (MinIO), the uploads are
// found through the bucket's listing, and those outside the prefix kept.
func TestUploadsByExactKeyFallback(t *testing.T) {
	f := newFake()
	f.exactKey = true
	seed(f, 1)
	_, ups := f.keys(), f.uploadKeys()
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitOK || !strings.Contains(out, "aborted 2 incomplete multipart uploads") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	checkOutsideUntouched(t, f, f.keys(), ups)
	if left := f.uploadKeys(); len(left) != 2 {
		t.Fatalf("uploads left: %v", left)
	}
}

// The failures so far are printed when a later request stops the wipe.
func TestFailuresPrintedOnEarlyExit(t *testing.T) {
	f := newFake()
	f.pageSize = 4
	seed(f, 1)
	f.deleteFailures = map[string]bool{"cache/ns/main/blobs/sha256/01": true}
	f.listErr = func(startAfter string) error {
		// Once deleting has started (the pre-listing is done), with f.mu
		// held by List.
		if startAfter != "" && len(f.deleted) > 0 {
			return errors.New("connection reset")
		}
		return nil
	}
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitError || !strings.Contains(out, `FAILED "cache/ns/main/blobs/sha256/01"`) || !strings.Contains(out, "deleted 3 objects") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}

// A failed abort whose upload is gone by the final listing: still exit 1,
// and the message says the prefix is empty.
func TestFailureButEmpty(t *testing.T) {
	f := newFake()
	seed(f, 1)
	f.abortErr = func(u Upload) error {
		f.uploads = slices.DeleteFunc(f.uploads, func(x Upload) bool { return x == u })
		return errors.New("timeout, though it took")
	}
	var out, errOut bytes.Buffer
	code := Run(context.Background(), Options{Bucket: "bkt", Prefix: "cache", Confirm: "bkt/cache"}, f, &out, &errOut)
	if code != ExitError || !strings.Contains(errOut.String(), "though the prefix is empty now") {
		t.Fatalf("exit %d\n%s%s", code, out.String(), errOut.String())
	}
}

// Every key failing its delete: the report counts them all and lists the
// first MaxShown, and only those are kept.
func TestFailuresBounded(t *testing.T) {
	f := newFake()
	f.deleteFailures = map[string]bool{}
	for i := range 2*PageSize + 5 {
		k := fmt.Sprintf("cache/k%05d", i)
		f.put(k, 1)
		f.deleteFailures[k] = true
	}
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	n := 2*PageSize + 5
	if code != ExitError || strings.Count(out, "  FAILED ") != MaxShown || !strings.Contains(out, fmt.Sprintf("; %d failed", n)) ||
		!strings.Contains(out, fmt.Sprintf("... and %d more failures", n-MaxShown)) {
		t.Fatalf("exit %d\n%s", code, out)
	}
	var l failureLog
	for range 3 {
		l.add(make([]Failure, PageSize)...)
	}
	l.add(Failure{})
	if l.n != 3*PageSize+1 || len(l.first) != MaxShown || cap(l.first) > 2*MaxShown {
		t.Fatalf("n %d, kept %d (cap %d)", l.n, len(l.first), cap(l.first))
	}
}

// On a store that lists uploads by exact key, an upload at exactly the
// prefix's root doesn't hide those below it: each is aborted once, and a
// dry run counts each once.
func TestUploadsByExactKeyRootUpload(t *testing.T) {
	newStore := func() *fakeStore {
		f := newFake()
		f.exactKey = true
		seed(f, 1)
		f.uploads = append(f.uploads, Upload{Key: "cache/", ID: "r1"}, Upload{Key: "cache/", ID: "r2"})
		return f
	}
	f := newStore()
	_, out := run(t, f, Options{Prefix: "cache", DryRun: true})
	if !strings.Contains(out, "incomplete multipart uploads: 4\n") {
		t.Fatalf("dry run:\n%s", out)
	}
	f = newStore()
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitOK || !strings.Contains(out, "aborted 4 incomplete multipart uploads; 0 failed") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	slices.Sort(f.aborted)
	if want := []string{"cache/", "cache/", "cache/ns/main/staging/a", "cache/ns/main/staging/a"}; !slices.Equal(f.aborted, want) {
		t.Fatalf("aborted %v, want %v", f.aborted, want)
	}
	for _, u := range f.uploadKeys() {
		if strings.HasPrefix(u, "cache/") {
			t.Fatalf("left %v", f.uploadKeys())
		}
	}
}

// A real wipe names the stores nested in the prefix before it deletes
// anything.
func TestWipeWarnsNestedMarkers(t *testing.T) {
	f := newFake()
	seed(f, 2)
	f.put("cache/team/cacheservice.json", 1)
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	w, d := strings.Index(out, "WARNING: 1 ownership markers under the prefix"), strings.Index(out, "deleted the ownership marker")
	if code != ExitOK || w < 0 || d < w || !strings.Contains(out, `  "cache/team/cacheservice.json"`) {
		t.Fatalf("exit %d, warning at %d, first delete at %d\n%s", code, w, d, out)
	}
}

// Uploads it can't list refuse the wipe before anything is deleted, not
// after the objects are gone.
func TestUploadListingRefusedBeforeDelete(t *testing.T) {
	f := newFake()
	seed(f, 2)
	f.uploadsErr = errors.New("AccessDenied: ListBucketMultipartUploads")
	keys := f.keys()
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitError || !strings.Contains(out, "nothing deleted") || !slices.Equal(f.keys(), keys) {
		t.Fatalf("exit %d\n%s", code, out)
	}
	for _, c := range f.calls {
		if strings.HasPrefix(c, "delete ") || strings.HasPrefix(c, "abort ") {
			t.Fatalf("made %s", c)
		}
	}
}

// An upload outside the prefix that a store lists anyway is left out:
// never aborted, never counted, and the wipe completes.
func TestUploadOutsidePrefixFiltered(t *testing.T) {
	f := newFake()
	seed(f, 1)
	f.extraUpload = &Upload{Key: "cache2/staging/b", ID: "3"}
	code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
	if code != ExitOK || !strings.Contains(out, "aborted 2 incomplete multipart uploads") || slices.Contains(f.aborted, "cache2/staging/b") {
		t.Fatalf("exit %d, aborted %v\n%s", code, f.aborted, out)
	}
	if _, out := run(t, f, Options{Prefix: "cache", DryRun: true}); !strings.Contains(out, "incomplete multipart uploads: 0") {
		t.Fatalf("dry run:\n%s", out)
	}
}

// An ancestor prefix claimed during the wipe stops it before its next
// delete: that store's keys may be under this prefix (wiping a/ns under a
// new store at a).
func TestAncestorClaimedDuringWipeStops(t *testing.T) {
	f := newFake()
	f.pageSize = 4
	for i := range 10 {
		f.put(fmt.Sprintf("a/ns/k%02d", i), 1)
	}
	f.uploads = []Upload{{Key: "a/ns/staging/x", ID: "1"}}
	once := sync.Once{}
	f.afterDelete = func(f *fakeStore) {
		once.Do(func() {
			f.mu.Lock()
			f.putMarker("a", 2)
			f.put("a/ns/main/index.json", 9) // the new store's
			f.mu.Unlock()
		})
	}
	code, out := run(t, f, Options{Prefix: "a/ns", Confirm: "bkt/a/ns"})
	if code != ExitError || !strings.Contains(out, "stopped before the next delete") || !strings.Contains(out, "holds an ownership marker (a/cacheservice.json)") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if _, ok := f.objects["a/ns/main/index.json"]; !ok || len(f.aborted) > 0 {
		t.Fatalf("deleted the new store's key, or aborted %v\n%s", f.aborted, out)
	}
	var dels int
	for _, c := range f.calls {
		if strings.HasPrefix(c, "delete ") {
			dels++
		}
	}
	if dels != 1 {
		t.Fatalf("%d deletes, want the first page's only", dels)
	}
}

// An ancestor claimed after the last object delete stops the wipe before
// any upload is aborted.
func TestAncestorClaimedBeforeAbortsStops(t *testing.T) {
	f := newFake()
	f.put("a/ns/k", 1)
	f.uploads = []Upload{{Key: "a/ns/staging/x", ID: "1"}}
	f.afterDelete = func(f *fakeStore) {
		f.mu.Lock()
		f.putMarker("a", 2)
		f.mu.Unlock()
	}
	code, out := run(t, f, Options{Prefix: "a/ns", Confirm: "bkt/a/ns"})
	if code != ExitError || len(f.aborted) > 0 || !strings.Contains(out, "stopped before the next delete") {
		t.Fatalf("exit %d, aborted %v\n%s", code, f.aborted, out)
	}
}

// A versioned bucket (Enabled or Suspended) is refused before anything is
// deleted; a key that may not read the setting gets a warning, and the
// wipe goes on; any other error refuses.
func TestVersionedBucket(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
		err    error
		want   int
		say    string
	}{
		{"enabled", "Enabled", nil, ExitError, "versioning is Enabled"},
		{"suspended", "Suspended", nil, ExitError, "versioning is Suspended"},
		{"denied", "", fmt.Errorf("get: %w: 403", metastore.ErrDenied), ExitOK, "WARNING: the key may not read the bucket's versioning"},
		{"5xx", "", errors.New("get the bucket's versioning: 503"), ExitError, "503; nothing deleted"},
		{"never enabled", "", nil, ExitOK, "wipe: the prefix is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			seed(f, 2)
			f.versioning, f.versioningErr = tc.status, tc.err
			keys := f.keys()
			code, out := run(t, f, Options{Prefix: "cache", Confirm: "bkt/cache"})
			if code != tc.want || !strings.Contains(out, tc.say) {
				t.Fatalf("exit %d, want %d, and %q\n%s", code, tc.want, tc.say, out)
			}
			if tc.want == ExitError && (!slices.Equal(f.keys(), keys) || !strings.Contains(out, "nothing deleted")) {
				t.Fatalf("changed the store\n%s", out)
			}
			// A dry run reports it, and exits 0.
			f = newFake()
			seed(f, 2)
			f.versioning, f.versioningErr = tc.status, tc.err
			if code, out := run(t, f, Options{Prefix: "cache", DryRun: true}); code != ExitOK || !strings.Contains(out, "versioning") {
				t.Fatalf("dry run: exit %d\n%s", code, out)
			}
		})
	}
}

// What the store answers is printed escaped: a key or an error message
// holding control characters can't write them to the terminal.
func TestStoreStringsEscaped(t *testing.T) {
	f := newFake()
	f.uploads = []Upload{{Key: "cache/\x1b[2Jx", ID: "\x1b]0;id"}}
	f.abortErr = func(u Upload) error { return errors.New("500 \x1b[31mred " + u.Key) }
	var out, errOut bytes.Buffer
	code := Run(context.Background(), Options{Bucket: "bkt", Prefix: "cache", Confirm: "bkt/cache"}, f, &out, &errOut)
	if code != ExitError || strings.Contains(out.String()+errOut.String(), "\x1b") || !strings.Contains(out.String(), `\x1b[31mred`) {
		t.Fatalf("exit %d\n%q\n%q", code, out.String(), errOut.String())
	}
	f = newFake()
	seed(f, 2)
	f.versioningErr = errors.New("503 \x1b[2J")
	errOut.Reset()
	if code := Run(context.Background(), Options{Bucket: "bkt", Prefix: "cache", Confirm: "bkt/cache"}, f, &out, &errOut); code != ExitError || strings.Contains(errOut.String(), "\x1b") {
		t.Fatalf("exit %d\n%q", code, errOut.String())
	}
}

func TestXMLSafe(t *testing.T) {
	for k, want := range map[string]bool{
		"cache/ns/x":      true,
		"cache/a b+c%&<>": true,
		"cache/\t\n\r":    true,
		"cache/é😀":        true,
		"cache/\x01":      false,
		"cache/\x1b":      false,
		"cache/\uFFFE":    false,
		"cache/\xff":      false,
	} {
		if got := xmlSafe(k); got != want {
			t.Errorf("xmlSafe(%q) = %v, want %v", k, got, want)
		}
	}
}
