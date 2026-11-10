package ownership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// counting is a Store that counts its requests, by kind.
type counting struct {
	*metastore.MemObjects
	mu    sync.Mutex
	calls map[string]int
	// afterGet runs after each Get, before it returns.
	afterGet func()
}

func newCounting() *counting {
	return &counting{MemObjects: metastore.NewMemObjects(), calls: map[string]int{}}
}

func (c *counting) count(k string) {
	c.mu.Lock()
	c.calls[k]++
	c.mu.Unlock()
}

func (c *counting) Get(ctx context.Context, key string) ([]byte, string, error) {
	c.count("get")
	b, e, err := c.MemObjects.Get(ctx, key)
	if c.afterGet != nil {
		f := c.afterGet
		c.afterGet = nil
		f()
	}
	return b, e, err
}

func (c *counting) Put(ctx context.Context, key string, body []byte, cond metastore.Condition) (string, error) {
	c.count("put")
	return c.MemObjects.Put(ctx, key, body, cond)
}

func (c *counting) ListKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	c.count("list")
	return c.MemObjects.ListKeys(ctx, prefix, max)
}

func (c *counting) ListUploadKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	c.count("uploads")
	return c.MemObjects.ListUploadKeys(ctx, prefix, max)
}

func (c *counting) Delete(context.Context, string) error {
	c.count("delete")
	return errors.New("the ownership check must never delete")
}

func (c *counting) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.calls {
		out[k] = v
	}
	return out
}

func keysOf(st *counting) []string {
	var out []string
	for _, o := range st.List("") {
		out = append(out, o.Key)
	}
	return out
}

func put(t *testing.T, st *counting, key, body string) {
	t.Helper()
	if _, err := st.MemObjects.Put(context.Background(), key, []byte(body), metastore.Condition{}); err != nil {
		t.Fatal(err)
	}
}

var now = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

// A writer on an empty prefix creates the marker, with the layout version,
// its creation time and the service version; a later start finds it. The
// costs are pinned: they are what the README says a startup costs.
func TestWriterClaimsEmptyPrefix(t *testing.T) {
	ctx := context.Background()
	// The marker's GET, then one per ancestor prefix ("" for the bucket's
	// top), only because there was no marker.
	for prefix, gets := range map[string]int{"": 1, "cache": 2, "/a/b/": 3} {
		st := newCounting()
		if prefix != "" {
			// An empty prefix needs only its own keys absent.
			put(t, st, "elsewhere/x", "not under the prefix")
		}
		state, m, err := Check(ctx, st, prefix, true, now)
		if err != nil || state != Claimed {
			t.Fatalf("prefix %q: %v, %v; want claimed", prefix, state, err)
		}
		if got := st.snapshot(); fmt.Sprint(got) != fmt.Sprint(map[string]int{"get": gets, "list": 1, "put": 1, "uploads": 1}) {
			t.Fatalf("prefix %q: requests %v", prefix, got)
		}
		body, _, err := st.MemObjects.Get(ctx, layout.MarkerKey(prefix))
		if err != nil {
			t.Fatal(err)
		}
		var stored map[string]any
		if err := json.Unmarshal(body, &stored); err != nil {
			t.Fatal(err)
		}
		if stored["layout"] != float64(2) || stored["created"] != "2026-11-05T12:00:00Z" || stored["serviceVersion"] == "" {
			t.Fatalf("marker %s", body)
		}
		if m.Layout != 2 {
			t.Fatalf("marker returned %+v", m)
		}

		st.calls = map[string]int{}
		state, _, err = Check(ctx, st, prefix, true, now)
		if err != nil || state != Owned {
			t.Fatalf("second start: %v, %v", state, err)
		}
		if got := st.snapshot(); fmt.Sprint(got) != fmt.Sprint(map[string]int{"get": 1}) {
			t.Fatalf("second start: requests %v, want one GET", got)
		}
	}
}

// Two writers starting together on an empty prefix both start, under one
// marker: one created it, the other found it created.
func TestWritersRaceOnEmptyPrefix(t *testing.T) {
	ctx := context.Background()
	for range 20 {
		st := newCounting()
		var wg sync.WaitGroup
		states := make([]State, 8)
		errs := make([]error, 8)
		for i := range states {
			wg.Add(1)
			go func() {
				defer wg.Done()
				states[i], _, errs[i] = Check(ctx, st, "p", true, now)
			}()
		}
		wg.Wait()
		claimed := 0
		for i := range states {
			if errs[i] != nil {
				t.Fatalf("writer %d: %v", i, errs[i])
			}
			if states[i] == Claimed {
				claimed++
			}
		}
		if claimed != 1 {
			t.Fatalf("%d writers claimed the prefix (%v), want 1", claimed, states)
		}
		if keys := keysOf(st); !slices.Equal(keys, []string{"p/cacheservice.json"}) {
			t.Fatalf("keys %v", keys)
		}
	}
}

// A writer that finds keys in its listing because another writer claimed the
// prefix, and wrote under it, between its read of the marker and its listing,
// starts: it reads the marker again.
func TestWriterListsAfterAnotherClaimed(t *testing.T) {
	ctx := context.Background()
	st := newCounting()
	st.afterGet = func() {
		if _, _, err := Check(ctx, st.MemObjects, "p", true, now); err != nil {
			t.Error(err)
		}
		put(t, st, "p/ns/default/conditional-write-check/0000000000000001", "{}")
	}
	state, _, err := Check(ctx, st, "p", true, now)
	if err != nil || state != Owned {
		t.Fatalf("%v, %v; want owned", state, err)
	}
}

// A marker of another layout version makes both kinds of instance refuse,
// naming both versions, and nothing is written.
func TestOtherLayoutRefused(t *testing.T) {
	ctx := context.Background()
	for _, writer := range []bool{true, false} {
		st := newCounting()
		put(t, st, "p/cacheservice.json", `{"layout":3,"created":"2027-01-01T00:00:00Z","serviceVersion":"future"}`)
		put(t, st, "p/ns/default/index.json", `{}`)
		before := st.List("")
		_, _, err := Check(ctx, st, "p", writer, now)
		var other *ErrOtherLayout
		if !errors.As(err, &other) || other.Found != 3 {
			t.Fatalf("writer %v: %v; want ErrOtherLayout", writer, err)
		}
		for _, want := range []string{"layout version 3", "layout 2", "Wipe the prefix", "another CACHESERVICE_S3_PREFIX"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("writer %v: error %q lacks %q", writer, err, want)
			}
		}
		if c := st.snapshot(); c["put"] != 0 || c["delete"] != 0 {
			t.Fatalf("writer %v: requests %v", writer, c)
		}
		if after := st.List(""); !slices.Equal(before, after) {
			t.Fatalf("writer %v: listing changed:\n%v\n%v", writer, before, after)
		}
	}
}

// A prefix with keys and no marker is refused by both kinds of instance,
// listing at most 10 of the keys, and nothing is written or deleted. That
// holds for another application's files and, on purpose, for a cache of the
// layout before this one (default/index.json, blobs/sha256/...), even though
// it is the service's own data: there is no migration, and reading the old
// layout to recognize it would be one (README, "Changing layouts").
func TestNonEmptyPrefixRefused(t *testing.T) {
	ctx := context.Background()
	hex := strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name string
		keys []string
		more bool
	}{
		{name: "another application's files", keys: []string{"p/backups/db.tar", "p/reports/2026.csv"}},
		{name: "the old layout", keys: []string{
			"p/blobs/sha256/" + hex,
			"p/default/bundles/" + hex + ".json",
			"p/default/index.json",
		}},
		{name: "a namespace without a marker", keys: []string{"p/ns/default/index.json"}},
		{name: "many keys", keys: func() []string {
			var k []string
			for i := range 15 {
				k = append(k, fmt.Sprintf("p/f%02d", i))
			}
			return k
		}(), more: true},
	} {
		for _, writer := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s, writer %v", tc.name, writer), func(t *testing.T) {
				st := newCounting()
				for _, k := range tc.keys {
					put(t, st, k, "x")
				}
				before := st.List("")
				_, _, err := Check(ctx, st, "p", writer, now)
				var not *ErrNotOwned
				if !errors.As(err, &not) {
					t.Fatalf("%v; want ErrNotOwned", err)
				}
				want := tc.keys[:min(len(tc.keys), MaxListed)]
				if !slices.Equal(not.Keys, want) || not.More != tc.more {
					t.Fatalf("listed %v (more %v), want %v (more %v)", not.Keys, not.More, want, tc.more)
				}
				for _, s := range []string{"prefix p/", "cacheservice.json", want[0], "a prefix of its own"} {
					if !strings.Contains(err.Error(), s) {
						t.Errorf("error %q lacks %q", err, s)
					}
				}
				c := st.snapshot()
				if c["put"] != 0 || c["delete"] != 0 || c["list"] != 1 {
					t.Fatalf("requests %v", c)
				}
				if after := st.List(""); !slices.Equal(before, after) {
					t.Fatalf("listing changed:\n%v\n%v", before, after)
				}
			})
		}
	}
}

// A read-only instance on an empty prefix starts, and writes nothing: no
// marker, since it never claims a prefix.
func TestReadOnlyEmptyPrefix(t *testing.T) {
	st := newCounting()
	state, _, err := Check(context.Background(), st, "p", false, now)
	if err != nil || state != Empty {
		t.Fatalf("%v, %v; want empty", state, err)
	}
	if c := st.snapshot(); fmt.Sprint(c) != fmt.Sprint(map[string]int{"get": 2, "list": 1}) {
		t.Fatalf("requests %v", c)
	}
	if keys := keysOf(st); len(keys) != 0 {
		t.Fatalf("keys %v", keys)
	}
	// And proceeds under a writer's marker.
	if _, _, err := Check(context.Background(), st, "p", true, now); err != nil {
		t.Fatal(err)
	}
	if state, _, err := Check(context.Background(), st, "p", false, now); err != nil || state != Owned {
		t.Fatalf("%v, %v; want owned", state, err)
	}
}

// A marker that isn't JSON is an error, not "no marker": nothing is claimed
// over it.
func TestBadMarker(t *testing.T) {
	st := newCounting()
	put(t, st, "cacheservice.json", "not json")
	if _, _, err := Check(context.Background(), st, "", true, now); err == nil || errors.Is(err, ErrNoMarker) {
		t.Fatalf("%v; want a decode error", err)
	}
	if c := st.snapshot(); c["put"] != 0 {
		t.Fatalf("requests %v", c)
	}
}

// A folder placeholder at the prefix itself (the zero-byte "p/" that the
// AWS and Spaces consoles create with "Create folder") is a key like any
// other, so the prefix isn't empty and a writer refuses it. Intended: the
// service owns everything under its prefix, and an empty prefix means no
// key at all. Delete the placeholder, or don't create the folder: the
// service needs none.
func TestFolderPlaceholderRefused(t *testing.T) {
	st := newCounting()
	put(t, st, "p/", "")
	_, _, err := Check(context.Background(), st, "p", true, now)
	var not *ErrNotOwned
	if !errors.As(err, &not) || !slices.Equal(not.Keys, []string{"p/"}) {
		t.Fatalf("%v; want ErrNotOwned listing p/", err)
	}
}

// A marker of layout 2 that lacks a field the service always writes isn't
// the service's: it claims nothing, for either kind of instance, and nothing
// is written over it.
func TestIncompleteMarkerRefused(t *testing.T) {
	for _, body := range []string{`{"layout":2}`, `{"layout":2,"created":"2026-01-01T00:00:00Z"}`, `{"layout":2,"serviceVersion":"x"}`} {
		for _, writer := range []bool{true, false} {
			st := newCounting()
			put(t, st, "p/cacheservice.json", body)
			put(t, st, "p/backups/db.tar", "foreign")
			if _, _, err := Check(context.Background(), st, "p", writer, now); !errors.Is(err, ErrBadMarker) {
				t.Fatalf("%s, writer %v: %v; want ErrBadMarker", body, writer, err)
			}
			if c := st.snapshot(); c["put"] != 0 {
				t.Fatalf("%s: requests %v", body, c)
			}
		}
	}
}

// A prefix whose only content is an incomplete multipart upload (another
// writer's, in progress) has no object to list, but isn't empty: a writer
// refuses it, naming the upload, and writes no marker. A read-only instance
// doesn't list uploads (it claims nothing, and its key needn't list them):
// it starts as on an empty prefix.
func TestWriterRefusesPrefixWithUpload(t *testing.T) {
	st := newCounting()
	st.AddUpload("p/backups/db.tar")
	_, _, err := Check(context.Background(), st, "p", true, now)
	var not *ErrNotOwned
	if !errors.As(err, &not) || !slices.Equal(not.Keys, []string{"upload:p/backups/db.tar"}) {
		t.Fatalf("%v; want ErrNotOwned naming the upload", err)
	}
	if c := st.snapshot(); c["put"] != 0 {
		t.Fatalf("requests %v", c)
	}
	if state, _, err := Check(context.Background(), st, "p", false, now); err != nil || state != Empty {
		t.Fatalf("read-only: %v, %v", state, err)
	}
	if c := st.snapshot(); c["uploads"] != 1 {
		t.Fatalf("the read-only check listed uploads: %v", c)
	}
}

// failGets answers GETs of some keys with an error, as a key allowed only
// its own prefix gets 403 above it, or as a store that fails.
type failGets struct {
	*counting
	fail map[string]error
}

func (d *failGets) Get(ctx context.Context, key string) ([]byte, string, error) {
	if err, ok := d.fail[key]; ok {
		return nil, "", fmt.Errorf("get %s: %w", key, err)
	}
	return d.counting.Get(ctx, key)
}

// s3Error is an S3 error as the SDK returns it: an HTTP status and a code.
func s3Error(status int, code string) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      &smithy.GenericAPIError{Code: code, Message: code},
	}
}

// A prefix inside one that holds a marker belongs to the service there:
// both kinds of instance refuse it, naming the ancestor, and write nothing.
// So does one under a marker at the bucket's top. A marker of any content
// counts. An ancestor whose marker the key may not read (403) is skipped:
// a nested claim costs no data, only the ancestor's GC, which stops on it.
// Any other error reading it refuses (TestAncestorUnreadRefused).
func TestNestedPrefixRefused(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ marker, prefix, ancestor string }{
		{"cache/cacheservice.json", "cache/team", "cache"},
		{"cache/cacheservice.json", "cache/ns/team/x", "cache"},
		{"cacheservice.json", "team", ""},
		{"a/b/cacheservice.json", "/a/b/c/d/", "a/b"},
	} {
		for _, writer := range []bool{true, false} {
			st := newCounting()
			put(t, st, tc.marker, `{"layout":1}`)
			_, _, err := Check(ctx, st, tc.prefix, writer, now)
			var nested *ErrNestedPrefix
			if !errors.As(err, &nested) || nested.Ancestor != tc.ancestor {
				t.Fatalf("%s under %s, writer %v: %v; want ErrNestedPrefix of %q", tc.prefix, tc.marker, writer, err, tc.ancestor)
			}
			if c := st.snapshot(); c["put"] != 0 {
				t.Fatalf("requests %v", c)
			}
		}
	}
	// An ancestor's marker the key may not read: skipped, whether the error
	// says so by its status or by its code.
	for _, denied := range []error{s3Error(http.StatusForbidden, "AccessDenied"), s3Error(http.StatusForbidden, "SignatureDoesNotMatch"), s3Error(0, "AccessDenied")} {
		st := &failGets{counting: newCounting(), fail: map[string]error{"cacheservice.json": denied}}
		if state, _, err := Check(ctx, st, "cache", true, now); err != nil || state != Claimed {
			t.Fatalf("an ancestor the key may not read (%v): %v, %v; want claimed", denied, state, err)
		}
	}
	// A sibling's marker isn't an ancestor's.
	st2 := newCounting()
	put(t, st2, "cachex/cacheservice.json", `{"layout":2}`)
	if state, _, err := Check(ctx, st2, "cache", true, now); err != nil || state != Claimed {
		t.Fatalf("a sibling's marker: %v, %v; want claimed", state, err)
	}
}

// A writer whose upload listing finds an upload because another writer
// claimed the prefix since the object listing, and started a promotion
// under it, starts: it reads the marker again before refusing.
func TestWriterUploadListingAfterAnotherClaimed(t *testing.T) {
	ctx := context.Background()
	st := &claimBeforeUploads{counting: newCounting()}
	state, _, err := Check(ctx, st, "p", true, now)
	if err != nil || state != Owned {
		t.Fatalf("%v, %v; want owned", state, err)
	}
}

// claimBeforeUploads lets another writer claim the prefix, and start an
// upload, just before the upload listing.
type claimBeforeUploads struct {
	*counting
	done bool
}

func (c *claimBeforeUploads) ListUploadKeys(ctx context.Context, prefix string, max int) ([]string, error) {
	if !c.done {
		c.done = true
		if _, _, err := Check(ctx, c.MemObjects, "p", true, now); err != nil {
			return nil, err
		}
		c.AddUpload("p/ns/default/blobs/sha256/" + strings.Repeat("0", 64))
	}
	return c.counting.ListUploadKeys(ctx, prefix, max)
}

// An ancestor's marker that can't be read for an error other than not found
// or access denied (a 5xx, a timeout, a dropped connection) refuses the
// prefix, for both kinds of instance: the check can't tell whether a
// service owns the ancestor. The error names the ancestor's key and wraps
// the store's, and nothing is written. Ancestors after it aren't read.
func TestAncestorUnreadRefused(t *testing.T) {
	ctx := context.Background()
	for name, cause := range map[string]error{
		"5xx":        s3Error(http.StatusInternalServerError, "InternalError"),
		"503":        s3Error(http.StatusServiceUnavailable, "SlowDown"),
		"timeout":    context.DeadlineExceeded,
		"connection": errors.New("connection reset by peer"),
	} {
		for _, tc := range []struct{ prefix, fail string }{
			{"cache/team", "cacheservice.json"},
			{"cache/team", "cache/cacheservice.json"},
		} {
			for _, writer := range []bool{true, false} {
				st := &failGets{counting: newCounting(), fail: map[string]error{tc.fail: cause}}
				_, _, err := Check(ctx, st, tc.prefix, writer, now)
				var unread *ErrAncestorUnread
				if !errors.As(err, &unread) || unread.Key != tc.fail || !errors.Is(err, cause) {
					t.Fatalf("%s at %s, writer %v: %v; want ErrAncestorUnread of %s wrapping %v", name, tc.fail, writer, err, tc.fail, cause)
				}
				if !strings.Contains(err.Error(), tc.fail) || !strings.Contains(err.Error(), "cache/team/") {
					t.Fatalf("%s: the error doesn't name the ancestor's key and the prefix: %v", name, err)
				}
				if c := st.snapshot(); c["put"] != 0 || c["uploads"] != 0 {
					t.Fatalf("%s, writer %v: requests %v", name, writer, c)
				}
			}
		}
	}
	// A missing marker at one ancestor and a 403 at another: both skipped.
	st := &failGets{counting: newCounting(), fail: map[string]error{"cacheservice.json": s3Error(http.StatusForbidden, "AccessDenied")}}
	if state, _, err := Check(ctx, st, "cache/team", true, now); err != nil || state != Claimed {
		t.Fatalf("403 then not found: %v, %v; want claimed", state, err)
	}
}
