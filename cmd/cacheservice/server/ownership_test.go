package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// emptyStores are a writer's in-memory stores before any writer started:
// no marker.
func emptyStores(t *testing.T) sharedStores {
	t.Helper()
	s := newSharedStores(t)
	if err := s.objs.Delete(context.Background(), "cacheservice.json"); err != nil {
		t.Fatal(err)
	}
	return s
}

// startOn starts a service on stores as EnableS3 would, a writer or
// read-only, with the metadata objects behind w, and returns its error.
func startOn(t *testing.T, stores sharedStores, writer bool, w metastore.Objects) (*Service, *logRecorder, error) {
	t.Helper()
	meta, err := metastore.New(metastore.Config{Objects: w})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	logs := &logRecorder{}
	svc := New(slog.New(logs), "")
	opts := S3Options{Scope: testScope, NoExport: !writer}
	err = svc.enableStores(ctx, storeSet{Blobs: stores.blobs, Meta: meta}, opts)
	t.Cleanup(func() {
		cancel()
		svc.Wait()
	})
	return svc, logs, err
}

// A writer on an empty prefix creates the marker and starts; the probe it
// runs next leaves nothing. A second writer finds the marker.
func TestWriterClaimsEmptyPrefix(t *testing.T) {
	stores := emptyStores(t)
	_, logs, err := startOn(t, stores, true, stores.objs)
	if err != nil {
		t.Fatal(err)
	}
	if keys := stores.objs.Keys(); !slices.Equal(keys, []string{"cacheservice.json"}) {
		t.Fatalf("keys %v, want only the marker", keys)
	}
	if len(logs.find("prefix claimed: it was empty, and the ownership marker is created")) != 1 {
		t.Error("no log of the claim")
	}
	_, logs, err = startOn(t, stores, true, stores.objs)
	if err != nil || len(logs.find("prefix owned")) != 1 {
		t.Fatalf("second writer: %v", err)
	}
}

// Both kinds of instance refuse a prefix with another layout's marker, or
// with keys and no marker (another application's, or the layout before
// ns/: refused on purpose, since there is no migration), and nothing is
// written or deleted there: not the marker, not the probe.
func TestStartupRefusesPrefixNotOwned(t *testing.T) {
	hex := strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name string
		keys map[string]string
		want string
	}{
		{name: "another layout", keys: map[string]string{"cacheservice.json": `{"layout":1}`, "ns/team/index.json": "{}"}, want: "layout version 1"},
		{name: "another application", keys: map[string]string{"backups/db.tar": "x"}, want: "backups/db.tar"},
		{name: "the old layout", keys: map[string]string{
			"team/index.json":                  "{}",
			"team/bundles/" + hex + ".json":    "{}",
			"blobs/sha256/" + hex:              "x",
			"team/staging/" + hex[:32] + "/sx": "x",
		}, want: "team/index.json"},
	} {
		for _, writer := range []bool{true, false} {
			stores := emptyStores(t)
			for k, v := range tc.keys {
				if _, err := stores.objs.Put(context.Background(), k, []byte(v), metastore.Condition{}); err != nil {
					t.Fatal(err)
				}
			}
			before := stores.snapshot(t)
			w := &refuseWrites{Objects: stores.objs}
			_, _, err := startOn(t, stores, writer, w)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "ownership check") {
				t.Fatalf("%s, writer %v: %v; want a refusal naming %q", tc.name, writer, err, tc.want)
			}
			if got := w.attempted(); len(got) > 0 {
				t.Fatalf("%s, writer %v: writes %v", tc.name, writer, got)
			}
			if after := stores.snapshot(t); !slices.Equal(before, after) {
				t.Fatalf("%s, writer %v: the stores changed:\n%v\n%v", tc.name, writer, before, after)
			}
		}
	}
}

// A read-only instance on an empty prefix (a pull-request job before the
// first cache-warming run) starts, says no store exists yet, merges nothing
// into an engine (after its import sync has run), and writes nothing.
func TestReadOnlyStartsOnEmptyPrefix(t *testing.T) {
	stores := emptyStores(t)
	h := newReadOnlyHarness(t, stores, readOnlyOptions())
	if len(h.logs.find("no store exists under the prefix yet: nothing to import until a writer (the cache-warming job) creates one")) != 1 {
		t.Error("no log that no store exists yet")
	}
	b := fakeengine.New("cache-b", 1)
	conn := h.connect(b)
	waitUntil(t, 10*time.Second, "the import sync at the hello", func() bool {
		c, err := h.svc.conn(conn)
		if err != nil {
			return false
		}
		_, ok := c.ns.importer.State(c.CacheID())
		return ok
	})
	if n := b.Merges(); n != 0 {
		t.Fatalf("%d merges into the engine", n)
	}
	if got := h.objs.attempted(); len(got) > 0 {
		t.Fatalf("writes %v", got)
	}
	if keys := stores.objs.Keys(); len(keys) != 0 || stores.blobs.Len() != 0 {
		t.Fatalf("keys %v, blobs %d", keys, stores.blobs.Len())
	}
}

// An invalid scope is refused before the ownership check, so it can't leave
// a marker behind on a prefix the service then doesn't use.
func TestInvalidScopeRefusedBeforeClaim(t *testing.T) {
	stores := emptyStores(t)
	meta, err := metastore.New(metastore.Config{Objects: stores.objs})
	if err != nil {
		t.Fatal(err)
	}
	svc := New(slog.New(slog.DiscardHandler), "")
	if err := svc.enableStores(context.Background(), storeSet{Blobs: stores.blobs, Meta: meta}, S3Options{Scope: "a/b"}); err == nil {
		t.Fatal("an invalid scope accepted")
	}
	if keys := stores.objs.Keys(); len(keys) != 0 {
		t.Fatalf("keys %v", keys)
	}
}

// failAncestor fails GETs of the bucket's top marker (an ancestor's, for
// prefix cache/team) with err.
type failAncestor struct {
	*metastore.MemObjects
	err error
}

func (f *failAncestor) Get(ctx context.Context, key string) ([]byte, string, error) {
	if key == "cacheservice.json" {
		return nil, "", fmt.Errorf("get %s: %w", key, f.err)
	}
	return f.MemObjects.Get(ctx, key)
}

// Both kinds of instance on an empty prefix, cache/team, refuse to start
// when an ancestor's marker can't be read for a 5xx or a timeout, naming
// its key, and write nothing. A 403 there is skipped: a writer claims the
// prefix, and a read-only instance starts on it.
func TestStartupAncestorMarkerErrors(t *testing.T) {
	start := func(t *testing.T, writer bool, cause error) (*metastore.MemObjects, error) {
		t.Helper()
		stores := emptyStores(t)
		w := &failAncestor{MemObjects: stores.objs, err: cause}
		meta, err := metastore.New(metastore.Config{Objects: w, Prefix: "cache/team"})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		svc := New(slog.New(slog.DiscardHandler), "")
		err = svc.enableStores(ctx, storeSet{Blobs: stores.blobs, Meta: meta}, S3Options{Scope: testScope, NoExport: !writer})
		t.Cleanup(func() {
			cancel()
			svc.Wait()
		})
		return stores.objs, err
	}
	for name, cause := range map[string]error{
		"5xx":     &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusInternalServerError}}, Err: &smithy.GenericAPIError{Code: "InternalError"}},
		"timeout": context.DeadlineExceeded,
	} {
		for _, writer := range []bool{true, false} {
			objs, err := start(t, writer, cause)
			if err == nil || !strings.Contains(err.Error(), "ownership check") || !strings.Contains(err.Error(), "can't read cacheservice.json") || !errors.Is(err, cause) {
				t.Fatalf("%s, writer %v: %v; want a refusal naming cacheservice.json", name, writer, err)
			}
			if keys := objs.Keys(); len(keys) > 0 {
				t.Fatalf("%s, writer %v: keys %v", name, writer, keys)
			}
		}
	}
	denied := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusForbidden}}, Err: &smithy.GenericAPIError{Code: "AccessDenied"}}
	for _, writer := range []bool{true, false} {
		objs, err := start(t, writer, denied)
		if err != nil {
			t.Fatalf("403, writer %v: %v; want it skipped", writer, err)
		}
		if has := slices.Contains(objs.Keys(), "cache/team/cacheservice.json"); has != writer {
			t.Fatalf("403, writer %v: keys %v", writer, objs.Keys())
		}
	}
}
