package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
)

// firstSyncs are the LogImportFirstSync lines of connection conn.
func firstSyncs(logs *logRecorder, conn uint64) []map[string]string {
	var found []map[string]string
	for _, attrs := range logs.find(LogImportFirstSync) {
		if attrs["conn"] == strconv.FormatUint(conn, 10) {
			found = append(found, attrs)
		}
	}
	return found
}

func firstSyncDone(t *testing.T, svc *Service, conn uint64) bool {
	t.Helper()
	for _, e := range svc.Engines() {
		if e.Conn == conn {
			return e.FirstSyncDone
		}
	}
	t.Fatalf("no engine %d", conn)
	return false
}

// A connection's first import sync is logged, and marked in the engine's
// listing, even when it had nothing to merge (zerobull-consulting/dagger#103):
// the sidecar action's start waits for it. Later syncs aren't.
func TestFirstImportSyncWithNothingToMerge(t *testing.T) {
	h := newReadOnlyHarness(t, emptyStores(t), readOnlyOptions())
	b := fakeengine.New("cache-b", 1)
	conn := h.connect(b)
	waitUntil(t, 10*time.Second, "the first import sync's line", func() bool {
		return len(firstSyncs(h.logs, conn)) > 0
	})
	got := firstSyncs(h.logs, conn)[0]
	if got["merged"] != "0" || got["failed"] != "0" || got["error"] != "" {
		t.Fatalf("first sync logged %v, want merged=0 failed=0 and no error", got)
	}
	if !firstSyncDone(t, h.svc, conn) {
		t.Fatal("the engine's listing doesn't say its first sync is done")
	}

	// Later syncs, by the loop and by hand, log no other first sync.
	c, err := h.svc.conn(conn)
	if err != nil {
		t.Fatal(err)
	}
	h.svc.syncImports(t.Context(), c)
	h.svc.triggerImports(c.ns, "")
	h.svc.syncImports(t.Context(), c)
	time.Sleep(100 * time.Millisecond)
	if n := len(firstSyncs(h.logs, conn)); n != 1 {
		t.Fatalf("%d first-sync lines, want 1", n)
	}
	if b.Merges() != 0 {
		t.Fatalf("%d merges into the engine", b.Merges())
	}
}

// The first sync's line counts what it merged.
func TestFirstImportSyncCountsMerged(t *testing.T) {
	stores := newSharedStores(t)
	writerStored(t, stores)
	h := newReadOnlyHarness(t, stores, readOnlyOptions())
	conn := h.connect(fakeengine.New("cache-b", 1))
	waitUntil(t, 10*time.Second, "the first import sync's line", func() bool {
		return len(firstSyncs(h.logs, conn)) > 0
	})
	if got := firstSyncs(h.logs, conn)[0]; got["merged"] != "1" {
		t.Fatalf("first sync logged %v, want merged=1", got)
	}
	if !firstSyncDone(t, h.svc, conn) {
		t.Fatal("the engine's listing doesn't say its first sync is done")
	}
}

// A sync cut short by the connection's end isn't the first sync done.
func TestFirstImportSyncNotLoggedWhenCutShort(t *testing.T) {
	h := newReadOnlyHarness(t, emptyStores(t), readOnlyOptions())
	conn := h.connect(fakeengine.New("cache-b", 1))
	c, err := h.svc.conn(conn)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, "the first import sync's line", func() bool {
		return len(firstSyncs(h.logs, conn)) > 0
	})
	// A fresh connection of the same engine, registered in its place, so
	// only its canceled context keeps its sync from being the first done.
	fresh := &engineConn{id: conn, ns: c.ns, hello: c.hello, logger: c.logger, svc: h.svc}
	h.svc.mu.Lock()
	h.svc.conns[conn] = fresh
	h.svc.mu.Unlock()
	defer func() {
		h.svc.mu.Lock()
		h.svc.conns[conn] = c
		h.svc.mu.Unlock()
	}()
	if !h.svc.connected(fresh) {
		t.Fatal("the fresh connection isn't registered")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	h.svc.syncImports(ctx, fresh)
	if fresh.firstSyncDone.Load() {
		t.Fatal("a canceled sync marked the first sync done")
	}
	if n := len(firstSyncs(h.logs, conn)); n != 1 {
		t.Fatalf("%d first-sync lines, want 1", n)
	}
}

// A sync of a connection that dropped while it ran, its context still
// live, isn't the first sync done either: the engine's next connection has
// its own (zerobull-consulting/dagger#103).
func TestFirstImportSyncNotLoggedWhenDropped(t *testing.T) {
	h := newReadOnlyHarness(t, emptyStores(t), readOnlyOptions())
	conn := h.connect(fakeengine.New("cache-b", 1))
	c, err := h.svc.conn(conn)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, "the first import sync's line", func() bool {
		return len(firstSyncs(h.logs, conn)) > 0
	})
	// The same connection number, no longer the registered connection.
	dropped := &engineConn{id: conn, ns: c.ns, hello: c.hello, logger: c.logger, svc: h.svc}
	h.svc.syncImports(t.Context(), dropped)
	if dropped.firstSyncDone.Load() {
		t.Fatal("a dropped connection's sync marked the first sync done")
	}
	if n := len(firstSyncs(h.logs, conn)); n != 1 {
		t.Fatalf("%d first-sync lines, want 1", n)
	}
}
