package server

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
)

// zerobull-consulting/dagger#59, through the service: a newer generation of
// the harness's cache (5, the engine is 4) recorded the build's entries,
// and GC collected them. The engine, an older generation still running,
// exports them late: the export ends with the roots refused, not failed,
// publishes no root, logs it once, and isn't tried again.
func TestStaleGenerationExportNotRetried(t *testing.T) {
	ctx := context.Background()
	blobs, meta := newMemStores(t)
	newer := &dagql.ValueBundle{
		Version: dagql.ValueBundleVersion,
		Values: []dagql.TransferredValue{
			{Ordinal: 1, SenderNumber: 10, Record: dagql.PersistedRecord{ResultID: 1}},
			{Ordinal: 2, SenderNumber: 11, DependencyIDs: []uint64{1}, Record: dagql.PersistedRecord{ResultID: 2}},
		},
		Roots: []dagql.TransferredRoot{{Ordinal: 2}},
	}
	if _, err := meta.Record(ctx, testScope, newer, "cache-a", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := meta.UpdateIndex(ctx, testScope, func(idx *metastore.Index) error {
		idx.Expire(time.Now().Add(time.Hour).Unix(), time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	collected, err := meta.Index(ctx, testScope)
	if err != nil {
		t.Fatal(err)
	}
	if c := collected.Caches["cache-a"]; len(collected.Entries) != 0 || c == nil || c.Generation != 5 {
		t.Fatalf("setup: entries %v, cache record %+v", collected.Entries, c)
	}

	rec := &logRecorder{}
	h := newExportHarnessLogged(t, blobs, meta, slog.New(rec), nil, nil)
	if h.cache.Generation != 4 {
		t.Fatalf("setup: the engine is generation %d", h.cache.Generation)
	}
	h.build()
	h.postSpans("session-1", cacheSpan(10), cacheSpan(11, "10"))
	h.postSpans("session-1", carrierSpan(2))

	// Settled as refused by the index, not as failures.
	deadline := time.Now().Add(20 * time.Second)
	for {
		entries := h.ns().tracker.Entries(h.cache.ID)
		refused := 0
		for _, e := range entries {
			if e.Refused == exporter.StaleGenerationMessage {
				refused++
			}
			if e.Stored || e.Failures != 0 {
				t.Fatalf("entry %+v: stored or failed, want refused", e)
			}
		}
		if len(entries) == 2 && refused == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the refusal: %+v", entries)
		}
		time.Sleep(10 * time.Millisecond)
	}
	exports := h.cache.Exports()

	// The same entries in a later session: not exported again.
	h.postSpans("session-2", cacheSpan(10), cacheSpan(11, "10"), carrierSpan(2))
	time.Sleep(300 * time.Millisecond)
	if n := h.cache.Exports(); n != exports {
		t.Fatalf("exported again: %d exports, was %d", n, exports)
	}
	idx, err := meta.Index(ctx, testScope)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Roots) != 0 || len(idx.Entries) != 0 || idx.NextNumber != collected.NextNumber {
		t.Fatalf("the stale export changed the index: roots %v, entries %v, next %d (was %d)", idx.Roots, idx.Entries, idx.NextNumber, collected.NextNumber)
	}
	if logged := rec.find("export: older generation than the index's, not recorded"); len(logged) != 1 {
		t.Fatalf("logged %d times, want once: %v", len(logged), logged)
	}
	if failed := rec.find("export failed"); len(failed) != 0 {
		t.Fatalf("logged as failed: %v", failed)
	}
}
