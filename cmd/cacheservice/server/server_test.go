package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
)

// EnableS3 refuses an incomplete configuration before it reaches a store,
// and leaves both paths off.
func TestEnableS3RefusesBadConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  blobstore.S3Config
		want string
	}{
		{"no bucket", blobstore.S3Config{Region: "us-east-1"}, "no bucket"},
		{"no region", blobstore.S3Config{Bucket: "b"}, "no region"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := New(slog.New(slog.DiscardHandler), "")
			err := svc.EnableS3(context.Background(), tc.cfg, S3Options{Scope: testScope})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error with %q", err, tc.want)
			}
			if svc.export != nil || svc.imports != nil {
				t.Fatal("a path is on after a refused configuration")
			}
		})
	}
}

// The accessors say which path is off, and which connection is unknown.
func TestAccessorErrors(t *testing.T) {
	ctx := context.Background()
	off := New(slog.New(slog.DiscardHandler), "")
	if _, err := off.Entries(1); !errors.Is(err, ErrExportOff) {
		t.Errorf("Entries: %v", err)
	}
	if _, err := off.Export(ctx, 1, exporter.Request{}); !errors.Is(err, ErrExportOff) {
		t.Errorf("Export: %v", err)
	}
	if _, err := off.Imports(1); !errors.Is(err, ErrImportOff) {
		t.Errorf("Imports: %v", err)
	}
	if _, err := off.Import(ctx, 1); !errors.Is(err, ErrImportOff) {
		t.Errorf("Import: %v", err)
	}
	if got := off.Engines(); len(got) != 0 {
		t.Errorf("Engines: %v", got)
	}

	h, _ := newMemExportHarness(t, nil)
	if _, err := h.svc.Entries(999); !errors.Is(err, ErrNoEngine) {
		t.Errorf("Entries of an unknown conn: %v", err)
	}
	if _, err := h.svc.Imports(999); !errors.Is(err, ErrNoEngine) {
		t.Errorf("Imports of an unknown conn: %v", err)
	}
	engines := h.svc.Engines()
	if len(engines) != 1 || engines[0].Conn != h.engine.id || engines[0].CacheID != h.cache.ID {
		t.Fatalf("Engines: %+v", engines)
	}
	if _, err := h.svc.Entries(h.engine.id); err != nil {
		t.Errorf("Entries: %v", err)
	}
	if st, err := h.svc.Imports(h.engine.id); err != nil || st.CacheID != h.cache.ID {
		t.Errorf("Imports: %+v, %v", st, err)
	}
}

// With a path off, its debug routes answer 409 before looking at the
// connection, as they did before they wrapped the accessors.
func TestDebugPathOffBeforeConn(t *testing.T) {
	debug := httptest.NewServer(New(slog.New(slog.DiscardHandler), "").DebugHandler())
	defer debug.Close()
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/debug/engines/abc/entries"},
		{http.MethodPost, "/debug/engines/abc/export"},
		{http.MethodGet, "/debug/engines/abc/imports"},
		{http.MethodPost, "/debug/engines/abc/import"},
		{http.MethodGet, "/debug/engines/1/entries"},
	} {
		req, _ := http.NewRequest(tc.method, debug.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("%s %s: status %d, want 409", tc.method, tc.path, resp.StatusCode)
		}
	}
}
