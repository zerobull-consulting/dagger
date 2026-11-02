package main

// The import path against MinIO (#8): engine A's build is exported to S3,
// and engine B, with an empty cache, has it merged and pulls its layers
// straight from S3 through presigned GETs. Skips unless
// CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh test` starts
// MinIO and runs it.

import (
	"context"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
)

// A second engine with an empty cache runs the same pipeline and gets cache
// hits pulled from S3.
func TestMinIOImportFreshEngine(t *testing.T) {
	h, _ := newMinIOExportHarness(t)
	layers := h.buildRecipes()
	h.exportBuild()

	b := fakeengine.New("cache-b", 1)
	eng := h.connectEngine(b)
	checkPull(t, b, "withExec(make)", layers)
	checkPull(t, b, "from(alpine)", layers[:1])

	st := h.debugImports(eng)
	if len(st.Merged) != 1 || len(st.Values) != 2 {
		t.Fatalf("imports %+v", st)
	}
}

// Renewed addresses are fresh presigned GETs that work.
func TestMinIOImportRenew(t *testing.T) {
	h, _ := newMinIOExportHarness(t)
	layers := h.buildRecipes()
	h.exportBuild()

	b := fakeengine.New("cache-b", 1)
	h.connectEngine(b)
	e := waitLookup(t, b, "withExec(make)", hasOffer)
	ok, err := b.Renew(context.Background(), e.Number)
	if err != nil || !ok {
		t.Fatalf("renew: %v, %v", ok, err)
	}
	checkPull(t, b, "withExec(make)", layers)
}

// #35 against MinIO: an importing engine doesn't export its imports back as
// new entries.
func TestMinIOImportsNotReexported(t *testing.T) {
	h, _ := newMinIOExportHarness(t)
	testImportsNotReexported(t, h)
}
