package e2e

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestDefaultEngineImageIsCIs checks that the harness's default engine image
// is the workflow's STOCK_ENGINE, the stock release CI runs the e2e and the
// sidecar demo on, and that STOCK_ENGINE_TAG is its tag: so `dev.sh e2e`
// tests locally the engine CI does. It runs without Docker, in the unit job.
func TestDefaultEngineImageIsCIs(t *testing.T) {
	data, err := os.ReadFile("../../../.github/workflows/remote-cache-tests.yml")
	if err != nil {
		t.Fatal(err)
	}
	setting := func(name string) string {
		t.Helper()
		re := regexp.MustCompile(`(?m)^\s*` + name + `:\s*(\S+)\s*$`)
		m := re.FindAllStringSubmatch(string(data), -1)
		if len(m) != 1 {
			t.Fatalf("the workflow sets %s %d times, want once (in its top-level env)", name, len(m))
		}
		return strings.Trim(m[0][1], `"'`)
	}
	if got := setting("STOCK_ENGINE"); got != DefaultEngineImage {
		t.Errorf("the workflow's STOCK_ENGINE is %s, the harness's DefaultEngineImage %s: update them together", got, DefaultEngineImage)
	}
	tag := setting("STOCK_ENGINE_TAG")
	if !strings.HasPrefix(DefaultEngineImage, "registry.dagger.io/engine:"+tag+"@sha256:") {
		t.Errorf("STOCK_ENGINE_TAG %s is not DefaultEngineImage's tag (%s)", tag, DefaultEngineImage)
	}
}

// TestPruneCLIs checks that pruneCLIs removes only the CLI copies of other
// images unused for cliKeep, and nothing it didn't name.
func TestPruneCLIs(t *testing.T) {
	root := t.TempDir()
	hex := func(c string) string { return strings.Repeat(c, 64) }
	old := time.Now().Add(-2 * cliKeep)
	dirs := map[string]bool{ // name: kept
		hex("a"):      true,  // the image in use, however old
		hex("b"):      false, // another image's, unused for longer than cliKeep
		hex("c"):      true,  // another image's, used recently
		"notes":       true,  // not an image ID: not copyCLI's
		hex("d")[:63]: true,
	}
	for name := range dirs {
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if name != hex("c") {
			if err := os.Chtimes(dir, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	pruneCLIs(t, root, hex("a"))
	for name, kept := range dirs {
		_, err := os.Stat(filepath.Join(root, name))
		if got := err == nil; got != kept {
			t.Errorf("%s: kept %v, want %v", name, got, kept)
		}
	}
}
