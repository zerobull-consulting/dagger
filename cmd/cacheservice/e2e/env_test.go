package e2e

import (
	"os"
	"regexp"
	"strings"
	"testing"
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
