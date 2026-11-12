package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/compat"
)

// TestDefaultEngineImageMatchesCI checks that the harness's default engine
// image is the one CI runs the e2e and the sidecar demo on: the manifest's
// first accept line, which the workflow reads as STOCK_ENGINE and
// STOCK_ENGINE_TAG with engines.sh pin, so `dev.sh e2e` tests locally the
// engine CI does. It runs without Docker, in the unit job.
func TestDefaultEngineImageMatchesCI(t *testing.T) {
	d := compat.Embedded().Default()
	if DefaultEngineImage != d.Image() {
		t.Errorf("DefaultEngineImage %s isn't the manifest's first accept line's image %s", DefaultEngineImage, d.Image())
	}
	cmd := exec.Command("../../../hack/remote-cache/engines.sh", "pin")
	cmd.Env = append(os.Environ(), "ENGINES_FILE=../compat/engines.txt")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("engines.sh pin: %v\n%s", err, out)
	}
	if want := "STOCK_ENGINE=" + DefaultEngineImage + "\nSTOCK_ENGINE_TAG=" + d.Version + "\n"; string(out) != want {
		t.Errorf("engines.sh pin, which CI runs, says\n%s\nwant\n%s", out, want)
	}
	if m := stockEngineRE.FindStringSubmatch(DefaultEngineImage); m == nil || m[1] != d.Version || m[2] != "@"+d.Digest {
		t.Errorf("stockEngineRE doesn't read DefaultEngineImage %s: %q", DefaultEngineImage, m)
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

// TestEnginesStartFromTheImageID checks that engines start from the run's
// image by its ID, not from its tag (Settings.Image), which another run
// building the same tag can move to an engine other than the one the CLI was
// copied out of.
func TestEnginesStartFromTheImageID(t *testing.T) {
	id := "sha256:" + strings.Repeat("e", 64)
	env := &Env{Settings: Settings{Image: "dagger-remote-cache-e2e-engine"}, RunID: "run", image: id}
	en := &Engine{Name: "a", Container: "c", Volume: "v", Token: "tok", env: env}
	args := en.runArgs(&Service{URL: "http://svc"})
	var image string
	for i, a := range args {
		if a == "--debug" && i > 0 {
			image = args[i-1]
		}
	}
	if image != id {
		t.Errorf("engines start from %q, want the image ID %s (args: %v)", image, id, args)
	}
	for _, a := range args {
		if a == env.Settings.Image {
			t.Errorf("the run's tag %q is in the engine's docker run: %v", a, args)
		}
	}
}
