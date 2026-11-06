package compat

// The engine pins outside the manifest: every one is generated from it or
// checked against it here (zerobull-consulting/dagger#117, D1), in the unit
// job. This replaces zerobull-consulting/dagger#119's check that they all
// equal the workflow's STOCK_ENGINE literal, which no longer exists.

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const repoRoot = "../../.."

// pinnedEngineRE matches a stock engine image pinned by digest.
var pinnedEngineRE = regexp.MustCompile(`registry\.dagger\.io/engine:([^\s"'` + "`" + `)@;,]*)@(sha256:[0-9a-f]{64})`)

// pinDirs are where the remote cache's docs, scripts, harness and CI name
// stock engines.
var pinDirs = []string{
	"cmd/cacheservice",
	"hack/remote-cache",
	".github/actions/remote-cache-sidecar",
	".github/workflows",
}

type pin struct {
	where, version, digest string
}

func findPins(t *testing.T, root string) []pin {
	t.Helper()
	var pins []pin
	for _, dir := range pinDirs {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Base(path) == "engines.txt" {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
			for n := 1; sc.Scan(); n++ {
				for _, m := range pinnedEngineRE.FindAllStringSubmatch(sc.Text(), -1) {
					rel, _ := filepath.Rel(root, path)
					pins = append(pins, pin{where: fmt.Sprintf("%s:%d", rel, n), version: m[1], digest: m[2]})
				}
			}
			return sc.Err()
		})
		require.NoError(t, err)
	}
	return pins
}

// checkPins returns the pins that aren't an accept line of m.
func checkPins(m *Manifest, pins []pin) []string {
	var bad []string
	for _, p := range pins {
		ok := false
		for _, a := range m.Accept {
			if a.Version == p.version && a.Digest == p.digest {
				ok = true
			}
		}
		if !ok {
			bad = append(bad, fmt.Sprintf("%s: %s@%s is not an accept line of engines.txt", p.where, p.version, p.digest))
		}
	}
	return bad
}

// TestPinsMatchManifest: every digest-pinned stock engine the remote
// cache's docs, scripts, harness and workflows name is an accept line, with
// its digest.
func TestPinsMatchManifest(t *testing.T) {
	pins := findPins(t, repoRoot)
	// The docs' examples are pins too: the scan must see them, or it checks
	// nothing.
	require.NotEmpty(t, pins, "no pinned engine found: the scan is broken")
	bad := checkPins(Embedded(), pins)
	require.Empty(t, bad, "pins that disagree with cmd/cacheservice/compat/engines.txt:\n%s", strings.Join(bad, "\n"))
}

// TestPinsCheckFails: a pin that disagrees with the manifest, in version or
// in digest, is reported.
func TestPinsCheckFails(t *testing.T) {
	root := t.TempDir()
	for _, d := range pinDirs {
		require.NoError(t, os.MkdirAll(filepath.Join(root, d), 0o755))
	}
	dir := filepath.Join(root, "hack/remote-cache")
	m := Embedded()
	good := m.Default().Image()
	otherDigest := EngineRepository + ":" + m.Default().Version + "@sha256:" + strings.Repeat("0", 64)
	otherVersion := EngineRepository + ":v0.0.1@" + m.Default().Digest
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.md"), []byte(good+"\n`"+otherDigest+"`\n  engine-image: "+otherVersion+"\n"), 0o644))
	bad := checkPins(m, findPins(t, root))
	require.Len(t, bad, 2, "%v", bad)
	require.Contains(t, bad[0], "hack/remote-cache/x.md:2:")
	require.Contains(t, bad[1], "hack/remote-cache/x.md:3: v0.0.1@")
}

// TestWorkflowPinFromManifest: the remote cache workflow copies no pin. It
// sets no STOCK_ENGINE or STOCK_ENGINE_TAG of its own, and every job that
// uses them reads them from the manifest (engines.sh pin) first.
func TestWorkflowPinFromManifest(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot, ".github/workflows/remote-cache-tests.yml"))
	require.NoError(t, err)
	var wf struct {
		Env  map[string]string `yaml:"env"`
		Jobs map[string]struct {
			Env   map[string]string `yaml:"env"`
			Steps []struct {
				Name string            `yaml:"name"`
				Run  string            `yaml:"run"`
				With map[string]string `yaml:"with"`
				Env  map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(data, &wf))
	for _, name := range []string{"STOCK_ENGINE", "STOCK_ENGINE_TAG"} {
		require.NotContains(t, wf.Env, name, "the workflow's env sets %s: read it from engines.txt (engines.sh pin)", name)
	}
	users := 0
	for job, j := range wf.Jobs {
		for _, name := range []string{"STOCK_ENGINE", "STOCK_ENGINE_TAG"} {
			require.NotContains(t, j.Env, name, "job %s sets %s", job, name)
		}
		pinned := false
		for _, s := range j.Steps {
			text := s.Run
			for _, v := range s.With {
				text += "\n" + v
			}
			for k, v := range s.Env {
				require.NotEqual(t, "STOCK_ENGINE", k, "job %s, step %q sets STOCK_ENGINE", job, s.Name)
				text += "\n" + v
			}
			if strings.Contains(s.Run, "engines.sh pin") && strings.Contains(s.Run, "GITHUB_ENV") {
				pinned = true
				continue
			}
			if strings.Contains(text, "STOCK_ENGINE") {
				users++
				require.True(t, pinned, "job %s, step %q uses STOCK_ENGINE before a step reads it from engines.txt (engines.sh pin >> $GITHUB_ENV)", job, s.Name)
			}
		}
	}
	require.NotZero(t, users, "no step uses STOCK_ENGINE: the check is broken")
}

func runScript(t *testing.T, file string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(filepath.Join(repoRoot, "hack/remote-cache/engines.sh"), args...)
	cmd.Env = append(os.Environ(), "ENGINES_FILE="+file)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestEnginesScriptMatchesParser: engines.sh, which CI reads the pin with
// before Go is set up, gives the parser's first accept line, and refuses
// what the parser refuses in the fields it reads: the cap above all.
func TestEnginesScriptMatchesParser(t *testing.T) {
	out, err := runScript(t, "engines.txt", "pin")
	require.NoError(t, err, out)
	d := Embedded().Default()
	require.Equal(t, "STOCK_ENGINE="+d.Image()+"\nSTOCK_ENGINE_TAG="+d.Version+"\n", out)

	write := func(n int, extra string) string {
		var b strings.Builder
		b.WriteString("base v1.0.0-beta.25\n")
		for i := range n {
			fmt.Fprintln(&b, acceptLine(fmt.Sprintf("v1.0.%d", i), digestA))
		}
		b.WriteString(extra)
		path := filepath.Join(t.TempDir(), "engines.txt")
		require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
		return path
	}
	out, err = runScript(t, write(MaxAccept, ""), "pin")
	require.NoError(t, err, out)
	require.Contains(t, out, "STOCK_ENGINE_TAG=v1.0.0\n")

	// A last line without a newline is a line to both.
	noNewline := filepath.Join(t.TempDir(), "engines.txt")
	require.NoError(t, os.WriteFile(noNewline, []byte("base v1.0.0-beta.25\n"+acceptLine("v1.0.0-beta.25", digestA)), 0o644))
	_, err = Parse(mustRead(t, noNewline))
	require.NoError(t, err)
	out, err = runScript(t, noNewline, "pin")
	require.NoError(t, err, out)
	require.Contains(t, out, "STOCK_ENGINE_TAG=v1.0.0-beta.25\n")

	for name, path := range map[string]string{
		"over the cap": write(MaxAccept+1, ""),
		"no accept":    write(0, ""),
		"bad digest":   write(0, "accept v1.0.0 sha256:xyz verified: "+runA+"\n"),
		"bad version":  write(0, "accept 1.0.0 "+digestA+" verified: "+runA+"\n"),
	} {
		t.Run(name, func(t *testing.T) {
			out, err := runScript(t, path, "pin")
			require.Error(t, err, out)
			_, perr := Parse(mustRead(t, path))
			require.Error(t, perr, "the parser takes what the script refuses")
		})
	}
}

// TestEnginesScriptCandidate: a candidate becomes the first accept line,
// verified by the run, and the parser takes the result; a version already
// listed, or a malformed one, is refused.
func TestEnginesScriptCandidate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engines.txt")
	require.NoError(t, os.WriteFile(path, embedded, 0o644))
	t.Setenv("GITHUB_SERVER_URL", "https://github.com")
	t.Setenv("GITHUB_REPOSITORY", "zerobull-consulting/dagger")
	t.Setenv("GITHUB_RUN_ID", "123")
	out, err := runScript(t, path, "candidate", "v9.9.9-beta.1", digestB)
	require.NoError(t, err, out)
	m, err := Parse(mustRead(t, path))
	require.NoError(t, err)
	require.Equal(t, Accepted{Version: "v9.9.9-beta.1", Digest: digestB, Verified: "https://github.com/zerobull-consulting/dagger/actions/runs/123"}, m.Default())
	require.Equal(t, append([]string{"v9.9.9-beta.1"}, Embedded().Versions()...), m.Versions())

	for name, args := range map[string][]string{
		"listed":      {"candidate", Embedded().Default().Version, digestB},
		"bad version": {"candidate", "v1; true", digestB},
		"bad digest":  {"candidate", "v9.9.9", "sha256:abc"},
	} {
		t.Run(name, func(t *testing.T) {
			before := mustRead(t, path)
			out, err := runScript(t, path, args...)
			require.Error(t, err, out)
			require.Equal(t, before, mustRead(t, path), "a refused candidate changed the manifest")
		})
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
