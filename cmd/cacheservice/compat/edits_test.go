package compat

// The release check (hack/remote-cache/release-check.sh,
// zerobull-consulting/dagger#117, D5) edits the manifest only through
// engines.sh. Every edit it makes must leave a manifest this package's
// parser takes, with the change it asked for; an edit the parser would
// refuse must write nothing.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const runB = "https://github.com/zerobull-consulting/dagger/actions/runs/42300000000"

func TestEnginesScriptEditsParse(t *testing.T) {
	start := "# a comment\nbase v1.0.0-beta.25\n\n" +
		acceptLine("v1.0.0-beta.25", digestA) + "\n" +
		acceptLine("v1.0.0-beta.24", digestB) + "\n" +
		"reject v1.0.0-beta.23 zerobull-consulting/dagger#1\n"
	fresh := func(t *testing.T) string {
		path := filepath.Join(t.TempDir(), "engines.txt")
		require.NoError(t, os.WriteFile(path, []byte(start), 0o644))
		return path
	}
	parse := func(t *testing.T, path string) *Manifest {
		m, err := Parse(mustRead(t, path))
		require.NoError(t, err)
		return m
	}

	t.Run("add-accept", func(t *testing.T) {
		path := fresh(t)
		out, err := runScript(t, path, "add-accept", "v1.0.0-beta.26", digestB, runB)
		require.NoError(t, err, out)
		m := parse(t, path)
		require.Equal(t, Accepted{Version: "v1.0.0-beta.26", Digest: digestB, Verified: runB}, m.Accept[0])
		require.Len(t, m.Accept, 3)
		require.Equal(t, "v1.0.0-beta.25", m.Base)
		require.Contains(t, string(mustRead(t, path)), "# a comment\n", "comments are kept")
	})
	t.Run("add-reject", func(t *testing.T) {
		path := fresh(t)
		out, err := runScript(t, path, "add-reject", "v1.0.0-beta.26", "zerobull-consulting/dagger#200")
		require.NoError(t, err, out)
		m := parse(t, path)
		require.Equal(t, Rejected{Version: "v1.0.0-beta.26", Issue: "zerobull-consulting/dagger#200"}, m.Reject[len(m.Reject)-1])
		require.Len(t, m.Accept, 2)
	})
	t.Run("set-base", func(t *testing.T) {
		path := fresh(t)
		out, err := runScript(t, path, "set-base", "v1.0.0-beta.26")
		require.NoError(t, err, out)
		require.Equal(t, "v1.0.0-beta.26", parse(t, path).Base)
	})
	t.Run("reset-accept", func(t *testing.T) {
		path := fresh(t)
		out, err := runScript(t, path, "reset-accept", "v1.0.0-beta.26", digestB, runB)
		require.NoError(t, err, out)
		m := parse(t, path)
		require.Equal(t, []Accepted{{Version: "v1.0.0-beta.26", Digest: digestB, Verified: runB}}, m.Accept)
		require.Len(t, m.Reject, 1, "reject lines stay")
	})
	t.Run("listed", func(t *testing.T) {
		out, err := runScript(t, fresh(t), "listed")
		require.NoError(t, err, out)
		require.Equal(t, "v1.0.0-beta.25\nv1.0.0-beta.25\nv1.0.0-beta.24\nv1.0.0-beta.23\n", out)
	})

	// Refused, and nothing written.
	for name, args := range map[string][]string{
		"accept twice":       {"add-accept", "v1.0.0-beta.24", digestB, runB},
		"accept a rejected":  {"add-accept", "v1.0.0-beta.23", digestB, runB},
		"reject an accepted": {"add-reject", "v1.0.0-beta.25", "zerobull-consulting/dagger#2"},
		"bad digest":         {"add-accept", "v1.0.0-beta.26", "sha256:xyz", runB},
		"bad run":            {"add-accept", "v1.0.0-beta.26", digestB, "https://example.com/runs/1"},
		"bad issue":          {"add-reject", "v1.0.0-beta.26", "#2"},
		"bad base":           {"set-base", "v1.0.0-beta.26; true"},
		"bad reset":          {"reset-accept", "1.0.0", digestB, runB},
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			path := fresh(t)
			out, err := runScript(t, path, args...)
			require.Error(t, err, out)
			require.Equal(t, start, string(mustRead(t, path)), "a refused edit writes nothing")
		})
	}
}

// TestEnginesScriptCandidateAgain: a candidate that is already accepted,
// with its own digest, is verified again: its line moves first, cited by
// the run, and the list keeps its length. With another digest it is
// refused.
func TestEnginesScriptCandidateAgain(t *testing.T) {
	start := "base v1.0.0-beta.25\n" +
		acceptLine("v1.0.0-beta.25", digestA) + "\n" +
		acceptLine("v1.0.0-beta.24", digestB) + "\n"
	path := filepath.Join(t.TempDir(), "engines.txt")
	require.NoError(t, os.WriteFile(path, []byte(start), 0o644))
	env := []string{"GITHUB_SERVER_URL=https://github.com", "GITHUB_REPOSITORY=zerobull-consulting/dagger", "GITHUB_RUN_ID=42300000000"}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		t.Setenv(k, v)
	}
	out, err := runScript(t, path, "candidate", "v1.0.0-beta.24", digestB)
	require.NoError(t, err, out)
	m, err := Parse(mustRead(t, path))
	require.NoError(t, err)
	require.Equal(t, []Accepted{
		{Version: "v1.0.0-beta.24", Digest: digestB, Verified: runB},
		{Version: "v1.0.0-beta.25", Digest: digestA, Verified: runA},
	}, m.Accept)

	require.NoError(t, os.WriteFile(path, []byte(start), 0o644))
	out, err = runScript(t, path, "candidate", "v1.0.0-beta.24", digestA)
	require.Error(t, err, out)
	require.Equal(t, start, string(mustRead(t, path)))
}

// TestEnginesScriptCandidateAgainOnly: verifying the manifest's only accept
// line again (after a reset, say) keeps it.
func TestEnginesScriptCandidateAgainOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engines.txt")
	require.NoError(t, os.WriteFile(path, []byte("base v1.0.0-beta.25\n"+acceptLine("v1.0.0-beta.25", digestA)+"\n"), 0o644))
	t.Setenv("GITHUB_SERVER_URL", "https://github.com")
	t.Setenv("GITHUB_REPOSITORY", "zerobull-consulting/dagger")
	t.Setenv("GITHUB_RUN_ID", "42300000000")
	out, err := runScript(t, path, "candidate", "v1.0.0-beta.25", digestA)
	require.NoError(t, err, out)
	m, err := Parse(mustRead(t, path))
	require.NoError(t, err)
	require.Equal(t, []Accepted{{Version: "v1.0.0-beta.25", Digest: digestA, Verified: runB}}, m.Accept)
}
