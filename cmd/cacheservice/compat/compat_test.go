package compat

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	digestA = "sha256:39abecfe0eba4c91035dcfc9f72c8a3963ae60481326082b48d78f2230202834"
	digestB = "sha256:3ad2b0b80942bb5044e57c1765710a5fab0b913557b6ab7d7f4e81700ac4da35"
	runA    = "https://github.com/zerobull-consulting/dagger/actions/runs/42287640433"
)

func acceptLine(v, d string) string { return "accept " + v + " " + d + " verified: " + runA }

// TestEmbeddedParses: the manifest built into the binary parses, so the
// service starts, and names its base.
func TestEmbeddedParses(t *testing.T) {
	_, err := Parse(embedded)
	require.NoError(t, err)
	m := Embedded()
	require.True(t, ValidVersion(m.Base))
	require.NotEmpty(t, m.Accept)
	require.LessOrEqual(t, len(m.Accept), MaxAccept)
}

func TestParse(t *testing.T) {
	m, err := Parse([]byte(strings.Join([]string{
		"# a comment",
		"",
		"base v1.0.0-beta.25",
		"  accept v1.0.0-beta.25 " + digestA + " verified: " + runA + "  ",
		"accept v1.0.0-beta.24 " + digestB + " verified: " + runA + "/attempts/2",
		"reject v1.0.0-beta.23 zerobull-consulting/dagger#117",
	}, "\n")))
	require.NoError(t, err)
	require.Equal(t, "v1.0.0-beta.25", m.Base)
	require.Equal(t, []string{"v1.0.0-beta.25", "v1.0.0-beta.24"}, m.Versions())
	require.Equal(t, Accepted{Version: "v1.0.0-beta.25", Digest: digestA, Verified: runA}, m.Default())
	require.Equal(t, "registry.dagger.io/engine:v1.0.0-beta.25@"+digestA, m.Default().Image())
	require.Equal(t, []Rejected{{Version: "v1.0.0-beta.23", Issue: "zerobull-consulting/dagger#117"}}, m.Reject)
}

// TestParseRefuses: every malformed manifest is refused, with the line.
func TestParseRefuses(t *testing.T) {
	base := "base v1.0.0-beta.25\n"
	ok := acceptLine("v1.0.0-beta.25", digestA) + "\n"
	for _, tc := range []struct{ name, data, want string }{
		{"empty", "", "no base line"},
		{"no base", ok, "no base line"},
		{"no accept", base, "no accept line"},
		{"only a reject", base + "reject v1.0.0-beta.24 o/r#1\n", "no accept line"},
		{"two bases", base + base + ok, "engines.txt:2: a second base line"},
		{"bad base", "base main\n" + ok, `base "main" is not a release version`},
		{"base with extra field", "base v1.0.0-beta.25 x\n" + ok, "want 'base"},
		{"unknown line", base + ok + "allow v1.0.0-beta.24\n", `engines.txt:3: unknown line "allow"`},
		{"capitalized keyword", base + "Accept v1.0.0-beta.25 " + digestA + " verified: " + runA + "\n", "unknown line"},
		{"duplicate accept", base + ok + ok, "engines.txt:3: v1.0.0-beta.25 is on line 2 already"},
		{"accepted and rejected", base + ok + "reject v1.0.0-beta.25 o/r#1\n", "is on line 2 already"},
		{"duplicate reject", base + ok + "reject v1.0.0-beta.2 o/r#1\nreject v1.0.0-beta.2 o/r#2\n", "is on line 3 already"},
		{"bad digest: short", base + acceptLine("v1.0.0-beta.25", digestA[:70]), "is not a sha256 digest"},
		{"bad digest: upper case", base + acceptLine("v1.0.0-beta.25", strings.ToUpper(digestA)), "is not a sha256 digest"},
		{"bad digest: other algorithm", base + acceptLine("v1.0.0-beta.25", "sha512:"+digestA[7:]), "is not a sha256 digest"},
		{"bad version: no v", base + acceptLine("1.0.0-beta.25", digestA), "is not a release version"},
		{"bad version: build metadata", base + acceptLine("v1.0.0-beta.25+dirty", digestA), "is not a release version"},
		{"bad version: range", base + acceptLine(">=v1.0.0", digestA), "is not a release version"},
		{"no verified run", base + "accept v1.0.0-beta.25 " + digestA + "\n", "want 'accept"},
		{"verified: not a run", base + "accept v1.0.0-beta.25 " + digestA + " verified: code-reading\n", "is not a GitHub Actions run URL"},
		{"verified: missing colon", base + "accept v1.0.0-beta.25 " + digestA + " verified " + runA + "\n", "want 'accept"},
		{"reject without issue", base + ok + "reject v1.0.0-beta.24\n", "want 'reject"},
		{"reject: bare issue number", base + ok + "reject v1.0.0-beta.24 #12\n", "is not an issue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.data))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// TestParseCap: MaxAccept accept lines parse, one more doesn't.
func TestParseCap(t *testing.T) {
	lines := func(n int) []byte {
		var b strings.Builder
		b.WriteString("base v1.0.0-beta.25\n")
		for i := range n {
			fmt.Fprintln(&b, acceptLine(fmt.Sprintf("v1.0.%d", i), digestA))
		}
		return []byte(b.String())
	}
	m, err := Parse(lines(MaxAccept))
	require.NoError(t, err)
	require.Len(t, m.Accept, MaxAccept)
	_, err = Parse(lines(MaxAccept + 1))
	require.ErrorContains(t, err, fmt.Sprintf("engines.txt:%d: more than %d accept lines", MaxAccept+2, MaxAccept))
	require.Equal(t, 100, MaxAccept, "the cap is D1's 100")
}

// TestAcceptsExactly: only an exact accept line accepts; nothing near it.
func TestAcceptsExactly(t *testing.T) {
	m, err := Parse([]byte("base v1.0.0-beta.25\n" + acceptLine("v1.0.0-beta.25", digestA) + "\nreject v1.0.0-beta.23 o/r#1\n"))
	require.NoError(t, err)
	require.True(t, m.Accepts("v1.0.0-beta.25"))
	for _, v := range []string{
		"", "v1.0.0-beta.2", "v1.0.0-beta.250", "v1.0.0-beta.25+dirty", "v1.0.0-beta.25-dev-abc",
		"1.0.0-beta.25", "V1.0.0-beta.25", " v1.0.0-beta.25", "v1.0.0-beta.23", "v1.0.0-beta.24",
	} {
		require.False(t, m.Accepts(v), "%q", v)
	}
}

func TestBuildBase(t *testing.T) {
	m := &Manifest{Base: "v1.0.0-beta.25"}
	old := Release
	t.Cleanup(func() { Release = old })

	Release = ""
	b, err := BuildBase(m)
	require.NoError(t, err)
	require.Equal(t, Dev, b)

	Release = "v1.0.0-beta.25"
	b, err = BuildBase(m)
	require.NoError(t, err)
	require.Equal(t, "v1.0.0-beta.25", b)

	Release = "v1.0.0-beta.24"
	_, err = BuildBase(m)
	require.ErrorContains(t, err, `stamped as release "v1.0.0-beta.24"`)
}
