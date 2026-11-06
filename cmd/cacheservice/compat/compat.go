// Package compat is the cache service's engine compatibility manifest
// (engines.txt, embedded): the upstream release the service is built from,
// the engine versions it accepts, each with the stock engine image's digest
// and the CI run that verified it, and the versions that were tried and
// rejected. See engines.txt for the format and
// zerobull-consulting/dagger#117 (D1-D3) for the policy.
package compat

import (
	"bufio"
	"bytes"
	_ "embed"
	"fmt"
	"regexp"
	"strings"
)

// MaxAccept is the most accept lines a manifest may have: a sanity limit.
// The list resets whenever the bundle or the protocol version changes, so
// in practice it stays short.
const MaxAccept = 100

// EngineRepository is where the stock engine images are published.
const EngineRepository = "registry.dagger.io/engine"

// Dev is the base a build that isn't stamped as its manifest's base
// release reports: one that tracks upstream main.
const Dev = "dev"

//go:embed engines.txt
var embedded []byte

var (
	versionRE  = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$`)
	digestRE   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	verifiedRE = regexp.MustCompile(`^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(/attempts/[0-9]+)?$`)
	issueRE    = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+$`)
)

// ValidVersion reports whether v has the shape of an upstream release tag,
// such as v1.0.0-beta.25: what an accept, reject or base line may name. A
// dev engine's version (with a +build or a -dev suffix of a commit) passes
// or fails this shape alike; only an exact accept line accepts it.
func ValidVersion(v string) bool { return versionRE.MatchString(v) }

// Accepted is an accept line.
type Accepted struct {
	// Version is the engine's version, as its hello names it.
	Version string
	// Digest is the stock engine image's multi-platform index digest.
	Digest string
	// Verified is the CI run that verified the version.
	Verified string
}

// Image is the stock engine image of the version, pinned by digest.
func (a Accepted) Image() string {
	return EngineRepository + ":" + a.Version + "@" + a.Digest
}

// Rejected is a reject line.
type Rejected struct {
	Version string
	// Issue explains why, as owner/repo#N.
	Issue string
}

// Manifest is a parsed engines.txt.
type Manifest struct {
	// Base is the upstream release the service is built from.
	Base string
	// Accept are the accepted versions, in the file's order; the first is
	// the one CI runs by default.
	Accept []Accepted
	Reject []Rejected
}

// Parse parses a manifest strictly: an unknown or malformed line, a
// missing or repeated base, a version on two lines, no accept line, or
// more than MaxAccept of them is an error naming the line.
func Parse(data []byte) (*Manifest, error) {
	m := &Manifest{}
	seen := map[string]int{}
	baseLine := 0
	sc := bufio.NewScanner(bytes.NewReader(data))
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		errf := func(format string, args ...any) error {
			return fmt.Errorf("engines.txt:%d: %s", n, fmt.Sprintf(format, args...))
		}
		version := func(v string) error {
			if !ValidVersion(v) {
				return errf("%q is not a release version such as v1.0.0-beta.25", v)
			}
			if prev, ok := seen[v]; ok {
				return errf("%s is on line %d already", v, prev)
			}
			seen[v] = n
			return nil
		}
		switch fields[0] {
		case "base":
			if len(fields) != 2 {
				return nil, errf("want 'base <upstream tag>'")
			}
			if baseLine != 0 {
				return nil, errf("a second base line (the first is line %d)", baseLine)
			}
			if !ValidVersion(fields[1]) {
				return nil, errf("base %q is not a release version", fields[1])
			}
			baseLine = n
			m.Base = fields[1]
		case "accept":
			if len(fields) != 5 || fields[3] != "verified:" {
				return nil, errf("want 'accept <version> <digest> verified: <CI run URL>'")
			}
			if err := version(fields[1]); err != nil {
				return nil, err
			}
			if !digestRE.MatchString(fields[2]) {
				return nil, errf("%q is not a sha256 digest", fields[2])
			}
			if !verifiedRE.MatchString(fields[4]) {
				return nil, errf("%q is not a GitHub Actions run URL", fields[4])
			}
			m.Accept = append(m.Accept, Accepted{Version: fields[1], Digest: fields[2], Verified: fields[4]})
			if len(m.Accept) > MaxAccept {
				return nil, errf("more than %d accept lines", MaxAccept)
			}
		case "reject":
			if len(fields) != 3 {
				return nil, errf("want 'reject <version> <owner/repo#issue>'")
			}
			if err := version(fields[1]); err != nil {
				return nil, err
			}
			if !issueRE.MatchString(fields[2]) {
				return nil, errf("%q is not an issue such as zerobull-consulting/dagger#117", fields[2])
			}
			m.Reject = append(m.Reject, Rejected{Version: fields[1], Issue: fields[2]})
		default:
			return nil, errf("unknown line %q: want base, accept or reject", fields[0])
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("engines.txt: %w", err)
	}
	if baseLine == 0 {
		return nil, fmt.Errorf("engines.txt: no base line")
	}
	if len(m.Accept) == 0 {
		return nil, fmt.Errorf("engines.txt: no accept line")
	}
	return m, nil
}

var embeddedManifest, embeddedErr = Parse(embedded)

// Embedded is the manifest built into the binary. It panics if the
// manifest doesn't parse, which the package's tests rule out.
func Embedded() *Manifest {
	if embeddedErr != nil {
		panic(embeddedErr)
	}
	return embeddedManifest
}

// Accepts reports whether v is exactly one of the accepted versions. There
// are no ranges, prefixes or normalization: "v1.0.0-beta.25+dirty" isn't
// "v1.0.0-beta.25".
func (m *Manifest) Accepts(v string) bool {
	for _, a := range m.Accept {
		if a.Version == v {
			return true
		}
	}
	return false
}

// Rejected returns v's reject line, if it has one.
func (m *Manifest) Rejected(v string) (Rejected, bool) {
	for _, r := range m.Reject {
		if r.Version == v {
			return r, true
		}
	}
	return Rejected{}, false
}

// Versions are the accepted versions, in the file's order.
func (m *Manifest) Versions() []string {
	vs := make([]string, len(m.Accept))
	for i, a := range m.Accept {
		vs[i] = a.Version
	}
	return vs
}

// Default is the first accept line: the engine CI, the e2e harness and the
// demo run unless told otherwise.
func (m *Manifest) Default() Accepted { return m.Accept[0] }

// Release is the upstream release this build is stamped as (-ldflags -X;
// the image's RELEASE build argument). Empty, the build is a dev build.
var Release = ""

// BuildBase is the base this build reports: the manifest's base when the
// build is stamped as that release, Dev when it isn't stamped. A build
// stamped as another release than the manifest's base is an error: its
// identity would be a lie.
func BuildBase(m *Manifest) (string, error) {
	switch Release {
	case "":
		return Dev, nil
	case m.Base:
		return m.Base, nil
	default:
		return "", fmt.Errorf("the build is stamped as release %q, but its engines.txt's base is %s", Release, m.Base)
	}
}

// Identity is the service's identity as it reports it: in the welcome, the
// debug API, refusals and its startup log.
type Identity struct {
	// Base is the upstream release the service is built from, or Dev.
	Base string `json:"base"`
	// Accept are the engine versions it accepts.
	Accept []string `json:"accept"`
	// Revision is the commit it is built from (ownership.Revision).
	Revision string `json:"revision"`
}
