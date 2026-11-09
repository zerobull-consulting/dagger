package server

// The engine accept list (zerobull-consulting/dagger#117, D1-D3): the
// service welcomes only the engines whose hello names a version on one of
// its manifest's accept lines (cmd/cacheservice/compat/engines.txt), and
// reports its identity in the welcome, the debug API and its refusals.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/dagger/dagger/cmd/cacheservice/compat"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// CloseEngineVersionRefused is the WebSocket close status of a connection
// refused before its welcome because the engine's version isn't accepted.
// It is in the application range (4000-4999), next to
// protocol.CloseTokenRevoked (4001); the engine's client treats it as any
// other close: it reconnects with its backoff, which an unwelcomed
// connection doesn't reset, so it retries every 30s at most.
const CloseEngineVersionRefused = 4002

// The log messages of a welcome and of a refusal for the engine's version.
// The sidecar action's start waits for one or the other
// (.github/actions/remote-cache/sidecar.sh;
// zerobull-consulting/dagger#146), and TestSidecarWaitsForTheseLines keeps
// the two in step: change them together.
const (
	LogEngineWelcomed = "engine welcomed"
	LogEngineRefused  = "engine refused: its version is not accepted"
)

// refusalWriteTimeout bounds the refusal's error reply.
const refusalWriteTimeout = 5 * time.Second

// maxQuotedVersion bounds an engine-supplied version where the service
// repeats it, in logs, close reasons and refusals.
const maxQuotedVersion = 64

// maxCloseReason is the longest close reason a WebSocket close frame holds.
const maxCloseReason = 123

// errEngineVersionRefused ends a connection refused for its version.
var errEngineVersionRefused = errors.New("engine version not accepted: refused before welcome")

// enginePolicy is what the service accepts, and who it says it is.
type enginePolicy struct {
	manifest *compat.Manifest
	identity compat.Identity
	// allowMismatch is -allow-engine-version-mismatch: development only.
	allowMismatch atomic.Bool
}

func newEnginePolicy(m *compat.Manifest, base string) *enginePolicy {
	return &enginePolicy{
		manifest: m,
		identity: compat.Identity{Base: base, Accept: m.Versions(), Revision: ownership.Revision()},
	}
}

// defaultEnginePolicy is the embedded manifest's, strict. A build stamped
// as another release than the manifest's base reports Dev here; main
// refuses to start with it (compat.BuildBase).
func defaultEnginePolicy() *enginePolicy {
	m := compat.Embedded()
	base, err := compat.BuildBase(m)
	if err != nil {
		base = compat.Dev
	}
	return newEnginePolicy(m, base)
}

// Identity is the service's identity: its base release (or "dev"), the
// engine versions it accepts and its revision.
func (s *Service) Identity() compat.Identity {
	id := s.engines.identity
	id.Accept = append([]string(nil), id.Accept...)
	return id
}

// AllowEngineVersionMismatch welcomes engines of any version, logging each
// whose version isn't accepted: -allow-engine-version-mismatch, for
// development against engines built from a checkout. The default is strict.
func (s *Service) AllowEngineVersionMismatch() {
	s.engines.allowMismatch.Store(true)
	s.logger.Warn("ENGINE VERSION CHECK OFF: -allow-engine-version-mismatch welcomes engines of any version; development only, never in production",
		"accept", s.engines.identity.Accept)
}

// useManifest replaces the manifest, for tests.
func (s *Service) useManifest(m *compat.Manifest) {
	p := newEnginePolicy(m, s.engines.identity.Base)
	p.allowMismatch.Store(s.engines.allowMismatch.Load())
	s.engines = p
}

// checkEngineVersion decides on a connection's hello: it returns false, once
// it has refused and closed the connection, for a version that isn't
// accepted, unless mismatches are allowed.
func (c *engineConn) checkEngineVersion(ctx context.Context, hello protocol.Envelope) bool {
	p := c.svc.engines
	v := c.hello.EngineVersion
	if p.manifest.Accepts(v) {
		c.versionAccepted = true
		return true
	}
	if p.allowMismatch.Load() {
		c.svc.metrics.EngineVersionMismatch(false)
		c.logger.Warn("ENGINE VERSION NOT ACCEPTED, welcomed anyway: -allow-engine-version-mismatch is set (development only)",
			"engineVersion", boundVersion(v), "accept", p.identity.Accept)
		return true
	}
	c.svc.metrics.EngineVersionMismatch(true)
	msg := refusalMessage(v, p.identity, p.manifest)
	c.logger.Warn(LogEngineRefused,
		"engineVersion", boundVersion(v), "accept", p.identity.Accept,
		"base", p.identity.Base, "closeStatus", CloseEngineVersionRefused)
	// The whole refusal as the hello's error reply, which the engine logs
	// ("remote cache connection ended; reconnecting"); the close reason
	// holds only what fits in a close frame.
	wctx, cancel := context.WithTimeout(ctx, refusalWriteTimeout)
	_ = c.replyError(wctx, hello, msg)
	cancel()
	_ = c.conn.Close(websocket.StatusCode(CloseEngineVersionRefused), closeReason(v, p.identity))
	return false
}

// refusalMessage tells a refused engine what it sent, what the service
// accepts and what to run instead.
func refusalMessage(v string, id compat.Identity, m *compat.Manifest) string {
	var b strings.Builder
	if v == "" {
		b.WriteString("the engine sent no version; ")
	} else {
		fmt.Fprintf(&b, "engine version %q is not accepted; ", boundVersion(v))
	}
	fmt.Fprintf(&b, "this cache service (base %s, revision %s) accepts only %s",
		id.Base, id.Revision, strings.Join(id.Accept, ", "))
	switch r, rejected := m.Rejected(v); {
	case rejected:
		// Tried and failed: no service accepts it until the issue is fixed.
		fmt.Fprintf(&b, ". Version %q was tried and rejected: see %s. Run an engine of an accepted version", boundVersion(v), r.Issue)
	case compat.ValidVersion(v) && len(v) <= maxQuotedVersion:
		// Only a bounded version of a release's shape names an image tag.
		fmt.Fprintf(&b, ". Run the cache service dagger-remote-cache:%s once that version is verified, or an engine of an accepted version", v)
	default:
		b.WriteString(". Run an engine of an accepted version")
	}
	return b.String()
}

// closeReason is the refusal in at most a close frame's 123 bytes.
func closeReason(v string, id compat.Identity) string {
	r := fmt.Sprintf("engine version %q not accepted; accepts %s", boundVersion(v), strings.Join(id.Accept, " "))
	return cutString(r, maxCloseReason)
}

// boundVersion is an engine-supplied version cut to maxQuotedVersion bytes.
func boundVersion(v string) string { return cutString(v, maxQuotedVersion) }

// welcome is the welcome's body: protocol.Welcome, which has no fields,
// and the service's identity. The engine's client doesn't decode the
// welcome's body (it checks only the reply's type), and its envelope
// decoding takes unknown fields, so the identity reaches a stock engine
// without changing the shared protocol (TestWelcomeIdentityStockClient).
type welcome struct {
	protocol.Welcome
	Service compat.Identity `json:"service"`
}
