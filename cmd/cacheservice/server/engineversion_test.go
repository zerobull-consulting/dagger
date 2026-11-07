package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/dagger/dagger/cmd/cacheservice/compat"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/cmd/cacheservice/metrics/metricstest"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// testEngineVersion is the version the tests' engines send: the embedded
// manifest's first accept line, so they are welcomed as a stock engine of
// that release is.
var testEngineVersion = compat.Embedded().Default().Version

const (
	testDigest   = "sha256:39abecfe0eba4c91035dcfc9f72c8a3963ae60481326082b48d78f2230202834"
	testVerified = "https://github.com/zerobull-consulting/dagger/actions/runs/42287640433"
)

// testManifest accepts v1.0.0-beta.25 and v1.0.0-beta.24 and rejects
// v1.0.0-beta.23.
func testManifest(t *testing.T) *compat.Manifest {
	t.Helper()
	m, err := compat.Parse([]byte(strings.Join([]string{
		"base v1.0.0-beta.25",
		"accept v1.0.0-beta.25 " + testDigest + " verified: " + testVerified,
		"accept v1.0.0-beta.24 " + testDigest + " verified: " + testVerified,
		"reject v1.0.0-beta.23 zerobull-consulting/dagger#117",
	}, "\n")))
	require.NoError(t, err)
	return m
}

func helloEnvelope(t *testing.T, version string) protocol.Envelope {
	t.Helper()
	env, err := protocol.NewEnvelope(1, 0, protocol.TypeHello, protocol.Hello{CacheID: "cache-v", Generation: 1, EngineVersion: version, EngineName: "e"})
	require.NoError(t, err)
	return env
}

// levels are the levels of every record of rec with the message msg.
func levels(rec *logRecorder, msg string) []slog.Level {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var ls []slog.Level
	for _, r := range rec.records {
		if r.Message == msg {
			ls = append(ls, r.Level)
		}
	}
	return ls
}

func mismatches(t *testing.T, svc *Service, outcome string) float64 {
	t.Helper()
	return metricstest.Value(t, svc.registry, "cacheservice_engine_version_mismatch_total", map[string]string{"outcome": outcome})
}

// An accepted version is welcomed, and the welcome carries the service's
// identity.
func TestEngineVersionAccepted(t *testing.T) {
	for _, v := range []string{"v1.0.0-beta.25", "v1.0.0-beta.24"} {
		t.Run(v, func(t *testing.T) {
			svc, rec, srv := newTestService(t, "")
			svc.useManifest(testManifest(t))
			e := dialRaw(t, srv, "tok")
			e.send(helloEnvelope(t, v))
			w := e.next()
			require.Equal(t, protocol.TypeWelcome, w.Type)
			var body struct {
				Service compat.Identity `json:"service"`
			}
			require.NoError(t, json.Unmarshal(w.Body, &body))
			require.Equal(t, []string{"v1.0.0-beta.25", "v1.0.0-beta.24"}, body.Service.Accept)
			require.Equal(t, svc.Identity(), body.Service)
			require.NotEmpty(t, body.Service.Base)
			require.NotEmpty(t, body.Service.Revision)
			c := waitEngines(t, svc, 1)[0]
			require.True(t, c.versionAccepted)
			require.True(t, svc.Engines()[0].VersionAccepted)
			require.Zero(t, mismatches(t, svc, "refused"))
			// The line the sidecar action's start waits for
			// (zerobull-consulting/dagger#146), logged after the welcome.
			waitUntil(t, 5*time.Second, "the welcome logged", func() bool {
				return len(rec.find("engine welcomed")) == 1
			})
			require.Equal(t, []slog.Level{slog.LevelInfo}, levels(rec, "engine welcomed"))
			got := rec.find("engine welcomed")[0]
			require.Equal(t, v, got["engineVersion"])
			require.Equal(t, "true", got["versionAccepted"])
		})
	}
}

// Every other version is refused before the welcome: closed with
// CloseEngineVersionRefused, after an error reply naming both sides,
// logged and counted, and never registered.
func TestEngineVersionRefused(t *testing.T) {
	for _, tc := range []struct{ name, version string }{
		{"empty", ""},
		{"dirty suffix", "v1.0.0-beta.25+dirty"},
		{"dev suffix", "v1.0.0-beta.26-dev-0123456789ab"},
		{"rejected", "v1.0.0-beta.23"},
		{"unlisted release", "v1.0.0-beta.26"},
		{"prefix of an accepted one", "v1.0.0-beta.2"},
		{"long", strings.Repeat("v", 500)},
		{"long, of a release's shape", "v1.0.0-" + strings.Repeat("a", 500)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, rec, srv := newTestService(t, "")
			svc.useManifest(testManifest(t))
			e := dialRaw(t, srv, "tok")
			e.send(helloEnvelope(t, tc.version))

			reply := e.next()
			require.Equal(t, protocol.TypeError, reply.Type, "hello answered with %+v", reply)
			require.Equal(t, uint64(1), reply.Re)
			msg := decodeBody[protocol.Error](t, reply).Message
			if tc.version == "" {
				require.Contains(t, msg, "the engine sent no version")
			} else {
				require.Contains(t, msg, `engine version "`+boundVersion(tc.version)+`" is not accepted`)
			}
			require.Contains(t, msg, "accepts only v1.0.0-beta.25, v1.0.0-beta.24")
			require.Contains(t, msg, "base "+svc.Identity().Base)
			require.Contains(t, msg, "revision "+svc.Identity().Revision)
			switch {
			case tc.version == "v1.0.0-beta.23":
				// Rejected: the issue, and no hint to wait for a verification.
				require.Contains(t, msg, `Version "v1.0.0-beta.23" was tried and rejected: see zerobull-consulting/dagger#117`)
				require.NotContains(t, msg, "dagger-remote-cache:")
			case compat.ValidVersion(tc.version) && len(tc.version) <= maxQuotedVersion:
				require.Contains(t, msg, "dagger-remote-cache:"+tc.version+" once that version is verified")
			default:
				require.NotContains(t, msg, "dagger-remote-cache:")
			}
			require.Less(t, len(msg), 1024)

			// The close frame: the code and a reason within 123 bytes.
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			_, _, err := e.conn.Read(ctx)
			var ce websocket.CloseError
			require.ErrorAs(t, err, &ce)
			require.Equal(t, websocket.StatusCode(CloseEngineVersionRefused), ce.Code)
			require.LessOrEqual(t, len(ce.Reason), 123)
			require.True(t, strings.HasPrefix(ce.Reason, "engine version "), ce.Reason)

			waitUntil(t, 5*time.Second, "the refusal logged", func() bool {
				return len(rec.find("engine refused: its version is not accepted")) == 1
			})
			require.Equal(t, []slog.Level{slog.LevelWarn}, levels(rec, "engine refused: its version is not accepted"))
			got := rec.find("engine refused: its version is not accepted")[0]
			require.Equal(t, boundVersion(tc.version), got["engineVersion"])
			require.LessOrEqual(t, len(got["engineVersion"]), maxQuotedVersion)
			require.Equal(t, "4002", got["closeStatus"])
			require.Equal(t, float64(1), mismatches(t, svc, "refused"))
			require.Zero(t, mismatches(t, svc, "allowed"))
			require.Empty(t, svc.Engines())
			// Never logged as welcomed: the sidecar's start tells the two
			// apart (zerobull-consulting/dagger#146).
			require.Empty(t, rec.find("engine welcomed"))
		})
	}
}

// The default policy is the embedded manifest, strict: a service from New
// refuses an engine whose version isn't on its accept lines.
func TestEngineVersionStrictByDefault(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	require.Equal(t, compat.Embedded().Versions(), svc.Identity().Accept)
	e := dialRaw(t, srv, "tok")
	e.send(helloEnvelope(t, "v0.0.0-not-accepted"))
	require.Equal(t, protocol.TypeError, e.next().Type)
	require.Equal(t, websocket.StatusCode(CloseEngineVersionRefused), e.closed())
	require.Empty(t, svc.Engines())
}

// With the escape hatch, an unaccepted version is welcomed, logged loudly
// and counted as allowed; the debug API marks it.
func TestEngineVersionMismatchAllowed(t *testing.T) {
	svc, rec, srv := newTestService(t, "")
	svc.useManifest(testManifest(t))
	svc.AllowEngineVersionMismatch()
	// Replacing the manifest keeps the escape hatch.
	svc.useManifest(testManifest(t))
	require.Equal(t, []slog.Level{slog.LevelWarn}, levels(rec, "ENGINE VERSION CHECK OFF: -allow-engine-version-mismatch welcomes engines of any version; development only, never in production"))
	e := dialRaw(t, srv, "tok")
	e.send(helloEnvelope(t, "v1.0.0-beta.26-dev-0123456789ab"))
	require.Equal(t, protocol.TypeWelcome, e.next().Type)
	waitEngines(t, svc, 1)
	got := rec.find("ENGINE VERSION NOT ACCEPTED, welcomed anyway: -allow-engine-version-mismatch is set (development only)")
	require.Len(t, got, 1)
	require.Equal(t, []slog.Level{slog.LevelWarn}, levels(rec, "ENGINE VERSION NOT ACCEPTED, welcomed anyway: -allow-engine-version-mismatch is set (development only)"))
	require.Equal(t, "v1.0.0-beta.26-dev-0123456789ab", got[0]["engineVersion"])
	waitUntil(t, 5*time.Second, "the welcome logged", func() bool {
		return len(rec.find("engine welcomed")) == 1
	})
	require.Equal(t, "false", rec.find("engine welcomed")[0]["versionAccepted"])
	require.Equal(t, float64(1), mismatches(t, svc, "allowed"))
	require.Zero(t, mismatches(t, svc, "refused"))
	require.False(t, svc.Engines()[0].VersionAccepted)
}

// A refused engine is never registered, owns nothing, is never tracked,
// merged into or exported from, even with the export and import paths on
// and the cache of an accepted engine already welcomed.
func TestEngineVersionRefusedGetsNothing(t *testing.T) {
	h, _ := newMemExportHarness(t, nil)
	refused := fakeengine.New("cache-refused", 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: refused}, remotecache.Config{EngineVersion: "v0.0.1", URL: h.srv.URL, Token: "engine-token", EngineName: "refused"})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitUntil(t, 10*time.Second, "the refusal counted", func() bool {
		return mismatches(t, h.svc, "refused") >= 1
	})
	// Long enough for an import sync or an export to have reached it, were
	// it welcomed.
	time.Sleep(300 * time.Millisecond)
	infos := h.svc.Engines()
	require.Len(t, infos, 1)
	require.Equal(t, "cache-a", infos[0].CacheID)
	require.Zero(t, refused.Merges())
	require.Zero(t, refused.Exports())
	ns := h.ns()
	ns.mu.Lock()
	_, owned := ns.owners["cache-refused"]
	ns.mu.Unlock()
	require.False(t, owned)
	require.Empty(t, ns.tracker.All("cache-refused", time.Now()))
	require.Empty(t, ns.tracker.Entries("cache-refused"))
}

// The engine's real client, remotecache.Run, takes a welcome carrying the
// service's identity: it stays connected on the first connection, and
// pings over it, rather than reconnecting.
func TestWelcomeIdentityStockClient(t *testing.T) {
	svc, rec, srv := newTestService(t, "")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeAdapter{}, remotecache.Config{URL: srv.URL, Token: "t", EngineVersion: testEngineVersion})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	first := waitEngines(t, svc, 1)[0]
	time.Sleep(500 * time.Millisecond)
	now := waitEngines(t, svc, 1)[0]
	require.Same(t, first, now, "the engine reconnected after the welcome")
	require.Len(t, rec.find("engine connected"), 1)
	require.Zero(t, mismatches(t, svc, "refused"))
}

// The refusal reaches the stock client's log: its hello fails with the
// service's error reply, naming the engine's version and the accepted ones,
// and it reconnects with its backoff, is refused again, and is never
// welcomed.
func TestEngineVersionRefusedStockClient(t *testing.T) {
	svc, _, srv := newTestService(t, "")
	engineLog := &logRecorder{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeAdapter{}, remotecache.Config{URL: srv.URL, Token: "t", EngineVersion: "v0.0.1", Logger: slog.New(engineLog)})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitUntil(t, 10*time.Second, "two refusals (the first try and one reconnect)", func() bool {
		return mismatches(t, svc, "refused") >= 2
	})
	require.Empty(t, svc.Engines())
	ended := engineLog.find("remote cache connection ended; reconnecting")
	require.NotEmpty(t, ended)
	// The error reply, or, should the close frame win the race, its
	// reason: either names the engine's version and the accepted ones.
	got := ended[0]["error"]
	t.Logf("the engine logged: %s", got)
	require.Contains(t, got, `engine version "v0.0.1"`)
	require.Contains(t, got, testEngineVersion)
}

// A long version on a reject line is cut in the refusal as any other is:
// the reply never repeats a version past maxQuotedVersion bytes.
func TestEngineVersionRejectedLongBounded(t *testing.T) {
	long := "v1.0.0-" + strings.Repeat("a", 1000)
	m, err := compat.Parse([]byte("base v1.0.0-beta.25\naccept v1.0.0-beta.25 " + testDigest + " verified: " + testVerified + "\nreject " + long + " zerobull-consulting/dagger#117\n"))
	require.NoError(t, err)
	svc, _, srv := newTestService(t, "")
	svc.useManifest(m)
	e := dialRaw(t, srv, "tok")
	e.send(helloEnvelope(t, long))
	reply := e.next()
	require.Equal(t, protocol.TypeError, reply.Type)
	msg := decodeBody[protocol.Error](t, reply).Message
	require.Contains(t, msg, "was tried and rejected: see zerobull-consulting/dagger#117")
	require.NotContains(t, msg, long[:maxQuotedVersion+1])
	require.Less(t, len(msg), 512)
}

// TestSidecarWaitsForTheseLines: the sidecar action's start decides from
// these two log lines whether the service welcomed or refused its engine
// (zerobull-consulting/dagger#146). It matches them as the text handler
// writes them, msg="...": a renamed message here would leave it waiting
// until its timeout.
func TestSidecarWaitsForTheseLines(t *testing.T) {
	data, err := os.ReadFile("../../../.github/actions/remote-cache/sidecar.sh")
	require.NoError(t, err)
	var b strings.Builder
	slog.New(slog.NewTextHandler(&b, nil)).Info(LogEngineWelcomed)
	slog.New(slog.NewTextHandler(&b, nil)).Warn(LogEngineRefused)
	for _, msg := range []string{LogEngineWelcomed, LogEngineRefused} {
		quoted := `msg="` + msg + `"`
		require.Contains(t, b.String(), quoted, "the text handler doesn't write %q so", msg)
		require.True(t, strings.Contains(string(data), `'`+quoted+`'`), "sidecar.sh doesn't wait for %s", quoted)
	}
}
