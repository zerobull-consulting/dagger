package server

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// The debug token of the tests, and another, for rotations.
const (
	testDebugToken  = "debug-token-0123456789abcdef"
	testDebugToken2 = "debug-token-fedcba9876543210"
)

// writeDebugToken writes a debug token file and returns its path.
func writeDebugToken(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "debug-token")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// debugRequest sends a request to the debug API, with an Authorization
// header when auth isn't empty, and returns the status, the
// WWW-Authenticate header and the body.
func debugRequest(t *testing.T, base, method, path, auth string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("WWW-Authenticate"), string(body)
}

// counterSum sums a counter of the service's metrics, over its labels.
func counterSum(t *testing.T, svc *Service, suffix string) float64 {
	t.Helper()
	mfs, err := svc.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, mf := range mfs {
		if !strings.HasSuffix(mf.GetName(), suffix) {
			continue
		}
		for _, m := range mf.GetMetric() {
			sum += counterValue(m)
		}
	}
	return sum
}

func counterValue(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	return 0
}

// Every debug route refuses a request without the debug token, with a wrong
// one, with an engine token, and with a malformed header: 401, with
// WWW-Authenticate: Bearer and no detail, and nothing done. POST /debug/gc
// runs no collection, export and request send the engine nothing, and import
// merges nothing. The right token works on every route.
func TestDebugTokenGuardsEveryRoute(t *testing.T) {
	st := newGCStores(t, nil)
	h := newExportHarness(t, st.blobs, st.meta, nil)
	enableGC(t, h.svc, st, nil)
	h.buildRecipes()
	h.exportBuild()
	if err := h.svc.UseDebugTokenFile(writeDebugToken(t, testDebugToken+"\n")); err != nil {
		t.Fatal(err)
	}
	conn := "/debug/engines/" + strconv.FormatUint(h.engine.id, 10)
	routes := []struct{ method, path string }{
		{"GET", "/debug/engines"},
		{"POST", conn + "/request?type=export"},
		{"POST", conn + "/export"},
		{"GET", conn + "/entries"},
		{"POST", conn + "/import"},
		{"GET", conn + "/imports"},
		{"POST", "/debug/gc"},
		{"POST", "/debug/gc?dry-run"},
		{"GET", "/debug/drain"},
		{"GET", "/metrics"},
		{"GET", "/no/such/route"},
	}
	refused := []struct{ name, auth string }{
		{"no token", ""},
		{"wrong token", "Bearer " + testDebugToken2},
		{"token prefix", "Bearer " + testDebugToken[:len(testDebugToken)-1]},
		{"engine token", "Bearer engine-token"},
		{"engine token as Basic", "Basic ZW5naW5lLXRva2VuOg=="},
		{"debug token as Basic", "Basic " + testDebugToken},
		{"no scheme", testDebugToken},
		{"empty bearer", "Bearer "},
		{"bearer, no space", "Bearer" + testDebugToken},
		{"two spaces", "Bearer  " + testDebugToken},
	}
	exports, merges, runs := h.cache.Exports(), h.cache.Merges(), counterSum(t, h.svc, "gc_runs_total")
	rejected := counterSum(t, h.svc, "auth_rejected_total")
	for _, r := range routes {
		for _, a := range refused {
			code, challenge, body := debugRequest(t, h.debug.URL, r.method, r.path, a.auth)
			if code != http.StatusUnauthorized || !strings.HasPrefix(challenge, "Bearer") || strings.TrimSpace(body) != "unauthorized" {
				t.Errorf("%s %s, %s: %d %q %q, want 401, a Bearer challenge and no detail", r.method, r.path, a.name, code, challenge, body)
			}
		}
	}
	// Two Authorization headers, one of them right: refused too.
	req, _ := http.NewRequest("GET", h.debug.URL+"/debug/engines", nil)
	req.Header.Add("Authorization", "Bearer "+testDebugToken)
	req.Header.Add("Authorization", "Bearer "+testDebugToken2)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("two Authorization headers: %d, want 401", resp.StatusCode)
	}
	if got := h.cache.Exports(); got != exports {
		t.Errorf("refused requests sent the engine %d export requests", got-exports)
	}
	if got := h.cache.Merges(); got != merges {
		t.Errorf("refused requests merged %d times", got-merges)
	}
	if got := counterSum(t, h.svc, "gc_runs_total"); got != runs {
		t.Errorf("refused requests ran %v garbage collections", got-runs)
	}
	if got, want := counterSum(t, h.svc, "auth_rejected_total")-rejected, float64(len(routes)*len(refused)+1); got != want {
		t.Errorf("auth_rejected_total counted %v refusals, want %v", got, want)
	}

	// The right token, on every route; the scheme's case doesn't matter.
	for _, r := range routes {
		code, _, body := debugRequest(t, h.debug.URL, r.method, r.path, "bearer "+testDebugToken)
		if code == http.StatusUnauthorized {
			t.Errorf("%s %s with the debug token: 401 %q", r.method, r.path, body)
		}
	}
	if code, _, body := debugRequest(t, h.debug.URL, "GET", "/debug/engines", "Bearer "+testDebugToken); code != http.StatusOK || !strings.Contains(body, "engine-a") {
		t.Errorf("GET /debug/engines with the debug token: %d %s", code, body)
	}
	if counterSum(t, h.svc, "gc_runs_total") == runs {
		t.Error("the debug token's POST /debug/gc ran no collection")
	}
}

// A reload that changes the token makes the old one fail and the new one
// work. A reload of a file that is empty, unreadable, or holds an engine
// token keeps the token loaded before: the API is never left open.
func TestDebugTokenReload(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	path := writeDebugToken(t, testDebugToken)
	if err := svc.UseDebugTokenFile(path); err != nil {
		t.Fatal(err)
	}
	debug := httptest.NewServer(svc.DebugHandler())
	defer debug.Close()
	works := func(token string) bool {
		t.Helper()
		code, _, _ := debugRequest(t, debug.URL, "GET", "/debug/engines", "Bearer "+token)
		return code == http.StatusOK
	}
	if !works(testDebugToken) || works(testDebugToken2) {
		t.Fatal("before the reload: only the first token should work")
	}
	if err := os.WriteFile(path, []byte(testDebugToken2+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := svc.ReloadDebugToken(); err != nil {
		t.Fatal(err)
	}
	if works(testDebugToken) || !works(testDebugToken2) {
		t.Fatal("after the reload: only the new token should work")
	}

	for _, bad := range []struct {
		name    string
		content string
		remove  bool
	}{
		{"empty", "", false},
		{"white space", " \n\t\n", false},
		{"too short", "tiny-tok", false},
		{"two tokens", testDebugToken + " " + testDebugToken2, false},
		{"too large", strings.Repeat("a", MaxDebugTokenFileSize+1), false},
		{"unreadable", "", true},
	} {
		t.Run(bad.name, func(t *testing.T) {
			if bad.remove {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte(bad.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := svc.ReloadDebugToken(); err == nil {
				t.Fatal("the reload took the file")
			} else if bad.content != "" && strings.Contains(err.Error(), bad.content) {
				t.Fatalf("the error shows the file's content: %v", err)
			}
			if !works(testDebugToken2) {
				t.Fatal("the token loaded before stopped working")
			}
			code, _, _ := debugRequest(t, debug.URL, "GET", "/debug/engines", "")
			if code != http.StatusUnauthorized {
				t.Fatalf("no token after a bad reload: %d, want 401", code)
			}
		})
	}
}

// An empty, unreadable or engine-token file is refused at startup, not taken
// as "no token".
func TestDebugTokenFileRefused(t *testing.T) {
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte(testTokens), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, path string }{
		{"empty", writeDebugToken(t, "")},
		{"missing", filepath.Join(t.TempDir(), "none")},
		{"directory", t.TempDir()},
		{"engine token", writeDebugToken(t, "tok-a-is-also-long-enough")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "engine token" {
				if err := os.WriteFile(tokens, []byte(testTokens+"tok-a-is-also-long-enough team-a\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			svc := New(slog.New(slog.DiscardHandler), "")
			if err := svc.UseTokensFile(tokens); err != nil {
				t.Fatal(err)
			}
			err := svc.UseDebugTokenFile(tc.path)
			if err == nil {
				t.Fatal("the file was taken")
			}
			if strings.Contains(err.Error(), "tok-a-is-also") {
				t.Fatalf("the error shows the token: %v", err)
			}
			if svc.debug.hash.Load() != nil {
				t.Fatal("a refused file set a token")
			}
		})
	}
}

// An engine token added to the tokens file after the debug token was loaded
// doesn't open the debug API either.
func TestDebugTokenNeverAnEngineToken(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte(testTokens), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := svc.UseTokensFile(tokens); err != nil {
		t.Fatal(err)
	}
	if err := svc.UseDebugTokenFile(writeDebugToken(t, testDebugToken)); err != nil {
		t.Fatal(err)
	}
	debug := httptest.NewServer(svc.DebugHandler())
	defer debug.Close()
	if code, _, _ := debugRequest(t, debug.URL, "GET", "/debug/engines", "Bearer "+testDebugToken); code != http.StatusOK {
		t.Fatalf("the debug token: %d", code)
	}
	if err := os.WriteFile(tokens, []byte(testTokens+testDebugToken+" team-a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := svc.ReloadTokens(); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := debugRequest(t, debug.URL, "GET", "/debug/engines", "Bearer "+testDebugToken); code != http.StatusUnauthorized {
		t.Fatalf("the debug token, now an engine token too: %d, want 401", code)
	}
}

// Without a debug token, the debug API is open on a service without engine
// tokens (a loopback bind, which main checks), and closed on one with them:
// an authenticated service has no unauthenticated side door.
func TestDebugWithoutTokenOpenOnlyWithoutEngineTokens(t *testing.T) {
	svc := New(slog.New(slog.DiscardHandler), "")
	debug := httptest.NewServer(svc.DebugHandler())
	defer debug.Close()
	if code, _, _ := debugRequest(t, debug.URL, "GET", "/debug/engines", ""); code != http.StatusOK {
		t.Fatalf("no engine tokens, no debug token: %d, want 200", code)
	}
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte(testTokens), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := svc.UseTokensFile(tokens); err != nil {
		t.Fatal(err)
	}
	for _, auth := range []string{"", "Bearer tok-a", "Basic dG9rLWE6"} {
		if code, _, _ := debugRequest(t, debug.URL, "GET", "/debug/engines", auth); code != http.StatusUnauthorized {
			t.Fatalf("engine tokens, no debug token, auth %q: %d, want 401", auth, code)
		}
	}
}

// Refusals are logged with the route and the remote address, never the
// token, at warning level at most once a minute per remote host; the
// others at debug level.
func TestDebugRefusalsLoggedBounded(t *testing.T) {
	var logs bytes.Buffer
	svc := New(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), "")
	if err := svc.UseDebugTokenFile(writeDebugToken(t, testDebugToken)); err != nil {
		t.Fatal(err)
	}
	debug := httptest.NewServer(svc.DebugHandler())
	defer debug.Close()
	for range 20 {
		debugRequest(t, debug.URL, "POST", "/debug/gc", "Bearer "+testDebugToken2)
	}
	debugRequest(t, debug.URL, "GET", "/debug/engines", "Bearer "+testDebugToken)
	out := logs.String()
	if n := strings.Count(out, "level=WARN msg=\"request refused: wrong debug token\""); n != 1 {
		t.Errorf("%d warnings for 20 refusals from one host, want 1:\n%s", n, out)
	}
	if n := strings.Count(out, "level=DEBUG msg=\"request refused: wrong debug token\""); n != 19 {
		t.Errorf("%d debug-level refusals, want 19", n)
	}
	if !strings.Contains(out, `route="POST /debug/gc"`) || !strings.Contains(out, "remote=127.0.0.1:") {
		t.Errorf("the refusal doesn't name the route and the remote address:\n%s", out)
	}
	for _, secret := range []string{testDebugToken, testDebugToken2} {
		if strings.Contains(out, secret) {
			t.Fatalf("the log shows a token:\n%s", out)
		}
	}
}

// A token a client puts in the request's path, query or method never reaches
// the log: refusals name the matched route, or "unmatched".
func TestDebugRefusalLogsNoRequestData(t *testing.T) {
	var logs bytes.Buffer
	svc := New(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), "")
	if err := svc.UseDebugTokenFile(writeDebugToken(t, testDebugToken)); err != nil {
		t.Fatal(err)
	}
	debug := httptest.NewServer(svc.DebugHandler())
	defer debug.Close()
	for _, tc := range []struct{ method, path string }{
		{"GET", "/" + testDebugToken},
		{"GET", "/debug/engines?token=" + testDebugToken},
		{"POST", "/debug/engines/" + testDebugToken + "/export"},
		{testDebugToken, "/debug/engines"},
	} {
		if code, _, _ := debugRequest(t, debug.URL, tc.method, tc.path, ""); code != http.StatusUnauthorized {
			t.Errorf("%s %s: %d, want 401", tc.method, tc.path, code)
		}
	}
	out := logs.String()
	if strings.Contains(out, testDebugToken) {
		t.Fatalf("the log shows a token put in the request:\n%s", out)
	}
	for _, want := range []string{"route=unmatched", `route="POST /debug/engines/{conn}/export"`} {
		if !strings.Contains(out, want) {
			t.Errorf("the log has no %s:\n%s", want, out)
		}
	}
}

// Engine and debug refusals share one warning budget: with the minute's
// warnings spent on engine tokens, a debug refusal is logged at debug level.
func TestDebugRefusalsShareTheWarningBudget(t *testing.T) {
	var logs bytes.Buffer
	svc := New(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), "")
	if err := svc.UseDebugTokenFile(writeDebugToken(t, testDebugToken)); err != nil {
		t.Fatal(err)
	}
	debug := httptest.NewServer(svc.DebugHandler())
	defer debug.Close()
	// As reject does for rejectionWarnings unknown engine tokens.
	now := time.Now()
	for i := range rejectionWarnings {
		if !svc.rejections.allow(tokenFingerprint("engine-"+strconv.Itoa(i)), now) {
			t.Fatalf("engine refusal %d wasn't allowed a warning", i)
		}
	}
	debugRequest(t, debug.URL, "GET", "/debug/engines", "")
	if out := logs.String(); !strings.Contains(out, `level=DEBUG msg="request refused: no debug token"`) {
		t.Fatalf("a debug refusal past the shared budget wasn't at debug level:\n%s", out)
	}
}
