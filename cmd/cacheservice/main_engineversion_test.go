package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/compat"
	"github.com/dagger/dagger/cmd/cacheservice/internal/fakeengine"
	"github.com/dagger/dagger/engine/remotecache"
)

// testEngineVersion is the version the tests' engines send: the embedded
// manifest's first accept line.
var testEngineVersion = compat.Embedded().Default().Version

// runTestEngine runs the engine's real client against url until the test
// ends, with its log in a buffer.
func runTestEngine(t *testing.T, url, version string) *syncBuffer {
	t.Helper()
	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeengine.Adapter{Cache: fakeengine.New("cache-v", 1)}, remotecache.Config{
			URL: url, Token: "engine-token", EngineName: "e", EngineVersion: version,
			Logger: slog.New(slog.NewTextHandler(logs, nil)),
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return logs
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func debugJSON(t *testing.T, url string, v any) {
	t.Helper()
	code, body := get(t, url, "")
	if code != 200 {
		t.Fatalf("GET %s: %d %s", url, code, body)
	}
	if err := json.Unmarshal([]byte(body), v); err != nil {
		t.Fatalf("GET %s: %v: %s", url, err, body)
	}
}

// The binary logs its identity at startup, reports it at
// GET /debug/identity, welcomes an engine of an accepted version and
// refuses, before the welcome, one of any other: the refusal is logged,
// counted in /metrics, and reaches the engine's own log.
func TestBinaryEngineVersionRefused(t *testing.T) {
	listen, debug := freeAddr(t), freeAddr(t)
	_, logs := storelessBinary(t, 2, "-listen", listen, "-debug-listen", debug)
	m := compat.Embedded()
	identity := fmt.Sprintf("msg=identity base=dev accept=[%s] revision=", strings.Join(m.Versions(), " "))
	if !strings.Contains(logs.String(), identity) {
		t.Fatalf("no startup identity %q:\n%s", identity, logs.String())
	}
	if strings.Contains(logs.String(), "ENGINE VERSION CHECK OFF") {
		t.Fatalf("the check is off by default:\n%s", logs.String())
	}
	var id compat.Identity
	debugJSON(t, "http://"+debug+"/debug/identity", &id)
	if id.Base != compat.Dev || strings.Join(id.Accept, " ") != strings.Join(m.Versions(), " ") || id.Revision == "" {
		t.Fatalf("GET /debug/identity: %+v", id)
	}

	engineLogs := runTestEngine(t, "http://"+listen, "v0.0.1")
	waitFor(t, 15*time.Second, "the refusal in the service's log", func() bool {
		return strings.Contains(logs.String(), `msg="engine refused: its version is not accepted"`)
	})
	if !regexp.MustCompile(`engineVersion=v0\.0\.1 accept=\[` + regexp.QuoteMeta(strings.Join(m.Versions(), " ")) + `\] base=dev closeStatus=4002`).MatchString(logs.String()) {
		t.Fatalf("the refusal's log line lacks its fields:\n%s", logs.String())
	}
	waitFor(t, 15*time.Second, "the refusal in the engine's log", func() bool {
		return strings.Contains(engineLogs.String(), `engine version \"v0.0.1\" is not accepted`)
	})
	_, metrics := get(t, "http://"+debug+"/metrics", "")
	if !regexp.MustCompile(`(?m)^cacheservice_engine_version_mismatch_total\{outcome="refused"\} [1-9]`).MatchString(metrics) {
		t.Fatalf("the refusal isn't counted:\n%s", metrics)
	}
	var engines []map[string]any
	debugJSON(t, "http://"+debug+"/debug/engines", &engines)
	if len(engines) != 0 {
		t.Fatalf("a refused engine is listed: %v", engines)
	}

	// An accepted engine is welcomed.
	runTestEngine(t, "http://"+listen, testEngineVersion)
	waitFor(t, 15*time.Second, "the accepted engine listed", func() bool {
		debugJSON(t, "http://"+debug+"/debug/engines", &engines)
		return len(engines) == 1 && engines[0]["engineVersion"] == testEngineVersion && engines[0]["versionAccepted"] == true
	})
}

// -allow-engine-version-mismatch says loudly that the check is off, and
// welcomes an engine of any version, logging it.
func TestBinaryEngineVersionMismatchAllowed(t *testing.T) {
	listen, debug := freeAddr(t), freeAddr(t)
	_, logs := storelessBinary(t, 2, "-listen", listen, "-debug-listen", debug, "-allow-engine-version-mismatch")
	if !strings.Contains(logs.String(), `level=WARN msg="ENGINE VERSION CHECK OFF`) {
		t.Fatalf("no warning that the check is off:\n%s", logs.String())
	}
	runTestEngine(t, "http://"+listen, "v1.0.0-beta.26-dev-0123456789ab")
	var engines []map[string]any
	waitFor(t, 15*time.Second, "the dev engine listed", func() bool {
		debugJSON(t, "http://"+debug+"/debug/engines", &engines)
		return len(engines) == 1 && engines[0]["versionAccepted"] == false
	})
	if !strings.Contains(logs.String(), `msg="ENGINE VERSION NOT ACCEPTED, welcomed anyway`) {
		t.Fatalf("the mismatch isn't logged:\n%s", logs.String())
	}
}

// A binary stamped as a release other than its manifest's base refuses to
// start; one stamped as the base reports it.
func TestBinaryReleaseStamp(t *testing.T) {
	m := compat.Embedded()
	build := func(release string) string {
		t.Helper()
		bin := filepath.Join(t.TempDir(), "cacheservice")
		out, err := exec.Command("go", "build", "-ldflags", "-X github.com/dagger/dagger/cmd/cacheservice/compat.Release="+release, "-o", bin, ".").CombinedOutput()
		if err != nil {
			t.Fatalf("build: %v\n%s", err, out)
		}
		return bin
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, build("v0.0.1"), "-listen", freeAddr(t))
	cmd.Env = storelessEnv()
	out, err := cmd.CombinedOutput()
	if code := cmd.ProcessState.ExitCode(); code != 2 {
		t.Fatalf("a mis-stamped binary exited %d (%v), want 2:\n%s", code, err, out)
	}
	if want := `stamped as release \"v0.0.1\", but its engines.txt's base is ` + m.Base; !strings.Contains(string(out), want) {
		t.Fatalf("want %q in:\n%s", want, out)
	}

	cmd = exec.CommandContext(ctx, build(m.Base), "-listen", freeAddr(t), "-debug-listen", "")
	cmd.Env = storelessEnv()
	logs := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	waitFor(t, 15*time.Second, "the identity logged", func() bool {
		return strings.Contains(logs.String(), "msg=identity base="+m.Base+" ")
	})
}
