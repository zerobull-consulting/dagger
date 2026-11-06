package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Label marks the containers and volumes the runs start, so `dev.sh
// e2e-down` can remove what a run with E2E_KEEP=1 left.
const Label = "dagger-remote-cache-e2e"

// Docker runs the docker CLI and returns its stdout, or an error with its
// stderr.
func Docker(ctx context.Context, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// Engine is an engine of the run's image (Settings.Image), in a container
// with its own, initially empty, cache volume, connected to one service.
type Engine struct {
	Name      string
	Container string
	Volume    string
	// Service is the service it is pointed at, and Conn its current
	// connection number there.
	Service *Service
	Conn    uint64
	// Token is the engine's DAGGER_CLOUD_TOKEN, which it authenticates
	// with: its host name, unless the engine was started with a token. An
	// engine started with one runs its CLI with the same token too, since
	// session spans reach the service under the CLI's token.
	Token string

	cliToken string
	env      *Env
}

// Hostname is the engine's host name, which it sends as its engine name.
func (en *Engine) Hostname() string { return "e2e-engine-" + en.Name }

// StartEngines starts engines with the given names, pointed at svc, and
// waits until each has connected. They boot at once.
func (e *Env) StartEngines(svc *Service, names ...string) []*Engine {
	e.T.Helper()
	return e.StartEnginesWithTokens(svc, nil, names...)
}

// StartEnginesWithTokens is StartEngines, with the token of each engine
// named in tokens; the others use their host name. An engine with a token
// runs its CLI with it too.
func (e *Env) StartEnginesWithTokens(svc *Service, tokens map[string]string, names ...string) []*Engine {
	t := e.T
	t.Helper()
	before := svc.maxConn()
	engines := make([]*Engine, len(names))
	for i, name := range names {
		en := &Engine{
			Name:      name,
			Container: "dagger-remote-cache-e2e-" + e.RunID + "-" + name,
			Volume:    "dagger-remote-cache-e2e-" + e.RunID + "-" + name,
			Service:   svc,
			cliToken:  "e2e-cli",
			env:       e,
		}
		en.Token = en.Hostname()
		if tok, ok := tokens[name]; ok {
			en.Token, en.cliToken = tok, tok
		}
		ctx, cancel := e.ctx()
		_, err := Docker(ctx, "volume", "create", "--label", Label+"="+e.RunID, en.Volume)
		if err == nil {
			_, err = Docker(ctx, "run", "-d", "--privileged",
				"--name", en.Container,
				"--hostname", en.Hostname(),
				"--label", Label+"="+e.RunID,
				"-v", en.Volume+":/var/lib/dagger",
				"-e", "_EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL="+svc.URL,
				"-e", "DAGGER_CLOUD_TOKEN="+en.Token,
				"-e", "DAGGER_CLOUD_URL="+svc.URL,
				e.Settings.Image, "--debug")
		}
		cancel()
		e.root.Cleanup(en.remove)
		if err != nil {
			t.Fatalf("engine %s: %v", name, err)
		}
		engines[i] = en
	}
	start := time.Now()
	for _, en := range engines {
		en.waitConn(before)
	}
	t.Logf("engines %s connected to %s in %s", strings.Join(names, ", "), svc.Name, time.Since(start).Round(time.Second))
	return engines
}

// waitConn waits for the engine to connect with a connection number above
// after, and records it.
func (en *Engine) waitConn(after uint64) {
	en.env.T.Helper()
	en.env.WaitFor(2*time.Minute, "engine "+en.Name+" to connect to "+en.Service.Name, func() (bool, error) {
		for _, info := range en.Service.Engines() {
			if info.Conn > after && info.EngineName == en.Hostname() {
				en.Conn = info.Conn
				// The version its hello reported: which engine the run tests.
				en.env.T.Logf("engine %s connected to %s: engine version %q", en.Name, en.Service.Name, info.EngineVersion)
				return true, nil
			}
		}
		return false, nil
	})
}

// CacheID and Generation are the engine's cache identity, from its hello.
func (en *Engine) CacheID() string {
	info, _ := en.Service.EngineInfo(en.Conn)
	return info.CacheID
}

func (en *Engine) Generation() uint64 {
	info, _ := en.Service.EngineInfo(en.Conn)
	return info.Generation
}

// Restart restarts the engine's container, keeping its cache volume, and
// waits for its new connection.
func (en *Engine) Restart() {
	en.env.T.Helper()
	ctx, cancel := en.env.ctx()
	defer cancel()
	if _, err := Docker(ctx, "restart", en.Container); err != nil {
		en.env.T.Fatalf("restart engine %s: %v", en.Name, err)
	}
	en.waitConn(en.Conn)
}

// RestartAsync starts restarting the engine's container, and returns a
// function that waits for the restart and the engine's new connection.
func (en *Engine) RestartAsync() (wait func()) {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := en.env.ctx()
		defer cancel()
		_, err := Docker(ctx, "restart", en.Container)
		done <- err
	}()
	return func() {
		en.env.T.Helper()
		if err := <-done; err != nil {
			en.env.T.Fatalf("restart engine %s: %v", en.Name, err)
		}
		en.waitConn(en.Conn)
	}
}

// Attach points the engine at svc, a restarted service on its address, and
// waits for its connection there.
func (en *Engine) Attach(svc *Service) {
	en.env.T.Helper()
	en.Service = svc
	en.waitConn(0)
}

// Logs returns the engine container's log.
func (en *Engine) Logs() string {
	en.env.T.Helper()
	ctx, cancel := en.env.ctx()
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "logs", en.Container).CombinedOutput()
	if err != nil {
		en.env.T.Fatalf("logs of engine %s: %v", en.Name, err)
	}
	return string(out)
}

func (en *Engine) remove() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if en.env.T.Failed() {
		logs, _ := exec.CommandContext(ctx, "docker", "logs", en.Container).CombinedOutput()
		_ = os.WriteFile(filepath.Join(en.env.RunDir, "engine-"+en.Name+".log"), logs, 0o644)
	}
	if en.env.Settings.Keep {
		return
	}
	_, _ = Docker(ctx, "rm", "-f", en.Container)
	_, _ = Docker(ctx, "volume", "rm", "-f", en.Volume)
}

// CLI runs the dagger CLI built for the run against the engine, with stdin
// and args, and its telemetry pointed at svc (the engine's own service when
// nil). It writes stdout to RunDir/<out>.out and stderr to RunDir/<out>.log,
// and returns stdout and how long it took. It fails the test if the CLI
// fails.
//
// The CLI gets a clean environment: no inherited TRACEPARENT (some CI and
// sandboxes set one, unsampled; the CLI then continues that unsampled
// trace, the engine records no session spans, and the service never hears
// of the entries), and no OTEL_* or DAGGER_* settings of the caller.
func (en *Engine) CLI(svc *Service, out, stdin string, args ...string) ([]byte, time.Duration) {
	t := en.env.T
	t.Helper()
	if svc == nil {
		svc = en.Service
	}
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if name == "TRACEPARENT" || name == "TRACESTATE" ||
			strings.HasPrefix(name, "OTEL_") || strings.HasPrefix(name, "DAGGER_") ||
			strings.HasPrefix(name, "_EXPERIMENTAL_DAGGER_") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"_EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://"+en.Container,
		"DAGGER_CLOUD_TOKEN="+en.cliToken,
		"DAGGER_CLOUD_URL="+svc.URL,
		"DAGGER_NO_NAG=1",
	)
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, en.env.cli, append([]string{"--progress=plain"}, args...)...)
	cmd.Env = env
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	err := cmd.Run()
	took := time.Since(start)
	_ = os.WriteFile(filepath.Join(en.env.RunDir, out+".out"), stdout.Bytes(), 0o644)
	_ = os.WriteFile(filepath.Join(en.env.RunDir, out+".log"), stderr.Bytes(), 0o644)
	if err != nil {
		tail := stderr.String()
		if len(tail) > 4000 {
			tail = "..." + tail[len(tail)-4000:]
		}
		t.Fatalf("the CLI on engine %s (%s): %v\n%s", en.Name, strings.Join(args, " "), err, tail)
	}
	return stdout.Bytes(), took
}

// Query runs a GraphQL query on the engine through the CLI, and returns the
// result's JSON.
func (en *Engine) Query(out, query string) (map[string]any, time.Duration) {
	en.env.T.Helper()
	raw, took := en.CLI(nil, out, query, "query")
	var res map[string]any
	if err := json.Unmarshal(raw, &res); err != nil {
		en.env.T.Fatalf("query %s on engine %s: %v: %s", out, en.Name, err, raw)
	}
	return res, took
}

// Field reads a string at a dotted path in a query result.
func Field(res map[string]any, path string) string {
	var cur any = res
	for _, key := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[key]
	}
	switch v := cur.(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		data, _ := json.Marshal(v)
		return string(data)
	}
}
