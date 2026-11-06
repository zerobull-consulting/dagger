package main

// The debug API's token (zerobull-consulting/dagger#60), on the built
// command: the startup checks of -debug-listen against -debug-token-file
// and -tokens-file, the token on a non-loopback address, its reload on
// SIGHUP, and -metrics-listen, which stays open. No bucket: the service
// starts without a store, so these run without MinIO.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	binDebugToken  = "binary-debug-token-0123456789"
	binDebugToken2 = "binary-debug-token-9876543210"
)

// storelessEnv is the environment without the CACHESERVICE_S3_* settings:
// the service starts without a store.
func storelessEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "CACHESERVICE_S3_") {
			env = append(env, kv)
		}
	}
	return env
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The service refuses to start, before it listens, with a debug API that
// would be open where it shouldn't be, and with a debug token file it can't
// use; the error names the flag to set, and never the token.
func TestBinaryDebugListenStartupErrors(t *testing.T) {
	bin := buildBinary(t)
	tokens := writeFile(t, "tokens", "engine-token-0123456789 team-a\n")
	for _, tc := range []struct {
		name string
		args []string
		code int
		want []string
	}{
		{"every interface, no token", []string{"-debug-listen", ":0"}, 2, []string{"-debug-listen", "-debug-token-file"}},
		{"0.0.0.0, no token", []string{"-debug-listen", "0.0.0.0:0"}, 2, []string{"-debug-listen", "-debug-token-file"}},
		{"a host name, no token", []string{"-debug-listen", "example.invalid:0"}, 2, []string{"-debug-listen", "-debug-token-file"}},
		// The listener would resolve it again: only an IP is checked.
		{"localhost, no token", []string{"-debug-listen", "localhost:0"}, 2, []string{"-debug-listen", "-debug-token-file"}},
		{"tokens file, explicit loopback, no token", []string{"-tokens-file", tokens, "-debug-listen", "127.0.0.1:0"}, 2, []string{"-tokens-file", "-debug-token-file"}},
		{"token without the debug API", []string{"-debug-listen=", "-debug-token-file", writeFile(t, "dt", binDebugToken)}, 2, []string{"-debug-token-file", "-debug-listen"}},
		{"empty token file", []string{"-debug-listen", ":0", "-debug-token-file", writeFile(t, "empty", "\n")}, 1, []string{"debug token file: empty"}},
		{"missing token file", []string{"-debug-token-file", filepath.Join(t.TempDir(), "none")}, 1, []string{"debug token file"}},
		{"an engine token", []string{"-tokens-file", tokens, "-debug-token-file", writeFile(t, "engine", "engine-token-0123456789")}, 1, []string{"also an engine token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A refusal is immediate: a service that starts instead is
			// killed after 10s.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, append([]string{"-listen", freeAddr(t)}, tc.args...)...)
			cmd.Env = storelessEnv()
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != tc.code {
				t.Fatalf("exit %v, want %d; log:\n%s", err, tc.code, out)
			}
			for _, want := range tc.want {
				if !strings.Contains(string(out), want) {
					t.Errorf("the error doesn't name %s:\n%s", want, out)
				}
			}
			if strings.Contains(string(out), "msg=listening") {
				t.Errorf("it listened before refusing:\n%s", out)
			}
			for _, secret := range []string{binDebugToken, "engine-token-0123456789"} {
				if strings.Contains(string(out), secret) {
					t.Fatalf("the log shows a token:\n%s", out)
				}
			}
		})
	}
}

// storelessBinary runs the command without a store, waits until it listens
// on its listeners addresses, and stops it at the end of the test.
func storelessBinary(t *testing.T, listeners int, args ...string) (*exec.Cmd, *syncBuffer) {
	t.Helper()
	bin := buildBinary(t)
	cmd := exec.Command(bin, append([]string{"-listen", freeAddr(t)}, args...)...)
	cmd.Env = storelessEnv()
	logs := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited
		if t.Failed() {
			t.Logf("the service's log:\n%s", logs.String())
		}
	})
	want := listeners
	deadline := time.Now().Add(30 * time.Second)
	for strings.Count(logs.String(), "msg=listening") < want {
		select {
		case <-exited:
			t.Fatalf("the service exited:\n%s", logs.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the service didn't listen on %d addresses within 30s", want)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cmd, logs
}

func get(t *testing.T, url, auth string) (int, string) {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
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
	return resp.StatusCode, string(body)
}

// A loopback -debug-listen without -tokens-file starts open, with a
// warning that says so.
func TestBinaryDebugLoopbackOpen(t *testing.T) {
	debug := freeAddr(t)
	_, logs := storelessBinary(t, 2, "-debug-listen", debug)
	if !strings.Contains(logs.String(), "UNAUTHENTICATED debug API on "+debug) {
		t.Errorf("no warning that the debug API is open:\n%s", logs.String())
	}
	if code, body := get(t, "http://"+debug+"/debug/engines", ""); code != http.StatusOK {
		t.Fatalf("GET /debug/engines: %d %s", code, body)
	}
}

// With -debug-token-file, a non-loopback -debug-listen starts, and every
// request needs the token, /metrics included; SIGHUP rotates it.
// -metrics-listen serves /metrics without credentials, and nothing else.
// With -tokens-file too, an engine token doesn't open it.
func TestBinaryDebugToken(t *testing.T) {
	debugFile := writeFile(t, "debug-token", binDebugToken+"\n")
	tokens := writeFile(t, "tokens", "engine-token-0123456789 team-a\n")
	port := strings.TrimPrefix(freeAddr(t), "127.0.0.1")
	metrics := freeAddr(t)
	cmd, logs := storelessBinary(t, 3, "-debug-listen", "0.0.0.0"+port, "-debug-token-file", debugFile,
		"-tokens-file", tokens, "-metrics-listen", metrics)
	debug := "http://127.0.0.1" + port
	for _, path := range []string{"/debug/engines", "/debug/drain", "/metrics"} {
		for _, auth := range []string{"", "Bearer " + binDebugToken2, "Bearer engine-token-0123456789", "Basic ZW5naW5lLXRva2VuLTAxMjM0NTY3ODk6"} {
			if code, _ := get(t, debug+path, auth); code != http.StatusUnauthorized {
				t.Errorf("GET %s with %q: %d, want 401", path, auth, code)
			}
		}
		if code, body := get(t, debug+path, "Bearer "+binDebugToken); code != http.StatusOK {
			t.Errorf("GET %s with the debug token: %d %s", path, code, body)
		}
	}
	if code, body := get(t, "http://"+metrics+"/metrics", ""); code != http.StatusOK || !strings.Contains(body, "auth_rejected_total") {
		t.Errorf("-metrics-listen's /metrics without credentials: %d", code)
	}
	if code, _ := get(t, "http://"+metrics+"/debug/engines", ""); code != http.StatusNotFound {
		t.Errorf("-metrics-listen serves /debug/engines: %d, want 404", code)
	}

	// SIGHUP: the new token works, the old one doesn't.
	if err := os.WriteFile(debugFile, []byte(binDebugToken2+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), `msg="debug token reloaded"`) {
		if time.Now().After(deadline) {
			t.Fatal("no reload within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code, _ := get(t, debug+"/debug/engines", "Bearer "+binDebugToken); code != http.StatusUnauthorized {
		t.Errorf("the old token after SIGHUP: %d, want 401", code)
	}
	if code, _ := get(t, debug+"/debug/engines", "Bearer "+binDebugToken2); code != http.StatusOK {
		t.Errorf("the new token after SIGHUP: %d, want 200", code)
	}
	for _, secret := range []string{binDebugToken, binDebugToken2, "engine-token-0123456789"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("the log shows a token:\n%s", logs.String())
		}
	}
}

// With -tokens-file, no debug token and the default -debug-listen, the
// service starts with the debug API off, and says so: a service that never
// used the debug API keeps starting after an upgrade.
func TestBinaryDebugOffByDefaultWithTokens(t *testing.T) {
	tokens := writeFile(t, "tokens", "engine-token-0123456789 team-a\n")
	_, logs := storelessBinary(t, 1, "-tokens-file", tokens)
	out := logs.String()
	if !strings.Contains(out, "debug API off") {
		t.Errorf("no warning that the debug API is off:\n%s", out)
	}
	if strings.Contains(out, "addr=127.0.0.1:8091") || strings.Contains(out, "UNAUTHENTICATED debug API") {
		t.Errorf("the debug API listens:\n%s", out)
	}
}

// ::1 is loopback, as 127.0.0.1 is.
func TestBinaryDebugIPv6LoopbackOpen(t *testing.T) {
	ln, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skip("no IPv6 loopback here")
	}
	addr := ln.Addr().String()
	ln.Close()
	_, logs := storelessBinary(t, 2, "-debug-listen", addr)
	if !strings.Contains(logs.String(), "UNAUTHENTICATED debug API on "+addr) {
		t.Errorf("no open-API warning:\n%s", logs.String())
	}
}
