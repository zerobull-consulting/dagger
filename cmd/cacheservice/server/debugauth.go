package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// endpointDebug names the debug API in auth_rejected_total and in logs.
const endpointDebug = "debug"

const (
	// MaxDebugTokenFileSize bounds the debug token file: it holds one
	// token, so anything larger is a mistake, such as the wrong file.
	MaxDebugTokenFileSize = 4096
	// MinDebugTokenLength is the shortest debug token taken. Generate one
	// with, for example, `openssl rand -hex 32`.
	MinDebugTokenLength = 16
)

// debugToken is the admin token of the debug API, kept as its SHA-256 hash.
type debugToken struct {
	// loadMu serializes UseDebugTokenFile and ReloadDebugToken, and guards
	// path. Requests never take it: they read hash, so a reload blocked on
	// its file holds up no request.
	loadMu sync.Mutex
	path   string
	hash   atomic.Pointer[[sha256.Size]byte]
	// rejections rate-limits the logs of refused debug requests, by remote
	// host and reason.
	rejections rejectionLog
}

// parseDebugToken reads the content of a debug token file: one token, with
// white space around it ignored, of at least MinDebugTokenLength printable
// ASCII characters and no white space. Errors never include the content.
func parseDebugToken(data []byte) ([]byte, error) {
	if len(data) > MaxDebugTokenFileSize {
		return nil, fmt.Errorf("debug token file: over %d bytes; it holds one token", MaxDebugTokenFileSize)
	}
	token := bytes.TrimSpace(data)
	if len(token) == 0 {
		return nil, errors.New("debug token file: empty")
	}
	for _, c := range token {
		if c <= ' ' || c > '~' {
			return nil, errors.New("debug token file: the token must be one line of printable ASCII without white space")
		}
	}
	if len(token) < MinDebugTokenLength {
		return nil, fmt.Errorf("debug token file: the token is shorter than %d characters", MinDebugTokenLength)
	}
	return token, nil
}

// readDebugToken reads a debug token file (see parseDebugToken), at most
// MaxDebugTokenFileSize bytes of it.
func readDebugToken(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("debug token file: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, MaxDebugTokenFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("debug token file: %w", err)
	}
	return parseDebugToken(data)
}

// UseDebugTokenFile loads the debug API's admin token from a file, and
// requires it, as `Authorization: Bearer <token>`, on every request the
// debug handler serves from now on. ReloadDebugToken reads the same file
// again. A file that can't be read, is empty, or holds an engine token is
// refused: the debug API is never left open, or opened by an engine token,
// because of it.
func (s *Service) UseDebugTokenFile(path string) error {
	s.debug.loadMu.Lock()
	defer s.debug.loadMu.Unlock()
	hash, err := s.loadDebugToken(path)
	if err != nil {
		return err
	}
	s.debug.path = path
	s.debug.hash.Store(&hash)
	s.logger.Info("debug token loaded", "file", path)
	return nil
}

// ReloadDebugToken reads the debug token file again, as on SIGHUP: the new
// token works from now on, and the old one no longer does. A file that can't
// be read or parsed changes nothing: the token loaded before stays.
func (s *Service) ReloadDebugToken() error {
	s.debug.loadMu.Lock()
	defer s.debug.loadMu.Unlock()
	if s.debug.path == "" {
		return errors.New("the service has no debug token file")
	}
	hash, err := s.loadDebugToken(s.debug.path)
	if err != nil {
		s.logger.Error("debug token not reloaded; the token loaded before stays", "file", s.debug.path, "error", err)
		return err
	}
	s.debug.hash.Store(&hash)
	s.logger.Info("debug token reloaded", "file", s.debug.path)
	return nil
}

// loadDebugToken reads a debug token file, and refuses a token the engine
// tokens map: an engine token must never open the debug API. It keeps only
// the token's hash.
func (s *Service) loadDebugToken(path string) ([sha256.Size]byte, error) {
	token, err := readDebugToken(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if s.isEngineToken(string(token)) {
		return [sha256.Size]byte{}, errors.New("debug token file: the token is also an engine token in the tokens file; use a token of its own")
	}
	return sha256.Sum256(token), nil
}

// isEngineToken reports whether the engine tokens map token. Without
// tokens, no token is an engine token.
func (s *Service) isEngineToken(token string) bool {
	s.mu.Lock()
	tokens := s.tokens
	s.mu.Unlock()
	if tokens == nil {
		return false
	}
	_, ok := tokens.Lookup(token)
	return ok
}

// debugAuth requires the debug token on every request to next. Without a
// debug token, it lets every request through, unless the service checks
// engine tokens: an authenticated service has no unauthenticated side door,
// so it then refuses every request. A refused request gets 401 with
// `WWW-Authenticate: Bearer` and no detail, and reaches nothing.
//
// Refusals are logged with the route mux matched, never the request's own
// path or method: a client can put anything there, the token included.
func (s *Service) debugAuth(mux *http.ServeMux) http.Handler {
	next := http.Handler(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := s.debug.hash.Load()
		reject := func(msg string) { s.rejectDebug(w, r, debugRoute(mux, r), msg) }
		if want == nil {
			if !s.authenticated() {
				next.ServeHTTP(w, r)
				return
			}
			reject("request refused: the service checks engine tokens and has no debug token (-debug-token-file)")
			return
		}
		token, ok := bearerToken(r)
		if !ok {
			reject("request refused: no debug token")
			return
		}
		got := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			reject("request refused: wrong debug token")
			return
		}
		// The debug token is checked against the engine tokens when it is
		// loaded; engine tokens can change after that.
		if s.isEngineToken(token) {
			reject("request refused: the debug token is also an engine token; give the debug API a token of its own")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// debugRoute names a debug request's route for logs: the pattern mux
// registered it under, such as "POST /debug/engines/{conn}/export", or
// "unmatched". Never the request's own path, query or method.
func debugRoute(mux *http.ServeMux, r *http.Request) string {
	if _, pattern := mux.Handler(r); pattern != "" {
		return pattern
	}
	return "unmatched"
}

// bearerToken returns the token of a request's one Authorization header, of
// the Bearer scheme.
func bearerToken(r *http.Request) (string, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}

// rejectDebug answers 401 to a debug request. Refusals are logged at warning
// level at most once a minute per remote host and reason (msg), so a probe
// doesn't hide a lockout's reason, and rejectionWarnings times a minute in
// all; the others at debug level. The log names the route (debugRoute) and
// the remote address, never the token or anything else from the request.
func (s *Service) rejectDebug(w http.ResponseWriter, r *http.Request, route, msg string) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	level := slog.LevelDebug
	if s.debug.rejections.allow(msg+"\x00"+host, time.Now()) {
		level = slog.LevelWarn
	}
	s.logger.Log(r.Context(), level, msg, "endpoint", endpointDebug, "route", route, "remote", r.RemoteAddr)
	s.metrics.AuthRejected(endpointDebug)
	w.Header().Set("WWW-Authenticate", `Bearer realm="cacheservice-debug"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
