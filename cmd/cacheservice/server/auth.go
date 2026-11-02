package server

import (
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// The endpoints a token is checked on, as logs and metrics name them.
const (
	endpointEngine = "engine"
	endpointOTLP   = "otlp"
)

// UseTokensFile loads a tokens file and authenticates engines with it from
// now on (see TokenFile). ReloadTokens reads the same file again. Without a
// tokens file, the service takes any token, and every engine is in the
// default namespace.
func (s *Service) UseTokensFile(path string) error {
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	tf, err := LoadTokenFile(path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.tokensFile = path
	s.mu.Unlock()
	s.setTokensLocked(tf)
	s.logger.Info("tokens loaded", "file", path, "tokens", tf.Len(), "namespaces", tf.Namespaces())
	return nil
}

// ReloadTokens reads the tokens file again, as on SIGHUP, and closes the
// connections whose token it no longer maps to their namespace. A file that
// can't be read or parsed changes nothing: the tokens loaded before stay.
// Reloads run one at a time, so the file read last is the one in effect.
func (s *Service) ReloadTokens() error {
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	s.mu.Lock()
	path := s.tokensFile
	s.mu.Unlock()
	if path == "" {
		return ErrNoTokensFile
	}
	tf, err := LoadTokenFile(path)
	if err != nil {
		s.logger.Error("tokens not reloaded; the tokens loaded before stay", "file", path, "error", err)
		return err
	}
	revoked := s.setTokensLocked(tf)
	s.logger.Info("tokens reloaded", "file", path, "tokens", tf.Len(), "namespaces", tf.Namespaces(), "revokedConnections", revoked)
	return nil
}

// SetTokens authenticates engines with tokens from now on, and closes, with
// protocol.CloseTokenRevoked, every connection whose token tokens doesn't map
// to the namespace it connected in: a removed token, or one moved to another
// namespace. Nil makes the service unauthenticated. It returns how many
// connections it closed.
//
// Calls run one at a time, with ReloadTokens and UseTokensFile: each
// revocation is decided by the tokens in effect, never by a set another call
// has already replaced.
func (s *Service) SetTokens(tokens Tokens) int {
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	return s.setTokensLocked(tokens)
}

// setTokensLocked is SetTokens; it requires s.tokensMu.
func (s *Service) setTokensLocked(tokens Tokens) int {
	s.mu.Lock()
	s.tokens = tokens
	s.tokensGen++
	conns := make([]*engineConn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	revoked := 0
	for _, c := range conns {
		if s.tokenMaps(tokens, c.secret, c.ns.name) {
			continue
		}
		// Revoked at once, before its close handshake: gone from the
		// list, so no export picks it; its import loop stopped, and its
		// renewals refused, so nothing more of the namespace reaches it.
		s.mu.Lock()
		if s.conns[c.id] == c {
			delete(s.conns, c.id)
		}
		s.mu.Unlock()
		if c.markRevoked() {
			revoked++
			go c.closeRevoked()
		}
	}
	return revoked
}

// authenticated reports whether the service checks tokens.
func (s *Service) authenticated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens != nil
}

// defaultNamespace is the namespace of every engine without tokens.
func (s *Service) defaultNamespace() string {
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	return s.defaultNS
}

// lookup returns the namespace of a token: from the tokens, or the default
// namespace when there are none.
func (s *Service) lookup(token string) (string, bool) {
	s.mu.Lock()
	tokens := s.tokens
	s.mu.Unlock()
	if tokens == nil {
		return s.defaultNamespace(), true
	}
	return tokens.Lookup(token)
}

// tokenMaps reports whether tokens map token to namespace ns.
func (s *Service) tokenMaps(tokens Tokens, token, ns string) bool {
	if tokens == nil {
		return ns == s.defaultNamespace()
	}
	got, ok := tokens.Lookup(token)
	return ok && got == ns
}

// register lists a connection that said hello as welcomed, if its token
// still maps to its namespace. The check and the listing see the same
// tokens: a SetTokens between them makes register check again, so no
// connection is welcomed on tokens already replaced.
func (s *Service) register(c *engineConn) bool {
	for {
		s.mu.Lock()
		tokens, gen := s.tokens, s.tokensGen
		s.mu.Unlock()
		ok := s.tokenMaps(tokens, c.secret, c.ns.name)
		s.mu.Lock()
		if s.tokensGen != gen {
			s.mu.Unlock()
			continue
		}
		if ok {
			s.conns[c.id] = c
		}
		s.mu.Unlock()
		if ok {
			c.ns.own(c.hello.CacheID, c.key)
			c.owned.Store(true)
		}
		return ok
	}
}

// markRevoked marks the connection revoked, and stops its import loop,
// cancelling a sync in flight. It reports false when it was already.
func (c *engineConn) markRevoked() bool {
	if !c.revoked.CompareAndSwap(false, true) {
		return false
	}
	c.mu.Lock()
	stop := c.stopImports
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
	c.logger.Info("closing the engine's connection: its token was revoked", "token", c.token)
	c.svc.metrics.TokenRevoked(c.ns.name)
	return true
}

// closeRevoked closes a revoked connection with protocol.CloseTokenRevoked,
// and waits for the close handshake (at most 5s, then it drops the
// connection).
func (c *engineConn) closeRevoked() {
	_ = c.conn.Close(websocket.StatusCode(protocol.CloseTokenRevoked), "token revoked")
}

// authenticate returns the namespace of a request's token, or answers 401
// and returns false when the token is unknown.
func (s *Service) authenticate(w http.ResponseWriter, r *http.Request, endpoint, token string) (*namespace, bool) {
	name, ok := s.lookup(token)
	if !ok {
		s.reject(w, r, endpoint, token)
		return nil, false
	}
	ns, err := s.namespace(name)
	if err != nil {
		s.logger.Error("namespace unusable", "namespace", name, "error", err)
		http.Error(w, "namespace unusable", http.StatusInternalServerError)
		return nil, false
	}
	return ns, true
}

// otlpAuth authenticates an OTLP request: with tokens, the HTTP Basic
// username must be a known token, as on the WebSocket, and the request
// belongs to its namespace. Without tokens, any request, with any token or
// none, belongs to the default namespace, as before tokens existed.
func (s *Service) otlpAuth(w http.ResponseWriter, r *http.Request) (*namespace, tokenKey, bool) {
	token, _, _ := r.BasicAuth()
	if !s.authenticated() {
		ns, err := s.namespace(s.defaultNamespace())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil, tokenKey{}, false
		}
		return ns, keyOf(token), true
	}
	if token == "" {
		s.reject(w, r, endpointOTLP, "")
		return nil, tokenKey{}, false
	}
	ns, ok := s.authenticate(w, r, endpointOTLP, token)
	if !ok {
		return nil, tokenKey{}, false
	}
	return ns, keyOf(token), true
}

// reject answers 401 to a request with no token or an unknown one. Engines
// whose token was removed keep retrying with their reconnection backoff, so
// each token's refusals are logged at warning level at most once a minute,
// and at most rejectionWarnings times a minute in all; the others at debug
// level.
func (s *Service) reject(w http.ResponseWriter, r *http.Request, endpoint, token string) {
	fp := ""
	if token != "" {
		fp = tokenFingerprint(token)
	}
	msg := "request refused: unknown token"
	if token == "" {
		msg = "request refused: no token"
	}
	level := slog.LevelDebug
	if s.rejections.allow(fp, time.Now()) {
		level = slog.LevelWarn
	}
	s.logger.Log(r.Context(), level, msg, "endpoint", endpoint, "path", r.URL.Path, "remote", r.RemoteAddr, "token", fp)
	s.metrics.AuthRejected(endpoint)
	w.Header().Set("WWW-Authenticate", `Basic realm="dagger-cache"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

const (
	// rejectionWarnings bounds the refusals logged at warning level per
	// rejectionWindow, and maxRejectionKeys the fingerprints remembered.
	rejectionWarnings = 10
	rejectionWindow   = time.Minute
	maxRejectionKeys  = 1024
)

// rejectionLog decides which refusals are logged at warning level.
type rejectionLog struct {
	mu          sync.Mutex
	last        map[string]time.Time
	windowStart time.Time
	inWindow    int
}

// allow reports whether a refusal of a token fingerprint is logged at
// warning level now: the token's first in rejectionWindow, within the
// window's budget.
func (l *rejectionLog) allow(fp string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.windowStart) >= rejectionWindow {
		l.windowStart, l.inWindow = now, 0
	}
	if l.last == nil || len(l.last) >= maxRejectionKeys {
		l.last = map[string]time.Time{}
	}
	if t, ok := l.last[fp]; ok && now.Sub(t) < rejectionWindow {
		return false
	}
	if l.inWindow >= rejectionWarnings {
		return false
	}
	l.last[fp] = now
	l.inWindow++
	return true
}
