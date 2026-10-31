package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// helloTimeout bounds the wait for an engine's hello after the upgrade.
const helloTimeout = 30 * time.Second

// service holds the connected engines.
type service struct {
	logger  *slog.Logger
	dumpDir string

	lastConn atomic.Uint64
	dumpSeq  atomic.Uint64

	mu    sync.Mutex
	conns map[uint64]*engineConn
}

func newService(logger *slog.Logger, dumpDir string) *service {
	return &service{logger: logger, dumpDir: dumpDir, conns: map[uint64]*engineConn{}}
}

func (s *service) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(protocol.EnginePath, s.serveEngine)
	s.otlpRoutes(mux)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("unhandled request", "method", r.Method, "path", r.URL.Path)
		http.NotFound(w, r)
	})
	return mux
}

// engineConn is one engine's connection. The service numbers its own
// messages, as the engine does, from 1.
type engineConn struct {
	id          uint64
	token       string
	remote      string
	connectedAt time.Time
	conn        *websocket.Conn
	logger      *slog.Logger
	svc         *service

	lastID atomic.Uint64
	hello  protocol.Hello

	mu sync.Mutex
	// pending are the service's requests waiting for their replies, by ID.
	pending map[uint64]chan protocol.Envelope
}

func (s *service) serveEngine(w http.ResponseWriter, r *http.Request) {
	token, _, ok := r.BasicAuth()
	if !ok || token == "" {
		s.logger.Warn("engine rejected: no token", "remote", r.RemoteAddr)
		w.Header().Set("WWW-Authenticate", `Basic realm="dagger-cache"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	c := &engineConn{
		id:          s.lastConn.Add(1),
		token:       tokenFingerprint(token),
		remote:      r.RemoteAddr,
		connectedAt: time.Now(),
		svc:         s,
		pending:     map[uint64]chan protocol.Envelope{},
	}
	c.logger = s.logger.With("conn", c.id)
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionNoContextTakeover,
		OnPingReceived: func(context.Context, []byte) bool {
			c.logger.Debug("ping")
			return true
		},
	})
	if err != nil {
		s.logger.Warn("engine upgrade failed", "remote", r.RemoteAddr, "error", err)
		return
	}
	conn.SetReadLimit(protocol.MaxMessageBytes)
	c.conn = conn
	c.logger.Info("engine connected", "remote", c.remote, "token", c.token,
		"userAgent", r.UserAgent(), "compression", r.Header.Get("Sec-WebSocket-Extensions"))

	err = c.serve(r.Context())
	s.mu.Lock()
	delete(s.conns, c.id)
	s.mu.Unlock()
	c.failPending()
	_ = conn.CloseNow()
	c.logger.Info("engine disconnected",
		"after", time.Since(c.connectedAt).Round(time.Millisecond),
		"closeStatus", int(websocket.CloseStatus(err)),
		"error", err)
}

// serve reads hello, welcomes the engine, and reads until the connection ends.
func (c *engineConn) serve(ctx context.Context) error {
	helloCtx, cancel := context.WithTimeout(ctx, helloTimeout)
	env, err := c.read(helloCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("waiting for hello: %w", err)
	}
	if env.Type != protocol.TypeHello || env.Re != 0 {
		_ = c.replyError(ctx, env, "expected hello")
		return fmt.Errorf("first message is %s, not hello", env.Type)
	}
	if err := env.Decode(&c.hello); err != nil {
		_ = c.replyError(ctx, env, err.Error())
		return err
	}
	c.svc.mu.Lock()
	c.svc.conns[c.id] = c
	c.svc.mu.Unlock()
	if err := c.reply(ctx, env, protocol.TypeWelcome, protocol.Welcome{}); err != nil {
		return err
	}
	for {
		env, err := c.read(ctx)
		if err != nil {
			return err
		}
		c.dispatch(ctx, env)
	}
}

func (c *engineConn) read(ctx context.Context) (protocol.Envelope, error) {
	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			return protocol.Envelope{}, err
		}
		if typ != websocket.MessageText {
			c.logger.Warn("ignoring non-text message", "bytes", len(data))
			continue
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			c.logger.Warn("undecodable message", "bytes", len(data), "error", err)
			continue
		}
		c.logEnvelope("in", env, len(data))
		return env, nil
	}
}

// dispatch routes a message from the engine: a reply to the service request
// it names, or an engine request.
func (c *engineConn) dispatch(ctx context.Context, env protocol.Envelope) {
	if env.Re != 0 {
		c.mu.Lock()
		waiter := c.pending[env.Re]
		delete(c.pending, env.Re)
		c.mu.Unlock()
		if waiter == nil {
			c.logger.Warn("reply to no pending request", "re", env.Re, "type", env.Type)
			return
		}
		waiter <- env
		return
	}
	switch env.Type {
	case protocol.TypeRenew:
		// Nothing is stored, so no layer can be renewed.
		go func() { _ = c.reply(ctx, env, protocol.TypeRenewed, protocol.Renewed{Unavailable: true}) }()
	default:
		go func() { _ = c.replyError(ctx, env, fmt.Sprintf("unexpected request %q", env.Type)) }()
	}
}

// request sends a service request to the engine and waits for its reply.
func (c *engineConn) request(ctx context.Context, typ protocol.Type, body json.RawMessage) (protocol.Envelope, error) {
	id := c.lastID.Add(1)
	env := protocol.Envelope{ID: id, Type: typ, Body: body}
	waiter := make(chan protocol.Envelope, 1)
	c.mu.Lock()
	c.pending[id] = waiter
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()
	if err := c.write(ctx, env); err != nil {
		return protocol.Envelope{}, err
	}
	select {
	case reply, ok := <-waiter:
		if !ok {
			return protocol.Envelope{}, errors.New("connection closed")
		}
		return reply, nil
	case <-ctx.Done():
		return protocol.Envelope{}, context.Cause(ctx)
	}
}

// failPending ends the requests waiting on a closed connection.
func (c *engineConn) failPending() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, waiter := range c.pending {
		close(waiter)
		delete(c.pending, id)
	}
}

func (c *engineConn) reply(ctx context.Context, to protocol.Envelope, typ protocol.Type, body any) error {
	env, err := protocol.NewEnvelope(c.lastID.Add(1), to.ID, typ, body)
	if err != nil {
		return c.replyError(ctx, to, err.Error())
	}
	return c.write(ctx, env)
}

func (c *engineConn) replyError(ctx context.Context, to protocol.Envelope, msg string) error {
	env, err := protocol.NewEnvelope(c.lastID.Add(1), to.ID, protocol.TypeError, protocol.Error{Message: msg})
	if err != nil {
		return err
	}
	return c.write(ctx, env)
}

func (c *engineConn) write(ctx context.Context, env protocol.Envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.logEnvelope("out", env, len(data))
	return c.conn.Write(ctx, websocket.MessageText, data)
}

// logEnvelope logs one message, with a summary of the bodies whose shape is
// worth seeing, and dumps it when a dump directory is set.
func (c *engineConn) logEnvelope(dir string, env protocol.Envelope, size int) {
	attrs := []any{"dir", dir, "type", env.Type, "id", env.ID, "re", env.Re, "bytes", size, "bodyBytes", len(env.Body)}
	attrs = append(attrs, summarize(env)...)
	c.logger.Info("envelope", attrs...)
	if c.svc.dumpDir != "" {
		name := fmt.Sprintf("%06d-conn%d-%s-%s-id%d.json", c.svc.dumpSeq.Add(1), c.id, dir, env.Type, env.ID)
		c.svc.dump(name, env.Body)
	}
}

func summarize(env protocol.Envelope) []any {
	switch env.Type {
	case protocol.TypeHello:
		var b protocol.Hello
		if env.Decode(&b) == nil {
			return []any{"cacheID", b.CacheID, "generation", b.Generation, "engineVersion", b.EngineVersion, "engineName", b.EngineName}
		}
	case protocol.TypeRenew:
		var b protocol.Renew
		if env.Decode(&b) == nil {
			return []any{"renewalKey", b.RenewalKey, "layers", len(b.Layers), "neededBlob", b.NeededBlob}
		}
	case protocol.TypeExported:
		var b protocol.Exported
		if env.Decode(&b) == nil {
			return []any{"exportID", b.ExportID, "bundle", b.Bundle != nil, "skipped", len(b.Skipped)}
		}
	case protocol.TypeUploaded:
		var b protocol.Uploaded
		if env.Decode(&b) == nil {
			return []any{"done", len(b.Done), "failed", len(b.Failed)}
		}
	case protocol.TypeMerged:
		var b protocol.Merged
		if env.Decode(&b) == nil {
			return []any{"generation", b.Generation, "values", len(b.Values), "retained", len(b.Retained), "skipped", len(b.Skipped)}
		}
	case protocol.TypeOffered:
		var b protocol.Offered
		if env.Decode(&b) == nil {
			return []any{"items", len(b.Items)}
		}
	case protocol.TypeError:
		var b protocol.Error
		if env.Decode(&b) == nil {
			return []any{"message", b.Message}
		}
	}
	return nil
}

func (s *service) dump(name string, data []byte) {
	if err := os.WriteFile(filepath.Join(s.dumpDir, name), data, 0o644); err != nil {
		s.logger.Warn("dump failed", "file", name, "error", err)
	}
}

// closeAll closes every engine connection, as the service going away.
func (s *service) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.conn.Close(websocket.StatusGoingAway, "service shutting down")
	}
}

// tokenFingerprint names a token in logs without revealing it.
func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:4])
}

// debugHandler serves the debug API, which is not authenticated.
func (s *service) debugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/engines", s.listEngines)
	mux.HandleFunc("POST /debug/engines/{conn}/request", s.sendRequest)
	return mux
}

type engineInfo struct {
	Conn          uint64    `json:"conn"`
	Remote        string    `json:"remote"`
	Token         string    `json:"token"`
	ConnectedAt   time.Time `json:"connectedAt"`
	CacheID       string    `json:"cacheID"`
	Generation    uint64    `json:"generation"`
	EngineVersion string    `json:"engineVersion"`
	EngineName    string    `json:"engineName"`
}

func (s *service) listEngines(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	infos := make([]engineInfo, 0, len(s.conns))
	for _, c := range s.conns {
		infos = append(infos, engineInfo{
			Conn:          c.id,
			Remote:        c.remote,
			Token:         c.token,
			ConnectedAt:   c.connectedAt,
			CacheID:       c.hello.CacheID,
			Generation:    c.hello.Generation,
			EngineVersion: c.hello.EngineVersion,
			EngineName:    c.hello.EngineName,
		})
	}
	s.mu.Unlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Conn < infos[j].Conn })
	writeJSON(w, infos)
}

// sendRequest sends the posted body as a request of the given type to one
// engine, and answers with the engine's reply envelope.
func (s *service) sendRequest(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("conn"), 10, 64)
	if err != nil {
		http.Error(w, "bad conn", http.StatusBadRequest)
		return
	}
	typ := protocol.Type(r.URL.Query().Get("type"))
	if _, ok := protocol.ReplyType(typ); !ok {
		http.Error(w, fmt.Sprintf("%q is not a request type", typ), http.StatusBadRequest)
		return
	}
	var body json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "body: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	c := s.conns[id]
	s.mu.Unlock()
	if c == nil {
		http.Error(w, "no such engine", http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	reply, err := c.request(ctx, typ, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, reply)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
