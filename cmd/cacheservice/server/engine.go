package server

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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/opencontainers/go-digest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// helloTimeout bounds the wait for an engine's hello after the upgrade.
const helloTimeout = 30 * time.Second

// Service holds the connected engines.
type Service struct {
	logger  *slog.Logger
	dumpDir string

	lastConn atomic.Uint64
	dumpSeq  atomic.Uint64

	mu    sync.Mutex
	conns map[uint64]*engineConn
	// tokens maps tokens to namespaces; nil means the service is
	// unauthenticated: any token, all in the default namespace.
	// tokensGen counts SetTokens calls, so a connection checked against
	// one set of tokens isn't welcomed after another replaced it.
	tokens    Tokens
	tokensGen uint64
	// tokensMu serializes SetTokens, ReloadTokens and UseTokensFile.
	tokensMu sync.Mutex
	// tokensFile is where ReloadTokens reads tokens from.
	tokensFile string

	// rejections rate-limits the logs of refused tokens, by fingerprint.
	rejections rejectionLog
	// debug is the debug API's admin token (UseDebugTokenFile).
	debug debugToken

	// nsMu guards the namespaces and the default namespace.
	nsMu       sync.Mutex
	namespaces map[string]*namespace
	// defaultNS is the namespace of every engine when there are no tokens:
	// the scope EnableExport and EnableImport name. scopeSet is set once
	// one of them named it.
	defaultNS string
	scopeSet  bool

	// export is the export path, nil when it is off.
	export *exportPath
	// readOnly is set by SetReadOnly: the export path is off, and stays
	// off, and nothing writes to or deletes from the bucket.
	readOnly bool
	// imports is the import path, nil when it is off.
	imports *importPath
	// gc is garbage collection, nil when it is off; limits are every
	// namespace's retention limits (UseLimits).
	gc     *gcPath
	limits limitsState

	// draining is set once Drain starts: the service takes no new work.
	draining atomic.Bool
	// closing is set, under mu, once CloseEngines runs: the service takes
	// no new engine, so none is welcomed after CloseEngines took its list.
	closing atomic.Bool
	drain   drainState
	// flushes are the flushes under way (flush.go).
	flushes flushRuns
	// testHookExportRecorded, set by tests only, runs after an export's
	// result reached the tracker.
	testHookExportRecorded func()
	// testHeld is the work HoldWaitForTest holds.
	testHeld workGroup
	// testHookDrainWoken, set by tests only, runs when a drain wakes on
	// the end of its context, before it reports.
	testHookDrainWoken func()
	// testHookHello, set by tests only, runs after a connection's hello
	// was read, before it is registered or refused.
	testHookHello func()

	// engines is the engine accept list and the service's identity
	// (engineversion.go).
	engines *enginePolicy

	// registry holds metrics, which records the storage costs.
	registry *prometheus.Registry
	metrics  *metrics.Metrics

	// otlp bounds the OTLP bodies (otlplimit.go); otlpRefusals rate-limits
	// the warnings of the requests it refuses, by route and reason.
	otlp         otlpLimits
	otlpRefusals rejectionLog
}

// New returns a service with no engines, and with the export and import
// paths off. dumpDir, when set, receives every envelope and engine event
// body; it must exist.
func New(logger *slog.Logger, dumpDir string) *Service {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return &Service{
		logger:     logger,
		dumpDir:    dumpDir,
		conns:      map[uint64]*engineConn{},
		namespaces: map[string]*namespace{},
		defaultNS:  DefaultNamespace,
		registry:   reg,
		metrics:    newMetrics(reg),
		otlp:       defaultOTLPLimits(),
		engines:    defaultEnginePolicy(),
	}
}

// newMetrics are a new service's metrics: its export path is off until
// EnableExport.
func newMetrics(reg prometheus.Registerer) *metrics.Metrics {
	m := metrics.New(reg)
	m.ExportEnabled(false)
	return m
}

// Metrics returns what the service records its storage costs into. The
// export and import paths record into it, through their metadata store,
// unless their config names a store with metrics of its own.
func (s *Service) Metrics() *metrics.Metrics { return s.metrics }

// MetricsHandler serves the service's metrics in the Prometheus format.
func (s *Service) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{Registry: s.registry})
}

// Handler serves engines: the WebSocket at protocol.EnginePath, the OTLP
// receivers, the blob PUT stand-in, and the flush of one engine cache
// (POST /v1/engines/{cacheID}/flush, and POST /v1/engines/flush for the
// cache of the request's token; see flush.go).
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(protocol.EnginePath, s.serveEngine)
	mux.HandleFunc("PUT /blobs/{digest}", s.serveBlobPut)
	mux.HandleFunc("POST /v1/engines/{cacheID}/flush", s.serveFlush)
	mux.HandleFunc("POST /v1/engines/flush", s.serveFlushToken)
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
	id uint64
	// token is the token's fingerprint, for logs; secret is the token
	// itself, to check it again when the tokens change, and key its hash.
	// Neither is ever logged.
	token  string
	secret string
	key    tokenKey
	// ns is the namespace of the token.
	ns *namespace
	// revoked is set once the token no longer maps to ns: the connection
	// gets nothing more of the namespace while its close handshake runs.
	revoked atomic.Bool
	// owned is set once register counted the connection as its cache's
	// owner; its end releases it.
	owned       atomic.Bool
	remote      string
	connectedAt time.Time
	conn        *websocket.Conn
	logger      *slog.Logger
	svc         *Service
	// drop ends the context the connection is read on: the WebSocket
	// library then closes the connection at once, which ends a close
	// handshake still waiting for the engine's answer (CloseEngines).
	drop func()

	lastID atomic.Uint64
	hello  protocol.Hello
	// versionAccepted is set when the hello's version is on an accept line;
	// it is unset for an engine welcomed by -allow-engine-version-mismatch.
	versionAccepted bool

	mu sync.Mutex
	// stopImports ends the import sync loop; set once it starts.
	stopImports context.CancelFunc
	// pending are the service's requests waiting for their replies, by ID.
	pending map[uint64]chan protocol.Envelope
}

func (s *Service) serveEngine(w http.ResponseWriter, r *http.Request) {
	// A draining service takes no new engine: the engine retries with its
	// backoff, and finds the next service on this address.
	if s.draining.Load() {
		s.refuseDraining(w, r)
		return
	}
	if s.closing.Load() {
		w.Header().Set("Retry-After", drainRetryAfter)
		http.Error(w, ErrStopping.Error(), http.StatusServiceUnavailable)
		return
	}
	token, _, ok := r.BasicAuth()
	if !ok || token == "" {
		s.reject(w, r, endpointEngine, "")
		return
	}
	// Before the upgrade: an unknown token gets 401, and never a protocol
	// message.
	ns, ok := s.authenticate(w, r, endpointEngine, token)
	if !ok {
		return
	}
	c := &engineConn{
		id:          s.lastConn.Add(1),
		token:       tokenFingerprint(token),
		secret:      token,
		key:         keyOf(token),
		ns:          ns,
		remote:      r.RemoteAddr,
		connectedAt: time.Now(),
		svc:         s,
		pending:     map[uint64]chan protocol.Envelope{},
	}
	c.logger = s.logger.With("conn", c.id, "namespace", ns.name)
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

	ctx, drop := context.WithCancel(r.Context())
	c.drop = drop
	err = c.serve(ctx)
	drop()
	s.mu.Lock()
	if s.conns[c.id] == c {
		delete(s.conns, c.id)
	}
	s.mu.Unlock()
	c.failPending()
	if c.owned.Load() {
		c.ns.release(c.hello.CacheID)
	}
	if c.ns.exporter != nil {
		c.ns.exporter.Forget(c)
	}
	_ = conn.CloseNow()
	c.logger.Info("engine disconnected",
		"after", time.Since(c.connectedAt).Round(time.Millisecond),
		"closeStatus", int(websocket.CloseStatus(err)),
		"revoked", c.revoked.Load(),
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
	if h := c.svc.testHookHello; h != nil {
		h()
	}
	// Before register: a refused engine is never registered, tracked,
	// merged into or exported from.
	if !c.checkEngineVersion(ctx, env) {
		return errEngineVersionRefused
	}
	switch c.svc.register(c) {
	case registered:
	case refusedDraining:
		// Upgraded before the drain started, but not welcomed yet: a
		// new engine all the same. register checks the drain under the
		// lock Drain starts it under, so an engine is either welcomed
		// before the drain starts, and counted in it, or refused here.
		c.svc.countRefused(metrics.DrainRefusedConnection)
		_ = c.conn.Close(websocket.StatusTryAgainLater, "service draining")
		return errors.New("service draining: refused before welcome")
	case refusedClosing:
		_ = c.conn.Close(websocket.StatusGoingAway, "service shutting down")
		return errors.New("service stopping: refused before welcome")
	default:
		c.markRevoked()
		c.closeRevoked()
		return errors.New("token revoked before welcome")
	}
	if err := c.reply(ctx, env, protocol.TypeWelcome, welcome{Service: c.svc.Identity()}); err != nil {
		return err
	}
	// The line the sidecar action's start waits for, or for the refusal's
	// (zerobull-consulting/dagger#146): "engine connected" is logged at the
	// upgrade, before the hello says whether the engine is accepted.
	c.logger.Info(LogEngineWelcomed, "engineVersion", boundVersion(c.hello.EngineVersion),
		"versionAccepted", c.versionAccepted)
	// The engine reports in at its generation: a newer one clears the
	// cache's stale-generation refusals, to be exported from it (#129).
	if c.ns.tracker != nil {
		c.ns.tracker.Connected(c.hello.CacheID, c.hello.Generation)
	}
	// The import sync loop ends with the connection.
	importCtx, stopImports := context.WithCancel(ctx)
	defer stopImports()
	c.mu.Lock()
	c.stopImports = stopImports
	c.mu.Unlock()
	// A revocation since register found no loop to stop.
	if c.revoked.Load() {
		stopImports()
	}
	c.svc.startImports(importCtx, c)
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
			c.releaseLateExport(ctx, env)
			return
		}
		waiter <- env
		return
	}
	switch env.Type {
	case protocol.TypeRenew:
		go func() {
			var req protocol.Renew
			if err := env.Decode(&req); err != nil {
				_ = c.replyError(ctx, env, err.Error())
				return
			}
			_ = c.reply(ctx, env, protocol.TypeRenewed, c.svc.renew(ctx, c, req))
		}()
	default:
		go func() { _ = c.replyError(ctx, env, fmt.Sprintf("unexpected request %q", env.Type)) }()
	}
}

// lateReleaseTimeout bounds the release of an export whose requester gave up.
const lateReleaseTimeout = time.Minute

// releaseLateExport releases the chains of an export whose reply came after
// its requester gave up, as when the wait timed out. Otherwise the engine
// would hold them, and that export's slot, until the connection ends; with
// all its slots held, it would queue every later export.
func (c *engineConn) releaseLateExport(ctx context.Context, env protocol.Envelope) {
	if env.Type != protocol.TypeExported {
		return
	}
	var exported protocol.Exported
	if err := env.Decode(&exported); err != nil || exported.ExportID == "" {
		return
	}
	body, err := json.Marshal(protocol.Upload{ExportID: exported.ExportID, URLs: map[digest.Digest]string{}})
	if err != nil {
		return
	}
	go func() {
		rctx, cancel := context.WithTimeout(ctx, lateReleaseTimeout)
		defer cancel()
		if _, err := c.request(rctx, protocol.TypeUpload, body); err != nil {
			c.logger.Warn("release of a late export failed", "exportID", exported.ExportID, "error", err)
			return
		}
		c.logger.Info("released a late export", "exportID", exported.ExportID)
	}()
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
	case protocol.TypeRenewed:
		var b protocol.Renewed
		if env.Decode(&b) == nil {
			return []any{"addresses", len(b.Addresses), "unavailable", b.Unavailable}
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

// dump writes one file to the dump directory. Names carry what engines send
// (an envelope's type, an event's kind), so dumpName keeps them one plain
// file name inside the directory.
func (s *Service) dump(name string, data []byte) {
	name = dumpName(name)
	if err := os.WriteFile(filepath.Join(s.dumpDir, name), data, 0o644); err != nil {
		s.logger.Warn("dump failed", "file", name, "error", err)
	}
}

// dumpName replaces every character of name but letters, digits, '.', '_'
// and '-' with '_': no separator is left, so the name can't leave the dump
// directory. It is cut to 200 bytes, within any file system's limit.
func dumpName(name string) string {
	b := []byte(name)
	for i, c := range b {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			b[i] = '_'
		}
	}
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}

// CloseEnginesTimeout bounds CloseEngines: how long it waits for the engines
// to answer its close frame before it drops the connections that haven't.
// It is within the stop's grace (cmd/cacheservice's shutdownGrace, 5s), which
// also covers shutting the HTTP servers down and Wait.
const CloseEnginesTimeout = 2 * time.Second

// CloseEngines closes every engine connection, as the service going away,
// within CloseEnginesTimeout: see CloseEnginesContext.
func (s *Service) CloseEngines() {
	ctx, cancel := context.WithTimeout(context.Background(), CloseEnginesTimeout)
	defer cancel()
	_ = s.CloseEnginesContext(ctx)
}

// CloseEnginesContext closes every engine connection, as the service going
// away: each gets a close frame (StatusGoingAway), all at once, and the
// engines that answer it end their connection cleanly. Once ctx ends, the
// connections still waiting for an answer are dropped, as CloseEngine
// does, and it returns once they are closed. It doesn't hold the service's
// lock meanwhile. From its start, the service welcomes no new engine: a
// connection gets 503 before the upgrade, or 1001 at its hello. It returns
// an error when connections it dropped were still closing closeDropWait
// later: the stop isn't clean.
func (s *Service) CloseEnginesContext(ctx context.Context) error {
	s.mu.Lock()
	// From now on no engine is welcomed (register), and none upgraded: the
	// engines closed here reconnect within about a second, while the HTTP
	// servers still listen.
	s.closing.Store(true)
	conns := make([]*engineConn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	if len(conns) == 0 {
		return nil
	}
	var (
		wg     sync.WaitGroup
		closed atomic.Int64
	)
	for _, c := range conns {
		wg.Go(func() {
			_ = c.conn.Close(websocket.StatusGoingAway, "service shutting down")
			closed.Add(1)
		})
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	// Ending a connection's read context makes the WebSocket library close
	// it at once (CloseNow would wait for the close under way), and the
	// close handshake waiting on it then ends.
	s.logger.Warn("engines didn't answer the close frame in time: dropping their connections",
		"engines", len(conns)-int(closed.Load()))
	for _, c := range conns {
		c.drop()
	}
	// Bounded too: a close still under way after this is the process's
	// to end.
	t := time.NewTimer(closeDropWait)
	defer t.Stop()
	select {
	case <-done:
		return nil
	case <-t.C:
		n := len(conns) - int(closed.Load())
		s.logger.Warn("engine connections still closing after they were dropped", "engines", n)
		return fmt.Errorf("%d engine connections still closing after they were dropped", n)
	}
}

// closeDropWait bounds CloseEnginesContext's wait for the connections it
// dropped to close: the library closes them at once, so this is a margin.
const closeDropWait = 500 * time.Millisecond

// CloseEngine drops one engine's connection at once, with no close frame,
// as a network failure would. The engine reconnects with backoff; requests
// in flight on the connection fail, and exports cut short by it are tried
// again on the engine's next connection.
func (s *Service) CloseEngine(conn uint64) error {
	c, err := s.conn(conn)
	if err != nil {
		return err
	}
	c.logger.Info("closing the engine's connection")
	return c.conn.CloseNow()
}

// tokenFingerprint names a token in logs without revealing it.
func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:4])
}

// DebugHandler serves the debug API. With a debug token (UseDebugTokenFile),
// every request, /metrics and unknown paths included, must carry it as
// `Authorization: Bearer <token>`, and is refused with 401 otherwise.
// Without one, it is open, unless the service checks engine tokens: it then
// refuses every request (see debugAuth).
func (s *Service) DebugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/engines", s.listEngines)
	mux.HandleFunc("POST /debug/engines/{conn}/request", s.sendRequest)
	mux.HandleFunc("POST /debug/engines/{conn}/export", s.serveExport)
	mux.HandleFunc("POST /debug/engines/{conn}/flush", s.serveDebugFlush)
	mux.HandleFunc("GET /debug/engines/{conn}/entries", s.serveEntries)
	mux.HandleFunc("POST /debug/engines/{conn}/import", s.serveImport)
	mux.HandleFunc("GET /debug/engines/{conn}/imports", s.serveImports)
	mux.HandleFunc("POST /debug/gc", s.serveGC)
	mux.HandleFunc("GET /debug/drain", s.serveDrain)
	mux.HandleFunc("GET /debug/identity", s.serveIdentity)
	mux.Handle("GET /metrics", s.MetricsHandler())
	return s.debugAuth(mux)
}

// EngineInfo is a welcomed engine connection, as Engines lists it. Token is
// the token's fingerprint, not the token.
type EngineInfo struct {
	Conn          uint64    `json:"conn"`
	Remote        string    `json:"remote"`
	Token         string    `json:"token"`
	Namespace     string    `json:"namespace"`
	ConnectedAt   time.Time `json:"connectedAt"`
	CacheID       string    `json:"cacheID"`
	Generation    uint64    `json:"generation"`
	EngineVersion string    `json:"engineVersion"`
	// VersionAccepted is false for an engine whose version isn't accepted,
	// welcomed by -allow-engine-version-mismatch.
	VersionAccepted bool   `json:"versionAccepted"`
	EngineName      string `json:"engineName"`
}

// serveIdentity answers with the service's identity: its base release (or
// "dev"), the engine versions it accepts and its revision.
func (s *Service) serveIdentity(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.Identity())
}

func (s *Service) listEngines(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.Engines())
}

// sendRequest sends the posted body as a request of the given type to one
// engine, and answers with the engine's reply envelope.
func (s *Service) sendRequest(w http.ResponseWriter, r *http.Request) {
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
