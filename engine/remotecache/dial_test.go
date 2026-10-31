package remotecache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// dialServer accepts one WebSocket at protocol.EnginePath and hands it to
// the test with the request that opened it.
func dialServer(t *testing.T) (*httptest.Server, <-chan *http.Request, <-chan *websocket.Conn) {
	t.Helper()
	reqs := make(chan *http.Request, 1)
	conns := make(chan *websocket.Conn, 1)
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.EnginePath {
			http.NotFound(w, r)
			return
		}
		reqs <- r
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		conns <- conn
		<-done
	}))
	t.Cleanup(func() {
		close(done)
		srv.Close()
	})
	return srv, reqs, conns
}

// The engine dials EnginePath under the service URL, with or without a
// trailing slash, and sends its token as the Basic username.
func TestDialService(t *testing.T) {
	for _, suffix := range []string{"", "/"} {
		t.Run("suffix="+suffix, func(t *testing.T) {
			srv, reqs, conns := dialServer(t)
			cfg := newSettings(Config{URL: srv.URL + suffix, Token: "engine-token"})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			conn, err := cfg.dialService(ctx)
			require.NoError(t, err)
			defer conn.CloseNow()

			r := <-reqs
			user, pass, ok := r.BasicAuth()
			require.True(t, ok)
			require.Equal(t, "engine-token", user)
			require.Empty(t, pass)
			<-conns
		})
	}
}

// A refused upgrade fails the dial.
func TestDialServiceRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()
	cfg := newSettings(Config{URL: srv.URL, Token: "t"})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := cfg.dialService(ctx)
	require.Error(t, err)
}

// The connection reads only text messages, skipping binary ones, and writes
// text.
func TestWebsocketConnSkipsBinary(t *testing.T) {
	srv, _, conns := dialServer(t)
	cfg := newSettings(Config{URL: srv.URL})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := cfg.dialService(ctx)
	require.NoError(t, err)
	defer conn.CloseNow()
	service := <-conns

	require.NoError(t, service.Write(ctx, websocket.MessageBinary, []byte("skipped")))
	require.NoError(t, service.Write(ctx, websocket.MessageText, []byte(`{"id":1}`)))
	data, err := conn.Read(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":1}`, string(data))

	require.NoError(t, conn.Write(ctx, []byte(`{"id":2}`)))
	typ, data, err := service.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, typ)
	require.JSONEq(t, `{"id":2}`, string(data))
}

// Run, over a real WebSocket, says hello and serves the service's requests
// once welcomed.
func TestRunOverWebSocket(t *testing.T) {
	srv, _, conns := dialServer(t)
	adapter := newFakeAdapter()
	adapter.offer = func(context.Context, protocol.Offer) (protocol.Offered, error) { return protocol.Offered{}, nil }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, adapter, Config{URL: srv.URL, Token: "t", EngineName: "e"}) }()

	service := <-conns
	read := func() protocol.Envelope {
		t.Helper()
		rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
		defer rcancel()
		_, data, err := service.Read(rctx)
		require.NoError(t, err)
		var env protocol.Envelope
		require.NoError(t, json.Unmarshal(data, &env))
		return env
	}
	write := func(env protocol.Envelope) {
		t.Helper()
		data, err := json.Marshal(env)
		require.NoError(t, err)
		require.NoError(t, service.Write(ctx, websocket.MessageText, data))
	}

	hello := read()
	require.Equal(t, protocol.TypeHello, hello.Type)
	require.Equal(t, "e", decode[protocol.Hello](t, hello).EngineName)
	write(protocol.Envelope{ID: 1, Re: hello.ID, Type: protocol.TypeWelcome, Body: json.RawMessage(`{}`)})

	write(request(t, 2, protocol.TypeOffer, protocol.Offer{}))
	reply := read()
	require.Equal(t, protocol.TypeOffered, reply.Type)
	require.Equal(t, uint64(2), reply.Re)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}
