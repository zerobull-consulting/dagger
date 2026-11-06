package server

// The engine's side of the connection as the service sees it: how
// remotecache.Run dials, over a bare WebSocket server rather than the
// service, so each test sees exactly what the engine sends. These moved here
// from engine/remotecache/dial_test.go, so that the fork has no code of its own
// under engine/ (zerobull-consulting/dagger#116); they use only Run, the
// client's exported entry point.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// dialed is one WebSocket the engine opened, with the request that opened it.
type dialed struct {
	req  *http.Request
	conn *websocket.Conn
}

// dialServer accepts WebSockets at protocol.EnginePath and hands each to the
// test with the request that opened it. refuse, when set, says whether to
// refuse the n-th upgrade (from 1) with 401. The connections are closed when
// the test ends.
func dialServer(t *testing.T, refuse func(n int64) bool) (*httptest.Server, <-chan dialed) {
	t.Helper()
	conns := make(chan dialed, 4)
	done := make(chan struct{})
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.EnginePath {
			http.NotFound(w, r)
			return
		}
		if refuse != nil && refuse(n.Add(1)) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		select {
		case conns <- dialed{req: r, conn: conn}:
		default:
			return
		}
		<-done
	}))
	t.Cleanup(func() {
		close(done)
		srv.Close()
	})
	return srv, conns
}

// runEngine runs remotecache.Run against url until the test ends, and fails
// the test if Run doesn't return promptly once cancelled.
func runEngine(t *testing.T, cfg remotecache.Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- remotecache.Run(ctx, fakeAdapter{}, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("Run did not return")
		}
	})
}

func takeConn(t *testing.T, conns <-chan dialed) dialed {
	t.Helper()
	select {
	case d := <-conns:
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("the engine did not connect")
		return dialed{}
	}
}

func readEnvelope(t *testing.T, c *websocket.Conn) protocol.Envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	typ, data, err := c.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, typ)
	var env protocol.Envelope
	require.NoError(t, json.Unmarshal(data, &env))
	return env
}

func writeMessage(t *testing.T, c *websocket.Conn, typ websocket.MessageType, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.Write(ctx, typ, data))
}

func writeEnvelope(t *testing.T, c *websocket.Conn, env protocol.Envelope) {
	t.Helper()
	data, err := json.Marshal(env)
	require.NoError(t, err)
	writeMessage(t, c, websocket.MessageText, data)
}

// The engine dials EnginePath under the service URL, with or without a
// trailing slash, and sends its token as the Basic username.
func TestEngineDialsEnginePath(t *testing.T) {
	for _, suffix := range []string{"", "/"} {
		t.Run("suffix="+suffix, func(t *testing.T) {
			srv, conns := dialServer(t, nil)
			runEngine(t, remotecache.Config{URL: srv.URL + suffix, Token: "engine-token", EngineName: "e"})
			d := takeConn(t, conns)
			user, pass, ok := d.req.BasicAuth()
			require.True(t, ok)
			require.Equal(t, "engine-token", user)
			require.Empty(t, pass)
			hello := readEnvelope(t, d.conn)
			require.Equal(t, protocol.TypeHello, hello.Type)
		})
	}
}

// A refused upgrade fails that dial, and the engine dials again.
func TestEngineRedialsAfterRefusedUpgrade(t *testing.T) {
	srv, conns := dialServer(t, func(n int64) bool { return n == 1 })
	runEngine(t, remotecache.Config{URL: srv.URL, Token: "t"})
	c := takeConn(t, conns).conn
	require.Equal(t, protocol.TypeHello, readEnvelope(t, c).Type)
}

// Over a real WebSocket, the engine says hello, skips binary messages, and
// once welcomed answers the service's requests in text.
func TestEngineServesOverWebSocket(t *testing.T) {
	srv, conns := dialServer(t, nil)
	runEngine(t, remotecache.Config{URL: srv.URL, Token: "t", EngineName: "e"})
	c := takeConn(t, conns).conn

	hello := readEnvelope(t, c)
	require.Equal(t, protocol.TypeHello, hello.Type)
	var body protocol.Hello
	require.NoError(t, json.Unmarshal(hello.Body, &body))
	require.Equal(t, "e", body.EngineName)

	writeMessage(t, c, websocket.MessageBinary, []byte("skipped"))
	writeEnvelope(t, c, protocol.Envelope{ID: 1, Re: hello.ID, Type: protocol.TypeWelcome, Body: json.RawMessage(`{}`)})
	writeMessage(t, c, websocket.MessageBinary, []byte(`{"id":2,"type":"offer","body":{}}`))
	offer, err := json.Marshal(protocol.Offer{})
	require.NoError(t, err)
	writeEnvelope(t, c, protocol.Envelope{ID: 3, Type: protocol.TypeOffer, Body: offer})

	reply := readEnvelope(t, c)
	require.Equal(t, protocol.TypeOffered, reply.Type)
	require.Equal(t, uint64(3), reply.Re)
}
