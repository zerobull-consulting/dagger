package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// fakeAdapter is an engine cache with nothing in it.
type fakeAdapter struct{}

func (fakeAdapter) CacheIdentity() (string, uint64) { return "cache-1", 3 }

func (fakeAdapter) Export(ctx context.Context, req protocol.Export, consume func(context.Context, remotecache.Export) error) error {
	skipped := make([]protocol.SkippedRoot, 0, len(req.Roots))
	for _, n := range req.Roots {
		skipped = append(skipped, protocol.SkippedRoot{Number: n, Reason: protocol.SkipGone})
	}
	return consume(ctx, remotecache.Export{Skipped: skipped})
}

func (fakeAdapter) Merge(context.Context, protocol.Merge) (protocol.Merged, error) {
	return protocol.Merged{}, nil
}

func (fakeAdapter) OfferParts(_ context.Context, req protocol.Offer) (protocol.Offered, error) {
	var offered protocol.Offered
	for _, item := range req.Items {
		offered.Items = append(offered.Items, protocol.OfferedItem{Number: item.Number, Gone: true})
	}
	return offered, nil
}

func (fakeAdapter) TakeRenewalRequest(ctx context.Context) (*dagql.RenewalRequest, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (fakeAdapter) ReplyRenewal(dagql.RenewalReply) dagql.RenewalReplyDisposition {
	return dagql.RenewalReplyDiscarded
}

// waitEngines waits until the service has n welcomed engines, and returns
// their connections.
func waitEngines(t *testing.T, svc *service, n int) []*engineConn {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		svc.mu.Lock()
		conns := make([]*engineConn, 0, len(svc.conns))
		for _, c := range svc.conns {
			conns = append(conns, c)
		}
		svc.mu.Unlock()
		if len(conns) == n {
			return conns
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d engines", n)
	return nil
}

func TestEngineConnectsAndReconnects(t *testing.T) {
	svc := newService(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewServer(svc.handler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- remotecache.Run(ctx, fakeAdapter{}, remotecache.Config{
			URL:           srv.URL,
			Token:         "secret-token",
			EngineVersion: "v-test",
			EngineName:    "engine-test",
		})
	}()

	first := waitEngines(t, svc, 1)[0]
	if first.hello.CacheID != "cache-1" || first.hello.Generation != 3 || first.hello.EngineName != "engine-test" {
		t.Fatalf("unexpected hello: %+v", first.hello)
	}
	if first.token == "secret-token" {
		t.Fatal("token logged in the clear")
	}

	// The engine answers a service request on the welcomed connection.
	reply, err := first.request(ctx, protocol.TypeExport, json.RawMessage(`{"roots":[7]}`))
	if err != nil {
		t.Fatal(err)
	}
	if reply.Type != protocol.TypeExported {
		t.Fatalf("export answered with %s", reply.Type)
	}
	var exported protocol.Exported
	if err := reply.Decode(&exported); err != nil {
		t.Fatal(err)
	}
	if len(exported.Skipped) != 1 || exported.Skipped[0].Reason != protocol.SkipGone {
		t.Fatalf("unexpected export reply: %+v", exported)
	}

	// The engine reconnects, with a new hello, after the service drops it.
	_ = first.conn.CloseNow()
	deadline := time.Now().Add(10 * time.Second)
	for {
		conns := waitEngines(t, svc, 1)
		if conns[0].id != first.id {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("engine did not reconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestEngineWithoutTokenRejected(t *testing.T) {
	svc := newService(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewServer(svc.handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + protocol.EnginePath)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestDebugRequest(t *testing.T) {
	svc := newService(slog.New(slog.DiscardHandler), "")
	srv := httptest.NewServer(svc.handler())
	defer srv.Close()
	debug := httptest.NewServer(svc.debugHandler())
	defer debug.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go remotecache.Run(ctx, fakeAdapter{}, remotecache.Config{URL: srv.URL, Token: "t"})
	c := waitEngines(t, svc, 1)[0]

	resp, err := http.Get(debug.URL + "/debug/engines")
	if err != nil {
		t.Fatal(err)
	}
	var infos []engineInfo
	err = json.NewDecoder(resp.Body).Decode(&infos)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].CacheID != "cache-1" {
		t.Fatalf("unexpected engines: %+v", infos)
	}

	resp, err = http.Post(debug.URL+"/debug/engines/"+strconv.FormatUint(c.id, 10)+"/request?type=offer", "application/json",
		strings.NewReader(`{"items":[{"number":4,"offers":[]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var reply protocol.Envelope
	if err := json.Unmarshal(body, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Type != protocol.TypeOffered {
		t.Fatalf("offer answered with %s: %s", reply.Type, body)
	}
}
