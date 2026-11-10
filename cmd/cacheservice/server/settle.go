package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// The engine's own requests since v1.0.0-beta.26, besides renew
// (zerobull-consulting/dagger#227):
//
//   - open: a session started on an engine with none live, which begins a
//     new idle period. The engine takes exports again, if it had given them
//     up, so the roots it refused meanwhile are due (exporter.Result.Paused).
//   - claims: the Dagger Cloud claim each session started under. This
//     service has no use for them; it only answers.
//   - settle: no session is live, and the engine asks the service to finish
//     what it wants of it. The service answers settled once it has exported
//     what is pending for the engine's cache, as a flush does (flush.go). An
//     engine that hears no settled within 20s of its last session's end
//     (engine/server's remoteCacheSettleBound) gives up its remote-cache
//     work: it cancels its exports in progress and refuses new ones until a
//     session starts again.
//   - lookup: the calls the engine's cache missed. This service keeps no
//     index of its values by call, and merges every stored root into every
//     engine anyway (package importer), so it looks nothing up: it answers
//     each miss as unknown, and sends no merge for it.

// DefaultSettleMarkerWait is how long a settle waits, from its arrival, for
// the first span of a listed session whose engine sent its completion
// marker, before it settles without it. The spans reach the service through
// the session's client, not the engine's connection, so the settle can come
// first; and they never come when the client sends its telemetry elsewhere.
// It leaves most of the engine's 20s for the exports.
const DefaultSettleMarkerWait = 5 * time.Second

// MaxSettleWait bounds a settle's wait. An engine gives up a settle it sent
// 20s after its last session ended, without saying so, and what then holds
// the wait back are the roots the engine refuses meanwhile: the bound, far
// past the engine's, only ends a wait nothing is listening for.
const MaxSettleWait = 2 * time.Minute

var (
	// errStaleSettle ends a settle whose period is no longer the
	// connection's.
	errStaleSettle = errors.New(protocol.StaleSettle)
	// errSettleTimeout ends a settle that waited MaxSettleWait.
	errSettleTimeout = errors.New("not settled within " + MaxSettleWait.String())
)

// periodState is a connection's idle period (protocol.Open): the hello's,
// then the latest open's.
type periodState struct {
	mu      sync.Mutex
	period  uint64
	changed chan struct{}
}

// current returns the period, and a channel closed when a later one opens.
func (p *periodState) current() (uint64, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.changed == nil {
		p.changed = make(chan struct{})
	}
	return p.period, p.changed
}

// open records period, and reports whether it is later than the one before.
func (p *periodState) open(period uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if period <= p.period {
		return false
	}
	p.period = period
	if p.changed != nil {
		close(p.changed)
		p.changed = nil
	}
	return true
}

// serveOpen answers open, and makes the roots that wait for the engine due.
func (c *engineConn) serveOpen(ctx context.Context, env protocol.Envelope) {
	var req protocol.Open
	if err := env.Decode(&req); err != nil {
		_ = c.replyError(ctx, env, err.Error())
		return
	}
	if c.period.open(req.Period) {
		if p := c.svc.export; p != nil && c.ns.tracker != nil && !c.revoked.Load() {
			c.ns.tracker.Opened(c.hello.CacheID, c.hello.Generation, p.now())
		}
	}
	_ = c.reply(ctx, env, protocol.TypeOpened, protocol.Opened{})
}

// serveClaims answers claims.
func (c *engineConn) serveClaims(ctx context.Context, env protocol.Envelope) {
	var req protocol.Claims
	if err := env.Decode(&req); err != nil {
		_ = c.replyError(ctx, env, err.Error())
		return
	}
	_ = c.reply(ctx, env, protocol.TypeClaimed, protocol.Claimed{})
}

// serveLookup answers lookup: every miss unknown (see above).
func (c *engineConn) serveLookup(ctx context.Context, env protocol.Envelope) {
	var req protocol.Lookup
	if err := env.Decode(&req); err != nil {
		_ = c.replyError(ctx, env, err.Error())
		return
	}
	answer := protocol.LookedUp{Misses: make([]protocol.LookedUpMiss, len(req.Misses))}
	for i := range answer.Misses {
		answer.Misses[i] = protocol.LookedUpMiss{Status: protocol.LookupUnknown}
	}
	_ = c.reply(ctx, env, protocol.TypeLookedUp, answer)
}

// serveSettle answers settle: settled once the service is done with the
// engine's cache for the period (Service.settle), or protocol.StaleSettle
// when the period isn't, or is no longer, the connection's. It answers
// nothing when the connection ends first.
func (c *engineConn) serveSettle(ctx context.Context, env protocol.Envelope) {
	var req protocol.Settle
	if err := env.Decode(&req); err != nil {
		_ = c.replyError(ctx, env, err.Error())
		return
	}
	start := time.Now()
	err := c.svc.settle(ctx, c, req)
	switch {
	case err == nil:
		c.logger.Info("settled", "period", req.Period, "sessions", len(req.Sessions), "took", time.Since(start).Round(time.Millisecond))
		_ = c.reply(ctx, env, protocol.TypeSettled, protocol.Settled{})
	case errors.Is(err, errStaleSettle):
		_ = c.replyError(ctx, env, protocol.StaleSettle)
	case errors.Is(err, errSettleTimeout):
		c.logger.Warn("settle: gave up", "period", req.Period, "sessions", len(req.Sessions), "after", time.Since(start).Round(time.Millisecond))
		_ = c.replyError(ctx, env, err.Error())
	default:
		c.logger.Info("settle ended without an answer", "period", req.Period, "sessions", len(req.Sessions),
			"after", time.Since(start).Round(time.Millisecond), "error", err)
	}
}

// settle waits until the service has nothing left to ask of c's engine for
// req's period, as the engine's settle wants:
//
//   - every listed session whose engine sent its completion marker was
//     seen, or DefaultSettleMarkerWait passed since the settle came;
//   - no session of the cache is waited for, as a flush waits for them: one
//     still sending spans, or ended with some of its counted spans missing,
//     until the tracker's Settle (at most 2s) passed since its last span;
//   - no root of the cache is dirty, in flight or waiting for another try.
//
// Unlike a flush, it makes nothing due sooner: the scheduler exports the
// cache's entries as it would anyway, a session's once its carrier came
// with every span in, so that a session's entries still go in one export,
// with all their parts, rather than span by span as they arrive.
//
// With the export path off, there is nothing to wait for. It returns
// errStaleSettle when the period isn't the connection's, or a later one
// opens meanwhile, ctx's error when the connection ends first, and
// errSettleTimeout once it waited MaxSettleWait, on its own clock: the
// engine abandons a settle it gave up on without saying so, and what then
// holds this back are the roots the engine refuses (exporter.Result.Paused),
// until its next open ends it as stale.
func (s *Service) settle(ctx context.Context, c *engineConn, req protocol.Settle) error {
	period, _ := c.period.current()
	if req.Period > period {
		// The engine opens a period before it settles it; taken as said.
		c.period.open(req.Period)
	} else if req.Period < period {
		return errStaleSettle
	}
	p := s.export
	tracker := c.ns.tracker
	if p == nil || tracker == nil {
		return nil
	}
	cacheID := c.hello.CacheID
	var marked []string
	for _, session := range req.Sessions {
		if session.Marker && validEngineID(session.ID) {
			marked = append(marked, session.ID)
		}
	}
	markerDeadline := p.now().Add(time.Duration(s.settleMarkerWait.Load()))

	p.changedWaiters.Add(1)
	defer p.changedWaiters.Add(-1)
	poll := time.NewTicker(min(p.tick, maxDrainPoll))
	defer poll.Stop()
	giveUp := time.NewTimer(time.Duration(s.maxSettleWait.Load()))
	defer giveUp.Stop()
	for {
		// Before the status: a kick after it wakes the wait below.
		changed := p.changed()
		current, opened := c.period.current()
		if current != req.Period {
			return errStaleSettle
		}
		now := p.now()
		st := tracker.FlushStatus(cacheID, now, time.Time{})
		unseen := 0
		if now.Before(markerDeadline) {
			unseen = tracker.UnseenSessions(cacheID, marked)
		}
		if flushEmpty(st) && unseen == 0 {
			return nil
		}
		select {
		case <-changed:
		case <-opened:
		case <-poll.C:
		case <-giveUp.C:
			return errSettleTimeout
		case <-p.ctx.Done():
			return context.Cause(p.ctx)
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}
