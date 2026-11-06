package main

// The linger after the drain (zerobull-consulting/dagger#102), on the built
// binary: after the drain, the engine address keeps answering the engines'
// OTLP requests until each connected engine whose events come here sent
// engine.stop, or -drain-linger passes; a second signal ends it at once;
// -drain-linger 0 stops right after the drain, as before; nothing is stored
// meanwhile; and the whole stop stays within -drain-timeout plus
// -drain-linger plus shutdownGrace. Like main_test.go, these run the built
// command against MinIO, and skip unless CACHESERVICE_S3_ENDPOINT is set.

import (
	"bytes"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"github.com/dagger/dagger/engine/telemetryattrs"
)

// event posts one of the engine's cache events, as its event export does,
// and returns the status.
func (b *binaryTest) event(kind string) int {
	b.t.Helper()
	kv := func(k, v string) *commonpb.KeyValue {
		return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
	}
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: telemetryattrs.EngineEventScope},
			LogRecords: []*logspb.LogRecord{{
				Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: `{"clean":true}`}},
				Attributes: []*commonpb.KeyValue{
					kv(telemetryattrs.EngineEventAttr, kind),
					kv(telemetryattrs.EngineCacheAttr, b.cache.ID+"/"+strconv.FormatUint(b.cache.Generation, 10)),
				},
			}},
		}},
	}}}
	data, err := proto.Marshal(req)
	if err != nil {
		b.t.Fatal(err)
	}
	hreq, err := http.NewRequest(http.MethodPost, b.url+"/v1/logs", bytes.NewReader(data))
	if err != nil {
		b.t.Fatal(err)
	}
	hreq.Header.Set("Content-Type", "application/x-protobuf")
	hreq.SetBasicAuth("engine-token", "")
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		// The service is gone: as the engine's export sees it.
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func (b *binaryTest) running() bool {
	select {
	case <-b.exited:
		return false
	default:
		return true
	}
}

// After its drain, the service keeps answering the engine's events until the
// engine's engine.stop arrives, then stops at once and exits 0: the engine's
// own stop isn't held for its event export's bound.
func TestMinIOBinaryLingersUntilEngineStop(t *testing.T) {
	b := startBinary(t, "-drain-timeout", "10s")
	if code := b.event(telemetryattrs.EngineEventStart); code != http.StatusOK {
		t.Fatalf("engine.start: %d", code)
	}
	b.waitLog(10*time.Second, `kind=engine.start`)
	b.signal()
	b.waitLog(10*time.Second, `msg="linger: started`)
	// Still up: the engine's events are answered, and the process waits.
	time.Sleep(500 * time.Millisecond)
	if !b.running() {
		t.Fatal("the service exited before the engine's engine.stop")
	}
	if code := b.event(telemetryattrs.EngineEventPrune); code != http.StatusOK {
		t.Fatalf("an engine event while lingering: %d, want 200", code)
	}
	start := time.Now()
	if code := b.event(telemetryattrs.EngineEventStop); code != http.StatusOK {
		t.Fatalf("engine.stop while lingering: %d, want 200", code)
	}
	b.waitExit(shutdownGrace + 2*time.Second)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the service took %s to exit after the engine's engine.stop", took)
	}
	b.logHas(t, `kind=engine.stop`, `msg="linger: waiting for engine.stop" namespace=default cacheID=cache-a`,
		`msg="linger: done" outcome=stopped`, `stopped=1`, `msg=stopped took=`)
}

// An engine that never sends engine.stop holds the stop only until
// -drain-linger: the process then exits 0, and the whole stop stays within
// -drain-timeout plus -drain-linger plus shutdownGrace.
func TestMinIOBinaryLingerBounded(t *testing.T) {
	const drainTimeout, linger = 2 * time.Second, 2 * time.Second
	b := startBinary(t, "-drain-timeout", drainTimeout.String(), "-drain-linger", linger.String())
	b.event(telemetryattrs.EngineEventStart)
	b.waitLog(10*time.Second, `kind=engine.start`)
	start := time.Now()
	b.signal()
	took := b.waitExit(drainTimeout + linger + shutdownGrace + 5*time.Second)
	took = max(took, time.Since(start))
	if bound := drainTimeout + linger + shutdownGrace; took > bound {
		t.Fatalf("the stop took %s, past -drain-timeout plus -drain-linger plus the shutdown's bound (%s)", took, bound)
	}
	if took < linger {
		t.Fatalf("the stop took %s: it didn't wait for the engine's engine.stop", took)
	}
	b.logHas(t, `msg="linger: done" outcome=deadline`, `msg="linger: no engine.stop" namespace=default cacheID=cache-a`)
}

// A second signal ends the linger at once.
func TestMinIOBinarySecondSIGTERMEndsLinger(t *testing.T) {
	b := startBinary(t, "-drain-linger", "10m")
	b.event(telemetryattrs.EngineEventStart)
	b.waitLog(10*time.Second, `kind=engine.start`)
	b.signal()
	b.waitLog(10*time.Second, `msg="linger: started`)
	b.signal()
	if took := b.waitExit(shutdownGrace + 2*time.Second); took > 2*time.Second {
		t.Fatalf("the service took %s to exit after the second SIGTERM", took)
	}
	b.logHas(t, "second signal: stopping without finishing the linger", `msg="linger: done" outcome=canceled`)
}

// -drain-linger 0 is the stop as before: it exits right after the drain,
// whatever the engine's events.
func TestMinIOBinaryNoLinger(t *testing.T) {
	b := startBinary(t, "-drain-linger", "0")
	b.event(telemetryattrs.EngineEventStart)
	b.waitLog(10*time.Second, `kind=engine.start`)
	b.signal()
	if took := b.waitExit(shutdownGrace + 2*time.Second); took > 2*time.Second {
		t.Fatalf("the stop with -drain-linger 0 took %s", took)
	}
	if strings.Contains(b.logs.String(), "linger:") {
		t.Fatal("the stop with -drain-linger 0 lingered")
	}
	b.logHas(t, `msg="drain: done" outcome=complete`, `msg=stopped took=`)
}

// Nothing is stored while lingering: an export the drain's deadline left in
// flight is cut off as the linger starts, and what it would have stored
// isn't, even though its engine answers during the linger. The stop exits 0:
// a drain cut off by its deadline isn't an unclean stop.
func TestMinIOBinaryLingerStoresNothing(t *testing.T) {
	const drainTimeout = time.Second
	b := startBinary(t, "-drain-timeout", drainTimeout.String(), "-drain-linger", "1m")
	b.event(telemetryattrs.EngineEventStart)
	b.build()
	release := b.cache.Hold()
	defer release()
	b.postSpans("session-1", true)
	b.waitFor(10*time.Second, "the export to be in flight", func() bool { return b.cache.InFlight() == 1 })
	b.signal()
	b.waitLog(10*time.Second, `msg="linger: started`)
	release()
	// The engine's export, released, now has the linger to finish in.
	time.Sleep(2 * time.Second)
	if b.stored() {
		t.Fatal("an export was stored while lingering")
	}
	b.event(telemetryattrs.EngineEventStop)
	b.waitExit(shutdownGrace + 2*time.Second)
	if b.stored() {
		t.Fatal("the export the drain's deadline cut off was stored")
	}
	b.logHas(t, `msg="drain: done" outcome=deadline`, `msg="linger: done" outcome=stopped`)
	if strings.Contains(b.logs.String(), `msg="export stored"`) {
		t.Fatal("the log says an export was stored")
	}
}
