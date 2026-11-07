package e2e

// The resilience scenarios (#12): a connection dropped mid-upload, an engine
// restarted mid-export, S3 throttling, 5xx and outage, and a service
// restarted mid-export. Each ends by checking that S3 is consistent and that
// the engines still work: B gets what A built after the fault as cache hits.

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
)

// faultQuery is a pipeline that only this call builds: alpine, then an exec
// that writes 1MB of random data and prints nonce. Its export uploads one
// new blob, the exec's layer; alpine's is stored already.
func faultQuery(nonce string) string {
	return `{ container { from(address: "alpine:3.22") {
      withExec(args: ["sh", "-c", "head -c 1048576 /dev/urandom > /fault.bin && echo ` + nonce + `"]) { stdout } } } }`
}

const faultPath = "container.from.withExec.stdout"

// storedSince are svc's "export stored" records of a cache after mark.
func storedSince(svc *Service, mark int, cacheID string) []LogRecord {
	return svc.Logs.Since(mark, "export stored", func(r LogRecord) bool { return r.Attr("cacheID") == cacheID })
}

// connLogs are svc's records of msg about connection conn after mark.
func connLogs(svc *Service, mark int, msg string, conn uint64) []LogRecord {
	return svc.Logs.Since(mark, msg, func(r LogRecord) bool { return r.Attr("conn") == fmt.Sprint(conn) })
}

// blobDigest is the digest of a blob request's object.
func blobDigest(r S3Request) digest.Digest {
	parts := strings.Split(r.Path, "/")
	if len(parts) < 2 {
		return ""
	}
	return digest.NewDigestFromEncoded(digest.Algorithm(parts[len(parts)-2]), parts[len(parts)-1])
}

// stalledBlobs are the blobs whose requests after mark a fault stalled.
func (s *state) stalledBlobs(mark int) []digest.Digest {
	var out []digest.Digest
	for _, r := range s.env.Proxy.Since(mark) {
		if r.Stalled && r.IsBlob() && !slices.Contains(out, blobDigest(r)) {
			out = append(out, blobDigest(r))
		}
	}
	return out
}

// bundlesHolding are the bundles, among the stored records, that name layer
// d in a chain.
func (s *state) bundlesHolding(recs []LogRecord, d digest.Digest) []string {
	var out []string
	for _, r := range recs {
		id := r.Attr("bundle")
		if !slices.Contains(out, id) && slices.Contains(s.chainLayersAll(id), d) {
			out = append(out, id)
		}
	}
	return out
}

func bundleIDs(recs []LogRecord) []string {
	var out []string
	for _, r := range recs {
		if id := r.Attr("bundle"); !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// checkServing checks that engine en still works after a fault: once svc
// has merged bundles into it, it runs query, which another engine built, and
// gets its exec as a cache hit, with that engine's output want.
func (s *state) checkServing(t *testing.T, svc *Service, en *Engine, bundles []string, query, want, out string) {
	t.Helper()
	env := s.env
	env.WaitFor(env.Settings.Timeout, fmt.Sprintf("bundles %v to be merged into %s", shorts(bundles), en.Name), func() (bool, error) {
		st, err := svc.Imports(en.Conn)
		if err != nil {
			return false, err
		}
		for _, id := range bundles {
			if !slices.ContainsFunc(st.Merged, func(m importer.MergedBundle) bool { return m.Bundle == id }) {
				return false, fmt.Errorf("bundle %s not merged yet", Short(id))
			}
		}
		return true, nil
	})
	mark := svc.Logs.Mark()
	res, took := en.Query(out, query)
	got := strings.TrimSpace(Field(res, faultPath))
	var hit, executed int
	env.WaitFor(30*time.Second, en.Name+"'s withExec cache span", func() (bool, error) {
		hit, executed = countOutcomes(svc.Logs.CacheSpans(mark, en.CacheID(), "Container.withExec"))
		return hit+executed > 0, nil
	})
	Check(t, got == want && hit > 0 && executed == 0,
		"%s works after it: it got the exec built before the fault as a cache hit (%d hit, %d executed), with its output %q (got %q), in %s",
		en.Name, hit, executed, want, got, took.Round(10*time.Millisecond))
}

// midUpload runs a new pipeline on A with its blob uploads stalled in the S3
// proxy, and returns once one is held: the stall, the pipeline's query and
// output, the stalled blobs, and the marks taken before.
func (s *state) midUpload(t *testing.T, out string) (stall *Fault, query, want string, stalled []digest.Digest, logMark int) {
	t.Helper()
	env := s.env
	stall = env.Proxy.Inject(&Fault{Name: "stall-blob-puts", Match: Methods("/staging/", http.MethodPut)})
	t.Cleanup(func() { env.Proxy.Remove(stall) })
	logMark, s3Mark := s.main.Logs.Mark(), env.Proxy.Mark()
	query = faultQuery(Nonce())
	res, _ := s.a.Query(out, query)
	want = strings.TrimSpace(Field(res, faultPath))
	env.WaitFor(env.Settings.Timeout, "A's export to upload a blob, stalled in the S3 proxy", func() (bool, error) {
		return env.Proxy.Hits(stall) > 0, nil
	})
	stalled = s.stalledBlobs(s3Mark)
	return stall, query, want, stalled, logMark
}

// disconnect (#12, case 1): A's WebSocket drops while its upload is in
// flight. The export is interrupted, not failed, nothing of it is published,
// A reconnects with backoff, and the export runs again on the new
// connection, from the start, and is stored.
func (s *state) disconnect(t *testing.T) {
	env, a := s.env, s.a
	cacheA, genA := a.CacheID(), a.Generation()
	stall, query, want, stalled, logMark := s.midUpload(t, "disconnect-run-a")
	idxBefore, bundlesBefore := env.Index(), env.BundleIDs()

	old := a.Conn
	t.Logf("dropping A's connection %d with the upload of %v in flight", old, shorts(digestStrings(stalled)))
	dropped := time.Now()
	if err := s.main.CloseEngine(old); err != nil {
		t.Fatal(err)
	}
	a.waitConn(old)
	reconnect := time.Since(dropped)
	// Before the stall ends: nothing of the export may be published.
	idxMid, bundlesMid := env.Index(), env.BundleIDs()
	env.Proxy.Remove(stall)
	env.WaitFor(env.Settings.Timeout, "A's export to be stored on its new connection", func() (bool, error) {
		return len(storedSince(s.main, logMark, cacheA)) > 0, nil
	})
	s.waitStored(s.main, a, "all of A's retained entries to be stored")
	stored := storedSince(s.main, logMark, cacheA)
	var holding []string
	for _, d := range stalled {
		holding = append(holding, s.bundlesHolding(stored, d)...)
	}
	holding = sortedCopy(holding)
	holding = slices.Compact(holding)

	Check(t, a.CacheID() == cacheA && a.Generation() == genA && a.Conn != old,
		"A reconnected in %s: connection %d, was %d; same cache %s and generation %d", reconnect.Round(100*time.Millisecond), a.Conn, old, Short(cacheA), genA)
	Check(t, len(connLogs(s.main, logMark, "export interrupted", old)) > 0 && len(connLogs(s.main, logMark, "export failed", old)) == 0,
		"the export on the dropped connection was interrupted, not failed")
	Check(t, len(stalled) > 0 && idxMid.NextNumber == idxBefore.NextNumber && len(idxMid.Entries) == len(idxBefore.Entries) && slices.Equal(bundlesMid, bundlesBefore),
		"nothing of the cut export was published: the index (%d entries) and the %d bundles are as before it", len(idxMid.Entries), len(bundlesMid))
	Check(t, len(stored) > 0 && len(holding) > 0,
		"the export ran again on the new connection and was stored, with the layer that was in flight (%v in bundles %v)", shorts(digestStrings(stalled)), shorts(holding))
	env.CheckConsistent(t, "after the dropped connection")
	s.checkServing(t, s.main, s.b, bundleIDs(stored), query, want, "disconnect-run-b")
}

// engineRestart (#12, case 2): A restarts while its upload is in flight. A
// clean restart keeps the cache ID and entry numbers and bumps the
// generation (ENTRY_DISCOVERY.md), so the export cut short is run again on
// the new generation, under the same numbers, and is stored as that
// generation's.
func (s *state) engineRestart(t *testing.T) {
	env, a := s.env, s.a
	cacheA, genA := a.CacheID(), a.Generation()
	stall, query, want, stalled, logMark := s.midUpload(t, "engine-restart-run-a")
	idxBefore, bundlesBefore := env.Index(), env.BundleIDs()
	// The entries the cut export carries, by A's engine numbers.
	var pending []uint64
	entries, err := s.main.Entries(a.Conn)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Retained && !e.Stored {
			pending = append(pending, e.Number)
		}
	}

	old := a.Conn
	t.Logf("restarting A with the upload of %v in flight", shorts(digestStrings(stalled)))
	start := time.Now()
	wait := a.RestartAsync()
	env.WaitFor(time.Minute, "A's connection to drop", func() (bool, error) {
		_, ok := s.main.EngineInfo(old)
		return !ok, nil
	})
	idxMid, bundlesMid := env.Index(), env.BundleIDs()
	env.Proxy.Remove(stall)
	wait()
	restart := time.Since(start)
	env.WaitFor(env.Settings.Timeout, "A's export to be stored after its restart", func() (bool, error) {
		return len(storedSince(s.main, logMark, cacheA)) > 0, nil
	})
	s.waitStored(s.main, a, "all of A's retained entries to be stored")
	stored := storedSince(s.main, logMark, cacheA)
	var holding []string
	for _, d := range stalled {
		holding = append(holding, s.bundlesHolding(stored, d)...)
	}
	holding = sortedCopy(holding)
	holding = slices.Compact(holding)
	idx := env.Index()
	newGen := 0
	for _, e := range idx.Entries {
		if slices.Contains(holding, e.Bundle) && e.Source.CacheID == cacheA && e.Source.Generation == a.Generation() {
			newGen++
		}
	}
	// Each engine number the cut export carried is stored under it, as
	// generation 2's: the numbers held across the restart.
	var renumbered []uint64
	for _, n := range pending {
		if m, ok := idx.Caches[cacheA].Numbers[n]; !ok || m.Generation != a.Generation() {
			renumbered = append(renumbered, n)
		}
	}

	Check(t, a.CacheID() == cacheA && a.Generation() == genA+1,
		"A restarted cleanly in %s: same cache %s, generation %d (was %d)", restart.Round(100*time.Millisecond), Short(a.CacheID()), a.Generation(), genA)
	Check(t, len(connLogs(s.main, logMark, "export interrupted", old)) > 0 && len(connLogs(s.main, logMark, "export failed", old)) == 0,
		"the export on the old generation's connection was interrupted, not failed")
	Check(t, len(stalled) > 0 && idxMid.NextNumber == idxBefore.NextNumber && len(idxMid.Entries) == len(idxBefore.Entries) && slices.Equal(bundlesMid, bundlesBefore),
		"nothing of the cut export was published: the index (%d entries) and the %d bundles are as before it", len(idxMid.Entries), len(bundlesMid))
	Check(t, len(pending) > 0 && len(renumbered) == 0,
		"the cut export's engine entries %v are each recorded under the same number as generation %d's (not: %v)", pending, a.Generation(), renumbered)
	Check(t, len(holding) > 0 && newGen > 0,
		"the export ran again on generation %d, under the same entry numbers, and was stored with the layer that was in flight (%d index entries of generation %d in bundles %v)",
		a.Generation(), newGen, a.Generation(), shorts(holding))
	env.CheckConsistent(t, "after the engine restart")
	s.checkServing(t, s.main, s.b, bundleIDs(stored), query, want, "engine-restart-run-b")
}

// s3Faults (#12, case 4): S3 throttles and fails requests. First a few
// requests of one export answer 503 SlowDown or 500 InternalError: the blob
// PUT the engine makes (to a staging key) is retried by the export path (a
// new export, with backoff, under a new stage), the bundle and index writes the service makes by its S3 client,
// and the export is stored. Then S3 answers every request with 503: the
// export fails, publishes nothing, and is stored once S3 is back.
func (s *state) s3Faults(t *testing.T) {
	env, a := s.env, s.a
	cacheA := a.CacheID()
	indexKey, err := env.Meta.IndexKey(env.Scope)
	if err != nil {
		t.Fatal(err)
	}
	faults := []*Fault{
		{Name: "blob-put-503", Match: Methods("/staging/", http.MethodPut), Status: http.StatusServiceUnavailable, Code: "SlowDown", Count: 2},
		{Name: "bundle-put-500", Match: Methods("/bundles/", http.MethodPut), Status: http.StatusInternalServerError, Code: "InternalError", Count: 1},
		{Name: "index-put-503", Match: Methods(indexKey, http.MethodPut), Status: http.StatusServiceUnavailable, Code: "SlowDown", Count: 2},
	}
	logMark, s3Mark := s.main.Logs.Mark(), env.Proxy.Mark()
	for _, f := range faults {
		env.Proxy.Inject(f)
		t.Cleanup(func() { env.Proxy.Remove(f) })
	}
	query1 := faultQuery(Nonce())
	res, _ := a.Query("s3-faults-run-a1", query1)
	want1 := strings.TrimSpace(Field(res, faultPath))
	env.WaitFor(env.Settings.Timeout, "A's export to be stored through the faults", func() (bool, error) {
		return len(storedSince(s.main, logMark, cacheA)) > 0, nil
	})
	s.waitStored(s.main, a, "all of A's retained entries to be stored")
	stored1 := storedSince(s.main, logMark, cacheA)
	reqs := env.Proxy.Since(s3Mark)
	retries := s.main.Logs.Since(logMark, "export: blobs failed to upload; trying again", nil)
	var waits []string
	for _, r := range retries {
		waits = append(waits, r.Attr("wait"))
	}
	for _, f := range faults {
		env.Proxy.Remove(f)
		// Each request the fault answered was made again, and that one the
		// store answered with success.
		var answered []string
		recovered := true
		for i, r := range reqs {
			if r.Fault != f.Name {
				continue
			}
			answered = append(answered, fmt.Sprintf("%s %d", r.Method, r.Status))
			// A blob upload is made again under the retry's own stage, so
			// at another key: the same blob's.
			same := func(o S3Request) bool { return o.Path == r.Path }
			if r.IsBlob() {
				same = func(o S3Request) bool { return o.IsBlob() && blobDigest(o) == blobDigest(r) }
			}
			if !slices.ContainsFunc(reqs[i+1:], func(o S3Request) bool {
				return same(o) && o.Method == r.Method && o.Fault == "" && o.Status/100 == 2
			}) {
				recovered = false
			}
		}
		Check(t, len(answered) == f.Count && recovered,
			"%s: the proxy answered %d of %d request(s) with %d %s (%v), and each was made again and succeeded", f.Name, len(answered), f.Count, f.Status, f.Code, answered)
	}
	Check(t, len(retries) > 0 && waits[0] == "1s",
		"the export path retried the export whose blob upload failed, with backoff: %d retries, waiting %v", len(retries), waits)
	Check(t, len(stored1) > 0 && len(s.main.Logs.Since(logMark, "export failed", nil)) == 0,
		"the export was stored through the faults, and no export failed")
	env.CheckConsistent(t, "after S3 throttled and failed requests")

	// S3 goes away.
	logMark = s.main.Logs.Mark()
	idxBefore, bundlesBefore := env.Index(), env.BundleIDs()
	down := env.Proxy.Inject(&Fault{Name: "s3-down", Status: http.StatusServiceUnavailable, Code: "ServiceUnavailable"})
	t.Cleanup(func() { env.Proxy.Remove(down) })
	t.Log("S3 answers every request with 503 from now")
	query2 := faultQuery(Nonce())
	res, _ = a.Query("s3-faults-run-a2", query2)
	want2 := strings.TrimSpace(Field(res, faultPath))
	failures := 0
	env.WaitFor(env.Settings.Timeout, "A's export to fail while S3 is down", func() (bool, error) {
		entries, err := s.main.Entries(a.Conn)
		if err != nil {
			return false, err
		}
		for _, e := range entries {
			failures = max(failures, e.Failures)
		}
		return failures > 0, nil
	})
	outage := env.Proxy.Hits(down)
	idxDown, bundlesDown := env.Index(), env.BundleIDs()
	env.Proxy.Remove(down)
	t.Logf("S3 is back, after answering %d requests with 503", outage)
	env.WaitFor(env.Settings.Timeout, "A's export to be stored once S3 is back", func() (bool, error) {
		return len(storedSince(s.main, logMark, cacheA)) > 0, nil
	})
	s.waitStored(s.main, a, "all of A's retained entries to be stored")
	stored2 := storedSince(s.main, logMark, cacheA)

	Check(t, failures > 0 && outage > 0, "while S3 was down, the export failed (%d failure(s) counted) and was tried again with backoff; S3 refused %d requests", failures, outage)
	Check(t, idxDown.NextNumber == idxBefore.NextNumber && len(idxDown.Entries) == len(idxBefore.Entries) && slices.Equal(bundlesDown, bundlesBefore),
		"nothing was published while S3 was down: the index (%d entries) and the %d bundles are as before", len(idxDown.Entries), len(bundlesDown))
	Check(t, len(stored2) > 0, "once S3 was back, the export was stored (bundles %v)", shorts(bundleIDs(stored2)))
	env.CheckConsistent(t, "after S3 was down")
	s.checkServing(t, s.main, s.b, bundleIDs(stored1), query1, want1, "s3-faults-run-b1")
	s.checkServing(t, s.main, s.b, bundleIDs(stored2), query2, want2, "s3-faults-run-b2")
}

// serviceRestart (#12, case 3): the service stops while A's upload is in
// flight, and a new one starts on the same address and scope. A and B
// reconnect with their backoff, the new service rebuilds what it needs from
// S3, merges every stored bundle into B again once (accepted: #12's first
// comment), and exports A's new sessions. The export cut short is lost until
// a session names its entries again (known gap).
func (s *state) serviceRestart(t *testing.T) {
	env, a, b := s.env, s.a, s.b
	cacheA := a.CacheID()
	stall, query, want, stalled, _ := s.midUpload(t, "service-restart-run-a")
	idxBefore := env.Index()
	before := s.imports(s.main, b)
	mergedBefore := map[string]bool{}
	for _, m := range before.Merged {
		mergedBefore[m.Bundle] = true
	}

	const down = 2 * time.Second
	t.Logf("restarting the service, down for %s, with A's upload of %v in flight", down, shorts(digestStrings(stalled)))
	start := time.Now()
	s.main = env.RestartService(s.main, "main-2", down)
	env.Proxy.Remove(stall)
	a.Attach(s.main)
	b.Attach(s.main)
	reconnect := time.Since(start)
	// The redo: what B had is merged into it again, less what expired since
	// (the superseded scenario's value has a TTL), which no plan merges.
	// C's bundles holding its imports, unmergeable before #47 was fixed
	// once prune had moved them, are merged again too.
	live := map[string]bool{}
	var unmergeable []string
	idxNow := env.Index()
	for _, c := range importer.Plan(idxNow, b.CacheID(), time.Now().Unix()) {
		if !mergedBefore[c.Bundle] {
			continue
		}
		var res *metastore.Resolution
		if c.Resolution != "" {
			res = env.Resolution(c.Resolution)
		}
		noSign := func(context.Context, digest.Digest) (dagql.BlobAddress, error) { return dagql.BlobAddress{}, nil }
		if _, err := importer.BuildMerge(context.Background(), idxNow, c.Bundle, env.Bundle(c.Bundle), res, c.Roots, noSign, env.Scope); err != nil {
			unmergeable = append(unmergeable, fmt.Sprintf("%s (%v)", Short(c.Bundle), err))
			continue
		}
		live[c.Bundle] = true
	}
	Check(t, len(unmergeable) == 0, "every live bundle B had before the restart builds a merge (unmergeable: %v)", unmergeable)
	var heldBefore []heldBundle
	for _, h := range s.cHeld {
		if mergedBefore[h.ID] {
			heldBefore = append(heldBefore, h)
		}
	}
	env.WaitFor(env.Settings.Timeout, "the new service to merge into B what the old one had", func() (bool, error) {
		st, err := s.main.Imports(b.Conn)
		if err != nil {
			return false, err
		}
		for id := range live {
			if !slices.ContainsFunc(st.Merged, func(m importer.MergedBundle) bool { return m.Bundle == id }) {
				return false, fmt.Errorf("bundle %s not merged again yet", Short(id))
			}
		}
		return true, nil
	})
	redone := s.imports(s.main, b)
	if len(heldBefore) > 0 {
		s.waitHeldMerged(t, s.main, b, heldBefore, "after the service restart")
	} else {
		t.Logf("B had none of C's bundles holding its imports before the restart (prune didn't run, or B wasn't merged them): not checking them (#47)")
	}
	// The redo's cost: from B's hello to the new service's last merge into
	// it, from the service's own log.
	mergeLogs := s.main.Logs.Since(0, "import: merged", func(r LogRecord) bool { return r.Attr("cacheID") == b.CacheID() })
	merges := len(mergeLogs)
	var redo time.Duration
	if hello := s.main.Logs.Since(0, "envelope", func(r LogRecord) bool {
		return r.Attr("conn") == fmt.Sprint(b.Conn) && r.Attr("type") == "hello"
	}); len(hello) > 0 && merges > 0 {
		redo = mergeLogs[merges-1].Time.Sub(hello[0].Time)
	}
	again := 0
	for range 3 {
		ctx, cancel := env.ctx()
		res, err := s.main.Import(ctx, b.Conn)
		cancel()
		if err != nil {
			t.Fatalf("sync B: %v", err)
		}
		again += len(res.Merged)
	}
	ownBack := 0
	for r := range mergedRoots(s.imports(s.main, a)) {
		if e, ok := env.Index().Entries[r]; ok && e.Source.CacheID == cacheA {
			ownBack++
		}
	}

	// The known gap: the export cut short isn't tried again, since the new
	// service never saw the spans that named its entries.
	time.Sleep(2*noEngineWait + time.Second)
	lost := true
	idxGap := env.Index()
	for _, d := range stalled {
		for _, id := range bundlesOf(idxGap) {
			if !slices.Contains(bundlesOf(idxBefore), id) && slices.Contains(s.chainLayersAll(id), d) {
				lost = false
			}
		}
	}

	// A new session names the entries again: the export resumes.
	logMark := s.main.Logs.Mark()
	res, _ := a.Query("service-restart-rerun-a", query)
	rerun := strings.TrimSpace(Field(res, faultPath))
	env.WaitFor(env.Settings.Timeout, "A's export to be stored by the new service", func() (bool, error) {
		return len(storedSince(s.main, logMark, cacheA)) > 0, nil
	})
	s.waitStored(s.main, a, "all of A's retained entries to be stored")
	stored := storedSince(s.main, logMark, cacheA)
	var holding []string
	for _, d := range stalled {
		holding = append(holding, s.bundlesHolding(stored, d)...)
	}
	holding = sortedCopy(holding)
	holding = slices.Compact(holding)

	Check(t, a.CacheID() == cacheA && b.Conn > 0,
		"A and B reconnected to the new service %s after the stop (down %s): A on connection %d, B on %d", reconnect.Round(100*time.Millisecond), down, a.Conn, b.Conn)
	Check(t, len(live) > 0 && again == 0,
		"the new service merged the %d live bundles of the %d B had again, once: %d merges, %d values, %s from B's hello; none more over 3 syncs (accepted, see #12)",
		len(live), len(mergedBefore), merges, len(redone.Values), redo.Round(time.Millisecond))
	Check(t, ownBack == 0, "nothing of A's own was merged back into A (%d)", ownBack)
	if lost {
		t.Logf("GAP   the export in flight at the restart was not tried again by the new service %s after it: its entries wait for a session to name them (known gap #45)", 2*noEngineWait+time.Second)
	} else {
		t.Logf("ok    the export in flight at the restart was stored by the new service on its own: the known gap may be fixed")
	}
	Check(t, rerun == want && len(holding) > 0,
		"exports resumed: A's next session of the pipeline (a local hit, same output) was exported and stored, with the layer that was in flight (bundles %v)", shorts(holding))
	env.CheckConsistent(t, "after the service restart")
	s.checkServing(t, s.main, b, bundleIDs(stored), query, want, "service-restart-run-b")
	// #47: across the whole run, every service, no merge failed because a
	// value its roots need was no longer stored.
	stale := staleMergeFailures(env)
	Check(t, len(stale) == 0, "no merge in the run failed with \"is no longer stored\", in any of the %d services' logs (seen: %v)", len(env.Services()), stale)
}

func digestStrings(ds []digest.Digest) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.String()
	}
	return out
}
