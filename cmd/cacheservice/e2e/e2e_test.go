package e2e

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// The pipeline: alpine, then two execs that each write 4MB of random data
// and a line with a random nonce to /stamp. Random output makes a re-run
// visible: an exec that ran again would write other stamps.
const execs = `withExec(args: ["sh", "-c", "head -c 4194304 /dev/urandom > /layer1.bin && echo \"exec1 $(cat /proc/sys/kernel/random/uuid) $(sha256sum /layer1.bin | cut -c1-16)\" > /stamp"]) {
        withExec(args: ["sh", "-c", "head -c 4194304 /dev/urandom > /layer2.bin && echo \"exec2 $(cat /proc/sys/kernel/random/uuid) $(sha256sum /layer2.bin | cut -c1-16)\" >> /stamp"]) {`

const pipeline = `{ container { from(address: "alpine:3.22") {
      ` + execs + `
          file(path: "/stamp") { contents }
} } } } }`

const stampPath = "container.from.withExec.withExec.file.contents"

// verifyQuery is one more exec, with a nonce so it is never cached, over the
// pipeline's container. It has to read both layer files, so the engine needs
// every layer of the chain on disk, while the pipeline itself, cached down
// to its result string, needs none.
func verifyQuery() string {
	return `{ container { from(address: "alpine:3.22") {
      ` + execs + `
          withExec(args: ["sh", "-c", "echo verify-` + Nonce() + ` >/dev/null; cat /stamp; sha256sum /layer1.bin /layer2.bin | cut -c1-16"]) { stdout }
} } } } }`
}

const verifyPath = "container.from.withExec.withExec.withExec.stdout"

// state is what the scenarios share: the run, the main service, engines A
// and B, and what A's pipeline stored.
type state struct {
	env  *Env
	main *Service
	a, b *Engine
	// stampA is A's pipeline output, and sumsA the layer files' checksums
	// in it, one per line.
	stampA, sumsA string
	// indexAfterA is the index once A's entries were stored.
	indexAfterA *metastore.Index
	// layersA is the longest fs chain in A's bundles: alpine's layer and
	// both execs', base first.
	layersA []digest.Digest

	// The replica, and engine C on it, once a scenario started them.
	replica *Service
	c       *Engine
	// cHeld are C's stored bundles that hold entries C had imported, which
	// prune found and moved (#47).
	cHeld []heldBundle
}

// TestRealEngines runs the remote cache with real engines. Engine A builds
// a pipeline, the service stores it in MinIO and merges it into engine B,
// and B gets cache hits for it. The scenarios after that reuse A, B and
// what they stored; select them with -run 'TestRealEngines/<name>'.
func TestRealEngines(t *testing.T) {
	env := New(t)
	st := &state{env: env, main: env.StartService("main", ServiceOptions{})}
	if !st.hit() {
		t.Fatal("A's entries didn't reach B: skipping the other scenarios")
	}
	env.Run("reexport", st.reexport)
	env.Run("renew", st.renew)
	env.Run("prune", st.prune)
	env.Run("superseded", st.superseded)
	env.Run("parts", st.parts)
	// Streaming exports (zerobull-consulting/dagger#134): an entry stored
	// while its session still runs.
	env.Run("streaming", st.streaming)
	// A read-only service (-export=false, #65): its own service and engine,
	// and nothing written.
	env.Run("readonly", st.readonly)
	// Auth and namespaces (#10): its own service, with tokens, and engines.
	env.Run("namespaces", st.namespaces)
	// Upload verification (#23): A's uploads corrupted on their way to S3.
	env.Run("verify", st.verify)
	// Retention and GC (#11): its own service, namespace and engines.
	env.Run("gc", st.gc)
	// The resilience scenarios (#12), last: they break things on purpose,
	// and service-restart replaces the main service.
	env.Run("disconnect", st.disconnect)
	env.Run("engine-restart", st.engineRestart)
	env.Run("s3-faults", st.s3Faults)
	env.Run("service-restart", st.serviceRestart)
	// A clean stop (#63): the service drains with an export in flight, and
	// replaces the main service once more.
	env.Run("drain", st.drain)
	// Last, over every service's log: the largest OTLP requests the engines
	// sent, against the limits (zerobull-consulting/dagger#77).
	env.Run("otlp-sizes", st.otlpSizes)
}

// retainedStored reports whether every retained entry the service tracks
// for conn is stored, and, with some, that there is at least one.
func retainedStored(svc *Service, conn uint64, some bool) (bool, error) {
	entries, err := svc.Entries(conn)
	if err != nil {
		return false, err
	}
	n := 0
	for _, e := range entries {
		if !e.Retained {
			continue
		}
		n++
		if !e.Stored {
			return false, fmt.Errorf("entry %d not stored yet", e.Number)
		}
	}
	return n > 0 || !some, nil
}

func (s *state) waitStored(svc *Service, en *Engine, what string) {
	s.env.T.Helper()
	s.env.WaitFor(s.env.Settings.Timeout, what, func() (bool, error) { return retainedStored(svc, en.Conn, true) })
}

func (s *state) imports(svc *Service, en *Engine) importer.State {
	s.env.T.Helper()
	st, err := svc.Imports(en.Conn)
	if err != nil {
		s.env.T.Fatalf("imports of %s: %v", en.Name, err)
	}
	return st
}

// mergedRoots are the service numbers merged into an engine.
func mergedRoots(st importer.State) map[uint64]bool {
	out := map[uint64]bool{}
	for _, m := range st.Merged {
		for _, r := range m.Roots {
			out[r] = true
		}
	}
	return out
}

func (s *state) waitMergedAll(svc *Service, en *Engine, roots []uint64) {
	s.env.T.Helper()
	s.env.WaitFor(s.env.Settings.Timeout, svc.Name+" to merge A's entries into "+en.Name, func() (bool, error) {
		st, err := svc.Imports(en.Conn)
		if err != nil {
			return false, err
		}
		merged := mergedRoots(st)
		for _, r := range roots {
			if !merged[r] {
				return false, fmt.Errorf("root %d not merged yet", r)
			}
		}
		return true, nil
	})
}

// chainLayers returns the longest fs chain of a set of bundles.
func (s *state) chainLayers(ids []string) []digest.Digest {
	var longest []digest.Digest
	for _, id := range ids {
		for _, o := range s.env.Bundle(id).Outputs {
			if o.Address.Part != "fs" || o.Chain == nil {
				continue
			}
			var chain []digest.Digest
			for _, l := range o.Chain.Layers {
				chain = append(chain, l.Descriptor.Digest)
			}
			if len(chain) > len(longest) {
				longest = chain
			}
		}
	}
	return longest
}

func bundlesOf(idx *metastore.Index) []string {
	set := map[string]bool{}
	for _, e := range idx.Entries {
		set[e.Bundle] = true
	}
	return slices.Sorted(maps.Keys(set))
}

// stampSums are the checksums in a stamp: the third field of each line.
func stampSums(stamp string) string {
	var sums []string
	for _, line := range strings.Split(strings.TrimSpace(stamp), "\n") {
		if f := strings.Fields(line); len(f) >= 3 {
			sums = append(sums, f[2])
		}
	}
	return strings.Join(sums, "\n")
}

func lines(s string) []string { return strings.Split(strings.TrimSpace(s), "\n") }

func countOutcomes(spans []LogRecord) (hit, executed int) {
	for _, r := range spans {
		switch r.Attr("outcome") {
		case "hit":
			hit++
		case "executed":
			executed++
		}
	}
	return hit, executed
}

func blobsOf(reqs []S3Request, d digest.Digest, ok func(S3Request) bool) bool {
	for _, r := range reqs {
		if r.IsBlobOf(d) && (ok == nil || ok(r)) {
			return true
		}
	}
	return false
}

// hit is the base run: A builds, B hits. It reports whether A's entries
// reached B, which the other scenarios need.
func (s *state) hit() bool {
	env, t := s.env, s.env.T
	engines := env.StartEngines(s.main, "a", "b")
	s.a, s.b = engines[0], engines[1]
	cacheB := s.b.CacheID()

	res, timeA := s.a.Query("run-a", pipeline)
	// Trimmed, as the outputs compared with it are: the file ends with a
	// newline, which the shell version's $(...) dropped.
	s.stampA = strings.TrimSpace(Field(res, stampPath))
	s.sumsA = stampSums(s.stampA)
	t.Logf("A took %s. Stamp:\n%s", timeA.Round(10*time.Millisecond), s.stampA)

	// Every retained entry, not just the first: a session can be exported
	// in several batches, and the index read below must hold all of them.
	s.waitStored(s.main, s.a, "all of A's retained entries to reach the index")
	s.indexAfterA = env.Index()
	env.Save("index-after-a", s.indexAfterA)
	if len(s.indexAfterA.Entries) == 0 {
		t.Fatal("the index in MinIO has no entries")
	}
	s.layersA = s.chainLayers(bundlesOf(s.indexAfterA))
	if len(s.layersA) == 0 {
		t.Fatal("A's bundles name no fs layers")
	}
	t.Logf("index: %d entries, %d roots; A's chain: %d layers", len(s.indexAfterA.Entries), len(s.indexAfterA.Roots), len(s.layersA))

	env.WaitFor(env.Settings.Timeout, "a merge into B", func() (bool, error) {
		st, err := s.main.Imports(s.b.Conn)
		return err == nil && len(st.Merged) > 0, err
	})

	logMark, s3Mark := s.main.Logs.Mark(), env.Proxy.Mark()
	res, timeB := s.b.Query("run-b", pipeline)
	stampB := strings.TrimSpace(Field(res, stampPath))
	t.Logf("B took %s. Stamp:\n%s", timeB.Round(10*time.Millisecond), stampB)
	// The pipeline has two withExec calls; their spans may come in several
	// batches.
	var hitB, executedB int
	env.WaitFor(30*time.Second, "B's withExec cache spans", func() (bool, error) {
		hitB, executedB = countOutcomes(s.main.Logs.CacheSpans(logMark, cacheB, "Container.withExec"))
		return hitB+executedB >= 2, nil
	})

	res, timeVerify := s.b.Query("verify-b", verifyQuery())
	verified := strings.TrimSpace(Field(res, verifyPath))
	t.Logf("B's verify took %s. Output:\n%s", timeVerify.Round(10*time.Millisecond), verified)
	gets := env.Proxy.Blobs(s3Mark, http.MethodGet)

	s.waitStored(s.main, s.b, "all of B's retained entries to be stored")
	env.WaitFor(env.Settings.Timeout, "a merge into A", func() (bool, error) {
		st, err := s.main.Imports(s.a.Conn)
		return err == nil && len(st.Merged) > 0, err
	})
	importsB, importsA := s.imports(s.main, s.b), s.imports(s.main, s.a)
	final := env.Index()
	env.Save("imports-b", importsB)
	env.Save("imports-a", importsA)
	env.Save("index-final", final)

	// The index must hold A's entries, unchanged, and only the entries B
	// built itself (the verify exec's), each once (#35): B's imports keep
	// A's numbers, so no number is handed out for them, and none of B's
	// entries is one it imported. Nothing expires during a run, so every
	// number handed out is an entry.
	entriesB := 0
	imported := map[uint64]bool{}
	for _, v := range importsB.Values {
		imported[v.Number] = true
	}
	reexported := 0
	for _, e := range final.Entries {
		if e.Source.CacheID == cacheB {
			entriesB++
			if imported[e.Source.Number] {
				reexported++
			}
		}
	}
	aUnchanged := true
	for n, e := range s.indexAfterA.Entries {
		if f, ok := final.Entries[n]; !ok || f.Source != e.Source {
			aUnchanged = false
		}
	}
	// The merges into A carry only B's new entries, not A's own back.
	echoed := 0
	for r := range mergedRoots(importsA) {
		if _, ok := s.indexAfterA.Entries[r]; ok {
			echoed++
		}
	}
	var missing []string
	for _, d := range s.layersA {
		if !blobsOf(gets, d, nil) {
			missing = append(missing, Short(d.String()))
		}
	}
	sumsB := strings.Join(lines(verified)[max(0, len(lines(verified))-2):], "\n")

	Check(t, s.stampA != "" && s.stampA == stampB, "cache hit: B's stamps are A's, so neither exec ran on B")
	Check(t, executedB == 0 && hitB == 2, "B's spans: Container.withExec %d hit, %d executed", hitB, executedB)
	Check(t, s.sumsA == sumsB, "layers: the layer files on B are A's (checksums match A's stamps)")
	afterA, total := len(s.indexAfterA.Entries), len(final.Entries)
	Check(t, entriesB > 0 && total == afterA+entriesB && int(final.NextNumber)-1 == total && reexported == 0 && echoed == 0 && aUnchanged,
		"index: %d entries: A's %d (unchanged: %v) and %d new from B (%d of them B's imports), %d numbers handed out, %d of A's own merged back into A",
		total, afterA, aUnchanged, entriesB, reexported, final.NextNumber-1, echoed)
	Check(t, len(gets) > 0 && len(missing) == 0, "from S3: B's engine read all %d layers of A's chain through the proxy (%d blob GETs; missing: %v)", len(s.layersA), len(gets), missing)
	t.Logf("timings: A %s (empty cache, builds), B %s (empty cache, entries merged from S3), B verify %s (one new exec, layers pulled from S3)",
		timeA.Round(10*time.Millisecond), timeB.Round(10*time.Millisecond), timeVerify.Round(10*time.Millisecond))
	return len(importsB.Merged) > 0
}

// reexport (#30): A's stored entries exported again upload nothing, keep
// their numbers and write no bundle; a second pipeline over alpine uploads
// only its own layers.
func (s *state) reexport(t *testing.T) {
	env := s.env
	entries, err := s.main.Entries(s.a.Conn)
	if err != nil {
		t.Fatal(err)
	}
	// Every stored root, with every part the tracker knows of, as the
	// tracker's own export sends them.
	var req exporter.Request
	for _, e := range entries {
		if !e.Retained || !e.Stored {
			continue
		}
		req.Roots = append(req.Roots, e.Number)
		for _, part := range e.Parts {
			var out protocol.ExportOutput
			if err := json.Unmarshal([]byte(part), &out.Address); err != nil {
				t.Fatalf("part %q: %v", part, err)
			}
			out.Number = e.Number
			req.Outputs = append(req.Outputs, out)
		}
	}
	env.Save("reexport-request", req)
	before, bundlesBefore := env.Index(), env.BundleIDs()
	ctx, cancel := env.ctx()
	start := time.Now()
	res, err := s.main.Export(ctx, s.a.Conn, req)
	took := time.Since(start)
	cancel()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	env.Save("reexport-result", res)
	after, bundlesAfter := env.Index(), env.BundleIDs()

	t.Logf("repeated export took %s", took.Round(10*time.Millisecond))
	Check(t, reflect.DeepEqual(sortedCopy(res.Stored), sortedCopy(req.Roots)), "every root stored again: %v", res.Stored)
	Check(t, len(res.Uploaded) == 0, "no blob uploaded (uploaded: %v)", res.Uploaded)
	missing := 0
	for _, d := range s.layersA {
		if !slices.Contains(res.Present, d) {
			missing++
		}
	}
	Check(t, missing == 0, "every blob present: %d blobs, all %d fs layers of A's chain among them", len(res.Present), len(s.layersA))
	Check(t, reflect.DeepEqual(before.Entries, after.Entries) && before.NextNumber == after.NextNumber,
		"the index is unchanged: the entries keep their service numbers and bundles (%d entries, next number %d)", len(after.Entries), after.NextNumber)
	Check(t, slices.Equal(bundlesBefore, bundlesAfter), "no new bundle: %d stored, as before; the export stored %v", len(bundlesAfter), shorts(res.Bundles))

	// A second pipeline on A over the same alpine.
	alpine := s.layersA[0]
	cacheA := s.a.CacheID()
	logMark, s3Mark := s.main.Logs.Mark(), env.Proxy.Mark()
	s.a.Query("reexport-run-a2", `{ container { from(address: "alpine:3.22") {
		withExec(args: ["sh", "-c", "head -c 2097152 /dev/urandom > /other.bin && sha256sum /other.bin | cut -c1-16 > /other"]) {
			file(path: "/other") { contents } } } } }`)
	stored := func() []LogRecord {
		return s.main.Logs.Since(logMark, "export stored", func(r LogRecord) bool { return r.Attr("cacheID") == cacheA })
	}
	env.WaitFor(env.Settings.Timeout, "the second pipeline's export", func() (bool, error) { return len(stored()) > 0, nil })
	s.waitStored(s.main, s.a, "all of A's retained entries to be stored")
	uploaded, present, inBundle := 0, 0, false
	for _, r := range stored() {
		uploaded += atoi(r.Attr("uploaded"))
		present += atoi(r.Attr("present"))
		if slices.Contains(s.chainLayersAll(r.Attr("bundle")), alpine) {
			inBundle = true
		}
	}
	puts := env.Proxy.Blobs(s3Mark, http.MethodPut)

	Check(t, uploaded > 0 && len(puts) == uploaded, "it uploaded only its own layers: %d blobs, %d blob PUTs through the proxy", uploaded, len(puts))
	Check(t, present > 0 && inBundle, "alpine's layer is in its bundle, and was present (%d blobs present)", present)
	Check(t, !blobsOf(puts, alpine, nil), "alpine's layer (%s...) was not uploaded again", Short(alpine.String()))
}

// chainLayersAll is every fs layer a bundle names.
func (s *state) chainLayersAll(id string) []digest.Digest {
	var out []digest.Digest
	for _, o := range s.env.Bundle(id).Outputs {
		if o.Chain == nil {
			continue
		}
		for _, l := range o.Chain.Layers {
			out = append(out, l.Descriptor.Digest)
		}
	}
	return out
}

// startReplica starts the replica: a second service on the same bucket and
// scope, as another replica of the service would be, with short presigned
// GETs and a short import poll. It learns of the main service's exports by
// polling the index.
func (s *state) startReplica() {
	if s.replica == nil {
		s.replica = s.env.StartService("replica", ServiceOptions{GetExpiry: s.env.Settings.GetExpiry, ImportPoll: s.env.Settings.ImportPoll})
	}
}

// envelopes are the replica's envelope records for a connection with the
// given types, after mark.
func envelopes(svc *Service, mark int, conn uint64, types ...protocol.Type) []LogRecord {
	return svc.Logs.Since(mark, "envelope", func(r LogRecord) bool {
		return r.Attr("conn") == strconv.FormatUint(conn, 10) && slices.Contains(types, protocol.Type(r.Attr("type")))
	})
}

// renew (#31): a pull survives presigned GET expiry through renew; a missing
// layer degrades to a cache miss.
func (s *state) renew(t *testing.T) {
	env := s.env
	s.startReplica()
	engines := env.StartEngines(s.replica, "c", "d")
	c, d := engines[0], engines[1]
	s.c = c
	roots := slices.Sorted(maps.Keys(s.indexAfterA.Roots))
	s.waitMergedAll(s.replica, c, roots)
	s.waitMergedAll(s.replica, d, roots)
	mergedAt := time.Now()
	wait := env.Settings.GetExpiry + 2*time.Second
	t.Logf("waiting %s for the merged GETs to expire", wait)
	time.Sleep(wait)

	// C reads the layers, after the GETs expired.
	logMark, s3Mark := s.replica.Logs.Mark(), env.Proxy.Mark()
	res, timeC := c.Query("renew-verify-c", verifyQuery())
	outC := strings.TrimSpace(Field(res, verifyPath))
	t.Logf("C took %s, %s after the merge. Output:\n%s", timeC.Round(10*time.Millisecond), time.Since(mergedAt).Round(time.Second), outC)
	renews := envelopes(s.replica, logMark, c.Conn, protocol.TypeRenew)
	reneweds := envelopes(s.replica, logMark, c.Conn, protocol.TypeRenewed)
	gets := env.Proxy.Blobs(s3Mark, http.MethodGet)
	ok := func(r S3Request) bool { return r.Status == http.StatusOK || r.Status == http.StatusPartialContent }
	var missing []string
	for _, l := range s.layersA {
		if !blobsOf(gets, l, ok) {
			missing = append(missing, Short(l.String()))
		}
	}
	allAvailable := len(reneweds) > 0
	for _, r := range reneweds {
		if r.Attr("unavailable") != "false" {
			allAvailable = false
		}
	}
	refused := 0
	for _, g := range gets {
		if g.Status == http.StatusForbidden {
			refused++
		}
	}

	Check(t, len(renews) > 0, "C sent renew: %d renew(s)", len(renews))
	Check(t, allAvailable, "the replica answered with fresh GETs: %d renewed, all available", len(reneweds))
	Check(t, len(missing) == 0 && len(gets) > 0, "S3 served C every layer of A's chain: %d blob GETs, missing %v", len(gets), missing)
	// The engine renews an expired address without trying it: no blob GET
	// before the renew, and none refused. The service's log and the proxy
	// share this process's clock.
	beforeRenew := len(renews) == 0 || len(gets) == 0 || gets[0].Time.Before(renews[0].Time)
	Check(t, !beforeRenew && refused == 0, "no GET before the renew, and none refused (%d refused)", refused)
	Check(t, outC == s.stampA+"\n"+s.sumsA, "the files C read are A's: its stamps and checksums")

	// C's verify exec is exported next, and its upload would put back a
	// layer deleted before it: the service asks only for the blobs the store
	// lacks, and C has them all on disk now.
	s.waitStored(s.replica, c, "C's verify exec to be stored")
	top := s.layersA[len(s.layersA)-1]
	t.Logf("deleting the last exec's layer %s... from MinIO, and reading the layers on D", Short(top.String()))
	env.DeleteBlob(top)
	logMark = s.replica.Logs.Mark()
	res, timeD := d.Query("renew-verify-d", verifyQuery())
	outD := strings.TrimSpace(Field(res, verifyPath))
	t.Logf("D took %s. Output:\n%s", timeD.Round(10*time.Millisecond), outD)
	unavailable := false
	for _, r := range envelopes(s.replica, logMark, d.Conn, protocol.TypeRenewed) {
		if r.Attr("unavailable") == "true" {
			unavailable = true
		}
	}
	missingLog := len(s.replica.Logs.Since(logMark, "import: renew of a missing layer", func(r LogRecord) bool { return r.Attr("digest") == top.String() })) > 0
	d4, a4 := lines(outD), append(lines(s.stampA), lines(s.sumsA)...)

	Check(t, unavailable && missingLog, "renew of the chain with the deleted layer answered unavailable, naming it")
	Check(t, len(d4) == 4 && len(a4) == 4 && d4[0] == a4[0] && d4[2] == a4[2], "exec1's files on D are A's")
	Check(t, len(d4) == 4 && len(a4) == 4 && d4[1] != a4[1] && d4[3] != a4[3], "exec2 ran again on D, as for a cache miss: new stamp and checksum, and the query succeeded")
	t.Logf("timings: C %s (layers through renewed GETs), D %s (exec2 recomputed)", timeC.Round(10*time.Millisecond), timeD.Round(10*time.Millisecond))
}

// prune (#32): entries an engine prunes are not merged back into it until it
// restarts.
func (s *state) prune(t *testing.T) {
	env := s.env
	if s.c == nil {
		s.startReplica()
		s.c = env.StartEngines(s.replica, "c")[0]
		s.waitMergedAll(s.replica, s.c, slices.Sorted(maps.Keys(s.indexAfterA.Roots)))
	}
	c := s.c
	cacheC, genC := c.CacheID(), c.Generation()
	// Earlier scenarios' exports may still be on their way to C: wait until
	// every engine's entries are stored, and C's merges stop changing for
	// three polls, so that what is merged after the prune can only be the
	// pruned entries coming back.
	for _, info := range s.replica.Engines() {
		env.WaitFor(env.Settings.Timeout, fmt.Sprintf("the replica's exports of connection %d", info.Conn), func() (bool, error) {
			return retainedStored(s.replica, info.Conn, false)
		})
	}
	bundles := func() []string {
		var ids []string
		for _, m := range s.imports(s.replica, c).Merged {
			ids = append(ids, m.Bundle)
		}
		slices.Sort(ids)
		return ids
	}
	for settled, prev := 0, bundles(); settled < 3; {
		time.Sleep(env.Settings.ImportPoll)
		if cur := bundles(); slices.Equal(cur, prev) {
			settled++
		} else {
			settled, prev = 0, cur
		}
	}
	before := s.imports(s.replica, c)
	env.Save("prune-imports-before", before)
	// #47: C's verify exec (renew) was exported on the entries C had
	// imported from A. The restart below moves those imports.
	held := s.heldImports(env.Index(), cacheC)
	env.Save("prune-held-imports", held)
	beforeBundles := map[string]bool{}
	for _, m := range before.Merged {
		beforeBundles[m.Bundle] = true
	}

	t.Log("pruning C's cache ({ engine { localCache { prune } } })")
	entrySet := `{ engine { localCache { entrySet { entryCount diskSpaceBytes } } } }`
	diskBefore, _ := c.Query("prune-entries-before", entrySet)
	logMark := s.replica.Logs.Mark()
	c.Query("prune-run", `{ engine { localCache { prune } } }`)
	var pruneEvents []LogRecord
	env.WaitFor(time.Minute, "C's engine.prune event", func() (bool, error) {
		pruneEvents = s.replica.Logs.Since(logMark, "engine event", func(r LogRecord) bool {
			return r.Attr("kind") == "engine.prune" && strings.HasPrefix(r.Attr("cache"), cacheC+"/")
		})
		return len(pruneEvents) > 0, nil
	})
	env.WaitFor(30*time.Second, "the importer to forget C's pruned entries", func() (bool, error) {
		return len(s.imports(s.replica, c).Values) < len(before.Values), nil
	})
	pruned := s.imports(s.replica, c)
	env.Save("prune-imports-pruned", pruned)
	diskAfter, _ := c.Query("prune-entries-after", entrySet)

	const polls = 5
	wait := env.Settings.ImportPoll*polls + time.Second
	t.Logf("waiting %s (%d import polls), then syncing C 3 times by hand", wait, polls)
	time.Sleep(wait)
	forced := 0
	for range 3 {
		ctx, cancel := env.ctx()
		res, err := s.replica.Import(ctx, c.Conn)
		cancel()
		if err != nil {
			t.Fatalf("sync C: %v", err)
		}
		forced += len(res.Merged)
	}
	polled := s.imports(s.replica, c)
	sent := envelopes(s.replica, logMark, c.Conn, protocol.TypeMerge, protocol.TypeOffer)
	var remerged []string
	for _, r := range s.replica.Logs.Since(logMark, "import: merged", func(r LogRecord) bool { return r.Attr("cacheID") == cacheC }) {
		if beforeBundles[r.Attr("bundle")] {
			remerged = append(remerged, Short(r.Attr("bundle")))
		}
	}

	t.Log("restarting C")
	c.Restart()
	env.WaitFor(env.Settings.Timeout, "the replica to merge into C again", func() (bool, error) {
		st, err := s.replica.Imports(c.Conn)
		return err == nil && len(st.Merged) >= len(before.Merged), err
	})
	time.Sleep(time.Second)
	restarted := s.imports(s.replica, c)
	env.Save("prune-imports-restarted", restarted)
	// The restarted C had A's entries merged onto new engine entries: wait
	// until the index records them, then check C's older bundle (#47).
	idxRestarted := env.Index()
	env.WaitFor(env.Settings.Timeout, "the replica to record C's new imports", func() (bool, error) {
		idxRestarted = env.Index()
		moved := 0
		for _, h := range held {
			moved += s.movedImports(idxRestarted, cacheC, h)
		}
		return moved > 0, nil
	})
	moved := 0
	for _, h := range held {
		moved += s.movedImports(idxRestarted, cacheC, h)
	}
	env.Save("prune-index-restarted", idxRestarted)
	wouldHaveFailed := s.checkHeldMergeable(t, held, "after C's imports moved")
	s.cHeld = held

	disk := func(res map[string]any) string { return Field(res, "engine.localCache.entrySet.diskSpaceBytes") }
	resultIDs := 0
	for _, ev := range pruneEvents {
		resultIDs += strings.Count(ev.Attr("body"), "resultId")
	}
	Check(t, len(pruned.Values) < len(before.Values),
		"C pruned imported entries: its engine.prune event named %d entries; the importer holds %d of its %d imported values; disk %s -> %s bytes",
		resultIDs, len(pruned.Values), len(before.Values), disk(diskBefore), disk(diskAfter))
	Check(t, len(remerged) == 0 && len(sent) == 0 && forced == 0,
		"no merge or offer to C over %d polls and 3 syncs by hand (%d sent, %d merged by the syncs, merged again: %v)", polls, len(sent), forced, remerged)
	Check(t, len(pruned.Values) == len(polled.Values) && len(pruned.Merged) == len(polled.Merged), "the importer's state for C is as the prune left it")
	Check(t, restarted.Generation > genC && len(restarted.Values) >= len(before.Values),
		"after C restarted (generation %d, was %d), the replica merged %d bundles into it again, %d values", restarted.Generation, genC, len(restarted.Merged), len(restarted.Values))
	Check(t, len(held) > 0, "C has stored bundles holding entries it imported from A: %v", held)
	Check(t, moved > 0 && len(wouldHaveFailed) > 0,
		"#47 reproduced: %d of those imported values moved to new engine entries of C, and resolving C's bundles through its current mappings fails (%v)", moved, wouldHaveFailed)
	stale := staleMergeFailures(env)
	Check(t, len(stale) == 0, "no merge so far in the run failed with \"is no longer stored\" (seen: %v)", stale)
}

// supersededRecord is one stored record of the superseded scenario's value.
type supersededRecord struct {
	engineNum, serviceNum uint64
	bundle                string
	layer                 digest.Digest
	expiresAt             time.Time
	replacements          uint64
}

// superseded (#32, #27): which record of a replaced value a new engine ends
// up with. A Dang module function with a cache TTL runs on A, expires, and
// runs again, which replaces its value; the service stores both. A new
// engine F then imports, and reads the value.
func (s *state) superseded(t *testing.T) {
	env := s.env
	info, _ := s.main.EngineInfo(s.a.Conn)
	cacheA := info.CacheID
	mod := filepath.Join(env.RunDir, "ttlmod")
	if err := os.MkdirAll(mod, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(mod, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("dagger.json", fmt.Sprintf(`{"name": "ttlmod", "engineVersion": %q, "sdk": "dang"}`+"\n", info.EngineVersion))
	// Nothing depends on stamp2's result in A's cache, so when it has
	// expired the engine replaces its value in place (same entry, one more
	// replacement) rather than computing it into a new entry.
	write("main.dang", fmt.Sprintf(`type Ttlmod {
  pub stamp2(): Container! @cache(ttl: "%ds") {
    container.from("alpine:3.22").withExec(["sh", "-c", "cat /proc/sys/kernel/random/uuid > /stamp2"])
  }
}
`, int(env.Settings.TTL.Seconds())))

	// lastSpan waits for an engine's Ttlmod.stamp2 span after mark, and
	// returns the latest.
	lastSpan := func(mark int, cacheID string) LogRecord {
		var spans []LogRecord
		env.WaitFor(30*time.Second, "a Ttlmod.stamp2 span", func() (bool, error) {
			spans = s.main.Logs.CacheSpans(mark, cacheID, "Ttlmod.stamp2")
			return len(spans) > 0, nil
		})
		return spans[len(spans)-1]
	}
	// record waits for A's entry n to be stored, with at least
	// replacements, and reads its record from the index.
	record := func(n, replacements uint64, name string) supersededRecord {
		env.WaitFor(env.Settings.Timeout, fmt.Sprintf("A's entry %d to be stored", n), func() (bool, error) {
			entries, err := s.main.Entries(s.a.Conn)
			if err != nil {
				return false, err
			}
			for _, e := range entries {
				if e.Number == n && e.Stored && e.Replacements >= replacements {
					return true, nil
				}
			}
			return false, nil
		})
		idx := env.Index()
		env.Save(name+"-index", idx)
		m, ok := idx.Caches[cacheA].Lookup(n)
		if !ok {
			t.Fatalf("A's entry %d is not in the index", n)
		}
		e := idx.Entries[m.Number]
		r := supersededRecord{engineNum: n, serviceNum: m.Number, bundle: e.Bundle, replacements: e.Replacements}
		if e.ExpiresAtUnix > 0 {
			r.expiresAt = time.Unix(e.ExpiresAtUnix, 0)
		}
		for _, o := range env.Bundle(e.Bundle).Outputs {
			if o.Ordinal == e.Ordinal && o.Address.Part == "fs" && o.Chain != nil && len(o.Chain.Layers) > 0 {
				r.layer = o.Chain.Layers[len(o.Chain.Layers)-1].Descriptor.Digest
				break
			}
		}
		return r
	}
	resultNum := func(span LogRecord) uint64 { return uint64(atoi(span.Attr("result"))) }

	t.Logf("running a Dang function with a %s cache TTL on A", env.Settings.TTL)
	mark := s.main.Logs.Mark()
	s.a.CLI(nil, "superseded-run-1", "", "-m", mod, "call", "stamp-2", "sync")
	r1 := record(resultNum(lastSpan(mark, cacheA)), 0, "superseded-1")
	t.Logf("first value: engine entry %d, service number %d, bundle %s, layer %s, expires %s",
		r1.engineNum, r1.serviceNum, Short(r1.bundle), Short(r1.layer.String()), r1.expiresAt.UTC().Format(time.TimeOnly))

	if wait := time.Until(r1.expiresAt) + 2*time.Second; wait > 0 {
		t.Logf("waiting %s for it to expire, then running it again on A", wait.Round(time.Second))
		time.Sleep(wait)
	}
	mark = s.main.Logs.Mark()
	s.a.CLI(nil, "superseded-run-2", "", "-m", mod, "call", "stamp-2", "sync")
	r2 := record(resultNum(lastSpan(mark, cacheA)), 1, "superseded-2")
	t.Logf("second value: engine entry %d, service number %d, bundle %s, layer %s, replacements %d",
		r2.engineNum, r2.serviceNum, Short(r2.bundle), Short(r2.layer.String()), r2.replacements)

	t.Log("starting engine F, with an empty cache, and reading the value on it")
	f := env.StartEngines(s.main, "f")[0]
	cacheF := f.CacheID()
	// Both bundles, before F reads: the importer records each merge as it
	// goes, and merges the newer bundle first when its lowest root is lower,
	// so the newer one alone doesn't mean F has the older record's bundle
	// yet.
	env.WaitFor(env.Settings.Timeout, fmt.Sprintf("merges of both bundles into F (%s and %s)", Short(r1.bundle), Short(r2.bundle)), func() (bool, error) {
		st, err := s.main.Imports(f.Conn)
		if err != nil {
			return false, err
		}
		var got []string
		for _, m := range st.Merged {
			got = append(got, m.Bundle)
		}
		return slices.Contains(got, r1.bundle) && slices.Contains(got, r2.bundle), nil
	})
	importsF := s.imports(s.main, f)
	env.Save("superseded-imports-f", importsF)
	// #47: F is the first engine started since C's imports moved (prune),
	// and gets C's bundles holding them merged.
	if len(s.cHeld) > 0 {
		s.waitHeldMerged(t, s.main, f, s.cHeld, "a fresh engine after C's imports moved")
	} else {
		t.Logf("prune didn't run, or found no bundle of C's holding imports: not checking that F gets one (#47)")
	}
	// The second value has to reach F, and F has to read it, before its
	// TTL runs out; otherwise the checks below would fail for the wrong
	// reason.
	tooShort := func(what string) {
		t.Fatalf("the second value expired at %s, %s: the TTL (%s) is too short for this host; raise E2E_TTL",
			r2.expiresAt.UTC().Format(time.TimeOnly), what, env.Settings.TTL)
	}
	for _, m := range importsF.Merged {
		if m.Bundle == r2.bundle && slices.Contains(m.Expired, r2.serviceNum) {
			tooShort("before it was merged into F")
		}
	}
	if !r2.expiresAt.IsZero() && time.Until(r2.expiresAt) < time.Second {
		tooShort("before F read it")
	}
	t.Logf("F reads the value %s before it expires", time.Until(r2.expiresAt).Round(100*time.Millisecond))
	mark, s3Mark := s.main.Logs.Mark(), env.Proxy.Mark()
	_, timeF := f.CLI(nil, "superseded-read-f", "", "-m", mod, "call", "stamp-2", "file", "--path", "/stamp2", "contents")
	readF := lastSpan(mark, cacheF)
	gets := env.Proxy.Blobs(s3Mark, http.MethodGet)

	Check(t, r1.engineNum == r2.engineNum && r2.replacements > 0,
		"A replaced the value in place: engine entry %d, then %d, %d replacement(s)", r1.engineNum, r2.engineNum, r2.replacements)
	Check(t, r1.serviceNum == r2.serviceNum && r1.bundle != r2.bundle && r1.layer != r2.layer,
		"the index kept service number %d and moved it to the newer bundle (%s -> %s); the older bundle still holds the first record",
		r1.serviceNum, Short(r1.bundle), Short(r2.bundle))
	// A function with an explicit cache TTL is evaluated again on every
	// call (core/sdk/dang/v2/entrypoint.go), so the hit is its body's exec,
	// which resolves to the imported entry.
	importedF := importsF.Values[r2.serviceNum].Number
	var execF LogRecord
	for _, sp := range s.main.Logs.CacheSpans(mark, cacheF, "Container.withExec") {
		if resultNum(sp) == importedF {
			execF = sp
		}
	}
	Check(t, importedF != 0 && execF.Attr("outcome") == "hit" && resultNum(readF) == importedF,
		"F hit the imported value: stamp2's exec hit F's entry %d (service number %d), and stamp2 returned it (%s as a TTL function)",
		importedF, r2.serviceNum, readF.Attr("outcome"))
	got1, got2 := blobsOf(gets, r1.layer, nil), blobsOf(gets, r2.layer, nil)
	switch {
	case got2 && !got1:
		Check(t, true, "F has the newer record: it read the second value's layer (%s...) from S3, not the first's (in %s)", Short(r2.layer.String()), timeF.Round(10*time.Millisecond))
	case got1:
		// The outcome #27 is about; expected until it is fixed.
		t.Logf("GAP   F has the older record: it read the first value's layer (%s...), from a superseded record (known gap #27)", Short(r1.layer.String()))
	default:
		Check(t, false, "F read neither value's layer from S3")
	}
}

func sortedCopy[T cmp.Ordered](s []T) []T {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

func shorts(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = Short(id)
	}
	return out
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
