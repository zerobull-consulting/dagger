package e2e

import (
	"context"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// namespaces (#10): a service with a tokens file, and engines on two tokens
// of different namespaces. Engine NA, on team A's token, builds; engine NB,
// on team B's, gets nothing of it, neither merges nor hits; engine NC, on
// NA's token, gets NA's entries and hits them. Removing team B's token and
// reloading the file closes NB's connection with 4001, and its
// reconnections are refused.
//
// It runs its own service and engines, on a pipeline of its own (one exec,
// with a nonce), so it shares nothing with the other scenarios.
func (s *state) namespaces(t *testing.T) {
	env := s.env
	tokA, tokB := "e2e-team-a-"+Nonce(), "e2e-team-b-"+Nonce()
	const nsA, nsB = "team-a", "team-b"
	file := filepath.Join(env.RunDir, "tokens")
	writeTokens := func(lines ...string) {
		t.Helper()
		if err := os.WriteFile(file, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeTokens("# e2e namespaces", tokA+" "+nsA, tokB+" "+nsB)
	svc := env.StartService("namespaces", ServiceOptions{TokensFile: file})
	engines := env.StartEnginesWithTokens(svc, map[string]string{"na": tokA, "nb": tokB, "nc": tokA}, "na", "nb")
	na, nb := engines[0], engines[1]
	for _, en := range engines {
		info, _ := svc.EngineInfo(en.Conn)
		Check(t, info.Namespace == map[string]string{"na": nsA, "nb": nsB}[en.Name], "%s connected in namespace %q", en.Name, info.Namespace)
	}

	nonce := Nonce()
	query := `{ container { from(address: "alpine:3.22") {
      withExec(args: ["sh", "-c", "echo ns-` + nonce + ` >/dev/null; head -c 1048576 /dev/urandom > /ns.bin && echo \"ns $(cat /proc/sys/kernel/random/uuid) $(sha256sum /ns.bin | cut -c1-16)\" > /stamp"]) {
        file(path: "/stamp") { contents }
} } } }`
	const path = "container.from.withExec.file.contents"

	res, took := na.Query("ns-run-a", query)
	stampA := strings.TrimSpace(Field(res, path))
	t.Logf("NA took %s. Stamp: %s", took.Round(10*time.Millisecond), stampA)
	s.waitStored(svc, na, "NA's retained entries to be stored in team A's namespace")
	idxA := env.IndexOf(nsA)
	env.Save("namespaces-index-a", idxA)
	Check(t, len(idxA.Roots) > 0 && idxA.Caches[na.CacheID()] != nil, "team A's index maps NA's entries: %d roots", len(idxA.Roots))

	// NB syncs now, after NA's export is stored: nothing to merge.
	imp, err := svc.Import(context.Background(), nb.Conn)
	if err != nil {
		t.Fatal(err)
	}
	stB := s.imports(svc, nb)
	Check(t, len(imp.Merged) == 0 && len(stB.Merged) == 0, "nothing of team A merged into NB: %d merged now, %d in all", len(imp.Merged), len(stB.Merged))

	logMark := svc.Logs.Mark()
	res, took = nb.Query("ns-run-b", query)
	stampB := strings.TrimSpace(Field(res, path))
	t.Logf("NB took %s. Stamp: %s", took.Round(10*time.Millisecond), stampB)
	var hitB, executedB int
	env.WaitFor(30*time.Second, "NB's withExec cache span", func() (bool, error) {
		hitB, executedB = countOutcomes(svc.Logs.CacheSpans(logMark, nb.CacheID(), "Container.withExec"))
		return hitB+executedB >= 1, nil
	})
	Check(t, stampB != "" && stampB != stampA && hitB == 0 && executedB >= 1,
		"NB ran the exec itself: no hit (%d hit, %d executed), its own stamp", hitB, executedB)
	s.waitStored(svc, nb, "NB's retained entries to be stored in team B's namespace")
	idxB := env.IndexOf(nsB)
	idxA = env.IndexOf(nsA)
	Check(t, idxB.Caches[na.CacheID()] == nil && idxA.Caches[nb.CacheID()] == nil,
		"neither index maps the other team's engine (team A's: %v, team B's: %v)", slices.Sorted(maps.Keys(idxA.Caches)), slices.Sorted(maps.Keys(idxB.Caches)))

	// Each team's blobs are under its own prefix: NA's exec layer is in
	// team A's, not team B's, and alpine's layer, which both pulled, is
	// stored once in each.
	layersA := env.ChainLayersIn(nsA, bundlesOf(idxA))
	layersB := env.ChainLayersIn(nsB, bundlesOf(idxB))
	if len(layersA) == 0 || len(layersB) == 0 {
		t.Fatalf("chains: team A's %d layers, team B's %d", len(layersA), len(layersB))
	}
	execA := layersA[len(layersA)-1]
	Check(t, env.HasKey(env.BlobKeyIn(nsA, execA)) && !env.HasKey(env.BlobKeyIn(nsB, execA)) && !env.HasKey(env.BlobKeyIn(env.Scope, execA)),
		"NA's exec layer %s is stored in team A's namespace only", Short(execA.String()))
	Check(t, layersA[0] == layersB[0] && env.HasKey(env.BlobKeyIn(nsA, layersA[0])) && env.HasKey(env.BlobKeyIn(nsB, layersB[0])),
		"alpine's layer %s is stored in both namespaces", Short(layersA[0].String()))

	// NC, on NA's token, gets NA's entries, and hits them.
	nc := env.StartEnginesWithTokens(svc, map[string]string{"nc": tokA}, "nc")[0]
	s.waitMergedAll(svc, nc, slices.Sorted(maps.Keys(idxA.Roots)))
	for _, m := range s.imports(svc, nc).Merged {
		Check(t, m.Source == na.CacheID(), "NC merged a bundle of NA's (source %s)", m.Source)
	}
	logMark = svc.Logs.Mark()
	res, took = nc.Query("ns-run-c", query)
	stampC := strings.TrimSpace(Field(res, path))
	t.Logf("NC took %s. Stamp: %s", took.Round(10*time.Millisecond), stampC)
	var hitC, executedC int
	env.WaitFor(30*time.Second, "NC's withExec cache span", func() (bool, error) {
		hitC, executedC = countOutcomes(svc.Logs.CacheSpans(logMark, nc.CacheID(), "Container.withExec"))
		return hitC+executedC >= 1, nil
	})
	Check(t, stampC == stampA && hitC >= 1 && executedC == 0, "NC hit NA's exec (%d hit, %d executed) and read NA's stamp", hitC, executedC)

	// Team B's token goes: NB is closed with 4001, and refused after.
	connB := nb.Conn
	logMark = svc.Logs.Mark()
	writeTokens("# e2e namespaces, team B revoked", tokA+" "+nsA)
	if err := svc.ReloadTokens(); err != nil {
		t.Fatal(err)
	}
	env.WaitFor(30*time.Second, "NB's connection to close", func() (bool, error) {
		return len(svc.Logs.Since(logMark, "engine disconnected", func(r LogRecord) bool {
			return r.Attr("conn") == strconv.FormatUint(connB, 10) && r.Attr("revoked") == "true"
		})) > 0, nil
	})
	env.WaitFor(30*time.Second, "NB to reconnect, and be refused", func() (bool, error) {
		return len(svc.Logs.Since(logMark, "request refused: unknown token", func(r LogRecord) bool { return r.Attr("endpoint") == "engine" })) > 0, nil
	})
	closedLine := ""
	for _, line := range strings.Split(nb.Logs(), "\n") {
		if strings.Contains(line, "remote cache connection ended") && strings.Contains(line, strconv.Itoa(protocol.CloseTokenRevoked)) {
			closedLine = line
		}
	}
	Check(t, closedLine != "", "NB's engine saw its connection closed with %d: %s", protocol.CloseTokenRevoked, strings.TrimSpace(closedLine))
	infos := svc.Engines()
	listed := map[uint64]string{}
	for _, info := range infos {
		listed[info.Conn] = info.EngineName
	}
	_, stillB := listed[connB]
	Check(t, !stillB && len(infos) == 2, "the service lists NA and NC only: %v", listed)
	var errs []string
	for _, r := range svc.Logs.Since(0, "", func(r LogRecord) bool { return r.Level >= slog.LevelError }) {
		errs = append(errs, r.Msg)
	}
	Check(t, len(errs) == 0, "the service logged nothing at error level: %q", errs)
}

// ChainLayersIn returns the longest fs chain of a namespace's bundles.
func (e *Env) ChainLayersIn(ns string, ids []string) []digest.Digest {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var longest []digest.Digest
	for _, id := range ids {
		b, err := e.Meta.GetBundle(ctx, ns, id)
		if err != nil {
			e.T.Fatalf("bundle %s of %s: %v", id, ns, err)
		}
		for _, o := range b.Outputs {
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
