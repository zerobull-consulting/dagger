package e2e

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// partsRecord is the index's record of the parts scenario's value V.
type partsRecord struct {
	service uint64
	bundle  string
	parts   []string
	root    bool
}

// parts (zerobull-consulting/dagger#133): a stored value recorded again,
// as a dependency in a later bundle, keeps the parts its first record
// holds, and engines that import it before and after that read it as a
// cache hit. On A, session one runs exec V, which writes a nonce to its
// stdout (the execMeta part) and to /v (fs); engine F1, empty, imports it.
// Session two runs exec W over V: V is in W's closure, not a root, and the
// engine exports only the roots' outputs, so W's bundle holds V's record
// with fewer parts. Engine F2, empty, imports after that. Both read V.
func (s *state) parts(t *testing.T) {
	env := s.env
	info, _ := s.main.EngineInfo(s.a.Conn)
	cacheA := info.CacheID
	n := Nonce()
	v := `container { from(address: "alpine:3.22") { withExec(args: ["sh", "-c", "echo v-` + n + ` $(cat /proc/sys/kernel/random/uuid) | tee /v"]) { `
	readV := `{ ` + v + `stdout file(path: "/v") { contents } } } } }`
	const stdoutPath, filePath = "container.from.withExec.stdout", "container.from.withExec.file.contents"
	readW := `{ ` + v + `withExec(args: ["sh", "-c", "cat /v > /w; echo w-$(cat /proc/sys/kernel/random/uuid) >> /w"]) { file(path: "/w") { contents } } } } } }`

	// executed returns the engine number of the one withExec span after
	// mark that A executed.
	executed := func(mark int, what string) uint64 {
		var nums []uint64
		for _, sp := range s.main.Logs.CacheSpans(mark, cacheA, "Container.withExec") {
			if sp.Attr("outcome") == "executed" {
				nums = append(nums, uint64(atoi(sp.Attr("result"))))
			}
		}
		if len(nums) != 1 {
			t.Fatalf("%s: A executed %d withExec calls (%v), want 1", what, len(nums), nums)
		}
		return nums[0]
	}
	recordOf := func(idx *metastore.Index, engineNum uint64) partsRecord {
		m, ok := idx.Caches[cacheA].Lookup(engineNum)
		if !ok {
			t.Fatalf("A's entry %d is not in the index", engineNum)
		}
		e := idx.Entries[m.Number]
		_, root := idx.Roots[m.Number]
		return partsRecord{service: m.Number, bundle: e.Bundle, parts: e.Parts, root: root}
	}
	// read runs readV on en and returns its stdout and file, and its
	// withExec spans' outcomes.
	read := func(en *Engine, out string) (stdout, file string, hit, ran int) {
		cacheID := en.CacheID()
		mark := s.main.Logs.Mark()
		res, took := en.Query(out, readV)
		stdout, file = strings.TrimSpace(Field(res, stdoutPath)), strings.TrimSpace(Field(res, filePath))
		// V's exec, and alpine's from: at least V's span.
		env.WaitFor(30*time.Second, en.Name+"'s withExec span", func() (bool, error) {
			hit, ran = countOutcomes(s.main.Logs.CacheSpans(mark, cacheID, "Container.withExec"))
			return hit+ran >= 1, nil
		})
		t.Logf("%s read V in %s: stdout %q, /v %q; withExec %d hit, %d executed", en.Name, took.Round(10*time.Millisecond), stdout, file, hit, ran)
		return stdout, file, hit, ran
	}

	t.Log("session one on A: exec V, which writes a nonce to its stdout and to /v")
	mark := s.main.Logs.Mark()
	res, _ := s.a.Query("parts-v-a", readV)
	stdoutA, fileA := strings.TrimSpace(Field(res, stdoutPath)), strings.TrimSpace(Field(res, filePath))
	engineV := executed(mark, "session one")
	s.waitStored(s.main, s.a, "A's entries after session one")
	idx1 := env.Index()
	env.Save("parts-index-1", idx1)
	r1 := recordOf(idx1, engineV)
	t.Logf("V: engine entry %d, service number %d, root %v, bundle %s, parts %v", engineV, r1.service, r1.root, Short(r1.bundle), r1.parts)

	t.Log("engine F1, empty, imports V")
	f1 := env.StartEngines(s.main, "f1")[0]
	s.waitMergedAll(s.main, f1, []uint64{r1.service})

	t.Log("session two on A: exec W over V, so W's bundle holds V's record, not as a root")
	mark = s.main.Logs.Mark()
	s.a.Query("parts-w-a", readW)
	engineW := executed(mark, "session two")
	s.waitStored(s.main, s.a, "A's entries after session two")
	idx2 := env.Index()
	env.Save("parts-index-2", idx2)
	r2, rw := recordOf(idx2, engineV), recordOf(idx2, engineW)
	t.Logf("V after: service number %d, bundle %s, parts %v; W: service number %d, bundle %s, parts %v",
		r2.service, Short(r2.bundle), r2.parts, rw.service, Short(rw.bundle), rw.parts)
	var inW []string
	for _, o := range env.Bundle(rw.bundle).Outputs {
		for _, val := range env.Bundle(rw.bundle).Values {
			if val.Ordinal == o.Ordinal && val.SenderNumber == engineV {
				inW = append(inW, metastore.PartKey(o.Address))
			}
		}
	}
	t.Logf("W's bundle holds V's record with parts %v", inW)

	// A partial import: X, alpine with a variable set, isn't retained, so
	// the store holds its record only in the closure of Y, an exec over it,
	// without X's parts. An engine that imports Y and reads X's variable
	// (its metadata part) has to compute it itself.
	t.Log("session three on A: exec Y over X, alpine with a variable set, which isn't retained")
	x := `container { from(address: "alpine:3.22") { withEnvVariable(name: "PARTS_X", value: "x-` + n + `") { `
	readX := `{ ` + x + `envVariable(name: "PARTS_X") } } } }`
	readY := `{ ` + x + `withExec(args: ["sh", "-c", "echo $PARTS_X"]) { stdout } } } } }`
	const xPath, yPath = "container.from.withEnvVariable.envVariable", "container.from.withEnvVariable.withExec.stdout"
	mark = s.main.Logs.Mark()
	res, _ = s.a.Query("parts-y-a", readY)
	stdoutY := strings.TrimSpace(Field(res, yPath))
	engineY := executed(mark, "session three")
	var engineX uint64
	for _, sp := range s.main.Logs.CacheSpans(mark, cacheA, "Container.withEnvVariable") {
		engineX = uint64(atoi(sp.Attr("result")))
	}
	s.waitStored(s.main, s.a, "A's entries after session three")
	idx3 := env.Index()
	env.Save("parts-index-3", idx3)
	ry, rx := recordOf(idx3, engineY), recordOf(idx3, engineX)
	t.Logf("Y: service number %d, bundle %s, parts %v; X: service number %d, root %v, bundle %s, parts %v",
		ry.service, Short(ry.bundle), ry.parts, rx.service, rx.root, Short(rx.bundle), rx.parts)

	t.Log("engine F2, empty, imports after sessions two and three")
	f2 := env.StartEngines(s.main, "f2")[0]
	s.waitMergedAll(s.main, f2, []uint64{r2.service, rw.service, ry.service})
	importsF2 := s.imports(s.main, f2)
	env.Save("parts-imports-f2", importsF2)

	stdout1, file1, hit1, ran1 := read(f1, "parts-v-f1")
	stdout2, file2, hit2, ran2 := read(f2, "parts-v-f2")

	// X's file, then Y, on F2.
	cacheF2 := f2.CacheID()
	mark = s.main.Logs.Mark()
	res, _ = f2.Query("parts-x-f2", readX)
	fileX := strings.TrimSpace(Field(res, xPath))
	res, _ = f2.Query("parts-y-f2", readY)
	stdoutYF2 := strings.TrimSpace(Field(res, yPath))
	var hitY, ranY int
	env.WaitFor(30*time.Second, "F2's withExec span for Y", func() (bool, error) {
		hitY, ranY = countOutcomes(s.main.Logs.CacheSpans(mark, cacheF2, "Container.withExec"))
		return hitY+ranY >= 1, nil
	})
	var xOutcomes []string
	for _, sp := range s.main.Logs.CacheSpans(mark, cacheF2, "Container.withEnvVariable") {
		xOutcomes = append(xOutcomes, sp.Attr("outcome"))
	}
	t.Logf("F2 read X's variable %q (withEnvVariable %v) and Y's stdout %q (withExec %d hit, %d executed)", fileX, xOutcomes, stdoutYF2, hitY, ranY)

	Check(t, strings.HasPrefix(stdoutA, "v-"+n) && stdoutA == fileA && r1.root && len(r1.parts) > 0,
		"A stored V as a root, with parts %v", r1.parts)
	Check(t, len(inW) < len(r1.parts),
		"W's bundle holds V's record with fewer parts (%v): the engine exports only its roots' outputs", inW)
	Check(t, r2.service == r1.service && r2.bundle == r1.bundle && slices.Equal(r2.parts, r1.parts),
		"the index kept V's first record: bundle %s, parts %v (after session two: bundle %s, parts %v)", Short(r1.bundle), r1.parts, Short(r2.bundle), r2.parts)
	Check(t, stdout1 == stdoutA && file1 == fileA && ran1 == 0 && hit1 > 0,
		"F1, which imported before session two, read A's V as a cache hit (%d hit, %d executed)", hit1, ran1)
	Check(t, stdout2 == stdoutA && file2 == fileA && ran2 == 0 && hit2 > 0,
		"F2, which imported after session two, read A's V as a cache hit (%d hit, %d executed)", hit2, ran2)
	Check(t, engineX != 0 && !rx.root && len(rx.parts) == 0 && rx.bundle == ry.bundle && len(ry.parts) > 0,
		"the store holds X only in Y's bundle, without parts (root %v, parts %v), and Y with parts %v", rx.root, rx.parts, ry.parts)
	Check(t, fileX == "x-"+n,
		"a partial import is safe: F2, which imported X without its parts, read X's variable without failing (%q, as A set it)", fileX)
	Check(t, stdoutYF2 == stdoutY && stdoutY == "x-"+n && ranY == 0 && hitY > 0,
		"F2 read A's Y as a cache hit (%d hit, %d executed), its stdout %q", hitY, ranY, stdoutYF2)
	if t.Failed() {
		t.Log(fmt.Sprintf("A's V: stdout %q, /v %q", stdoutA, fileA))
	}
}
