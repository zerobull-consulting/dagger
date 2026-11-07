package e2e

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/exporter"
)

// streaming (zerobull-consulting/dagger#134): an entry is stored while its
// session is still running, once it settles, rather than when the session
// ends. A's query runs an exec, then a long sleep on top of it: the first
// exec's entry must be stored during the sleep, within about Settle plus one
// scheduler tick of the last span naming it.
func (s *state) streaming(t *testing.T) {
	env := s.env
	cacheA := s.a.CacheID()
	const sleep = 40 * time.Second
	n := Nonce()
	query := `{ container { from(address: "alpine:3.22") { withExec(args: ["sh", "-c", "echo streaming-` + n + ` > /s"]) { ` +
		`withExec(args: ["sleep", "` + strconv.Itoa(int(sleep.Seconds())) + `"]) { file(path: "/s") { contents } } } } } }`

	mark := s.main.Logs.Mark()
	type result struct {
		res  map[string]any
		took time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		// Query fails the test itself if the CLI fails; the wait below is
		// bounded either way.
		res, took := s.a.Query("streaming-a", query)
		done <- result{res, took}
	}()

	// The first exec's call span: its engine number. It names the "echo"
	// exec, executed in this session (the nonce makes it new).
	var number uint64
	env.WaitFor(sleep/2, "A's span of the first withExec", func() (bool, error) {
		for _, sp := range s.main.Logs.CacheSpans(mark, cacheA, "Container.withExec") {
			if sp.Attr("outcome") == "executed" {
				number = uint64(atoi(sp.Attr("result")))
				return true, nil
			}
		}
		return false, nil
	})
	var storedAt time.Time
	env.WaitFor(sleep/2, fmt.Sprintf("entry %d to be stored while the session runs", number), func() (bool, error) {
		entries, err := s.main.Entries(s.a.Conn)
		if err != nil {
			return false, err
		}
		for _, e := range entries {
			if e.Number == number && e.Stored {
				storedAt = time.Now()
				return true, nil
			}
		}
		return false, nil
	})
	select {
	case r := <-done:
		t.Fatalf("the query ended (after %s) before entry %d was stored: the test can't tell streaming did it", r.took, number)
	default:
	}
	// The last span naming the entry before it was stored.
	var last time.Time
	for _, sp := range s.main.Logs.Since(mark, "cache span", nil) {
		if sp.Attr("result") == strconv.FormatUint(number, 10) && sp.Time.Before(storedAt) && sp.Time.After(last) {
			last = sp.Time
		}
	}
	lag := storedAt.Sub(last)
	t.Logf("ok    entry %d stored %s after the query started, %s after the last span naming it (Settle %s, tick 1s), while the session still runs",
		number, storedAt.Sub(start).Round(10*time.Millisecond), lag.Round(10*time.Millisecond), exporter.DefaultSettle)
	// Settle, one tick, and the export itself (compressing and uploading
	// the layer), with room for a loaded runner.
	if bound := exporter.DefaultSettle + time.Second + 5*time.Second; lag > bound {
		t.Errorf("entry %d stored %s after its last span, want within %s", number, lag, bound)
	}

	select {
	case r := <-done:
		t.Logf("ok    the session ended %s after it started", r.took.Round(10*time.Millisecond))
	case <-time.After(2 * sleep):
		t.Fatalf("the query didn't end within %s", 2*sleep)
	}
	s.waitStored(s.main, s.a, "A's retained entries to be stored after the session")
}
