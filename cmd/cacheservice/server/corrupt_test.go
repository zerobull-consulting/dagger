package server

import (
	"bytes"
	"io"
	"net/http"
	"path"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
)

// An engine now hashes a blob as it reads it for upload and fails the PUT
// when the bytes aren't its digest's, so a fake engine can't make its engine
// code send a lying body by corrupting what it hands over. These tests are
// about the service refusing such a body, so the corruption happens on the
// wire instead: the blob server flips the first byte of the PUT bodies of
// chosen digests, as someone holding the presigned URL could.
var putCorruption = struct {
	sync.Mutex
	left map[digest.Digest]int
}{left: map[digest.Digest]int{}}

// corruptPuts makes the next n PUTs of blob d reach the blob store with their
// first byte flipped: every one from now on if n < 0, none if n is 0. The
// bodies keep their size. It lasts until the test ends. Server tests don't
// run in parallel, so the setting is process-wide.
func corruptPuts(t *testing.T, d digest.Digest, n int) {
	t.Helper()
	putCorruption.Lock()
	putCorruption.left[d] = n
	putCorruption.Unlock()
	t.Cleanup(func() {
		putCorruption.Lock()
		delete(putCorruption.left, d)
		putCorruption.Unlock()
	})
}

// takeCorruption reports whether this PUT of d is to be corrupted, and counts it.
func takeCorruption(d digest.Digest) bool {
	putCorruption.Lock()
	defer putCorruption.Unlock()
	n := putCorruption.left[d]
	if n == 0 {
		return false
	}
	if n > 0 {
		putCorruption.left[d] = n - 1
	}
	return true
}

// corruptingPuts wraps a blob store's HTTP handler with corruptPuts. A
// presigned address ends in the blob's digest.
func corruptingPuts(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			if d, err := digest.Parse(path.Base(r.URL.Path)); err == nil && takeCorruption(d) {
				body, err := io.ReadAll(r.Body)
				_ = r.Body.Close()
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if len(body) > 0 {
					body = bytes.Clone(body)
					body[0] ^= 0xff
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
		}
		next.ServeHTTP(w, r)
	})
}
