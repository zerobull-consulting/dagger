package main

import (
	"io"
	"net/http"
	"time"

	"github.com/opencontainers/go-digest"
)

// serveBlobPut stands in for a presigned PUT: it reads the blob, checks it
// against the digest in its path, logs it, and stores nothing. Give the engine
// http://<host>:<port>/blobs/<digest> as an upload address.
func (s *service) serveBlobPut(w http.ResponseWriter, r *http.Request) {
	want, err := digest.Parse(r.PathValue("digest"))
	if err != nil {
		http.Error(w, "bad digest", http.StatusBadRequest)
		return
	}
	start := time.Now()
	verifier := want.Verifier()
	n, err := io.Copy(verifier, r.Body)
	if err != nil {
		s.logger.Warn("blob upload failed", "digest", want, "bytes", n, "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ok := verifier.Verified()
	s.logger.Info("blob uploaded", "digest", want, "bytes", n, "contentLength", r.ContentLength,
		"contentType", r.Header.Get("Content-Type"), "verified", ok, "took", time.Since(start).Round(time.Millisecond))
	if !ok {
		http.Error(w, "digest mismatch", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}
