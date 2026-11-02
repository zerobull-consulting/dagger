package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

// MemStore is a Store in memory, for tests. It is also the http.Handler that
// serves its presigned addresses: create it, mount it on a server, then give
// it that server's URL with SetBaseURL. Like a presigned S3 PUT, a PUT must carry exactly the
// size it was signed for; unlike S3, it also checks the body against the
// digest.
//
// Namespace returns a view of one namespace's blobs: it shares the store's
// memory and server, and keeps its blobs apart from every other namespace's,
// as the S3 store's keys do.
type MemStore struct {
	// ns is the view's namespace; "" for the store itself.
	ns string
	*memState
}

type memState struct {
	mu      sync.Mutex
	baseURL string
	blobs   map[memKey][]byte
	puts    int
	// failPuts are PUTs still to fail, by digest, in any namespace.
	failPuts map[digest.Digest]int
}

type memKey struct {
	ns   string
	dgst digest.Digest
}

var (
	_ Store      = (*MemStore)(nil)
	_ Namespacer = (*MemStore)(nil)
)

// NewMemStore returns an empty MemStore whose addresses start with baseURL.
func NewMemStore(baseURL string) *MemStore {
	return &MemStore{memState: &memState{
		baseURL:  strings.TrimSuffix(baseURL, "/"),
		blobs:    map[memKey][]byte{},
		failPuts: map[digest.Digest]int{},
	}}
}

// Namespace returns the view of one namespace's blobs.
func (m *MemStore) Namespace(ns string) (Store, error) {
	return m.In(ns)
}

// In is Namespace, typed: the view of one namespace's blobs.
func (m *MemStore) In(ns string) (*MemStore, error) {
	if err := ValidateNamespace(ns); err != nil {
		return nil, err
	}
	return &MemStore{ns: ns, memState: m.memState}, nil
}

// SetBaseURL changes where the store's addresses point, for a store created
// before its server.
func (m *MemStore) SetBaseURL(baseURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.baseURL = strings.TrimSuffix(baseURL, "/")
}

func (m *MemStore) key(dgst digest.Digest) memKey { return memKey{ns: m.ns, dgst: dgst} }

// path is the address path of a blob of the view: /mem/<digest>, or
// /mem/<ns>/<digest> in a namespace.
func (m *MemStore) path(dgst digest.Digest) string {
	if m.ns == "" {
		return "/mem/" + dgst.String()
	}
	return "/mem/" + m.ns + "/" + dgst.String()
}

// Has reports whether the blob is stored.
func (m *MemStore) Has(_ context.Context, dgst digest.Digest) (bool, error) {
	if err := dgst.Validate(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.blobs[m.key(dgst)]
	return ok, nil
}

// PresignPut returns an address that takes a PUT of exactly size bytes.
func (m *MemStore) PresignPut(_ context.Context, dgst digest.Digest, size int64) (Address, error) {
	if err := dgst.Validate(); err != nil {
		return Address{}, err
	}
	if size <= 0 {
		return Address{}, ErrNotPositiveSize
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return Address{
		URL:       fmt.Sprintf("%s%s?size=%d", m.baseURL, m.path(dgst), size),
		ExpiresAt: time.Now().Add(DefaultPutExpiry),
	}, nil
}

// PresignGet returns an address that serves the blob.
func (m *MemStore) PresignGet(_ context.Context, dgst digest.Digest) (Address, error) {
	if err := dgst.Validate(); err != nil {
		return Address{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return Address{
		URL:       m.baseURL + m.path(dgst),
		ExpiresAt: time.Now().Add(DefaultGetExpiry),
	}, nil
}

// Put stores a blob directly, as if an engine had uploaded it.
func (m *MemStore) Put(data []byte) digest.Digest {
	dgst := digest.FromBytes(data)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[m.key(dgst)] = bytes.Clone(data)
	return dgst
}

// Get returns a stored blob.
func (m *MemStore) Get(dgst digest.Digest) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.blobs[m.key(dgst)]
	return bytes.Clone(data), ok
}

// Delete drops a blob, as an expiry or a cleanup would.
func (m *MemStore) Delete(dgst digest.Digest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, m.key(dgst))
}

// Len returns how many blobs the view stores.
func (m *MemStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k := range m.blobs {
		if k.ns == m.ns {
			n++
		}
	}
	return n
}

// Puts returns how many PUTs the store has accepted, in every namespace.
func (m *MemStore) Puts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts
}

// FailPuts makes the next n PUTs of dgst fail with 503.
func (m *MemStore) FailPuts(dgst digest.Digest, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failPuts[dgst] = n
}

// ServeHTTP serves the addresses of the store and of every namespace view.
func (m *MemStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name, ok := strings.CutPrefix(r.URL.Path, "/mem/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	view := &MemStore{memState: m.memState}
	if ns, rest, found := strings.Cut(name, "/"); found {
		if ValidateNamespace(ns) != nil {
			http.Error(w, "bad namespace", http.StatusBadRequest)
			return
		}
		view.ns, name = ns, rest
	}
	dgst, err := digest.Parse(name)
	if err != nil {
		http.Error(w, "bad digest", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		data, ok := view.Get(dgst)
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	case http.MethodPut:
		size, err := strconv.ParseInt(r.URL.Query().Get("size"), 10, 64)
		if err != nil || r.ContentLength != size {
			http.Error(w, "content length doesn't match the signed size", http.StatusForbidden)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if digest.FromBytes(data) != dgst {
			http.Error(w, "digest mismatch", http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		if m.failPuts[dgst] > 0 {
			m.failPuts[dgst]--
			m.mu.Unlock()
			http.Error(w, "injected failure", http.StatusServiceUnavailable)
			return
		}
		m.blobs[view.key(dgst)] = data
		m.puts++
		m.mu.Unlock()
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
