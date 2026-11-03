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
// it that server's URL with SetBaseURL.
//
// It behaves as the S3 store does: a PUT goes to its stage's staging key, and
// must carry exactly the size it was signed for, but its content isn't
// checked; Promote checks it against the digest, and only then stores it
// under its digest, without overwriting a blob already there. Has and GETs
// see only promoted blobs. Its promotions are hashed by the service.
//
// Namespace returns a view of one namespace's blobs: it shares the store's
// memory and server, and keeps its blobs, staged and promoted, apart from
// every other namespace's, as the S3 store's keys do.
type MemStore struct {
	// ns is the view's namespace; "" for the store itself.
	ns string
	*memState
}

type memState struct {
	mu      sync.Mutex
	baseURL string
	blobs   map[memKey][]byte
	staged  map[stagedKey][]byte
	puts    int
	// failPuts are PUTs still to fail, by digest, in any namespace.
	failPuts map[digest.Digest]int
	// beforePromote, when set, runs before each promotion, outside the
	// lock; an error fails the promotion.
	beforePromote func(ctx context.Context, ns string, dgst digest.Digest) error
}

type memKey struct {
	ns   string
	dgst digest.Digest
}

type stagedKey struct {
	ns    string
	stage Stage
	dgst  digest.Digest
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
		staged:   map[stagedKey][]byte{},
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

// BeforePromote sets a function that runs before each promotion, in every
// namespace; an error it returns fails the promotion. Nil removes it.
func (m *MemStore) BeforePromote(f func(ctx context.Context, ns string, dgst digest.Digest) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beforePromote = f
}

func (m *MemStore) key(dgst digest.Digest) memKey { return memKey{ns: m.ns, dgst: dgst} }

// nsPath is the address path of the view: /mem, or /mem/<ns>.
func (m *MemStore) nsPath() string {
	if m.ns == "" {
		return "/mem"
	}
	return "/mem/" + m.ns
}

// Has reports whether the blob is stored: promoted.
func (m *MemStore) Has(_ context.Context, dgst digest.Digest) (bool, error) {
	if err := dgst.Validate(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.blobs[m.key(dgst)]
	return ok, nil
}

// PresignPut returns an address that takes a PUT of exactly size bytes,
// staged under stage: /mem[/<ns>]/staging/<stage>/<digest>?size=<size>.
func (m *MemStore) PresignPut(_ context.Context, stage Stage, dgst digest.Digest, size int64) (Address, error) {
	if err := stage.Validate(); err != nil {
		return Address{}, err
	}
	if err := dgst.Validate(); err != nil {
		return Address{}, err
	}
	if size <= 0 {
		return Address{}, ErrNotPositiveSize
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return Address{
		URL:       fmt.Sprintf("%s%s/staging/%s/%s?size=%d", m.baseURL, m.nsPath(), stage, dgst, size),
		ExpiresAt: time.Now().Add(DefaultPutExpiry),
	}, nil
}

// PresignGet returns an address that serves the blob:
// /mem[/<ns>]/<digest>.
func (m *MemStore) PresignGet(_ context.Context, dgst digest.Digest) (Address, error) {
	if err := dgst.Validate(); err != nil {
		return Address{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return Address{
		URL:       m.baseURL + m.nsPath() + "/" + dgst.String(),
		ExpiresAt: time.Now().Add(DefaultGetExpiry),
	}, nil
}

// Promote verifies the blob staged under stage, and stores it under its
// digest if it matches and no blob is there yet. Like the S3 store, it
// deletes the staged blob once it decides.
func (m *MemStore) Promote(ctx context.Context, stage Stage, dgst digest.Digest, size int64) (Promotion, error) {
	start := time.Now()
	if err := stage.Validate(); err != nil {
		return Promotion{}, err
	}
	if err := dgst.Validate(); err != nil {
		return Promotion{}, err
	}
	m.mu.Lock()
	hook := m.beforePromote
	m.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, m.ns, dgst); err != nil {
			return Promotion{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return Promotion{}, context.Cause(ctx)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	sk := stagedKey{ns: m.ns, stage: stage, dgst: dgst}
	data, ok := m.staged[sk]
	if !ok {
		return Promotion{Outcome: Missing, Duration: time.Since(start)}, nil
	}
	delete(m.staged, sk)
	if _, ok := m.blobs[m.key(dgst)]; ok {
		return Promotion{Outcome: Existing, Duration: time.Since(start)}, nil
	}
	p := Promotion{HashedBy: HashedByService, Bytes: int64(len(data))}
	if int64(len(data)) != size || digest.FromBytes(data) != dgst {
		p.Outcome = Mismatch
	} else {
		m.blobs[m.key(dgst)] = data
		p.Outcome = Promoted
	}
	p.Duration = time.Since(start)
	return p, nil
}

// Put stores a blob directly under its digest, as if an engine had uploaded
// it and the service had promoted it.
func (m *MemStore) Put(data []byte) digest.Digest {
	dgst := digest.FromBytes(data)
	m.PutUnverified(dgst, data)
	return dgst
}

// PutUnverified stores data under dgst whatever it hashes to, as an upload
// stored before uploads were verified could have.
func (m *MemStore) PutUnverified(dgst digest.Digest, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[m.key(dgst)] = bytes.Clone(data)
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

// Len returns how many blobs the view stores, staged ones left out.
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

// Staged returns the digests of the view's staged blobs, under any stage:
// uploaded and not promoted, nor decided on.
func (m *MemStore) Staged() []digest.Digest {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []digest.Digest
	for k := range m.staged {
		if k.ns == m.ns {
			out = append(out, k.dgst)
		}
	}
	return out
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

// ServeHTTP serves the addresses of the store and of every namespace view:
// GETs of promoted blobs, and PUTs to staging keys. A staged blob is never
// served, and a PUT can't reach a digest key.
func (m *MemStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name, ok := strings.CutPrefix(r.URL.Path, "/mem/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	view := &MemStore{memState: m.memState}
	// /mem/staging/<stage>/... is the store's own staging; any other first
	// segment followed by more is a namespace, "staging" included.
	if ns, rest, found := strings.Cut(name, "/"); found && !isRootStaging(ns, rest) {
		if ValidateNamespace(ns) != nil {
			http.Error(w, "bad namespace", http.StatusBadRequest)
			return
		}
		view.ns, name = ns, rest
	}
	var stage Stage
	if rest, staged := strings.CutPrefix(name, "staging/"); staged {
		s, d, _ := strings.Cut(rest, "/")
		stage, name = Stage(s), d
		if stage.Validate() != nil {
			http.Error(w, "bad stage", http.StatusBadRequest)
			return
		}
	}
	dgst, err := digest.Parse(name)
	if err != nil {
		http.Error(w, "bad digest", http.StatusBadRequest)
		return
	}
	switch {
	case r.Method == http.MethodGet && stage == "":
		data, ok := view.Get(dgst)
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	case r.Method == http.MethodPut && stage != "":
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
		m.mu.Lock()
		if m.failPuts[dgst] > 0 {
			m.failPuts[dgst]--
			m.mu.Unlock()
			http.Error(w, "injected failure", http.StatusServiceUnavailable)
			return
		}
		m.staged[stagedKey{ns: view.ns, stage: stage, dgst: dgst}] = data
		m.puts++
		m.mu.Unlock()
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// isRootStaging reports whether a path's segments first/rest are a staging
// address of the store itself: staging/<stage>/<digest>.
func isRootStaging(first, rest string) bool {
	stage, _, _ := strings.Cut(rest, "/")
	return first == "staging" && Stage(stage).Validate() == nil
}
