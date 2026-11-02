package server

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/exporter"
	"github.com/dagger/dagger/cmd/cacheservice/importer"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// DefaultNamespace is the namespace of every engine of a service without
// tokens, until EnableExport or EnableImport names another (their scope).
const DefaultNamespace = "default"

// namespace is everything the service keeps for one namespace: the export
// path's tracker and exporter, and the import path's importer, each on the
// namespace's own index, bundles and blobs. A connection, and every OTLP
// request, belongs to the namespace of its token; nothing it sends reaches
// another namespace's state, and nothing another namespace stored is
// recorded, merged, offered or renewed for it.
//
// A namespace is created on first use, after the paths are enabled, and its
// paths never change after that.
type namespace struct {
	name string
	// tracker and exporter are nil when the export path is off, importer
	// when the import path is.
	tracker  *exporter.Tracker
	exporter *exporter.Exporter
	importer *importer.Importer

	mu sync.Mutex
	// owners maps each engine cache that said hello in the namespace to the
	// token of its latest welcomed connection. Engine events (engine.start,
	// engine.prune) about a cache are taken only from that token: another
	// token of the namespace, such as a client's or another engine's, can't
	// steer what the service knows of it. See ownerAccepts.
	owners map[string]tokenKey
}

// tokenKey identifies a token without holding it: its SHA-256.
type tokenKey [sha256.Size]byte

func keyOf(token string) tokenKey { return sha256.Sum256([]byte(token)) }

// own records that a connection with token key said hello as cacheID.
func (n *namespace) own(cacheID string, key tokenKey) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.owners[cacheID] = key
}

// disown forgets the owner of a wiped cache, whose ID never connects again.
func (n *namespace) disown(cacheID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.owners, cacheID)
}

// ownerAccepts reports whether an engine event about cacheID may come from
// token key: when no connection of the namespace has said hello as cacheID
// (an engine's start event may arrive before its hello), or when the latest
// one that did had the same token.
func (n *namespace) ownerAccepts(cacheID string, key tokenKey) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	owner, ok := n.owners[cacheID]
	return !ok || owner == key
}

// namespace returns a namespace's state, created with the paths enabled now.
func (s *Service) namespace(name string) (*namespace, error) {
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	if n := s.namespaces[name]; n != nil {
		return n, nil
	}
	n, err := s.newNamespace(name)
	if err != nil {
		return nil, err
	}
	s.namespaces[name] = n
	return n, nil
}

// newNamespace builds a namespace's paths from the enabled paths' configs.
func (s *Service) newNamespace(name string) (*namespace, error) {
	n := &namespace{name: name, owners: map[string]tokenKey{}}
	if s.export == nil && s.imports == nil {
		return n, nil
	}
	if err := metastore.ValidateScope(name); err != nil {
		return nil, fmt.Errorf("namespace: %w", err)
	}
	if p := s.export; p != nil {
		cfg := p.cfg
		cfg.Scope = name
		blobs, err := namespaceBlobs(cfg.Blobs, name)
		if err != nil {
			return nil, err
		}
		cfg.Blobs = blobs
		if n.exporter, err = exporter.New(cfg); err != nil {
			return nil, err
		}
		n.tracker = exporter.NewTracker(p.trackerCfg)
	}
	if p := s.imports; p != nil {
		cfg := p.cfg
		cfg.Scope = name
		blobs, err := namespaceBlobs(cfg.Blobs, name)
		if err != nil {
			return nil, err
		}
		cfg.Blobs = blobs
		if cfg.Imported == nil && n.tracker != nil {
			tracker := n.tracker
			cfg.Imported = func(cacheID string, generation uint64, values map[uint64]importer.Value) {
				out := make([]exporter.ImportedValue, 0, len(values))
				for _, v := range values {
					parts := slices.Concat(v.Complete, v.Offered)
					out = append(out, exporter.ImportedValue{Number: v.Number, Replacements: v.Replacements, Parts: parts})
				}
				tracker.Imported(cacheID, generation, out)
			}
		}
		if n.importer, err = importer.New(cfg); err != nil {
			return nil, err
		}
	}
	return n, nil
}

// namespaceBlobs is the store of a namespace's blobs: the namespace's view of
// a store that keeps namespaces apart, or else the store itself, shared.
func namespaceBlobs(store blobstore.Store, name string) (blobstore.Store, error) {
	if ns, ok := store.(blobstore.Namespacer); ok {
		return ns.Namespace(name)
	}
	return store, nil
}

// namespacesNow returns the namespaces created so far, by name.
func (s *Service) namespacesNow() []*namespace {
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	out := make([]*namespace, 0, len(s.namespaces))
	for _, n := range s.namespaces {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// setDefaultNamespace takes a path's scope as the namespace of a service
// without tokens. Both paths must name the same one.
func (s *Service) setDefaultNamespace(scope string) error {
	if err := metastore.ValidateScope(scope); err != nil {
		return err
	}
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	if s.scopeSet && s.defaultNS != scope {
		return fmt.Errorf("the export and import paths name different scopes, %q and %q", s.defaultNS, scope)
	}
	if len(s.namespaces) > 0 {
		return errors.New("enable the export and import paths before engines connect")
	}
	s.defaultNS, s.scopeSet = scope, true
	return nil
}

// Namespace returns the namespace a connection's token maps to.
func (s *Service) Namespace(conn uint64) (string, error) {
	c, err := s.conn(conn)
	if err != nil {
		return "", err
	}
	return c.ns.name, nil
}
