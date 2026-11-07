package server

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// Tokens maps engine tokens to namespaces. The service asks it on every
// WebSocket upgrade and OTLP request, and again for every connection after
// SetTokens, so it should answer from memory. Implementations other than
// TokenFile, such as a lookup in another service, must be safe to call from
// many goroutines.
type Tokens interface {
	// Lookup returns the namespace of token, and false when the token is
	// unknown. It must not log the token.
	Lookup(token string) (namespace string, ok bool)
}

// TokenFile is Tokens read from a file: one token and its namespace per
// line, separated by white space. Blank lines and lines starting with # are
// ignored:
//
//	# team A's engines
//	3f9c0e...   team-a
//	b72d41...   team-b
//
// A token may not contain white space or ':' (HTTP Basic ends the username
// at the first colon), and may appear once; a namespace is
// what metastore.ValidateScope accepts. Many tokens may share a namespace.
//
// TokenFile keeps only the tokens' SHA-256 hashes, and compares them in
// constant time: a lookup compares against every token, whether or not one
// matched.
type TokenFile struct {
	entries []tokenEntry
}

type tokenEntry struct {
	hash      [sha256.Size]byte
	namespace string
}

// LoadTokenFile reads a tokens file. Errors name the line, never the token.
func LoadTokenFile(path string) (*TokenFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tokens file: %w", err)
	}
	return ParseTokens(data)
}

// ParseTokens reads the content of a tokens file.
func ParseTokens(data []byte) (*TokenFile, error) {
	tf := &TokenFile{}
	seen := map[[sha256.Size]byte]int{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("tokens file: line %d: want a token and a namespace, separated by white space", n)
		}
		if strings.Contains(fields[0], ":") {
			// HTTP Basic splits the username from the password at the
			// first colon, so such a token could never authenticate.
			return nil, fmt.Errorf("tokens file: line %d: a token may not contain ':'", n)
		}
		ns := fields[1]
		if err := metastore.ValidateScope(ns); err != nil {
			return nil, fmt.Errorf("tokens file: line %d: %w", n, err)
		}
		hash := sha256.Sum256([]byte(fields[0]))
		if prev, ok := seen[hash]; ok {
			return nil, fmt.Errorf("tokens file: line %d: the token of line %d again", n, prev)
		}
		seen[hash] = n
		tf.entries = append(tf.entries, tokenEntry{hash: hash, namespace: ns})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("tokens file: %w", err)
	}
	return tf, nil
}

// Len returns how many tokens the file maps.
func (tf *TokenFile) Len() int { return len(tf.entries) }

// Namespaces returns how many distinct namespaces the file maps to.
func (tf *TokenFile) Namespaces() int { return len(tf.NamespaceNames()) }

// Lookup returns the namespace of token. It hashes the token and compares
// the hash with every entry's in constant time, so how long it takes says
// nothing about which entry, if any, matched.
func (tf *TokenFile) Lookup(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	hash := sha256.Sum256([]byte(token))
	found := -1
	for i := range tf.entries {
		if subtle.ConstantTimeCompare(hash[:], tf.entries[i].hash[:]) == 1 {
			found = i
		}
	}
	if found < 0 {
		return "", false
	}
	return tf.entries[found].namespace, true
}

// ErrNoTokensFile is returned by ReloadTokens when the service was started
// without a tokens file: it is unauthenticated, and stays so.
var ErrNoTokensFile = errors.New("the service has no tokens file: it is unauthenticated")

// NamespaceNames returns the distinct namespaces the file maps to, sorted:
// the lifecycle command's namespaces (package lifecycle). It never returns
// tokens.
func (tf *TokenFile) NamespaceNames() []string {
	set := map[string]bool{}
	var out []string
	for _, e := range tf.entries {
		if !set[e.namespace] {
			set[e.namespace] = true
			out = append(out, e.namespace)
		}
	}
	slices.Sort(out)
	return out
}
