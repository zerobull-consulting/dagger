// Package layout is the cache service's storage layout: every key the
// service writes under its prefix (CACHESERVICE_S3_PREFIX, or the bucket's
// top when it is empty), in one place. The writers (the metadata and blob
// stores) build their keys here, and GC recognizes keys here, so the two
// can't drift apart.
//
// The service owns everything under its prefix: nothing else may write
// there (see package ownership, and the README's "Bucket ownership"). Layout
// version 2 is:
//
//	<prefix>/cacheservice.json                           the ownership marker
//	<prefix>/ns/<namespace>/index.json                   the namespace's index
//	<prefix>/ns/<namespace>/gc-epoch.json                GC's published epoch
//	<prefix>/ns/<namespace>/bundles/<sha256>.json        bundles
//	<prefix>/ns/<namespace>/resolutions/<sha256>.json    resolutions
//	<prefix>/ns/<namespace>/blobs/<alg>/<hex>            verified blobs
//	<prefix>/ns/<namespace>/staging/<stage>/<alg>/<hex>  uploads not yet verified
//	<prefix>/ns/<namespace>/conditional-write-check/<nonce>  the startup probe
//
// Incomplete multipart uploads exist only at blob keys and staging keys: a
// promotion copies a staged upload to its blob key as a multipart upload,
// and a service-hashed promotion first copies it to a private staging key,
// under a stage of its own that is never presigned, as a multipart upload
// when it is over 5 GiB.
//
// Any other key under the prefix is not the service's: an ownership
// violation, which GC reports and never deletes.
package layout

import (
	// Registered for go-digest, so a blob key's digest validates the same
	// whatever else the binary links.
	_ "crypto/sha256"
	_ "crypto/sha512"
	"fmt"
	"regexp"
	"strings"

	"github.com/opencontainers/go-digest"
)

// Version is the layout this package describes, as the marker records it.
const Version = 2

const (
	// MarkerName is the ownership marker's key, relative to the prefix.
	MarkerName = "cacheservice.json"
	// NamespacesDir is where namespaces live, relative to the prefix.
	NamespacesDir = "ns/"
	// IndexName is a namespace's index, relative to the namespace.
	IndexName = "index.json"
	// EpochName is a namespace's GC epoch object, relative to the
	// namespace.
	EpochName = "gc-epoch.json"
	// StagingDir is where a namespace's uploads are staged, relative to
	// the namespace. A lifecycle rule on <namespace prefix>staging/
	// expires what is left there.
	StagingDir = "staging/"
	// BlobsDir, BundlesDir and ResolutionsDir hold a namespace's live
	// data, relative to the namespace.
	BlobsDir       = "blobs/"
	BundlesDir     = "bundles/"
	ResolutionsDir = "resolutions/"
	// ProbeDir holds the conditional-write probe, relative to the
	// namespace. The probe deletes its object again; one left by a failed
	// delete is the service's, and harmless.
	ProbeDir = "conditional-write-check/"
)

// LiveRel are the relative keys and prefixes of a namespace's live data: a
// lifecycle rule that expires any of them by age deletes what the index
// names.
var LiveRel = []string{BlobsDir, BundlesDir, ResolutionsDir, IndexName, EpochName}

// Root is the prefix every key starts with: "" for an empty prefix, or the
// prefix, without surrounding slashes, followed by one.
func Root(prefix string) string {
	if p := strings.Trim(prefix, "/"); p != "" {
		return p + "/"
	}
	return ""
}

// MarkerKey is the ownership marker's key: <prefix>/cacheservice.json.
func MarkerKey(prefix string) string { return Root(prefix) + MarkerName }

// NamespacesPrefix is the prefix of every namespace: <prefix>/ns/.
func NamespacesPrefix(prefix string) string { return Root(prefix) + NamespacesDir }

// namespacePattern is what a namespace may be: one key segment, with
// nothing a path could misread.
var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidateNamespace reports whether ns can name a namespace's keys.
func ValidateNamespace(ns string) error {
	if !namespacePattern.MatchString(ns) || strings.Contains(ns, "..") {
		return fmt.Errorf("invalid scope %q: want 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit, without '..'", ns)
	}
	return nil
}

// NamespacePrefix is the prefix of every key of a namespace:
// <prefix>/ns/<ns>/.
func NamespacePrefix(prefix, ns string) (string, error) {
	if err := ValidateNamespace(ns); err != nil {
		return "", err
	}
	return NamespacesPrefix(prefix) + ns + "/", nil
}

// Keys relative to a namespace.

// BundleRel is a bundle's key: bundles/<id>.json.
func BundleRel(id string) string { return BundlesDir + id + ".json" }

// ResolutionRel is a resolution's key: resolutions/<id>.json.
func ResolutionRel(id string) string { return ResolutionsDir + id + ".json" }

// BlobRel is a blob's digest key: blobs/<algorithm>/<hex>.
func BlobRel(d digest.Digest) string {
	return BlobsDir + d.Algorithm().String() + "/" + d.Encoded()
}

// StagingRel is a blob's staged key under a stage:
// staging/<stage>/<algorithm>/<hex>.
func StagingRel(stage string, d digest.Digest) string {
	return StagingDir + stage + "/" + d.Algorithm().String() + "/" + d.Encoded()
}

// ProbeRel is the conditional-write probe's key for a nonce:
// conditional-write-check/<16 hex>.
func ProbeRel(nonce uint64) string { return fmt.Sprintf("%s%016x", ProbeDir, nonce) }

// Kind is the shape of a key relative to a namespace.
type Kind string

const (
	KindIndex      Kind = "index"
	KindEpoch      Kind = "epoch"
	KindBundle     Kind = "bundle"
	KindResolution Kind = "resolution"
	KindBlob       Kind = "blob"
	KindStaging    Kind = "staging"
	KindProbe      Kind = "probe"
	// KindUnknown is no shape of the service's: an ownership violation.
	KindUnknown Kind = ""
)

var (
	bundlePattern     = regexp.MustCompile(`^bundles/([0-9a-f]{64})\.json$`)
	resolutionPattern = regexp.MustCompile(`^resolutions/([0-9a-f]{64})\.json$`)
	// stagePattern is a blobstore.Stage: 16 random bytes, in hex.
	stagingPattern = regexp.MustCompile(`^staging/([0-9a-f]{32})/([^/]+/[^/]+)$`)
	probePattern   = regexp.MustCompile(`^conditional-write-check/[0-9a-f]{16}$`)
)

// StagePattern is what a stage (blobstore.Stage) is: 32 lowercase hex.
var StagePattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// Classify returns the shape of rel, a key relative to a namespace, and
// for a bundle or resolution its ID. Every shape is exact: a blob's or
// staged upload's digest must be valid and written as BlobRel writes it.
func Classify(rel string) (Kind, string) {
	switch {
	case rel == IndexName:
		return KindIndex, ""
	case rel == EpochName:
		return KindEpoch, ""
	case strings.HasPrefix(rel, BundlesDir):
		if m := bundlePattern.FindStringSubmatch(rel); m != nil {
			return KindBundle, m[1]
		}
	case strings.HasPrefix(rel, ResolutionsDir):
		if m := resolutionPattern.FindStringSubmatch(rel); m != nil {
			return KindResolution, m[1]
		}
	case strings.HasPrefix(rel, BlobsDir):
		if _, ok := ParseBlobRel(rel); ok {
			return KindBlob, ""
		}
	case strings.HasPrefix(rel, StagingDir):
		if _, _, ok := ParseStagingRel(rel); ok {
			return KindStaging, ""
		}
	case probePattern.MatchString(rel):
		return KindProbe, ""
	}
	return KindUnknown, ""
}

// IsUploadRel reports whether an incomplete multipart upload at rel, a key
// relative to a namespace, is of the service's: only promotions start
// multipart uploads, at blob keys, and, for a service-hashed promotion of a
// blob over 5 GiB, at the staging key of its private copy. Engines upload
// with a plain PUT, never a multipart upload.
func IsUploadRel(rel string) bool {
	k, _ := Classify(rel)
	return k == KindBlob || k == KindStaging
}

// ParseBlobRel returns the digest of a blob key relative to a namespace.
func ParseBlobRel(rel string) (digest.Digest, bool) {
	enc, ok := strings.CutPrefix(rel, BlobsDir)
	if !ok {
		return "", false
	}
	d, ok := parseDigestPath(enc)
	if !ok || BlobRel(d) != rel {
		return "", false
	}
	return d, true
}

// ParseStagingRel returns the stage and digest of a staged key relative to
// a namespace.
func ParseStagingRel(rel string) (string, digest.Digest, bool) {
	m := stagingPattern.FindStringSubmatch(rel)
	if m == nil {
		return "", "", false
	}
	d, ok := parseDigestPath(m[2])
	if !ok || StagingRel(m[1], d) != rel {
		return "", "", false
	}
	return m[1], d, true
}

// parseDigestPath parses <algorithm>/<hex>.
func parseDigestPath(p string) (digest.Digest, bool) {
	alg, enc, ok := strings.Cut(p, "/")
	if !ok {
		return "", false
	}
	d := digest.NewDigestFromEncoded(digest.Algorithm(alg), enc)
	if d.Validate() != nil {
		return "", false
	}
	return d, true
}

// Place is where a key is under a prefix.
type Place int

const (
	// Outside is under the prefix but not the marker nor in a namespace:
	// not the service's.
	Outside Place = iota
	// Marker is the ownership marker.
	Marker
	// InNamespace is under ns/<namespace>/, with a valid namespace name.
	// Its relative key may still be of no known shape (Classify).
	InNamespace
	// NotUnder is not under the prefix at all.
	NotUnder
)

// Locate says where key is under prefix, and for a key in a namespace, the
// namespace and the key relative to it.
func Locate(prefix, key string) (place Place, ns, rel string) {
	rest, ok := strings.CutPrefix(key, Root(prefix))
	if !ok {
		return NotUnder, "", ""
	}
	if rest == MarkerName {
		return Marker, "", ""
	}
	in, ok := strings.CutPrefix(rest, NamespacesDir)
	if !ok {
		return Outside, "", ""
	}
	ns, rel, ok = strings.Cut(in, "/")
	if !ok || ValidateNamespace(ns) != nil || rel == "" {
		return Outside, "", ""
	}
	return InNamespace, ns, rel
}

// Known reports whether key, under prefix, is of one of the service's
// shapes: the marker, or a key of known shape in a namespace.
func Known(prefix, key string) bool {
	place, _, rel := Locate(prefix, key)
	switch place {
	case Marker:
		return true
	case InNamespace:
		k, _ := Classify(rel)
		return k != KindUnknown
	}
	return false
}
