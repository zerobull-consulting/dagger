package importer

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/dagql"
)

// What BuildMerge adds to a stored bundle, at most, besides the download
// addresses:
const (
	// mergeOverheadBytes is the merge request's and the envelope's own
	// fields.
	mergeOverheadBytes = 512
	// rootGrowthBytes is a root's expiry, which the merge sets on every root:
	// `,"expiresAtUnix":` and up to 20 digits.
	rootGrowthBytes = 40
	// valueGrowthBytes is a value's SenderNumber becoming its service number,
	// up to 20 digits where the stored one may have 1.
	valueGrowthBytes = 20
	// chainOverheadBytes is a chain's `,"renewalKey":`, without the key.
	chainOverheadBytes = 16
)

// MergeSize returns an upper bound on the size of the merge request
// BuildMerge makes from a stored bundle, as it is sent: every root gets an
// expiry, every value a service number, each output's chain renewalKey, and
// each layer blob a download address, whose JSON is at most addressBytes
// (see AddressBytes).
//
// A merge lists its chains' layers, and each blob's address, once
// (protocol.Merge's encoding, dagql.ShareChainLayers), however many of the
// bundle's outputs build on them, so the bound starts from the bundle in
// that form, not as stored, where each chain lists every layer from its
// base up.
//
// The importer sends a stored bundle whole, so a bundle whose merge is over
// the message limit is never imported. The export path checks a bundle with
// this before storing it, and splits an export whose bundle is over.
func MergeSize(bundle *dagql.ValueBundle, addressBytes int, renewalKey string) (int, error) {
	shared, err := dagql.ShareChainLayers(*bundle)
	if err != nil {
		return 0, err
	}
	data, err := json.Marshal(shared)
	if err != nil {
		return 0, err
	}
	n := len(data) + mergeOverheadBytes +
		len(bundle.Roots)*rootGrowthBytes +
		len(bundle.Values)*valueGrowthBytes
	keyBytes := len(strconv.Quote(renewalKey))
	signed := map[digest.Digest]bool{}
	for _, o := range bundle.Outputs {
		if o.Chain == nil {
			continue
		}
		n += chainOverheadBytes + keyBytes
		for _, l := range o.Chain.Layers {
			d := l.Descriptor.Digest
			if signed[d] {
				continue
			}
			signed[d] = true
			// "<digest>":<address>, where the address names the blob's key,
			// which holds the digest's hex once: longer for a longer
			// digest than AddressBytes measured.
			n += len(d.String()) + 4 + addressBytes + max(0, len(d.Encoded())-canonicalHexBytes)
		}
	}
	return n, nil
}

// canonicalHexBytes is the hex length of a digest of the algorithm
// AddressBytes signs for.
var canonicalHexBytes = digest.Canonical.Size() * 2

// AddressBytes returns the size of one download address from sign, as it is
// encoded in a merge, for a blob of the canonical algorithm (sha256). Every
// address the signer makes for such a blob has the same size, up to its
// expiry's digits, which it counts at their most. A blob's address names its
// key, which holds the algorithm's name and the digest's hex once
// (blobstore's blobKey): MergeSize adds the extra hex of a longer digest,
// such as sha512's, per layer.
func AddressBytes(ctx context.Context, sign Signer) (int, error) {
	addr, err := sign(ctx, digest.Canonical.FromString(""))
	if err != nil {
		return 0, err
	}
	// The longest expiry: the address's own may have fewer digits.
	addr.ExpiresAtUnix = 1<<63 - 1
	data, err := json.Marshal(addr)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// DefaultAddressBytes stands in for AddressBytes when a signer can't be
// asked: a presigned S3 GET is about 1 KB, and up to about 2 KB with a
// session token.
const DefaultAddressBytes = 4 << 10
