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
	// chainOverheadBytes is a chain's `,"addresses":{}` and
	// `,"renewalKey":""`, without the key itself.
	chainOverheadBytes = 40
)

// MergeSize returns an upper bound on the size of the merge request
// BuildMerge makes from a stored bundle of size storedBytes (its JSON):
// every root gets an expiry, every value a service number, and each output's
// chain renewalKey and a download address per layer, whose JSON is at most
// addressBytes (see AddressBytes).
//
// The importer sends a stored bundle whole, so a bundle whose merge is over
// the message limit is never imported. The export path checks a bundle with
// this before storing it, and splits an export whose bundle is over.
func MergeSize(bundle *dagql.ValueBundle, storedBytes, addressBytes int, renewalKey string) int {
	n := storedBytes + mergeOverheadBytes +
		len(bundle.Roots)*rootGrowthBytes +
		len(bundle.Values)*valueGrowthBytes
	keyBytes := len(strconv.Quote(renewalKey))
	for _, o := range bundle.Outputs {
		if o.Chain == nil {
			continue
		}
		n += chainOverheadBytes + keyBytes
		for _, l := range o.Chain.Layers {
			// "<digest>":<address>,
			n += len(l.Descriptor.Digest.String()) + 4 + addressBytes
		}
	}
	return n
}

// AddressBytes returns the size of one download address from sign, as it is
// encoded in a merge. Every address the signer makes for a blob of the
// default algorithm has the same size, up to its expiry's digits, which it
// counts at their most.
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
