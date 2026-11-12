package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine/remotecache/protocol"
)

// MergeSize bounds what the importer sends for a stored bundle, and is close
// to it: the export path refuses to store a bundle over the import path's
// message limit by it (#12), so it must not let one through that the
// importer would then refuse, nor refuse many it could send.
func TestMergeSizeBoundsBuildMerge(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values int
		// merged is how many of the chain's entries are merged roots; the
		// rest are sent expired.
		merged int
		// sha512 gives every layer a sha512 digest, whose address is longer
		// than the sha256 one AddressBytes measures.
		sha512 bool
	}{
		{name: "one value", values: 1, merged: 1},
		{name: "a deep chain, every entry a root", values: 40, merged: 40},
		{name: "a deep chain, its top merged", values: 40, merged: 1},
		{name: "a deep chain of sha512 layers", values: 40, merged: 40, sha512: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recipes := make([]string, tc.values)
			for i := range recipes {
				recipes[i] = fmt.Sprintf("step-%d", i)
			}
			stored := chainBundle(10, recipes...)
			if tc.sha512 {
				for _, o := range stored.Outputs {
					for i := range o.Chain.Layers {
						l := &o.Chain.Layers[i].Descriptor
						l.Digest = digest.SHA512.FromString(l.Digest.String())
					}
				}
			}
			stored.Roots = nil
			for i := range tc.values {
				stored.Roots = append(stored.Roots, dagql.TransferredRoot{Ordinal: stored.Values[i].Ordinal})
			}
			idx := metastore.NewIndex()
			id := testBundleID(tc.name)
			numbers := record(t, idx, id, stored, "cache-a")
			var roots []uint64
			for i := tc.values - tc.merged; i < tc.values; i++ {
				n := numbers[stored.Values[i].Ordinal]
				idx.Roots[n] = metastore.Root{ExpiresAtUnix: 4000000000}
				roots = append(roots, n)
			}
			mb, err := BuildMerge(context.Background(), idx, id, stored, resolutionOf(t, idx, roots[0]), roots, testSigner, "team")
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(protocol.Merge{Bundle: mb.Bundle})
			if err != nil {
				t.Fatal(err)
			}
			// As the importer counts it against the limit (encode).
			sent := len(data) + 128

			storedData, err := json.Marshal(stored)
			if err != nil {
				t.Fatal(err)
			}
			addr, err := AddressBytes(context.Background(), testSigner)
			if err != nil {
				t.Fatal(err)
			}
			est, err := MergeSize(stored, addr, "team")
			if err != nil {
				t.Fatal(err)
			}
			if sent > est {
				t.Fatalf("the merge is %d bytes as sent, over its estimate %d", sent, est)
			}
			// Within 10%, and the fixed overhead, of what was sent: the
			// growth counted per root and value is at its most, and a
			// merge that lists its layers once is small beside it.
			if est-sent > est/10+mergeOverheadBytes {
				t.Fatalf("the merge is %d bytes as sent, its estimate %d: too far over", sent, est)
			}
			t.Logf("stored %d bytes; merge %d bytes as sent, estimated %d", len(storedData), sent, est)
		})
	}
}
