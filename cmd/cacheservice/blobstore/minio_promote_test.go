package blobstore

// Tests of staging and promotion (#23) against MinIO. They skip unless
// CACHESERVICE_S3_ENDPOINT is set; `hack/remote-cache/dev.sh test` runs them.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/opencontainers/go-digest"
)

// hashModes are the two ways a promotion is hashed: by the store as it
// copies (the default for SHA-256 blobs up to 5 GiB), and by the service
// reading the staged object (HashInService, larger blobs, stores that don't
// hash).
var hashModes = []struct {
	name      string
	inService bool
	hashedBy  string
}{
	{"store", false, HashedByStore},
	{"service", true, HashedByService},
}

// #23's acceptance, replacing TestMinIOStoreKnownGapMisdeclaredDigest: S3
// takes a same-size body with the wrong content (a presigned PUT bounds the
// size only), but it lands at a staging key. Its promotion is a mismatch, so
// it never appears at the digest key; the staged object is dropped; Has
// reports nothing, so UploadURLs asks for the blob again; and the right
// upload is then promoted.
func TestMinIOStoreMisdeclaredDigestNeverStored(t *testing.T) {
	for _, mode := range hashModes {
		t.Run(mode.name, func(t *testing.T) {
			s := newMinIOStore(t, func(c *S3Config) { c.HashInService = mode.inService })
			ctx := context.Background()
			blob, d := randomBlob(t, 4096)
			wrong := bytes.Clone(blob)
			wrong[0] ^= 0xff

			stage := NewStage()
			urls, err := UploadURLs(ctx, s, stage, []Blob{{Digest: d, Size: int64(len(blob))}})
			if err != nil || len(urls) != 1 {
				t.Fatalf("upload URLs %v, %v", urls, err)
			}
			if status := enginePut(t, urls[d], wrong); status != http.StatusOK {
				t.Fatalf("PUT of the wrong content: status %d; S3 checks sizes only", status)
			}
			key, _ := s.Key(d)
			if mustHas(t, s, d) || objectExists(t, s, key) {
				t.Fatal("the wrong content is at the digest key before promotion")
			}
			p, err := s.Promote(ctx, stage, d, int64(len(blob)))
			if err != nil {
				t.Fatal(err)
			}
			if p.Outcome != Mismatch || p.HashedBy != mode.hashedBy || p.Bytes != int64(len(blob)) {
				t.Fatalf("promote: %+v, want a mismatch hashed by the %s", p, mode.hashedBy)
			}
			if mustHas(t, s, d) || objectExists(t, s, key) {
				t.Fatal("the wrong content reached the digest key")
			}
			if left := listKeys(t, s, s.StagingPrefix()); len(left) != 0 {
				t.Fatalf("staged objects left: %v", left)
			}
			if left := listUploads(t, s); len(left) != 0 {
				t.Fatalf("multipart uploads left: %v", left)
			}
			get, _ := s.PresignGet(ctx, d)
			if status, _ := httpGet(t, get.URL); status != http.StatusNotFound {
				t.Fatalf("GET of the digest key: status %d", status)
			}

			// Asked for again, and the right content is promoted.
			stage = NewStage()
			urls, err = UploadURLs(ctx, s, stage, []Blob{{Digest: d, Size: int64(len(blob))}})
			if err != nil || len(urls) != 1 {
				t.Fatalf("the blob isn't asked for again: %v, %v", urls, err)
			}
			if status := enginePut(t, urls[d], blob); status != http.StatusOK {
				t.Fatalf("PUT: status %d", status)
			}
			if p, err := s.Promote(ctx, stage, d, int64(len(blob))); err != nil || p.Outcome != Promoted {
				t.Fatalf("promote: %+v, %v", p, err)
			}
			if status, data := httpGet(t, get.URL); status != http.StatusOK || digest.FromBytes(data) != d {
				t.Fatalf("GET: status %d, digest %s", status, digest.FromBytes(data))
			}
		})
	}
}

// listUploads lists the multipart uploads in progress under the store's
// prefix.
func listUploads(t *testing.T, s *S3Store) []string {
	t.Helper()
	out, err := s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: &s.bucket, Prefix: aws.String(s.prefix + "/")})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, u := range out.Uploads {
		keys = append(keys, aws.ToString(u.Key))
	}
	return keys
}

// #23's open question 1: can plain S3-compatible storage do the hashing?
// MinIO can. A CopyObject with ChecksumAlgorithm SHA256 stores and returns
// the full object's SHA-256, computed on the server from an object uploaded
// with no checksum at all (the engine's bare PUT). So does an UploadPartCopy
// into a multipart upload created with that algorithm: one part's checksum
// is the whole object's, returned before the upload completes, which is what
// Promote uses, so the digest key only appears once the hash matches. A
// failure here means the store stopped hashing: Promote then falls back to
// hashing in the service (TestMinIOPromoteStoreWithoutChecksums).
func TestMinIOStoreHashesCopies(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	blob, d := randomBlob(t, 3<<20+5)
	src := s.prefix + "/probe/src"
	put, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &src, ContentLength: aws.Int64(int64(len(blob)))})
	if err != nil {
		t.Fatal(err)
	}
	if status := enginePut(t, put.URL, blob); status != http.StatusOK {
		t.Fatalf("PUT: status %d", status)
	}
	sum := sha256.Sum256(blob)
	want := base64.StdEncoding.EncodeToString(sum[:])

	dst := s.prefix + "/probe/copy"
	out, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: &s.bucket, Key: &dst, CopySource: aws.String(copySource(s.bucket, src)),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(out.CopyObjectResult.ChecksumSHA256); got != want {
		t.Fatalf("CopyObject's SHA-256 %q, want %q (%s)", got, want, d)
	}

	up, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &s.bucket, Key: aws.String(s.prefix + "/probe/part"), ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.aborter(ctx, s.prefix+"/probe/part", up.UploadId)()
	part, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket: &s.bucket, Key: aws.String(s.prefix + "/probe/part"), UploadId: up.UploadId,
		PartNumber: aws.Int32(1), CopySource: aws.String(copySource(s.bucket, src)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(part.CopyPartResult.ChecksumSHA256); got != want {
		t.Fatalf("UploadPartCopy's SHA-256 %q, want %q", got, want)
	}
}

// Promotion's conditional write, and what MinIO honors of it:
//
//   - If-None-Match on CopyObject is ignored: a second conditional copy
//     overwrites the key. That is why Promote never uses CopyObject.
//   - If-None-Match on CompleteMultipartUpload is honored (412), and so is
//     CopySourceIfMatch on a copy, which the service-hashed path relies on.
//   - Two exports racing to promote one blob from two stages both end with
//     the blob stored and correct: whichever completes first wins, and the
//     other finds it Existing. Neither staged object is left.
func TestMinIOConditionalPromotion(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	blob, d := randomBlob(t, 4096)
	src := s.prefix + "/probe/src"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &src, Body: bytes.NewReader(blob)}); err != nil {
		t.Fatal(err)
	}
	dst := s.prefix + "/probe/dst"
	for i := range 2 {
		_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &s.bucket, Key: &dst, CopySource: aws.String(copySource(s.bucket, src)), IfNoneMatch: aws.String("*")})
		if err != nil {
			t.Fatalf("conditional CopyObject %d: %v; MinIO now honors If-None-Match on copies (update this test and the README)", i+1, err)
		}
	}
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &s.bucket, Key: aws.String(dst + "2"), CopySource: aws.String(copySource(s.bucket, src)), CopySourceIfMatch: aws.String(`"0123"`)})
	if !isConditionFailed(err) {
		t.Fatalf("copy with a stale CopySourceIfMatch: %v, want 412", err)
	}
	up, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.bucket, Key: &dst})
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: &s.bucket, Key: &dst, UploadId: up.UploadId, PartNumber: aws.Int32(1), CopySource: aws.String(copySource(s.bucket, src))})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := s.complete(ctx, dst, up.UploadId, []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.CopyPartResult.ETag}}, d)
	if err == nil || outcome != "" {
		t.Fatalf("CompleteMultipartUpload over an existing key: %q, %v; want a 412", outcome, err)
	}
	if !isConditionFailed(err) {
		t.Fatalf("CompleteMultipartUpload over an existing key: %v, want a 412", err)
	}

	for _, mode := range hashModes {
		t.Run("race-"+mode.name, func(t *testing.T) {
			s := newMinIOStore(t, func(c *S3Config) { c.HashInService = mode.inService })
			blob, d := randomBlob(t, 1<<20)
			stages := []Stage{NewStage(), NewStage()}
			for _, stage := range stages {
				put, _ := s.PresignPut(ctx, stage, d, int64(len(blob)))
				if status := enginePut(t, put.URL, blob); status != http.StatusOK {
					t.Fatalf("PUT: status %d", status)
				}
			}
			var wg sync.WaitGroup
			outcomes := make([]Outcome, len(stages))
			for i, stage := range stages {
				wg.Add(1)
				go func() {
					defer wg.Done()
					p, err := s.Promote(ctx, stage, d, int64(len(blob)))
					if err != nil {
						t.Error(err)
					}
					outcomes[i] = p.Outcome
				}()
			}
			wg.Wait()
			promoted := 0
			for _, o := range outcomes {
				if o == Promoted {
					promoted++
				} else if o != Existing {
					t.Fatalf("outcomes %v", outcomes)
				}
			}
			// Exactly one promotes: the other either sees the blob in its
			// Has check, or, when both copied, loses the conditional
			// CompleteMultipartUpload.
			if promoted != 1 {
				t.Fatalf("outcomes %v, want one promoted and one existing", outcomes)
			}
			get, _ := s.PresignGet(ctx, d)
			if status, data := httpGet(t, get.URL); status != http.StatusOK || digest.FromBytes(data) != d {
				t.Fatalf("GET: status %d, digest %s", status, digest.FromBytes(data))
			}
			if left := listKeys(t, s, s.StagingPrefix()); len(left) != 0 {
				t.Fatalf("staged objects left: %v", left)
			}
			if left := listUploads(t, s); len(left) != 0 {
				t.Fatalf("multipart uploads left: %v", left)
			}
		})
	}
}

// A blob over the largest part copy (5 GiB on S3; lowered here to 5 MiB,
// S3's smallest part) is hashed by the service and copied in parts, each
// conditioned on the ETag it read. A mismatch is caught the same way.
func TestMinIOPromoteManyParts(t *testing.T) {
	s := newMinIOStore(t, nil)
	s.maxPartCopy, s.copyPartSize = 5<<20, 5<<20
	ctx := context.Background()
	blob, d := randomBlob(t, 12<<20+3)
	p := stageAndPromote(t, s, d, int64(len(blob)), blob)
	if p.Outcome != Promoted || p.HashedBy != HashedByService || p.Bytes != int64(len(blob)) {
		t.Fatalf("promote: %+v", p)
	}
	get, _ := s.PresignGet(ctx, d)
	if status, data := httpGet(t, get.URL); status != http.StatusOK || !bytes.Equal(data, blob) {
		t.Fatalf("GET: status %d, %d bytes, equal %v", status, len(data), bytes.Equal(data, blob))
	}

	other, od := randomBlob(t, 12<<20+3)
	wrong := bytes.Clone(other)
	wrong[len(wrong)-1] ^= 0xff
	if p := stageAndPromote(t, s, od, int64(len(other)), wrong); p.Outcome != Mismatch || p.HashedBy != HashedByService {
		t.Fatalf("promote of the wrong content: %+v", p)
	}
	if mustHas(t, s, od) {
		t.Fatal("the wrong content reached the digest key")
	}
	if left := listUploads(t, s); len(left) != 0 {
		t.Fatalf("multipart uploads left: %v", left)
	}
}

// A store whose part copies return no checksum (seen here by hashing with
// it off: a store must hash on request) has its blobs hashed by the service.
// Simulated by HashInService, which takes the same path; the detection
// itself is noStoreHash, set by the first copy without a checksum.
func TestMinIOPromoteStoreWithoutChecksums(t *testing.T) {
	s := newMinIOStore(t, nil)
	s.noStoreHash.Store(true)
	blob, d := randomBlob(t, 4096)
	if p := stageAndPromote(t, s, d, int64(len(blob)), blob); p.Outcome != Promoted || p.HashedBy != HashedByService {
		t.Fatalf("promote: %+v", p)
	}
}

// Time to promotion as a function of blob size, for both ways of hashing:
// what the README's table comes from. The sizes are small by default, to
// keep CI fast (about a second in all); CACHESERVICE_TEST_PROMOTE_MB adds
// one size in MiB (1024 takes about 30s against the local MinIO, and writes
// 3 GiB: the upload, its copy, and the staged copy's deletion).
func TestMinIOPromoteLatency(t *testing.T) {
	sizes := []int{64 << 10, 1 << 20, 16 << 20, 64 << 20}
	if v := os.Getenv("CACHESERVICE_TEST_PROMOTE_MB"); v != "" {
		mb, err := strconv.Atoi(v)
		if err != nil || mb <= 0 {
			t.Fatalf("CACHESERVICE_TEST_PROMOTE_MB=%q", v)
		}
		sizes = append(sizes, mb<<20)
	}
	for _, mode := range hashModes {
		s := newMinIOStore(t, func(c *S3Config) { c.HashInService = mode.inService })
		for _, size := range sizes {
			blob, d := randomBlob(t, size)
			start := time.Now()
			stage := NewStage()
			put, _ := s.PresignPut(context.Background(), stage, d, int64(size))
			if status := enginePut(t, put.URL, blob); status != http.StatusOK {
				t.Fatalf("PUT: status %d", status)
			}
			uploaded := time.Since(start)
			p, err := s.Promote(context.Background(), stage, d, int64(size))
			if err != nil || p.Outcome != Promoted || p.HashedBy != mode.hashedBy {
				t.Fatalf("promote: %+v, %v", p, err)
			}
			t.Logf("hashed by the %s: %8.1f MiB uploaded in %v, promoted in %v (%.0f MiB/s)",
				mode.name, float64(size)/(1<<20), uploaded.Round(time.Millisecond), p.Duration.Round(time.Millisecond),
				float64(size)/(1<<20)/p.Duration.Seconds())
		}
	}
}

// Known gap, pinned on purpose (#23's open question 3; see the README, "Blob
// store"): an object stored at a digest key before uploads were verified is
// trusted as it is. Has reports it, UploadURLs never asks for it, and a
// correct upload's promotion finds it Existing and leaves it. The service
// never re-verifies or deletes a digest key on its own, nor because an
// engine reported a failure; repairing such objects is the one-off job in
// the README. When the service re-verifies stored blobs, this test fails:
// replace it with one of the repair.
func TestMinIOStoreKnownGapUnverifiedOldObjects(t *testing.T) {
	s := newMinIOStore(t, nil)
	ctx := context.Background()
	blob, d := randomBlob(t, 4096)
	wrong := bytes.Clone(blob)
	wrong[0] ^= 0xff
	key, _ := s.Key(d)
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(wrong)}); err != nil {
		t.Fatal(err)
	}
	if !mustHas(t, s, d) {
		t.Fatal("the old object isn't reported; replace this test")
	}
	if urls, err := UploadURLs(ctx, s, NewStage(), []Blob{{Digest: d, Size: int64(len(blob))}}); err != nil || len(urls) != 0 {
		t.Fatalf("UploadURLs asks for the blob again (%v, %v); replace this test", urls, err)
	}
	if p := stageAndPromote(t, s, d, int64(len(blob)), blob); p.Outcome != Existing {
		t.Fatalf("a correct upload's promotion: %+v; replace this test", p)
	}
	get, _ := s.PresignGet(ctx, d)
	if status, data := httpGet(t, get.URL); status != http.StatusOK || !bytes.Equal(data, wrong) {
		t.Fatalf("GET: status %d, still the old content %v; replace this test", status, bytes.Equal(data, wrong))
	}
}
