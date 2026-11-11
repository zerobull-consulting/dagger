package blobstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/opencontainers/go-digest"
)

const (
	// maxCopyParts is S3's limit on the parts of a multipart upload.
	maxCopyParts = 10000
	// cleanupTimeout bounds a staged object's delete and an upload's abort,
	// which run even after the promotion's context ends.
	cleanupTimeout = 30 * time.Second
)

// Promote verifies the blob staged under stage and copies it to its digest
// key if it matches. See Store.Promote.
//
// The copy is a multipart upload whose CompleteMultipartUpload carries
// If-None-Match: *, so the digest key appears only once the content is
// verified, and a blob already there is never overwritten: when two exports
// race, the first verified copy wins and the other finds it Existing. (MinIO
// ignores If-None-Match on CopyObject, but honors it on
// CompleteMultipartUpload; TestMinIOConditionalPromotion.)
//
// Who hashes:
//
//   - A SHA-256 blob of up to 5 GiB is copied as one part with the upload's
//     checksum algorithm SHA-256: the store hashes the bytes as it copies
//     them, and returns the part's checksum, which for one part is the
//     whole blob's. A mismatch aborts the upload before it completes, so
//     nothing is written to the digest key. No byte passes through the
//     service.
//   - A larger blob is copied in parts (UploadPartCopy's limit is 5 GiB),
//     whose checksums are per part and can't be combined into the blob's.
//     So is a blob of another algorithm, every blob with
//     S3Config.HashInService, and every blob once the store has returned a
//     part copy without a checksum. The service first copies the staged
//     object to a private key, a staging key under a stage of its own that
//     no upload URL reaches, then reads that copy and hashes it, and copies
//     it to the digest key. So what reaches the digest key is what the
//     service hashed, even if the staged object is overwritten meanwhile by
//     someone who holds its presigned PUT: no store condition
//     (CopySourceIfMatch, which DigitalOcean Spaces ignores) and no ETag
//     (an MD5, or not even that with SSE-KMS) is relied on
//     (zerobull-consulting/dagger#67). It costs one more copy in the store.
func (s *S3Store) Promote(ctx context.Context, stage Stage, dgst digest.Digest, size int64) (Promotion, error) {
	start := time.Now()
	p, err := s.promote(ctx, stage, dgst, size)
	p.Duration = time.Since(start)
	return p, err
}

func (s *S3Store) promote(ctx context.Context, stage Stage, dgst digest.Digest, size int64) (Promotion, error) {
	if size <= 0 {
		return Promotion{}, fmt.Errorf("promote %s: size %d: %w", dgst, size, ErrNotPositiveSize)
	}
	src, err := s.StagingKey(stage, dgst)
	if err != nil {
		return Promotion{}, err
	}
	dst, err := s.Key(dgst)
	if err != nil {
		return Promotion{}, err
	}
	select {
	case s.promotions <- struct{}{}:
	case <-ctx.Done():
		return Promotion{}, context.Cause(ctx)
	}
	defer func() { <-s.promotions }()

	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &src})
	if isNotFound(err) {
		return Promotion{Outcome: Missing}, nil
	}
	if err != nil {
		return Promotion{}, fmt.Errorf("head %s: %w", src, err)
	}
	// decided deletes the staged object, which nothing reads any more.
	// A failed delete leaves it to the lifecycle rule.
	decided := func(p Promotion) (Promotion, error) {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_, _ = s.client.DeleteObject(dctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &src})
		return p, nil
	}
	has, err := s.Has(ctx, dgst)
	if err != nil {
		return Promotion{}, err
	}
	if has {
		return decided(Promotion{Outcome: Existing})
	}
	if aws.ToInt64(head.ContentLength) != size {
		return decided(Promotion{Outcome: Mismatch})
	}
	etag := aws.ToString(head.ETag)

	if dgst.Algorithm() == digest.SHA256 && size <= s.maxPartCopy && !s.hashInService && !s.noStoreHash.Load() {
		p, hashed, err := s.promoteStoreHashed(ctx, src, dst, etag, dgst, size)
		if err != nil {
			return Promotion{}, err
		}
		if hashed {
			return decided(p)
		}
		// The store copied without a checksum: it doesn't hash. Hash in
		// the service, from now on too.
		s.noStoreHash.Store(true)
	}
	p, err := s.promoteServiceHashed(ctx, src, dst, etag, dgst, size)
	if err != nil {
		return Promotion{}, err
	}
	return decided(p)
}

// promoteStoreHashed copies src to dst as one part, and completes the copy
// only if the store's SHA-256 of the part is dgst. hashed is false when the
// store returned no checksum; nothing was written then.
//
// The store hashes the very bytes it copies, so an overwrite of src can't
// slip between the hash and the copy; the condition on etag only spares a
// copy of an object that changed since the HEAD.
func (s *S3Store) promoteStoreHashed(ctx context.Context, src, dst, etag string, dgst digest.Digest, size int64) (_ Promotion, hashed bool, _ error) {
	up, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            &s.bucket,
		Key:               &dst,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return Promotion{}, false, fmt.Errorf("start copy to %s: %w", dst, err)
	}
	abort := s.aborter(ctx, dst, up.UploadId)
	part, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:            &s.bucket,
		Key:               &dst,
		UploadId:          up.UploadId,
		PartNumber:        aws.Int32(1),
		CopySource:        aws.String(copySource(s.bucket, src)),
		CopySourceIfMatch: aws.String(etag),
	})
	if err != nil {
		abort()
		return Promotion{}, false, fmt.Errorf("copy %s: %w", src, err)
	}
	if part.CopyPartResult == nil {
		// A part copy the store answered without its result: no ETag to
		// complete with, and no checksum. That is a malformed answer, not
		// a store that doesn't hash, so it fails this promotion rather
		// than switch every later one to hashing in the service.
		abort()
		return Promotion{}, false, fmt.Errorf("copy %s: %w", src, errNoCopyResult)
	}
	sum := aws.ToString(part.CopyPartResult.ChecksumSHA256)
	if sum == "" {
		abort()
		return Promotion{}, false, nil
	}
	got, err := base64.StdEncoding.DecodeString(sum)
	if err != nil || len(got) != 32 {
		abort()
		return Promotion{}, false, fmt.Errorf("copy %s: the store returned checksum %q, not a SHA-256", src, sum)
	}
	p := Promotion{HashedBy: HashedByStore, Bytes: size}
	want, _ := hex.DecodeString(dgst.Encoded())
	if !bytes.Equal(got, want) {
		abort()
		p.Outcome = Mismatch
		return p, true, nil
	}
	p.Outcome, err = s.complete(ctx, dst, up.UploadId, []types.CompletedPart{{
		PartNumber:     aws.Int32(1),
		ETag:           part.CopyPartResult.ETag,
		ChecksumSHA256: aws.String(sum),
	}}, dgst)
	return p, true, err
}

// promoteServiceHashed copies src to a private key, hashes that copy, and,
// if it is dgst, copies it to dst in parts.
//
// The private key is a staging key under a stage of its own, new for this
// promotion and never presigned: no upload URL reaches it, so what the
// service hashes is what it then copies, whatever happens to src meanwhile
// (zerobull-consulting/dagger#67). The private copy is deleted once the
// promotion is over; one left by a service stopped midway is a staged
// object like any other, which the lifecycle rule or GC expires.
//
// The copy to the private key is conditioned on the ETag the service read
// (CopySourceIfMatch), so a store that honors it fails early when src was
// overwritten since the HEAD; nothing relies on it.
func (s *S3Store) promoteServiceHashed(ctx context.Context, src, dst, etag string, dgst digest.Digest, size int64) (Promotion, error) {
	priv, err := s.StagingKey(NewStage(), dgst)
	if err != nil {
		return Promotion{}, err
	}
	// Deleted even when the copy to it failed: a single copy may have
	// landed before its error.
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_, _ = s.client.DeleteObject(dctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &priv})
	}()
	if err := s.copyPrivate(ctx, src, priv, etag, size); err != nil {
		return Promotion{}, err
	}

	obj, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &priv})
	if err != nil {
		return Promotion{}, fmt.Errorf("read %s: %w", priv, err)
	}
	h := dgst.Algorithm().Hash()
	n, err := io.Copy(h, obj.Body)
	_ = obj.Body.Close()
	p := Promotion{HashedBy: HashedByService, Bytes: n}
	if err != nil {
		return p, fmt.Errorf("read %s: %w", priv, err)
	}
	if n != size || digest.NewDigest(dgst.Algorithm(), h) != dgst {
		p.Outcome = Mismatch
		return p, nil
	}

	up, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.bucket, Key: &dst})
	if err != nil {
		return p, fmt.Errorf("start copy to %s: %w", dst, err)
	}
	parts, err := s.copyParts(ctx, priv, dst, "", up.UploadId, size)
	if err != nil {
		s.aborter(ctx, dst, up.UploadId)()
		return p, err
	}
	p.Outcome, err = s.complete(ctx, dst, up.UploadId, parts, dgst)
	return p, err
}

// copyPrivate copies src, of size bytes, to the private key priv: with one
// CopyObject up to the largest part copy (CopyObject's limit is the same
// 5 GiB), else as a multipart upload of part copies. Every copy is
// conditioned on etag.
func (s *S3Store) copyPrivate(ctx context.Context, src, priv, etag string, size int64) error {
	if size <= s.maxPartCopy {
		_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            &s.bucket,
			Key:               &priv,
			CopySource:        aws.String(copySource(s.bucket, src)),
			CopySourceIfMatch: aws.String(etag),
		})
		if err != nil {
			return fmt.Errorf("copy %s: %w", src, err)
		}
		return nil
	}
	up, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.bucket, Key: &priv})
	if err != nil {
		return fmt.Errorf("start copy to %s: %w", priv, err)
	}
	abort := s.aborter(ctx, priv, up.UploadId)
	parts, err := s.copyParts(ctx, src, priv, etag, up.UploadId, size)
	if err != nil {
		abort()
		return err
	}
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &s.bucket,
		Key:             &priv,
		UploadId:        up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		abort()
		return fmt.Errorf("complete copy to %s: %w", priv, err)
	}
	return nil
}

// copyParts copies src, of size bytes, into the multipart upload uploadID
// at dst: as one part up to the largest part copy, else in parts of
// copyPartSize (larger when that would make over S3's 10,000 parts). Each
// part copy is conditioned on etag, unless it is "". The caller aborts the
// upload on an error.
func (s *S3Store) copyParts(ctx context.Context, src, dst, etag string, uploadID *string, size int64) ([]types.CompletedPart, error) {
	partSize := s.copyPartSize
	if size <= s.maxPartCopy {
		partSize = size
	} else if least := (size + maxCopyParts - 1) / maxCopyParts; partSize < least {
		partSize = least
	}
	var parts []types.CompletedPart
	for off, num := int64(0), int32(1); off < size; off, num = off+partSize, num+1 {
		in := &s3.UploadPartCopyInput{
			Bucket:     &s.bucket,
			Key:        &dst,
			UploadId:   uploadID,
			PartNumber: aws.Int32(num),
			CopySource: aws.String(copySource(s.bucket, src)),
		}
		if etag != "" {
			in.CopySourceIfMatch = aws.String(etag)
		}
		if partSize < size {
			end := min(off+partSize, size) - 1
			in.CopySourceRange = aws.String(fmt.Sprintf("bytes=%d-%d", off, end))
		}
		part, err := s.client.UploadPartCopy(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("copy %s, part %d: %w", src, num, err)
		}
		if part.CopyPartResult == nil {
			return nil, fmt.Errorf("copy %s, part %d: %w", src, num, errNoCopyResult)
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(num), ETag: part.CopyPartResult.ETag})
	}
	return parts, nil
}

// errNoCopyResult is an UploadPartCopy answered without a CopyPartResult:
// the part's ETag, which completing the upload needs, is missing.
var errNoCopyResult = errors.New("the store answered the part copy without a CopyPartResult")

// complete completes a copy to dst without overwriting a blob already there:
// Promoted, or Existing when another copy completed first. Any failure
// aborts the upload.
func (s *S3Store) complete(ctx context.Context, dst string, uploadID *string, parts []types.CompletedPart, dgst digest.Digest) (Outcome, error) {
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &s.bucket,
		Key:             &dst,
		UploadId:        uploadID,
		IfNoneMatch:     aws.String("*"),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err == nil {
		return Promoted, nil
	}
	s.aborter(ctx, dst, uploadID)()
	if isConditionFailed(err) {
		if has, herr := s.Has(ctx, dgst); herr == nil && has {
			return Existing, nil
		}
	}
	return "", fmt.Errorf("complete copy to %s: %w", dst, err)
}

// aborter returns a function that aborts a multipart upload, even after ctx
// ends. An abort that fails leaves the upload's parts to the bucket's
// lifecycle rule (AbortIncompleteMultipartUpload).
func (s *S3Store) aborter(ctx context.Context, key string, uploadID *string) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		_, _ = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &s.bucket, Key: &key, UploadId: uploadID})
	}
}

// copySource is the x-amz-copy-source of a key: bucket/key, URL-encoded by
// segment.
func copySource(bucket, key string) string {
	segs := strings.Split(bucket+"/"+key, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	return strings.Join(segs, "/")
}
