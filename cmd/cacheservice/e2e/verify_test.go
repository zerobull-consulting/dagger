package e2e

// The verify scenario (#23): uploads are verified before they reach the
// shared cache.

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencontainers/go-digest"
)

// verify: the S3 proxy rewrites every blob A uploads to other content of the
// same length, as a buggy or compromised engine would send, while A runs a
// new pipeline. S3 takes each upload (a presigned PUT bounds only the
// size), but at a staging key; the service's verification finds the
// mismatch, so nothing reaches the digest keys and the bundle isn't
// published. Once the rewrite is lifted, the tracker's retry asks for the
// rejected blobs again, under a new stage, and stores the right ones; B then
// gets the pipeline as a cache hit.
func (s *state) verify(t *testing.T) {
	env, a := s.env, s.a
	cacheA := a.CacheID()
	logMark, s3Mark := s.main.Logs.Mark(), env.Proxy.Mark()
	corrupt := env.Proxy.Inject(&Fault{Name: "corrupt-uploads", Match: Methods("/staging/", http.MethodPut), Rewrite: SameSizeCorruption})
	t.Cleanup(func() { env.Proxy.Remove(corrupt) })
	rejectedOf := func() []LogRecord {
		return s.main.Logs.Since(logMark, "export: uploaded blob not stored; asking for it again", func(r LogRecord) bool { return r.Attr("cacheID") == cacheA })
	}

	query := faultQuery(Nonce())
	res, _ := a.Query("verify-run-a", query)
	want := strings.TrimSpace(Field(res, faultPath))
	failures := 0
	env.WaitFor(env.Settings.Timeout, "A's export to fail on its corrupted uploads", func() (bool, error) {
		entries, err := s.main.Entries(a.Conn)
		if err != nil {
			return false, err
		}
		for _, e := range entries {
			failures = max(failures, e.Failures)
		}
		return failures > 0, nil
	})
	rewritten := map[digest.Digest]bool{}
	for _, r := range env.Proxy.Since(s3Mark) {
		if r.Rewritten && r.Status/100 == 2 {
			rewritten[blobDigest(r)] = true
		}
	}
	rejected := map[digest.Digest]bool{}
	outcomes := map[string]int{}
	for _, r := range rejectedOf() {
		rejected[digest.Digest(r.Attr("digest"))] = true
		outcomes[r.Attr("outcome")]++
	}
	var atDigestKey []string
	for d := range rewritten {
		if env.HasKey(env.BlobKey(d)) {
			atDigestKey = append(atDigestKey, Short(d.String()))
		}
	}
	storedWhileCorrupt := len(storedSince(s.main, logMark, cacheA))
	env.Proxy.Remove(corrupt)
	t.Logf("uploads are no longer rewritten, after %d rewritten PUTs of %d blobs", env.Proxy.Hits(corrupt), len(rewritten))

	Check(t, len(rewritten) > 0 && outcomes["mismatch"] > 0 && len(outcomes) == 1,
		"S3 took each rewritten upload (%d blobs), and the service rejected them as mismatches (%v)", len(rewritten), outcomes)
	Check(t, len(atDigestKey) == 0 && mapsEqual(rejected, rewritten),
		"no rewritten blob reached its digest key (at it: %v), and every one was rejected (%d of %d)", atDigestKey, len(rejected), len(rewritten))
	Check(t, storedWhileCorrupt == 0 && failures > 0,
		"nothing of A's was published while its uploads were corrupted: the export failed (%d failure(s) counted)", failures)

	storeMark := env.Proxy.Mark()
	env.WaitFor(env.Settings.Timeout, "A's export to be stored once uploads are intact", func() (bool, error) {
		return len(storedSince(s.main, logMark, cacheA)) > 0, nil
	})
	s.waitStored(s.main, a, "all of A's retained entries to be stored")
	stored := storedSince(s.main, logMark, cacheA)
	var reuploaded, wrong []string
	puts := env.Proxy.Blobs(storeMark, http.MethodPut)
	for d := range rewritten {
		if blobsOf(puts, d, func(r S3Request) bool { return r.Status/100 == 2 && !r.Rewritten }) {
			reuploaded = append(reuploaded, Short(d.String()))
		}
		if got := env.StoredDigest(d); got != d {
			wrong = append(wrong, Short(d.String())+" holds "+Short(got.String()))
		}
	}
	Check(t, len(reuploaded) == len(rewritten) && len(wrong) == 0,
		"the retry asked for each rejected blob again and stored the right content (%d of %d uploaded again; wrong: %v)", len(reuploaded), len(rewritten), wrong)
	Check(t, len(stored) > 0, "A's export was then stored (bundles %v)", shorts(bundleIDs(stored)))
	env.CheckConsistent(t, "after the corrupted uploads")
	// Every staged upload was decided on, and its staged object deleted;
	// one left over (by an upload never reported) would be the lifecycle
	// rule's to expire, and never read.
	t.Logf("staged uploads left in the scope: %d", len(env.StagedKeys()))
	s.checkServing(t, s.main, s.b, bundleIDs(stored), query, want, "verify-run-b")
}

func mapsEqual(a, b map[digest.Digest]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// StoredDigest reads the object at a blob's digest key in the run's scope
// from MinIO, and returns the digest of its content ("" when it is missing).
func (e *Env) StoredDigest(d digest.Digest) digest.Digest {
	e.T.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	out, err := e.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(e.S3Config.Bucket), Key: aws.String(e.BlobKey(d))})
	if err != nil {
		return ""
	}
	defer out.Body.Close()
	h := d.Algorithm().Digester()
	if _, err := io.Copy(h.Hash(), out.Body); err != nil {
		e.T.Fatalf("read %s: %v", e.BlobKey(d), err)
	}
	return h.Digest()
}

// StagedKeys lists the staged uploads of the run's scope: what the bucket's
// lifecycle rule expires.
func (e *Env) StagedKeys() []string {
	e.T.Helper()
	return e.listKeys(e.StagingPrefixIn(e.Scope))
}
