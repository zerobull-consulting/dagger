package gc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
)

// state is everything under the env's prefix: every object with its size
// and ETag, and every incomplete multipart upload. MinIO lists uploads only
// by exact key (FilterAllUploads), so they are listed through GC's own
// bucket, which falls back to the bucket's whole upload listing.
func (e *minioEnv) state() []string {
	e.t.Helper()
	ctx := context.Background()
	var out []string
	p := s3.NewListObjectsV2Paginator(e.client, &s3.ListObjectsV2Input{Bucket: &e.bucket, Prefix: aws.String(e.prefix + "/")})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			e.t.Fatal(err)
		}
		for _, o := range page.Contents {
			out = append(out, fmt.Sprintf("%s %d %s", aws.ToString(o.Key), aws.ToInt64(o.Size), aws.ToString(o.ETag)))
		}
	}
	ups, err := e.gcb.ListUploads(ctx, e.prefix+"/")
	if err != nil {
		e.t.Fatal(err)
	}
	for _, u := range ups {
		out = append(out, "upload "+u.Key+" "+u.ID)
	}
	slices.Sort(out)
	return out
}

// upload starts a real incomplete multipart upload at key, relative to the
// prefix, and returns its ID.
func (e *minioEnv) upload(key string) string {
	e.t.Helper()
	up, err := e.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: &e.bucket, Key: aws.String(e.prefix + "/" + key)})
	if err != nil {
		e.t.Fatal(err)
	}
	id := aws.ToString(up.UploadId)
	e.t.Cleanup(func() {
		_, _ = e.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: &e.bucket, Key: aws.String(e.prefix + "/" + key), UploadId: &id})
	})
	return id
}

// collectable gives a namespace something every step of a run deletes or
// aborts: an orphan blob, a staged upload and a promotion's upload, all
// past their ages on the collector's clock (3h ahead).
func (e *minioEnv) collectable(ns string) {
	e.t.Helper()
	e.export(ns, "c-"+ns, buildX(ns), 2)
	orphan := []byte("orphan-" + ns)
	e.put("ns/"+ns+"/"+layout.BlobRel(digest.FromBytes(orphan)), orphan)
	e.put("ns/"+ns+"/"+layout.StagingRel(string(blobstore.NewStage()), digest.FromBytes(orphan)), orphan)
	e.upload("ns/" + ns + "/" + layout.BlobRel(digest.FromString("promotion-"+ns)))
}

func (e *minioEnv) collector() *Collector {
	e.t.Helper()
	c, err := New(Config{Bucket: e.gcb, Meta: e.meta, Now: NewNow(time.Now().Add(3 * time.Hour)).Now})
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

func under(state []string, sub string) []string {
	var out []string
	for _, s := range state {
		if strings.Contains(s, sub) {
			out = append(out, s)
		}
	}
	return out
}

// Against a real bucket: a key or an upload of no known shape in a
// namespace leaves that namespace exactly as it was, uploads included, and
// the other namespace is collected; one outside the namespaces leaves the
// whole prefix exactly as it was; and so does a prefix without its marker.
// Each case compares full listings, objects and uploads, before and after.
func TestMinIOGCOwnership(t *testing.T) {
	ctx := context.Background()
	hex := digest.FromString("x").Encoded()
	for _, tc := range []struct {
		name string
		// foreign adds what isn't the service's.
		foreign func(e *minioEnv)
		// root: the whole run does nothing.
		root bool
	}{
		{name: "a key in a namespace", foreign: func(e *minioEnv) { e.put("ns/team-a/notes.txt", []byte("x")) }},
		{name: "an upload in a namespace", foreign: func(e *minioEnv) { e.upload("ns/team-a/backups/db.tar") }},
		{name: "a staged key as an upload", foreign: func(e *minioEnv) {
			e.upload("ns/team-a/" + layout.StagingRel(string(blobstore.NewStage()), digest.FromString("y")))
		}},
		{name: "another application's key", root: true, foreign: func(e *minioEnv) { e.put("reports/2026.csv", []byte("x")) }},
		{name: "the old layout", root: true, foreign: func(e *minioEnv) {
			e.put("default/index.json", []byte("{}"))
			e.put("blobs/sha256/"+hex, []byte("x"))
		}},
		{name: "an upload outside the namespaces", root: true, foreign: func(e *minioEnv) { e.upload("backups/db.tar") }},
		{name: "no marker", root: true, foreign: func(e *minioEnv) {
			if err := e.meta.Objects().Delete(ctx, layout.MarkerKey(e.prefix)); err != nil {
				e.t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newMinIOEnv(t)
			e.collectable("team-a")
			e.collectable("team-b")
			tc.foreign(e)
			before := e.state()
			for _, dry := range []bool{true, false} {
				c := e.collector()
				if dry {
					c = c.DryRunner()
				}
				reps, err := c.Run(ctx)
				if err == nil {
					t.Fatalf("dry run %v: no error", dry)
				}
				if tc.root {
					if !errors.Is(err, ErrNotOwned) {
						t.Fatalf("dry run %v: %v", dry, err)
					}
					continue
				}
				if r := reportOf(reps, "team-a"); r.UnexpectedCount != 1 || len(r.Deleted) != 0 || len(r.WouldDelete) != 0 {
					t.Fatalf("dry run %v, team-a: %+v", dry, r)
				}
				b := reportOf(reps, "team-b")
				if dry && len(b.WouldDelete) != 3 {
					t.Fatalf("dry run, team-b: %+v", b)
				}
				if !dry && (b.Deleted["blob"] != 1 || b.Deleted["staging"] != 1 || b.Deleted["upload"] != 1) {
					t.Fatalf("team-b: %+v", b)
				}
			}
			after := e.state()
			if tc.root {
				if !slices.Equal(before, after) {
					t.Fatalf("the prefix changed:\nbefore:\n  %s\nafter:\n  %s", strings.Join(before, "\n  "), strings.Join(after, "\n  "))
				}
				return
			}
			if a, b := under(before, "/ns/team-a/"), under(after, "/ns/team-a/"); !slices.Equal(a, b) {
				t.Fatalf("team-a changed:\nbefore:\n  %s\nafter:\n  %s", strings.Join(a, "\n  "), strings.Join(b, "\n  "))
			}
			// team-b lost its orphan blob, staged upload and promotion's
			// upload, and gained its epoch object.
			a, b := under(before, "/ns/team-b/"), under(after, "/ns/team-b/")
			if len(b) != len(a)-2 || len(under(b, "/ns/team-b/gc-epoch.json ")) != 1 {
				t.Fatalf("team-b:\nbefore:\n  %s\nafter:\n  %s", strings.Join(a, "\n  "), strings.Join(b, "\n  "))
			}
		})
	}
}

// Against a real bucket: a namespace without an index is left exactly as it
// was, staged and multipart uploads included.
func TestMinIOGCNamespaceWithoutIndex(t *testing.T) {
	e := newMinIOEnv(t)
	e.collectable("team")
	d := digest.FromString("fresh")
	e.put("ns/fresh/"+layout.BlobRel(d), []byte("fresh"))
	e.put("ns/fresh/"+layout.StagingRel(string(blobstore.NewStage()), d), []byte("fresh"))
	e.upload("ns/fresh/" + layout.BlobRel(digest.FromString("p")))
	before := under(e.state(), "/ns/fresh/")
	reps, err := e.collector().Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r := reportOf(reps, "fresh"); r.Skipped != "no index" || len(r.Deleted) != 0 {
		t.Fatalf("fresh: %+v", r)
	}
	if r := reportOf(reps, "team"); r.Deleted["upload"] != 1 {
		t.Fatalf("team: %+v", r)
	}
	if after := under(e.state(), "/ns/fresh/"); !slices.Equal(before, after) {
		t.Fatalf("fresh changed:\n%v\n%v", before, after)
	}
}
