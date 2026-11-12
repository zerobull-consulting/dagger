package layout

import (
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
)

var (
	d     = digest.FromString("layer")
	hex   = d.Encoded()
	id    = strings.Repeat("ab", 32)
	stage = strings.Repeat("0f", 16)
)

// Every shape the writers produce is recognized, through the same functions
// they build keys with; near misses are not.
func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		rel  string
		want Kind
	}{
		{IndexName, KindIndex},
		{EpochName, KindEpoch},
		{BundleRel(id), KindBundle},
		{ResolutionRel(id), KindResolution},
		{BlobRel(d), KindBlob},
		{BlobRel(digest.SHA512.FromString("x")), KindBlob},
		{StagingRel(stage, d), KindStaging},
		{ProbeRel(0xfeedbeef), KindProbe},

		// Near misses: not the service's.
		{"index.json.bak", KindUnknown},
		{"Index.json", KindUnknown},
		{"bundles/" + id, KindUnknown},
		{"bundles/" + strings.ToUpper(id) + ".json", KindUnknown},
		{"bundles/x/" + id + ".json", KindUnknown},
		{"resolutions/" + id[:62] + ".json", KindUnknown},
		{"blobs/sha256/" + hex[:63], KindUnknown},
		{"blobs/sha256/" + strings.ToUpper(hex), KindUnknown},
		{"blobs/md5/d41d8cd98f00b204e9800998ecf8427e", KindUnknown},
		{"blobs/sha256/" + hex + "/x", KindUnknown},
		{"blobs/sha256", KindUnknown},
		{"blobs/", KindUnknown},
		{"staging/" + stage + "/sha256/" + hex[:10], KindUnknown},
		{"staging/" + stage[:30] + "/sha256/" + hex, KindUnknown},
		{"staging/" + stage + "/x", KindUnknown},
		{"staging/" + stage + "/", KindUnknown},
		{"conditional-write-check/123", KindUnknown},
		{"gc-epoch.json.tmp", KindUnknown},
		{"other.txt", KindUnknown},
		{"", KindUnknown},
	} {
		if got, _ := Classify(tc.rel); got != tc.want {
			t.Errorf("Classify(%q) = %q, want %q", tc.rel, got, tc.want)
		}
	}
	if _, got := Classify(BundleRel(id)); got != id {
		t.Errorf("bundle ID %q", got)
	}
}

// Only blob and staging keys may hold incomplete multipart uploads:
// promotions start them, at the digest key, and at the staging key of a
// large service-hashed blob's private copy.
func TestIsUploadRel(t *testing.T) {
	if !IsUploadRel(BlobRel(d)) {
		t.Error("blob key refused")
	}
	if !IsUploadRel(StagingRel(stage, d)) {
		t.Error("staging key refused")
	}
	for _, rel := range []string{StagingRel(stage, d) + "/x", "staging/" + stage, IndexName, BundleRel(id), "backups/db.tar"} {
		if IsUploadRel(rel) {
			t.Errorf("upload at %q accepted", rel)
		}
	}
}

func TestLocate(t *testing.T) {
	for _, tc := range []struct {
		prefix, key string
		place       Place
		ns, rel     string
	}{
		{"", "cacheservice.json", Marker, "", ""},
		{"p", "p/cacheservice.json", Marker, "", ""},
		{"/p/q/", "p/q/cacheservice.json", Marker, "", ""},
		{"p", "p/ns/team-a/index.json", InNamespace, "team-a", "index.json"},
		{"", "ns/team-a/blobs/sha256/x", InNamespace, "team-a", "blobs/sha256/x"},
		{"p", "p/ns/blobs/index.json", InNamespace, "blobs", "index.json"},
		// Outside a namespace: not the service's.
		{"p", "p/default/index.json", Outside, "", ""},
		{"p", "p/blobs/sha256/" + hex, Outside, "", ""},
		{"p", "p/ns/x", Outside, "", ""},
		{"p", "p/ns/team-a/", Outside, "", ""},
		{"p", "p/ns/-bad/index.json", Outside, "", ""},
		{"p", "p/ns/a..b/index.json", Outside, "", ""},
		{"p", "p/cacheservice.json.old", Outside, "", ""},
		{"", "backups/db.tar", Outside, "", ""},
		// Not under the prefix at all.
		{"p", "q/ns/a/index.json", NotUnder, "", ""},
		{"p", "pp/cacheservice.json", NotUnder, "", ""},
	} {
		place, ns, rel := Locate(tc.prefix, tc.key)
		if place != tc.place || ns != tc.ns || rel != tc.rel {
			t.Errorf("Locate(%q, %q) = %v %q %q, want %v %q %q", tc.prefix, tc.key, place, ns, rel, tc.place, tc.ns, tc.rel)
		}
	}
}

func TestKnown(t *testing.T) {
	ns, err := NamespacePrefix("p", "team-a")
	if err != nil {
		t.Fatal(err)
	}
	if ns != "p/ns/team-a/" {
		t.Fatalf("namespace prefix %q", ns)
	}
	for _, k := range []string{MarkerKey("p"), ns + IndexName, ns + BlobRel(d), ns + StagingRel(stage, d), ns + EpochName} {
		if !Known("p", k) {
			t.Errorf("%q not known", k)
		}
	}
	for _, k := range []string{"p/x", ns + "x", "p/ns/team-a", "p/default/index.json"} {
		if Known("p", k) {
			t.Errorf("%q known", k)
		}
	}
	if _, err := NamespacePrefix("p", "a/b"); err == nil {
		t.Error("namespace a/b accepted")
	}
}

func TestParse(t *testing.T) {
	if got, ok := ParseBlobRel(BlobRel(d)); !ok || got != d {
		t.Errorf("ParseBlobRel: %s %v", got, ok)
	}
	if s, got, ok := ParseStagingRel(StagingRel(stage, d)); !ok || got != d || s != stage {
		t.Errorf("ParseStagingRel: %s %s %v", s, got, ok)
	}
	if _, ok := ParseBlobRel(StagingRel(stage, d)); ok {
		t.Error("staged key parsed as a blob")
	}
}
