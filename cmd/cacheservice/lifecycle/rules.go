// Package lifecycle is the cacheservice lifecycle command: it generates the
// bucket lifecycle rules the service's prefix wants (one expiration rule per
// namespace on its staging prefix, and one rule aborting incomplete multipart
// uploads under the prefix), merges them into the bucket's configuration, and
// checks a bucket against them.
//
//	cacheservice lifecycle plan  [-tokens-file …] [-scope …]
//	cacheservice lifecycle claim -confirm=<bucket>/<prefix>
//	cacheservice lifecycle apply [-tokens-file …] [-scope …] -confirm=<bucket>/<prefix>
//	cacheservice lifecycle check [-tokens-file …] [-scope …]
//
// It is a one-shot command, run by an operator with the bucket's admin key
// (on DigitalOcean Spaces, a full-access key) when a bucket is set up or its
// namespaces change. The service itself never writes lifecycle rules: it
// only reads them, once per GC run (gc.S3Bucket.ExpirationPrefixes).
//
// # Which rules it owns
//
// PutBucketLifecycleConfiguration replaces a bucket's whole configuration, so
// apply always reads it, replaces only the rules it owns, and writes back
// every other rule as it read it. A rule is the command's when its ID starts
// with OwnedIDPrefix ("cacheservice-") and the prefix's hash (below), and its
// filter's prefix is under the service's prefix (see Owned): so two services
// on two prefixes of one bucket each own their own rules, and rules of other
// tools are never touched.
//
// # Rule IDs
//
// An ID is OwnedIDPrefix, then 8 hex digits of the SHA-256 of the service's
// prefix (its layout.Root), then "-staging-<namespace>" or
// "-incomplete-uploads". The hash keeps IDs unique between prefixes of one
// bucket, as S3 requires, whatever characters the prefix has. A namespace is
// validated (layout.ValidateNamespace: at most 128 letters, digits, '.', '_'
// or '-'), so an ID is at most 158 characters of those, under S3's 255; every
// generated ID is checked against that again before it is written.
package lifecycle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dagger/dagger/cmd/cacheservice/layout"
)

const (
	// OwnedIDPrefix starts the ID of every rule the command owns.
	OwnedIDPrefix = "cacheservice-"
	// StagingDays is how long a staged upload stays before its namespace's
	// rule expires it: an export's uploads and promotions finish within
	// its upload timeout (1h).
	StagingDays = 1
	// AbortDays is how long an incomplete multipart upload stays before
	// the prefix's rule aborts it.
	AbortDays = 1
	// MaxRules is the most rules a bucket's configuration may hold: AWS
	// S3's limit. apply refuses a result over it.
	MaxRules = 1000
	// MaxIDLength is S3's limit on a rule's ID.
	MaxIDLength = 255
)

// idPattern is what a generated ID may be.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// prefixTag is the part of a rule ID that names the service's prefix.
func prefixTag(prefix string) string {
	sum := sha256.Sum256([]byte(layout.Root(prefix)))
	return hex.EncodeToString(sum[:4])
}

// StagingID is the ID of a namespace's staging rule.
func StagingID(prefix, ns string) string {
	return OwnedIDPrefix + prefixTag(prefix) + "-staging-" + ns
}

// AbortID is the ID of the prefix's rule that aborts incomplete multipart
// uploads.
func AbortID(prefix string) string {
	return OwnedIDPrefix + prefixTag(prefix) + "-incomplete-uploads"
}

// StagingPrefix is a namespace's staging prefix: <prefix>/ns/<ns>/staging/.
func StagingPrefix(prefix, ns string) (string, error) {
	p, err := layout.NamespacePrefix(prefix, ns)
	if err != nil {
		return "", err
	}
	return p + layout.StagingDir, nil
}

// Rules are the rules the prefix wants for namespaces: one expiration rule per
// namespace on its staging prefix, after StagingDays, in the order given, and,
// with abort, one rule on the prefix aborting incomplete multipart uploads
// after AbortDays. These are the rules the README's JSON example shows.
func Rules(prefix string, namespaces []string, abort bool) ([]types.LifecycleRule, error) {
	var out []types.LifecycleRule
	seen := map[string]bool{}
	for _, ns := range namespaces {
		if seen[ns] {
			continue
		}
		seen[ns] = true
		sp, err := StagingPrefix(prefix, ns)
		if err != nil {
			return nil, err
		}
		out = append(out, types.LifecycleRule{
			ID:         aws.String(StagingID(prefix, ns)),
			Status:     types.ExpirationStatusEnabled,
			Filter:     &types.LifecycleRuleFilter{Prefix: aws.String(sp)},
			Expiration: &types.LifecycleExpiration{Days: aws.Int32(StagingDays)},
		})
	}
	if abort {
		out = append(out, types.LifecycleRule{
			ID:                             aws.String(AbortID(prefix)),
			Status:                         types.ExpirationStatusEnabled,
			Filter:                         &types.LifecycleRuleFilter{Prefix: aws.String(layout.Root(prefix))},
			AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(AbortDays)},
		})
	}
	return out, nil
}

// filterPrefix is the prefix of a rule's filter, whatever else it filters by,
// and whether it filters by anything else too. Unlike gc.rulePrefix, which
// counts only a rule filtered by prefix alone (one that expires every object
// under it), it returns the prefix of a tag- or size-filtered rule too: such a
// rule still expires some live objects, which check reports, and a rule the
// command writes must not have one.
func filterPrefix(r types.LifecycleRule) (prefix string, only bool) {
	f := r.Filter
	if f == nil {
		return aws.ToString(r.Prefix), true
	}
	if a := f.And; a != nil {
		return aws.ToString(a.Prefix), len(a.Tags) == 0 && a.ObjectSizeGreaterThan == nil && a.ObjectSizeLessThan == nil
	}
	return aws.ToString(f.Prefix), f.Tag == nil && f.ObjectSizeGreaterThan == nil && f.ObjectSizeLessThan == nil
}

// Legacy IDs, which the README's hand-made rules used before the command:
// unique per bucket only with one prefix, so they are the command's only on
// their exact target under its prefix.
const (
	legacyStagingID = OwnedIDPrefix + "staging-"
	legacyAbortID   = OwnedIDPrefix + "incomplete-copies"
)

// Owned reports whether a rule is the command's for prefix: its ID starts
// with OwnedIDPrefix and the prefix's tag (see StagingID), and its filter's
// prefix is under the prefix; or it has one of the README's former IDs on
// exactly what that ID was for, under the prefix: legacy
// cacheservice-staging-<ns> on <prefix>/ns/<ns>/staging/, and
// cacheservice-incomplete-copies on <prefix>/. So another service's rules,
// on another prefix of the bucket, nested or not, are never the command's.
func Owned(r types.LifecycleRule, prefix string) bool {
	id := aws.ToString(r.ID)
	p, _ := filterPrefix(r)
	root := layout.Root(prefix)
	if strings.HasPrefix(id, OwnedIDPrefix+prefixTag(prefix)+"-") {
		return strings.HasPrefix(p, root)
	}
	if id == legacyAbortID {
		return p == root
	}
	if ns, ok := strings.CutPrefix(id, legacyStagingID); ok && layout.ValidateNamespace(ns) == nil {
		sp, _ := StagingPrefix(prefix, ns)
		return p == sp
	}
	return false
}

// CheckSafe returns an error unless a rule is one the command may write for
// prefix: never broader than a namespace's staging prefix, except the rule
// that only aborts incomplete multipart uploads, on the prefix itself. It is
// the last check before anything is written, whatever generated the rule.
//
//   - Its ID is a valid ID of the command's, at most MaxIDLength.
//   - It filters by prefix alone.
//   - A rule that expires anything (current or noncurrent versions, delete
//     markers, or transitions, which move objects out of reach) has exactly
//     a valid namespace's staging prefix, and nothing that aborts uploads.
//   - Otherwise it only aborts incomplete multipart uploads, and has exactly
//     the service's prefix.
func CheckSafe(r types.LifecycleRule, prefix string) error {
	id := aws.ToString(r.ID)
	if !strings.HasPrefix(id, OwnedIDPrefix) || len(id) > MaxIDLength || !idPattern.MatchString(id) {
		return fmt.Errorf("rule ID %q: want %q followed by up to %d letters, digits, '.', '_' or '-'", id, OwnedIDPrefix, MaxIDLength-len(OwnedIDPrefix))
	}
	p, only := filterPrefix(r)
	if !only {
		return fmt.Errorf("rule %s filters by a tag or size: the command writes rules filtered by prefix alone", id)
	}
	root := layout.Root(prefix)
	expires := r.Expiration != nil || r.NoncurrentVersionExpiration != nil || len(r.Transitions) > 0 || len(r.NoncurrentVersionTransitions) > 0
	if expires {
		if r.AbortIncompleteMultipartUpload != nil {
			return fmt.Errorf("rule %s both expires objects and aborts uploads", id)
		}
		ns, ok := strings.CutPrefix(p, layout.NamespacesPrefix(prefix))
		ns, ok2 := strings.CutSuffix(ns, "/"+layout.StagingDir)
		if !ok || !ok2 || layout.ValidateNamespace(ns) != nil {
			return fmt.Errorf("rule %s expires objects under %q, which is not a namespace's staging prefix (%sns/<namespace>/staging/): it could expire live data", id, p, root)
		}
		return nil
	}
	if r.AbortIncompleteMultipartUpload == nil {
		return fmt.Errorf("rule %s does nothing", id)
	}
	if p != root {
		return fmt.Errorf("rule %s aborts uploads under %q, not the prefix %q", id, p, root)
	}
	return nil
}

// describe is a rule as the diff shows it, and what two rules are compared
// by: the parts the command sets, however the store spells them.
func describe(r types.LifecycleRule) string {
	var parts []string
	if r.Status != types.ExpirationStatusEnabled {
		parts = append(parts, "status "+string(r.Status))
	}
	p, only := filterPrefix(r)
	if !only {
		parts = append(parts, "filtered by tag or size too")
	}
	if e := r.Expiration; e != nil {
		switch {
		case e.Days != nil:
			parts = append(parts, fmt.Sprintf("expire after %d day(s)", *e.Days))
		case e.Date != nil:
			parts = append(parts, "expire on "+e.Date.UTC().Format("2006-01-02"))
		}
		if aws.ToBool(e.ExpiredObjectDeleteMarker) {
			parts = append(parts, "expire delete markers")
		}
	}
	if r.NoncurrentVersionExpiration != nil {
		parts = append(parts, "expire noncurrent versions")
	}
	if len(r.Transitions) > 0 || len(r.NoncurrentVersionTransitions) > 0 {
		parts = append(parts, "transitions")
	}
	if a := r.AbortIncompleteMultipartUpload; a != nil {
		parts = append(parts, fmt.Sprintf("abort incomplete uploads after %d day(s)", aws.ToInt32(a.DaysAfterInitiation)))
	}
	if d := details(r, only); d != "" {
		parts = append(parts, d)
	}
	return fmt.Sprintf("%q: %s", p, strings.Join(parts, ", "))
}

// details are, in full, the parts of a rule the command never generates
// (other filters, a date, delete markers, noncurrent versions,
// transitions), or "" when it has none: so a rule of the command's that
// drifted from what it generates never compares equal to it, and is
// updated (TestPlanDiffSeesDetails).
func details(r types.LifecycleRule, only bool) string {
	var d struct {
		Filter                       *types.LifecycleRuleFilter          `json:",omitempty"`
		Date                         string                              `json:",omitempty"`
		ExpiredObjectDeleteMarker    *bool                               `json:",omitempty"`
		NoncurrentVersionExpiration  *types.NoncurrentVersionExpiration  `json:",omitempty"`
		Transitions                  []types.Transition                  `json:",omitempty"`
		NoncurrentVersionTransitions []types.NoncurrentVersionTransition `json:",omitempty"`
	}
	if !only {
		d.Filter = r.Filter
	}
	if e := r.Expiration; e != nil {
		if e.Date != nil {
			d.Date = e.Date.UTC().Format(time.RFC3339)
		}
		d.ExpiredObjectDeleteMarker = e.ExpiredObjectDeleteMarker
	}
	d.NoncurrentVersionExpiration = r.NoncurrentVersionExpiration
	d.Transitions, d.NoncurrentVersionTransitions = r.Transitions, r.NoncurrentVersionTransitions
	b, err := json.Marshal(d)
	if err != nil || string(b) == "{}" {
		return ""
	}
	return "details " + string(b)
}

// target is what a rule of the command's is for, regardless of its ID: a
// rule replacing another of the same target keeps the same coverage.
func target(r types.LifecycleRule) string {
	p, _ := filterPrefix(r)
	if r.AbortIncompleteMultipartUpload != nil && r.Expiration == nil {
		return "abort " + p
	}
	return "expire " + p
}
