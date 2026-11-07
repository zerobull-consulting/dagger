package lifecycle

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dagger/dagger/cmd/cacheservice/gc"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
)

// Kind is what apply does to one of the command's rules.
type Kind string

const (
	// Keep: the rule is there as wanted.
	Keep Kind = "keep"
	// Add: a wanted rule isn't there.
	Add Kind = "add"
	// Update: a rule of the same ID is there, and differs.
	Update Kind = "update"
	// Replace: a rule of the command's for the same prefix and action is
	// there under another ID (such as the README's former hand-made IDs),
	// and the wanted one takes its place.
	Replace Kind = "replace"
	// Remove: a rule of the command's that nothing wanted replaces, such as
	// the staging rule of a namespace no longer in the tokens file.
	Remove Kind = "remove"
)

// Change is one line of the diff.
type Change struct {
	Kind Kind
	// ID is the wanted rule's, or the removed one's.
	ID string
	// Desc describes the wanted rule, or the removed one.
	Desc string
	// OldID and OldDesc are the rule an Update or Replace changes.
	OldID, OldDesc string
}

// Plan is what apply would do to a bucket's configuration.
type Plan struct {
	// Wanted are the command's rules, as generated.
	Wanted []types.LifecycleRule
	// Foreign are the bucket's other rules, which apply writes back as it
	// read them.
	Foreign []types.LifecycleRule
	// Changes is the diff, wanted rules first, in order, then removals.
	Changes []Change
	// Result is what apply writes: the foreign rules, then the wanted ones.
	Result []types.LifecycleRule
}

// Changed reports whether apply would write anything.
func (p *Plan) Changed() bool {
	return slices.ContainsFunc(p.Changes, func(c Change) bool { return c.Kind != Keep })
}

// Count counts the changes of a kind.
func (p *Plan) Count(k Kind) int {
	n := 0
	for _, c := range p.Changes {
		if c.Kind == k {
			n++
		}
	}
	return n
}

// NewPlan merges wanted, the command's rules for prefix, into current, the
// bucket's configuration. It refuses (an error, and no plan) when a wanted
// rule isn't one the command may write (CheckSafe), when a wanted ID is
// taken by a rule the command doesn't own, and when the result would hold
// more than maxRules rules.
func NewPlan(prefix string, current, wanted []types.LifecycleRule, maxRules int) (*Plan, error) {
	p := &Plan{Wanted: wanted}
	ids := map[string]bool{}
	for _, r := range wanted {
		if err := CheckSafe(r, prefix); err != nil {
			return nil, fmt.Errorf("refusing to write it: %w", err)
		}
		if ids[aws.ToString(r.ID)] {
			return nil, fmt.Errorf("rule ID %s twice", aws.ToString(r.ID))
		}
		ids[aws.ToString(r.ID)] = true
	}
	var owned []types.LifecycleRule
	for _, r := range current {
		if Owned(r, prefix) {
			owned = append(owned, r)
			continue
		}
		if ids[aws.ToString(r.ID)] {
			return nil, fmt.Errorf("rule ID %s is taken by a rule this command doesn't own (%s): rename or remove that rule first", aws.ToString(r.ID), describe(r))
		}
		p.Foreign = append(p.Foreign, r)
	}
	used := make([]bool, len(owned))
	byID := func(id string) int {
		return slices.IndexFunc(owned, func(r types.LifecycleRule) bool { return aws.ToString(r.ID) == id })
	}
	// IDs first, so that a replacement never takes a rule another wanted
	// rule keeps.
	matched := make([]int, len(wanted))
	for i, w := range wanted {
		matched[i] = -1
		if j := byID(aws.ToString(w.ID)); j >= 0 {
			matched[i], used[j] = j, true
		}
	}
	for i, w := range wanted {
		c := Change{ID: aws.ToString(w.ID), Desc: describe(w)}
		switch j := matched[i]; {
		case j >= 0 && describe(owned[j]) == c.Desc:
			c.Kind = Keep
		case j >= 0:
			c.Kind, c.OldID, c.OldDesc = Update, c.ID, describe(owned[j])
		default:
			c.Kind = Add
			for k, o := range owned {
				if !used[k] && target(o) == target(w) {
					used[k] = true
					c.Kind, c.OldID, c.OldDesc = Replace, aws.ToString(o.ID), describe(o)
					break
				}
			}
		}
		p.Changes = append(p.Changes, c)
	}
	for k, o := range owned {
		if !used[k] {
			p.Changes = append(p.Changes, Change{Kind: Remove, ID: aws.ToString(o.ID), Desc: describe(o)})
		}
	}
	p.Result = append(slices.Clone(p.Foreign), wanted...)
	if len(p.Result) > maxRules {
		return nil, fmt.Errorf("the configuration would hold %d rules (%d of other tools', %d of this command's), over the store's limit of %d", len(p.Result), len(p.Foreign), len(wanted), maxRules)
	}
	return p, nil
}

// Report is what check found.
type Report struct {
	// Missing are the namespaces whose staging prefix no rule expires, as
	// GC counts it (gc_staging_rule_missing 1).
	Missing []string
	// Live are, by namespace, the prefixes of enabled rules that expire
	// its live data (blobs, bundles, resolutions, the index or the GC
	// epoch): GC's liveRules warning. Unlike GC, it counts a rule that
	// expires on a date, or filters by a tag or size too, and a rule that
	// transitions current objects to another storage class (which can
	// leave them unreadable, as Glacier does): each puts live data out of
	// reach all the same.
	Live map[string][]string
	// Unsafe describe the command's own rules that it wouldn't write
	// (CheckSafe), such as a hand-made one too broad.
	Unsafe []string
	// Abort is whether an enabled rule aborts incomplete multipart uploads
	// under the whole prefix. Its absence is reported, not a failure: MinIO
	// refuses such a rule, and drops stale uploads itself.
	Abort bool
}

// OK reports whether nothing is missing or too broad.
func (r *Report) OK() bool {
	return len(r.Missing) == 0 && len(r.Live) == 0 && len(r.Unsafe) == 0
}

// Check checks a bucket's rules for prefix's namespaces. It reads nothing:
// rules are the bucket's configuration, as read.
func Check(prefix string, rules []types.LifecycleRule, namespaces []string) (*Report, error) {
	rep := &Report{Live: map[string][]string{}}
	staging := gc.ExpiringPrefixes(rules)
	var expiring []string
	root := layout.Root(prefix)
	for _, r := range rules {
		if r.Status != types.ExpirationStatusEnabled {
			continue
		}
		p, only := filterPrefix(r)
		if e := r.Expiration; (e != nil && (aws.ToInt32(e.Days) > 0 || e.Date != nil)) || len(r.Transitions) > 0 {
			expiring = append(expiring, p)
		}
		// Only a rule filtered by prefix alone aborts every upload under
		// the prefix.
		if a := r.AbortIncompleteMultipartUpload; a != nil && only && aws.ToInt32(a.DaysAfterInitiation) > 0 && strings.HasPrefix(root, p) {
			rep.Abort = true
		}
		if Owned(r, prefix) {
			if err := CheckSafe(r, prefix); err != nil {
				rep.Unsafe = append(rep.Unsafe, err.Error())
			}
		}
	}
	for _, ns := range namespaces {
		nsPrefix, err := layout.NamespacePrefix(prefix, ns)
		if err != nil {
			return nil, err
		}
		if !gc.StagingCovered(nsPrefix, staging) {
			rep.Missing = append(rep.Missing, ns)
		}
		if live := gc.LiveRules(nsPrefix, expiring); len(live) > 0 {
			rep.Live[ns] = live
		}
	}
	return rep, nil
}
