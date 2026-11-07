package gc

import "github.com/aws/aws-sdk-go-v2/service/s3/types"

// The rule parsing GC uses, exported for the lifecycle command (package
// lifecycle), so that its check and GC agree on what covers a namespace's
// staging prefix and what expires its live data.

// ExpiringPrefixes are the prefixes under which enabled rules expire every
// current object by its age, as ExpirationPrefixes reads them.
func ExpiringPrefixes(rules []types.LifecycleRule) []string { return expiringPrefixes(rules) }

// StagingCovered reports whether one of prefixes covers every key under a
// namespace's staging prefix and is within it: what GC counts as the
// namespace's staging rule (gc_staging_rule_missing 0).
func StagingCovered(nsPrefix string, prefixes []string) bool {
	return stagingCovered(nsPrefix, prefixes)
}

// LiveRules are the prefixes that expire keys of a namespace's live data:
// what GC reports as liveRules.
func LiveRules(nsPrefix string, prefixes []string) []string { return liveRules(nsPrefix, prefixes) }
