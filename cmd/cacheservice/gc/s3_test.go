package gc

import (
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Only enabled rules that expire objects by age, filtered by prefix alone,
// cover a staging prefix: a rule with an expiration Date expires what
// exists on that day, once, not staged uploads as they get old.
func TestExpiringPrefixes(t *testing.T) {
	on, off := types.ExpirationStatusEnabled, types.ExpirationStatusDisabled
	days := &types.LifecycleExpiration{Days: aws.Int32(1)}
	date := &types.LifecycleExpiration{Date: aws.Time(time.Now().Add(24 * time.Hour))}
	prefix := func(p string) *types.LifecycleRuleFilter { return &types.LifecycleRuleFilter{Prefix: aws.String(p)} }
	got := expiringPrefixes([]types.LifecycleRule{
		{Status: on, Expiration: days, Filter: prefix("c/a/staging/")},
		{Status: on, Expiration: date, Filter: prefix("c/b/staging/")},
		{Status: off, Expiration: days, Filter: prefix("c/c/staging/")},
		{Status: on, Filter: prefix("c/d/staging/"), AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)}},
		{Status: on, Expiration: days, Filter: &types.LifecycleRuleFilter{Tag: &types.Tag{Key: aws.String("k"), Value: aws.String("v")}}},
		{Status: on, Expiration: days, Filter: &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("c/e/staging/")}}},
		{Status: on, Expiration: days, Prefix: aws.String("c/f/staging/")},
	})
	want := []string{"c/a/staging/", "c/e/staging/", "c/f/staging/"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
