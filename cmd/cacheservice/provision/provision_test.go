package provision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/lifecycle"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// bucket is a bucket's lifecycle configuration and marker in memory.
type bucket struct {
	rules  []types.LifecycleRule
	marker bool
	// keys: the prefix holds keys and no marker, so claim must refuse it.
	keys bool
	puts int
}

func (b *bucket) GetLifecycle(context.Context) (lifecycle.Config, error) {
	return lifecycle.Config{Rules: slices.Clone(b.rules)}, nil
}

func (b *bucket) PutLifecycle(_ context.Context, cfg lifecycle.Config) error {
	b.puts++
	b.rules = slices.Clone(cfg.Rules)
	return nil
}

func (b *bucket) ReadMarker(context.Context, string) (ownership.Marker, error) {
	if !b.marker {
		return ownership.Marker{}, ownership.ErrNoMarker
	}
	return ownership.Marker{Layout: layout.Version, Created: time.Unix(1, 0), ServiceVersion: "test"}, nil
}

func (b *bucket) Claim(context.Context, string) (ownership.State, ownership.Marker, error) {
	switch {
	case b.marker:
		return ownership.Owned, ownership.Marker{Layout: layout.Version}, nil
	case b.keys:
		return 0, ownership.Marker{}, &ownership.ErrNotOwned{Prefix: "cache", Keys: []string{"cache/x"}}
	}
	b.marker = true
	return ownership.Claimed, ownership.Marker{Layout: layout.Version}, nil
}

// foreign is another tool's rule, which provision must keep.
var foreign = types.LifecycleRule{
	ID:         aws.String("someone-elses"),
	Status:     types.ExpirationStatusEnabled,
	Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
	Expiration: &types.LifecycleExpiration{Days: aws.Int32(30)},
}

func setenv(t *testing.T, namespaces string) {
	t.Helper()
	t.Setenv(blobstore.EnvEndpoint, "http://127.0.0.1:1")
	t.Setenv(blobstore.EnvRegion, "nyc3")
	t.Setenv(blobstore.EnvBucket, "b")
	t.Setenv(blobstore.EnvPrefix, "cache")
	t.Setenv(blobstore.EnvAccessKeyID, "id")
	t.Setenv(blobstore.EnvSecretAccessKey, "secret")
	t.Setenv(EnvNamespaces, namespaces)
	t.Setenv(EnvAbortRule, "")
}

// do runs `provision <resource> <action>` against b, and returns its exit
// code and the state it printed (nil when it printed none).
func do(t *testing.T, b *bucket, resource, action string) (int, map[string]string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	open := func(context.Context, blobstore.S3Config) (lifecycle.Store, error) { return b, nil }
	code := Main(context.Background(), []string{resource, action}, &out, &errOut, open)
	if strings.Contains(out.String()+errOut.String(), "secret") {
		t.Fatalf("printed the secret key:\n%s%s", out.String(), errOut.String())
	}
	if out.Len() == 0 {
		return code, nil, errOut.String()
	}
	var state map[string]string
	if err := json.Unmarshal(out.Bytes(), &state); err != nil {
		t.Fatalf("stdout isn't one JSON object of strings: %v\n%s", err, out.String())
	}
	return code, state, errOut.String()
}

func ids(rules []types.LifecycleRule) []string {
	var out []string
	for _, r := range rules {
		out = append(out, aws.ToString(r.ID))
	}
	slices.Sort(out)
	return out
}

func TestClaim(t *testing.T) {
	setenv(t, "")
	b := &bucket{}

	code, state, _ := do(t, b, "claim", "read")
	if code != ExitOK || state != nil {
		t.Fatalf("read before claim: code %d, state %v; want 0 and no state (gone)", code, state)
	}
	code, created, stderr := do(t, b, "claim", "create")
	if code != ExitOK || !b.marker {
		t.Fatalf("create: code %d, marker %v\n%s", code, b.marker, stderr)
	}
	want := map[string]string{"bucket": "b", "prefix": "cache/", "marker": "cache/cacheservice.json", "layout": "2"}
	if !maps(created, want) {
		t.Fatalf("create's state %v, want %v", created, want)
	}
	if _, read, _ := do(t, b, "claim", "read"); !maps(read, created) {
		t.Fatalf("read's state %v differs from create's %v: Terraform would see drift", read, created)
	}
	// Again: idempotent.
	if code, again, _ := do(t, b, "claim", "create"); code != ExitOK || !maps(again, created) {
		t.Fatalf("second create: code %d, state %v", code, again)
	}
	// delete leaves the marker.
	if code, _, _ := do(t, b, "claim", "delete"); code != ExitOK || !b.marker {
		t.Fatalf("delete: code %d, marker %v; want 0 and the marker kept", code, b.marker)
	}
}

func TestClaimRefusesPrefixWithKeys(t *testing.T) {
	setenv(t, "")
	b := &bucket{keys: true}
	code, state, stderr := do(t, b, "claim", "create")
	if code != lifecycle.ExitError || state != nil || b.marker {
		t.Fatalf("code %d, state %v, marker %v; want refused, nothing written\n%s", code, state, b.marker, stderr)
	}
}

func TestRules(t *testing.T) {
	setenv(t, "default, team-a")
	b := &bucket{marker: true, rules: []types.LifecycleRule{foreign}}

	code, state, _ := do(t, b, "rules", "read")
	if code != ExitOK || state["in_sync"] != "false" {
		t.Fatalf("read before create: code %d, state %v; want in_sync false", code, state)
	}
	code, created, stderr := do(t, b, "rules", "create")
	if code != ExitOK {
		t.Fatalf("create: code %d\n%s", code, stderr)
	}
	want := map[string]string{"bucket": "b", "prefix": "cache/", "namespaces": "default,team-a", "abort_rule": "true", "in_sync": "true"}
	if !maps(created, want) {
		t.Fatalf("create's state %v, want %v", created, want)
	}
	wantIDs := []string{
		"cacheservice-daa886c7-incomplete-uploads",
		"cacheservice-daa886c7-staging-default",
		"cacheservice-daa886c7-staging-team-a",
		"someone-elses",
	}
	if got := ids(b.rules); !slices.Equal(got, wantIDs) {
		t.Fatalf("rules %v, want %v", got, wantIDs)
	}
	if _, read, _ := do(t, b, "rules", "read"); !maps(read, created) {
		t.Fatalf("read's state %v differs from create's %v", read, created)
	}

	// Drift: a rule removed by hand reads as out of sync, so Terraform
	// updates, and update puts it back.
	b.rules = slices.DeleteFunc(b.rules, func(r types.LifecycleRule) bool { return strings.HasSuffix(aws.ToString(r.ID), "team-a") })
	if _, read, _ := do(t, b, "rules", "read"); read["in_sync"] != "false" {
		t.Fatalf("after drift, read's state %v; want in_sync false", read)
	}
	if code, _, _ := do(t, b, "rules", "update"); code != ExitOK || !slices.Equal(ids(b.rules), wantIDs) {
		t.Fatalf("update: code %d, rules %v", code, ids(b.rules))
	}

	// A namespace taken out of the list loses its rule.
	t.Setenv(EnvNamespaces, "default")
	if code, _, _ := do(t, b, "rules", "update"); code != ExitOK {
		t.Fatalf("update to one namespace: code %d", code)
	}
	if got, want := ids(b.rules), []string{"cacheservice-daa886c7-incomplete-uploads", "cacheservice-daa886c7-staging-default", "someone-elses"}; !slices.Equal(got, want) {
		t.Fatalf("rules %v, want %v", got, want)
	}

	// delete removes this prefix's rules only.
	if code, state, _ := do(t, b, "rules", "delete"); code != ExitOK || state != nil {
		t.Fatalf("delete: code %d, state %v", code, state)
	}
	if got := ids(b.rules); !slices.Equal(got, []string{"someone-elses"}) {
		t.Fatalf("after delete, rules %v; want only the other tool's", got)
	}
}

func TestRulesNeedTheClaim(t *testing.T) {
	setenv(t, "default")
	b := &bucket{}
	if code, _, stderr := do(t, b, "rules", "create"); code != lifecycle.ExitError || b.puts != 0 {
		t.Fatalf("create on an unclaimed prefix: code %d, puts %d; want refused\n%s", code, b.puts, stderr)
	}
	// Without the marker, delete has nothing to do, and succeeds, so a
	// destroy after the bucket was emptied doesn't get stuck.
	if code, _, _ := do(t, b, "rules", "delete"); code != ExitOK {
		t.Fatalf("delete without a marker: code %d", code)
	}
}

func TestAbortRuleOff(t *testing.T) {
	setenv(t, "default")
	t.Setenv(EnvAbortRule, "false")
	b := &bucket{marker: true}
	if code, state, _ := do(t, b, "rules", "create"); code != ExitOK || state["abort_rule"] != "false" {
		t.Fatalf("code %d, state %v", code, state)
	}
	if got := ids(b.rules); !slices.Equal(got, []string{"cacheservice-daa886c7-staging-default"}) {
		t.Fatalf("rules %v", got)
	}
}

func TestUsage(t *testing.T) {
	setenv(t, "default")
	for _, tc := range []struct {
		args []string
		env  string
	}{
		{args: nil},
		{args: []string{"claim"}},
		{args: []string{"marker", "create"}},
		{args: []string{"rules", "plan"}},
		{args: []string{"rules", "create"}, env: " , "},
		{args: []string{"rules", "create"}, env: "bad..ns"},
	} {
		if tc.env != "" {
			t.Setenv(EnvNamespaces, tc.env)
		}
		var out, errOut bytes.Buffer
		open := func(context.Context, blobstore.S3Config) (lifecycle.Store, error) {
			return nil, errors.New("must not open the store")
		}
		if code := Main(context.Background(), tc.args, &out, &errOut, open); code != ExitUsage || out.Len() != 0 {
			t.Errorf("%v with %q: code %d, stdout %q; want %d and nothing\n%s", tc.args, tc.env, code, out.String(), ExitUsage, errOut.String())
		}
	}
}

func maps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
