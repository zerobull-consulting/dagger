package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

const prefix = "cache"

func expireRule(id, p string) types.LifecycleRule {
	return types.LifecycleRule{
		ID: aws.String(id), Status: types.ExpirationStatusEnabled,
		Filter:     &types.LifecycleRuleFilter{Prefix: aws.String(p)},
		Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
	}
}

func abortRule(id, p string) types.LifecycleRule {
	return types.LifecycleRule{
		ID: aws.String(id), Status: types.ExpirationStatusEnabled,
		Filter:                         &types.LifecycleRuleFilter{Prefix: aws.String(p)},
		AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)},
	}
}

// The rules are the README's pipeline's: one staging rule per namespace,
// plus the abort rule on the prefix, for 0, 1 and many namespaces.
func TestRules(t *testing.T) {
	for _, tc := range []struct {
		namespaces []string
		prefixes   []string
	}{
		{nil, nil},
		{[]string{"default"}, []string{"cache/ns/default/staging/"}},
		{[]string{"team-a", "team-b", "team-a", "x.y_z"}, []string{"cache/ns/team-a/staging/", "cache/ns/team-b/staging/", "cache/ns/x.y_z/staging/"}},
	} {
		rules, err := Rules(prefix, tc.namespaces, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(rules) != len(tc.prefixes)+1 {
			t.Fatalf("%v: %d rules", tc.namespaces, len(rules))
		}
		for i, p := range tc.prefixes {
			r := rules[i]
			if pf, _ := filterPrefix(r); pf != p || aws.ToInt32(r.Expiration.Days) != 1 || r.AbortIncompleteMultipartUpload != nil {
				t.Fatalf("rule %d: %s", i, describe(r))
			}
			ns := strings.Split(p, "/")[2]
			if aws.ToString(r.ID) != StagingID(prefix, ns) {
				t.Fatalf("rule %d: ID %s", i, aws.ToString(r.ID))
			}
		}
		last := rules[len(rules)-1]
		if pf, _ := filterPrefix(last); pf != "cache/" || last.Expiration != nil || aws.ToInt32(last.AbortIncompleteMultipartUpload.DaysAfterInitiation) != 1 {
			t.Fatalf("abort rule: %s", describe(last))
		}
		for _, r := range rules {
			if err := CheckSafe(r, prefix); err != nil {
				t.Fatal(err)
			}
		}
		// What GC reads of them: every namespace covered, no live rule.
		rep, err := Check(prefix, rules, tc.namespaces)
		if err != nil || !rep.OK() || !rep.Abort {
			t.Fatalf("check of the generated rules: %+v, %v", rep, err)
		}
	}
	rules, _ := Rules(prefix, []string{"a"}, false)
	if len(rules) != 1 || rules[0].AbortIncompleteMultipartUpload != nil {
		t.Fatalf("without the abort rule: %v", ids(rules))
	}
	if _, err := Rules(prefix, []string{"../x"}, true); err == nil {
		t.Fatal("an invalid namespace became a rule")
	}
}

// IDs are unique per prefix, valid, and within S3's limit even for the
// longest namespace.
func TestRuleIDs(t *testing.T) {
	long := strings.Repeat("n", 128)
	if err := layout.ValidateNamespace(long); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"", "cache", "/cache/", "a/b/c"} {
		rules, err := Rules(p, []string{long}, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rules {
			if len(aws.ToString(r.ID)) > MaxIDLength {
				t.Fatalf("ID of %d characters", len(aws.ToString(r.ID)))
			}
			if err := CheckSafe(r, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	if StagingID("cache", "a") == StagingID("cache2", "a") || AbortID("cache") == AbortID("other") {
		t.Fatal("two prefixes share IDs")
	}
	if StagingID("cache", "a") != StagingID("/cache/", "a") {
		t.Fatal("the same prefix, spelled with slashes, has other IDs")
	}
	bad := expireRule(OwnedIDPrefix+strings.Repeat("x", MaxIDLength), "cache/ns/a/staging/")
	if CheckSafe(bad, prefix) == nil {
		t.Fatal("an ID over 255 characters passed")
	}
	if CheckSafe(expireRule(OwnedIDPrefix+"a b", "cache/ns/a/staging/"), prefix) == nil {
		t.Fatal("an ID with a space passed")
	}
}

// Nothing broader than a namespace's staging prefix is ever written, except
// the abort rule on the prefix itself.
func TestCheckSafeRefusesBroadRules(t *testing.T) {
	id := OwnedIDPrefix + "x"
	tagged := expireRule(id, "")
	tagged.Filter = &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("cache/ns/a/staging/"), Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}}
	both := expireRule(id, "cache/ns/a/staging/")
	both.AbortIncompleteMultipartUpload = &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)}
	noncurrent := abortRule(id, "cache/")
	noncurrent.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1)}
	transition := types.LifecycleRule{ID: aws.String(id), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("cache/")}, Transitions: []types.Transition{{Days: aws.Int32(1)}}}
	for name, r := range map[string]types.LifecycleRule{
		"the prefix":              expireRule(id, "cache/"),
		"the bucket":              expireRule(id, ""),
		"the namespaces":          expireRule(id, "cache/ns/"),
		"a namespace":             expireRule(id, "cache/ns/a/"),
		"a namespace's blobs":     expireRule(id, "cache/ns/a/blobs/"),
		"staging without a slash": expireRule(id, "cache/ns/a/staging"),
		"another prefix":          expireRule(id, "other/ns/a/staging/"),
		"an invalid namespace":    expireRule(id, "cache/ns/../staging/"),
		"a nested staging":        expireRule(id, "cache/ns/a/b/staging/"),
		"a tag filter":            tagged,
		"expire and abort":        both,
		"noncurrent versions":     noncurrent,
		"a transition":            transition,
		"abort under a namespace": abortRule(id, "cache/ns/a/"),
		"abort the bucket":        abortRule(id, ""),
		"a foreign ID":            expireRule("other", "cache/ns/a/staging/"),
		"nothing":                 {ID: aws.String(id), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("cache/")}},
	} {
		if err := CheckSafe(r, prefix); err == nil {
			t.Errorf("%s: passed", name)
		}
		if name == "a foreign ID" {
			continue
		}
		if _, err := NewPlan(prefix, nil, []types.LifecycleRule{r}, MaxRules); err == nil {
			t.Errorf("%s: planned", name)
		}
	}
}

// apply's result keeps every rule of other tools as it read it, in order,
// and replaces only the command's rules for its prefix: not those of
// another prefix in the same bucket.
func TestPlanMergesAroundForeignRules(t *testing.T) {
	foreign := []types.LifecycleRule{
		expireRule("logs", "logs/"),
		expireRule("cacheservice-other-staging-a", "cache2/ns/a/staging/"), // another service's prefix
		abortRule("someone-else", "cache/"),                                // not ours: not our ID
	}
	current := []types.LifecycleRule{foreign[0], expireRule(StagingID(prefix, "a"), "cache/ns/a/staging/"), foreign[1], foreign[2]}
	wanted, _ := Rules(prefix, []string{"a", "b"}, true)
	p, err := NewPlan(prefix, current, wanted, MaxRules)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids(p.Foreign), ids(foreign)) {
		t.Fatalf("foreign %v", ids(p.Foreign))
	}
	for i := range foreign {
		if describe(p.Result[i]) != describe(foreign[i]) {
			t.Fatalf("foreign rule %d changed", i)
		}
	}
	if want := append(ids(foreign), ids(wanted)...); !slices.Equal(ids(p.Result), want) {
		t.Fatalf("result %v, want %v", ids(p.Result), want)
	}
	if p.Count(Keep) != 1 || p.Count(Add) != 2 || p.Count(Remove) != 0 {
		t.Fatalf("changes %+v", p.Changes)
	}
	// A second plan against the result changes nothing.
	again, err := NewPlan(prefix, p.Result, wanted, MaxRules)
	if err != nil || again.Changed() {
		t.Fatalf("second plan: %+v, %v", again, err)
	}
}

// A rule is the command's only with its prefix's hash and a filter under
// the prefix, or a README-era ID on exactly its target: never another
// service's, whatever the two prefixes.
func TestOwned(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rule   types.LifecycleRule
		prefix string
		want   bool
	}{
		{"its staging rule", expireRule(StagingID("cache", "a"), "cache/ns/a/staging/"), "cache", true},
		{"its abort rule", abortRule(AbortID("cache"), "cache/"), "cache", true},
		{"its hash, a broad hand-made rule", expireRule(OwnedIDPrefix+prefixTag("cache")+"-oops", "cache/ns/"), "cache", true},
		{"its hash, outside the prefix", expireRule(StagingID("cache", "a"), "other/ns/a/staging/"), "cache", false},
		{"another prefix's, from the empty prefix", expireRule(StagingID("team2", "x"), "team2/ns/x/staging/"), "", false},
		{"a nested prefix's", expireRule(StagingID("cache/sub", "x"), "cache/sub/ns/x/staging/"), "cache", false},
		{"the parent prefix's, from a nested one", abortRule(AbortID("cache"), "cache/"), "cache/sub", false},
		{"legacy staging", expireRule("cacheservice-staging-a", "cache/ns/a/staging/"), "cache", true},
		{"legacy staging, another prefix", expireRule("cacheservice-staging-a", "cache2/ns/a/staging/"), "cache", false},
		{"legacy staging, nested prefix", expireRule("cacheservice-staging-a", "cache/sub/ns/a/staging/"), "cache", false},
		{"legacy staging, another namespace's prefix", expireRule("cacheservice-staging-a", "cache/ns/b/staging/"), "cache", false},
		{"legacy abort", abortRule("cacheservice-incomplete-copies", "cache/"), "cache", true},
		{"legacy abort, another prefix", abortRule("cacheservice-incomplete-copies", "other/"), "cache", false},
		{"another cacheservice- rule", expireRule("cacheservice-oops", "cache/"), "cache", false},
		{"another tool's", expireRule("logs", "cache/ns/a/staging/"), "cache", false},
	} {
		if got := Owned(tc.rule, tc.prefix); got != tc.want {
			t.Errorf("%s: owned %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The diff: keep, add, update, replace (the README's former IDs) and remove
// (a namespace no longer configured).
func TestPlanDiff(t *testing.T) {
	changed := expireRule(StagingID(prefix, "b"), "cache/ns/b/staging/")
	changed.Expiration.Days = aws.Int32(7)
	current := []types.LifecycleRule{
		expireRule(StagingID(prefix, "a"), "cache/ns/a/staging/"),
		changed,
		expireRule("cacheservice-staging-c", "cache/ns/c/staging/"),
		abortRule("cacheservice-incomplete-copies", "cache/"),
		expireRule(StagingID(prefix, "gone"), "cache/ns/gone/staging/"),
	}
	wanted, _ := Rules(prefix, []string{"a", "b", "c", "d"}, true)
	p, err := NewPlan(prefix, current, wanted, MaxRules)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range p.Changes {
		got = append(got, string(c.Kind)+" "+c.ID+" "+c.OldID)
	}
	want := []string{
		"keep " + StagingID(prefix, "a") + " ",
		"update " + StagingID(prefix, "b") + " " + StagingID(prefix, "b"),
		"replace " + StagingID(prefix, "c") + " cacheservice-staging-c",
		"add " + StagingID(prefix, "d") + " ",
		"replace " + AbortID(prefix) + " cacheservice-incomplete-copies",
		"remove " + StagingID(prefix, "gone") + " ",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("diff\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if len(p.Foreign) != 0 || len(p.Result) != len(wanted) {
		t.Fatalf("result %v", ids(p.Result))
	}
}

func TestPlanRuleLimit(t *testing.T) {
	var current []types.LifecycleRule
	for i := range 8 {
		current = append(current, expireRule("other-"+string(rune('a'+i)), "x/"))
	}
	wanted, _ := Rules(prefix, []string{"a"}, true)
	if _, err := NewPlan(prefix, current, wanted, 10); err != nil {
		t.Fatalf("10 rules of 10: %v", err)
	}
	if _, err := NewPlan(prefix, current, wanted, 9); err == nil || !strings.Contains(err.Error(), "limit of 9") {
		t.Fatalf("10 rules of 9: %v", err)
	}
}

// A wanted ID taken by a rule the command doesn't own (another prefix's) is
// refused, rather than written twice or taken over.
func TestPlanIDTakenByForeignRule(t *testing.T) {
	current := []types.LifecycleRule{expireRule(StagingID(prefix, "a"), "elsewhere/")}
	wanted, _ := Rules(prefix, []string{"a"}, true)
	if _, err := NewPlan(prefix, current, wanted, MaxRules); err == nil {
		t.Fatal("planned over a foreign rule's ID")
	}
}

// check reports what GC reports: a namespace without its staging rule, and a
// rule expiring live data; and the command's own rules that are too broad.
func TestCheck(t *testing.T) {
	good, _ := Rules(prefix, []string{"a", "b"}, true)
	dated := types.LifecycleRule{ID: aws.String("dated"), Status: types.ExpirationStatusEnabled,
		Filter: &types.LifecycleRuleFilter{Prefix: aws.String("cache/ns/b/blobs/")}, Expiration: &types.LifecycleExpiration{Date: aws.Time(time.Now())}}
	disabled := expireRule("off", "cache/")
	disabled.Status = types.ExpirationStatusDisabled
	for _, tc := range []struct {
		name    string
		rules   []types.LifecycleRule
		missing []string
		live    []string
		unsafe  int
		abort   bool
	}{
		{"none", nil, []string{"a", "b"}, nil, 0, false},
		{"all", good, nil, nil, 0, true},
		{"one missing", good[1:], []string{"a"}, nil, 0, true},
		{"broad foreign", append(slices.Clone(good), expireRule("logs", "cache/")), nil, []string{"a", "b"}, 0, true},
		{"bucket-wide", []types.LifecycleRule{expireRule("all", "")}, []string{"a", "b"}, []string{"a", "b"}, 0, false},
		{"dated", append(slices.Clone(good), dated), nil, []string{"b"}, 0, true},
		{"disabled", append(slices.Clone(good), disabled), nil, nil, 0, true},
		{"broad own", []types.LifecycleRule{expireRule(OwnedIDPrefix+prefixTag(prefix)+"-x", "cache/ns/a/")}, []string{"a", "b"}, []string{"a"}, 1, false},
		{"abort above the prefix", []types.LifecycleRule{abortRule("x", "")}, []string{"a", "b"}, nil, 0, true},
		{"a transition", append(slices.Clone(good), types.LifecycleRule{ID: aws.String("cold"), Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String("cache/ns/a/blobs/")}, Transitions: []types.Transition{{Days: aws.Int32(7), StorageClass: types.TransitionStorageClassGlacier}}}), nil, []string{"a"}, 0, true},
	} {
		rep, err := Check(prefix, tc.rules, []string{"a", "b"})
		if err != nil {
			t.Fatal(err)
		}
		var live []string
		for ns := range rep.Live {
			live = append(live, ns)
		}
		slices.Sort(live)
		if !slices.Equal(rep.Missing, tc.missing) || !slices.Equal(live, tc.live) || len(rep.Unsafe) != tc.unsafe || rep.Abort != tc.abort {
			t.Errorf("%s: %+v", tc.name, rep)
		}
		if rep.OK() != (len(tc.missing) == 0 && len(tc.live) == 0 && tc.unsafe == 0) {
			t.Errorf("%s: OK %v", tc.name, rep.OK())
		}
	}
}

// fakeStore is a bucket's lifecycle configuration and marker in memory.
type fakeStore struct {
	cfg    Config
	marker error
	getErr error
	putErr error
	puts   int
	gets   int
	onGet  func(n int)
}

func (f *fakeStore) GetLifecycle(context.Context) (Config, error) {
	f.gets++
	if f.onGet != nil {
		f.onGet(f.gets)
	}
	if f.getErr != nil {
		return Config{}, f.getErr
	}
	return Config{Rules: slices.Clone(f.cfg.Rules), TransitionDefaultMinimumObjectSize: f.cfg.TransitionDefaultMinimumObjectSize}, nil
}

func (f *fakeStore) PutLifecycle(_ context.Context, cfg Config) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.puts++
	f.cfg = cfg
	return nil
}

func (f *fakeStore) ReadMarker(context.Context, string) (ownership.Marker, error) {
	if f.marker != nil {
		return ownership.Marker{}, f.marker
	}
	return ownership.Marker{Layout: layout.Version}, nil
}

func apiError(status int, code string) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      &smithy.GenericAPIError{Code: code, Message: code},
	}
}

func opts(action string, namespaces ...string) Options {
	return Options{Action: action, Bucket: "bkt", Prefix: prefix, Namespaces: namespaces, Source: "test",
		Confirm: "bkt/cache", AbortRule: true, MaxRules: MaxRules}
}

func run(t *testing.T, o Options, st Store) (int, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := Run(context.Background(), o, st, &out, &errOut)
	return code, out.String() + errOut.String()
}

// apply writes the merged configuration once; a second apply writes
// nothing; a foreign rule survives both.
func TestApply(t *testing.T) {
	foreign := expireRule("logs", "logs/")
	st := &fakeStore{cfg: Config{Rules: []types.LifecycleRule{foreign}, TransitionDefaultMinimumObjectSize: "all_storage_classes_128K"}}
	if code, out := run(t, opts("plan", "a"), st); code != ExitOK || st.puts != 0 || !strings.Contains(out, "+ add") {
		t.Fatalf("plan: %d, %d puts\n%s", code, st.puts, out)
	}
	code, out := run(t, opts("apply", "a"), st)
	if code != ExitOK || st.puts != 1 {
		t.Fatalf("apply: %d, %d puts\n%s", code, st.puts, out)
	}
	if want := []string{"logs", StagingID(prefix, "a"), AbortID(prefix)}; !slices.Equal(ids(st.cfg.Rules), want) {
		t.Fatalf("rules %v, want %v", ids(st.cfg.Rules), want)
	}
	if st.cfg.TransitionDefaultMinimumObjectSize != "all_storage_classes_128K" {
		t.Fatal("the configuration's other setting wasn't written back")
	}
	if code, out := run(t, opts("apply", "a"), st); code != ExitOK || st.puts != 1 {
		t.Fatalf("second apply: %d, %d puts\n%s", code, st.puts, out)
	}
	if code, out := run(t, opts("check", "a"), st); code != ExitOK {
		t.Fatalf("check after apply: %d\n%s", code, out)
	}
}

// -confirm missing or wrong exits 2, before reading anything.
func TestApplyConfirm(t *testing.T) {
	for _, confirm := range []string{"", "bkt", "bkt/", "bkt/cache/", "other/cache", "bkt/cache2", "bkt/Cache"} {
		st := &fakeStore{}
		o := opts("apply", "a")
		o.Confirm = confirm
		if code, out := run(t, o, st); code != ExitUsage || st.puts != 0 || st.gets != 0 {
			t.Fatalf("-confirm=%q: %d, %d puts, %d gets\n%s", confirm, code, st.puts, st.gets, out)
		}
	}
	st := &fakeStore{}
	o := opts("apply", "a")
	o.Prefix, o.Confirm = "", "bkt/"
	if code, out := run(t, o, st); code != ExitOK || st.puts != 1 {
		t.Fatalf("empty prefix: %d\n%s", code, out)
	}
	if ConfirmValue("bkt", "/a/b/") != "bkt/a/b" {
		t.Fatal(ConfirmValue("bkt", "/a/b/"))
	}
}

// A namespace removed from the tokens file loses its rule, only with
// -allow-removals.
func TestApplyRemovals(t *testing.T) {
	st := &fakeStore{}
	if code, _ := run(t, opts("apply", "a", "b"), st); code != ExitOK {
		t.Fatal(code)
	}
	o := opts("plan", "a")
	o.TokensFile, o.AbortRule = "/etc/cache service/tokens", false
	code, out := run(t, o, st)
	if code != ExitOK || !strings.Contains(out, "- remove   "+StagingID(prefix, "b")) {
		t.Fatalf("plan of a removal:\n%s", out)
	}
	// The apply command it prints repeats the namespaces' flag and the
	// options, so pasting it applies this plan, not -scope's.
	if want := "cacheservice lifecycle apply -tokens-file '/etc/cache service/tokens' -abort-rule=false -allow-removals -confirm=bkt/cache"; !strings.Contains(out, want) {
		t.Fatalf("plan's apply command, want %q:\n%s", want, out)
	}
	if code, out := run(t, opts("apply", "a"), st); code != ExitUsage || st.puts != 1 {
		t.Fatalf("a removal without -allow-removals: %d\n%s", code, out)
	}
	o = opts("apply", "a")
	o.AllowRemovals = true
	if code, out := run(t, o, st); code != ExitOK || st.puts != 2 || slices.Contains(ids(st.cfg.Rules), StagingID(prefix, "b")) {
		t.Fatalf("with -allow-removals: %d, %v\n%s", code, ids(st.cfg.Rules), out)
	}
	// With no rules left, the configuration is deleted (PutLifecycle with
	// none).
	o = opts("apply")
	o.AllowRemovals, o.AbortRule = true, false
	if code, out := run(t, o, st); code != ExitOK || len(st.cfg.Rules) != 0 {
		t.Fatalf("removing every rule: %d, %v\n%s", code, ids(st.cfg.Rules), out)
	}
}

// apply refuses a prefix without the service's marker, as GC does.
func TestApplyRefusesUnownedPrefix(t *testing.T) {
	for _, err := range []error{ownership.ErrNoMarker, &ownership.ErrOtherLayout{Key: "k", Found: 1}, errors.New("unreadable")} {
		st := &fakeStore{marker: err}
		if code, out := run(t, opts("apply", "a"), st); code != ExitError || st.puts != 0 {
			t.Fatalf("%v: %d\n%s", err, code, out)
		}
		// plan and check only report it.
		if code, _ := run(t, opts("plan", "a"), st); code != ExitOK {
			t.Fatalf("plan with %v: %d", err, code)
		}
	}
}

// Errors from the store are explained, and nothing is written past one.
func TestApplyStoreErrors(t *testing.T) {
	st := &fakeStore{putErr: apiError(http.StatusForbidden, "AccessDenied")}
	if code, out := run(t, opts("apply", "a"), st); code != ExitError || !strings.Contains(out, "admin key") {
		t.Fatalf("403 on put: %d\n%s", code, out)
	}
	st = &fakeStore{putErr: apiError(http.StatusBadRequest, "MalformedXML")}
	if code, out := run(t, opts("apply", "a"), st); code != ExitError || !strings.Contains(out, "-abort-rule=false") {
		t.Fatalf("400 on put: %d\n%s", code, out)
	}
	st = &fakeStore{getErr: apiError(http.StatusForbidden, "AccessDenied")}
	if code, out := run(t, opts("apply", "a"), st); code != ExitError || st.puts != 0 || !strings.Contains(out, "s3:GetLifecycleConfiguration") {
		t.Fatalf("403 on get: %d\n%s", code, out)
	}
	// What is read back after the write is verified: a foreign rule
	// dropped, or changed in a detail the diff doesn't show (its tag), is
	// reported.
	st = &fakeStore{cfg: Config{Rules: []types.LifecycleRule{expireRule("logs", "logs/")}}}
	st.onGet = func(n int) {
		if n == 2 { // reading it back: a store that dropped the foreign rule

			st.cfg.Rules = st.cfg.Rules[1:]
		}
	}
	if code, out := run(t, opts("apply", "a"), st); code != ExitError || !strings.Contains(out, "other rules aren't as they were written") {
		t.Fatalf("a foreign rule lost: %d\n%s", code, out)
	}
	tagged := func(v string) types.LifecycleRule {
		r := expireRule("logs", "")
		r.Filter = &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("logs/"), Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String(v)}}}}
		return r
	}
	st = &fakeStore{cfg: Config{Rules: []types.LifecycleRule{tagged("v")}}}
	st.onGet = func(n int) {
		if n == 2 {
			st.cfg.Rules[0] = tagged("other")
		}
	}
	if code, out := run(t, opts("apply", "a"), st); code != ExitError || !strings.Contains(out, "other rules aren't as they were written") {
		t.Fatalf("a foreign rule's tag changed: %d\n%s", code, out)
	}
}

// check exits 3 when a namespace lacks its rule or a rule is too broad, 0
// when all is there, and 1 when the configuration can't be read.
func TestCheckExitCodes(t *testing.T) {
	good, _ := Rules(prefix, []string{"a"}, false)
	for _, tc := range []struct {
		name string
		st   *fakeStore
		want int
	}{
		{"no configuration", &fakeStore{}, ExitFindings},
		{"the rules", &fakeStore{cfg: Config{Rules: good}}, ExitOK},
		{"a live rule", &fakeStore{cfg: Config{Rules: append(slices.Clone(good), expireRule("x", "cache/ns/"))}}, ExitFindings},
		{"unreadable", &fakeStore{getErr: apiError(http.StatusForbidden, "AccessDenied")}, ExitError},
		{"no marker", &fakeStore{cfg: Config{Rules: good}, marker: ownership.ErrNoMarker}, ExitOK},
	} {
		if code, out := run(t, opts("check", "a"), tc.st); code != tc.want || tc.st.puts != 0 {
			t.Errorf("%s: %d, want %d\n%s", tc.name, code, tc.want, out)
		}
	}
}

// Main: flags, the action, the tokens file, and the S3 settings.
func TestMain(t *testing.T) {
	for _, k := range []string{blobstore.EnvEndpoint, blobstore.EnvRegion, blobstore.EnvAccessKeyID, blobstore.EnvSecretAccessKey} {
		t.Setenv(k, "")
	}
	t.Setenv(blobstore.EnvBucket, "bkt")
	t.Setenv(blobstore.EnvPrefix, "cache")
	dir := t.TempDir()
	tokens := filepath.Join(dir, "tokens")
	if err := os.WriteFile(tokens, []byte("# comment\nsecret-1 team-b\nsecret-2 team-a\nsecret-3 team-b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st := &fakeStore{}
	open := func(context.Context, blobstore.S3Config) (Store, error) { return st, nil }
	main := func(args ...string) (int, string) {
		var out, errOut bytes.Buffer
		code := Main(context.Background(), args, &out, &errOut, open)
		return code, out.String() + errOut.String()
	}
	code, out := main("-tokens-file", tokens)
	if code != ExitOK || !strings.Contains(out, "(2): team-a, team-b") || st.puts != 0 {
		t.Fatalf("plan by default: %d\n%s", code, out)
	}
	if strings.Contains(out, "secret-") {
		t.Fatal("a token was printed")
	}
	if code, out := main("plan", "-scope", "ci"); code != ExitOK || !strings.Contains(out, "(1): ci") {
		t.Fatalf("-scope: %d\n%s", code, out)
	}
	if code, out := main("apply", "-tokens-file", tokens, "-confirm=bkt/other"); code != ExitUsage || st.puts != 0 {
		t.Fatalf("a -confirm mismatch: %d\n%s", code, out)
	}
	if code, out := main("apply", "-tokens-file", tokens, "-confirm=bkt/cache"); code != ExitOK || st.puts != 1 {
		t.Fatalf("apply: %d\n%s", code, out)
	}
	if code, out := main("check", "-tokens-file", tokens); code != ExitOK {
		t.Fatalf("check: %d\n%s", code, out)
	}
	for _, args := range [][]string{
		{"wipe"},
		{"plan", "extra"},
		{"-no-such-flag"},
		{"-scope", "../x"},
		{"-timeout", "0"},
	} {
		if code, _ := main(args...); code != ExitUsage {
			t.Errorf("%q: %d, want %d", args, code, ExitUsage)
		}
	}
	if code, _ := main("-tokens-file", filepath.Join(dir, "missing")); code != ExitError {
		t.Errorf("a missing tokens file: %d", code)
	}
	t.Setenv(blobstore.EnvPrefix, "a/../b")
	if code, _ := main(); code != ExitUsage {
		t.Errorf("a prefix with ..: %d", code)
	}
	t.Setenv(blobstore.EnvPrefix, "cache")
	t.Setenv(blobstore.EnvBucket, "")
	if code, _ := main(); code != ExitUsage {
		t.Errorf("no bucket: %d", code)
	}
}
