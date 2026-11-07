package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
	"github.com/dagger/dagger/cmd/cacheservice/server"
)

// Exit codes.
const (
	// ExitOK: done; for check, nothing missing or too broad.
	ExitOK = 0
	// ExitError: something failed, or apply refused; nothing was written.
	ExitError = 1
	// ExitUsage: bad flags, or a confirmation missing or wrong (apply's
	// -confirm, or -allow-removals when it would remove rules); nothing was
	// written.
	ExitUsage = 2
	// ExitFindings: check ran, and a namespace lacks its staging rule, or
	// a rule expires live data.
	ExitFindings = 3
)

// DefaultTimeout bounds a whole run, every request and wait included.
const DefaultTimeout = 2 * time.Minute

// Options are a run's settings, from the flags and CACHESERVICE_S3_*.
type Options struct {
	// Action is plan, apply or check.
	Action string
	Bucket string
	Prefix string
	// Namespaces are the namespaces the rules are for, and Source says
	// where they came from.
	Namespaces []string
	Source     string
	// TokensFile or Scope is the flag the namespaces came from, repeated in
	// the apply command plan prints.
	TokensFile, Scope string
	// Confirm is apply's -confirm: <bucket>/<prefix>.
	Confirm string
	// AllowRemovals lets apply remove rules of the command's that no
	// wanted rule replaces.
	AllowRemovals bool
	// AbortRule generates the rule aborting incomplete multipart uploads.
	AbortRule bool
	// MaxRules is the store's limit on a configuration's rules.
	MaxRules int
}

// ConfirmValue is what apply's -confirm must repeat: the bucket, a slash,
// and the prefix without surrounding slashes ("<bucket>/" for an empty
// prefix).
func ConfirmValue(bucket, prefix string) string {
	return bucket + "/" + strings.Trim(prefix, "/")
}

// Opener makes the Store for an S3 configuration.
type Opener func(ctx context.Context, cfg blobstore.S3Config) (Store, error)

// OpenS3 is the Opener of the real command: the service's own S3 client.
func OpenS3(ctx context.Context, cfg blobstore.S3Config) (Store, error) {
	client, err := blobstore.NewS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3Store{Client: client, Bucket: cfg.Bucket}, nil
}

const usage = `usage: cacheservice lifecycle [plan|apply|check] [flags]

  plan   print the rules the prefix wants and the diff against the bucket's (the default; changes nothing)
  apply  merge them into the bucket's lifecycle configuration; needs -confirm=<bucket>/<prefix>
         and the admin key (on DigitalOcean Spaces, a full-access key)
  check  exit 3 when a namespace lacks its staging rule, or a rule expires live data (read-only)

The bucket and credentials come from CACHESERVICE_S3_*, as for the service.
Exit codes: 0 ok, 1 error, 2 usage or confirmation, 3 check found something.

flags:
`

// Main runs the command with args (after "lifecycle"), and returns its exit
// code. It never prints the credentials.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer, open Opener) int {
	fs := flag.NewFlagSet("cacheservice lifecycle", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}
	o := Options{Action: "plan"}
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		o.Action, args = args[0], args[1:]
	}
	var tokensFile, scope string
	var timeout time.Duration
	fs.StringVar(&tokensFile, "tokens-file", "", "the service's tokens file: one staging rule per namespace it names (empty: -scope)")
	fs.StringVar(&scope, "scope", server.DefaultNamespace, "the one namespace when there is no -tokens-file, as for the service")
	fs.StringVar(&o.Confirm, "confirm", "", "apply: the bucket and prefix it changes, as <bucket>/<prefix>")
	fs.BoolVar(&o.AllowRemovals, "allow-removals", false, "apply: remove this command's rules that nothing wanted replaces, such as a namespace's no longer in the tokens file")
	fs.BoolVar(&o.AbortRule, "abort-rule", true, "include the rule aborting incomplete multipart uploads under the prefix (false on stores that refuse it, such as MinIO)")
	fs.IntVar(&o.MaxRules, "max-rules", MaxRules, "the store's limit on rules in a bucket's configuration")
	fs.DurationVar(&timeout, "timeout", DefaultTimeout, "bound on the whole run")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	fail := func(code int, format string, a ...any) int {
		fmt.Fprintf(stderr, "cacheservice lifecycle: "+format+"\n", a...)
		return code
	}
	if fs.NArg() > 0 {
		return fail(ExitUsage, "unexpected arguments %q: flags go after the action", fs.Args())
	}
	if !slices.Contains([]string{"plan", "apply", "check"}, o.Action) {
		fs.Usage()
		return fail(ExitUsage, "unknown action %q: want plan, apply or check", o.Action)
	}
	if timeout <= 0 || o.MaxRules <= 0 {
		return fail(ExitUsage, "-timeout and -max-rules must be positive")
	}
	if tokensFile != "" {
		tf, err := server.LoadTokenFile(tokensFile)
		if err != nil {
			return fail(ExitError, "%v", err)
		}
		o.Namespaces, o.Source, o.TokensFile = tf.NamespaceNames(), "the tokens file "+tokensFile, tokensFile
	} else {
		if err := layout.ValidateNamespace(scope); err != nil {
			return fail(ExitUsage, "-scope: %v", err)
		}
		o.Namespaces, o.Source, o.Scope = []string{scope}, "-scope, without a tokens file", scope
	}
	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		return fail(ExitUsage, "%v", err)
	}
	if cfg.Bucket == "" {
		return fail(ExitUsage, "%s is not set", blobstore.EnvBucket)
	}
	o.Bucket, o.Prefix = cfg.Bucket, cfg.Prefix
	if slices.ContainsFunc(strings.Split(strings.Trim(o.Prefix, "/"), "/"), func(s string) bool { return s == ".." || s == "." }) {
		return fail(ExitUsage, "%s is %q: it can't contain . or .. segments", blobstore.EnvPrefix, o.Prefix)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	st, err := open(ctx, cfg)
	if err != nil {
		return fail(ExitError, "%v", err)
	}
	return Run(ctx, o, st, stdout, stderr)
}

// Run runs an action against st, printing its report to out and why it
// failed to errOut.
func Run(ctx context.Context, o Options, st Store, out, errOut io.Writer) int {
	fail := func(code int, format string, a ...any) int {
		fmt.Fprintf(errOut, "cacheservice lifecycle %s: "+format+"\n", append([]any{o.Action}, a...)...)
		return code
	}
	root := layout.Root(o.Prefix)
	fmt.Fprintf(out, "bucket %s, prefix %q\n", o.Bucket, root)
	fmt.Fprintf(out, "namespaces, from %s (%d): %s\n", o.Source, len(o.Namespaces), strings.Join(o.Namespaces, ", "))
	if len(o.Namespaces) == 0 {
		fmt.Fprintln(out, "warning: no namespaces: no staging rules are wanted")
	}

	if o.Action == "apply" {
		// Before anything is read: a wrong bucket or prefix is refused at
		// once, whatever the store says.
		if o.Confirm == "" {
			return fail(ExitUsage, "needs -confirm=%s, repeating the bucket and prefix it changes", ConfirmValue(o.Bucket, o.Prefix))
		}
		if want := ConfirmValue(o.Bucket, o.Prefix); o.Confirm != want {
			return fail(ExitUsage, "-confirm=%s doesn't match the bucket and prefix it would change, %s; nothing written", o.Confirm, want)
		}
	}

	marker, merr := st.ReadMarker(ctx, o.Prefix)
	var other *ownership.ErrOtherLayout
	switch {
	case merr == nil:
		fmt.Fprintf(out, "ownership marker: layout %d, the service's\n", marker.Layout)
	case errors.Is(merr, ownership.ErrNoMarker):
		fmt.Fprintf(out, "ownership marker: none at %s\n", layout.MarkerKey(o.Prefix))
	case errors.As(merr, &other):
		fmt.Fprintf(out, "ownership marker: %v\n", merr)
	default:
		fmt.Fprintf(out, "ownership marker: can't read it: %v\n", merr)
	}
	if o.Action == "apply" && merr != nil {
		return fail(ExitError, "refusing a prefix the service doesn't own, as GC does: %v. "+
			"Start the service on the prefix first (a writer claims an empty one), or check %s", merr, blobstore.EnvPrefix)
	}

	current, err := st.GetLifecycle(ctx)
	if err != nil {
		return fail(ExitError, "%v", explainRead(err))
	}

	if o.Action == "check" {
		return runCheck(o, current.Rules, out, fail)
	}

	wanted, err := Rules(o.Prefix, o.Namespaces, o.AbortRule)
	if err != nil {
		return fail(ExitError, "%v", err)
	}
	p, err := NewPlan(o.Prefix, current.Rules, wanted, o.MaxRules)
	if err != nil {
		return fail(ExitError, "%v", err)
	}
	printPlan(out, o, p)
	if rep, err := Check(o.Prefix, p.Result, o.Namespaces); err == nil {
		printLive(out, rep)
	}
	removals := p.Count(Remove)
	if o.Action == "plan" {
		if p.Changed() {
			fmt.Fprintf(out, "plan only: nothing written. To write it, with the admin key and the same CACHESERVICE_S3_* settings:\n  %s\n", applyCommand(o, removals > 0))
		} else {
			fmt.Fprintln(out, "plan only: the bucket's rules are as wanted, nothing to apply")
		}
		return ExitOK
	}

	// apply
	if !p.Changed() {
		fmt.Fprintln(out, "apply: the bucket's rules are as wanted; nothing written")
		return ExitOK
	}
	if removals > 0 && !o.AllowRemovals {
		return fail(ExitUsage, "it would remove %d of this command's rules (the lines marked -): pass -allow-removals to remove them; nothing written", removals)
	}
	// S3 can't write the configuration conditionally: a change by someone
	// else between the read above and this write is lost, which the read
	// back below reports. Don't run apply while another tool changes the
	// rules.
	if err := st.PutLifecycle(ctx, Config{Rules: p.Result, TransitionDefaultMinimumObjectSize: current.TransitionDefaultMinimumObjectSize}); err != nil {
		return fail(ExitError, "%v; nothing written", explainWrite(err, o.AbortRule))
	}
	after, err := st.GetLifecycle(ctx)
	if err != nil {
		return fail(ExitError, "written, but can't read it back to verify: %v", explainRead(err))
	}
	if err := verify(o.Prefix, p, after.Rules, o.MaxRules); err != nil {
		return fail(ExitError, "written, but what the bucket holds now isn't what was written: %v", err)
	}
	fmt.Fprintf(out, "apply: written and read back: %d added, %d updated, %d replaced, %d removed; %d other rules unchanged\n",
		p.Count(Add), p.Count(Update), p.Count(Replace), removals, len(p.Foreign))
	return ExitOK
}

func runCheck(o Options, rules []types.LifecycleRule, out io.Writer, fail func(int, string, ...any) int) int {
	rep, err := Check(o.Prefix, rules, o.Namespaces)
	if err != nil {
		return fail(ExitError, "%v", err)
	}
	for _, ns := range o.Namespaces {
		if slices.Contains(rep.Missing, ns) {
			fmt.Fprintf(out, "MISSING  %s: no rule expires %s (GC expires its staged uploads itself)\n", ns, layout.Root(o.Prefix)+layout.NamespacesDir+ns+"/"+layout.StagingDir)
		} else {
			fmt.Fprintf(out, "ok       %s: its staging rule is there\n", ns)
		}
	}
	printLive(out, rep)
	for _, u := range rep.Unsafe {
		fmt.Fprintf(out, "UNSAFE   %s\n", u)
	}
	if rep.Abort {
		fmt.Fprintln(out, "ok       incomplete multipart uploads under the prefix are aborted by a rule")
	} else {
		fmt.Fprintln(out, "note     no rule aborts incomplete multipart uploads under the prefix (GC aborts stale ones at blob keys; MinIO refuses such a rule and drops them itself)")
	}
	if !rep.OK() {
		fmt.Fprintln(out, "check: rules are missing or too broad: run cacheservice lifecycle plan, then apply it with the admin key")
		return ExitFindings
	}
	fmt.Fprintln(out, "check: ok")
	return ExitOK
}

func printLive(out io.Writer, rep *Report) {
	nss := make([]string, 0, len(rep.Live))
	for ns := range rep.Live {
		nss = append(nss, ns)
	}
	slices.Sort(nss)
	for _, ns := range nss {
		fmt.Fprintf(out, "LIVE     %s: rules on %q expire its live data (blobs, bundles or the index the entries still name): limit them to its staging prefix\n", ns, rep.Live[ns])
	}
}

func printPlan(out io.Writer, o Options, p *Plan) {
	fmt.Fprintf(out, "this command's rules for the prefix (IDs %s<prefix hash>-...):\n", OwnedIDPrefix)
	marks := map[Kind]string{Keep: "=", Add: "+", Update: "~", Replace: "~", Remove: "-"}
	for _, c := range p.Changes {
		line := fmt.Sprintf("  %s %-8s %s  %s", marks[c.Kind], c.Kind, c.ID, c.Desc)
		switch c.Kind {
		case Update:
			line += "  (was " + c.OldDesc + ")"
		case Replace:
			line += "  (replaces " + c.OldID + ")"
		case Remove:
			line += "  (nothing wanted replaces it"
			if !o.AllowRemovals {
				line += "; apply needs -allow-removals"
			}
			line += ")"
		}
		fmt.Fprintln(out, line)
	}
	fmt.Fprintf(out, "other rules, written back as they are: %d\n", len(p.Foreign))
	for _, r := range p.Foreign {
		if pf, _ := filterPrefix(r); strings.HasPrefix(pf, layout.Root(o.Prefix)) || strings.HasPrefix(layout.Root(o.Prefix), pf) {
			fmt.Fprintf(out, "  note: %s covers the prefix: %s\n", aws.ToString(r.ID), describe(r))
		}
	}
	fmt.Fprintf(out, "result: %d rules (the store allows %d); %d to add, %d to update, %d to replace, %d to remove\n",
		len(p.Result), o.MaxRules, p.Count(Add), p.Count(Update), p.Count(Replace), p.Count(Remove))
	fmt.Fprintln(out, "this command's rules, as JSON:")
	b, _ := json.MarshalIndent(jsonRules(p.Wanted), "", "  ")
	fmt.Fprintln(out, string(b))
}

type jsonRule struct {
	ID         string
	Status     string
	Filter     struct{ Prefix string }
	Expiration *struct {
		Days int32
	} `json:",omitempty"`
	AbortIncompleteMultipartUpload *struct {
		DaysAfterInitiation int32
	} `json:",omitempty"`
}

// jsonRules are rules in the shape of the README's example (and of
// aws s3api's lifecycle.json).
func jsonRules(rules []types.LifecycleRule) map[string][]jsonRule {
	out := []jsonRule{}
	for _, r := range rules {
		j := jsonRule{ID: aws.ToString(r.ID), Status: string(r.Status)}
		j.Filter.Prefix, _ = filterPrefix(r)
		if r.Expiration != nil {
			j.Expiration = &struct{ Days int32 }{aws.ToInt32(r.Expiration.Days)}
		}
		if r.AbortIncompleteMultipartUpload != nil {
			j.AbortIncompleteMultipartUpload = &struct{ DaysAfterInitiation int32 }{aws.ToInt32(r.AbortIncompleteMultipartUpload.DaysAfterInitiation)}
		}
		out = append(out, j)
	}
	return map[string][]jsonRule{"Rules": out}
}

// applyCommand is the apply command for what plan just showed: the same
// namespaces' flag and the same options, so that pasting it applies that
// plan, not another one.
func applyCommand(o Options, removals bool) string {
	args := []string{"cacheservice", "lifecycle", "apply"}
	if o.TokensFile != "" {
		args = append(args, "-tokens-file", shellQuote(o.TokensFile))
	} else if o.Scope != "" {
		args = append(args, "-scope", shellQuote(o.Scope))
	}
	if !o.AbortRule {
		args = append(args, "-abort-rule=false")
	}
	if o.MaxRules != MaxRules {
		args = append(args, fmt.Sprintf("-max-rules=%d", o.MaxRules))
	}
	if removals {
		args = append(args, "-allow-removals")
	}
	return strings.Join(append(args, shellQuote("-confirm="+ConfirmValue(o.Bucket, o.Prefix))), " ")
}

// safeShell are the characters a word needs no quotes for.
var safeShell = regexp.MustCompile(`^[A-Za-z0-9._/=:@,+-]+$`)

func shellQuote(s string) string {
	if safeShell.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ruleKey is a rule in full, as the read back compares it.
func ruleKey(r types.LifecycleRule) string {
	b, err := json.Marshal(r)
	if err != nil {
		return aws.ToString(r.ID) + " " + describe(r)
	}
	return string(b)
}

// verify checks what the bucket holds after a write: the command's rules as
// wanted, and the other rules as they were.
func verify(prefix string, p *Plan, after []types.LifecycleRule, maxRules int) error {
	again, err := NewPlan(prefix, after, p.Wanted, maxRules)
	if err != nil {
		return err
	}
	if again.Changed() {
		for _, c := range again.Changes {
			if c.Kind != Keep {
				return fmt.Errorf("rule %s: %s", c.ID, c.Kind)
			}
		}
	}
	key := func(rules []types.LifecycleRule) []string {
		var out []string
		for _, r := range rules {
			out = append(out, ruleKey(r))
		}
		slices.Sort(out)
		return out
	}
	if !slices.Equal(key(p.Foreign), key(again.Foreign)) {
		return fmt.Errorf("the other rules aren't as they were written (changed by someone else at the same time, or by the store): %d written, %d now: %v", len(p.Foreign), len(again.Foreign), ids(again.Foreign))
	}
	return nil
}

func ids(rules []types.LifecycleRule) []string {
	out := make([]string, 0, len(rules))
	for _, r := range rules {
		out = append(out, aws.ToString(r.ID))
	}
	return out
}
