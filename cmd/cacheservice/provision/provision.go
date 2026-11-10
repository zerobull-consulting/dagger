// Package provision is `cacheservice provision`: the prefix's ownership
// marker and the bucket's lifecycle rules as two declarative resources, for
// Terraform's shell provider (scottwinkler/shell, the shell_script
// resource). hack/remote-cache/spaces uses it.
//
//	cacheservice provision claim create|read|update|delete
//	cacheservice provision rules create|read|update|delete
//
// It speaks the shell provider's contract: the settings come from the
// environment (CACHESERVICE_S3_*, as for the service, plus
// CACHESERVICE_NAMESPACES and CACHESERVICE_ABORT_RULE for rules), the
// resource's previous output comes on stdin (ignored: the bucket is the
// truth), and the resource's state goes to stdout as one JSON object of
// strings. Reports go to stderr. A read that prints nothing says the
// resource is gone, so Terraform creates it again; a read whose output
// differs from the state marks it for update.
//
// The work is lifecycle's own, with the same checks: claim is `lifecycle
// claim` (it refuses a prefix that holds keys and writes the marker with
// If-None-Match), and rules is `lifecycle apply -allow-removals` (it merges
// with other tools' rules, writes only rules CheckSafe passes, and reads
// them back). Unlike `cacheservice lifecycle`, it takes no -confirm:
// Terraform's plan and apply are the confirmation, and the bucket and
// prefix are the resource's.
//
//   - claim: create claims the prefix; read reports the marker, or nothing
//     when there is none; delete leaves the marker (removing it would
//     disown a prefix that holds a store: destroying the bucket removes it).
//   - rules: create and update apply the rules for CACHESERVICE_NAMESPACES;
//     read reports whether the bucket holds exactly them (in_sync); delete
//     removes this prefix's rules, and only those.
package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/lifecycle"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

const (
	// EnvNamespaces lists the namespaces the rules are for, separated by
	// commas or white space.
	EnvNamespaces = "CACHESERVICE_NAMESPACES"
	// EnvAbortRule is lifecycle's -abort-rule: true (the default) or false.
	EnvAbortRule = "CACHESERVICE_ABORT_RULE"
)

// Exit codes.
const (
	ExitOK    = 0
	ExitError = 1
	ExitUsage = 2
)

const usage = `usage: cacheservice provision claim|rules create|read|update|delete

For Terraform's shell provider (shell_script): settings from CACHESERVICE_S3_*,
and for rules CACHESERVICE_NAMESPACES (comma or space separated) and
CACHESERVICE_ABORT_RULE (true or false, default true). The resource's state is
printed to stdout as JSON; reports go to stderr. Use the admin key.
`

// Main runs the command with args (after "provision") and returns its exit
// code. It never prints the credentials.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer, open lifecycle.Opener) int {
	fail := func(code int, format string, a ...any) int {
		fmt.Fprintf(stderr, "cacheservice provision: "+format+"\n", a...)
		return code
	}
	if len(args) != 2 || !slices.Contains([]string{"claim", "rules"}, args[0]) ||
		!slices.Contains([]string{"create", "read", "update", "delete"}, args[1]) {
		fmt.Fprint(stderr, usage)
		return ExitUsage
	}
	resource, action := args[0], args[1]

	cfg, err := blobstore.S3ConfigFromEnv()
	if err != nil {
		return fail(ExitUsage, "%v", err)
	}
	if cfg.Bucket == "" {
		return fail(ExitUsage, "%s is not set", blobstore.EnvBucket)
	}
	if slices.ContainsFunc(strings.Split(strings.Trim(cfg.Prefix, "/"), "/"), func(s string) bool { return s == ".." || s == "." }) {
		return fail(ExitUsage, "%s is %q: it can't contain . or .. segments", blobstore.EnvPrefix, cfg.Prefix)
	}
	o := lifecycle.Options{
		Bucket:   cfg.Bucket,
		Prefix:   cfg.Prefix,
		Confirm:  lifecycle.ConfirmValue(cfg.Bucket, cfg.Prefix),
		MaxRules: lifecycle.MaxRules,
		Source:   EnvNamespaces,
	}
	if resource == "rules" {
		if o.Namespaces, err = namespaces(os.Getenv(EnvNamespaces)); err != nil {
			return fail(ExitUsage, "%v", err)
		}
		if o.AbortRule, err = boolSetting(os.Getenv(EnvAbortRule), true); err != nil {
			return fail(ExitUsage, "%s: %v", EnvAbortRule, err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, lifecycle.DefaultTimeout)
	defer cancel()
	st, err := open(ctx, cfg)
	if err != nil {
		return fail(ExitError, "%v", err)
	}

	p := &provisioner{o: o, st: st, stdout: stdout, stderr: stderr, fail: fail}
	switch resource + " " + action {
	case "claim create", "claim update":
		return p.claim(ctx)
	case "claim read":
		return p.readClaim(ctx)
	case "claim delete":
		fmt.Fprintf(stderr, "claim delete: leaving %s, which the bucket's deletion removes\n", layout.MarkerKey(o.Prefix))
		return ExitOK
	case "rules create", "rules update":
		return p.applyRules(ctx)
	case "rules read":
		return p.readRules(ctx)
	default: // rules delete
		return p.deleteRules(ctx)
	}
}

type provisioner struct {
	o      lifecycle.Options
	st     lifecycle.Store
	stdout io.Writer
	stderr io.Writer
	fail   func(int, string, ...any) int
}

// state prints the resource's state: one JSON object of strings.
func (p *provisioner) state(fields map[string]string) int {
	fields["bucket"] = p.o.Bucket
	fields["prefix"] = layout.Root(p.o.Prefix)
	b, err := json.Marshal(fields)
	if err != nil {
		return p.fail(ExitError, "%v", err)
	}
	fmt.Fprintf(p.stdout, "%s\n", b)
	return ExitOK
}

func (p *provisioner) markerState(m ownership.Marker) int {
	return p.state(map[string]string{
		"marker": layout.MarkerKey(p.o.Prefix),
		"layout": strconv.Itoa(m.Layout),
	})
}

func (p *provisioner) claim(ctx context.Context) int {
	o := p.o
	o.Action = "claim"
	if code := lifecycle.Run(ctx, o, p.st, p.stderr, p.stderr); code != lifecycle.ExitOK {
		return code
	}
	m, err := p.st.ReadMarker(ctx, p.o.Prefix)
	if err != nil {
		return p.fail(ExitError, "claimed, but can't read the marker back: %v", err)
	}
	return p.markerState(m)
}

func (p *provisioner) readClaim(ctx context.Context) int {
	m, err := p.st.ReadMarker(ctx, p.o.Prefix)
	if errors.Is(err, ownership.ErrNoMarker) {
		fmt.Fprintf(p.stderr, "claim read: no marker at %s\n", layout.MarkerKey(p.o.Prefix))
		return ExitOK // gone: Terraform claims it again
	}
	if err != nil {
		return p.fail(ExitError, "%v", err)
	}
	return p.markerState(m)
}

func (p *provisioner) rulesState(inSync bool) int {
	return p.state(map[string]string{
		"namespaces": strings.Join(p.o.Namespaces, ","),
		"abort_rule": strconv.FormatBool(p.o.AbortRule),
		"in_sync":    strconv.FormatBool(inSync),
	})
}

func (p *provisioner) applyRules(ctx context.Context) int {
	o := p.o
	o.Action = "apply"
	// The namespaces are declared: a rule of a namespace no longer listed
	// is meant to go.
	o.AllowRemovals = true
	if code := lifecycle.Run(ctx, o, p.st, p.stderr, p.stderr); code != lifecycle.ExitOK {
		return code
	}
	return p.rulesState(true)
}

// readRules: the state, in sync when applying would change nothing. Out of
// sync, the output differs from the state, so Terraform updates (applies).
func (p *provisioner) readRules(ctx context.Context) int {
	current, err := p.st.GetLifecycle(ctx)
	if err != nil {
		return p.fail(ExitError, "%v", err)
	}
	wanted, err := lifecycle.Rules(p.o.Prefix, p.o.Namespaces, p.o.AbortRule)
	if err != nil {
		return p.fail(ExitError, "%v", err)
	}
	plan, err := lifecycle.NewPlan(p.o.Prefix, current.Rules, wanted, p.o.MaxRules)
	if err != nil {
		return p.fail(ExitError, "%v", err)
	}
	if plan.Changed() {
		fmt.Fprintf(p.stderr, "rules read: out of sync: %d to add, %d to update, %d to replace, %d to remove\n",
			plan.Count(lifecycle.Add), plan.Count(lifecycle.Update), plan.Count(lifecycle.Replace), plan.Count(lifecycle.Remove))
	}
	return p.rulesState(!plan.Changed())
}

// deleteRules removes the prefix's rules: apply with no namespaces and no
// abort rule. Other tools' rules, and other prefixes', stay. A prefix
// without its marker (the bucket emptied or gone) has nothing to remove
// that apply would touch.
func (p *provisioner) deleteRules(ctx context.Context) int {
	if _, err := p.st.ReadMarker(ctx, p.o.Prefix); errors.Is(err, ownership.ErrNoMarker) {
		fmt.Fprintf(p.stderr, "rules delete: no marker at %s: nothing to remove\n", layout.MarkerKey(p.o.Prefix))
		return ExitOK
	}
	o := p.o
	o.Action, o.Namespaces, o.AbortRule, o.AllowRemovals = "apply", nil, false, true
	return lifecycle.Run(ctx, o, p.st, p.stderr, p.stderr)
}

// namespaces parses CACHESERVICE_NAMESPACES: at least one, each valid,
// without repeats.
func namespaces(s string) ([]string, error) {
	var out []string
	for _, ns := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' }) {
		if err := layout.ValidateNamespace(ns); err != nil {
			return nil, fmt.Errorf("%s: %v", EnvNamespaces, err)
		}
		if !slices.Contains(out, ns) {
			out = append(out, ns)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s is not set: list the namespaces the rules are for", EnvNamespaces)
	}
	return out, nil
}

func boolSetting(s string, def bool) (bool, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseBool(s)
}
