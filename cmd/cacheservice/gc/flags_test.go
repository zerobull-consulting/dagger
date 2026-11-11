package gc

import (
	"flag"
	"strings"
	"testing"
)

// The service's flags keep their names, defaults and help text (the service
// moved onto RegisterFlags); the subcommand's set is the same without
// -gc-dry-run, and with a -namespaces-file that says nothing of SIGHUP.
func TestRegisterFlags(t *testing.T) {
	want := map[string]string{
		"namespaces-file": "",
		"max-age":         "0",
		"max-bytes":       "0",
		"gc-dry-run":      "false",
		"gc-grace":        DefaultGrace.String(),
		"gc-staging-age":  DefaultStagingAge.String(),
		"gc-max-deletes":  "5000",
	}
	service := flag.NewFlagSet("service", flag.ContinueOnError)
	RegisterFlags(service, true)
	job := flag.NewFlagSet("job", flag.ContinueOnError)
	RegisterFlags(job, false)
	for name, def := range want {
		f := service.Lookup(name)
		if f == nil || f.DefValue != def {
			t.Errorf("service -%s: %+v, want default %q", name, f, def)
		}
		g := job.Lookup(name)
		if name == "gc-dry-run" {
			if g != nil {
				t.Error("the subcommand takes -gc-dry-run: no -confirm is its dry run")
			}
			continue
		}
		if g == nil || g.DefValue != def {
			t.Errorf("job -%s: %+v, want default %q", name, g, def)
		}
		if name != "namespaces-file" && g != nil && g.Usage != f.Usage {
			t.Errorf("-%s: the two sets' help differs:\n%s\n%s", name, f.Usage, g.Usage)
		}
	}
	if u := service.Lookup("namespaces-file").Usage; !strings.Contains(u, "SIGHUP reloads it") {
		t.Errorf("the service's -namespaces-file help changed: %s", u)
	}
	if u := job.Lookup("namespaces-file").Usage; strings.Contains(u, "SIGHUP") {
		t.Errorf("a one-shot job's -namespaces-file help mentions SIGHUP: %s", u)
	}
}

func TestFlagsLimitsAndCollector(t *testing.T) {
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	f := RegisterFlags(fs, true)
	if err := fs.Parse([]string{"-max-age=720h", "-max-bytes=1GiB", "-gc-dry-run", "-gc-grace=3h", "-gc-max-deletes=7"}); err != nil {
		t.Fatal(err)
	}
	lim, err := f.Limits()
	if err != nil || lim.MaxAge.Hours() != 720 || lim.MaxBytes != 1<<30 {
		t.Fatalf("limits %+v, %v", lim, err)
	}
	c := f.Collector()
	if !c.DryRun || c.Grace.Hours() != 3 || c.MaxDeletes != 7 || c.StagingAge != DefaultStagingAge {
		t.Fatalf("collector %+v", c)
	}
	if err := fs.Parse([]string{"-max-age=soon"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Limits(); err == nil || !strings.Contains(err.Error(), "-max-age") {
		t.Fatalf("a bad -max-age: %v", err)
	}
}
