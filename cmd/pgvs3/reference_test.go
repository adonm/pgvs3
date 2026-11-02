package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

var updateReference = flag.Bool("update", false, "rewrite docs/reference.md from the flag sets")

const referencePath = "../../docs/reference.md"

const metricsHeading = "## Metrics"

func renderReference(t *testing.T) string {
	t.Helper()
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "PGVS3_") {
			value := os.Getenv(name)
			os.Unsetenv(name)
			t.Cleanup(func() { os.Setenv(name, value) })
		}
	}
	sets := map[string]*flag.FlagSet{}
	describeFlags = func(name string, fs *flag.FlagSet) { sets[name] = fs }
	t.Cleanup(func() { describeFlags = nil })
	for _, c := range commandTable {
		c.run(nil)
	}

	var b strings.Builder
	b.WriteString("# CLI and metrics reference\n\n")
	b.WriteString("<!-- Generated from the binary's flag sets by `just reference`; `go test ./cmd/pgvs3` fails on drift.\n")
	b.WriteString("     The metrics block is checked against a live /metrics scrape by `just contract`. -->\n\n")
	b.WriteString("Every `serve` flag can also be set with `PGVS3_<FLAG>` (upper case, `-` becomes `_`); an explicit\n")
	b.WriteString("flag wins. Maintenance commands default `--url` from `PGVS3_URL`; `reap` also reads\n")
	b.WriteString("`PGVS3_REAP_GRACE` and `PGVS3_JOB_RETENTION`, `health` reads `PGVS3_HEALTH_URL` and `doctor`\n")
	b.WriteString("reads `PGVS3_POOL_MAX`.\n\n")
	b.WriteString("## Commands\n\n| Command | Purpose |\n| --- | --- |\n")
	for _, c := range commandTable {
		fmt.Fprintf(&b, "| [`%s`](#pgvs3-%s) | %s |\n", c.name, c.name, c.summary)
	}
	for _, c := range commandTable {
		fmt.Fprintf(&b, "\n## `pgvs3 %s`\n\n%s\n", c.name, c.summary)
		fs := sets[c.name]
		if fs == nil {
			b.WriteString("\nNo flags.\n")
			continue
		}
		if c.name == "serve" {
			b.WriteString("\n| Flag | Environment | Default | Description |\n| --- | --- | --- | --- |\n")
		} else {
			b.WriteString("\n| Flag | Default | Description |\n| --- | --- | --- |\n")
		}
		fs.VisitAll(func(f *flag.Flag) {
			if strings.Contains(f.Usage, "`") {
				t.Errorf("%s --%s: back-quotes in usage become the --help placeholder name", c.name, f.Name)
			}
			def := "`" + f.DefValue + "`"
			if f.DefValue == "" {
				def = "—"
			}
			usage := strings.ReplaceAll(f.Usage, "|", `\|`)
			if c.name == "serve" {
				env := "PGVS3_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
				fmt.Fprintf(&b, "| `--%s` | `%s` | %s | %s |\n", f.Name, env, def, usage)
			} else {
				fmt.Fprintf(&b, "| `--%s` | %s | %s |\n", f.Name, def, usage)
			}
		})
	}
	return b.String()
}

func TestReferenceMatchesFlags(t *testing.T) {
	want := renderReference(t)
	raw, err := os.ReadFile(referencePath)
	if err != nil && !*updateReference {
		t.Fatalf("%v (run `just reference`)", err)
	}
	current := string(raw)
	cli, metrics, found := strings.Cut(current, metricsHeading)
	if !found {
		metrics = "\n\n<!-- metrics:begin -->\n<!-- metrics:end -->\n"
	}
	if *updateReference {
		if err := os.WriteFile(referencePath, []byte(want+"\n"+metricsHeading+metrics), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if strings.TrimSpace(cli) != strings.TrimSpace(want) {
		t.Fatalf("docs/reference.md is out of date with the CLI flags; run `just reference`")
	}
}

func TestEveryCommandHasASummary(t *testing.T) {
	for _, c := range commandTable {
		if c.summary == "" || !strings.HasSuffix(c.summary, ".") {
			t.Fatalf("command %q needs a one-sentence summary", c.name)
		}
	}
}
