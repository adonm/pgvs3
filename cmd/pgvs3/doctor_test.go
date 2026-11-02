package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/adonm/pgvs3/internal/store"
)

func healthy() (store.Diagnosis, store.ServerFacts) {
	return store.Diagnosis{ServerVersion: 180006, Layout: store.LayoutVersion, ChunksConfigured: true},
		store.ServerFacts{
			Settings: map[string]string{
				"server_version": "18.6", "max_connections": "256", "superuser_reserved_connections": "3",
				"reserved_connections": "0", "fsync": "on", "full_page_writes": "on",
				"synchronous_commit": "on", "max_wal_size": "4GB",
			},
			MaxWALBytes: 4 << 30,
		}
}

func statusOf(checks []check, name string) check {
	for _, c := range checks {
		if c.Name == name {
			return c
		}
	}
	return check{}
}

func TestDoctorHealthyClusterHasNoFailures(t *testing.T) {
	d, f := healthy()
	for _, c := range doctorChecks(d, f, fleetPlan{PoolMax: 64, Surge: 1}) {
		if c.Status == statusFail || c.Status == statusWarn {
			t.Fatalf("unexpected %s %s: %s", c.Status, c.Name, c.Detail)
		}
	}
}

func TestDoctorConnectionArithmetic(t *testing.T) {
	_, f := healthy()
	cases := []struct {
		plan fleetPlan
		want checkStatus
		need string
	}{
		{fleetPlan{Gateways: 0, PoolMax: 64, Surge: 1}, statusInfo, "needs up to 128"},
		{fleetPlan{Gateways: 1, PoolMax: 64, Surge: 1}, statusFail, "need 256 = (1 gateways + 1 surge) × (64 main + 64 chunk) + 0 other; 253 usable"},
		{fleetPlan{Gateways: 2, PoolMax: 16, Surge: 1, Other: 100}, statusPass, "need 196"},
		{fleetPlan{Gateways: 3, PoolMax: 16, Surge: 1, Other: 100}, statusWarn, "need 228"},
		{fleetPlan{Gateways: 1, PoolMax: 100, Surge: 0}, statusPass, "(100 main + 64 chunk)"},
	}
	for _, tc := range cases {
		c := connectionCheck(f, tc.plan)
		if c.Status != tc.want || !strings.Contains(c.Detail, tc.need) {
			t.Fatalf("%+v: got %s %q", tc.plan, c.Status, c.Detail)
		}
	}
	if c := connectionCheck(f, fleetPlan{Gateways: 1, PoolMax: 64, Surge: 1}); !strings.Contains(c.Fix, "at least 259") {
		t.Fatalf("fix should account for reserved slots: %q", c.Fix)
	}
}

func TestDoctorDurabilityAndWAL(t *testing.T) {
	d, f := healthy()
	f.Settings["fsync"] = "off"
	f.Settings["synchronous_commit"] = "off"
	f.MaxWALBytes = 1 << 30
	f.Settings["max_wal_size"] = "1GB"
	checks := doctorChecks(d, f, fleetPlan{PoolMax: 64})
	for name, want := range map[string]checkStatus{"durability": statusFail, "synchronous_commit": statusWarn, "max_wal_size": statusWarn} {
		if got := statusOf(checks, name); got.Status != want || got.Fix == "" {
			t.Fatalf("%s: got %+v want %s with a fix", name, got, want)
		}
	}
	f.Settings["synchronous_commit"] = "local"
	f.Settings["synchronous_standby_names"] = "FIRST 1 (a)"
	if got := statusOf(doctorChecks(d, f, fleetPlan{PoolMax: 64}), "synchronous_commit"); got.Status != statusWarn {
		t.Fatalf("local with sync standbys: %+v", got)
	}
}

func TestDoctorIntegrityFailuresCarryFixes(t *testing.T) {
	d, f := healthy()
	d.ServerVersion, d.Layout, d.OrphanSchemas, d.MissingSchemas = 170000, store.LayoutVersion-1, 1, 2
	f.DeadUploads, f.PendingUploads, f.InRecovery = 1, 3, true
	checks := doctorChecks(d, f, fleetPlan{PoolMax: 64})
	for _, name := range []string{"postgres", "layout", "orphan schemas", "missing schemas"} {
		if got := statusOf(checks, name); got.Status != statusFail || got.Fix == "" {
			t.Fatalf("%s: %+v", name, got)
		}
	}
	if got := statusOf(checks, "pending uploads"); got.Status != statusWarn || !strings.Contains(got.Fix, "pending retry") {
		t.Fatalf("pending: %+v", got)
	}
	var buf bytes.Buffer
	printChecks(&buf, checks)
	if !strings.Contains(buf.String(), "FAIL  layout") || !strings.Contains(buf.String(), "fix: ") ||
		!strings.Contains(buf.String(), "--read-only") {
		t.Fatalf("output:\n%s", buf.String())
	}
}
