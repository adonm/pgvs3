package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

type checkStatus string

const (
	statusPass checkStatus = "PASS"
	statusWarn checkStatus = "WARN"
	statusFail checkStatus = "FAIL"
	statusInfo checkStatus = "INFO"
)

type check struct {
	Status checkStatus `json:"status"`
	Name   string      `json:"name"`
	Detail string      `json:"detail"`
	Fix    string      `json:"fix,omitempty"`
}

// fleetPlan is the deployment doctor checks connection capacity against.
// Gateways == 0 means the fleet size is unknown, so capacity is only reported.
type fleetPlan struct {
	Gateways, Surge, PoolMax, Other int
}

func (p fleetPlan) perGateway() int { return p.PoolMax + min(p.PoolMax, 64) }

func doctor(args []string) int {
	var plan fleetPlan
	var asJSON *bool
	return withStore("doctor", args, 30*time.Second, false, func(fs *flag.FlagSet) {
		fs.IntVar(&plan.Gateways, "gateways", 0, "writable+read-only gateways sharing this database (0 skips the capacity check)")
		fs.IntVar(&plan.Surge, "surge", 1, "extra gateways that run during a rolling update")
		fs.IntVar(&plan.PoolMax, "pool-max", envInt("PGVS3_POOL_MAX", 64), "gateway --pool-max")
		fs.IntVar(&plan.Other, "other-connections", 0, "connections reserved for applications, poolers and admin")
		asJSON = fs.Bool("json", false, "print JSON")
	}, func(ctx context.Context, st *store.Store, _ []string) int {
		d, err := st.Diagnose(ctx)
		if err != nil {
			return fail(err)
		}
		facts, err := st.ServerFacts(ctx)
		if err != nil {
			return fail(err)
		}
		checks := doctorChecks(d, facts, plan)
		if !facts.InRecovery {
			if err := st.EnsureMaintenance(ctx); err != nil {
				checks = append(checks, check{statusFail, "schema drift", err.Error(), "run a writable `pgvs3 serve` of this version once"})
			}
		}
		checks = append(checks, checkOriginSpecs(ctx, st))
		if *asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(map[string]any{"diagnosis": d, "facts": facts, "checks": checks})
		} else {
			fmt.Printf("server_version_num=%d aurora_version=%q layout=%d namespaces=%d over_supported_scale=%v buckets=%d queues=%d caches=%d configured=%v objects=%d garbage=%d garbage_referenced=%d orphan_schemas=%d missing_schemas=%d db=%d MiB\n",
				d.ServerVersion, d.AuroraVersion, d.Layout, d.Namespaces, d.OverScale, d.Buckets, d.Queues, d.Caches,
				d.ChunksConfigured, d.Objects, d.GarbageFiles, d.GarbageReferenced,
				d.OrphanSchemas, d.MissingSchemas, d.DatabaseBytes>>20)
			printChecks(os.Stdout, checks)
		}
		for _, c := range checks {
			if c.Status == statusFail {
				return 1
			}
		}
		return 0
	})
}

func checkOriginSpecs(ctx context.Context, st *store.Store) check {
	namespaces, err := st.NamespacesWithAttr(ctx, "origin")
	if err != nil {
		return check{statusFail, "origin specs", err.Error(), "fix database access, then re-run pgvs3 doctor"}
	}
	var invalid []string
	for _, n := range namespaces {
		raw, err := json.Marshal(n.Attrs["origin"])
		if err != nil {
			invalid = append(invalid, fmt.Sprintf("%s %s: %v", n.Kind, n.Name, err))
			continue
		}
		var spec objcache.Spec
		if err := json.Unmarshal(raw, &spec); err != nil {
			invalid = append(invalid, fmt.Sprintf("%s %s: invalid origin JSON: %v", n.Kind, n.Name, err))
			continue
		}
		bucket := ""
		if n.Kind == ns.Bucket {
			bucket = n.Name
			if spec.Bucket == "" {
				spec.Bucket, spec.ScopeKind = n.Name, "bucket"
			}
		}
		if err := objcache.ValidateSpec(bucket, spec); err != nil {
			invalid = append(invalid, fmt.Sprintf("%s %s: %v", n.Kind, n.Name, err))
		}
	}
	if len(invalid) > 0 {
		return check{statusFail, "origin specs", strings.Join(invalid, "; "), "re-run pgvs3 origin set or clear the invalid origin"}
	}
	return check{statusPass, "origin specs", fmt.Sprintf("%d persisted, all valid", len(namespaces)), ""}
}

func printChecks(w io.Writer, checks []check) {
	for _, c := range checks {
		fmt.Fprintf(w, "%-4s  %-18s %s\n", c.Status, c.Name, c.Detail)
		if c.Fix != "" {
			fmt.Fprintf(w, "      %-18s fix: %s\n", "", c.Fix)
		}
	}
}

func doctorChecks(d store.Diagnosis, f store.ServerFacts, plan fleetPlan) []check {
	var out []check
	add := func(ok bool, name, detail, fix string) {
		if ok {
			out = append(out, check{statusPass, name, detail, ""})
		} else {
			out = append(out, check{statusFail, name, detail, fix})
		}
	}
	add(d.ServerVersion >= 180000, "postgres", fmt.Sprintf("PostgreSQL %s", f.Settings["server_version"]),
		"pgvs3 requires PostgreSQL 18 or newer")
	add(d.Layout == store.LayoutVersion, "layout", fmt.Sprintf("storage layout %d (binary expects %d)", d.Layout, store.LayoutVersion),
		"start one writable `pgvs3 serve` of this version to migrate, or run the matching binary")
	add(d.ChunksConfigured && len(d.FunctionIssues) == 0, "schema contract",
		contractDetail(d), "start one writable `pgvs3 serve` of this version; it recreates functions and chunk settings")
	add(d.OrphanSchemas == 0, "orphan schemas", fmt.Sprintf("%d s3b_/s3c_/s3w_/s3q_ schemas missing from the catalog", d.OrphanSchemas),
		"`pgvs3 adopt --all` registers them, or drop them if they are leftovers")
	add(d.MissingSchemas == 0, "missing schemas", fmt.Sprintf("%d catalog namespaces without a schema", d.MissingSchemas),
		"restore the schema from backup, or remove the namespace (operations.md)")
	add(d.GarbageReferenced == 0, "garbage", fmt.Sprintf("%d queued garbage files, %d still referenced", d.GarbageFiles, d.GarbageReferenced),
		"do not reap manually; a referenced garbage file is a bug, keep the database and report it")
	add(!d.OverScale, "namespaces", fmt.Sprintf("%d of %d supported", d.Namespaces, store.SupportedNamespaces),
		"consolidate namespaces or split across databases")

	if on(f.Settings["fsync"]) && on(f.Settings["full_page_writes"]) {
		out = append(out, check{statusPass, "durability", "fsync=on full_page_writes=on", ""})
	} else {
		out = append(out, check{statusFail, "durability", fmt.Sprintf("fsync=%s full_page_writes=%s: a crash can corrupt acknowledged objects",
			f.Settings["fsync"], f.Settings["full_page_writes"]), "ALTER SYSTEM SET fsync = on; ALTER SYSTEM SET full_page_writes = on; then restart"})
	}
	switch sc := f.Settings["synchronous_commit"]; sc {
	case "off":
		out = append(out, check{statusWarn, "synchronous_commit", "off: a crash can lose PUTs that already returned 200",
			"ALTER SYSTEM SET synchronous_commit = on; SELECT pg_reload_conf();"})
	case "local":
		if f.Settings["synchronous_standby_names"] != "" {
			out = append(out, check{statusWarn, "synchronous_commit", "local with synchronous standbys: failover can lose acknowledged PUTs",
				"use on/remote_write/remote_apply if promotion must keep every acknowledged write"})
			break
		}
		fallthrough
	default:
		out = append(out, check{statusPass, "synchronous_commit", sc, ""})
	}
	if f.MaxWALBytes > 0 && f.MaxWALBytes < 2<<30 {
		out = append(out, check{statusWarn, "max_wal_size", fmt.Sprintf("%s: a 1 GiB PUT writes ~1 GiB of WAL and can force a checkpoint mid-write", f.Settings["max_wal_size"]),
			"ALTER SYSTEM SET max_wal_size = '4GB'; SELECT pg_reload_conf();"})
	} else {
		out = append(out, check{statusPass, "max_wal_size", f.Settings["max_wal_size"], ""})
	}
	out = append(out, connectionCheck(f, plan))
	out = append(out, check{statusInfo, "memory/io", fmt.Sprintf("shared_buffers=%s io_method=%s wal_level=%s",
		f.Settings["shared_buffers"], f.Settings["io_method"], f.Settings["wal_level"]), ""})

	if f.DeadUploads > 0 {
		out = append(out, check{statusWarn, "pending uploads", fmt.Sprintf("%d dead of %d (%d MiB); dead uploads stop retrying and keep their only copy in PostgreSQL",
			f.DeadUploads, f.PendingUploads, f.PendingBytes>>20), "fix the origin, then `pgvs3 pending retry`"})
	} else {
		out = append(out, check{statusPass, "pending uploads", fmt.Sprintf("%d waiting (%d MiB), oldest %s", f.PendingUploads, f.PendingBytes>>20,
			(time.Duration(f.OldestPendingSecs) * time.Second).String()), ""})
	}
	if f.InRecovery {
		out = append(out, check{statusInfo, "role", "standby: serve it only with --read-only", ""})
	} else {
		out = append(out, check{statusInfo, "role", fmt.Sprintf("primary with %d streaming standbys, max replay lag %d MiB", f.Standbys, f.MaxReplayLagBytes>>20), ""})
	}
	return out
}

func connectionCheck(f store.ServerFacts, plan fleetPlan) check {
	maxConns := atoi(f.Settings["max_connections"])
	usable := maxConns - atoi(f.Settings["superuser_reserved_connections"]) - atoi(f.Settings["reserved_connections"])
	if plan.Gateways <= 0 {
		return check{statusInfo, "connections", fmt.Sprintf("%d usable, %d in use; each gateway at --pool-max %d needs up to %d (pass --gateways to check a fleet)",
			usable, f.ClientBackends, plan.PoolMax, plan.perGateway()), ""}
	}
	need := (plan.Gateways+plan.Surge)*plan.perGateway() + plan.Other
	detail := fmt.Sprintf("need %d = (%d gateways + %d surge) × (%d main + %d chunk) + %d other; %d usable",
		need, plan.Gateways, plan.Surge, plan.PoolMax, min(plan.PoolMax, 64), plan.Other, usable)
	switch {
	case need > usable:
		perGW := max((usable-plan.Other)/max(plan.Gateways+plan.Surge, 1), 0)
		return check{statusFail, "connections", detail, fmt.Sprintf("raise max_connections to at least %d, or run --pool-max %d",
			need+maxConns-usable, max(perGW/2, 1))}
	case need*10 > usable*9:
		return check{statusWarn, "connections", detail + " (over 90%)", "leave headroom for maintenance and failover"}
	default:
		return check{statusPass, "connections", detail, ""}
	}
}

func contractDetail(d store.Diagnosis) string {
	if len(d.FunctionIssues) == 0 && d.ChunksConfigured {
		return "chunk storage settings and SQL functions match"
	}
	parts := []string{}
	if !d.ChunksConfigured {
		parts = append(parts, "chunk tables lack toast_tuple_target=8160 or the claim index")
	}
	parts = append(parts, d.FunctionIssues...)
	return strings.Join(parts, "; ")
}

func on(v string) bool { return v == "on" }

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}
