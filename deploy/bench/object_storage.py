#!/usr/bin/env python3
"""SlateDB ObjFS profiles in the existing DuckDB/PostgreSQL benchmark harness."""
import argparse
import json
import math
import os
from pathlib import Path
import platform
import resource
import re
import statistics
import subprocess
import sys
import time
import uuid

from differential import (Cohort, Storage, api, checked, delta, digest, extension_platform,
                          gateway_metrics, ident, inventory, pg_sql, quote, read_worker,
                          redact, reference, save, successful_requests, summarize)


ROOT = Path(__file__).resolve().parents[2]
VERSION = "0.2.0"
COMMIT = "56d9c7b928836cfaffa6790c77b7640d50102fe3"
DUCKDB = "2.0.0"
SHA256 = {"linux_amd64": "dfd7040b4a0204aaa9aff142932ae7b5d2ec78634541d2c9f6e3bc0f329b4e12"}
PROFILES = ("ducklake", "objfs-default", "objfs-lean")
CONFIGS = {
    "objfs-default": {"async_threads": None, "data_cache": 512 << 20, "metadata_cache": 128 << 20, "shards": 0},
    "objfs-lean": {"async_threads": 4, "data_cache": 256 << 20, "metadata_cache": 64 << 20, "shards": 4},
}


def provision(override=None):
    platform_name = extension_platform()
    if override:
        path = Path(override).expanduser().resolve(strict=True)
        return {"source": "explicit override", "path": str(path), "sha256": digest(path), "unsigned": True,
                "expected_version": VERSION, "duckdb": DUCKDB, "platform": platform_name}
    if platform_name not in SHA256:
        raise RuntimeError(f"No qualified ObjFS artifact pin for {platform_name}; supply --object-storage-extension for an upstream-supported platform")
    folder = ROOT / ".tmp/deps/duckdb-object-storage" / VERSION
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / ("v" + DUCKDB) / platform_name / "duckdb_object_storage.duckdb_extension"
    url = f"https://community-extensions.duckdb.org/v{DUCKDB}/{platform_name}/duckdb_object_storage.duckdb_extension.gz"
    if not path.exists():
        import duckdb
        checked(duckdb.__version__, DUCKDB, "ObjFS provisioning DuckDB target")
        with duckdb.connect(config={"extension_directory": str(folder)}) as con:
            con.install_extension("duckdb_object_storage", repository="community")
    if digest(path) != SHA256[platform_name]:
        raise RuntimeError(f"ObjFS artifact differs from the pin; keep a verified cache or explicitly update the pin, never silently use latest: {path}")
    return {"source": "signed community artifact", "version": VERSION, "commit": COMMIT, "duckdb": DUCKDB,
            "platform": platform_name, "path": str(path), "sha256": SHA256[platform_name], "url": url, "unsigned": False}


def stats(con):
    result = {}
    for label, function in (("cache", "duckdb_objfs_cache_stats"), ("io", "duckdb_objfs_io_stats")):
        result[label] = [json.loads(row[0]) for row in con.execute(f"SELECT to_json(r) FROM {function}() r").fetchall()]
    result["settings"] = dict(con.execute("SELECT name,value FROM duckdb_settings() WHERE name LIKE 'duckdb_objfs_%' OR name IN ('threads','async_threads','memory_limit')").fetchall())
    return result


def peak_rss():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if platform.system() == "Darwin" else value * 1024


def connection(job, reader=False):
    from duck_bench import connect
    start = time.perf_counter()
    endpoint = os.environ["PGVS3_TEST_ENDPOINT_B" if reader else "PGVS3_TEST_ENDPOINT_A"]
    endpoint = job.get("endpoint", endpoint)
    explicit = job.get("unsigned") and job["profile"] != "ducklake"
    con = connect(endpoint, 4, unsigned_extension=job["extension"] if explicit else None)
    con.execute("SET memory_limit='2GB'; SET http_client_pool_capacity=16; SET parquet_metadata_cache=false")
    client = Path(job["folder"]) / "client" / str(job.get("generation", 0))
    client.mkdir(parents=True, exist_ok=False)
    con.execute("SET temp_directory=" + quote(client / "spill"))
    if job["profile"] != "ducklake":
        if not explicit:
            con.execute("SET extension_directory=" + quote(Path(job["extension"]).parents[2]))
            con.install_extension("duckdb_object_storage", repository="community")
            con.load_extension("duckdb_object_storage")
        checked(con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='duckdb_object_storage'").fetchone()[0], VERSION, "ObjFS version")
        config = CONFIGS[job["profile"]]
        if config["async_threads"] is not None:
            con.execute(f"SET async_threads={config['async_threads']}")
        for name, value in (("memory_cache_size", config["data_cache"]), ("metadata_cache_size", config["metadata_cache"]),
                            ("cache_shards", config["shards"])):
            con.execute(f"SET duckdb_objfs_{name}={value}")
        con.execute("SET duckdb_objfs_persistent_cache_path=''")
        con.execute("SET duckdb_objfs_backend='s3'; SET duckdb_objfs_bucket=" + quote(job["bucket"]) + "; SET duckdb_objfs_root='state'")
    return con, time.perf_counter() - start


def attach_sql(job, reader=False):
    if job["profile"] == "ducklake":
        return (f"ATTACH {quote('ducklake:postgres:' + os.environ['PGVS3_CATALOG'])} AS lake "
                f"(DATA_PATH {quote(job['location'])}, METADATA_SCHEMA {quote(job['metadata_schema'])})")
    return "ATTACH 'duckdb_objfs://bench.db' AS lake" + (" (READ_ONLY)" if reader else "")


class ObjStorage(Storage):
    def snapshot(self, connection=None):
        result = super().snapshot(None)
        if connection is not None and self.job["profile"] != "ducklake":
            result["object_storage"] = stats(connection)
        result["client_peak_rss_bytes"] = peak_rss()
        return result


def validate(job):
    root = Path(os.environ["PGVS3_TEST_RUN_DIR"]).resolve() / "object-storage"
    marker = json.loads((root / "ownership.json").read_text())
    if job["owner"] != marker["run"] or not Path(job["folder"]).resolve().is_relative_to(root) or Path(job["folder"]).resolve() == root:
        raise RuntimeError("ObjFS worker does not own this run/directory")
    pattern = "os-" + job["owner"] + r"-(?:proof|m[0-9]+-p[0-9]+-(?:ducklake|objfs-default|objfs-lean))"
    if not re.fullmatch(pattern, job["bucket"]) or job["location"] != f"s3://{job['bucket']}/state/":
        raise RuntimeError("ObjFS worker does not own this provider namespace")
    if job["profile"] == "ducklake" and not job["metadata_schema"].startswith(job["owner"] + "_"):
        raise RuntimeError("ObjFS worker does not own this catalog schema")


def worker_call(job, action):
    validate(job)
    folder = Path(job["folder"])
    folder.mkdir(parents=True, exist_ok=False)
    save(folder / "job.json", job)
    with (folder / "worker.log").open("w") as log:
        process = subprocess.run([sys.executable, str(Path(__file__).resolve()), "worker", action, str(folder / "job.json")],
                                 stdout=log, stderr=subprocess.STDOUT, timeout=job["timeout"],
                                 env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    output = folder / "result.json"
    record = json.loads(output.read_text()) if output.exists() else {"complete": False}
    if process.returncode or not record.get("complete"):
        raise RuntimeError(f"ObjFS {action}/{job['profile']} failed; retained evidence: {folder}")
    return record


class ObjCohort(Cohort):
    def storage_observer(self):
        return ObjStorage(self.job)

    def open_connection(self):
        return connection(self.job)

    def attach_statement(self):
        return attach_sql(self.job)

    def call_worker(self, job, action):
        return worker_call(job, action)

    def durability(self):
        return "PostgreSQL catalog + pgvs3 objects" if self.job["profile"] == "ducklake" else "native sync -> SlateDB durable object WAL"

    def publish(self, label):
        if self.job["profile"] != "ducklake":
            self.execute(label + "_checkpoint", "CHECKPOINT lake")

    def after_reads(self, stage):
        result = {"stage": stage, "readers": {}}
        start = time.perf_counter()
        self.con.close()
        result["writer_close_seconds"] = time.perf_counter() - start
        result["storage_after_close"] = self.storage.snapshot()
        for query in ("scan", "range"):
            job = self.job | {"stage": stage, "query": query, "folder": str(Path(self.job["folder"]) / f"reader-sealed-{query}")}
            result["readers"][query] = worker_call(job, "read")
        self.record["sealed_reads"] = result
        self.job["generation"] = self.job.get("generation", 0) + 1
        self.con, result["reopen_client_setup_seconds"] = connection(self.job)
        self.execute("reopen_writer_after_seal", attach_sql(self.job))
        self.flush()

    def maintain(self, stage):
        if self.job["profile"] == "ducklake":
            return super().maintain(stage)
        result = {"stage": stage, "steps": [], "forced_lsm_compaction": False,
                  "scope": "No exposed SlateDB compaction/retention override; observe the backend's normal policy"}
        self.record.setdefault("maintenance", []).append(result)
        self.execute(stage + "_maintenance_checkpoint", "CHECKPOINT lake")
        result["steps"].append({"operation": "checkpoint", "storage": self.storage.snapshot(self.con)})
        start = time.perf_counter()
        time.sleep(self.job["settle_seconds"])
        result["steps"].append({"operation": "background_observation", "seconds": time.perf_counter() - start,
                                "storage": self.storage.snapshot(self.con)})
        self.storage.reap_vacuum(result)
        self.flush()


def reader(job):
    result = read_worker(job, connect_client=connection, attach_client=attach_sql, stats=stats, stats_key="object_storage")
    result["client_peak_rss_bytes"] = peak_rss()
    result["sql_read_only"] = True
    result["requires_writable_object_endpoint"] = job["profile"] != "ducklake"
    return result


def proof_worker(job):
    action = job["proof_step"]
    con, setup = connection(job, reader=action != "writer")
    rows = job["proof_rows"]
    expected = [rows + 3, str(rows * (rows - 1) // 2 + 3 * rows + 3), str(rows * 32 + 15), 7]
    query = "SELECT count(*),sum(i)::VARCHAR,sum(length(id))::VARCHAR,min(revision) FROM lake.events"
    result = {"complete": False, "step": action, "client_setup_seconds": setup}
    if action == "read-only-gateway":
        before = gateway_metrics(job["endpoint"])
        try:
            con.execute(attach_sql(job, reader=True))
        except Exception as error:
            message = redact(str(error))
            if not any(word in message.lower() for word in ("403", "accessdenied", "permissiondenied")):
                raise
            result["supported"] = False
            result["reason"] = message
        else:
            result["supported"] = True
        result["gateway"] = delta(before, gateway_metrics(job["endpoint"]))
        con.close()
    elif action == "writer":
        from duck_bench import fixture_select
        con.execute(attach_sql(job))
        con.execute("CREATE TABLE lake.events AS " + fixture_select(rows))
        con.execute("ALTER TABLE lake.events ADD COLUMN revision BIGINT DEFAULT 7")
        for number in range(3):
            con.execute(f"INSERT INTO lake.events VALUES ({rows + number},'acked',0.5,7)")
        checked(list(con.execute(query).fetchone()), expected, "ObjFS writer")
        result["expected"] = expected
        result["exit"] = "os._exit: no explicit CHECKPOINT, DETACH or clean filesystem shutdown"
        result["object_storage"] = stats(con)
    else:
        con.execute(attach_sql(job, reader=action == "read"))
        checked(list(con.execute(query).fetchone()), expected, "ObjFS fresh-process WAL recovery")
        if action == "recover":
            con.execute(f"INSERT INTO lake.events VALUES ({rows + 3},'after',0.5,7)")
            checked(con.execute("SELECT count(*) FROM lake.events").fetchone()[0], rows + 4, "ObjFS resumed write")
        result["object_storage"] = stats(con)
        con.close()
    result["complete"] = True
    return result


def compatibility(root, owner, artifact, rows, timeout):
    bucket = "os-" + owner + "-proof"
    api(os.environ["PGVS3_TEST_ENDPOINT_A"], bucket)
    base = {"owner": owner, "profile": "objfs-lean", "bucket": bucket, "location": f"s3://{bucket}/state/",
            "extension": artifact["path"], "unsigned": artifact["unsigned"], "proof_rows": rows, "timeout": timeout}
    result = {"complete": False, "steps": []}
    for action in ("writer", "read", "recover"):
        job = base | {"folder": str(root / ("proof-" + action)), "proof_step": action}
        result["steps"].append(worker_call(job, "proof"))
        save(root / "compatibility.json", result)
    read = next(item for item in result["steps"] if item["step"] == "read")
    result["sql_read_only_uses_object_writes"] = any(row["operation"] == "write" and row["request_count"] for row in read["object_storage"]["io"])
    if os.environ.get("PGVS3_TEST_ENDPOINT_RO"):
        ro = base | {"folder": str(root / "proof-read-only-gateway"), "proof_step": "read-only-gateway",
                     "endpoint": os.environ["PGVS3_TEST_ENDPOINT_RO"], "timeout": 5}
        before = gateway_metrics(ro["endpoint"])
        try:
            result["read_only_gateway"] = worker_call(ro, "proof")
        except subprocess.TimeoutExpired:
            measured = delta(before, gateway_metrics(ro["endpoint"]))
            if not any(value and 'method="PUT",code="403"' in name for name, value in measured.items()):
                raise
            result["read_only_gateway"] = {"complete": True, "supported": False, "bounded_probe_seconds": 5,
                                           "worker_timed_out": True, "gateway": measured,
                                           "reason": "SQL read-only attach attempted forbidden checkpoint PUTs and did not return within the negative-probe deadline"}
    result["complete"] = True
    save(root / "compatibility.json", result)
    subprocess.run([os.environ["PGVS3_TEST_BIN"], "drop", "--url", os.environ["PGVS3_TEST_DB_URL"], "--force", bucket],
                   check=True, capture_output=True, timeout=120)
    return result


def contract():
    artifact = provision(os.environ.get("PGVS3_OBJECT_STORAGE_EXTENSION"))
    root = Path(os.environ["PGVS3_TEST_RUN_DIR"]) / "object-storage"
    root.mkdir(exist_ok=False)
    owner = "r" + uuid.uuid4().hex[:12]
    save(root / "ownership.json", {"run": owner})
    result = compatibility(root, owner, artifact, 10000, 300)
    print("ObjFS 0.2.0 / DuckDB 2.0: abrupt-exit remote-WAL recovery, separate SQL read-only client and resumed writes passed", flush=True)
    return result


def tuning_summary(cases):
    return [{"target_mib": case["target_mib"], "operation": case["operation"],
             "default_seconds": case["median_seconds"]["objfs-default"], "lean_seconds": case["median_seconds"]["objfs-lean"],
             "lean_to_default_ratio": case["median_seconds"]["objfs-lean"] / case["median_seconds"]["objfs-default"],
             "within_20pct_latency_budget": case["median_seconds"]["objfs-lean"] <= 1.2 * case["median_seconds"]["objfs-default"]}
            for case in cases]


def sealed_summary(cohorts):
    result = []
    for size, profile in sorted({(c["target_mib"], c["profile"]) for c in cohorts}):
        rows = [c for c in cohorts if (c["target_mib"], c["profile"]) == (size, profile)]
        live = statistics.median(c["remote_reads"]["loaded"]["scan"]["attach_schema_first_query_seconds"] for c in rows)
        sealed = statistics.median(c["sealed_reads"]["readers"]["scan"]["attach_schema_first_query_seconds"] for c in rows)
        close = statistics.median(c["sealed_reads"]["writer_close_seconds"] for c in rows)
        result.append({"target_mib": size, "profile": profile, "samples": len(rows), "live_first_query_seconds": live,
                       "sealed_first_query_seconds": sealed, "writer_close_seconds": close,
                       "saving_per_fresh_reader_seconds": live - sealed,
                       "close_amortization_readers": close / (live - sealed) if live > sealed else None,
                       "contract": "same published data; writer explicitly closed/quiescent, not a live-writer speedup"})
    return result


def run(args, owner):
    import duckdb
    from capacity import host_info
    from duck_bench import fixture_select
    checked(duckdb.__version__, DUCKDB, "ObjFS DuckDB target")
    if args.passes < 1 or args.object_storage_ddl_iterations < 1 or args.object_storage_small_commits < 1 or \
       args.object_storage_proof_rows < 1 or args.object_storage_timeout <= 0 or not math.isfinite(args.object_storage_settle) or args.object_storage_settle < 0 or \
       any(size < 1 or size > 1024 for size in args.object_storage_mib) or len(set(args.object_storage_mib)) != len(args.object_storage_mib):
        raise ValueError("Use positive repetitions and distinct ObjFS fixture targets from 1 to 1024 MiB")
    artifact = provision(args.object_storage_extension)
    root = Path(os.environ["PGVS3_TEST_RUN_DIR"]) / "object-storage"
    root.mkdir(exist_ok=False)
    save(root / "ownership.json", {"run": owner})
    output = root / "progress.json"
    result = {"schema_version": 1, "complete": False, "artifact": artifact, "host": host_info(), "profiles": CONFIGS,
              "cohorts": [], "fixtures": [], "default_storage_client_changed": False,
              "gateway_sha256": digest(os.environ["PGVS3_TEST_BIN"]),
              "runner_sha256": {name: digest(Path(__file__).parent / name) for name in ("object_storage.py", "differential.py", "duck_bench.py", "capacity.py", "stack.sh")},
              "memory_scope": "2GB DuckDB buffer limit plus independently configured SlateDB caches; process RSS is also recorded",
              "scope": "Native buckets, one externally coordinated writer; fresh/live vs sealed readers and cache/thread budgets are separate"}
    save(output, result)
    result["compatibility"] = compatibility(root, owner, artifact, args.object_storage_proof_rows, args.object_storage_timeout)
    save(output, result)
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='2GB'")
    for size in args.object_storage_mib:
        path = root / f"input-{size}MiB.parquet"
        rows = size * 23700
        source = fixture_select(rows)
        con.execute(f"COPY ({source}) TO {quote(path)} (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880)")
        fixture = {"path": str(path.resolve()), "bytes": path.stat().st_size, "sha256": digest(path), "rows": rows,
                   "generation_sql": source, "compression": "snappy", "row_group_rows": 122880}
        expected = reference(con, path, rows, args.object_storage_small_commits)
        result["fixtures"].append(fixture)
        save(output, result)
        for repeat in range(args.passes):
            order = PROFILES[repeat % 3:] + PROFILES[:repeat % 3]
            for profile in order:
                suffix = f"m{size}_p{repeat}_{profile}"
                bucket = "os-" + owner + "-" + suffix.replace("_", "-")
                api(os.environ["PGVS3_TEST_ENDPOINT_A"], bucket)
                job = {"owner": owner, "profile": profile, "mib": size, "pass": repeat, "rows": rows,
                       "input": fixture, "expected": expected, "folder": str(root / suffix), "bucket": bucket,
                       "location": f"s3://{bucket}/state/", "metadata_schema": owner + "_" + suffix.replace("-", "_"),
                       "extension": artifact["path"], "unsigned": artifact["unsigned"], "threads": 4,
                       "ddl_iterations": args.object_storage_ddl_iterations, "small_commits": args.object_storage_small_commits,
                       "settle_seconds": args.object_storage_settle, "timeout": args.object_storage_timeout}
                print(f"ObjFS comparison: {size} MiB, pass {repeat + 1}, {profile}", flush=True)
                cohort = worker_call(job, "cohort")
                cohort["client_after_process_exit"] = inventory(Path(job["folder"]) / "client")
                result["cohorts"].append(cohort)
                save(output, result)
                subprocess.run([os.environ["PGVS3_TEST_BIN"], "drop", "--url", os.environ["PGVS3_TEST_DB_URL"], "--force", bucket],
                               check=True, capture_output=True, timeout=120)
                if profile == "ducklake":
                    pg_sql(f"DROP SCHEMA {ident(job['metadata_schema'])} CASCADE", "ducklake_catalog")
    con.close()
    result["cases"] = summarize(result["cohorts"], profiles=PROFILES)
    result["tuning"] = tuning_summary(result["cases"])
    result["sealed"] = sealed_summary(result["cohorts"])
    result["complete"] = True
    save(output, result)
    print(f"ObjFS completed {len(result['cohorts'])} cohorts; evidence: {output}", flush=True)
    for case in result["cases"]:
        if case["operation"] in ("ddl_create", "bulk_load", "small_append", "loaded_scan_fresh_total"):
            values = " / ".join(f"{case['median_seconds'][profile] * 1000:.2f}" for profile in PROFILES)
            print(f"{case['target_mib']:4} MiB {case['operation']}: {values} ms (DuckLake / ObjFS default / lean)", flush=True)
    print(f"Lean profile budget misses: {sum(not row['within_20pct_latency_budget'] for row in result['tuning'])}; live/sealed reads and closing costs are separate", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    setup = sub.add_parser("setup")
    setup.add_argument("--extension", default=os.environ.get("PGVS3_OBJECT_STORAGE_EXTENSION"))
    sub.add_parser("contract")
    worker = sub.add_parser("worker")
    worker.add_argument("action", choices=("cohort", "read", "proof"))
    worker.add_argument("job")
    args = parser.parse_args()
    if args.command == "setup":
        print(json.dumps(provision(args.extension), indent=2))
        return
    if args.command == "contract":
        contract()
        return
    job = json.loads(Path(args.job).read_text())
    validate(job)
    try:
        record = {"cohort": lambda: ObjCohort(job).run(), "read": lambda: reader(job), "proof": lambda: proof_worker(job)}[args.action]()
        save(Path(job["folder"]) / "result.json", record)
        if args.action == "proof" and job["proof_step"] == "writer":
            os._exit(0)
    except BaseException as error:
        path = Path(job["folder"]) / "result.json"
        record = json.loads(path.read_text()) if path.exists() else {"complete": False}
        record["error"] = {"type": type(error).__name__, "message": redact(str(error))}
        save(path, record)
        raise RuntimeError(redact(str(error))) from None


if __name__ == "__main__":
    main()
