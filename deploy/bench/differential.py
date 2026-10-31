#!/usr/bin/env python3
"""Opt-in Differential client provisioning and the DuckDB benchmark part.

The gateway and its storage layout are not changed. Workers use independent
DuckDB processes/repositories and the existing Docker and capacity harnesses.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from urllib.parse import quote as urlquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen
import zipfile

from capacity import Capacity, host_info


ROOT = Path(__file__).resolve().parents[2]
VERSION = "v1.1.0"
COMMIT = "43203569f46b3a7f0c3a88fa8b4c34a50052ceb0"
DUCKDB = "2.0.0"
WORKFLOW = 40585660508
ARTIFACTS = {
    "linux_amd64": (12771813274, "67adca05a2def879effa08cb665c2d81c9fa263cc92305bde25da7153f585d76"),
    "linux_arm64": (12771302755, "1cac2a9066b4f26ffbf73aa4b7798d35d3ef29065fd6844ed41b5c712a85a519"),
    "osx_arm64": (12772254042, "248514ad79abcbb2824992d1f149e752a8429a1cc6386178d028d9d858a0eeec"),
}
PROFILES = ("ducklake", "snapshot", "sync")
DIGEST = "count(*),sum(i)::VARCHAR,sum(length(id))::VARCHAR,sum(hash(i,id,value)::HUGEINT)::VARCHAR"


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def save(path, record):
    Path(path).write_text(json.dumps(record, indent=2) + "\n")


def redact(text):
    for name in ("PGVS3_SECRET_KEY", "PGVS3_CATALOG"):
        value = os.environ.get(name)
        if value:
            text = text.replace(value, "<redacted>")
    return re.sub(r"(postgres(?:ql)?://)[^/\s]*@", r"\1<redacted>@", text)


def extension_platform():
    system = {"Linux": "linux", "Darwin": "osx"}.get(platform.system(), platform.system().lower())
    machine = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine(), platform.machine())
    return system + "_" + machine


def extract(archive, target, expected):
    if digest(archive) != expected:
        raise RuntimeError(f"Differential archive checksum mismatch: {archive}")
    with zipfile.ZipFile(archive) as source, source.open("differential.duckdb_extension") as extension:
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".extension-", delete=False) as temporary:
            pending = Path(temporary.name)
            try:
                shutil.copyfileobj(extension, temporary)
            except BaseException:
                pending.unlink()
                raise
    try:
        if target.exists():
            if digest(target) != digest(pending):
                raise RuntimeError(f"Refusing to overwrite a different extension: {target}")
        else:
            pending.rename(target)
    finally:
        pending.unlink(missing_ok=True)


def provision(override=None, cache=None):
    platform_name = extension_platform()
    if override:
        extension = Path(override).expanduser().resolve(strict=True)
        return {"source": "explicit override", "path": str(extension), "sha256": digest(extension),
                "platform": platform_name, "expected_version": VERSION, "duckdb": DUCKDB}
    if platform_name not in ARTIFACTS:
        raise RuntimeError(f"No pinned Differential artifact for {platform_name}; supply --differential-extension from a {VERSION}/DuckDB {DUCKDB} source build")
    artifact_id, checksum = ARTIFACTS[platform_name]
    folder = Path(cache or ROOT / ".tmp/deps/differential") / VERSION / platform_name
    folder.mkdir(parents=True, exist_ok=True)
    archive = folder / "upstream.zip"
    name = f"differential-v{DUCKDB}-extension-{platform_name}"
    url = f"https://nightly.link/tobilg/duckdb-differential/actions/runs/{WORKFLOW}/{name}.zip"
    if not archive.exists():
        with tempfile.NamedTemporaryFile(dir=folder, prefix=".download-", delete=False) as target:
            pending = Path(target.name)
            try:
                with urlopen(url, timeout=120) as source:
                    shutil.copyfileobj(source, target)
                target.flush()
                if digest(pending) != checksum:
                    raise RuntimeError("Downloaded Differential archive does not match its pinned SHA-256")
                pending.rename(archive)
            except BaseException as error:
                pending.unlink(missing_ok=True)
                raise RuntimeError(f"Cannot provision the pinned Differential artifact; it expires 2027-01-25. Keep a verified cache or build {COMMIT} for DuckDB {DUCKDB} and supply --differential-extension") from error
    extension = folder / "differential.duckdb_extension"
    extract(archive, extension, checksum)
    return {"source": "upstream CI artifact", "repository": "https://github.com/tobilg/duckdb-differential",
            "version": VERSION, "commit": COMMIT, "duckdb": DUCKDB, "platform": platform_name,
            "workflow_run": WORKFLOW, "artifact_id": artifact_id, "artifact_url": url,
            "artifact_expires": "2027-01-25", "archive_sha256": checksum,
            "path": str(extension.resolve()), "sha256": digest(extension)}


def inventory(folder):
    groups = {}
    if Path(folder).exists():
        for path in Path(folder).rglob("*"):
            if path.is_file() and not path.is_symlink():
                relative = path.relative_to(folder)
                group = "/".join(relative.parts[:2])
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                item = groups.setdefault(group, {"files": 0, "bytes": 0, "allocated_bytes": 0})
                item["files"] += 1
                item["bytes"] += stat.st_size
                item["allocated_bytes"] += getattr(stat, "st_blocks", 0) * 512
    return {"bytes": sum(g["bytes"] for g in groups.values()),
            "allocated_bytes": sum(g["allocated_bytes"] for g in groups.values()), "groups": groups}


def gateway_metrics(endpoint):
    with urlopen(endpoint + "/metrics", timeout=15) as response:
        text = response.read().decode()
    result = {name: float(value) for name, value in re.findall(
        r'^(pgvs3_(?:requests_total\{[^\n]+\}|response_bytes_total)) ([0-9.eE+-]+)$', text, re.M)}
    if "pgvs3_response_bytes_total" not in result:
        raise RuntimeError("Missing gateway request/byte counters")
    return result


def delta(before, after):
    result = {key: after.get(key, 0) - before.get(key, 0) for key in before.keys() | after.keys()}
    if any(value < 0 for value in result.values()):
        raise RuntimeError("A measured gateway counter went backwards")
    return {key: value for key, value in result.items() if value}


def successful_requests(counters):
    errors = {key: value for key, value in counters.items() if re.search(r'code="5[0-9][0-9]"', key) and value}
    if errors:
        raise RuntimeError(f"Timed client work received gateway errors/retries, not a qualified sample: {errors}")


def pg_sql(statement, database=None):
    url = os.environ["PGVS3_TEST_DB_URL"]
    if database:
        parts = urlsplit(url)
        url = urlunsplit((parts.scheme, parts.netloc, "/" + database, parts.query, parts.fragment))
    result = subprocess.run([os.environ["PGVS3_TEST_PSQL"], url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", statement],
                            capture_output=True, text=True, check=True, timeout=600)
    return result.stdout.strip()


def api(endpoint, bucket, method="PUT"):
    with urlopen(Request(endpoint + "/" + urlquote(bucket), data=b"" if method == "PUT" else None,
                         method=method), timeout=120) as response:
        response.read()


def validate_job(job):
    owned = Path(os.environ["PGVS3_TEST_RUN_DIR"]).resolve() / "differential"
    marker = json.loads((owned / "ownership.json").read_text())
    if job["owner"] != marker["run"] or not re.fullmatch(r"r[0-9a-f]{12}", job["owner"]):
        raise RuntimeError("The worker does not own this disposable run")
    if not Path(job["folder"]).resolve().is_relative_to(owned) or Path(job["folder"]).resolve() == owned:
        raise RuntimeError("Worker files must stay in their owned run directory")
    pattern = "dd-" + job["owner"] + r"-(?:m[0-9]+-p[0-9]+-(?:ducklake|snapshot|sync)|compat)"
    if not re.fullmatch(pattern, job["bucket"]) or job["location"] != f"s3://{job['bucket']}/state/":
        raise RuntimeError("Refusing management outside the owned provider namespace")
    if "repository" in job and not Path(job["repository"]).resolve().is_relative_to(owned):
        raise RuntimeError("Refusing an external writer repository")
    if "client_source" in job and not Path(job["client_source"]).resolve().is_relative_to(owned):
        raise RuntimeError("Refusing an external client inventory")
    if "metadata_schema" in job and not re.fullmatch(job["owner"] + r"_m[0-9]+_p[0-9]+_ducklake", job["metadata_schema"]) and job["profile"] == "ducklake":
        raise RuntimeError("Refusing an unowned DuckLake catalog schema")


class Storage:
    def __init__(self, job):
        self.job = job
        fixture = SimpleNamespace(args=SimpleNamespace(psql=os.environ["PGVS3_TEST_PSQL"]),
                                  endpoints={"after": os.environ["PGVS3_TEST_ENDPOINT_A"]},
                                  sql=lambda side, statement: pg_sql(statement))
        self.capacity = Capacity(fixture, {})

    def snapshot(self, connection=None):
        job = self.job
        raw = self.capacity.snapshot("after")
        relations = [r for r in raw.pop("relations") if r["schema"] == "s3b_" + job["bucket"]]
        raw["bucket"] = {"relations": relations, "allocated_relation_bytes": sum(r["total_bytes"] for r in relations)}
        raw["retained_wal_bytes"] = int(pg_sql("SELECT coalesce(sum(size),0) FROM pg_ls_waldir()"))
        raw["bucket"]["objects"] = json.loads(pg_sql(
            f"SELECT json_build_object('count',count(*),'bytes',coalesce(sum(size),0)) FROM {ident('s3b_' + job['bucket'])}.objects"))
        if job["profile"] == "ducklake":
            raw["catalog"] = json.loads(pg_sql(
                "SELECT json_build_object('database_bytes',pg_database_size(current_database()),'relations',coalesce(json_agg("
                "json_build_object('name',c.relname,'total_bytes',pg_total_relation_size(c.oid)) ORDER BY c.relname),'[]')) "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                f"WHERE c.relkind='r' AND n.nspname={quote(job['metadata_schema'])}", "ducklake_catalog"))
        raw["client"] = inventory(job.get("client_source", Path(job["folder"]) / "client"))
        if connection is not None and job["profile"] != "ducklake":
            raw["differential"] = differential_stats(connection)
        return raw

    def reap_vacuum(self, record):
        start = time.perf_counter()
        deadline = start + 600
        while True:
            result = subprocess.run([os.environ["PGVS3_TEST_BIN"], "reap", "--url", os.environ["PGVS3_TEST_DB_URL"],
                                     "--grace", "0s", "--all"], capture_output=True, text=True, check=True,
                                    timeout=max(1, deadline - time.perf_counter()))
            if re.search(r"\bgarbage_files=0\b", result.stdout):
                break
            if time.perf_counter() >= deadline:
                raise RuntimeError("pgvs3 reaper did not drain")
            time.sleep(0.1)
        record["postgres_reap"] = {"seconds": time.perf_counter() - start, "result": result.stdout.strip(), "storage": self.snapshot()}
        start = time.perf_counter()
        schemas = [("s3b_" + self.job["bucket"], None)]
        if self.job["profile"] == "ducklake":
            schemas.append((self.job["metadata_schema"], "ducklake_catalog"))
        for schema, database in schemas:
            tables = json.loads(pg_sql("SELECT coalesce(json_agg(c.relname),'[]') FROM pg_class c "
                                      f"JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={quote(schema)} AND c.relkind='r'", database))
            for table in tables:
                pg_sql(f"VACUUM (ANALYZE) {ident(schema)}.{ident(table)}", database)
        record["postgres_vacuum"] = {"seconds": time.perf_counter() - start, "storage": self.snapshot()}


def differential_stats(connection):
    return {key: value for key, value in connection.execute("SELECT metric,value FROM differential_storage_stats()").fetchall()
            if key.startswith(("local_", "remote_", "s3_", "ship_", "export_", "branch_head_")) and "nanoseconds" not in key}


def connection(job, reader=False):
    from duck_bench import connect
    endpoint = os.environ["PGVS3_TEST_ENDPOINT_B" if reader else "PGVS3_TEST_ENDPOINT_A"]
    start = time.perf_counter()
    con = connect(endpoint, job["threads"], unsigned_extension=job["extension"] if job["profile"] != "ducklake" else None)
    client = Path(job["folder"]) / "client"
    client.mkdir(parents=True, exist_ok=False)
    con.execute("SET memory_limit='2GB'")
    con.execute("SET http_client_pool_capacity=16")
    con.execute("SET parquet_metadata_cache=false")
    con.execute("SET temp_directory=" + quote(client / "spill"))
    if job["profile"] != "ducklake":
        con.execute("SET differential_repository=" + quote(job.get("repository", client / "repository")))
        con.execute("SET differential_auto_gc=false; SET differential_remote_cache_size='0'; SET differential_s3_max_requests=16")
        actual = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='differential'").fetchone()[0]
        status = dict(con.execute("SELECT * FROM differential_status()").fetchall())
        if actual != VERSION or status.get("test_hooks") != "absent":
            raise RuntimeError(f"Expected the {VERSION} release extension without test hooks, got {actual}")
    return con, time.perf_counter() - start


def attach_sql(job, reader=False):
    if job["profile"] == "ducklake":
        return (f"ATTACH {quote('ducklake:postgres:' + os.environ['PGVS3_CATALOG'])} AS lake "
                f"(DATA_PATH {quote(job['location'])}, METADATA_SCHEMA {quote(job['metadata_schema'])})")
    options = "REQUEST_TIMEOUT_MS 120000"
    if job.get("multipart_threshold"):
        options += f", MULTIPART_THRESHOLD {job['multipart_threshold']}"
    if reader:
        return f"ATTACH {quote('differential:' + job['location'] + 'heads/bench/main')} AS lake ({options})"
    extra = ", SHIP_WAL 'sync'" if job["profile"] == "sync" else ""
    return f"ATTACH 'differential:bench' AS lake (DATA_PATH {quote(job['location'])}, {options}{extra})"


def checked(actual, expected, label):
    if actual != expected:
        raise RuntimeError(f"{label}: result mismatch: {actual!r} != {expected!r}")


def epoch_limit_failure(profile, record):
    message = record.get("error", {}).get("message", "")
    return profile == "sync" and "durable locally" in message and "[differential:busy] A shipping epoch exceeds 256 MiB" in message


def worker_call(job, action="cohort"):
    validate_job(job)
    folder = Path(job["folder"])
    folder.mkdir(parents=True, exist_ok=False)
    save(folder / "job.json", job)
    environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true")
    with (folder / "worker.log").open("w") as log:
        result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "worker", action, str(folder / "job.json")],
                                stdout=log, stderr=subprocess.STDOUT, env=environment, timeout=job["timeout"])
    output = folder / "result.json"
    record = json.loads(output.read_text()) if output.exists() else {"complete": False}
    if result.returncode or not record.get("complete"):
        raise RuntimeError(f"Differential {action}/{job['profile']} failed; retained evidence: {folder}")
    return record


def finalize_worker(job):
    con, setup_seconds = connection(job)
    storage = Storage(job)
    result = {"complete": False, "client_setup_seconds": setup_seconds, "before": storage.snapshot(con), "steps": [],
              "test_remote_gc_grace_seconds": 1}
    operations = [("local_gc_after_writer_exit", "SELECT * FROM differential_cleanup_old_files(cleanup_all => true, max_objects => 10000)"),
                  ("metadata_compact_after_writer_exit", "SELECT * FROM differential_compact(max_pages => 10000)")]
    for phase in range(3):
        operations.append((f"quiescent_remote_gc_{phase + 1}", "SELECT * FROM differential_cleanup_remote_files(" + quote(job["location"]) +
                           ", older_than => now() - INTERVAL '1 second')"))
    for label, statement in operations:
        if label.startswith("quiescent_remote"):
            time.sleep(1.1)
        start = time.perf_counter()
        rows = con.execute("SELECT to_json(r) FROM (" + statement + ") r").fetchall()
        result["steps"].append({"operation": label, "seconds": time.perf_counter() - start,
                                "result": [json.loads(row[0]) for row in rows], "storage": storage.snapshot(con)})
    storage.reap_vacuum(result)
    con.close()
    result["complete"] = True
    return result


def proof_worker(job):
    step = job["proof_step"]
    con, setup_seconds = connection(job, reader=step != "writer")
    rows = job["proof_rows"]
    expected = [rows + 5, str(rows * (rows - 1) // 2 + 5 * rows + 10), str(rows * 64 + 25), 7]
    result = {"complete": False, "step": step, "client_setup_seconds": setup_seconds}
    query = "SELECT count(*),sum(i)::VARCHAR,sum(length(id))::VARCHAR,min(revision) FROM lake.events"
    if step == "writer":
        result["extensions"] = dict(con.execute(
            "SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded AND extension_name IN ('httpfs','postgres','postgres_scanner','ducklake','differential')").fetchall())
        result["release_status"] = {key: value for key, value in con.execute("SELECT * FROM differential_status()").fetchall()
                                    if key in ("v1_complete", "duckdb_target", "test_hooks")}
        con.execute(attach_sql(job))
        con.execute("CREATE TABLE lake.events AS SELECT i,md5(i::VARCHAR)||md5((i*3)::VARCHAR) AS id,"
                    f"hash(i)::DOUBLE/18446744073709551615.0 AS value FROM range({rows}) t(i)")
        con.execute("CHECKPOINT lake")
        checked(con.execute("SELECT state,export_state FROM differential_create_snapshot('lake')").fetchone(),
                ("complete", "complete"), "proof export")
        con.execute("ALTER TABLE lake.events ADD COLUMN revision BIGINT DEFAULT 7")
        for number in range(5):
            con.execute(f"INSERT INTO lake.events VALUES ({rows + number},'acked',0.5,7)")
        checked(list(con.execute(query).fetchone()), expected, "writer before abrupt exit")
        result["expected"] = expected
        result["exit"] = "os._exit: no DETACH, connection close or shutdown checkpoint"
        result["stats"] = differential_stats(con)
        if result["stats"].get("s3_request_multipart_part_calls", 0) < 2 or result["stats"].get("s3_request_conditional_put_calls", 0) < 1:
            raise RuntimeError("The release did not exercise multipart and conditional publication")
        result["shipping"] = con.execute("SELECT ship_wal,shipped_chain,shipped_epoch,ship_pending FROM differential_branches('lake')").fetchall()
        checked(result["shipping"][0][0], "sync", "shipping mode")
        checked(result["shipping"][0][3], False, "all ACKs shipped")
    elif step == "snapshot":
        con.execute(attach_sql(job, reader=True))
        actual = list(con.execute("SELECT count(*),sum(i)::VARCHAR,sum(length(id))::VARCHAR FROM lake.events").fetchone())
        checked(actual, [rows, str(rows * (rows - 1) // 2), str(rows * 64)], "snapshot is intentionally behind shipped commits")
        result["snapshot_result"] = actual
        result["visibility"] = "plain head attachment reads the last exported snapshot, not the later committed WAL"
    elif step == "stale":
        try:
            con.execute(attach_sql(job))
            con.execute("INSERT INTO lake.events VALUES (-999,'stale',0.5,7)")
        except Exception as error:
            if "head_conflict" not in str(error):
                raise
            result["stale_commit_rejected"] = True
        else:
            result["stale_commit_rejected"] = False
            result["stale_commit_acknowledged"] = True
            try:
                con.execute("SELECT state,export_state FROM differential_create_snapshot('lake')").fetchall()
            except Exception as error:
                if "head_conflict" not in str(error):
                    raise
                result["conflicting_snapshot_publication_rejected"] = True
            else:
                raise RuntimeError("An outdated snapshot-head ETag was not rejected")
    else:
        recovery = con.execute("SELECT database_id,database_name,shipped_chain,shipped_epoch FROM differential_recover_branch(" +
                               quote(job["location"] + "heads/bench/main") + ", 'bench', same_identity => " +
                               ("true" if step == "recover" else "false") +
                               f", multipart_threshold => {job['multipart_threshold']}, request_timeout_ms => 120000)").fetchall()
        result["recovery"] = recovery
        if step == "recover":
            result["explicit_head_claim_after_old_writer_exit"] = con.execute(
                "SELECT branch_name,snapshot_id FROM differential_claim_branch_heads('bench')").fetchall()
            con.execute(attach_sql(job))
            checked(list(con.execute(query).fetchone()), expected, "fresh-repository recovery of every ACK")
            con.execute("CHECKPOINT lake")
            con.execute(f"INSERT INTO lake.events VALUES ({rows + 5},'after',0.5,7)")
            try:
                checked(con.execute("SELECT state,export_state FROM differential_create_snapshot('lake')").fetchone(),
                        ("complete", "complete"), "recovered writer snapshot publication")
            except Exception as error:
                if "head_conflict" not in str(error):
                    raise
                result["snapshot_publication_required_head_takeover"] = True
                result["head_takeover"] = con.execute(
                    "SELECT branch_name,snapshot_id FROM differential_claim_branch_heads('lake')").fetchall()
                con.execute("SELECT snapshot_id,export_state FROM differential_retry_export('lake')").fetchall()
            result["continued_same_identity"] = True
            result["remote_gc"] = []
            for _ in range(3):
                time.sleep(1.1)
                collected = con.execute("SELECT to_json(r) FROM differential_cleanup_remote_files(" + quote(job["location"]) +
                                        ", older_than => now() - INTERVAL '1 second') r").fetchone()[0]
                result["remote_gc"].append(json.loads(collected))
            if not any(item["deleted_objects"] for item in result["remote_gc"]):
                raise RuntimeError("Recovery proof did not exercise collection of superseded remote chains")
        elif step == "verify":
            con.execute("ATTACH 'differential:bench' AS lake")
            expected = [expected[0] + 1, str(int(expected[1]) + rows + 5), str(int(expected[2]) + 5), 7]
            checked(list(con.execute(query).fetchone()), expected, "fresh recovery after remote GC")
        else:
            con.execute("ATTACH 'differential:bench' AS lake")
            result["previous_recovery_marker_rows"] = con.execute(f"SELECT count(*) FROM lake.events WHERE i={rows + 5}").fetchone()[0]
            result["stale_marker_rows"] = con.execute("SELECT count(*) FROM lake.events WHERE i=-999").fetchone()[0]
        result["result"] = list(con.execute(query).fetchone())
        result["stats"] = differential_stats(con)
    if step != "writer":
        con.close()
    result["complete"] = True
    return result


def compatibility(folder, run_name, artifact, timeout):
    bucket = "dd-" + run_name + "-compat"
    api(os.environ["PGVS3_TEST_ENDPOINT_A"], bucket)
    job = {"owner": run_name, "profile": "sync", "bucket": bucket, "location": f"s3://{bucket}/state/",
           "extension": artifact["path"], "threads": 4, "timeout": timeout, "proof_rows": 600000,
           "multipart_threshold": 5 * 1024 * 1024}
    result = {"complete": False, "steps": []}
    for step in ("writer", "snapshot", "recover", "verify", "stale", "inspect"):
        current = job | {"proof_step": step, "folder": str(folder / ("proof-" + step))}
        if step == "stale":
            current["repository"] = str(folder / "proof-writer/client/repository")
        result["steps"].append(worker_call(current, "proof"))
        save(folder / "compatibility.json", result)
    stale = next(item for item in result["steps"] if item["step"] == "stale")
    inspected = result["steps"][-1]
    result["automatic_writer_fencing"] = stale["stale_commit_rejected"]
    result["external_writer_fencing_required"] = True
    result["unfenced_restart_changed_recovered_state"] = inspected["previous_recovery_marker_rows"] != 1 or inspected["stale_marker_rows"] != 0
    checksum_rows = pg_sql("SELECT coalesce(json_object_agg(name,n),'{}') FROM (SELECT "
                          "coalesce(checksum_name,'none') AS name,count(*) AS n FROM " + ident("s3b_" + bucket) + ".objects GROUP BY 1) r")
    result["stored_checksum_algorithms"] = json.loads(checksum_rows)
    if not any("sha256" in key.lower() and value for key, value in result["stored_checksum_algorithms"].items()):
        raise RuntimeError("The SDK uploads did not persist SHA-256 checksum metadata")
    result["complete"] = True
    save(folder / "compatibility.json", result)
    subprocess.run([os.environ["PGVS3_TEST_BIN"], "drop", "--url", os.environ["PGVS3_TEST_DB_URL"], "--force", bucket],
                   capture_output=True, text=True, check=True, timeout=120)
    return result


class Cohort:
    def __init__(self, job):
        self.job = job
        self.output = Path(job["folder"]) / "result.json"
        self.record = {"complete": False, "profile": job["profile"], "target_mib": job["mib"], "pass": job["pass"],
                       "input": job["input"], "events": [], "storage": [], "remote_reads": {}}
        self.storage = self.storage_observer()
        self.con, setup_seconds = self.open_connection()
        self.record["client_setup_seconds"] = setup_seconds
        self.flush()

    def storage_observer(self):
        return Storage(self.job)

    def open_connection(self):
        return connection(self.job)

    def attach_statement(self):
        return attach_sql(self.job)

    def call_worker(self, job, action):
        return worker_call(job, action)

    def durability(self):
        return {"ducklake": "PostgreSQL catalog + pgvs3 objects", "snapshot": "synchronous snapshot export",
                "sync": "synchronous WAL shipping"}[self.job["profile"]]

    def after_reads(self, stage):
        pass

    def flush(self):
        save(self.output, self.record)

    def execute(self, label, statement, mutation=False):
        endpoint = os.environ["PGVS3_TEST_ENDPOINT_A"]
        before = gateway_metrics(endpoint)
        start = time.perf_counter()
        try:
            rows = self.con.execute(statement).fetchall()
        except Exception as error:
            self.record["events"].append({"operation": label, "sql": redact(statement), "succeeded": False,
                                          "statement_seconds": time.perf_counter() - start,
                                          "error": redact(str(error)), "gateway": delta(before, gateway_metrics(endpoint))})
            self.flush()
            raise
        executed = time.perf_counter()
        event = {"operation": label, "sql": redact(statement), "statement_seconds": executed - start}
        if mutation and self.job["profile"] == "snapshot":
            publication = self.con.execute("SELECT snapshot_id,state,export_state FROM differential_create_snapshot('lake')").fetchone()
            checked(publication[1:], ("complete", "complete"), "synchronous snapshot export")
            event["publication_seconds"] = time.perf_counter() - executed
            event["snapshot_id"] = publication[0]
        if mutation:
            event["remote_ack_seconds"] = time.perf_counter() - start
            event["durability"] = self.durability()
        event["gateway"] = delta(before, gateway_metrics(endpoint))
        self.record["events"].append(event)
        self.flush()
        successful_requests(event["gateway"])
        return rows

    def capture(self, label, attached=True):
        self.record["storage"].append({"stage": label, **self.storage.snapshot(self.con if attached else None)})
        self.flush()

    def publish(self, label):
        if self.job["profile"] == "sync":
            result = self.execute(label + "_snapshot_visibility",
                                  "SELECT snapshot_id,state,export_state FROM differential_create_snapshot('lake')")
            checked(result[0][1:], ("complete", "complete"), "snapshot visibility")

    def remote(self, stage):
        results = {}
        for query in ("scan", "range") if stage != "dropped" else ("sentinel",):
            reader = self.job | {"folder": str(Path(self.job["folder"]) / f"reader-{stage}-{query}"), "stage": stage, "query": query}
            results[query] = self.call_worker(reader, "read")
        self.record["remote_reads"][stage] = results
        self.flush()

    def maintain(self, stage):
        result = {"stage": stage, "steps": []}
        self.record.setdefault("maintenance", []).append(result)

        def step(label, sql):
            start = time.perf_counter()
            rows = self.con.execute("SELECT to_json(r) FROM (" + sql + ") r").fetchall()
            result["steps"].append({"operation": label, "seconds": time.perf_counter() - start,
                                    "result": [json.loads(row[0]) for row in rows], "storage": self.storage.snapshot(self.con)})
            self.flush()

        if self.job["profile"] == "ducklake":
            for label, sql in (("merge", "SELECT * FROM ducklake_merge_adjacent_files('lake')"),
                               ("rewrite", "SELECT * FROM ducklake_rewrite_data_files('lake', delete_threshold => 0.5)"),
                               ("expire", "SELECT * FROM ducklake_expire_snapshots('lake', older_than => now())"),
                               ("object_cleanup", "SELECT * FROM ducklake_cleanup_old_files('lake', cleanup_all => true)")):
                step(label, sql)
        else:
            versions = [row[0] for row in self.con.execute(
                "SELECT snapshot_id FROM differential_snapshots('lake') WHERE NOT retired "
                "AND snapshot_id < (SELECT max(snapshot_id) FROM differential_snapshots('lake'))").fetchall()]
            if versions:
                step("expire", f"SELECT * FROM differential_expire_snapshots('lake', versions => {versions})")
            step("local_gc", "SELECT * FROM differential_cleanup_old_files(cleanup_all => true, max_objects => 10000)")
            step("metadata_compact", "SELECT * FROM differential_compact(max_pages => 10000)")
            for phase in range(3):
                time.sleep(1.1)
                step(f"remote_gc_{phase + 1}", "SELECT * FROM differential_cleanup_remote_files(" + quote(self.job["location"]) +
                     ", older_than => now() - INTERVAL '1 second')")
            result["test_remote_gc_grace_seconds"] = 1
        self.storage.reap_vacuum(result)
        self.flush()

    def run(self):
        job = self.job
        self.capture("empty_bucket", attached=False)
        self.execute("writer_attach", self.attach_statement())
        if job["profile"] == "ducklake":
            self.con.execute("CALL lake.set_option('data_inlining_row_limit',0)")
        for iteration in range(job["ddl_iterations"]):
            table = f"lake.ddl_{iteration}"
            self.execute("ddl_create", f"CREATE TABLE {table}(i BIGINT)", True)
            self.execute("ddl_add_column", f"ALTER TABLE {table} ADD COLUMN note BIGINT DEFAULT 7", True)
            self.execute("ddl_rename_column", f"ALTER TABLE {table} RENAME COLUMN note TO revision", True)
            self.execute("ddl_drop", f"DROP TABLE {table}", True)
        self.execute("sentinel", "CREATE TABLE lake.sentinel AS SELECT 42 AS marker", True)
        self.execute("bulk_load", f"CREATE TABLE lake.events AS SELECT * FROM read_parquet({quote(job['input']['path'])})", True)
        self.publish("loaded")
        self.capture("loaded")
        checked(list(self.con.execute(f"SELECT {DIGEST} FROM lake.events").fetchone()), job["expected"]["loaded"]["scan"], "writer load")
        self.remote("loaded")
        self.after_reads("loaded")
        self.execute("ddl_loaded_add", "ALTER TABLE lake.events ADD COLUMN note BIGINT DEFAULT 7", True)
        self.execute("ddl_loaded_rename", "ALTER TABLE lake.events RENAME COLUMN note TO revision", True)
        rows = job["rows"]
        from duck_bench import fixture_select
        for number in range(job["small_commits"]):
            lo = rows + number * 32
            source = fixture_select(32, lo)
            self.execute("small_append", f"INSERT INTO lake.events SELECT *,7 FROM ({source})", True)
        self.execute("update_1pct", f"UPDATE lake.events SET id=md5('updated:'||i::VARCHAR) WHERE i < {rows // 100}", True)
        self.publish("updated")
        self.capture("updated")
        checked(list(self.con.execute(f"SELECT {DIGEST} FROM lake.events").fetchone()), job["expected"]["updated"]["scan"], "writer updates")
        checked(self.con.execute("SELECT min(revision),max(revision),count(*) FILTER (WHERE revision IS NULL) FROM lake.events").fetchone(),
                (7, 7, 0), "schema default on existing and appended rows")
        self.execute("delete_half", f"DELETE FROM lake.events WHERE i >= {rows // 2} AND i < {rows}", True)
        self.publish("half")
        self.capture("half_before_maintenance")
        self.maintain("half")
        self.remote("half")
        self.execute("drop_data_table", "DROP TABLE lake.events", True)
        self.publish("dropped")
        self.capture("dropped_before_maintenance")
        self.maintain("dropped")
        self.remote("dropped")
        self.capture("final_attached")
        self.con.close()
        self.record["final_closed"] = self.storage.snapshot()
        self.record["complete"] = True
        self.flush()
        return self.record


def read_worker(job, connect_client=connection, attach_client=attach_sql, stats=differential_stats, stats_key="differential"):
    con, setup_seconds = connect_client(job, reader=True)
    endpoint = os.environ["PGVS3_TEST_ENDPOINT_B"]
    result = {"complete": False, "client_setup_seconds": setup_seconds, "writer_repository_available": False,
              "queries": [], "external_file_cache": False, "remote_disk_cache_bytes": 0}
    before = gateway_metrics(endpoint)
    start = time.perf_counter()
    con.execute(attach_client(job, reader=True))
    result["attach_seconds"] = time.perf_counter() - start
    result["attach_gateway"] = delta(before, gateway_metrics(endpoint))
    successful_requests(result["attach_gateway"])
    before = gateway_metrics(endpoint)
    start = time.perf_counter()
    columns = con.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_catalog='lake' AND table_name='events' ORDER BY ordinal_position").fetchall()
    result["schema_seconds"] = time.perf_counter() - start
    result["schema_gateway"] = delta(before, gateway_metrics(endpoint))
    successful_requests(result["schema_gateway"])
    expected_columns = [("i", "BIGINT"), ("id", "VARCHAR"), ("value", "DOUBLE")]
    if job["stage"] == "half":
        expected_columns.append(("revision", "BIGINT"))
    checked(columns, [] if job["stage"] == "dropped" else expected_columns, "remote schema")
    if job["query"] == "scan":
        sql = f"SELECT {DIGEST} FROM lake.events"
    elif job["query"] == "range":
        sql = f"SELECT {DIGEST} FROM lake.events WHERE i BETWEEN {job['rows'] // 4} AND {job['rows'] // 4 + job['rows'] // 100}"
    else:
        sql = "SELECT marker FROM lake.sentinel"
    expected = [42] if job["stage"] == "dropped" else job["expected"][job["stage"]][job["query"]]
    for state in ("fresh_client", "same_client_repeat"):
        before = gateway_metrics(endpoint)
        start = time.perf_counter()
        actual = list(con.execute(sql).fetchone())
        seconds = time.perf_counter() - start
        checked(actual, expected, "remote " + state)
        result["queries"].append({"state": state, "seconds": seconds, "result": actual,
                                  "gateway": delta(before, gateway_metrics(endpoint))})
        if state == "fresh_client":
            result["attach_schema_first_query_seconds"] = result["attach_seconds"] + result["schema_seconds"] + seconds
        save(Path(job["folder"]) / "result.json", result)
        successful_requests(result["queries"][-1]["gateway"])
    if job["stage"] == "half":
        actual = con.execute("SELECT min(revision),max(revision),count(*) FILTER (WHERE revision IS NULL) FROM lake.events").fetchone()
        checked(actual, (7, 7, 0), "remote schema default")
        result["revision_check_outside_timing"] = list(actual)
    if job["profile"] != "ducklake":
        result[stats_key] = stats(con)
    con.execute("DETACH lake")
    con.close()
    result["client"] = inventory(Path(job["folder"]) / "client")
    result["complete"] = True
    return result


def reference(con, source, rows, commits):
    from duck_bench import fixture_select
    base = f"SELECT * FROM read_parquet({quote(source)})"
    append = fixture_select(commits * 32, rows)
    updated = ("SELECT i,CASE WHEN i < " + str(rows // 100) + " THEN md5('updated:'||i::VARCHAR) ELSE id END AS id,value FROM (" +
               base + " UNION ALL " + append + ")")
    result = {}
    for stage, query in (("loaded", base), ("updated", updated), ("half", updated + f" WHERE i < {rows // 2} OR i >= {rows}")):
        result[stage] = {"scan": list(con.execute(f"SELECT {DIGEST} FROM ({query})").fetchone()),
                         "range": list(con.execute(f"SELECT {DIGEST} FROM ({query}) WHERE i BETWEEN {rows // 4} AND {rows // 4 + rows // 100}").fetchone())}
    return result


def summarize(cohorts, failures=(), profiles=PROFILES):
    timings = {}
    unavailable = {(failure["target_mib"], failure["profile"]) for failure in failures}
    for cohort in cohorts:
        if not cohort["complete"]:
            raise RuntimeError("Incomplete cohorts cannot enter a comparison")
        for event in cohort["events"]:
            if "remote_ack_seconds" in event:
                timings.setdefault((cohort["target_mib"], event["operation"], cohort["profile"]), []).append(event["remote_ack_seconds"])
        for stage, reads in cohort["remote_reads"].items():
            for query, read in reads.items():
                timings.setdefault((cohort["target_mib"], stage + "_" + query + "_attach", cohort["profile"]), []).append(read["attach_seconds"])
                timings.setdefault((cohort["target_mib"], stage + "_" + query + "_fresh_total", cohort["profile"]), []).append(read["attach_schema_first_query_seconds"])
                for sample in read["queries"]:
                    timings.setdefault((cohort["target_mib"], stage + "_" + query + "_" + sample["state"], cohort["profile"]), []).append(sample["seconds"])
    cases = []
    for size, operation in sorted({(key[0], key[1]) for key in timings}):
        missing = [profile for profile in profiles if (size, operation, profile) not in timings]
        if any((size, profile) not in unavailable for profile in missing):
            raise RuntimeError(f"Missing a comparable profile for {size}/{operation}")
        medians = {profile: statistics.median(timings[size, operation, profile]) if profile not in missing else None for profile in profiles}
        if any(not math.isfinite(value) or value <= 0 for values in
               (timings.get((size, operation, profile), []) for profile in profiles) for value in values):
            raise RuntimeError("Invalid timing sample")
        cases.append({"target_mib": size, "operation": operation, "median_seconds": medians,
                       "samples": {profile: len(timings.get((size, operation, profile), [])) for profile in profiles},
                      "unavailable_profiles": missing,
                      "relative_to_ducklake": {profile: medians[profile] / medians["ducklake"] if medians[profile] is not None else None for profile in profiles[1:]},
                      "within_20pct_latency_budget": {profile: medians[profile] is not None and medians[profile] <= medians["ducklake"] * 1.2 for profile in profiles[1:]}})
    return cases


def print_summary(record):
    for case in record["cases"]:
        if case["operation"] not in ("ddl_create", "bulk_load", "small_append", "loaded_scan_fresh_total", "half_scan_fresh_total"):
            continue
        values = " / ".join("unavailable" if case["median_seconds"][profile] is None else f"{case['median_seconds'][profile] * 1000:.2f}" for profile in PROFILES)
        print(f"{case['target_mib']:4} MiB {case['operation']}: {values} ms (DuckLake / snapshot / sync)", flush=True)
    misses = {profile: sum(not case["within_20pct_latency_budget"][profile] for case in record["cases"]) for profile in PROFILES[1:]}
    print(f"Per-path latency budget misses/unavailable: {misses}; storage/client default unchanged", flush=True)


def run(args, run_name):
    import duckdb
    from duck_bench import fixture_select
    if duckdb.__version__ != DUCKDB:
        raise ValueError(f"This experiment requires DuckDB {DUCKDB}, not {duckdb.__version__}")
    if args.passes < 1 or args.differential_ddl_iterations < 1 or args.differential_small_commits < 1 or args.differential_timeout <= 0 or \
       any(size < 1 or size > 1024 for size in args.differential_mib) or len(set(args.differential_mib)) != len(args.differential_mib):
        raise ValueError("Use positive sample counts and distinct fixture sizes from 1 to 1024 MiB")
    artifact = provision(args.differential_extension)
    folder = Path(os.environ["PGVS3_TEST_RUN_DIR"]) / "differential"
    folder.mkdir(exist_ok=False)
    save(folder / "ownership.json", {"run": run_name})
    output = folder / "progress.json"
    result = {"schema_version": 1, "complete": False, "created_at": datetime.now(timezone.utc).isoformat(),
              "artifact": artifact, "host": host_info(), "python": platform.python_version(), "cohorts": [], "fixtures": [],
              "failed_cohorts": [], "failures": [], "skipped_cohorts": [],
              "client_settings": {"threads": 4, "memory_limit": "2GB", "http_client_pool_capacity": 16,
                                  "external_file_cache": False, "parquet_metadata_cache": False,
                                  "differential_remote_cache_size": "0", "differential_s3_max_requests": 16},
              "ducklake_delete_rewrite_threshold": 0.5,
              "runner_sha256": {name: digest(Path(__file__).parent / name) for name in ("differential.py", "duck_bench.py", "capacity.py", "stack.sh")},
              "postgres": json.loads(pg_sql("SELECT json_build_object('version',version(),'shared_buffers',current_setting('shared_buffers'),'synchronous_commit',current_setting('synchronous_commit'),'synchronous_standby_names',current_setting('synchronous_standby_names'))")),
              "gateway_sha256": digest(os.environ["PGVS3_TEST_BIN"]),
              "default_storage_client_changed": False,
              "comparison_contract": "Write timings compare remote durability, not identical reader refresh semantics; fresh reader timings include attach + schema + query but exclude process/extension setup",
              "scope": "Native pgvs3 buckets; snapshot exports vs per-commit remote WAL; single writer, no multi-writer or managed-service qualification"}
    save(output, result)
    print("Differential compatibility: multipart/checksums, conditional heads, abrupt-exit recovery and remote GC", flush=True)
    result["compatibility"] = compatibility(folder, run_name, artifact, args.differential_timeout)
    if not result["compatibility"]["automatic_writer_fencing"]:
        print("Safety finding: stale repositories are not automatically fenced for WAL shipping; use one externally fenced writer. DuckLake remains available and unchanged.", flush=True)
    save(output, result)
    reference_con = duckdb.connect()
    reference_con.execute("SET threads=4; SET memory_limit='2GB'")
    blocked = {}
    for size in args.differential_mib:
        source = folder / f"input-{size}MiB.parquet"
        rows = size * 23700
        sql = fixture_select(rows)
        reference_con.execute(f"COPY ({sql}) TO {quote(source)} (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880)")
        fixture = {"path": str(source.resolve()), "bytes": source.stat().st_size, "sha256": digest(source), "rows": rows,
                   "generation_sql": sql, "compression": "snappy", "row_group_rows": 122880}
        expected = reference(reference_con, source, rows, args.differential_small_commits)
        result["fixtures"].append(fixture)
        save(output, result)
        for repeat in range(args.passes):
            order = PROFILES[repeat % len(PROFILES):] + PROFILES[:repeat % len(PROFILES)]
            for profile in order:
                if (size, profile) in blocked:
                    result["skipped_cohorts"].append({"target_mib": size, "profile": profile, "pass": repeat,
                                                       "reason": "same fixture already failed the synchronous remote-ACK contract", "cause": blocked[size, profile]})
                    save(output, result)
                    continue
                suffix = f"m{size}_p{repeat}_{profile}"
                bucket = "dd-" + run_name + "-" + suffix.replace("_", "-")
                api(os.environ["PGVS3_TEST_ENDPOINT_A"], bucket)
                job = {"owner": run_name, "profile": profile, "mib": size, "pass": repeat, "rows": rows, "input": fixture, "expected": expected,
                       "folder": str(folder / suffix), "bucket": bucket, "location": f"s3://{bucket}/state/",
                       "metadata_schema": run_name + "_" + suffix, "extension": artifact["path"], "threads": 4,
                       "ddl_iterations": args.differential_ddl_iterations, "small_commits": args.differential_small_commits,
                       "timeout": args.differential_timeout}
                print(f"Differential comparison: {size} MiB, pass {repeat + 1}, {profile}", flush=True)
                try:
                    cohort = worker_call(job)
                except RuntimeError:
                    partial_path = Path(job["folder"]) / "result.json"
                    partial = json.loads(partial_path.read_text()) if partial_path.exists() else {}
                    if not epoch_limit_failure(profile, partial):
                        raise
                    failure = {"profile": profile, "target_mib": size, "pass": repeat,
                               "kind": "synchronous remote acknowledgement failed", "remote_acknowledged": False,
                               "local_commit_may_exist": True, "error": partial["error"], "evidence": str(partial_path)}
                    result["failures"].append(failure)
                    partial["failure_storage_after_process_exit"] = Storage(job).snapshot()
                    result["failed_cohorts"].append(partial)
                    blocked[size, profile] = str(partial_path)
                    save(output, result)
                    print(f"Unsupported sync-WAL workload at {size} MiB: epoch exceeds 256 MiB; retained failure, no batching/retry/weaker ACK", flush=True)
                    subprocess.run([os.environ["PGVS3_TEST_BIN"], "drop", "--url", os.environ["PGVS3_TEST_DB_URL"], "--force", bucket],
                                   capture_output=True, text=True, check=True, timeout=120)
                    continue
                cohort["client_after_writer_exit"] = inventory(Path(job["folder"]) / "client")
                result["cohorts"].append(cohort)
                save(output, result)
                if profile != "ducklake":
                    final_job = job | {"folder": str(Path(job["folder"]) / "finalize"),
                                       "repository": str(Path(job["folder"]) / "client/repository"),
                                       "client_source": str(Path(job["folder"]) / "client")}
                    cohort["after_writer_exit_maintenance"] = worker_call(final_job, "finalize")
                    cohort["client_after_finalizer_exit"] = inventory(Path(job["folder"]) / "client")
                    reader = job | {"folder": str(Path(job["folder"]) / "reader-quiescent"), "stage": "dropped", "query": "sentinel"}
                    cohort["quiescent_remote_check"] = worker_call(reader, "read")
                    save(output, result)
                subprocess.run([os.environ["PGVS3_TEST_BIN"], "drop", "--url", os.environ["PGVS3_TEST_DB_URL"], "--force", bucket],
                               capture_output=True, text=True, check=True, timeout=120)
                if profile == "ducklake":
                    pg_sql(f"DROP SCHEMA {ident(job['metadata_schema'])} CASCADE", "ducklake_catalog")
    reference_con.close()
    result["cases"] = summarize(result["cohorts"], result["failures"])
    result["all_profiles_completed"] = not result["failures"]
    result["complete"] = True
    save(output, result)
    print(f"Completed {len(result['cohorts'])} cohorts; {len(result['cases'])} per-path comparisons. Raw evidence: {output}", flush=True)
    print_summary(result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("setup", help="cache the checksum-pinned upstream release artifact (no global INSTALL)")
    setup.add_argument("--extension", default=os.environ.get("PGVS3_DIFFERENTIAL_EXTENSION"))
    setup.add_argument("--cache")
    worker = commands.add_parser("worker")
    worker.add_argument("action", choices=("cohort", "read", "finalize", "proof"))
    worker.add_argument("job")
    args = parser.parse_args()
    if args.command == "setup":
        print(json.dumps(provision(args.extension, args.cache), indent=2))
        return
    job = json.loads(Path(args.job).read_text())
    validate_job(job)
    try:
        result = {"cohort": lambda: Cohort(job).run(), "read": lambda: read_worker(job),
                  "finalize": lambda: finalize_worker(job), "proof": lambda: proof_worker(job)}[args.action]()
        save(Path(job["folder"]) / "result.json", result)
        if args.action == "proof" and job["proof_step"] == "writer":
            os._exit(0)
    except BaseException as error:
        path = Path(job["folder"]) / "result.json"
        record = json.loads(path.read_text()) if path.exists() else {"complete": False}
        message = redact(str(error))
        record["error"] = {"type": type(error).__name__, "message": message}
        save(path, record)
        raise RuntimeError(message) from None


if __name__ == "__main__":
    main()
