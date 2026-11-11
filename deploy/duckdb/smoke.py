#!/usr/bin/env python3
"""Host-side, local, no-PG/no-S3 smoke for the Hardcharger (ChargeWire) image.

This script drives Docker from the host. It runs the copied official /duckdb
CLI through ``docker exec`` for the pure extension checks, then starts the
image's own ``hardcharger-server`` entrypoint in a read-only, non-root
container and talks to it with ``adbc-driver-chargewire``. It covers all eight
baked modules with unsigned loading off, spatial RTree, VSS HNSW recall, FTS,
RaQuet function presence, a local Differential snapshot, ChargeWire readiness
via the plaintext ``/health`` port, unqualified writes landing in ``lake``,
``?`` bind parameters, a DuckLake restart and a clean ``docker stop`` exit 0
with no ``-i`` and no ``--init``.
"""

from __future__ import annotations

import csv
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOCK_PATH = ROOT / "extensions.lock"
MODULES = (
    "spatial",
    "ducklake",
    "postgres_scanner",
    "fts",
    "vss",
    "httpfs",
    "raquet",
    "differential",
)
PRELUDE = "\n".join(
    [
        "SET autoinstall_known_extensions=false;",
        "SET autoload_known_extensions=false;",
        "SET allow_unsigned_extensions=false;",
        "SET threads=2;",
        "SET memory_limit='2GB';",
        *(f"LOAD {module};" for module in MODULES),
    ]
)
SQL_DIR = "/opt/pgvs3-duckdb"
ADMIN_USER = "pgvs3"


class SmokeError(RuntimeError):
    pass


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def docker(*args: str, input_text: str | None = None, timeout: float = 300) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args],
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def first_line(output: str, label: str) -> str:
    lines = [line for line in output.splitlines() if line.strip()]
    if len(lines) != 1:
        raise SmokeError(f"{label}: expected one row, got {lines!r}")
    return lines[0]


def expect_one(output: str, expected: str, label: str) -> None:
    actual = first_line(output, label)
    if actual != expected:
        raise SmokeError(f"{label}: expected {expected!r}, got {actual!r}")


def locked_modules() -> dict[str, dict]:
    lock = json.loads(LOCK_PATH.read_text())
    return {module["name"]: module for module in lock["extensions"]}


class Holder:
    """A read-only, non-root container used as the native CLI execution sandbox."""

    def __init__(self, image: str) -> None:
        self.image = image
        self.name = f"pgvs3-duckdb-smoke-{os.getpid()}"
        docker("rm", "-f", self.name)
        result = docker(
            "run",
            "-d",
            "--rm",
            "--name",
            self.name,
            "--read-only",
            "--tmpfs",
            "/tmp:rw,size=2g",
            "--entrypoint",
            "sleep",
            image,
            "infinity",
        )
        if result.returncode != 0:
            raise SmokeError(f"cannot start smoke container: {result.stderr.strip()}")

    def exec_sql(self, sql: str, timeout: float = 180) -> str:
        result = docker(
            "exec",
            "-i",
            self.name,
            "/duckdb",
            "-no-agent",
            "-batch",
            "-noheader",
            "-csv",
            "-bail",
            input_text=PRELUDE + "\n" + sql + "\n",
            timeout=timeout,
        )
        if result.returncode != 0:
            raise SmokeError((result.stderr or result.stdout).strip())
        return result.stdout.strip()

    def close(self) -> None:
        docker("rm", "-f", self.name)


def chargewire_connect(port: int, password: str):
    from adbc_driver_chargewire import dbapi

    return dbapi.connect(
        f"chargewire://127.0.0.1:{port}?tls=false",
        username=ADMIN_USER,
        password=password,
    )


class ChargeWire:
    """The image's own hardcharger-server entrypoint, no stdin and no init."""

    def __init__(self, image: str, state: Path) -> None:
        self.image = image
        self.name = f"pgvs3-chargewire-smoke-{os.getpid()}"
        self.port = free_port()
        self.health_port = free_port()
        self.password = os.urandom(16).hex()
        self.state = state
        state.mkdir(parents=True, exist_ok=True)
        os.chmod(state, 0o777)
        # Hardcharger's DuckDB has no getenv(): render the concrete init SQL
        # from a local DuckLake template before the container starts.
        init_sql = state / "lake-init.sql"
        init_sql.write_text(
            "SET extension_directories=['/opt/pgvs3-duckdb/extensions'];\n"
            "SET autoinstall_known_extensions=false;\n"
            "SET autoload_known_extensions=false;\n"
            "SET allow_unsigned_extensions=false;\n"
            + "".join(f"LOAD {module};\n" for module in MODULES)
            + f"ATTACH 'ducklake:/state/smoke.ducklake' AS lake "
              f"(DATA_PATH '/state/data/', AUTOMATIC_MIGRATION false);\n"
        )
        os.chmod(init_sql, 0o644)
        docker("rm", "-f", self.name)
        args = [
            "run",
            "-d",
            "--name",
            self.name,
            "--read-only",
            "--tmpfs",
            "/tmp:rw,size=1g",
            "-v",
            f"{state}:/state:rw",
            "-v",
            f"{init_sql}:/opt/pgvs3-duckdb/init.sql:ro",
            "-p",
            f"127.0.0.1:{self.port}:31337",
            "-p",
            f"127.0.0.1:{self.health_port}:31338",
            "-e",
            f"HARDCHARGER_ADMIN_USERNAME={ADMIN_USER}",
            "-e",
            f"HARDCHARGER_ADMIN_PASSWORD={self.password}",
            "-e",
            "HARDCHARGER_MEMORY_LIMIT=1GB",
            "-e",
            "DUCKDB_MEMORY_LIMIT=1GB",
            "-e",
            "DUCKDB_THREADS=4",
        ]
        result = docker(*args, image, timeout=120)
        if result.returncode != 0:
            raise SmokeError(f"cannot start ChargeWire container: {result.stderr.strip()}")
        try:
            self.wait_ready()
        except SmokeError:
            self.logs()
            raise

    def health(self) -> dict:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.health_port}/health", timeout=2
        ) as response:
            return json.loads(response.read())

    def wait_ready(self, seconds: float = 90) -> None:
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                health = self.health()
                if health.get("status") == "serving":
                    return
                last = health
            except OSError as error:
                last = error
            state = json.loads(
                docker("inspect", "--format", "{{json .State}}", self.name).stdout or "{}"
            )
            if state.get("Status") == "exited":
                raise SmokeError("ChargeWire server exited before ready:\n" + self.logs())
            time.sleep(0.1)
        raise SmokeError(f"ChargeWire server did not become ready: {last}")

    def logs(self) -> str:
        return docker("logs", self.name).stdout + docker("logs", self.name).stderr

    def stop(self) -> dict:
        started = time.perf_counter()
        docker("stop", "--timeout", "20", self.name, timeout=60)
        elapsed = round((time.perf_counter() - started) * 1000, 1)
        state = json.loads(docker("inspect", "--format", "{{json .State}}", self.name).stdout)
        return {"exit_code": state["ExitCode"], "oom_killed": state["OOMKilled"], "stop_ms": elapsed}

    def close(self) -> None:
        docker("rm", "-f", self.name)


def test_modules(holder: Holder) -> float:
    started = time.perf_counter()
    names = ",".join(sql_literal(module) for module in MODULES)
    output = holder.exec_sql(
        "SELECT version(), current_setting('allow_unsigned_extensions');\n"
        "SELECT extension_name, extension_version, installed_from, loaded\n"
        "FROM duckdb_extensions()\n"
        f"WHERE extension_name IN ({names})\n"
        "ORDER BY extension_name;"
    )
    rows = [row for row in csv.reader(output.splitlines()) if row]
    if not rows or rows[0][0] != "v2.0.0" or rows[0][1] != "false":
        raise SmokeError(f"modules: unexpected metadata rows {rows[:2]!r}")
    expected = locked_modules()
    extensions = rows[1:]
    if len(extensions) != len(MODULES):
        raise SmokeError(f"modules: expected {len(MODULES)} rows, got {extensions!r}")
    for name, version, origin, loaded in extensions:
        if name not in expected:
            raise SmokeError(f"modules: unexpected extension {name}")
        if loaded != "true" or version != expected[name]["version"] or origin != expected[name]["origin"]:
            raise SmokeError(f"modules: bad row {name},{version},{origin},{loaded}")
    return (time.perf_counter() - started) * 1000


def test_spatial(holder: Holder) -> float:
    started = time.perf_counter()
    output = holder.exec_sql(
        "SELECT ST_AsText(ST_Point(1,2));\n"
        "CREATE TABLE pts AS\n"
        "  SELECT i AS id, ST_Point(i % 100, i // 100) AS geom\n"
        "  FROM range(1000) t(i);\n"
        "CREATE INDEX pts_rtree ON pts USING RTREE (geom);\n"
        "SELECT count(*) FROM duckdb_indexes() WHERE index_name='pts_rtree';"
    )
    lines = [line for line in output.splitlines() if line.strip()]
    if lines != ["POINT (1 2)", "1"]:
        raise SmokeError(f"spatial: unexpected output {lines!r}")
    return (time.perf_counter() - started) * 1000


def test_vss(holder: Holder) -> float:
    started = time.perf_counter()
    query = [math.sin(42 * 0.01 + j * 0.1) for j in range(32)]
    literal = "[" + ",".join(f"{value:.9g}" for value in query) + "]::FLOAT[32]"
    columns = ", ".join(f"sin(i*0.01 + {j}*0.1)::FLOAT" for j in range(32))
    output = holder.exec_sql(
        "CREATE TABLE vecs AS\n"
        f"  SELECT i AS id, array_value({columns}) AS vec\n"
        "  FROM range(5000) t(i);\n"
        "CREATE TABLE exact AS\n"
        "  SELECT id FROM vecs\n"
        f"  ORDER BY array_cosine_distance(vec, {literal}) LIMIT 10;\n"
        "CREATE INDEX vec_hnsw ON vecs USING HNSW (vec) WITH (metric='cosine');\n"
        "CREATE TABLE approx AS\n"
        "  SELECT id FROM vecs\n"
        f"  ORDER BY array_cosine_distance(vec, {literal}) LIMIT 10;\n"
        "SELECT count(*) FROM exact JOIN approx USING (id);\n"
        "SELECT count(*) FROM duckdb_indexes() WHERE index_name='vec_hnsw';\n"
        "EXPLAIN SELECT id FROM vecs\n"
        f"  ORDER BY array_cosine_distance(vec, {literal}) LIMIT 10;"
    )
    if "hnsw index scan" not in output.lower():
        raise SmokeError("vss: EXPLAIN did not show HNSW Index Scan")
    lines = output.splitlines()
    if lines[:2] != ["10", "1"]:
        raise SmokeError(f"vss: recall/index check failed: {lines[:5]!r}")
    return (time.perf_counter() - started) * 1000


def test_fts(holder: Holder) -> float:
    started = time.perf_counter()
    output = holder.exec_sql(
        "CREATE TABLE docs (id INTEGER, body VARCHAR);\n"
        "INSERT INTO docs VALUES (100,'zebra striped'),(200,'quokka'),(101,'another zebra');\n"
        "PRAGMA create_fts_index('docs','id','body');\n"
        "SELECT count(*) FROM (\n"
        "  SELECT *, fts_main_docs.match_bm25(id, 'zebra') AS score FROM docs\n"
        ") WHERE score IS NOT NULL;\n"
        "SELECT id FROM (\n"
        "  SELECT *, fts_main_docs.match_bm25(id, 'zebra') AS score FROM docs\n"
        ") WHERE score IS NOT NULL ORDER BY score DESC LIMIT 1;"
    )
    if output.splitlines() != ["2", "101"]:
        raise SmokeError(f"fts: unexpected output {output!r}")
    return (time.perf_counter() - started) * 1000


def test_raquet(holder: Holder) -> float:
    started = time.perf_counter()
    output = holder.exec_sql(
        "SELECT count(DISTINCT function_name) FROM duckdb_functions()\n"
        "WHERE function_name IN ('read_raquet_metadata','read_raquet_at');"
    )
    expect_one(output, "2", "raquet")
    return (time.perf_counter() - started) * 1000


def test_differential(holder: Holder) -> float:
    started = time.perf_counter()
    first = holder.exec_sql(
        f"SET differential_repository={sql_literal('/tmp/differential/repo')};\n"
        "ATTACH 'differential:smoke' AS d;\n"
        "USE d;\n"
        "CREATE TABLE t (id INTEGER);\n"
        "INSERT INTO t VALUES (1),(2),(3);\n"
        "CHECKPOINT;\n"
        "CALL differential_create_snapshot('d', name='v1');\n"
        "SELECT count(*) FROM differential_snapshots('d')\n"
        "  WHERE snapshot_name='v1' AND publication_state='local_published'\n"
        "    AND NOT retired;\n"
        "USE memory;\n"
        "DETACH d;"
    )
    if first.splitlines()[-1] != "1":
        raise SmokeError(f"differential: snapshot failed: {first!r}")
    second = holder.exec_sql(
        f"SET differential_repository={sql_literal('/tmp/differential/repo')};\n"
        "ATTACH 'differential:smoke' AS d2;\n"
        "SELECT count(*) FROM d2.main.t;\n"
        "DETACH d2;"
    )
    expect_one(second, "3", "differential reopen")
    return (time.perf_counter() - started) * 1000


def test_chargewire(holder: Holder, server: ChargeWire) -> float:
    """Readiness, unqualified lake writes, ``?`` binds, native memory tables."""
    started = time.perf_counter()
    health = server.health()
    if health != {"status": "serving"}:
        raise SmokeError(f"health: unexpected body {health!r}")
    connection = chargewire_connect(server.port, server.password)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT current_catalog(), current_schema()")
        if cursor.fetchall() != [("lake", "main")]:
            raise SmokeError("chargewire: default catalog is not lake")
        cursor.execute("CREATE TABLE persisted (id INTEGER, s VARCHAR)")
        cursor.execute("INSERT INTO persisted VALUES (?, ?)", [7, "seven"])
        cursor.execute("INSERT INTO persisted VALUES (?, ?)", [8, "eight"])
        cursor.execute("SELECT * FROM persisted ORDER BY id")
        if cursor.fetchall() != [(7, "seven"), (8, "eight")]:
            raise SmokeError("chargewire: unqualified inserts did not round-trip")
        cursor.execute(
            "SELECT table_catalog FROM information_schema.tables WHERE table_name='persisted'"
        )
        if cursor.fetchall() != [("lake",)]:
            raise SmokeError("chargewire: persisted did not land in lake")
        cursor.execute("CALL ducklake_flush_inlined_data('lake')")
        cursor.execute("CREATE TABLE memory.main.native_t (id INTEGER)")
        cursor.execute("INSERT INTO memory.main.native_t VALUES (1)")
        cursor.execute("SELECT count(*) FROM memory.main.native_t")
        if cursor.fetchall() != [(1,)]:
            raise SmokeError("chargewire: native memory table failed")
        cursor.execute("SELECT count(*) FROM persisted")
        if cursor.fetchall() != [(2,)]:
            raise SmokeError("chargewire: the client left the lake catalog")
        cursor.close()
    finally:
        connection.close()

    stopped = server.stop()
    if stopped["exit_code"] != 0:
        raise SmokeError(f"chargewire: docker stop exited {stopped['exit_code']}: {server.logs()[-1500:]}")
    if stopped["oom_killed"]:
        raise SmokeError("chargewire: container was OOM killed")

    restarted = ChargeWire(server.image, server.state)
    try:
        connection = chargewire_connect(restarted.port, restarted.password)
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT count(*) FROM persisted")
            if cursor.fetchall() != [(2,)]:
                raise SmokeError("chargewire: persisted rows did not survive restart")
            cursor.close()
        finally:
            connection.close()
        restarted_stop = restarted.stop()
        if restarted_stop["exit_code"] != 0:
            raise SmokeError(f"chargewire: restart stop exited {restarted_stop['exit_code']}")
    finally:
        restarted.close()
    return (time.perf_counter() - started) * 1000


def run(image: str) -> int:
    holder = Holder(image)
    timings: list[tuple[str, float]] = []
    state = Path("/tmp") / f"pgvs3-chargewire-smoke-{os.getpid()}"
    server = ChargeWire(image, state)
    try:
        for name, function in (
            ("modules", test_modules),
            ("spatial", test_spatial),
            ("vss", test_vss),
            ("fts", test_fts),
            ("raquet", test_raquet),
            ("differential", test_differential),
        ):
            elapsed = function(holder)
            timings.append((name, elapsed))
            print(f"ok {name} {elapsed:.1f}ms", flush=True)
        elapsed = test_chargewire(holder, server)
        timings.append(("chargewire", elapsed))
        print(f"ok chargewire {elapsed:.1f}ms", flush=True)
    except (SmokeError, subprocess.TimeoutExpired) as error:
        print(f"smoke failed: {error}", file=sys.stderr)
        return 1
    finally:
        server.close()
        holder.close()
        shutil.rmtree(state, ignore_errors=True)
    print(
        "smoke ok; illustrative process-level timings: "
        + " ".join(f"{name}={elapsed:.1f}ms" for name, elapsed in timings)
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = sys.argv[1:] if argv is None else argv
    if len(parser) != 1:
        print("usage: smoke.py IMAGE", file=sys.stderr)
        return 2
    return run(parser[0])


if __name__ == "__main__":
    raise SystemExit(main())
