#!/usr/bin/env python3
"""Host-side, local, no-PG/no-S3 smoke for the baked native DuckDB image.

This script drives the image's own /duckdb CLI through Docker. It does not run
Python or a shell inside the image, does not use ``docker exec python``, and
does not start a custom SQL service. The tests exercise the exact signed
artifacts baked into the image: module versions/unsigned-off, spatial RTree,
VSS HNSW recall, FTS token search, RaQuet function presence, a local
Differential snapshot, Quack memory and a local DuckLake restart.
"""

from __future__ import annotations

import csv
import json
import math
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOCK_PATH = ROOT / "extensions.lock"
MODULES = (
    "spatial",
    "ducklake",
    "postgres_scanner",
    "fts",
    "vss",
    "quack",
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


class SmokeError(RuntimeError):
    pass


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Holder:
    """A read-only, non-root container used as the native CLI execution sandbox."""

    def __init__(self, image: str) -> None:
        self.image = image
        self.name = f"pgvs3-duckdb-smoke-{os.getpid()}"
        docker("rm", "-f", self.name)
        result = docker(
            "run",
            "-d",
            "-i",
            "--init",
            "--rm",
            "--name",
            self.name,
            "--read-only",
            "--tmpfs",
            "/tmp:rw,size=2g",
            "--tmpfs",
            "/smoke:rw,size=2g",
            "--entrypoint",
            "/duckdb",
            image,
            "-no-agent",
            "-batch",
            "-bail",
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

    def start_server(self, sql: str) -> "Server":
        command = [
            "docker",
            "exec",
            "-i",
            self.name,
            "/duckdb",
            "-no-agent",
            "-batch",
            "-noheader",
            "-csv",
            "-bail",
        ]
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdin is not None
        process.stdin.write(PRELUDE + "\n" + sql + "\n")
        process.stdin.flush()
        return Server(process)

    def close(self) -> None:
        docker("rm", "-f", self.name)


class Server:
    def __init__(self, process: subprocess.Popen[str]) -> None:
        self.process = process
        self.lines: list[str] = []
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._read, name="quack-output", daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with self.lock:
                ready = any(line.strip() == "READY" for line in self.lines)
            if ready:
                return
            if process.poll() is not None:
                raise SmokeError("Quack server exited before ready:\n" + self.detail())
            time.sleep(0.1)
        self.stop()
        raise SmokeError("Quack server did not become ready:\n" + self.detail())

    def _read(self) -> None:
        assert self.process.stdout is not None
        for raw in self.process.stdout:
            with self.lock:
                self.lines.append(raw.rstrip())
                if len(self.lines) > 100:
                    del self.lines[:50]

    def detail(self) -> str:
        with self.lock:
            return "\n".join(self.lines[-20:])

    def stop(self) -> None:
        try:
            if self.process.stdin is not None:
                self.process.stdin.write(".quit\n")
                self.process.stdin.flush()
                self.process.stdin.close()
        except (BrokenPipeError, OSError):
            pass
        try:
            self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.thread.join(1)


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
        raise SmokeError(f"modules: expected 9 rows, got {extensions!r}")
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
        f"SET differential_repository={sql_literal('/smoke/differential/repo')};\n"
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
        f"SET differential_repository={sql_literal('/smoke/differential/repo')};\n"
        "ATTACH 'differential:smoke' AS d2;\n"
        "SELECT count(*) FROM d2.main.t;\n"
        "DETACH d2;"
    )
    expect_one(second, "3", "differential reopen")
    return (time.perf_counter() - started) * 1000


def test_quack_memory(holder: Holder) -> float:
    started = time.perf_counter()
    port = free_port()
    token = f"smoke-token-{os.getpid()}"
    server = holder.start_server(
        ".output /dev/null\n"
        "CREATE TABLE memory_t (id INTEGER, s VARCHAR);\n"
        "INSERT INTO memory_t VALUES (1,'one'),(2,'two');\n"
        f"CALL quack_serve('quack:0.0.0.0:{port}', disable_ssl=true, "
        f"allow_other_hostname=true, token={sql_literal(token)});\n"
        ".output\n"
        "SELECT 'READY';"
    )
    try:
        output = holder.exec_sql(
            "LOAD quack;\n"
            f"ATTACH 'quack:127.0.0.1:{port}' AS q (TYPE quack, TOKEN {sql_literal(token)});\n"
            "SELECT * FROM q.main.memory_t ORDER BY id;"
        )
        if output.splitlines() != ["1,one", "2,two"]:
            raise SmokeError(f"quack memory: unexpected output {output!r}")
    finally:
        server.stop()
    return (time.perf_counter() - started) * 1000


def test_quack_lake_restart(holder: Holder) -> float:
    started = time.perf_counter()
    port = free_port()
    token = f"smoke-token-lake-{os.getpid()}"
    catalog = "/smoke/catalog.ducklake"
    files = "/smoke/lake-data/"
    server_sql = (
        ".output /dev/null\n"
        f"ATTACH {sql_literal('ducklake:' + catalog)} AS lake "
        f"(DATA_PATH {sql_literal(files)}, AUTOMATIC_MIGRATION false);\n"
        "USE lake;\n"
        f"CALL quack_serve('quack:0.0.0.0:{port}', disable_ssl=true, "
        f"allow_other_hostname=true, token={sql_literal(token)});\n"
        ".output\n"
        "SELECT 'READY';"
    )
    server = holder.start_server(server_sql)
    try:
        output = holder.exec_sql(
            "LOAD quack;\n"
            f"ATTACH 'quack:127.0.0.1:{port}' AS q (TYPE quack, TOKEN {sql_literal(token)});\n"
            "CREATE TABLE q.lake.main.persisted (id INTEGER);\n"
            "INSERT INTO q.lake.main.persisted VALUES (7),(8);\n"
            "SELECT count(*) FROM q.lake.main.persisted;"
        )
        if output.splitlines()[-1] != "2":
            raise SmokeError(f"quack lake write: unexpected output {output!r}")
    finally:
        server.stop()

    server = holder.start_server(server_sql)
    try:
        output = holder.exec_sql(
            "LOAD quack;\n"
            f"ATTACH 'quack:127.0.0.1:{port}' AS q (TYPE quack, TOKEN {sql_literal(token)});\n"
            "SELECT * FROM q.lake.main.persisted ORDER BY id;"
        )
        if output.splitlines() != ["7", "8"]:
            raise SmokeError(f"quack lake restart: unexpected output {output!r}")
    finally:
        server.stop()
    return (time.perf_counter() - started) * 1000


def run(image: str) -> int:
    holder = Holder(image)
    timings: list[tuple[str, float]] = []
    try:
        for name, function in (
            ("modules", test_modules),
            ("spatial", test_spatial),
            ("vss", test_vss),
            ("fts", test_fts),
            ("raquet", test_raquet),
            ("differential", test_differential),
            ("quack_memory", test_quack_memory),
            ("quack_lake_restart", test_quack_lake_restart),
        ):
            elapsed = function(holder)
            timings.append((name, elapsed))
            print(f"ok {name} {elapsed:.1f}ms", flush=True)
    except (SmokeError, subprocess.TimeoutExpired) as error:
        print(f"smoke failed: {error}", file=sys.stderr)
        return 1
    finally:
        holder.close()
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
