#!/usr/bin/env python3
"""DuckDB on pgvs3: S3 request benchmark, DuckLake compatibility and tuning.

Run through `just bench` (deploy/bench/stack.sh), which starts a disposable
PostgreSQL and two gateways and exports PGVS3_TEST_ENDPOINT_A/B,
PGVS3_ACCESS_KEY, PGVS3_SECRET_KEY and PGVS3_CATALOG.

Parts:
  s3      small-object PUT/GET rate and latency, large-object throughput,
          measured by DuckDB httpfs with its HTTP request log.
  compat  DuckLake lifecycle on pgvs3: writer on gateway A, reader on B.
  tuning  DuckLake Parquet row group size / file size vs load and query time.
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time
import uuid

import duckdb

MIB = 1 << 20


def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(endpoint: str, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """A fresh DuckDB with an S3 secret for one gateway and no file cache."""
    con = duckdb.connect()
    for ext in ("httpfs", "postgres", "ducklake"):
        con.sql(f"INSTALL {ext}")
        con.sql(f"LOAD {ext}")
    host = endpoint.split("://", 1)[-1]
    con.sql(
        "CREATE SECRET pgvs3 (TYPE s3, URL_STYLE 'path', USE_SSL false, REGION 'us-east-1', "
        f"ENDPOINT {sql_str(host)}, KEY_ID {sql_str(os.environ['PGVS3_ACCESS_KEY'])}, "
        f"SECRET {sql_str(os.environ['PGVS3_SECRET_KEY'])})"
    )
    # Measure the store, not DuckDB's in-process cache of remote bytes.
    con.sql("SET enable_external_file_cache=false")
    if threads:
        con.sql(f"SET threads={threads}")
    con.sql("CALL enable_logging('HTTP')")
    return con


def timed(con, sql: str) -> tuple[float, list]:
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    return time.perf_counter() - t0, rows


def requests(con, *methods: str) -> dict:
    """Count, whole-ms latency percentiles and bytes of logged requests, then
    clear the log."""
    rows = con.sql(
        "SELECT request.duration_ms, "
        "CASE WHEN request.type = 'GET' THEN TRY_CAST(response.headers['content-length'] AS BIGINT) END "
        "FROM duckdb_logs_parsed('HTTP') WHERE list_contains(?, request.type)",
        params=[list(methods)],
    ).fetchall()
    con.sql("CALL truncate_duckdb_logs()")
    if not rows:
        return {"count": 0}
    ms = sorted(r[0] for r in rows)
    pick = lambda q: ms[min(len(ms) - 1, int(q * len(ms)))]
    return {
        "count": len(ms),
        "p50_ms": pick(0.50),
        "p99_ms": pick(0.99),
        "max_ms": ms[-1],
        "get_bytes": sum(r[1] or 0 for r in rows),
    }


# --- s3 ----------------------------------------------------------------------


def concurrent(con, statements: list[str], workers: int) -> dict:
    """Run one statement per object from `workers` cursors at once; per-object
    latency is measured here, at microsecond resolution."""
    latencies: list[float] = []
    lock = threading.Lock()
    queue = list(reversed(statements))

    def work():
        cur = con.cursor()
        mine = []
        while True:
            with lock:
                if not queue:
                    break
                sql = queue.pop()
            t0 = time.perf_counter()
            cur.execute(sql).fetchall()
            mine.append(time.perf_counter() - t0)
        with lock:
            latencies.extend(mine)

    t0 = time.perf_counter()
    threads = [threading.Thread(target=work) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    secs = time.perf_counter() - t0
    ms = sorted(x * 1000 for x in latencies)
    pick = lambda q: round(ms[min(len(ms) - 1, int(q * len(ms)))], 2)
    return {"workers": workers, "seconds": round(secs, 3), "objects_per_s": round(len(ms) / secs, 1),
            "p50_ms": pick(0.50), "p99_ms": pick(0.99)}


def s3(args, run: str) -> dict:
    con = connect(os.environ["PGVS3_TEST_ENDPOINT_A"])
    out = {}
    for label, count, size, workers in (
        ("small", args.small_objects, args.small_bytes, (1, 64)),
        ("large", args.large_objects, args.large_mib * MIB, (1, 16)),
    ):
        result = {"objects": count, "object_bytes": size}
        for n in workers:
            paths = [f"s3://bench/{run}/{label}-w{n}/{i}.csv" for i in range(count)]
            # One single-row CSV per object: exactly `size` bytes with its newline.
            put = concurrent(con, [
                f"COPY (SELECT repeat('x', {size - 1})) TO '{p}' (FORMAT csv, HEADER false)"
                for p in paths
            ], n)
            put["requests"] = requests(con, "PUT", "POST")
            reads = [f"SELECT octet_length(content) FROM read_blob('{p}')" for p in paths]
            # The first read after a write also sets PostgreSQL hint bits; the
            # repeat is the steady state.
            get = concurrent(con, reads, n)
            get["requests"] = requests(con, "GET")
            again = concurrent(con, reads, n)
            again["requests"] = requests(con, "GET")
            for r in (put, get, again):
                r["mib_per_s"] = round(r["objects_per_s"] * size / MIB, 1)
            result[f"put_w{n}"] = put
            result[f"get_w{n}_first"] = get
            result[f"get_w{n}"] = again
        check = con.execute(
            f"SELECT count(*), sum(size) FROM read_blob('s3://bench/{run}/{label}-w1/*')"
        ).fetchall()
        assert check == [(count, count * size)], check
        out[label] = result
    return out


# --- compat ------------------------------------------------------------------


def attach(con, run: str) -> None:
    con.sql(
        f"ATTACH {sql_str('ducklake:postgres:' + os.environ['PGVS3_CATALOG'])} AS lake "
        f"(DATA_PATH 's3://lake/{run}/', METADATA_SCHEMA {sql_str(run)})"
    )


def compat(args, run: str) -> dict:
    writer = connect(os.environ["PGVS3_TEST_ENDPOINT_A"])
    reader = connect(os.environ["PGVS3_TEST_ENDPOINT_B"])
    attach(writer, run)
    attach(reader, run)
    checks = []

    def check(name, got, want):
        ok = got == want
        checks.append({"check": name, "ok": ok, **({} if ok else {"got": repr(got), "want": repr(want)})})
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f": got {got!r}, want {want!r}"))

    def one(con, sql):
        return con.sql(sql).fetchall()

    # Every insert writes Parquet to pgvs3 rather than inlining rows in the catalog.
    writer.sql("CALL lake.set_option('data_inlining_row_limit', 0)")
    writer.sql("CREATE TABLE lake.t AS SELECT i AS id, i % 97 AS k, md5(i::VARCHAR) AS s FROM range(100000) t(i)")
    v1 = one(writer, "SELECT id FROM lake.current_snapshot()")[0][0]
    check("reader on gateway B sees the writer's table",
          one(reader, "SELECT count(*), sum(id) FROM lake.t"), [(100000, 4999950000)])

    writer.sql("UPDATE lake.t SET k = -1 WHERE id < 1000")
    writer.sql("DELETE FROM lake.t WHERE id >= 99000")
    check("update and delete visible on B",
          one(reader, "SELECT count(*), count(*) FILTER (k = -1) FROM lake.t"), [(99000, 1000)])
    check("time travel to the first snapshot",
          one(reader, f"SELECT count(*) FROM lake.t AT (VERSION => {v1})"), [(100000,)])

    writer.sql("ALTER TABLE lake.t ADD COLUMN note VARCHAR DEFAULT 'new'")
    for part in range(5):
        writer.sql(f"INSERT INTO lake.t SELECT 200000 + {part} * 10 + i, 0, 'x', 'small' FROM range(10) t(i)")
    check("schema evolution and small appends on B",
          one(reader, "SELECT count(*), count(*) FILTER (note = 'new'), count(*) FILTER (note = 'small') FROM lake.t"),
          [(99050, 99000, 50)])

    files_before = one(writer, "SELECT count(*) FROM ducklake_list_files('lake', 't')")[0][0]
    writer.sql("CALL ducklake_merge_adjacent_files('lake')")
    writer.sql("CALL ducklake_rewrite_data_files('lake')")
    writer.sql("CALL ducklake_expire_snapshots('lake', older_than => now())")
    writer.sql("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
    files_after = one(writer, "SELECT count(*) FROM ducklake_list_files('lake', 't')")[0][0]
    check("compaction reduces the file count", files_after < files_before, True)
    check("reader still correct after compaction and cleanup",
          one(reader, "SELECT count(*), sum(id) FILTER (id < 99000) FROM lake.t"),
          [(99050, 99000 * 98999 // 2)])

    referenced = {
        path
        for row in one(writer, "SELECT data_file, delete_file FROM ducklake_list_files('lake', 't')")
        for path in row
        if path
    }
    stored = {row[0] for row in one(writer, f"SELECT file FROM glob('s3://lake/{run}/**')")}
    check("cleanup deleted every unreferenced object", stored - referenced, set())
    check("every referenced object is stored", referenced - stored, set())

    writer.sql("DROP TABLE lake.t")
    writer.sql("CALL ducklake_expire_snapshots('lake', older_than => now())")
    writer.sql("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
    check("dropped table's objects are deleted",
          one(writer, f"SELECT count(*) FROM glob('s3://lake/{run}/**')"), [(0,)])
    return {"passed": sum(c["ok"] for c in checks), "failed": sum(not c["ok"] for c in checks),
            "checks": checks}


# --- tuning ------------------------------------------------------------------

QUERIES = {
    "full_scan": "SELECT sum(v), sum(k) FROM lake.t",
    "string_filter": "SELECT count(*) FROM lake.t WHERE s LIKE 'ab%'",
    "range_1pct": "SELECT sum(v) FROM lake.t WHERE id BETWEEN {lo} AND {lo} + {rows} // 100",
    "point_lookup": "SELECT v FROM lake.t WHERE id = {lo}",
}


def tuning(args, run: str) -> dict:
    a = os.environ["PGVS3_TEST_ENDPOINT_A"]
    rows = args.tuning_rows
    results = []
    for rg in args.row_groups:
        name = f"{run}_rg{rg}"
        con = connect(a)
        attach(con, name)
        con.sql(f"CALL lake.set_option('parquet_row_group_size', {rg})")
        con.sql("CALL truncate_duckdb_logs()")
        load, _ = timed(
            con,
            "CREATE TABLE lake.t AS SELECT i AS id, (hash(i) % 1000)::INT AS k, "
            f"(hash(i + 1) % 1000000) / 100.0 AS v, md5(i::VARCHAR) AS s FROM range({rows}) t(i)",
        )
        put = requests(con, "PUT", "POST")
        files, stored = con.sql(
            "SELECT count(*), sum(data_file_size_bytes) FROM ducklake_list_files('lake', 't')"
        ).fetchall()[0]
        record = {"row_group_rows": rg, "rows": rows, "load_seconds": round(load, 3),
                  "files": files, "stored_mib": round(stored / MIB, 1), "put_requests": put["count"],
                  "queries": {}}
        for query, template in QUERIES.items():
            sql = template.format(lo=rows // 2, rows=rows)
            times, gets = [], None
            for _ in range(args.passes):
                # A new DuckDB per pass: Parquet metadata is not reused.
                q = connect(a)
                attach(q, name)
                q.sql("CALL truncate_duckdb_logs()")
                secs, _ = timed(q, sql)
                times.append(secs)
                gets = requests(q, "GET")
            record["queries"][query] = {"median_s": round(statistics.median(times), 3),
                                        "gets": gets["count"],
                                        "mib_read": round(gets.get("get_bytes", 0) / MIB, 1)}
        con.sql("DROP TABLE lake.t")
        results.append(record)
        print(json.dumps(record))
    return {"results": results}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parts", default="s3,compat,tuning")
    p.add_argument("--out", default=".tmp/pgvs3/bench.json")
    p.add_argument("--small-objects", type=int, default=2000)
    p.add_argument("--small-bytes", type=int, default=4096)
    p.add_argument("--large-objects", type=int, default=128)
    p.add_argument("--large-mib", type=int, default=8)
    p.add_argument("--tuning-rows", type=int, default=20_000_000)
    p.add_argument("--row-groups", type=lambda s: [int(x) for x in s.split(",")],
                   default=[30_720, 122_880, 491_520, 1_966_080])
    p.add_argument("--passes", type=int, default=3)
    args = p.parse_args()
    run = "r" + uuid.uuid4().hex[:12]
    parts = {"s3": s3, "compat": compat, "tuning": tuning}
    record = {"duckdb": duckdb.__version__, "cores": os.cpu_count(), "run": run}
    for part in args.parts.split(","):
        print(f"=== {part} ===", flush=True)
        record[part] = parts[part](args, run)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(record, f, indent=1)
    print(json.dumps({k: v for k, v in record.items() if k != "tuning"}, indent=1))
    print(f"wrote {args.out}")
    return 1 if record.get("compat", {}).get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())
