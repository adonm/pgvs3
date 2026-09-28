#!/usr/bin/env python3
"""Upstream ClickBench and SpatialBench inputs on DuckLake over pgvs3 in kind.

Suites:
  click    ClickBench `hits.parquet` + 43 queries (queries/clickbench.sql)
  spatial  apache-sedona/spatialbench parquet + 12 queries (queries/spatialbench.sql)

This is the DuckLake-side runner: it loads upstream data into DuckLake on
pgvs3 and times the pinned queries.
"""

import argparse
import datetime
import decimal
import json
import os
import subprocess
import sys
import urllib.request

import duckdb

import benchlib

CLICK_TABLES = ["hits"]
SPATIAL_TABLES = ["trip", "customer", "driver", "vehicle", "zone", "building"]
FILE = {"click": "clickbench", "spatial": "spatialbench"}
CLICK_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
CLICK_BYTES = 14_779_976_446
HF_TREE = "https://huggingface.co/api/datasets/apache-sedona/spatialbench/tree/main"
HF_DL = "https://huggingface.co/datasets/apache-sedona/spatialbench/resolve/main"


def load_queries(bench: str) -> list[str]:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries", f"{FILE[bench]}.sql")
    text = open(path).read()
    if bench == "click":  # one query per non-empty, non-comment line
        return [ln.strip() for ln in text.splitlines()
                if ln.strip() and not ln.strip().startswith("--")]
    queries, cur = [], None  # spatialbench: -- @qN markers
    for ln in text.splitlines():
        if ln.strip().startswith("-- @"):
            queries.append([])
            cur = queries[-1]
        elif cur is not None:
            cur.append(ln)
    return ["\n".join(q).strip() for q in queries]


def download_click(src: str, parts: int) -> None:
    if parts:  # fast loop: 1% slices of the canonical data (typed like it too)
        os.makedirs(src, exist_ok=True)
        for i in range(parts):
            dest = os.path.join(src, f"hits_{i}.parquet")
            if os.path.exists(dest) and os.path.getsize(dest) > 0:
                continue
            print(f"fetch hits_{i}.parquet")
            subprocess.run(["curl", "-fsL", "-o", dest,
                            f"{CLICK_URL.rsplit('/', 1)[0]}/athena_partitioned/hits_{i}.parquet"],
                           check=True)
        return
    dest = os.path.join(src, "hits.parquet")
    if os.path.exists(dest) and os.path.getsize(dest) == CLICK_BYTES:
        return
    os.makedirs(src, exist_ok=True)
    print(f"fetch {CLICK_URL} (13.8 GiB)")
    subprocess.run(["curl", "-fsSL", "-C", "-", "-o", dest, CLICK_URL], check=True)
    assert os.path.getsize(dest) == CLICK_BYTES, "truncated hits.parquet"


def download_spatial(src: str, sf: float) -> None:
    tag = f"v0.1.0/sf{sf:g}"
    listing = json.loads(urllib.request.urlopen(f"{HF_TREE}/{tag}?recursive=true").read())
    for f in listing:
        if f.get("type") != "file":
            continue
        rel = f["path"]  # v0.1.0/sf10/trip/trip.1.parquet
        dest = os.path.join(src, rel[len("v0.1.0/"):])
        if os.path.exists(dest) and os.path.getsize(dest) == f.get("size", -1):
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        print(f"fetch {rel} ({f.get('size', 0) / 2**20:.0f} MiB)")
        subprocess.run(["curl", "-fsL", "--retry", "3", "-o", dest, f"{HF_DL}/{rel}"], check=True)


def src_glob(bench: str, src: str, sf: float, table: str, parts: int) -> str:
    if bench == "click":
        # exact file for the full run; numbered slices for --parts (kept
        # disjoint so both can share a source dir)
        return os.path.join(src, "hits_[0-9]*.parquet" if parts else "hits.parquet")
    return os.path.join(src, f"sf{sf:g}", table, "*.parquet")


# ClickBench's duckdb/load normalization (verbatim from ClickHouse/ClickBench
# duckdb/load): hits.parquet stores packed ints and binary strings; the 43
# queries expect the typed schema.
CLICK_SELECT = """* REPLACE (
    make_date(EventDate) AS EventDate,
    epoch_ms(EventTime * 1000) AS EventTime,
    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)"""


def load(con, bench: str, src: str, sf: float, parts: int) -> tuple[float, dict]:
    tables = CLICK_TABLES if bench == "click" else SPATIAL_TABLES
    t0 = __import__("time").perf_counter()
    rows = {}
    for t in tables:
        dest = f"lake.{t}"
        sel = CLICK_SELECT if bench == "click" else "*"
        rd = (f"read_parquet('{src_glob(bench, src, sf, t, parts)}', binary_as_string=True)"
              if bench == "click" else f"read_parquet('{src_glob(bench, src, sf, t, parts)}')")
        con.sql(f"DROP TABLE IF EXISTS {dest}")
        con.sql(f"CREATE TABLE {dest} AS SELECT {sel} FROM {rd}")
        rows[t] = con.sql(f"SELECT count(*) FROM {dest}").fetchone()[0]
    con.sql("CALL ducklake_flush_inlined_data('lake')")  # force data into files on the data path
    for t in tables:
        con.sql(f"CREATE OR REPLACE VIEW main.{t} AS SELECT * FROM lake.{t}")
    return __import__("time").perf_counter() - t0, rows


def query_numbers(spec: str, count: int) -> list[int]:
    numbers = []
    for item in spec.split(","):
        bounds = item.split("-")
        if len(bounds) not in (1, 2) or not all(bound.isdigit() for bound in bounds):
            raise ValueError(f"invalid query selection: {spec}")
        lo, hi = int(bounds[0]), int(bounds[-1])
        if not 1 <= lo <= hi <= count:
            raise ValueError(f"query selection outside 1-{count}: {item}")
        numbers.extend(range(lo, hi + 1))
    if len(set(numbers)) != len(numbers):
        raise ValueError(f"duplicate queries in selection: {spec}")
    return numbers


def run_queries(con, queries: list[str], numbers: list[int], passes: int,
                timeout: float) -> tuple[list[dict], dict]:
    # ClickBench runs each query three times consecutively; runs 2 and 3 are
    # hot. These are not true cold runs (neither engine drops the OS page cache).
    records = [{"times": {}, "errors": {}} for _ in range(passes)]
    answers = {}
    for n in numbers:
        for p, record in enumerate(records):
            secs, err, rows = benchlib.run_sql(con, queries[n - 1], timeout)
            record["times"][f"Q{n}"] = secs
            if err:
                record["errors"][f"Q{n}"] = err
            elif p == 0:
                answers[f"Q{n}"] = [[(value.hex() if isinstance(value, bytes) else str(value)
                                       if isinstance(value, (datetime.date, datetime.timedelta, decimal.Decimal))
                                       else value) for value in row] for row in rows]
    return records, answers


def run_fresh(args, stack: str, extensions: tuple, queries: list[str], numbers: list[int],
              passes: int, timeout: float | None) -> list[dict]:
    """Each query on a brand-new DuckDB instance: no DuckDB cache, storage
    connection already open. The per-query latency storage actually sets."""
    tables = CLICK_TABLES if args.bench == "click" else SPATIAL_TABLES
    records = [{"times": {}, "errors": {}} for _ in range(passes)]
    for n in numbers:
        for record in records:
            con = benchlib.connect(args, extensions, stack, fresh=True)
            for t in tables:
                con.sql(f"CREATE VIEW main.{t} AS SELECT * FROM lake.{t}")
            benchlib.warm_storage(con, args, stack)
            secs, err, _ = benchlib.run_sql(con, queries[n - 1], timeout)
            con.close()
            record["times"][f"Q{n}"] = secs
            if err:
                record["errors"][f"Q{n}"] = err
    return records


def report(label: str, records: list[dict]) -> None:
    for p, rec in enumerate(records):
        times, errors = rec["times"], rec["errors"]
        print(f"[{label}] pass {p + 1}: total {sum(times.values()):.1f}s")
        print("  " + "  ".join(f"{k}={v:.2f}" for k, v in times.items()))
        for k, v in errors.items():
            print(f"  {k}: {v}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", choices=["click", "spatial"], required=True)
    ap.add_argument("--sf", type=float, default=10.0, help="spatialbench scale factor (0.1/1/10/100)")
    ap.add_argument("--download", action="store_true", help="fetch the source parquet first")
    ap.add_argument("--load", action="store_true", help="load source parquet before querying")
    ap.add_argument("--views-only", action="store_true", help="skip load; just make main views over lake")
    ap.add_argument("--passes", type=int, default=3)
    ap.add_argument("--fresh-passes", type=int, default=0,
                    help="extra passes with a new DuckDB instance per query")
    ap.add_argument("--parts", type=int, default=0,
                    help="clickbench fast loop: load N of the 100 1%% slices (0 = full hits.parquet)")
    ap.add_argument("--queries", default=None, help="query numbers/ranges like 1-3,6 (default: all)")
    ap.add_argument("--query-timeout", type=float, default=0, help="per-query seconds (0 = unlimited)")
    ap.add_argument("--memory-limit", default=None, help="DuckDB memory_limit (default: DuckDB's 80%% of RAM)")
    ap.add_argument("--src-dir", default=None)
    ap.add_argument("--scratch-db", default=".tmp/pgvs3/scratch.duckdb")
    ap.add_argument("--stacks", default="s3", help="comma list of s3 (pgvs3 gateway) and pgvfs")
    ap.add_argument("--data-path", default=None, help="s3 stack DuckLake DATA_PATH")
    ap.add_argument("--catalog", required=True, help="s3 stack DuckLake catalog DSN")
    ap.add_argument("--pgvfs-data-path", default="pgvfs://lake/", help="pgvfs stack DATA_PATH")
    ap.add_argument("--pgvfs-catalog", default=None, help="pgvfs stack DuckLake catalog DSN")
    ap.add_argument("--metadata-schema", default=None, help="DuckLake METADATA_SCHEMA (both stacks)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    bench = args.bench
    name = FILE[bench]
    for attr, default in [
        ("src_dir", f".tmp/pgvs3/data/{name}"),
        ("data_path", f"s3://lake/{name}/"),
    ]:
        if getattr(args, attr) is None:
            setattr(args, attr, default)
    stacks = args.stacks.split(",")
    if not set(stacks) <= {"s3", "pgvfs"} or len(set(stacks)) != len(stacks):
        raise SystemExit(f"invalid --stacks: {args.stacks}")
    if "pgvfs" in stacks and not args.pgvfs_catalog:
        raise SystemExit("--pgvfs-catalog is required for the pgvfs stack")

    queries = load_queries(bench)
    numbers = query_numbers(args.queries or f"1-{len(queries)}", len(queries))
    timeout = args.query_timeout or None
    extensions = ("spatial",) if bench == "spatial" else ()

    if args.download:
        (download_click(args.src_dir, args.parts) if bench == "click"
         else download_spatial(args.src_dir, args.sf))

    failed = False
    checksums = {}
    for stack in stacks:
        label = f"lake-{stack}"
        con = benchlib.connect(args, extensions, stack)
        record = {"bench": bench, "stack": label, "comparable": False,
                  "duckdb": duckdb.__version__, "queries": numbers, "passes": [], "answers": {}}
        if bench == "spatial":
            record["sf"] = args.sf
        if args.views_only:
            for t in (CLICK_TABLES if bench == "click" else SPATIAL_TABLES):
                con.sql(f"CREATE OR REPLACE VIEW main.{t} AS SELECT * FROM lake.{t}")
            record["rows"] = {t: con.sql(f"SELECT count(*) FROM lake.{t}").fetchone()[0]
                              for t in (CLICK_TABLES if bench == "click" else SPATIAL_TABLES)}
            record["reused"] = True
        elif args.load:
            record["load_s"], record["rows"] = load(con, bench, args.src_dir, args.sf, args.parts)
            print(f"[{label}] load: {record['load_s']:.1f}s rows={record['rows']}")

        # Every row, order-independent: the stacks must hold identical data.
        tables = CLICK_TABLES if bench == "click" else SPATIAL_TABLES
        record["checksum"] = {t: str(con.sql(f"SELECT bit_xor(hash(r)) FROM lake.{t} r").fetchone()[0])
                              for t in tables}
        record["passes"], record["answers"] = run_queries(
            con, queries, numbers, args.passes, timeout)
        con.close()
        report(label, record["passes"])
        record["fresh"] = run_fresh(args, stack, extensions, queries, numbers,
                                    args.fresh_passes, timeout)
        report(f"{label} fresh", record["fresh"])

        out = args.out or f".tmp/pgvs3/{name}-{label}-sf{args.sf:g}.json"
        if len(stacks) > 1:
            out = out.replace(".json", f"-{stack}.json")
        benchlib.write_record(record, out)
        # One compact line for the results JSONL (the pretty record goes to --out).
        print(json.dumps({"suite": bench, "stack": label, "comparable": False, "queries": numbers,
                          "sf": record.get("sf"),
                          "load_s": record.get("load_s"), "rows": record.get("rows"),
                          "reused": record.get("reused", False),
                          "checksum": record["checksum"],
                          "duckdb": duckdb.__version__, "passes": record["passes"],
                          "pass_s": [round(sum(p["times"].values()), 2) for p in record["passes"]],
                          "fresh": record["fresh"],
                          "fresh_s": [round(sum(p["times"].values()), 2) for p in record["fresh"]],
                          "answers": record["answers"]}, default=str))
        failed |= any(p["errors"] for p in record["passes"] + record["fresh"])
        checksums[stack] = record["checksum"]
    if len({json.dumps(c, sort_keys=True) for c in checksums.values()}) > 1:
        raise SystemExit(f"{bench}: stacks hold different data: {checksums}")
    if failed:
        raise SystemExit(f"{bench}: queries failed (see results above)")


if __name__ == "__main__":
    main()
