"""DuckLake on pgvs3 in kind, plus a timeout-aware query runner."""

import json
import os
import threading
import time

import duckdb

def connect(args, extensions: tuple = ()) -> duckdb.DuckDBPyConnection:
    """Open a scratch connection and attach the kind DuckLake catalog."""
    db = args.scratch_db
    os.makedirs(os.path.dirname(db) or ".", exist_ok=True)
    os.makedirs(".tmp/pgvs3", exist_ok=True)
    try:
        con = duckdb.connect(db)
    except duckdb.IOException:
        # An alpha DuckDB scratch DB can be incompatible with a new wheel.
        os.remove(db)
        con = duckdb.connect(db)
    con.sql("SET temp_directory='.tmp/pgvs3/duckdb-temp'")
    con.sql("SET http_timeout=300")
    if args.memory_limit:
        con.sql(f"SET memory_limit='{args.memory_limit}'")
    for ext in ("postgres", "httpfs", "ducklake", *extensions):
        con.sql(f"INSTALL {ext}")
        con.sql(f"LOAD {ext}")
    con.sql(f"SET s3_endpoint='{os.environ.get('PGVS3_ENDPOINT', 'pgvs3:8014')}'")
    con.sql("SET s3_use_ssl=false")
    con.sql("SET s3_url_style='path'")
    for setting, key in (("s3_access_key_id", "AWS_ACCESS_KEY_ID"),
                         ("s3_secret_access_key", "AWS_SECRET_ACCESS_KEY")):
        value = os.environ[key].replace("'", "''")
        con.sql(f"SET {setting}='{value}'")
    con.sql(f"ATTACH 'ducklake:postgres:{args.catalog}' AS lake (DATA_PATH '{args.data_path}')")
    return con


def run_sql(con, sql: str, timeout: float | None = None) -> tuple[float, str | None, list]:
    """Run one query to completion: (elapsed seconds, error or None, rows)."""
    timer = None
    if timeout:
        timer = threading.Timer(timeout, con.interrupt)
        timer.daemon = True
        timer.start()
    t0 = time.perf_counter()
    err = None
    rows = []
    try:
        rows = con.execute(sql).fetchall()
    except Exception as e:
        name = type(e).__name__
        err = f"timeout>{timeout}s" if "Interrupt" in name else f"{name}: {e}"[:300]
    finally:
        if timer:
            timer.cancel()
    return round(time.perf_counter() - t0, 3), err, rows


def write_record(record: dict, out: str) -> None:
    with open(out, "w") as f:
        json.dump(record, f, indent=1)
    print(f"wrote {out}")
