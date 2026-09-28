"""DuckLake on pgvs3 (S3) or pgvfs in kind, plus a timeout-aware query runner."""

import json
import os
import threading
import time

import duckdb

PGVFS_EXT = os.environ.get("PGVFS_EXT", "/bench/pgvfs.duckdb_extension")


def connect(args, extensions: tuple = (), stack: str = "s3",
            fresh: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a connection and attach the stack's DuckLake catalog.

    stack "s3" reads data through the pgvs3 gateway (httpfs); "pgvfs" reads
    it straight from PostgreSQL through the pgvfs extension. fresh=True opens
    a new in-memory DuckDB instance: empty caches, nothing shared.
    """
    config = {"allow_unsigned_extensions": "true"} if stack == "pgvfs" else {}
    if fresh:
        con = duckdb.connect(":memory:", config=config)
    else:
        db = f"{args.scratch_db}.{stack}"
        os.makedirs(os.path.dirname(db) or ".", exist_ok=True)
        try:
            con = duckdb.connect(db, config=config)
        except duckdb.IOException:
            # An alpha DuckDB scratch DB can be incompatible with a new wheel.
            os.remove(db)
            con = duckdb.connect(db, config=config)
    os.makedirs(".tmp/pgvs3", exist_ok=True)
    con.sql("SET temp_directory='.tmp/pgvs3/duckdb-temp'")
    if args.memory_limit:
        con.sql(f"SET memory_limit='{args.memory_limit}'")
    for ext in ("postgres", "ducklake", *extensions):
        con.sql(f"INSTALL {ext}")
        con.sql(f"LOAD {ext}")
    if stack == "pgvfs":
        con.sql(f"LOAD '{PGVFS_EXT}'")
        # One postgres secret serves DuckLake's catalog and pgvfs's data,
        # both in the pgvfs database.
        q = lambda v: v.replace("'", "''")  # noqa: E731
        con.sql(f"CREATE SECRET (TYPE postgres, HOST '{q(os.environ['PG_HOST'])}', "
                f"USER '{q(os.environ['PG_USER'])}', PASSWORD '{q(os.environ['PG_PASSWORD'])}', "
                f"DATABASE '{q(args.pgvfs_database)}')")
        catalog, data_path = "", args.pgvfs_data_path
    else:
        con.sql("INSTALL httpfs")
        con.sql("LOAD httpfs")
        con.sql("SET http_timeout=300")
        con.sql(f"SET s3_endpoint='{os.environ.get('PGVS3_ENDPOINT', 'pgvs3:8014')}'")
        con.sql("SET s3_use_ssl=false")
        con.sql("SET s3_url_style='path'")
        for setting, key in (("s3_access_key_id", "AWS_ACCESS_KEY_ID"),
                             ("s3_secret_access_key", "AWS_SECRET_ACCESS_KEY")):
            value = os.environ[key].replace("'", "''")
            con.sql(f"SET {setting}='{value}'")
        catalog, data_path = args.catalog, args.data_path
    options = [f"DATA_PATH '{data_path}'"]
    if args.metadata_schema:
        options.append(f"METADATA_SCHEMA '{args.metadata_schema}'")
    con.sql(f"ATTACH 'ducklake:postgres:{catalog}' AS lake ({', '.join(options)})")
    return con


def warm_storage(con, args, stack: str) -> None:
    """Open the stack's storage connection (HTTP or PostgreSQL pool) without
    reading data, so a fresh-instance timing excludes connection setup."""
    root = args.pgvfs_data_path if stack == "pgvfs" else args.data_path
    con.sql(f"SELECT count(*) FROM glob('{root.rstrip('/')}/.warm/*')").fetchall()


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
