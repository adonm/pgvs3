#!/usr/bin/env python3
"""DuckLake reachability check: attach the catalog (PostgreSQL) with the
pgvs3 data path and read one table. Exits 0 on success — the `validate`
suite uses this so a benchmark failure is never "the cluster wasn't ready".
"""
import os

import duckdb

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_USER = os.environ.get("PG_USER", "postgres")
# PGPASSWORD/PGSSLMODE are set by the Job from its database Secret on EC2.
# The catalog remembers its data path; every suite shares one root.
LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3://lake/v/")

c = duckdb.connect()
for ext in ("postgres", "httpfs", "ducklake"):
    c.sql(f"INSTALL {ext}")
    c.sql(f"LOAD {ext}")
c.sql("SET s3_endpoint='pgvs3:8014'")
c.sql("SET s3_use_ssl=false")
c.sql("SET s3_url_style='path'")
for setting, key in (("s3_access_key_id", "AWS_ACCESS_KEY_ID"),
                     ("s3_secret_access_key", "AWS_SECRET_ACCESS_KEY")):
    c.sql(f"SET {setting}='{os.environ[key].replace(chr(39), chr(39) * 2)}'")
c.sql(
    f"ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={PG_HOST} "
    f"user={PG_USER} sslmode={os.environ.get('PGSSLMODE', 'prefer')}' AS lake "
    f"(DATA_PATH '{LAKE_ROOT}')"
)
# The catalog is empty until the suites load it; attaching without error is
# the reachability proof. `ducklake_flush_inlined_data` is a cheap round trip
# that exercises catalog + storage together.
c.sql("CALL ducklake_flush_inlined_data('lake')")
print("ducklake ok")
