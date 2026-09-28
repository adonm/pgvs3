"""End-to-end: DuckDB + the pgvfs extension against a real PostgreSQL.

    PGVFS_URL=postgres://... python e2e.py path/to/pgvfs.duckdb_extension

PGVFS_URL must name a database the S3 gateway never uses. The DuckLake
catalog lives in the same database (its own tables; no s3p schema).
Each run uses a fresh volume, so reruns never collide.
"""

import os
import sys
import time
from urllib.parse import urlsplit

import duckdb

ext = sys.argv[1]
url = os.environ["PGVFS_URL"]
vol = f"e2e-{os.getpid()}-{int(time.time())}"
root = f"pgvfs://{vol}"


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext}'")
    return con


def one(con, sql, *args):
    return con.execute(sql, args).fetchone()


con = connect()

# Parquet round trip, including a multi-row-group file larger than a read piece.
con.execute(
    f"COPY (SELECT i, i * 2 AS j, repeat('x', i % 50) AS s FROM range(2000000) t(i)) "
    f"TO '{root}/p/big.parquet' (ROW_GROUP_SIZE 100000)"
)
con.execute(f"COPY (SELECT 1 AS i) TO '{root}/p/small.parquet'")
assert one(con, f"SELECT count(*), sum(j) FROM '{root}/p/big.parquet'") == (
    2000000,
    2 * sum(range(2000000)),
)
assert one(con, f"SELECT count(*) FROM read_parquet('{root}/p/*.parquet')") == (2000001,)
assert one(con, f"SELECT count(*) FROM glob('{root}/**')") == (2,)
assert one(con, f"SELECT count(*) FROM glob('{root}/q/*')") == (0,)

# Overwrite replaces the file; a new connection must see the new bytes.
con.execute(f"COPY (SELECT 42 AS i) TO '{root}/p/small.parquet'")
assert one(connect(), f"SELECT i FROM '{root}/p/small.parquet'") == (42,)

# Missing files fail cleanly.
try:
    con.execute(f"SELECT * FROM '{root}/p/missing.parquet'")
    raise AssertionError("missing file read succeeded")
except duckdb.IOException:
    pass

# DuckLake with its data on pgvfs.
con.execute("INSTALL ducklake")
con.execute("INSTALL postgres")
u = urlsplit(url)
libpq = (
    f"host={u.hostname} port={u.port or 5432} user={u.username} "
    f"password={u.password} dbname={u.path.lstrip('/')}"
)
schema = "dl_" + vol.replace("-", "_")
con.execute(
    f"ATTACH 'ducklake:postgres:{libpq}' AS lake "
    f"(DATA_PATH '{root}/lake/', METADATA_SCHEMA '{schema}')"
)
con.execute("CREATE TABLE lake.t AS SELECT i, i % 7 AS k FROM range(100000) t(i)")
con.execute("INSERT INTO lake.t SELECT i, i % 7 FROM range(100000, 150000) t(i)")
assert one(con, "SELECT count(*), sum(k) FROM lake.t") == (
    150000,
    sum(i % 7 for i in range(150000)),
)
con.execute("DELETE FROM lake.t WHERE k = 0")
assert one(con, "SELECT count(*) FROM lake.t WHERE k = 0") == (0,)
files = one(con, f"SELECT count(*) FROM glob('{root}/lake/**')")[0]
assert files >= 3, files

# Dropped data is removed from pgvfs by DuckLake's cleanup.
con.execute("CREATE TABLE lake.gone AS SELECT range AS i FROM range(1000)")
assert one(con, f"SELECT count(*) FROM glob('{root}/lake/main/gone/*')")[0] == 1
con.execute("DROP TABLE lake.gone")
con.execute("CALL ducklake_expire_snapshots('lake', older_than => now())")
con.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
assert one(con, f"SELECT count(*) FROM glob('{root}/lake/main/gone/*')") == (0,)
after = one(con, f"SELECT count(*) FROM glob('{root}/lake/**')")[0]
assert one(con, "SELECT count(*) FROM lake.t") == (
    150000 - sum(1 for i in range(150000) if i % 7 == 0),
)

# A second process-level connection reads the lake through the cache path.
con2 = connect()
con2.execute(
    f"ATTACH 'ducklake:postgres:{libpq}' AS lake "
    f"(METADATA_SCHEMA '{schema}')"
)
assert one(con2, "SELECT count(*) FROM lake.t") == one(con, "SELECT count(*) FROM lake.t")

print(f"pgvfs e2e ok: volume {vol}, lake files {files} -> {after}")
