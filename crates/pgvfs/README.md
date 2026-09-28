# pgvfs: a `pgvfs://` DuckDB filesystem on PostgreSQL

DuckDB reads and writes files stored as PostgreSQL rows directly, with no S3
gateway or HTTP in between. It is built for DuckLake data files:

```sql
LOAD 'pgvfs.duckdb_extension';            -- allow_unsigned_extensions
SET pgvfs_url = 'postgres://user:pass@host/lakefs';   -- or $PGVFS_URL
ATTACH 'ducklake:postgres:dbname=lakefs host=...' AS lake (DATA_PATH 'pgvfs://lake/');
```

Paths are `pgvfs://<volume>/<path>`. A volume is a namespace inside one
database (`[a-z0-9][a-z0-9._-]{0,62}`), so a single database can hold several lakes.

## One mode per database

pgvfs and the pgvs3 S3 gateway never share a database. Each one's `init`
refuses the other's schema (`pgvfs` vs `s3p`). Both can run on the same
PostgreSQL server in separate databases.

## Why it is faster than the S3 path

Files are immutable. A write streams rows under a fresh `file_id` and
publishes `(volume, path) → file_id` when the file is closed. That means:

- **Reads** are one primary-key range query per 8 MiB piece, with no
  overwrite guard, snapshot or metadata cache. The pieces of a large read are
  fetched in parallel on separate pooled connections, and rows are copied
  straight into DuckDB's buffer.
- **Caching:** `file_id` is DuckDB's cache version tag. With DuckLake (which
  skips cache validation), warm queries don't reach PostgreSQL at all.
- **Writes** are a single binary `COPY` with no MD5/SHA-256, ETags or multipart
  bookkeeping. Integrity comes from Parquet's structure and PostgreSQL page
  checksums.
- **Deletes** queue the old `file_id` for reaping. Rows outlive the delete by
  a grace period (10 min), so queries that already opened the file finish
  reading it. Reaping runs in the background from the extension, at most once
  a minute; `SELECT pgvfs.maintain()` can also be scheduled externally.

The chunk layout is the gateway's: 8120-byte inline rows, one per 8 KB page,
32 hash partitions (`schema.sql`).

## Layout

- `src/store.rs`: the storage layer (pool, reads, `COPY` writer, list, remove, rename).
- `src/lib.rs`: its C ABI (`extension/src/include/pgvfs.h`).
- `extension/`: the thin C++ DuckDB `FileSystem` adapter. The adapter has
  to be C++ because DuckDB's stable C API can use filesystems but cannot
  register one.

## Build and test

```sh
just pgvfs-ext                  # container build -> target/pgvfs/pgvfs.duckdb_extension
just contract                   # includes tests/store_contract.rs
PGVFS_URL=postgres://... python crates/pgvfs/extension/test/e2e.py target/pgvfs/pgvfs.duckdb_extension
```

The extension is statically linked against `duckdb_static` of the exact
DuckDB release that loads it (v1.5.6). Like DuckDB's own extensions, it
needs that because Python loads DuckDB with `RTLD_LOCAL`. The link uses the
release's prebuilt `static-libs-linux-amd64.zip` plus the source tarball for
headers, both pinned by SHA-256 in `extension/Containerfile`, so DuckDB is
never compiled (`extension/build.sh`, about 5 s). To change DuckDB versions,
update the version and both digests together.

Pool sizing: `PGVFS_POOL_MIN` (default 4) and `PGVFS_POOL_MAX` (default 32).
TLS follows the gateway: remote servers require `sslmode=require`, and
`PGVS3_DB_ALLOW_PLAINTEXT=true` allows plaintext on an isolated rig.
