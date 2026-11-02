# pgvs3 — PostgreSQL as the object data plane

**S3/HTTP object storage whose bytes live in PostgreSQL.** Stateless gateways
store 10 MiB–1 GiB Parquet objects for DuckDB and [DuckLake](https://ducklake.select/)
in PostgreSQL tables, and use its transactions, MVCC snapshots, WAL, replication
and locks instead of separate durability, cache-coherence and coordination services.

> **Trusted networks only — permanently.** pgvs3 does not authenticate clients,
> terminate TLS or validate S3 signatures, and never will. Run it on loopback, a
> private network or behind a mesh that enforces access control, and treat `:8014`
> like a database port. See [docs/design.md](docs/design.md#trust-model-permanent).

**Status: pre-alpha.** PostgreSQL 18+ (Aurora 18.3+). Storage layout **v25** has no
in-place upgrade: a layout change needs a fresh database and a re-import.

## What it is for

| Scope | Use | Where the bytes live |
| --- | --- | --- |
| **Core** | PostgreSQL object store | Logged bucket tables (`s3b_`); no origin needed. |
| **Core** | Shared cache and durable write-back stage in front of S3/HTTP | Committed objects at the origin; read fills and **acknowledged-but-not-yet-uploaded bodies** in PostgreSQL (`s3c_`/`s3w_`). Pending bodies may be the only copy. |
| Companion | RESP2 cache subset, OJS jobs and streams | `s3c_`/`s3q_`; bounded, for co-located services. |
| Experimental | Differential and SlateDB/ObjFS client comparisons | Benchmark harnesses only. |

The trade-off: object writes and cache fills consume the cluster's disk, WAL, I/O
and replication bandwidth. Fewer services is not free storage. See
[interfaces and scope](docs/interfaces.md) for the full protocol surface.

## Quick start

Requires [mise](https://mise.jdx.dev/) and Docker (or an existing PostgreSQL 18).

```sh
just setup
just dev-db   # PostgreSQL 18 on 127.0.0.1:5432 with pgvs3_bench and ducklake_catalog
export PGVS3_URL='postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable'
mise exec go -- go run ./cmd/pgvs3 serve --addr 127.0.0.1:8014
```

In another terminal, check the database and create a bucket. A write to a missing
bucket returns `NoSuchBucket` rather than creating it:

```sh
mise exec go -- go run ./cmd/pgvs3 doctor --gateways 1 --surge 0   # PASS/WARN/FAIL with fixes
curl -fsS -X PUT http://127.0.0.1:8014/lake
curl -fsS -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl -fsS http://127.0.0.1:8014/lake/README.md
```

### Use from DuckDB 2.0+

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3, KEY_ID 'test', SECRET 'test',
    REGION 'us-east-1', ENDPOINT '127.0.0.1:8014',
    URL_STYLE 'path', USE_SSL false
);
COPY (
    SELECT range AS id, (range % 100)::INTEGER AS group_id,
           md5(range::VARCHAR) AS payload
    FROM range(1000000)
) TO 's3://lake/events.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT group_id, count(*) AS rows
FROM read_parquet('s3://lake/events.parquet')
GROUP BY group_id ORDER BY group_id LIMIT 5;
```

Each group has 10,000 rows. The dummy credentials satisfy the client; they do not
enable authentication.

### Use with DuckLake (catalog and objects on one cluster)

In the same DuckDB session:

```sql
INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
               DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 's3://lake/ducklake/');
CALL lake.set_option('data_inlining_row_limit', 0);
CREATE TABLE lake.events (site_id INTEGER, day DATE, value DOUBLE);
INSERT INTO lake.events VALUES (42, DATE '2026-09-01', 1.0);
SELECT * FROM lake.events;
```

The DuckLake catalog and the pgvs3 objects share a cluster but commit in
**separate transactions**; co-location does not make them atomic together.
`just analytics` runs both snippets in both storage roles in a disposable stack.

## How it works

![FIG 01 — System Topology & Parallel Authority Lanes](docs/assets/architecture-flow.svg)

- **Bytes in pages.** Objects up to 64 KiB are inline; larger ones are 8,120-byte
  rows, one per 8 KiB PostgreSQL page with no TOAST, Zstd-compressed only when that
  shrinks them. Range offsets are `no × 8,120`.
- **Immutable files, atomic publish.** Binary `COPY` writes a new `file_id`; one
  transaction swaps the object pointer. Readers pin the files they stream (or use
  MVCC snapshots on standbys), so no gateway invalidation is needed.
- **Shared coordination.** Fills, the write-back outbox and maintenance coordinate
  through leases, fences and advisory locks in PostgreSQL. Gateway memory and
  scratch files are never the copy of record.
- **Measured against the hardware.** Writes run at the raw PostgreSQL `COPY`
  floor. Whole-object GETs reach 83–89% of the loopback HTTP floor and 8 MiB
  ranges 53–58%; an opt-in gateway cache of immutable blocks serves hot reads
  several times faster. See [speed of light](docs/performance.md#speed-of-light).

## Operate it

- `pgvs3 doctor --gateways N --pool-max P` checks PostgreSQL version, layout,
  durability settings, `max_wal_size`, fleet connection capacity and pending
  uploads, and prints the fix for each failure.
- Every command, flag, environment variable and metric is in the generated
  [reference](docs/reference.md). `pgvs3 <command> -h` prints the same flags.
- Deployment, failover, backups and the cache tier: [operations.md](docs/operations.md).
- Images and charts: `ghcr.io/adonm/pgvs3` and `oci://ghcr.io/adonm/charts/pgvs3`
  (`just image`, `just chart`). Pin versions that match your layout.

## Develop and verify

```sh
just test       # Go unit tests
just contract   # end-to-end S3, OJS, DuckDB, DuckLake, guides and metric reference
just sol        # pgvs3 vs loopback HTTP and direct PostgreSQL floors
just ci         # every local/CI gate
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full gate list and invariants.

## Documentation

- [Site](https://adonm.github.io/pgvs3/) ([source](docs/index.md)): overview, figures and guides.
- [Workload guides](docs/guides/index.md): OLTP protection, analytics, cache, jobs, recovery.
- [Design](docs/design.md): trust model, envelope, limits and safety invariants.
- [Interfaces and scope](docs/interfaces.md): S3, origin cache, RESP2, OJS, experiments.
- [Performance](docs/performance.md) and [benchmarks](docs/benchmarks.md): layout, I/O paths, measured results.
- [Roadmap](docs/roadmap.md): ranked work and acceptance criteria.

## License

Apache-2.0. See [LICENSE](LICENSE).
