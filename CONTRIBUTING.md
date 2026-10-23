# Contributing

pgvs3 is a stateless Go gateway over PostgreSQL 18+.

## Toolchain

```sh
mise install   # Go, just, ko, uv, Python, Helm, Rust, rclone, restic
just setup     # download Go modules
```

Tool versions are pinned in `mise.toml`. Container images are built directly
from Go source with `ko` (no `Dockerfile`, `CGO_ENABLED=0`).

## Verification

```sh
just test       # unit tests
just race       # unit tests with race detector
just vet        # go vet
just contract   # end-to-end S3, boto3, DuckDB 2.0, DuckLake, duckdb-object-storage
just interop    # rclone, restic, boto3, DuckLake, and SlateDB
```

`just contract` is the required correctness gate before merging. It starts a
disposable PostgreSQL 18 container and two local gateways, then verifies
conditional writes, byte ranges, multipart uploads, zero-copy copies,
cross-gateway read-after-write consistency, garbage collection, DuckDB 2.0,
DuckLake, and `duckdb-object-storage`.

Format code before committing:

```sh
mise exec go -- gofmt -w cmd internal test
```

## Storage layout changes

`internal/store/schema.sql` defines layout `v11` (`store.LayoutVersion = 11`).
Namespaces are PostgreSQL schemas typed by prefix: `s3b_` bucket, `s3c_` cache
(UNLOGGED) and `s3q_` queue. Objects up to the kind's inline ceiling live in
`objects.inline_data`; larger objects use 8,120-byte rows in `chunks`.

Any incompatible schema change must:

1. Increment `store.LayoutVersion` in `internal/store/store.go`.
2. Update `internal/store/schema.sql` and document the page/TOAST math.
3. Fail fast on startup when connected to an older layout version.
4. Add an idempotent `s3p.migrate_vN()` function and wire it into `pgvs3
   migrate`; keep the previous catalog/tables readable rather than dropping
   them. Migrations run only when the operator asks for them.

## Performance invariants

- **8,120-byte chunk payload:** keep chunk rows at 8,120 bytes with
  `toast_tuple_target = 8160` and `STORAGE EXTERNAL` so each row fits one 8 KB
  page without TOAST.
- **Zero gateway state:** do not add in-process object or metadata caches.
  PostgreSQL `shared_buffers` on `s3p.objects` is the single MVCC-consistent
  cache across gateways, and DuckDB caches remote file bytes client-side.
- **Split pools:** keep `enable_indexscan = on` on the main pool for B-tree
  point lookups and listings, and `enable_indexscan = off` on `chunkPool` for
  PostgreSQL 18 bitmap heap scan read-ahead on `s3p.chunks`.
- **Cut-through streaming:** stream rows from PostgreSQL to HTTP without
  buffering whole objects in memory.
- **Bounded maintenance:** keep background reaping bounded per pass and
  serialized across gateways with `pg_try_advisory_lock`.

## Commits

- Keep one logical change per commit and state the user-visible effect.
- Update `README.md` and `docs/` when flags, S3 behavior, or layout rules
  change.
- Never commit `.tmp/`, database dumps, or credentials.

## License

Apache-2.0. See [LICENSE](LICENSE).

## Package map

| Package | Responsibility |
| --- | --- |
| `internal/ns` | namespace kinds, name validation, schema naming |
| `internal/store` | PostgreSQL layout, objects, blocks, cache, queues, GC |
| `internal/s3` | S3 + plain HTTP handlers, cache-tier proxy |
| `internal/sqs` | SQS JSON and Query protocols |
| `internal/resp` | RESP2 server |
| `internal/objcache` | thanos-objstore read-through tier |
| `internal/gateway` | protocol mux and HTTP server |
