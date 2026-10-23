# Contributing

pgvs3 is a stateless Go gateway over PostgreSQL 18+.

## Scope

pgvs3 is for trusted networks. Authentication, authorization, TLS
termination, and presigned URLs are explicit non-goals, not unfinished work.
Do not add them without an explicit project decision; see
[docs/design.md](docs/design.md).

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
just integration # store/queue/cache integration tests against PostgreSQL 18 (testcontainers)
just s3t        # fast s3t allow-list (Go port of ceph/s3-tests) against a running gateway
just vet        # go vet
just contract   # end-to-end S3, boto3, DuckDB 2.0, DuckLake, duckdb-object-storage
just interop    # rclone, restic, boto3, DuckLake, and SlateDB
```

`just contract` is the required correctness gate before merging. It starts a
disposable PostgreSQL 18 container and two local gateways, then verifies
conditional writes, byte ranges, multipart uploads, zero-copy copies,
cross-gateway read-after-write consistency, garbage collection, DuckDB 2.0,
DuckLake, and `duckdb-object-storage`.

## Test surface

Keep one harness per layer. Do not add a second framework for the same job.

| Layer | Harness |
| --- | --- |
| Pure logic | Go unit tests |
| SQL, store, queues, cache | `just integration` (testcontainers) |
| S3 conformance | `just s3t` (pinned Go port of `ceph/s3-tests`) |
| Real clients and end-to-end flows | `just contract`, `just interop` |

S3 compatibility is **`s3t` only**. The Python `ceph/s3-tests`/tox/pytest path
was removed deliberately: it required a separate Python toolchain, duplicated
the Go integration layer, and was slow and Ceph-specific. Do not reintroduce
it or vendor a second conformance suite.

Grow coverage by adding node IDs to `test/s3t-allow.txt` when the pinned `s3t`
version already has the test, or by contributing the port upstream. The pinned
commit lives in `test/s3t.sh`; bump it deliberately and re-run the allow-list.
A new test harness must replace an existing layer, not add a parallel one.

Format code before committing:

```sh
mise exec go -- gofmt -w cmd internal test
```

## Schema migrations

Migrations live in `internal/store/migrations/` and are applied with
[goose](https://github.com/pressly/goose). `store.LayoutVersion` remains the
on-disk compatibility marker; goose owns incremental changes.

- `00001_baseline.sql` is the frozen v11 baseline. Do not edit it.
- Add `000NN_description.sql` with `-- +goose Up`; wrap PL/pgSQL blocks in
  `-- +goose StatementBegin` / `-- +goose StatementEnd`.
- `pgvs3 migrate` applies every pending migration, including the in-place
  v10 → v11 upgrade. `Init` applies them under the same advisory lock.
- Increment `store.LayoutVersion` only when an older binary cannot read the
  new schema; otherwise a goose migration is enough.
- For an incompatible layout jump, add an idempotent `s3p.migrate_vN()` and
  keep the previous catalog/tables readable rather than dropping them.
- Run `just test`, `just integration`, and `pgvs3 migrate` against a copy of
  the target database before merging.

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
| `internal/reqguard` | protocol-independent request policy checks (presigned URLs) |
| `internal/gateway` | protocol mux and HTTP server |
