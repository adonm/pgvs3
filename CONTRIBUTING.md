# Contributing

pgvs3 is a persistent, shared read-through cache in front of S3, backed by
PostgreSQL 18 and run inside a trusted cluster network.

## Scope

The product is one path: cache S3 reads in PostgreSQL so every gateway replica
shares one warm cache, and proxy writes and listings to origin. Keep it small.

- Authentication, authorization, gateway TLS and presigned URLs are permanent
  non-goals.
- No write-through, outbox, native buckets, extra protocols or in-process byte
  caches. Large-object parallelism belongs to clients (concurrent range GETs).
- A change should name the cache problem it solves, keep state in PostgreSQL,
  and show evidence (tests, and before/after numbers for hot paths).

## Toolchain

```sh
mise install   # Go, just, ko, hk, Go linters, uv, Python, Helm
just setup     # download Go modules
just hooks     # install hk Git hooks for this clone
```

Images are built from Go source with `ko` (no `Dockerfile`, `CGO_ENABLED=0`).

## Verification

```sh
just lint         # hk: go fix, goimports, gofmt, go mod tidy, vet, staticcheck (CI adds govulncheck)
just fix          # apply the same safe fixes
just test         # unit tests
just race         # unit tests with the race detector (needs cgo)
just integration  # PostgreSQL 18 + in-process fake S3 + versitygw, two gateways, boto3/DuckDB
just fuzz         # range and aws-chunked parsers
just ci           # everything CI runs
```

`just integration` uses `PGVS3_TEST_DATABASE_URL` when set (CI service) and
otherwise starts a disposable PostgreSQL container. Test layers:

| Layer | Harness |
| --- | --- |
| Pure logic | Go unit tests |
| Store, cache and S3 behaviour against PostgreSQL | `-tags=integration` Go tests with the in-process fake origin |
| Real S3 wire behaviour and clients | versitygw + boto3/DuckDB smoke in `test/integration.sh` |

## Style

Plain functions and data, explicit control flow, stdlib first. `pgx` is the
only dependency; justify any new one in the commit message. Comments only for
non-obvious invariants.

## Schema changes

`internal/store/schema.sql` is the only schema. Bump `store.LayoutVersion` for
any change; a gateway refuses a mismatched `cache_layout`. The cache is
rebuildable from S3, so there are no migrations: upgrades use a fresh database.

## Commits

- One logical change per commit, stating the user-visible effect.
- Update `README.md` and `docs/` when flags, behaviour or metrics change.
- Never commit `.tmp/`, database dumps or credentials.

## License

Apache-2.0. See [LICENSE](LICENSE).
