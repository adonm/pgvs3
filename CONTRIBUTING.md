# Contributing

pgvs3 is Go. The gateway is stateless over PostgreSQL; the storage layout is
versioned and any change requires a fresh database.

## Toolchain

```sh
mise install        # Go, just, ko, uv, Python, Helm
just setup          # go mod download
```

Go is pinned by `mise.toml` (currently 1.27.3). CI uses the same pins. The
container is built by `ko` from source; there is no Dockerfile and CGO is
disabled.

## Checks

```sh
just test           # Go unit tests
just vet            # go vet
just contract       # PostgreSQL + two gateways + DuckDB 2.0 + DuckLake + Express APIs
```

`just contract` is the correctness gate. It starts a disposable PostgreSQL 18
in Docker, two gateways with the cross-AZ metadata cache, and drives S3, boto3,
DuckDB 2.0, DuckLake 1.1 and `duckdb-object-storage`. It also covers
CreateSession, session auth, directory buckets, copy/rename/append, attributes,
multipart copy, pagination, conditional writes and the reaper.

Format before sending a change:

```sh
mise exec go -- gofmt -w cmd internal test
```

## Storage layout

`internal/store/schema.sql` is layout v8. It is shared with the earlier Rust
gateway on purpose: both binaries can serve the same database. The Go gateway
applies one idempotent, backwards-compatible change at startup
(`DROP CONSTRAINT IF EXISTS objects_file_id_key`) so metadata-only copies can
share immutable files. The reaper checks every object and upload-part reference
before deleting a file.

A breaking layout change means:

1. bump `store.LayoutVersion`;
2. update `schema.sql` with a comment explaining the page/TOAST budget;
3. refuse older versions rather than guessing;
4. document that operators need a fresh database (there is no migration).

Do not add indexes to `s3p.chunks` without measuring: every read is a
`file_id` primary-key range, and the bitmap heap plan is deliberate.

## Performance rules

- Keep chunk rows at 8,120 payload bytes so one row fills one 8 KB page with no
  TOAST.
- Do not add a gateway byte cache. DuckDB's external file cache already holds
  immutable file bytes; the gateway caches metadata only.
- Keep the response path cut-through: stream PostgreSQL rows to HTTP, do not
  assemble the whole object.
- Keep the connection pool most-recently-used first.
- Maintenance must stay bounded and serialised with `pg_try_advisory_lock`.
- Measure with `just bench`; report the layout (compression, row groups, file
  size) with any number.

## Commits

- One logical change per commit.
- State the user-visible effect and the test that covers it.
- Update `README.md` / `docs/` when configuration, the S3 surface or the
  storage layout changes.
- Do not commit `.tmp/`, database files or credentials.

## License

Contributions are Apache-2.0, matching [LICENSE](LICENSE).
