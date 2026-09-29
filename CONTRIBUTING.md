# Contributing

Run `mise install`, then:

| Command | What it checks |
| --- | --- |
| `hk check --all` | rustfmt, Clippy, shell/Python syntax, chart lint, justfile |
| `just contract` | S3/DB contract through two gateways on a disposable PostgreSQL |
| `just ducklake` | DuckLake lifecycle: writer on one gateway, reader on the other |
| `just churn` | Opt-in sustained overwrite/delete/multipart churn with vacuum evidence |
| `just bench` | DuckDB S3 benchmark, DuckLake compatibility and row-group tuning |

`just ci` runs the first three plus the build and unit tests. Each recipe
starts its own PostgreSQL container and gateways (`deploy/bench/stack.sh`) and
removes them on exit; nothing reuses existing data.

## Benchmarks

`just bench` writes `.tmp/pgvs3/bench.json`. Give it the host: stop other
containers and heavy processes first. Record the commit, DuckDB version,
machine and core count with any published numbers, and replace the tables in
the README and [docs/benchmarks.md](docs/benchmarks.md) with the newest
complete run rather than appending. S3 comparison figures are third-party
references (cited in docs/benchmarks.md), not measured here.

A storage layout change bumps `db::LAYOUT_VERSION` and needs a fresh database;
there are no migrations.
