# pgvs3 tasks. Toolchain: `mise install`. Testing and benchmarks run in kind.

URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"
# Local development database. Smoke and CI use the kind stack instead.
PG_URL := env("PG_URL", URL)

# List the recipes.
default:
    @{{ just_executable() }} --list

# Fetch everything the build needs (mise installs the toolchain).
[group('dev')]
setup:
    mise install
    cargo fetch
    @echo "ready. next: just smoke (or just dev-db for local development)"

# PostgreSQL 18 + pg_cron in Docker (external DBs need pg_cron too: set PG_URL).
[group('dev')]
dev-db:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "${PG_URL:-}" ] && [ "${PG_URL:-}" != "{{ URL }}" ]; then
      echo "using PG_URL=$PG_URL"
      exit 0
    fi
    if (echo > /dev/tcp/127.0.0.1/5432) 2>/dev/null; then
      echo "postgres already listening on :5432"
    elif docker inspect pgvs3-pg >/dev/null 2>&1; then
      docker start pgvs3-pg >/dev/null
    else
      docker build -q -t pgvs3-postgres:18-cron deploy/postgres
      docker run -d --name pgvs3-pg -p 127.0.0.1:5432:5432 \
        -e POSTGRES_PASSWORD=postgres pgvs3-postgres:18-cron \
        -c shared_preload_libraries=pg_cron -c cron.database_name=pgvs3_bench
    fi
    for i in $(seq 1 30); do
      docker exec pgvs3-pg psql -U postgres -c 'SELECT 1' >/dev/null 2>&1 && break
      [ "$i" = 30 ] && { echo "postgres did not come up; try: just dev-db-clean && just dev-db"; docker logs --tail 5 pgvs3-pg; exit 1; }
      sleep 1
    done
    for db in pgvs3_bench ducklake_catalog ducklake_catalog_local; do
      docker exec pgvs3-pg psql -U postgres -c "CREATE DATABASE $db" 2>/dev/null || true
    done
    docker exec pgvs3-pg psql -U postgres -d pgvs3_bench -v ON_ERROR_STOP=1 \
      -c 'CREATE EXTENSION IF NOT EXISTS pg_cron' || {
      echo 'pgvs3 requires pg_cron; an existing pgvs3-pg without it must be upgraded (preserve its data) or replaced explicitly with just dev-db-clean' >&2
      exit 1
    }
    echo "postgres up: {{ URL }}"

# Delete the Docker PostgreSQL and its data.
[group('dev')]
dev-db-clean:
    docker rm -f pgvs3-pg || true

# Kind QA: contract, service health, and upstream ClickBench/SpatialBench inputs at smoke scale.
[group('kind')]
smoke: kind-up
    #!/usr/bin/env bash
    set -euo pipefail
    {{ just_executable() }} kind-contract
    QUICK=1 SUITES=validate,click,spatial {{ just_executable() }} kind-bench

# Rust S3 contract against both kind gateway pods and its PostgreSQL storage.
[group('kind')]
kind-contract:
    bash deploy/kind/contract.sh

# Opt-in sustained overwrite/delete/multipart churn with DB vacuum evidence.
[group('kind')]
kind-churn:
    bash deploy/kind/contract.sh churn

# Image name for `just image` and the kind gateway chart. CI may override it.
IMAGE := env("IMAGE", "ghcr.io/adonm/pgvs3")

# Build the container image (amd64 + arm64, so AWS Graviton works); push=true publishes to GHCR.
[group('image')]
image push="false":
    #!/usr/bin/env bash
    set -euo pipefail
    # buildx cannot --load a multi-platform build; only the publish needs both.
    args=(-t "{{ IMAGE }}:latest")
    if [ "{{ push }}" = "true" ]; then
      args+=(--platform linux/amd64,linux/arm64 --push)
    else
      args+=(--load)
    fi
    docker buildx build "${args[@]}" .

# What CI runs (fmt, clippy, build, tests, then `just smoke` on kind).
[group('ci')]
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    hk check --all
    python3 -m unittest discover -s deploy/bench/harness -p 'test_*.py'
    python3 -m unittest discover -s deploy/bench/suites -p 'test_*.py'
    mbx build --release --locked
    mbx test --workspace
    just contract
    just smoke

# The pgvfs:// DuckDB extension, built in a container -> target/pgvfs/.
[group('dev')]
pgvfs-ext:
    docker buildx build -f crates/pgvfs/extension/Containerfile \
      --output type=local,dest=target/pgvfs .

# S3/DB tests against a temporary local PostgreSQL; no kind cluster required.
[group('dev')]
contract:
    bash deploy/bench/contract.sh

# --- kind: Postgres 18 + pgvs3 + DuckLake ------------------------------------
# Select SUITES explicitly; QUICK=1 is smoke QA, never a full-scale score.

# Stand up pgvs3 and DuckLake in kind with local PostgreSQL.
[group('kind')]
kind-up:
    bash deploy/kind/up.sh '{{ IMAGE }}'

# Verify services and the overwrite regression in the same kind test runner.
[group('kind')]
kind-validate:
    SUITES=validate {{ just_executable() }} kind-bench

# Run selected upstream suites sequentially (SUITES); QUICK=1 is smoke QA.
[group('kind')]
kind-bench:
    bash deploy/kind/bench.sh

# Tear down the kind cluster.
[group('kind')]
kind-down:
    kind delete cluster --name pgvs3

# Pre-release test data only: replace the kind databases.
[group('kind')]
[confirm("Discard pgvs3 and DuckLake data in this kind cluster?")]
kind-reset:
    bash deploy/kind/reset.sh
