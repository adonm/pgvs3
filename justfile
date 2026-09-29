# pgvs3 tasks. Toolchain: `mise install`. Tests and benchmarks start their own
# disposable PostgreSQL in Docker and two local gateways (deploy/bench/stack.sh).

URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"

# List the recipes.
default:
    @{{ just_executable() }} --list

# Fetch everything the build needs (mise installs the toolchain).
[group('dev')]
setup:
    mise install
    cargo fetch
    @echo "ready. next: just contract && just ducklake"

# PostgreSQL 18 + pg_cron in Docker on :5432 for running a gateway by hand.
[group('dev')]
dev-db:
    #!/usr/bin/env bash
    set -euo pipefail
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
    for db in pgvs3_bench ducklake_catalog; do
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

# S3/DB contract through two gateways on a disposable PostgreSQL.
[group('test')]
contract:
    bash deploy/bench/stack.sh contract

# Opt-in sustained overwrite/delete/multipart churn with vacuum evidence.
[group('test')]
churn:
    bash deploy/bench/stack.sh churn

# DuckLake compatibility: writer on one gateway, reader on the other.
[group('test')]
ducklake:
    bash deploy/bench/stack.sh bench compat

# DuckDB S3 benchmark, DuckLake compatibility and row-group tuning (parts: s3,compat,tuning).
[group('bench')]
bench parts="s3,compat,tuning":
    bash deploy/bench/stack.sh bench '{{ parts }}'

# Image name for `just image`. CI may override it.
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

# What CI runs.
[group('ci')]
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    hk check --all
    mbx build --release --locked
    mbx test --workspace
    just contract
    just ducklake
