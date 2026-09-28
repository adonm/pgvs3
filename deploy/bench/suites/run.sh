#!/usr/bin/env bash
# One kind QA Job per invocation. DuckLake inputs are upstream
# ClickBench/SpatialBench pinned revisions.
set -euo pipefail

PG_HOST=${PG_HOST:-postgres}
PG_USER=${PG_USER:-postgres}
PG_PASSWORD=${PG_PASSWORD:-postgres}
export PGPASSWORD="$PG_PASSWORD"
PGVS3_URL=${PGVS3_URL:-http://pgvs3:8014}
export PGVS3_URL
export PGVS3_ENDPOINT=${PGVS3_ENDPOINT:-${PGVS3_URL#*://}}
LAKE_ROOT=${LAKE_ROOT:-s3://lake/v/}
OUT=/bench/out
mkdir -p "$OUT"

# Smoke exercises a subset for functionality only, not official-scale results.
case "${QUICK:-0}" in
1 | true)
  PASSES=${PASSES:-1}
  PARTS=${PARTS:-1}
  SPATIAL_SF=${SPATIAL_SF:-0.1}
  SPATIAL_QUERIES=${SPATIAL_QUERIES:-1-3,6}
  ;;
esac
load_flags=(--download --load)
if [ "${BENCH_REUSE:-0}" = 1 ]; then load_flags=(--views-only); fi

suite=${1:?suite: validate | click | spatial}
case "$suite" in
validate)
  pass=0; fail=0
  check() {
    local name=$1; shift
    if out=$("$@" 2>&1); then echo "PASS  $name"; pass=$((pass + 1));
    else echo "FAIL  $name: $(echo "$out" | tail -1)"; fail=$((fail + 1)); fi
  }
  check "postgres (pg_isready)" pg_isready -h "$PG_HOST" -U "$PG_USER"
  check "ducklake (catalog database)" psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -U "$PG_USER" -d ducklake_catalog -tAc 'SELECT 1'
  check "pgvs3 (healthz)" curl -fsS --max-time 10 "$PGVS3_URL/healthz"
  check "ducklake (attach + read)" python3 /bench/suites/duck_check.py
  echo "validate: $pass passed, $fail failed"
  [ "$fail" = 0 ]
  ;;

click)
  python3 /bench/harness/analytics_bench.py --bench click "${load_flags[@]}" \
    --parts "${PARTS:-0}" --passes "${PASSES:-3}" \
    --memory-limit "${DUCKDB_MEMORY_LIMIT:-6GiB}" \
    --catalog "dbname=ducklake_catalog host=$PG_HOST user=$PG_USER" \
    --data-path "$LAKE_ROOT" --out "$OUT/click.json"
  ;;

spatial)
  python3 /bench/harness/analytics_bench.py --bench spatial "${load_flags[@]}" \
    --sf "${SPATIAL_SF:-10}" --queries "${SPATIAL_QUERIES:-1-12}" \
    --query-timeout "${SPATIAL_QUERY_TIMEOUT:-600}" --passes "${PASSES:-3}" \
    --memory-limit "${DUCKDB_MEMORY_LIMIT:-6GiB}" \
    --catalog "dbname=ducklake_catalog host=$PG_HOST user=$PG_USER" \
    --data-path "$LAKE_ROOT" --out "$OUT/spatial.json"
  ;;

storage)
  # Same data, DuckDB and queries on both storage paths: DuckLake over the
  # pgvs3 S3 gateway, and over pgvfs in its own database (never shared with
  # the gateway's). Fresh passes time each query on a new DuckDB instance.
  export PGVS3_DB_ALLOW_PLAINTEXT=true  # isolated kind network
  schema=${STORAGE_SCHEMA:-ab}
  python3 /bench/harness/analytics_bench.py --bench click "${load_flags[@]}" \
    --stacks "${STACKS:-s3,pgvfs}" \
    --parts "${PARTS:-0}" --passes "${PASSES:-3}" --fresh-passes "${FRESH_PASSES:-3}" \
    --memory-limit "${DUCKDB_MEMORY_LIMIT:-6GiB}" \
    --catalog "dbname=ducklake_catalog host=$PG_HOST user=$PG_USER" \
    --data-path "s3://lake/$schema/" \
    --pgvfs-database pgvfs \
    --pgvfs-data-path "pgvfs://lake/$schema/" \
    --metadata-schema "$schema" --out "$OUT/storage.json"
  ;;

*) echo "unknown suite: $suite" >&2; exit 2 ;;
esac
