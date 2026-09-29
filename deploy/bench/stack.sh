#!/usr/bin/env bash
# Disposable PostgreSQL + two local gateways, then one mode:
#   contract            S3/DB contract through both gateways
#   churn               opt-in sustained overwrite/delete/multipart churn
#   bench [parts]       DuckDB bench: s3,compat,tuning (default: all)
set -euo pipefail
cd "$(dirname "$0")/../.."
mode=${1:-contract}
case "$mode" in contract | churn | bench) ;; *) echo "unknown mode: $mode" >&2; exit 2 ;; esac

container="pgvs3-$mode-$$"
pids=()
cleanup() {
  if [ "${#pids[@]}" -gt 0 ]; then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT
# Bash skips EXIT traps when killed by a signal; exit normally instead.
trap 'exit 130' INT
trap 'exit 143' TERM HUP

# Two 64-connection gateway pools plus the DuckLake catalog clients.
docker build -q -t pgvs3-postgres:18-cron deploy/postgres >/dev/null
docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres \
  -p 127.0.0.1::5432 pgvs3-postgres:18-cron \
  -c max_connections=256 -c shared_buffers=1GB \
  -c shared_preload_libraries=pg_cron -c cron.database_name=pgvs3 -c cron.log_run=off >/dev/null
for i in $(seq 1 40); do
  if docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then break; fi
  [ "$i" = 40 ] && { docker logs "$container" >&2; exit 1; }
  sleep 0.25
done
for db in pgvs3 ducklake_catalog; do
  docker exec "$container" psql -U postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $db" >/dev/null
done
docker exec "$container" psql -U postgres -d pgvs3 -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION pg_cron' >/dev/null

pg=$(docker port "$container" 5432/tcp)
export PGVS3_TEST_DB_URL="postgres://postgres:postgres@$pg/pgvs3"
export PGVS3_ACCESS_KEY="pgvs3-$mode"
export PGVS3_SECRET_KEY
PGVS3_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
# The contract needs few connections; the bench uses the gateway defaults.
if [ "$mode" != bench ]; then export PGVS3_POOL_MIN=1 PGVS3_POOL_MAX=4; fi

mbx build --release --locked -p pgvs3
read -r port_a port_b < <(python3 -c '
import socket
ports = []
for _ in range(2):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        ports.append(listener.getsockname()[1])
print(*ports)
')
mkdir -p .tmp/pgvs3
for entry in "a:$port_a" "b:$port_b"; do
  label=${entry%%:*}
  port=${entry#*:}
  ./target/release/pgvs3 --url "$PGVS3_TEST_DB_URL" serve --addr "127.0.0.1:$port" \
    > ".tmp/pgvs3/$mode-$label.log" 2>&1 & pids+=("$!")
  for i in $(seq 1 120); do
    if curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then break; fi
    if ! kill -0 "${pids[-1]}" 2>/dev/null || [ "$i" = 120 ]; then
      cat ".tmp/pgvs3/$mode-$label.log" >&2
      exit 1
    fi
    sleep 0.25
  done
done
export PGVS3_TEST_ENDPOINT_A="http://127.0.0.1:$port_a"
export PGVS3_TEST_ENDPOINT_B="http://127.0.0.1:$port_b"
for bucket in pgvs3-contract bench lake; do
  curl -fsS --aws-sigv4 aws:amz:us-east-1:s3 \
    --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
    -X PUT "$PGVS3_TEST_ENDPOINT_A/$bucket" >/dev/null
done

case "$mode" in
contract)
  mbx test -p pgvs3 --test s3_contract -- --ignored \
    --skip clean_failed_prior_contract_objects --skip scheduled_expiry_removes_old_empty_uploads \
    --skip sustained_churn_and_db_reclaim --test-threads=1
  mbx test -p pgvs3 --lib -- --ignored --test-threads=1
  ;;
churn)
  mbx test -p pgvs3 --test s3_contract sustained_churn_and_db_reclaim -- --ignored --exact --nocapture
  ;;
bench)
  export PGVS3_CATALOG="dbname=ducklake_catalog host=${pg%:*} port=${pg#*:} user=postgres password=postgres"
  uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:?run with mise}" \
    python deploy/bench/duck_bench.py --parts "${2:-s3,compat,tuning}" --out .tmp/pgvs3/bench.json
  ;;
esac
