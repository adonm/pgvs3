#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=test/harness.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../test/harness.sh"
mode=${1:-contract}
case "$mode" in contract | fault | interop | s3t | bench) ;; *) echo "unknown mode: $mode" >&2; exit 2 ;; esac
test_run "$mode"

shared_buffers=1GB
[ "$mode" = fault ] && shared_buffers=256MB
test_postgres "$shared_buffers"
for db in pgvs3 ducklake_catalog; do
  docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $db" \
    >> "$PGVS3_TEST_RUN_DIR/postgres-start.log" 2>&1
done
pg="127.0.0.1:$PGVS3_TEST_PG_PORT"
export PGVS3_TEST_DB_URL="postgres://postgres:postgres@$pg/pgvs3?sslmode=disable"
export PGVS3_ACCESS_KEY="pgvs3-$mode"
PGVS3_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
export PGVS3_SECRET_KEY
if [ "$mode" != bench ]; then export PGVS3_POOL_MAX=8; fi

test_build_gateway

read -r port_a port_b < <(python3 -c '
import socket
sockets=[socket.socket() for _ in range(2)]
for s in sockets:
    s.bind(("127.0.0.1",0))
print(*(s.getsockname()[1] for s in sockets))
for s in sockets:
    s.close()
')
for entry in "a:$port_a" "b:$port_b"; do
  label=${entry%%:*}; port=${entry#*:}
  args=()
  if [ "$mode" = contract ] || [ "$mode" = fault ] || [ "$mode" = interop ]; then
    args+=(--reap-grace 1s)
  fi
  test_start_gateway "$label" "$PGVS3_TEST_DB_URL" "$port" "${args[@]}"
done
export PGVS3_TEST_ENDPOINT_A="http://127.0.0.1:$port_a"
export PGVS3_TEST_ENDPOINT_B="http://127.0.0.1:$port_b"
export PGVS3_TEST_PID_A="${test_pids[0]}"
export PGVS3_TEST_PID_B="${test_pids[1]}"

case "$mode" in
contract)
  bash test/e2e.sh
  ;;
fault)
  bash test/fault.sh
  ;;
interop)
  bash test/interop.sh
  ;;
s3t)
  PGVS3_S3_HOST=127.0.0.1 PGVS3_S3_PORT="$port_a" bash test/s3t.sh
  ;;
bench)
  mkdir -p .tmp/pgvs3
  export PGVS3_CATALOG="dbname=ducklake_catalog host=${pg%:*} port=${pg#*:} user=postgres password=postgres"
  mise exec uv -- uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-2.0.0}" \
    python deploy/bench/duck_bench.py --parts "${2:-s3,compat,tuning}" --out .tmp/pgvs3/bench.json
  ;;
esac > "$PGVS3_TEST_RUN_DIR/suite.log" 2>&1
