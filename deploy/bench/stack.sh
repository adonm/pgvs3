#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=test/harness.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../test/harness.sh"
mode=${1:-contract}
case "$mode" in contract | analytics | fault | interop | s3t | bench) ;; *) echo "unknown mode: $mode" >&2; exit 2 ;; esac
test_run "$mode"
[ "$mode" != analytics ] || test_keep_artifacts=true
parts=${2:-s3,compat,tuning}
if [[ ",$parts," = *,differential,* || ",$parts," = *,object-storage,* ]]; then test_keep_artifacts=true; fi

shared_buffers=1GB
[ "$mode" = fault ] && shared_buffers=256MB
if [ "$mode" = bench ] && [[ ",$parts," = *,differential,* || ",$parts," = *,object-storage,* ]]; then
  test_postgres "$shared_buffers" "" -c max_wal_size=4GB
else
  test_postgres "$shared_buffers"
fi
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

if [ "$mode" = bench ] && [[ ",$parts," = *,object-storage,* ]]; then
  ro_port=$(test_port)
  test_start_gateway objfs-ro "$PGVS3_TEST_DB_URL" "$ro_port" --read-only --pool-max 8 --log-level warn
  export PGVS3_TEST_ENDPOINT_RO="http://127.0.0.1:$ro_port"
fi

if [ "$mode" = contract ] || [ "$mode" = analytics ] || [ "$mode" = fault ]; then
  primary_container=$PGVS3_TEST_PG_CONTAINER primary_port=$PGVS3_TEST_PG_PORT primary_url=$PGVS3_TEST_DATABASE_URL
  test_postgres 128MB origin
  export PGVS3_TEST_ORIGIN_DB_URL=$PGVS3_TEST_DATABASE_URL
  origin_port=$(test_port)
  test_start_gateway origin "$PGVS3_TEST_ORIGIN_DB_URL" "$origin_port" --pool-max 8 --log-level warn
  export PGVS3_TEST_ORIGIN_ENDPOINT="http://127.0.0.1:$origin_port"
  PGVS3_TEST_PG_CONTAINER=$primary_container PGVS3_TEST_PG_PORT=$primary_port PGVS3_TEST_DATABASE_URL=$primary_url
fi

case "$mode" in
contract)
  bash test/e2e.sh
  ;;
analytics)
  mise exec uv -- uv run --quiet --no-project --with duckdb==2.0.0 python test/data_plane.py analytics
  ;;
fault)
  bash test/fault.sh
  test_standby "$PGVS3_TEST_PG_CONTAINER" sync
  export PGVS3_TEST_SYNC_URL="postgres://postgres:postgres@127.0.0.1:$PGVS3_TEST_STANDBY_PORT/pgvs3?sslmode=disable"
  export PGVS3_TEST_SYNC_APP=$PGVS3_TEST_STANDBY_APP
  test_standby "$PGVS3_TEST_PG_CONTAINER" async
  export PGVS3_TEST_ASYNC_URL="postgres://postgres:postgres@127.0.0.1:$PGVS3_TEST_STANDBY_PORT/pgvs3?sslmode=disable"
  test_postgres 128MB restore
  export PGVS3_TEST_RESTORE_CONTAINER=$PGVS3_TEST_PG_CONTAINER
  export PGVS3_TEST_RESTORE_URL="postgres://postgres:postgres@127.0.0.1:$PGVS3_TEST_PG_PORT/pgvs3_restored?sslmode=disable"
  docker exec "$PGVS3_TEST_PG_CONTAINER" createdb -U postgres pgvs3_restored
  PGVS3_TEST_PG_CONTAINER=$primary_container PGVS3_TEST_PG_PORT=$primary_port PGVS3_TEST_DATABASE_URL=$primary_url
  mise exec uv -- uv run --quiet --no-project --with duckdb==2.0.0 python test/data_plane.py replication
  ;;
interop)
  bash test/interop.sh
  ;;
s3t)
  PGVS3_S3_HOST=127.0.0.1 PGVS3_S3_PORT="$port_a" bash test/s3t.sh
  ;;
bench)
  mkdir -p .tmp/pgvs3
  if [[ ",$parts," = *,s3,* ]]; then
    curl -fsS -X PUT "$PGVS3_TEST_ENDPOINT_A/bench" >/dev/null
  fi
  if [[ ",$parts," = *,compat,* || ",$parts," = *,tuning,* ]]; then
    curl -fsS -X PUT "$PGVS3_TEST_ENDPOINT_A/lake" >/dev/null
  fi
  export PGVS3_CATALOG="dbname=ducklake_catalog host=${pg%:*} port=${pg#*:} user=postgres password=postgres"
  if [ "$#" -ge 2 ]; then shift 2; else shift; fi
  out=.tmp/pgvs3/bench.json
  if [[ ",$parts," = *,differential,* ]]; then out="$PGVS3_TEST_RUN_DIR/differential.json"; fi
  if [[ ",$parts," = *,object-storage,* ]]; then out="$PGVS3_TEST_RUN_DIR/object-storage.json"; fi
  mise exec uv -- uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-2.0.0}" \
    python deploy/bench/duck_bench.py --parts "$parts" --out "$out" "$@"
  ;;
esac > "$PGVS3_TEST_RUN_DIR/suite.log" 2>&1
if [ "$mode" = analytics ] || [[ ",$parts," = *,differential,* || ",$parts," = *,object-storage,* ]]; then cat "$PGVS3_TEST_RUN_DIR/suite.log"; fi
