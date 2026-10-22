#!/usr/bin/env bash
# Disposable PostgreSQL + two local gateways, then one mode:
#   contract  end-to-end S3, DuckDB 2.0 and DuckLake contract through both gateways
#   fault     gateway/PostgreSQL failure and churn checks
#   interop   rclone/restic/boto3/DuckLake/SlateDB compatibility
#   bench     DuckDB benchmark (parts: s3,compat,tuning)
set -euo pipefail
cd "$(dirname "$0")/../.."
mode=${1:-contract}
case "$mode" in contract | fault | interop | bench) ;; *) echo "unknown mode: $mode" >&2; exit 2 ;; esac

container="pgvs3-$mode-$$"
pids=()
cleanup() {
  if [ "${#pids[@]}" -gt 0 ]; then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
  docker rm -fv "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

shared_buffers=1GB
[ "$mode" = fault ] && shared_buffers=256MB
pg_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres \
  -p "127.0.0.1:$pg_port:5432" postgres:18 \
  -c max_connections=256 -c shared_buffers=$shared_buffers >/dev/null
for i in $(seq 1 60); do
  if docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then break; fi
  [ "$i" = 60 ] && { docker logs "$container" >&2; exit 1; }
  sleep 0.25
done
for db in pgvs3 ducklake_catalog; do
  docker exec "$container" psql -U postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $db" >/dev/null
done
pg="127.0.0.1:$pg_port"
export PGVS3_TEST_PG_PORT=$pg_port
export PGVS3_TEST_DB_URL="postgres://postgres:postgres@$pg/pgvs3?sslmode=disable"
export PGVS3_ACCESS_KEY="pgvs3-$mode"
PGVS3_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
export PGVS3_SECRET_KEY
if [ "$mode" != bench ]; then export PGVS3_POOL_MIN=1 PGVS3_POOL_MAX=8; fi

mkdir -p .tmp/pgvs3
(cd "$(dirname "$0")/../.." && mise exec go -- go build -o .tmp/pgvs3/pgvs3 ./cmd/pgvs3)

read -r port_a port_b web_port < <(python3 -c '
import socket
ports=[]
for _ in range(3):
    with socket.socket() as s:
        s.bind(("127.0.0.1",0))
        ports.append(s.getsockname()[1])
print(*ports)
')
for entry in "a:$port_a" "b:$port_b"; do
  label=${entry%%:*}; port=${entry#*:}
  args=()
  if [ "$mode" = contract ] || [ "$mode" = fault ] || [ "$mode" = interop ]; then
    args+=(--reap-grace 1s --maintenance-interval 1s)
  fi
  if [ "$label" = a ] && { [ "$mode" = contract ] || [ "$mode" = interop ]; }; then
    args+=(--web-addr "127.0.0.1:$web_port")
  fi
  .tmp/pgvs3/pgvs3 --url "$PGVS3_TEST_DB_URL" serve --addr "127.0.0.1:$port" "${args[@]}" \
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
export PGVS3_TEST_WEB_URL="http://127.0.0.1:$web_port"
export PGVS3_TEST_PID_A="${pids[0]}"
export PGVS3_TEST_PID_B="${pids[1]}"
export PGVS3_TEST_PG_CONTAINER="$container"
if [ "$mode" = contract ] || [ "$mode" = interop ]; then
  for i in $(seq 1 120); do
    curl -fsS --max-time 1 "$PGVS3_TEST_WEB_URL/healthz" >/dev/null 2>&1 && break
    [ "$i" = 120 ] && { cat .tmp/pgvs3/$mode-a.log >&2; exit 1; }
    sleep 0.25
  done
fi
for bucket in pgvs3-contract pgvs3-fault bench lake; do
  curl -fsS --aws-sigv4 aws:amz:us-east-1:s3 \
    --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
    -X PUT "$PGVS3_TEST_ENDPOINT_A/$bucket" >/dev/null
done

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
bench)
  export PGVS3_CATALOG="dbname=ducklake_catalog host=${pg%:*} port=${pg#*:} user=postgres password=postgres"
  mise exec uv -- uv run --quiet --no-project --with "duckdb==${DUCKDB_PY:-2.0.0}" \
    python deploy/bench/duck_bench.py --parts "${2:-s3,compat,tuning}" --out .tmp/pgvs3/bench.json
  ;;
esac
