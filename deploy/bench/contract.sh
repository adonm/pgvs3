#!/usr/bin/env bash
# Run the S3/DB contract against a disposable local PostgreSQL and two gateways.
set -euo pipefail
cd "$(dirname "$0")/../.."

container="pgvs3-contract-$$"
pids=()
cleanup() {
  if [ "${#pids[@]}" -gt 0 ]; then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker build -q -t pgvs3-postgres:18-cron deploy/postgres >/dev/null
docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres \
  -p 127.0.0.1::5432 pgvs3-postgres:18-cron \
  -c shared_preload_libraries=pg_cron -c cron.database_name=pgvs3_contract >/dev/null
for i in $(seq 1 40); do
  if docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then break; fi
  [ "$i" = 40 ] && { docker logs "$container" >&2; exit 1; }
  sleep 0.25
done
docker exec "$container" psql -U postgres -v ON_ERROR_STOP=1 \
  -c 'CREATE DATABASE pgvs3_contract' >/dev/null
docker exec "$container" psql -U postgres -d pgvs3_contract -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION pg_cron' >/dev/null

port=$(docker port "$container" 5432/tcp)
export PGVS3_TEST_DB_URL="postgres://postgres:postgres@$port/pgvs3_contract"
export PGVS3_ACCESS_KEY=contract
export PGVS3_SECRET_KEY
PGVS3_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
export PGVS3_POOL_MIN=1 PGVS3_POOL_MAX=4

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
    > ".tmp/pgvs3/local-contract-$label.log" 2>&1 & pids+=("$!")
  for i in $(seq 1 120); do
    if curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then break; fi
    if ! kill -0 "${pids[-1]}" 2>/dev/null; then
      cat ".tmp/pgvs3/local-contract-$label.log" >&2
      exit 1
    fi
    [ "$i" = 120 ] && { cat ".tmp/pgvs3/local-contract-$label.log" >&2; exit 1; }
    sleep 0.25
  done
done
export PGVS3_TEST_ENDPOINT_A="http://127.0.0.1:$port_a"
export PGVS3_TEST_ENDPOINT_B="http://127.0.0.1:$port_b"
curl -fsS --aws-sigv4 aws:amz:us-east-1:s3 \
  --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
  -X PUT "$PGVS3_TEST_ENDPOINT_A/pgvs3-contract" >/dev/null
mbx test -p pgvs3 --test s3_contract -- --ignored \
  --skip clean_failed_prior_contract_objects --skip scheduled_expiry_removes_old_empty_uploads \
  --skip sustained_churn_and_db_reclaim --test-threads=1
mbx test -p pgvs3 --lib -- --ignored --test-threads=1
