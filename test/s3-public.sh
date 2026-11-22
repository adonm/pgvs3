#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/pgvs3-s3pub.XXXXXX")
PG_CONTAINER=""
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [ -n "$PG_CONTAINER" ]; then
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  fi
  rm -rf "$SCRATCH"
}
trap cleanup EXIT

if [ -z "${PGVS3_TEST_DATABASE_URL:-}" ]; then
  PG_CONTAINER="pgvs3-s3pub-$$"
  docker run -d --name "$PG_CONTAINER" -p 127.0.0.1:15437:5432 \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pgvs3_test \
    postgres:18 -c max_connections=256 >/dev/null
  for i in $(seq 1 30); do
    if docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then
      break
    fi
    [ "$i" = 30 ] && { echo "postgres failed to start"; exit 1; }
    sleep 1
  done
  export PGVS3_TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:15437/pgvs3_test?sslmode=disable"
fi

BIN="$SCRATCH/pgvs3"
mise exec go -- go build -o "$BIN" ./cmd/pgvs3

unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CONTAINER_CREDENTIALS_FULL_URI AWS_WEB_IDENTITY_TOKEN_FILE || true
export AWS_EC2_METADATA_DISABLED=true

start_gw() {
  local addr="$1" region="$2" buckets="$3" log="$4"
  "$BIN" serve \
    --url "$PGVS3_TEST_DATABASE_URL" \
    --addr "$addr" \
    --origin-region "$region" \
    --buckets "$buckets" \
    --range-cache-chunk 1048576 \
    --spool-dir "$SCRATCH" >"$log" 2>&1 &
  PIDS+=("$!")
}

start_gw "127.0.0.1:18024" "us-west-2" "overturemaps-us-west-2" "$SCRATCH/gwW1.log"
start_gw "127.0.0.1:18025" "us-west-2" "overturemaps-us-west-2" "$SCRATCH/gwW2.log"
start_gw "127.0.0.1:18026" "us-east-1" "noaa-gsod-pds,tripdata,overturemaps-us-west-2" "$SCRATCH/gwE1.log"
start_gw "127.0.0.1:18027" "us-east-1" "noaa-gsod-pds,tripdata,overturemaps-us-west-2" "$SCRATCH/gwE2.log"

for url in \
  "http://127.0.0.1:18024/readyz" \
  "http://127.0.0.1:18025/readyz" \
  "http://127.0.0.1:18026/readyz" \
  "http://127.0.0.1:18027/readyz"; do
  for i in $(seq 1 30); do
    if "$BIN" health --url "$url" >/dev/null 2>&1; then
      break
    fi
    [ "$i" = 30 ] && { echo "gateway $url failed to become ready"; cat "$SCRATCH"/gw*.log; exit 1; }
    sleep 0.5
  done
done

PGVS3_GW_W1="http://127.0.0.1:18024" \
PGVS3_GW_W2="http://127.0.0.1:18025" \
PGVS3_GW_E1="http://127.0.0.1:18026" \
PGVS3_GW_E2="http://127.0.0.1:18027" \
TMPDIR="$SCRATCH" \
  mise exec uv -- uv run --quiet --no-project --with boto3 --with duckdb python test/s3_public.py || {
    cat "$SCRATCH"/gw*.log
    exit 1
  }
