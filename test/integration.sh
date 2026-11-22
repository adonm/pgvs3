#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

export DOCKER_HOST="${DOCKER_HOST:-unix:///run/host/run/docker.sock}"
if ! command -v docker >/dev/null 2>&1 && [ -x /run/host/usr/bin/docker ]; then
  export PATH="/run/host/usr/bin:$PATH"
fi

SCRATCH=$(mktemp -d "${TMPDIR:-/tmp/opencode}/pgvs3-int.XXXXXX")
PG_CONTAINER=""
VGW_PID=""
GW_A_PID=""
GW_B_PID=""

cleanup() {
  for pid in "$GW_A_PID" "$GW_B_PID" "$VGW_PID"; do
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
  PG_CONTAINER="pgvs3-int-$$"
  docker run -d --name "$PG_CONTAINER" -p 127.0.0.1:15432:5432 \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pgvs3_test \
    postgres:18 -c max_connections=256 >/dev/null
  for i in $(seq 1 30); do
    if docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then
      break
    fi
    [ "$i" = 30 ] && { echo "postgres failed to start"; exit 1; }
    sleep 1
  done
  export PGVS3_TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:15432/pgvs3_test?sslmode=disable"
fi

if [ -z "${CGO_ENABLED:-}" ] && ! printf '%s\n' '#include <stdio.h>' | cc -x c - -o /dev/null 2>/dev/null; then
  export CGO_ENABLED=0
fi

echo "=== Tier 1: Go integration tests (in-process fake S3 + PostgreSQL 18) ==="
mise exec go -- go test -tags=integration -v -count=1 ./internal/s3 -run TestReadThroughCacheIntegration

echo "=== Tier 2: versitygw + two pgvs3 gateways + boto3/DuckDB smoke ==="
BIN="$SCRATCH/pgvs3"
mise exec go -- go build ${GOFLAGS:-} -o "$BIN" ./cmd/pgvs3

export AWS_ACCESS_KEY_ID="vgw-ak"
export AWS_SECRET_ACCESS_KEY="vgw-sk"
export AWS_REGION="us-east-1"

VGW_DIR="$SCRATCH/vgw-data"
mkdir -p "$VGW_DIR"
GOBIN="$SCRATCH" mise exec go -- go install github.com/versity/versitygw/cmd/versitygw@v1.11.0
"$SCRATCH/versitygw" \
  --access "$AWS_ACCESS_KEY_ID" --secret "$AWS_SECRET_ACCESS_KEY" \
  --region "$AWS_REGION" --port "127.0.0.1:17070" \
  posix "$VGW_DIR" >"$SCRATCH/vgw.log" 2>&1 &
VGW_PID=$!

for i in $(seq 1 60); do
  if (echo >/dev/tcp/127.0.0.1/17070) 2>/dev/null; then
    break
  fi
  [ "$i" = 60 ] && { echo "versitygw failed to start"; cat "$SCRATCH/vgw.log"; exit 1; }
  sleep 0.5
done

"$BIN" serve \
  --url "$PGVS3_TEST_DATABASE_URL" \
  --addr "127.0.0.1:18014" \
  --origin-endpoint "http://127.0.0.1:17070" \
  --origin-region "$AWS_REGION" \
  --origin-path-style \
  --range-cache-chunk 1048576 \
  --spool-dir "$SCRATCH" >"$SCRATCH/gwA.log" 2>&1 &
GW_A_PID=$!

"$BIN" serve \
  --url "$PGVS3_TEST_DATABASE_URL" \
  --addr "127.0.0.1:18015" \
  --origin-endpoint "http://127.0.0.1:17070" \
  --origin-region "$AWS_REGION" \
  --origin-path-style \
  --range-cache-chunk 1048576 \
  --spool-dir "$SCRATCH" >"$SCRATCH/gwB.log" 2>&1 &
GW_B_PID=$!

for url in "http://127.0.0.1:18014/readyz" "http://127.0.0.1:18015/readyz"; do
  for i in $(seq 1 30); do
    if "$BIN" health --url "$url" >/dev/null 2>&1; then
      break
    fi
    [ "$i" = 30 ] && { echo "gateway $url failed to become ready"; cat "$SCRATCH/gwA.log" "$SCRATCH/gwB.log"; exit 1; }
    sleep 0.5
  done
done

PGVS3_GW_A="http://127.0.0.1:18014" \
PGVS3_GW_B="http://127.0.0.1:18015" \
TMPDIR="$SCRATCH" \
  mise exec uv -- uv run --quiet --no-project --with boto3 --with duckdb python test/smoke_versity.py || {
    echo "=== gwA.log ==="
    cat "$SCRATCH/gwA.log"
    echo "=== vgw.log ==="
    cat "$SCRATCH/vgw.log"
    exit 1
  }
