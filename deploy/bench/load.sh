#!/usr/bin/env bash
# Direct gateway benchmark (no DuckDB client) on a disposable PostgreSQL.
# Writes NDJSON to .tmp/pgvs3/load.json and logs to .tmp/pgvs3/load-*.log.
set -euo pipefail
cd "$(dirname "$0")/../.."
container="pgvs3-load-$$"
pid=""
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  docker rm -fv "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres \
  -p 127.0.0.1::5432 postgres:18 \
  -c max_connections=256 -c shared_buffers=1GB >/dev/null
for i in $(seq 1 60); do
  docker exec "$container" pg_isready -U postgres >/dev/null 2>&1 && break
  [ "$i" = 60 ] && { docker logs "$container" >&2; exit 1; }
  sleep 0.25
done
docker exec "$container" psql -U postgres -v ON_ERROR_STOP=1 -c 'CREATE DATABASE pgvs3' >/dev/null
pg=$(docker port "$container" 5432/tcp)
url="postgres://postgres:postgres@$pg/pgvs3?sslmode=disable"

mkdir -p .tmp/pgvs3
mise exec go -- go build -o .tmp/pgvs3/pgvs3 ./cmd/pgvs3
read -r port < <(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
PGVS3_URL="$url" PGVS3_ACCESS_KEY=bench PGVS3_SECRET_KEY=bench-secret \
  PGVS3_POOL_MAX=128 PGVS3_LOG_LEVEL=warn \
  .tmp/pgvs3/pgvs3 serve --addr "127.0.0.1:$port" > .tmp/pgvs3/load-gateway.log 2>&1 &
pid=$!
for i in $(seq 1 120); do
  curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 && break
  [ "$i" = 120 ] && { cat .tmp/pgvs3/load-gateway.log >&2; exit 1; }
  sleep 0.25
done
out=.tmp/pgvs3/load.json
: > "$out"
run_at() {
  local endpoint=$1; shift
  echo "== $*" >&2
  mise exec go -- go run ./test/loadgen -endpoint "$endpoint" \
    -bucket bench -access bench -secret bench-secret "$@" | tee -a "$out"
}
run() { run_at "http://127.0.0.1:$port" "$@"; }

# 4 KiB objects: one and many workers.
for op in put get; do
  for w in 1 16 64 256; do
    run -op "$op" -size 4096 -workers "$w" -duration 5s -count 2000
  done
done
# Cached HEAD metadata path.
for w in 1 16 64; do
  run -op head -size 4096 -workers "$w" -duration 5s -count 2000
done
# 8 MiB and 64 MiB reads; 64 MiB writes.
for size in 8388608 67108864; do
  count=16
  [ "$size" = 67108864 ] && count=8
  for w in 1 8 16; do
    run -op get -size "$size" -workers "$w" -duration 5s -count "$count"
  done
done
for w in 1 8; do
  run -op put -size 67108864 -workers "$w" -duration 5s -count 8
done
# 8 KiB range reads from 64 MiB objects, and listing.
run -op range -size 67108864 -range-size 8192 -workers 64 -duration 5s -count 8
run -op list -workers 64 -duration 5s -count 2000
# RESP (Redis/Valkey) cache protocol on the same listener (unpipelined and pipelined).
for op in resp-set resp-get; do
  run -op "$op" -size 4096 -workers 1 -pipeline 1 -duration 5s -count 2000
  run -op "$op" -size 4096 -workers 64 -pipeline 1 -duration 5s -count 2000
  run -op "$op" -size 4096 -workers 64 -pipeline 16 -duration 5s -count 2000
done
# OJS queue protocol: enqueue and fetch+ack on the same listener.
for w in 1 64; do
  run -op ojs-enqueue -size 4096 -workers "$w" -pipeline 1 -duration 5s -queue benchq
  run -op ojs-enqueue -size 4096 -workers 64 -pipeline 16 -duration 5s -queue benchq
done
for w in 1 32; do
  run -op ojs-fetch -size 4096 -workers "$w" -pipeline 1 -duration 5s -queue benchq
done

# S3 cache tier in front of a filesystem origin.
origin_dir="$PWD/.tmp/pgvs3/origin"
rm -rf "$origin_dir"; mkdir -p "$origin_dir"
cat > .tmp/pgvs3/origin.json <<JSON
{"type":"filesystem","config":{"directory":"$origin_dir"}}
JSON
read -r cport < <(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
PGVS3_URL="$url" PGVS3_POOL_MAX=64 PGVS3_LOG_LEVEL=warn \
  .tmp/pgvs3/pgvs3 serve --addr "127.0.0.1:$cport" --origin-config .tmp/pgvs3/origin.json \
  --cache-namespace loadcache --cache-ttl 5m > .tmp/pgvs3/load-origin.log 2>&1 &
cpid=$!
for i in $(seq 1 120); do
  curl -fsS --max-time 1 "http://127.0.0.1:$cport/healthz" >/dev/null 2>&1 && break
  [ "$i" = 120 ] && { cat .tmp/pgvs3/load-origin.log >&2; exit 1; }
  sleep 0.25
done
cache="http://127.0.0.1:$cport"
for size in 4096 67108864; do
  count=2000; [ "$size" = 67108864 ] && count=8
  for w in 1 16; do
    run_at "$cache" -op put -size "$size" -workers "$w" -duration 5s -count "$count"
    run_at "$cache" -op get -size "$size" -workers "$w" -duration 5s -count "$count"
  done
done
kill "$cpid" 2>/dev/null || true

echo "wrote $out" >&2
