#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=test/harness.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../test/harness.sh"

before_bin="" after_bin="" profile=native range_chunk=65536 out=""
comparison=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --before-bin) before_bin=$(realpath "${2:?binary required}"); shift 2 ;;
    --after-bin) after_bin=$(realpath "${2:?binary required}"); shift 2 ;;
    --profile) profile=${2:?profile required}; shift 2 ;;
    --range-chunk) range_chunk=${2:?chunk size required}; shift 2 ;;
    --out) out=$(realpath -m "${2:?output required}"); shift 2 ;;
    --help)
      echo 'load.sh [--before-bin BINARY] [--after-bin BINARY] [--profile native|origin] [--out JSON] [compare.py options]'
      echo 'Without --before-bin, run the existing S3/RESP/OJS load matrix. Paired runs build the current gateway when --after-bin is omitted.'
      exit 0 ;;
    *) comparison+=("$1"); shift ;;
  esac
done
case "$profile" in native|origin) ;; *) echo 'profile must be native or origin' >&2; exit 2 ;; esac
if [ -n "$after_bin" ] && [ -z "$before_bin" ]; then echo '--after-bin requires --before-bin' >&2; exit 2; fi
if [ -n "$out" ] && [ -e "$out" ]; then echo "output already exists: $out" >&2; exit 2; fi
if [ -z "$before_bin" ] && [ "${#comparison[@]}" -gt 0 ]; then echo 'comparison options require --before-bin' >&2; exit 2; fi
if [ -z "$before_bin" ] && [ "$profile" != native ]; then echo '--profile requires --before-bin' >&2; exit 2; fi

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
test_run load
test_keep_artifacts=true
test_postgres 1GB
test_build_gateway
mise exec go -- go build -o "$PGVS3_TEST_RUN_DIR/loadgen" ./test/loadgen > "$PGVS3_TEST_RUN_DIR/loadgen-build.log" 2>&1

if [ -n "$before_bin" ]; then
  cp "$before_bin" "$PGVS3_TEST_RUN_DIR/before"
  cp "${after_bin:-$PGVS3_TEST_BIN}" "$PGVS3_TEST_RUN_DIR/after"
  cat > "$PGVS3_TEST_RUN_DIR/psql" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
database=${1%%\?*}
database=${database##*/}
shift
exec docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d "$database" "$@"
SH
  chmod +x "$PGVS3_TEST_RUN_DIR/psql"
  endpoints=() databases=() directories=()
  for side in before after; do
    docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $side" >> "$PGVS3_TEST_RUN_DIR/postgres-start.log" 2>&1
    url="postgres://postgres:postgres@127.0.0.1:$PGVS3_TEST_PG_PORT/$side?sslmode=disable"
    port=$(free_port)
    PGVS3_TEST_BIN="$PGVS3_TEST_RUN_DIR/$side" PGVS3_POOL_MAX=32 test_start_gateway "$side" "$url" "$port" --log-level warn
    endpoints+=("http://127.0.0.1:$port")
    databases+=("$url")
    if [ "$profile" = origin ]; then
      directory="$PGVS3_TEST_RUN_DIR/$side-origin"
      directories+=("$directory")
      mkdir -p "$directory"
      python3 - "$directory" > "$PGVS3_TEST_RUN_DIR/$side.json" <<'PY'
import json, sys
print(json.dumps({"type": "filesystem", "config": {"directory": sys.argv[1]}}))
PY
      "$PGVS3_TEST_RUN_DIR/$side" origin --url "$url" --bucket bench --cache-namespace benchmark-origin \
        --cache-ttl 1h --range-cache-chunk "$range_chunk" set "$PGVS3_TEST_RUN_DIR/$side.json" > "$PGVS3_TEST_RUN_DIR/spec-$side.log" 2>&1
    fi
  done
  origin_args=()
  if [ "$profile" = origin ]; then origin_args=(--origin-directories "${directories[@]}" --origin-namespace benchmark-origin --range-chunk "$range_chunk"); fi
  out=${out:-$PGVS3_TEST_RUN_DIR/paired.json}
  python3 deploy/bench/compare.py "${comparison[@]}" "${origin_args[@]}" \
    --before "${endpoints[0]}" --after "${endpoints[1]}" --loadgen "$PGVS3_TEST_RUN_DIR/loadgen" \
    --before-binary "$PGVS3_TEST_RUN_DIR/before" --after-binary "$PGVS3_TEST_RUN_DIR/after" \
    --postgres-before "${databases[0]}" --postgres-after "${databases[1]}" --psql "$PGVS3_TEST_RUN_DIR/psql" --out "$out" \
    | tee "$PGVS3_TEST_RUN_DIR/comparison.log"
  exit 0
fi

docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -v ON_ERROR_STOP=1 -c 'CREATE DATABASE pgvs3' >> "$PGVS3_TEST_RUN_DIR/postgres-start.log" 2>&1
url="postgres://postgres:postgres@127.0.0.1:$PGVS3_TEST_PG_PORT/pgvs3?sslmode=disable"
port=$(free_port)
PGVS3_POOL_MAX=128 test_start_gateway native "$url" "$port" --log-level warn
out=${out:-$PGVS3_TEST_RUN_DIR/load.json}
mkdir -p "$(dirname "$out")"
run_at() {
  local endpoint=$1; shift
  "$PGVS3_TEST_RUN_DIR/loadgen" -endpoint "$endpoint" -bucket bench -unsigned "$@" -duration "${PGVS3_BENCH_DURATION:-5s}" | python3 -c '
import json, sys
r=json.load(sys.stdin)
with open(sys.argv[1], "a") as output: output.write(json.dumps(r, separators=(",", ":"))+"\n")
print(f"{r['\''op'\'']} {r['\''size'\'']}B c={r['\''workers'\'']}: {r['\''p50_ms'\'']:.3f} ms, {r['\''requests_per_s'\'']:.1f}/s, errors={r['\''errors'\'']}")
' "$out"
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
origin_dir="$PGVS3_TEST_RUN_DIR/origin"
mkdir -p "$origin_dir"
cat > "$PGVS3_TEST_RUN_DIR/origin.json" <<JSON
{"type":"filesystem","config":{"directory":"$origin_dir"}}
JSON
cport=$(free_port)
PGVS3_POOL_MAX=64 test_start_gateway origin "$url" "$cport" --origin-config "$PGVS3_TEST_RUN_DIR/origin.json" --cache-namespace loadcache --cache-ttl 5m --log-level warn
cache="http://127.0.0.1:$cport"
for size in 4096 67108864; do
  count=2000; [ "$size" = 67108864 ] && count=8
  for w in 1 16; do
    run_at "$cache" -op put -size "$size" -workers "$w" -duration 5s -count "$count"
    run_at "$cache" -op get -size "$size" -workers "$w" -duration 5s -count "$count"
  done
done
echo "matrix completed; inspect errors in $out (use --before-bin for gated comparisons)" >&2
