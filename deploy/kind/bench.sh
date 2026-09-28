#!/usr/bin/env bash
# Build the benchmark image once, then run each Job in turn against the same
# kind stack. A failed Job must fail the recipe (and therefore CI).
set -euo pipefail
cd "$(dirname "$0")/../.."

cluster=pgvs3
namespace=pgvs3
kubectl=(kubectl --context "kind-$cluster" -n "$namespace")
helm=(helm --kube-context "kind-$cluster")
"${kubectl[@]}" get namespace "$namespace" >/dev/null

docker build -q -t kind-bench:latest --build-arg "DUCKDB_PY=${DUCKDB_PY:?run with mise}" -f deploy/bench/Dockerfile .
kind load docker-image kind-bench:latest --name "$cluster"

suites=${SUITES:-click,spatial}
env_args=()
full=0
if [[ "${QUICK:-0}" != 1 && "${QUICK:-0}" != true ]]; then
  full=1
  DUCKDB_MEMORY_LIMIT=${DUCKDB_MEMORY_LIMIT:-16GiB}
fi
for key in QUICK BENCH_REUSE PASSES PARTS SPATIAL_SF SPATIAL_QUERIES \
           SPATIAL_QUERY_TIMEOUT DUCKDB_MEMORY_LIMIT; do
  if [ -n "${!key:-}" ]; then
    value=${!key}
    # Helm's --set-string treats unescaped commas as value separators.
    value=${value//,/\\,}
    env_args+=(--set-string "suiteEnv.$key=$value")
  fi
done

mkdir -p .tmp/pgvs3/jobs
results=.tmp/pgvs3/kind-bench.jsonl
: > "$results"
wait_s=7200
case "${QUICK:-}" in 1 | true) wait_s=300 ;; esac
if [ -n "${BENCH_WAIT_S:-}" ]; then
  [[ "$BENCH_WAIT_S" =~ ^[1-9][0-9]*$ ]] || { echo 'BENCH_WAIT_S must be positive' >&2; exit 2; }
  wait_s=$BENCH_WAIT_S
fi

for suite in ${suites//,/ }; do
  case "$suite" in
    validate|click|spatial) ;;
    *) echo "unknown suite: $suite" >&2; exit 2 ;;
  esac
done

job=""
cleanup() {
  if [ -n "$job" ]; then
    "${kubectl[@]}" delete job "$job" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
# Bash skips EXIT traps when killed by a signal; exit normally instead.
trap 'exit 130' INT
trap 'exit 143' TERM HUP

for suite in ${suites//,/ }; do
  echo "=== $suite ==="
  resource_args=()
  if [ "$full" = 1 ] && [ "$suite" != validate ]; then
    resource_args+=(--set "resources.limits.memory=24Gi")
  fi
  # A Job's pod template is immutable, so each new image needs a new Job.
  "${kubectl[@]}" delete job "bench-$suite" --ignore-not-found --wait=true >/dev/null
  job="bench-$suite"
  "${helm[@]}" upgrade --install kind-bench deploy/charts/kind-bench \
    --namespace "$namespace" --reset-values --set image=kind-bench:latest \
    "${env_args[@]}" "${resource_args[@]}" --set "suite=$suite"

  waited=0
  while :; do
    status=$("${kubectl[@]}" get job "bench-$suite" \
      -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
    case "$status" in *Complete=True*|*Failed=True*|*FailureTarget=True*) break ;; esac
    if [ "$waited" -ge "$wait_s" ]; then
      echo "TIMEOUT $suite after ${wait_s}s" >&2
      "${kubectl[@]}" describe job "bench-$suite" >&2
      exit 1
    fi
    sleep 2; waited=$((waited + 2))
  done

  log=".tmp/pgvs3/jobs/$suite.log"
  "${kubectl[@]}" logs "job/bench-$suite" > "$log"
  job=""
  if [[ "$status" != *Complete=True* ]]; then
    tail -50 "$log" | cut -c1-300 >&2
    echo "FAILED $suite: $status" >&2
    exit 1
  fi
  # Full query answers stay in the local JSONL for correctness checking; do
  # not dump large result rows into the operator's terminal on every run.
  grep -v '^{' "$log" || true
  python3 - "$log" <<'PY'
import json,sys
for line in open(sys.argv[1]):
    if not line.startswith('{'):
        continue
    try:
        record=json.loads(line)
    except ValueError:
        continue
    if 'suite' in record:
        record.pop('answers', None)
        record.pop('operations', None)
        record.pop('passes', None)
        print(json.dumps(record, ensure_ascii=True))
PY
  if [ "$suite" != validate ] && ! grep -q '^{.*}$' "$log"; then
    echo "FAILED $suite: no result JSON" >&2
    exit 1
  fi
  grep '^{.*}$' "$log" >> "$results" || true
done

echo "results: $results"
