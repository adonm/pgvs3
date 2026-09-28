#!/usr/bin/env bash
# Pre-release test data only. Stop writers before replacing the test
# databases, then use kind-up to redeploy them from scratch.
set -euo pipefail
cd "$(dirname "$0")/../.."

kubectl=(kubectl --context kind-pgvs3 -n pgvs3)
active=$("${kubectl[@]}" get jobs -o json | python3 -c '
import json, sys
print(sum(job.get("status", {}).get("active", 0) for job in json.load(sys.stdin)["items"]))
')
[ "$active" = 0 ] || { echo "$active benchmark/setup Jobs still running; refusing reset" >&2; exit 1; }
if "${kubectl[@]}" get deployment pgvs3 >/dev/null 2>&1; then
  "${kubectl[@]}" scale deployment/pgvs3 --replicas=0
fi
if [ -n "$("${kubectl[@]}" get pods -l app=pgvs3 -o name)" ]; then
  "${kubectl[@]}" wait --for=delete pod -l app=pgvs3 --timeout=180s
fi
bash deploy/kind/db.sh reset
echo 'test databases reset; run just kind-up to bring the stack back'
