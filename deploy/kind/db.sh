#!/usr/bin/env bash
# Idempotent kind database setup (or reset) through a psql Job.
set -euo pipefail
cd "$(dirname "$0")/../.."

phase=${1:-databases}
case "$phase" in databases|reset) ;; *) echo "invalid database phase: $phase" >&2; exit 2 ;; esac
job="pgvs3-db-$phase"
kubectl=(kubectl --context kind-pgvs3 -n pgvs3)
"${kubectl[@]}" delete job "$job" --ignore-not-found --wait=true >/dev/null
sed -e "s/__DB_SECRET__/postgres/g" -e "s/__JOB_NAME__/$job/g" \
  -e "s/__PHASE__/$phase/g" deploy/kind/db-job.yaml | "${kubectl[@]}" apply -f -

for _ in $(seq 1 90); do
  status=$("${kubectl[@]}" get job "$job" \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
  case "$status" in *Complete=True*|*Failed=True*|*FailureTarget=True*) break ;; esac
  sleep 2
done
"${kubectl[@]}" logs "job/$job"
if [[ "$status" != *Complete=True* ]]; then
  echo "database $phase failed or timed out: $status" >&2
  "${kubectl[@]}" describe job "$job" >&2
  exit 1
fi
