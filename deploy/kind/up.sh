#!/usr/bin/env bash
# The kind stack: PostgreSQL and two pgvs3 gateways (DuckLake catalog + objects).
set -euo pipefail
cd "$(dirname "$0")/../.."

image=${1:?pass the pgvs3 image name}
cluster=pgvs3
namespace=pgvs3
kubectl=(kubectl --context "kind-$cluster")
helm=(helm --kube-context "kind-$cluster")

if ! kind get clusters | grep -Fxq "$cluster"; then
  kind create cluster --name "$cluster" --config deploy/kind/cluster.yaml
fi

# The tag does not change between builds. Load this checkout, then restart
# the pods: IfNotPresent keeps kind from trying to pull an unpublished image.
docker build -q -t "$image:latest" .
kind load docker-image "$image:latest" --name "$cluster"
"${kubectl[@]}" create namespace "$namespace" --dry-run=client -o yaml \
  | "${kubectl[@]}" apply -f -

# This key is local to this disposable kind namespace. Reuse it on redeploy;
# never put it in Helm release values, a ConfigMap or the repository.
if ! "${kubectl[@]}" -n "$namespace" get secret pgvs3-s3 >/dev/null 2>&1; then
  python3 -c 'import json,secrets; print(json.dumps({"apiVersion":"v1","kind":"Secret","metadata":{"name":"pgvs3-s3","namespace":"pgvs3"},"type":"Opaque","stringData":{"accessKey":"pgvs3-"+secrets.token_hex(8),"secretKey":secrets.token_hex(32)}}))' \
    | "${kubectl[@]}" -n "$namespace" apply -f - >/dev/null
fi
export PGVS3_ACCESS_KEY PGVS3_SECRET_KEY
PGVS3_ACCESS_KEY=$("${kubectl[@]}" -n "$namespace" get secret pgvs3-s3 -o jsonpath='{.data.accessKey}' | base64 --decode)
PGVS3_SECRET_KEY=$("${kubectl[@]}" -n "$namespace" get secret pgvs3-s3 -o jsonpath='{.data.secretKey}' | base64 --decode)

docker build -q -t pgvs3-postgres:18-cron deploy/postgres
kind load docker-image pgvs3-postgres:18-cron --name "$cluster"
"${helm[@]}" upgrade --install postgres deploy/charts/postgres --namespace "$namespace" \
  --reset-values --set "storage=${PG_STORAGE:-20Gi}"
"${kubectl[@]}" -n "$namespace" rollout status statefulset/postgres --timeout=300s
bash deploy/kind/db.sh
"${helm[@]}" upgrade --install pgvs3 deploy/charts/pgvs3 --namespace "$namespace" \
  --reset-values --set "image=$image:latest" --set allowHttp=true --set allowPlaintextDb=true
"${kubectl[@]}" -n "$namespace" rollout restart deployment/pgvs3
"${kubectl[@]}" -n "$namespace" rollout status deployment/pgvs3 --timeout=300s
"${kubectl[@]}" -n "$namespace" port-forward service/pgvs3 18016:8014 >/dev/null 2>&1 &
forward=$!
trap 'kill "$forward" 2>/dev/null || true' EXIT
for _ in $(seq 1 60); do
  if curl -fsS --max-time 1 http://127.0.0.1:18016/healthz >/dev/null 2>&1; then break; fi
  sleep 0.25
done
bash deploy/kind/buckets.sh http://127.0.0.1:18016 lake
kill "$forward" 2>/dev/null || true
wait "$forward" 2>/dev/null || true
trap - EXIT
echo 'kind up. next: just kind-validate && just kind-bench'
