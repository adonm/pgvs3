#!/usr/bin/env bash
# Installs the chart into a disposable kind cluster under the restricted Pod
# Security Standard, then checks the hardened pod spec, NetworkPolicy, probes,
# and a same-namespace S3 round trip.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cluster=${KIND_CLUSTER_NAME:-pgvs3-helm-$$}
app_ns=pgvs3-helm
db_ns=pgvs3-helm-db
release=pgvs3
tag=kind-test
image=""
tmp=$(mktemp -d)

kind() { mise exec kind -- kind "$@"; }
kubectl() { mise exec kubectl -- kubectl --context "kind-$cluster" "$@"; }
helm() { mise exec helm -- helm --kube-context "kind-$cluster" "$@"; }

cleanup() {
  status=$?
  if [ "$status" -ne 0 ] && kind get clusters 2>/dev/null | grep -qx "$cluster"; then
    echo "::group::kind diagnostics"
    kubectl get pods -A || true
    kubectl -n "$db_ns" describe deploy/postgres || true
    kubectl -n "$db_ns" logs deploy/postgres --tail=100 || true
    kubectl -n "$app_ns" describe deploy/pgvs3 || true
    kubectl -n "$app_ns" logs deploy/pgvs3 --tail=100 || true
    echo "::endgroup::"
  fi
  if kind get clusters 2>/dev/null | grep -qx "$cluster"; then
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
  exit "$status"
}

trap cleanup EXIT

echo "building gateway image"
image=$(cd "$ROOT" && KO_DOCKER_REPO=ko.local mise exec go ko -- ko build --local --tags "$tag" ./cmd/pgvs3 | tail -n1)
repo=${image%%@*}
repo=${repo%:*}
image="$repo:$tag"

echo "creating kind cluster $cluster"
kind create cluster --name "$cluster" --wait 180s
kind load docker-image "$image" --name "$cluster"

echo "starting PostgreSQL"
kubectl create namespace "$db_ns"
kubectl -n "$db_ns" apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  replicas: 1
  selector:
    matchLabels: { app: postgres }
  template:
    metadata:
      labels: { app: postgres }
    spec:
      securityContext:
        fsGroup: 999
      containers:
        - name: postgres
          image: postgres:18
          env:
            - { name: POSTGRES_PASSWORD, value: postgres }
          ports:
            - { containerPort: 5432 }
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            initialDelaySeconds: 2
            periodSeconds: 2
          volumeMounts:
            - { name: data, mountPath: /var/lib/postgresql }
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector: { app: postgres }
  ports:
    - { port: 5432, targetPort: 5432 }
YAML
kubectl -n "$db_ns" rollout status deploy/postgres --timeout=180s

echo "installing the chart under restricted Pod Security"
kubectl create namespace "$app_ns"
kubectl label namespace "$app_ns" \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/warn=restricted \
  pod-security.kubernetes.io/audit=restricted --overwrite
kubectl -n "$app_ns" create secret generic pgvs3-db \
  --from-literal=url="postgres://postgres:postgres@postgres.$db_ns.svc.cluster.local:5432/postgres?sslmode=disable"
helm install "$release" "$ROOT/deploy/charts/pgvs3" -n "$app_ns" \
  --set image="$image" \
  --set replicas=1 \
  --set topologySpread.enabled=false \
  --set podDisruptionBudget.enabled=false \
  --wait --timeout 5m
kubectl -n "$app_ns" rollout status deploy/pgvs3 --timeout=180s

echo "checking the rendered pod security and network policy"
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.securityContext.runAsNonRoot}')" = true
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.securityContext.runAsUser}')" = 65532
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.securityContext.fsGroup}')" = 65532
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.securityContext.seccompProfile.type}')" = RuntimeDefault
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.automountServiceAccountToken}')" = false
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}')" = 60
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.containers[0].securityContext.readOnlyRootFilesystem}')" = true
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.containers[0].securityContext.allowPrivilegeEscalation}')" = false
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.containers[0].securityContext.capabilities.drop[0]}')" = ALL
test "$(kubectl -n "$app_ns" get deploy pgvs3 -o jsonpath='{.spec.template.spec.containers[0].volumeMounts[0].mountPath}')" = /tmp
test "$(kubectl -n "$app_ns" get networkpolicy pgvs3 -o jsonpath='{.spec.podSelector.matchLabels.app}')" = pgvs3

echo "checking ServiceMonitor rendering"
mise exec helm -- helm template "$release" "$ROOT/deploy/charts/pgvs3" \
  --set serviceMonitor.enabled=true | grep -q 'kind: ServiceMonitor'

echo "checking a same-namespace S3 round trip"
kubectl -n "$app_ns" apply -f - <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: pgvs3-check
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: curl
      image: curlimages/curl:8.10.1
      command: ["sh", "-c", "while true; do sleep 3600; done"]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: { drop: ["ALL"] }
YAML
kubectl -n "$app_ns" wait --for=condition=Ready pod/pgvs3-check --timeout=120s
code=$(kubectl -n "$app_ns" exec pgvs3-check -- curl -sS -o /dev/null -w '%{http_code}' http://pgvs3:8014/readyz)
test "$code" = 200
kubectl -n "$app_ns" exec pgvs3-check -- curl -fsS -X PUT --data-binary 'kind' http://pgvs3:8014/kind-bucket/kind.txt
body=$(kubectl -n "$app_ns" exec pgvs3-check -- curl -fsS http://pgvs3:8014/kind-bucket/kind.txt)
test "$body" = kind
echo "helm-kind: ok"
