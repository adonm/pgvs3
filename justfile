# pgvs3 tasks. Toolchain: `mise install`.

URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_test"

default:
    @{{ just_executable() }} --list

[group('dev')]
setup:
    mise install
    mise exec go -- go mod download
    @echo "ready. next: just hooks && just lint"

[group('dev')]
hooks:
    #!/usr/bin/env bash
    set -euo pipefail
    if printf '2.54\n%s\n' "$(git version | awk '{print $3}')" | sort -V -C; then
      mise exec -- hk install --mise
    else
      mise exec -- hk install --mise --force-local
    fi

[group('dev')]
fix:
    mise exec -- hk fix --all

[group('test')]
lint:
    mise exec -- hk check --all

# PostgreSQL 18 in Docker on :5432 for running a gateway or tests by hand.
[group('dev')]
dev-db:
    #!/usr/bin/env bash
    set -euo pipefail
    if (echo > /dev/tcp/127.0.0.1/5432) 2>/dev/null; then
      echo "postgres already listening on :5432"
    elif docker inspect pgvs3-pg >/dev/null 2>&1; then
      docker start pgvs3-pg >/dev/null
    else
      docker run -d --name pgvs3-pg -p 127.0.0.1:5432:5432 \
        -e POSTGRES_PASSWORD=postgres postgres:18 \
        -c max_connections=256 -c shared_buffers=1GB
    fi
    for i in $(seq 1 30); do
      docker exec pgvs3-pg psql -U postgres -c 'SELECT 1' >/dev/null 2>&1 && break
      [ "$i" = 30 ] && { echo "postgres did not come up; try: just dev-db-clean && just dev-db"; docker logs --tail 5 pgvs3-pg; exit 1; }
      sleep 1
    done
    docker exec pgvs3-pg psql -U postgres -c "CREATE DATABASE pgvs3_test" 2>/dev/null || true
    echo "postgres up: {{ URL }}"

[group('dev')]
dev-db-clean:
    docker rm -f pgvs3-pg || true

[group('test')]
test:
    mise exec go -- go test ./...

[group('test')]
race:
    mise exec go -- go test -race ./...

[group('test')]
vet:
    mise exec go -- go vet ./...

# Integration suite against PostgreSQL 18, in-process fake S3 origin, and versitygw.
[group('test')]
integration:
    bash test/integration.sh

# Live smoke against real AWS S3 public RODA buckets (network-dependent; not in `just ci`).
[group('test')]
s3-public:
    bash test/s3-public.sh

[group('test')]
fuzz:
    mise exec go -- go test -fuzz=FuzzParseRange -fuzztime=10s ./internal/s3
    mise exec go -- go test -fuzz=FuzzChunkedBody -fuzztime=10s ./internal/s3

IMAGE := env("KO_DOCKER_REPO", "ghcr.io/adonm/pgvs3")
CHART_REPO := env("CHART_REPO", "oci://ghcr.io/adonm/charts")

[group('image')]
image push="false":
    #!/usr/bin/env bash
    set -euo pipefail
    ver=$(git describe --tags --always --dirty 2>/dev/null || echo dev)
    kwargs=(--bare --ldflags "-s -w -X main.version=$ver")
    if [ "{{ push }}" = "true" ]; then
      KO_DOCKER_REPO="{{ IMAGE }}" mise exec go ko -- ko build "${kwargs[@]}" \
        --platform=linux/amd64,linux/arm64 --tags "latest,$ver" ./cmd/pgvs3
    else
      KO_DOCKER_REPO=ko.local mise exec go ko -- ko build "${kwargs[@]}" --local ./cmd/pgvs3
    fi

[group('image')]
chart push="false":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .tmp/pgvs3
    args=(-d .tmp/pgvs3)
    if [ -n "${CHART_VERSION:-}" ]; then
      v="$CHART_VERSION"
      v="${v#v}"
      args+=(--version "$v" --app-version "v$v")
    fi
    mise exec helm -- helm lint deploy/charts/pgvs3
    pkg=$(mise exec helm -- helm package deploy/charts/pgvs3 "${args[@]}" | awk '{print $NF}')
    if [ "{{ push }}" = "true" ]; then
      mise exec helm -- helm push "$pkg" "{{ CHART_REPO }}"
    fi

[group('ci')]
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    mise exec -- hk check --all --profile ci
    mise exec go -- go vet -tags=integration ./...
    mise exec go -- go test -race ./cmd/pgvs3 ./internal/objcache ./internal/s3client
    GOFLAGS=-race just integration
    mise exec helm -- helm lint deploy/charts/pgvs3
    mise exec helm -- helm template pgvs3 deploy/charts/pgvs3 \
      --set autoscaling.enabled=true \
      --set serviceMonitor.enabled=true \
      --set networkPolicy.enabled=true \
      --set serviceAccount.create=true \
      --set serviceAccount.automountServiceAccountToken=true >/dev/null
