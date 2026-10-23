# pgvs3 tasks. Toolchain: `mise install`. Tests and benchmarks start their own
# disposable PostgreSQL in Docker and two local gateways (deploy/bench/stack.sh).

URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"
default:
    @{{ just_executable() }} --list

[group('dev')]
setup:
    mise install
    mise exec go -- go mod download
    @echo "ready. next: just contract"

# PostgreSQL 18 in Docker on :5432 for running a gateway by hand.
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
    for db in pgvs3_bench ducklake_catalog; do
      docker exec pgvs3-pg psql -U postgres -c "CREATE DATABASE $db" 2>/dev/null || true
    done
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
integration:
    mise exec go -- go test -tags=integration -timeout=15m ./internal/...

[group('test')]
s3t:
    bash test/s3t.sh

[group('test')]
interop:
    bash deploy/bench/stack.sh interop

[group('test')]
fault:
    bash deploy/bench/stack.sh fault

[group('test')]
fuzz:
    mise exec go -- go test -fuzz=FuzzParseRange -fuzztime=10s ./internal/s3
    mise exec go -- go test -fuzz=FuzzRowFramer -fuzztime=10s ./internal/store
    mise exec go -- go test -fuzz=FuzzBlockCodec -fuzztime=10s ./internal/store

[group('test')]
vet:
    mise exec go -- go vet ./...

# End-to-end S3/DuckDB/DuckLake contract through two gateways.
[group('test')]
contract:
    bash deploy/bench/stack.sh contract

# DuckLake compatibility: writer on one gateway, reader on the other.
[group('test')]
ducklake:
    bash deploy/bench/stack.sh bench compat

# DuckDB S3 benchmark, DuckLake compatibility and row-group tuning.
[group('bench')]
bench parts="s3,compat,tuning":
    bash deploy/bench/stack.sh bench '{{ parts }}'

# Direct gateway benchmark: small/large PUT/GET, ranges and listings.
[group('bench')]
load:
    bash deploy/bench/load.sh

# Image and Helm OCI repositories for `just image` and `just chart`.
IMAGE := env("KO_DOCKER_REPO", "ghcr.io/adonm/pgvs3")
CHART_REPO := env("CHART_REPO", "oci://ghcr.io/adonm/charts")

# Local compose stack (build the image first with `just image`).
[group('dev')]
compose:
    docker compose -f deploy/compose.yaml up

# ko builds a distroless static image directly from source: amd64 + arm64.
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

# Lint, package and optionally push the OCI Helm chart to GHCR.
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
    test -z "$(mise exec go -- gofmt -l cmd internal test)" || { mise exec go -- gofmt -l cmd internal test; exit 1; }
    mise exec go -- go vet ./...
    mise exec go -- go test -race ./...
    mise exec go -- go test -tags=integration -timeout=15m ./internal/...
    mise exec go -- go test -fuzz=FuzzParseRange -fuzztime=5s ./internal/s3
    mise exec go -- go test -fuzz=FuzzRowFramer -fuzztime=5s ./internal/store
    mise exec go -- go test -fuzz=FuzzBlockCodec -fuzztime=5s ./internal/store
    mise exec helm -- helm lint deploy/charts/pgvs3
    just contract
