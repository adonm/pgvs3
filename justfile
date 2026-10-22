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

# Image name for `just image`; CI may override it.
IMAGE := env("KO_DOCKER_REPO", "ghcr.io/adonm/pgvs3")

# ko builds a distroless static image directly from source: amd64 + arm64.
[group('image')]
image push="false":
    #!/usr/bin/env bash
    set -euo pipefail
    kwargs=(--bare --ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)")
    if [ "{{ push }}" = "true" ]; then
      KO_DOCKER_REPO="{{ IMAGE }}" mise exec go ko -- ko build "${kwargs[@]}" \
        --platform=linux/amd64,linux/arm64 --tags latest ./cmd/pgvs3
    else
      KO_DOCKER_REPO=ko.local mise exec go ko -- ko build "${kwargs[@]}" --local ./cmd/pgvs3
    fi

[group('ci')]
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    test -z "$(mise exec go -- gofmt -l cmd internal test)" || { mise exec go -- gofmt -l cmd internal test; exit 1; }
    mise exec go -- go vet ./...
    mise exec go -- go test ./...
    just contract
