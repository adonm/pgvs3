# pgvs3 tasks. Toolchain: `mise install`. Tests and benchmarks start their own
# disposable PostgreSQL in Docker and two local gateways (deploy/bench/stack.sh).

URL := "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"
default:
    @{{ just_executable() }} --list

[group('dev')]
setup:
    mise install
    mise exec go -- go mod download
    @echo "ready. next: just hooks && just lint"

# Install the hk Git hooks for this clone (uses the mise tool environment).
# Git <2.54 cannot execute global config hooks, so force a local install when
# hk sees an inert global configuration.
[group('dev')]
hooks:
    #!/usr/bin/env bash
    set -euo pipefail
    if printf '2.54\n%s\n' "$(git version | awk '{print $3}')" | sort -V -C; then
      mise exec -- hk install --mise
    else
      mise exec -- hk install --mise --force-local
    fi

# Format and apply safe modern Go fixes to the working tree.
[group('dev')]
fix:
    mise exec -- hk fix --all

# Full hk check: formatting, imports, go fix, tidy, vet and staticcheck.
[group('test')]
lint:
    mise exec -- hk check --all

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

# Store/queue/cache integration tests against PostgreSQL and a self-hosted S3 origin.
[group('test')]
integration:
    bash test/integration.sh

[group('test')]
s3t:
    bash test/s3t.sh

# Install the chart into a throwaway kind cluster under restricted Pod Security.
[group('test')]
helm-kind:
    bash test/helm-kind.sh

[group('test')]
interop:
    bash deploy/bench/stack.sh interop

[group('test')]
fault:
    bash deploy/bench/stack.sh fault

[group('test')]
fuzz:
    mise exec go -- go test -fuzz=FuzzParseRange -fuzztime=10s ./internal/s3
    mise exec go -- go test -fuzz=FuzzChunkedBody -fuzztime=10s ./internal/s3
    mise exec go -- go test -fuzz=FuzzRowFramer -fuzztime=10s ./internal/store
    mise exec go -- go test -fuzz=FuzzBlockCodec -fuzztime=10s ./internal/store

[group('test')]
vet:
    mise exec go -- go vet ./...

# End-to-end S3/OJS/DuckDB/DuckLake contract through two gateways.
[group('test')]
contract:
    bash deploy/bench/stack.sh contract

# Maintained README Parquet/DuckLake walkthrough in native and origin-backed roles.
[group('dev')]
analytics:
    bash deploy/bench/stack.sh analytics

# RaQuet queries on a real LINZ orthophoto in native, origin-cache and write-through
# roles. Downloads and converts once into ~/.cache/pgvs3/ortho; PGVS3_ORTHO_SHEETS=4
# (1..16 sheets per edge, about 1.1 GB at 4 and 4.5 GB at 8).
[group('dev')]
ortho:
    PGVS3_ANALYTICS=ortho bash deploy/bench/stack.sh analytics

# Iceberg (pyiceberg writer, DuckDB reader) straight from an origin with injected
# latency versus through the cache tier. PGVS3_ICEBERG_COMMITS=100 snapshots,
# PGVS3_ICEBERG_ORIGIN_MS=20 per origin request, PGVS3_ICEBERG_REPEAT=3.
[group('dev')]
iceberg:
    PGVS3_ANALYTICS=iceberg bash deploy/bench/stack.sh analytics

# Anonymous Overture Maps release + STAC discovery: direct us-west-2 S3, cold
# 1 MiB shared range fill, warm PostgreSQL s3c_, then a warm 256 MiB gateway
# block cache. PGVS3_OVERTURE_RELEASE, PGVS3_OVERTURE_DIRECT_REPEAT=3 and
# PGVS3_OVERTURE_REPEAT=5. Reads only the Perth CBD row groups, not the files.
[group('bench')]
overture:
    PGVS3_ANALYTICS=overture bash deploy/bench/stack.sh analytics

# Trino 484 + Nessie 0.109 (JDBC version store in the same PostgreSQL) on TPC-H
# lineitem committed month by month plus one large-file CTAS: direct origin,
# cache tier, Trino's Alluxio cache, then a one-snapshot rebuild and sorted
# layout. Cache gateways read shared PostgreSQL chunks by default
# (PGVS3_READ_CACHE_BYTES=0); set 3221225472 for the optional 3 GiB
# gateway-local cache used by the 2026-11-03 historical and 2026-11-05 paired tables.
# PGVS3_TRINO_ORIGIN_MS=20, PGVS3_TRINO_REPEAT=3, PGVS3_TRINO_BIG_SCALE=sf10,
# PGVS3_TRINO_RANGE_CHUNK=1048576, PGVS3_TRINO_FILL_CONCURRENCY (per tier),
# PGVS3_POOL_MAX=8 (cache gateways). PGVS3_TRINO_PAIRED_CYCLES=15 enables the
# ABBA paired warm runs; PGVS3_TEST_PPROF_A plus PGVS3_TRINO_PROFILE=1 opts into
# the separate profile diagnostic.
# Pulls two container images.
[group('dev')]
trino:
    PGVS3_READ_CACHE_BYTES=${PGVS3_READ_CACHE_BYTES:-0} PGVS3_ANALYTICS=trino bash deploy/bench/stack.sh analytics

# Bound SQL/API workload recipes and failure windows in the existing disposable fixture.
[group('test')]
guides:
    bash deploy/bench/stack.sh guides

# Render the Quarto documentation site and static SVG figures into docs/_site.
[group('dev')]
docs:
    mise exec github:quarto-dev/quarto-cli -- quarto render docs
    touch docs/_site/.nojekyll

# Retired: docs deploy automatically from main after CI; use `just docs` locally.
[group('dev')]
docs-publish:
    @echo "Docs deploy automatically from main after CI; run just docs locally, then commit/push changes." >&2
    @exit 1

# DuckLake compatibility: writer on one gateway, reader on the other.
[group('test')]
ducklake:
    bash deploy/bench/stack.sh bench compat

# DuckDB S3 benchmark, DuckLake compatibility and row-group tuning.
[group('bench')]
[positional-arguments]
bench parts="s3,compat,tuning" *args:
    bash deploy/bench/stack.sh bench "$@"

# Cache the pinned experimental Differential release; no global DuckDB INSTALL.
[group('dev')]
[positional-arguments]
differential-setup *args:
    mise exec python -- python deploy/bench/differential.py setup "$@"

# Compare Differential snapshot/sync-WAL profiles with DuckLake on disposable pgvs3.
[group('bench')]
[positional-arguments]
differential *args:
    bash deploy/bench/stack.sh bench differential "$@"

# Cache the signed, checksum-pinned SlateDB object-storage extension for DuckDB 2.0.
[group('dev')]
[positional-arguments]
object-storage-setup *args:
    mise exec uv -- uv run --quiet --no-project --with duckdb==2.0.0 python deploy/bench/object_storage.py setup "$@"

# Compare ObjFS defaults and bounded cache/thread budgets with DuckLake.
[group('bench')]
[positional-arguments]
object-storage *args:
    bash deploy/bench/stack.sh bench object-storage "$@"

# Direct gateway benchmark: small/large PUT/GET, ranges, listings and OJS queues.
[group('bench')]
[positional-arguments]
load *args:
    bash deploy/bench/load.sh "$@"

# Regenerate docs/reference.md: CLI flags from the binary, metrics from a live contract scrape.
[group('dev')]
reference:
    mise exec go -- go test ./cmd/pgvs3 -run '^TestReferenceMatchesFlags$' -update
    PGVS3_UPDATE_REFERENCE=1 bash deploy/bench/stack.sh contract

# Speed-of-light table: pgvs3 PUT/GET/range vs loopback HTTP and direct PostgreSQL floors.
[group('bench')]
[positional-arguments]
sol *args:
    bash deploy/bench/stack.sh sol "$@"

# Opt-in transport comparison; SDK prototypes never enter the gateway binary.
[group('bench')]
s3-clients:
    cd test/s3clients && mise exec go -- go test -run '^$' -bench '^BenchmarkS3OriginClients$' -benchtime=250ms -count=5

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

# Pinned DuckDB 2.0.0 + nine signed extensions + native Quack.
DUCKDB_IMAGE := env("DUCKDB_IMAGE", "pgvs3-duckdb:2.0.0")
DUCKDB_IMAGE_REPO := env("DUCKDB_IMAGE_REPO", "ghcr.io/adonm/pgvs3-duckdb")

# Fetch/verify the pinned artifacts, build and check the amd64 image, and
# optionally push it to GHCR.
[group('image')]
duckdb-image push="false":
    #!/usr/bin/env bash
    set -euo pipefail
    python3 deploy/duckdb/fetch_artifacts.py
    docker build --platform linux/amd64 -t "{{ DUCKDB_IMAGE }}" deploy/duckdb
    python3 deploy/duckdb/image_tools.py check "{{ DUCKDB_IMAGE }}"
    if [ "{{ push }}" = "true" ]; then
      docker tag "{{ DUCKDB_IMAGE }}" "{{ DUCKDB_IMAGE_REPO }}:2.0.0"
      docker tag "{{ DUCKDB_IMAGE }}" "{{ DUCKDB_IMAGE_REPO }}:latest"
      docker push "{{ DUCKDB_IMAGE_REPO }}:2.0.0"
      docker push "{{ DUCKDB_IMAGE_REPO }}:latest"
    fi

# Offline extension/entrypoint check in the non-root, read-only-root image.
[group('test')]
duckdb-check:
    python3 deploy/duckdb/image_tools.py check "{{ DUCKDB_IMAGE }}"

# Host-driven local no-PG/no-S3 functional smoke and illustrative timings.
[group('test')]
duckdb-smoke:
    python3 deploy/duckdb/smoke.py "{{ DUCKDB_IMAGE }}"

# One-shot PG/S3 DuckLake attach without starting Quack (next-phase integration).
[group('test')]
duckdb-no-daemon:
    docker run --rm --read-only --tmpfs /tmp:rw,size=1g \
      -e PGVS3_URL -e PGVS3_BUCKET -e PGVS3_PREFIX -e PGVS3_ENDPOINT \
      -e PGVS3_S3_USE_SSL -e PGVS3_S3_ACCESS_KEY -e PGVS3_S3_SECRET_KEY \
      --entrypoint /duckdb "{{ DUCKDB_IMAGE }}" \
      -no-agent -batch -bail -init /opt/pgvs3-duckdb/lake-only.sql

# Functional compute image on the disposable pgvs3 stack: same PG database,
# native S3 bucket through gateways A/B, native Quack and a fresh-container restart.
[group('test')]
duckdb-integration:
    PGVS3_ANALYTICS=compute bash deploy/bench/stack.sh analytics

# Opt-in small spatial/vector/FTS measurements in the same compute fixture.
[group('test')]
duckdb-integration-bench:
    PGVS3_DUCKDB_BENCH=1 PGVS3_ANALYTICS=compute bash deploy/bench/stack.sh analytics

# C clients x W native Quack workers against DuckLake, a native RTree and one
# cache-controlled gateway; writes quack_pool.json into the kept run directory.
[group('bench')]
duckdb-pool-bench:
    bash deploy/bench/stack.sh pool

# Focused host-side stdlib tests for the native SQL/artifact lock and the
# compute helper contracts; no Docker/PG/S3 runtime work.
[group('test')]
duckdb-test:
    python3 -m unittest discover -s deploy/duckdb -p '*_test.py'
    mise exec python -- python -m unittest discover -s deploy/bench -p 'data_plane_test.py'

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
    mise exec -- hk check --all --profile ci
    mise exec go -- go vet -tags=integration ./...
    mise exec go -- go test -race ./...
    mise exec python -- python -m unittest discover -s deploy/bench -p '*_test.py'
    mise exec python -- python -m unittest discover -s deploy/duckdb -p '*_test.py'
    GOFLAGS=-race just integration
    mise exec go -- go test -fuzz=FuzzParseRange -fuzztime=5s ./internal/s3
    mise exec go -- go test -fuzz=FuzzChunkedBody -fuzztime=5s ./internal/s3
    mise exec go -- go test -fuzz=FuzzRowFramer -fuzztime=5s ./internal/store
    mise exec go -- go test -fuzz=FuzzBlockCodec -fuzztime=5s ./internal/store
    mise exec helm -- helm lint deploy/charts/pgvs3
    just helm-kind
    just contract
    just fault
    just interop
    bash deploy/bench/stack.sh s3t
