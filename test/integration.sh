#!/usr/bin/env bash
# Runs the Go integration tests against PostgreSQL and an S3 origin. The
# origin defaults to a second pgvs3 gateway over the same database; set
# PGVS3_TEST_S3_ENDPOINT/ACCESS_KEY/SECRET_KEY to use an external origin.
# PostgreSQL starts in Docker when PGVS3_TEST_DATABASE_URL is unset.
set -euo pipefail

containers=()
origin_pid=""
origin_dir=""
cleanup() {
  if [ -n "$origin_pid" ]; then
    kill "$origin_pid" 2>/dev/null || true
    wait "$origin_pid" 2>/dev/null || true
  fi
  if [ -n "$origin_dir" ]; then
    rm -rf "$origin_dir"
  fi
  if [ "${#containers[@]}" -gt 0 ]; then
    docker rm -fv "${containers[@]}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ -z "${PGVS3_TEST_DATABASE_URL:-}" ]; then
  command -v docker >/dev/null 2>&1 || {
    echo "PGVS3_TEST_DATABASE_URL is unset and docker is unavailable" >&2
    exit 1
  }
  name=pgvs3-integration-pg-$$
  containers+=("$name")
  docker run -d --name "$name" -e POSTGRES_PASSWORD=postgres \
    -p 127.0.0.1::5432 postgres:18 \
    -c max_connections=256 -c shared_buffers=1GB >/dev/null
  for i in $(seq 1 120); do
    docker exec "$name" pg_isready -U postgres >/dev/null 2>&1 && break
    [ "$i" = 120 ] && { docker logs "$name" >&2; exit 1; }
    sleep 0.5
  done
  pg=$(docker port "$name" 5432/tcp)
  export PGVS3_TEST_DATABASE_URL="postgres://postgres:postgres@$pg/postgres?sslmode=disable"
fi

if [ -z "${PGVS3_TEST_S3_ENDPOINT:-}" ]; then
  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  origin_dir=$(mktemp -d)
  origin_log="$origin_dir/origin.log"
  mise exec go -- go build -o "$origin_dir/pgvs3" ./cmd/pgvs3
  "$origin_dir/pgvs3" --url "$PGVS3_TEST_DATABASE_URL" serve \
    --addr "127.0.0.1:$port" --log-level error >"$origin_log" 2>&1 &
  origin_pid=$!
  export PGVS3_TEST_S3_ACCESS_KEY=pgvs3
  export PGVS3_TEST_S3_SECRET_KEY=pgvs3-secret
  export PGVS3_TEST_S3_ENDPOINT="127.0.0.1:$port"
  for i in $(seq 1 120); do
    curl -fsS "http://$PGVS3_TEST_S3_ENDPOINT/healthz" >/dev/null 2>&1 && break
    if ! kill -0 "$origin_pid" 2>/dev/null; then
      cat "$origin_log" >&2
      exit 1
    fi
    [ "$i" = 120 ] && { cat "$origin_log" >&2; exit 1; }
    sleep 0.5
  done
fi

mise exec go -- go test -tags=integration -p 1 -timeout=20m ./internal/...
