#!/usr/bin/env bash
# Runs the Go integration tests against PostgreSQL. Uses
# PGVS3_TEST_DATABASE_URL when set (CI); otherwise starts a disposable
# PostgreSQL 18 container with the Docker CLI.
set -euo pipefail

if [ -z "${PGVS3_TEST_DATABASE_URL:-}" ]; then
  command -v docker >/dev/null 2>&1 || {
    echo "PGVS3_TEST_DATABASE_URL is unset and docker is unavailable" >&2
    exit 1
  }
  name=pgvs3-integration-$$
  cleanup() { docker rm -fv "$name" >/dev/null 2>&1 || true; }
  trap cleanup EXIT
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

exec mise exec go -- go test -tags=integration -timeout=20m ./internal/...
