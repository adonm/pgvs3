#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=test/harness.sh
source "$(dirname "${BASH_SOURCE[0]}")/harness.sh"
test_run integration

if [ -z "${PGVS3_TEST_DATABASE_URL:-}" ]; then
  command -v docker >/dev/null 2>&1 || {
    echo "PGVS3_TEST_DATABASE_URL is unset and docker is unavailable" >&2
    exit 1
  }
  test_postgres 1GB
fi

if [ -z "${PGVS3_TEST_S3_ENDPOINT:-}" ]; then
  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  test_build_gateway
  test_start_gateway origin "$PGVS3_TEST_DATABASE_URL" "$port" --log-level error
  export PGVS3_TEST_S3_ACCESS_KEY=pgvs3
  export PGVS3_TEST_S3_SECRET_KEY=pgvs3-secret
  export PGVS3_TEST_S3_ENDPOINT="127.0.0.1:$port"
fi

mise exec go -- go test -count=1 -tags=integration -p 1 -timeout=20m ./internal/... \
  > "$PGVS3_TEST_RUN_DIR/tests.log" 2>&1
