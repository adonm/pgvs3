#!/usr/bin/env bash
# Runs the pinned s3t (Go port of ceph/s3-tests) allow-list against a running
# pgvs3 gateway. Much faster than the Python suite; intended as the nightly
# fast gate. The gateway must be reachable at PGVS3_S3_HOST:PGVS3_S3_PORT.
set -euo pipefail

HOST=${PGVS3_S3_HOST:-127.0.0.1}
PORT=${PGVS3_S3_PORT:-8014}
COMMIT=${S3T_COMMIT:-51506ac904f6e35424b3ec9d38716985023beba6}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

sed -e "s/@HOST@/$HOST/" -e "s/@PORT@/$PORT/" "$ROOT/test/s3t.conf.tmpl" > "$TMP/s3tests.conf"

GOBIN="$TMP/bin" mise exec go -- go install "github.com/go-faster/s3t/cmd/s3t@$COMMIT"
"$TMP/bin/s3t" run -c "$TMP/s3tests.conf" --allow-list "$ROOT/test/s3t-allow.txt" \
  -p 8 --timeout 2m
