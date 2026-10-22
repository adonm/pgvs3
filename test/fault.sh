#!/usr/bin/env bash
# Failure/churn checks. Requires the environment exported by stack.sh.
set -euo pipefail
: "${PGVS3_TEST_DB_URL:?}"
: "${PGVS3_TEST_ENDPOINT_A:?}"
: "${PGVS3_TEST_ENDPOINT_B:?}"
: "${PGVS3_ACCESS_KEY:?}"
: "${PGVS3_SECRET_KEY:?}"
: "${PGVS3_TEST_PID_A:?}"
: "${PGVS3_TEST_PID_B:?}"
: "${PGVS3_TEST_PG_CONTAINER:?}"
: "${PGVS3_TEST_PG_PORT:?}"

A=$PGVS3_TEST_ENDPOINT_A
B=$PGVS3_TEST_ENDPOINT_B
BUCKET=pgvs3-fault
RUN="fault-$(date +%s)-$$"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" "$@"; }
code() { s3 -o /dev/null -w '%{http_code}' "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
wait_gateway() {
  for _ in $(seq 1 480); do
    curl -fsS --max-time 1 "$1/readyz" >/dev/null 2>&1 && return 0
    sleep .25
  done
  return 1
}
wait_pg() {
  for _ in $(seq 1 480); do
    docker exec "$PGVS3_TEST_PG_CONTAINER" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return 0
    sleep .25
  done
  return 1
}

head -c 1048576 /dev/urandom > "$TMP/base"
s3 -X PUT --data-binary @"$TMP/base" "$A/$BUCKET/$RUN/base" >/dev/null
s3 "$B/$BUCKET/$RUN/base" -o "$TMP/base.out"
cmp "$TMP/base" "$TMP/base.out" || fail "cross-gateway baseline"

# Peer loss: the surviving gateway must keep serving from PostgreSQL.
kill "$PGVS3_TEST_PID_B"
sleep .5
s3 "$A/$BUCKET/$RUN/base" -o "$TMP/base.a"
cmp "$TMP/base" "$TMP/base.a" || fail "read after peer loss"

# PostgreSQL restart: the gateway must reconnect and publish nothing torn.
head -c 33554432 /dev/urandom > "$TMP/big"
( s3 -X PUT --data-binary @"$TMP/big" "$A/$BUCKET/$RUN/big" > "$TMP/big.code" 2>/dev/null || true ) &
putpid=$!
sleep .1
docker restart --time 1 "$PGVS3_TEST_PG_CONTAINER" >/dev/null
wait "$putpid" || true
wait_pg || fail "PostgreSQL did not return"
wait_gateway "$A" || fail "gateway did not return after PostgreSQL restart"
s3 "$A/$BUCKET/$RUN/base" -o "$TMP/base.after"
cmp "$TMP/base" "$TMP/base.after" || fail "read after PostgreSQL restart"
got=$(s3 -o /dev/null -w '%{http_code}' "$A/$BUCKET/$RUN/big")
[ "$got" = 200 ] || [ "$got" = 404 ] || fail "torn object state: HTTP $got"
if [ "$got" = 200 ]; then
  s3 "$A/$BUCKET/$RUN/big" -o "$TMP/big.out"
  cmp "$TMP/big" "$TMP/big.out" || fail "published big object is torn"
fi

# Churn: overwrite/delete many keys while maintenance runs.
for i in $(seq 1 200); do
  printf 'v%d' "$i" | s3 -X PUT --data-binary @- "$A/$BUCKET/$RUN/churn" >/dev/null
  if [ $((i % 7)) = 0 ]; then s3 -X DELETE "$A/$BUCKET/$RUN/churn" >/dev/null; fi
done
s3 "$A/$BUCKET/$RUN/churn" | grep -Eq '^v[0-9]+$' || fail "churn object corrupt"
echo "fault ok"
