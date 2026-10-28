#!/usr/bin/env bash
# Failure/churn checks. Requires the environment exported by stack.sh.
set -euo pipefail
: "${PGVS3_TEST_ENDPOINT_A:?}"
: "${PGVS3_TEST_ENDPOINT_B:?}"
: "${PGVS3_ACCESS_KEY:?}"
: "${PGVS3_SECRET_KEY:?}"
: "${PGVS3_TEST_PID_B:?}"
: "${PGVS3_TEST_PG_CONTAINER:?}"

A=$PGVS3_TEST_ENDPOINT_A
B=$PGVS3_TEST_ENDPOINT_B
BUCKET=pgvs3-fault
RUN="fault-$(date +%s)-$$"
TMP=$(mktemp -d "${PGVS3_TEST_RUN_DIR:?}/fault.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" "$@"; }
code() { s3 -o /dev/null -w '%{http_code}' "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
poll() { local deadline=$((SECONDS + $1)); shift; until "$@"; do [ "$SECONDS" -lt "$deadline" ] || return 1; sleep 0.1; done; }
b_down() { ! curl -fs --max-time 1 -o /dev/null "$B/readyz"; }

curl -fsS -X PUT "$A/$BUCKET" >/dev/null
head -c 1048576 /dev/urandom > "$TMP/base"
[ "$(code -X PUT --data-binary @"$TMP/base" "$A/$BUCKET/$RUN/base")" = 200 ] || fail "baseline PUT"

# Peer loss: the surviving gateway must keep serving from PostgreSQL.
kill "$PGVS3_TEST_PID_B" || fail "peer gateway is not running"
poll 30 b_down || fail "peer gateway still serving after kill"
s3 "$A/$BUCKET/$RUN/base" -o "$TMP/base.a"
cmp "$TMP/base" "$TMP/base.a" || fail "read after peer loss"

# PostgreSQL restart: the body is streamed through a pipe so the restart happens
# while the request is still open. The gateway must reconnect and publish nothing torn.
[ "$(printf streamed | code -T - "$A/$BUCKET/$RUN/streamed")" = 200 ] || fail "streamed PUT"
head -c 33554432 /dev/urandom > "$TMP/big"
{ head -c 1048576 "$TMP/big"; docker restart --time 1 "$PGVS3_TEST_PG_CONTAINER" >/dev/null && : > "$TMP/restarted"; tail -c +1048577 "$TMP/big"; } \
  | s3 -T - "$A/$BUCKET/$RUN/big" >/dev/null 2>&1 || true
[ -e "$TMP/restarted" ] || fail "PostgreSQL restart did not run"
poll 120 docker exec "$PGVS3_TEST_PG_CONTAINER" pg_isready -q -h 127.0.0.1 -U postgres || fail "PostgreSQL did not return"
poll 120 curl -fs --max-time 1 -o /dev/null "$A/readyz" || fail "gateway did not return after PostgreSQL restart"
s3 "$A/$BUCKET/$RUN/base" -o "$TMP/base.after"
cmp "$TMP/base" "$TMP/base.after" || fail "read after PostgreSQL restart"
got=$(code "$A/$BUCKET/$RUN/big")
[ "$got" = 200 ] || [ "$got" = 404 ] || fail "torn object state: HTTP $got"
if [ "$got" = 200 ]; then
  s3 "$A/$BUCKET/$RUN/big" -o "$TMP/big.out"
  cmp "$TMP/big" "$TMP/big.out" || fail "published big object is torn"
fi

# Churn: overwrite and delete one key while maintenance runs.
for i in $(seq 1 200); do
  [ "$(printf 'v%d' "$i" | code -X PUT --data-binary @- "$A/$BUCKET/$RUN/churn")" = 200 ] || fail "churn PUT $i"
  if [ $((i % 7)) = 0 ]; then
    [ "$(code -X DELETE "$A/$BUCKET/$RUN/churn")" = 204 ] || fail "churn DELETE $i"
  fi
done
[ "$(s3 "$A/$BUCKET/$RUN/churn")" = v200 ] || fail "churn object corrupt"
