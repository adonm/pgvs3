#!/usr/bin/env bash
# End-to-end S3 + OJS contract against two live gateways sharing one PostgreSQL.
# Requires: curl, python3, uv (DuckDB 2.0), docker (DuckDB 1.5.6 extension).
set -euo pipefail
: "${PGVS3_TEST_DB_URL:?}"
: "${PGVS3_TEST_ENDPOINT_A:?}"
: "${PGVS3_TEST_ENDPOINT_B:?}"
: "${PGVS3_ACCESS_KEY:?}"
: "${PGVS3_SECRET_KEY:?}"
: "${PGVS3_TEST_PG_PORT:?}"
: "${PGVS3_TEST_BIN:?}"
: "${PGVS3_TEST_RUN_DIR:?}"

A=$PGVS3_TEST_ENDPOINT_A
B=$PGVS3_TEST_ENDPOINT_B
BUCKET=pgvs3-contract
RUN="e2e-$(date +%s)-$$"
TMP=$(mktemp -d "$PGVS3_TEST_RUN_DIR/e2e.XXXXXX")
ro_pid=""
cleanup() {
  if [ -n "$ro_pid" ]; then kill "$ro_pid" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" "$@"; }
code() { s3 -o /dev/null -w '%{http_code}' "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
expect() { local want=$1; shift; local got; got=$(code "$@"); [ "$got" = "$want" ] || fail "expected HTTP $want, got $got: $*"; }
poll() { local seconds=$1; shift; local deadline=$((SECONDS + seconds)); until "$@"; do [ "$SECONDS" -lt "$deadline" ] || return 1; sleep 0.1; done; }

WEB=$A

# Buckets are explicit resources: object writes never create or resurrect one.
expect 200 -X PUT "$A/$BUCKET"

# --- basic object, cross-gateway read, HEAD, ranges -------------------------
head -c 1048576 /dev/urandom > "$TMP/one.bin"
expect 200 -X PUT --data-binary @"$TMP/one.bin" "$A/$BUCKET/$RUN/one.bin"
s3 "$B/$BUCKET/$RUN/one.bin" -o "$TMP/one.out"
cmp "$TMP/one.bin" "$TMP/one.out" || fail "cross-gateway GET differs"
[ "$(s3 -I "$A/$BUCKET/$RUN/one.bin" | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2}')" = 1048576 ] || fail "HEAD length"
s3 -H 'Range: bytes=100-199' "$B/$BUCKET/$RUN/one.bin" -o "$TMP/range.out"
dd if="$TMP/one.bin" of="$TMP/range.want" bs=1 skip=100 count=100 status=none
cmp "$TMP/range.want" "$TMP/range.out" || fail "range bytes"

# --- plain HTTP static web and unsigned S3 ----------------------------------
printf '<html>index</html>' > "$TMP/index.html"
expect 200 -X PUT --data-binary @"$TMP/index.html" "$A/$BUCKET/$RUN/web/index.html"
[ "$(curl -fsS "$WEB/$BUCKET/$RUN/web/")" = '<html>index</html>' ] || fail "web index"
[ "$(curl -fsS "$B/$BUCKET/$RUN/one.bin" | wc -c)" = 1048576 ] || fail "unsigned GET"
curl -fsS -X PUT --data-binary 'unsigned' "$B/$BUCKET/$RUN/noauth" >/dev/null || fail "unsigned PUT"
[ "$(s3 "$A/$BUCKET/$RUN/noauth")" = unsigned ] || fail "unsigned PUT body"
expect 200 -X PUT --data-binary '' "$A/$BUCKET/$RUN/empty"
[ "$(s3 "$B/$BUCKET/$RUN/empty" | wc -c)" = 0 ] || fail "empty object body"

# --- conditional writes and reads ------------------------------------------
expect 200 -X PUT -H 'If-None-Match: *' --data-binary 'created' "$A/$BUCKET/$RUN/if.txt"
expect 412 -X PUT -H 'If-None-Match: *' --data-binary 'again' "$A/$BUCKET/$RUN/if.txt"
etag=$(s3 -I "$A/$BUCKET/$RUN/if.txt" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
expect 200 -X PUT -H "If-Match: $etag" --data-binary 'updated' "$A/$BUCKET/$RUN/if.txt"
etag2=$(s3 -I "$A/$BUCKET/$RUN/if.txt" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
expect 412 -X PUT -H "If-Match: $etag" --data-binary 'stale' "$A/$BUCKET/$RUN/if.txt"
expect 200 -H "If-Match: $etag2" "$A/$BUCKET/$RUN/if.txt"
expect 412 -H 'If-Match: "00000000000000000000000000000000"' "$A/$BUCKET/$RUN/if.txt"
expect 304 -H "If-None-Match: $etag2" "$A/$BUCKET/$RUN/if.txt"

# Concurrent create-if-absent: exactly one winner, no torn object.
for i in $(seq 1 8); do
  ( curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
      -o /dev/null -w '%{http_code}\n' -X PUT -H 'If-None-Match: *' \
      --data-binary "race-$i" "$A/$BUCKET/$RUN/race.txt" > "$TMP/race.$i" ) &
done
wait
cat "$TMP"/race.* > "$TMP/race.codes"
[ "$(grep -c '^200$' "$TMP/race.codes")" = 1 ] || fail "exactly one conditional create must win"
[ "$(grep -c '^412$' "$TMP/race.codes")" = 7 ] || fail "other conditional creates must be 412"
s3 "$B/$BUCKET/$RUN/race.txt" | grep -Eq '^race-[0-9]+$' || fail "torn race object"

# --- unsupported feature policy --------------------------------------------
expect 501 -X PUT -H 'x-amz-server-side-encryption: aws:kms' --data-binary 'x' "$A/$BUCKET/$RUN/sse-kms"

# --- listing, pagination and encoding --------------------------------------
for i in 0 1 2 3 4; do
  expect 200 -X PUT --data-binary "page-$i" "$A/$BUCKET/$RUN/page/$i"
done
expect 200 -X PUT --data-binary 'space' "$A/$BUCKET/$RUN/page/with%20space%2Bplus"

python3 - "$A" "$BUCKET" "$RUN" "$PGVS3_ACCESS_KEY" "$PGVS3_SECRET_KEY" <<'PY'
import sys, urllib.parse, xml.etree.ElementTree as ET
import subprocess, json
endpoint, bucket, run, key, secret = sys.argv[1:6]
def s3(url):
    out = subprocess.check_output(["curl","-sS","--aws-sigv4","aws:amz:us-east-1:s3",
        "--user",f"{key}:{secret}",url])
    return out
seen=[]; token=None
for _ in range(10):
    url=f"{endpoint}/{bucket}?list-type=2&prefix={urllib.parse.quote(run+'/page')}&max-keys=2&encoding-type=url"
    if token: url += "&continuation-token="+urllib.parse.quote(token)
    root=ET.fromstring(s3(url))
    ns={"s3":"http://s3.amazonaws.com/doc/2006-03-01/"}
    for c in root.findall("s3:Contents",ns):
        seen.append(urllib.parse.unquote(c.findtext("s3:Key",namespaces=ns)))
    trunc=root.findtext("s3:IsTruncated",namespaces=ns)=="true"
    if not trunc: break
    token=root.findtext("s3:NextContinuationToken",namespaces=ns)
    assert token, "truncated listing without a token"
assert len(seen)==6, seen
assert run+"/page/with space+plus" in seen, seen
PY

# --- batch delete ----------------------------------------------------------
cat > "$TMP/delete.xml" <<XML
<Delete><Object><Key>$RUN/page/0</Key></Object><Object><Key>$RUN/page/1</Key></Object><Object><Key>$RUN/page/2</Key></Object><Object><Key>$RUN/page/missing</Key></Object></Delete>
XML
s3 -X POST --data-binary @"$TMP/delete.xml" "$A/$BUCKET?delete" >/dev/null
expect 404 "$B/$BUCKET/$RUN/page/0"
expect 404 "$B/$BUCKET/$RUN/page/1"

# --- multipart -------------------------------------------------------------
head -c 5242880 /dev/urandom > "$TMP/p1"
head -c 1048576 /dev/urandom > "$TMP/p2"
init=$(s3 -X POST "$A/$BUCKET/$RUN/multi.bin?uploads")
uid=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("UploadId")][0])' <<<"$init")
e1=$(s3 -D - -o /dev/null -X PUT --data-binary @"$TMP/p1" "$A/$BUCKET/$RUN/multi.bin?partNumber=1&uploadId=$uid" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
e2=$(s3 -D - -o /dev/null -X PUT --data-binary @"$TMP/p2" "$A/$BUCKET/$RUN/multi.bin?partNumber=2&uploadId=$uid" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
cat > "$TMP/complete.xml" <<XML
<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>$e1</ETag></Part><Part><PartNumber>2</PartNumber><ETag>$e2</ETag></Part></CompleteMultipartUpload>
XML
expect 200 -X POST --data-binary @"$TMP/complete.xml" "$A/$BUCKET/$RUN/multi.bin?uploadId=$uid"
cat "$TMP/p1" "$TMP/p2" > "$TMP/multi.want"
s3 "$B/$BUCKET/$RUN/multi.bin" -o "$TMP/multi.out"
cmp "$TMP/multi.want" "$TMP/multi.out" || fail "multipart bytes"
[ "$(s3 -H 'Range: bytes=5242880-' "$A/$BUCKET/$RUN/multi.bin" | wc -c)" = 1048576 ] || fail "multipart range"
s3 "$A/$BUCKET/$RUN/multi.bin?partNumber=1" -o "$TMP/part1.out"
cmp "$TMP/p1" "$TMP/part1.out" || fail "partNumber 1 bytes"
s3 -I "$A/$BUCKET/$RUN/multi.bin?partNumber=2" | tr -d '\r' | grep -qi 'x-amz-mp-parts-count: 2' || fail "part count header"

# --- overwrite through A, read through B (peer cache invalidation) ----------
expect 200 -X PUT --data-binary 'version-one' "$A/$BUCKET/$RUN/over.txt"
[ "$(s3 "$B/$BUCKET/$RUN/over.txt")" = version-one ] || fail "peer read v1"
expect 200 -X PUT --data-binary 'version-two' "$A/$BUCKET/$RUN/over.txt"
[ "$(s3 "$B/$BUCKET/$RUN/over.txt")" = version-two ] || fail "peer read v2"

# --- standard S3 copy, shared-file reaping and attributes ------------------
ED="$A/$BUCKET"
expect 200 -X PUT --data-binary 'copy me' "$ED/src"
expect 200 -X PUT -H 'x-amz-copy-source: /src' "$ED/copy"
[ "$(s3 "$ED/copy")" = 'copy me' ] || fail "copy bytes"
expect 204 -X DELETE "$ED/src"
"$PGVS3_TEST_BIN" reap --url "$PGVS3_TEST_DB_URL" --grace 0s >/dev/null || fail "reap with a shared file"
[ "$(s3 "$ED/copy")" = 'copy me' ] || fail "reaper removed a shared file"

# --- indexed file_id reference checks and multi-pass reaping ----------------
plan=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c '
  SET enable_seqscan = off;
  EXPLAIN (COSTS OFF)
  SELECT f FROM unnest(ARRAY[1,2]::int8[]) AS f
  WHERE NOT EXISTS (SELECT 1 FROM "s3b_pgvs3-contract".objects o WHERE o.file_id IS NOT NULL AND o.file_id = f)
    AND NOT EXISTS (SELECT 1 FROM "s3b_pgvs3-contract".objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[f])
    AND NOT EXISTS (SELECT 1 FROM "s3b_pgvs3-contract".upload_parts p WHERE p.file_id = f);
')
echo "$plan" | grep -q "objects_file_id_idx" || fail "missing objects_file_id_idx in plan: $plan"
echo "$plan" | grep -q "objects_parts_idx" || fail "missing objects_parts_idx in plan: $plan"
echo "$plan" | grep -q "upload_parts_file_id_idx" || fail "missing upload_parts_file_id_idx in plan: $plan"

docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -v ON_ERROR_STOP=1 -c '
  INSERT INTO "s3b_pgvs3-contract".chunks (file_id, no, data)
  SELECT 999999999, g, '\''x'\''::bytea FROM generate_series(0, 69999) AS g;
  INSERT INTO "s3b_pgvs3-contract".garbage (file_id, queued_at) VALUES (999999999, now());
' >/dev/null
"$PGVS3_TEST_BIN" reap --url "$PGVS3_TEST_DB_URL" --grace 0s --all >/dev/null || fail "multi-pass reap"
left_garbage=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  'SELECT count(*) FROM "s3b_pgvs3-contract".garbage WHERE file_id = 999999999')
[ "$left_garbage" = 0 ] || fail "multi-pass reap left $left_garbage garbage rows"
left_chunks=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  'SELECT count(*) FROM "s3b_pgvs3-contract".chunks WHERE file_id = 999999999')
[ "$left_chunks" = 0 ] || fail "multi-pass reap left $left_chunks chunk rows"

# --- per-bucket schema isolation: clone, snapshot/restore, force-drop -------
SRC_B="src-$RUN"
CLONE_HTTP="clone-http-$RUN"
CLONE_CLI="clone-cli-$RUN"
expect 200 -X PUT "$A/$SRC_B"
expect 200 -X PUT --data-binary 'inline-val' "$A/$SRC_B/inline.txt"
expect 200 -X PUT --data-binary @"$TMP/one.bin" "$A/$SRC_B/chunked.bin"

# Whole-bucket clone via HTTP (`PUT /{dst}` with `x-amz-copy-source`) and CLI (`pgvs3 clone`).
expect 200 -X PUT -H "x-amz-copy-source: /$SRC_B" "$A/$CLONE_HTTP"
[ "$(s3 "$B/$CLONE_HTTP/inline.txt")" = 'inline-val' ] || fail "cloned bucket inline read"
s3 "$B/$CLONE_HTTP/chunked.bin" -o "$TMP/cloned.bin"
cmp "$TMP/one.bin" "$TMP/cloned.bin" || fail "cloned bucket chunked read"
expect 409 -X PUT -H "x-amz-copy-source: /$SRC_B" "$A/$CLONE_HTTP"

"$PGVS3_TEST_BIN" clone --url "$PGVS3_TEST_DB_URL" "$SRC_B" "$CLONE_CLI" >/dev/null || fail "pgvs3 clone CLI"
[ "$(s3 "$A/$CLONE_CLI/inline.txt")" = 'inline-val' ] || fail "CLI cloned bucket read"

# Cross-bucket CopyObject (chunked and inline).
expect 200 -X PUT -H "x-amz-copy-source: /$SRC_B/chunked.bin" "$A/$CLONE_CLI/cross-copy.bin"
s3 "$B/$CLONE_CLI/cross-copy.bin" -o "$TMP/cross-copy.bin"
cmp "$TMP/one.bin" "$TMP/cross-copy.bin" || fail "cross-bucket CopyObject bytes"

# Non-empty standard DELETE fails with 409; force DELETE drops the schema immediately.
expect 409 -X DELETE "$A/$CLONE_HTTP"
expect 204 -X DELETE "$A/$CLONE_HTTP?force=true"

# Single-bucket `pg_dump -n` snapshot and `pg_restore` after dropping the bucket.
docker exec "$PGVS3_TEST_PG_CONTAINER" pg_dump -U postgres -Fc -n "\"s3b_$CLONE_CLI\"" pgvs3 > "$TMP/bucket.dump"
"$PGVS3_TEST_BIN" drop --url "$PGVS3_TEST_DB_URL" --force "$CLONE_CLI" >/dev/null || fail "pgvs3 drop --force"
docker exec -i "$PGVS3_TEST_PG_CONTAINER" pg_restore --single-transaction -U postgres -d pgvs3 < "$TMP/bucket.dump"
# Restored schemas are immediately live (no manual adoption step required) and
# carry their own metadata in <schema>.meta; reap/maintenance reconciles the
# catalog and file_ids sequence automatically.
[ "$(s3 "$B/$CLONE_CLI/inline.txt")" = 'inline-val' ] || fail "restored bucket inline read"
s3 "$B/$CLONE_CLI/cross-copy.bin" -o "$TMP/restored.bin"
cmp "$TMP/one.bin" "$TMP/restored.bin" || fail "restored bucket chunked read"
"$PGVS3_TEST_BIN" reap --url "$PGVS3_TEST_DB_URL" --grace 0s >/dev/null
"$PGVS3_TEST_BIN" drop --url "$PGVS3_TEST_DB_URL" --force "$SRC_B" >/dev/null
"$PGVS3_TEST_BIN" drop --url "$PGVS3_TEST_DB_URL" --force "$CLONE_CLI" >/dev/null

# A catalog row whose schema vanished must not block layout checks, and
# maintenance (`reap`) prunes it automatically.
MISSING="missing-$RUN"
expect 200 -X PUT "$A/$MISSING"
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -q -c \
  "SET client_min_messages=warning; DROP SCHEMA \"s3b_$MISSING\" CASCADE"
doctor_out=$("$PGVS3_TEST_BIN" doctor --url "$PGVS3_TEST_DB_URL" || true)
echo "$doctor_out" | grep -q "missing_schemas=1" || fail "missing schema not reported: $doctor_out"
"$PGVS3_TEST_BIN" reap --url "$PGVS3_TEST_DB_URL" --grace 0s >/dev/null || fail "reap with a missing schema"
doctor_out=$("$PGVS3_TEST_BIN" doctor --url "$PGVS3_TEST_DB_URL")
echo "$doctor_out" | grep -q "missing_schemas=0" || fail "missing schema not pruned: $doctor_out"

# CRC64NVME / FULL_OBJECT checksum on a single PUT.
crc64=$(python3 -c 'import base64,struct; print(base64.b64encode(struct.pack(">Q",0xAE8B14860A799888)).decode())')
expect 200 -X PUT -H "x-amz-checksum-crc64nvme: $crc64" -H 'x-amz-sdk-checksum-algorithm: CRC64NVME' -H 'x-amz-checksum-type: FULL_OBJECT' --data-binary '123456789' "$ED/crc64"
[ "$(s3 "$ED/crc64")" = 123456789 ] || fail "crc64 body"
expect 400 -X PUT -H "x-amz-checksum-crc64nvme: AAAAAAAAAAA=" -H 'x-amz-sdk-checksum-algorithm: CRC64NVME' --data-binary '123456789' "$ED/crc64-bad"

# Attributes, UploadPartCopy and multipart listings.
s3 -H 'x-amz-object-attributes: ETag,ObjectSize,Checksum,ObjectParts,StorageClass' "$ED/copy?attributes" | grep -q ObjectSize || fail "attributes"
head -c 1048576 /dev/urandom > "$TMP/partcopy"
expect 200 -X PUT --data-binary @"$TMP/partcopy" "$ED/partcopy.src"
upc=$(s3 -X POST "$ED/partcopy.dst?uploads")
upc_id=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("UploadId")][0])' <<<"$upc")
ce=$(s3 -D - -o /dev/null -X PUT -H 'x-amz-copy-source: /partcopy.src' -H 'x-amz-copy-source-range: bytes=0-1048575' "$ED/partcopy.dst?partNumber=1&uploadId=$upc_id" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
[ -n "$ce" ] || fail "upload part copy etag"
s3 "$ED/partcopy.dst?uploadId=$upc_id" | grep -q '<PartNumber>1</PartNumber>' || fail "list parts"
s3 "$ED?uploads" | grep -q '<UploadId>' || fail "list multipart uploads"
s3 -X POST "$ED/$RUN/mp/a/one?uploads" >/dev/null
s3 -X POST "$ED/$RUN/mp/b/two?uploads" >/dev/null
s3 "$ED?uploads&prefix=$RUN/mp/&delimiter=/" | grep -q "<Prefix>$RUN/mp/a/</Prefix>" || fail "multipart delimiter"
cat > "$TMP/upc.xml" <<XML
<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>$ce</ETag></Part></CompleteMultipartUpload>
XML
expect 200 -X POST --data-binary @"$TMP/upc.xml" "$ED/partcopy.dst?uploadId=$upc_id"
s3 "$ED/partcopy.dst" -o "$TMP/partcopy.out"
cmp "$TMP/partcopy" "$TMP/partcopy.out" || fail "upload part copy bytes"

# The standard S3 surface through an SDK.
E2E_RUN="$RUN" E2E_ENDPOINT="$A" E2E_KEY="$PGVS3_ACCESS_KEY" E2E_SECRET="$PGVS3_SECRET_KEY" \
mise exec uv -- uv run --quiet --no-project --with boto3 python - <<'PY'
import os, subprocess, urllib.error, urllib.request, boto3
from botocore.config import Config
pc=boto3.client('s3', endpoint_url=os.environ['E2E_ENDPOINT'],
    aws_access_key_id=os.environ['E2E_KEY'], aws_secret_access_key=os.environ['E2E_SECRET'],
    region_name='us-east-1', config=Config(signature_version='s3v4', s3={'addressing_style':'path'}))
run=os.environ['E2E_RUN']; gen='pgvs3-contract'
pc.put_object(Bucket=gen, Key=f'{run}/sdk', Body=b'sdk')
assert pc.get_object(Bucket=gen, Key=f'{run}/sdk')['Body'].read()==b'sdk'
pc.copy_object(Bucket=gen, Key=f'{run}/sdk-copy', CopySource={'Bucket':gen,'Key':f'{run}/sdk'})
assert pc.get_object(Bucket=gen, Key=f'{run}/sdk-copy')['Body'].read()==b'sdk'
pc.put_object(Bucket=gen, Key=f'{run}/presigned', Body=b'presigned')
# Presigned URLs are deliberately rejected: access control is the network's job.
u=pc.generate_presigned_url('get_object', Params={'Bucket':gen,'Key':f'{run}/presigned'}, ExpiresIn=60)
try:
    urllib.request.urlopen(u)
except urllib.error.HTTPError as e:
    assert e.code == 400, e.code
else:
    raise AssertionError("presigned GET must be rejected")
u=pc.generate_presigned_url('put_object', Params={'Bucket':gen,'Key':f'{run}/presigned-put'}, ExpiresIn=60)
try:
    urllib.request.urlopen(urllib.request.Request(u,data=b'put',method='PUT'))
except urllib.error.HTTPError as e:
    assert e.code == 400, e.code
else:
    raise AssertionError("presigned PUT must be rejected")
pc.put_object(Bucket=gen, Key=f'{run}/over-inline', Body=b'old')
up=pc.create_multipart_upload(Bucket=gen, Key=f'{run}/over-inline')
part=pc.upload_part(Bucket=gen, Key=f'{run}/over-inline', UploadId=up['UploadId'], PartNumber=1, Body=b'n'*1048576)
pc.complete_multipart_upload(Bucket=gen, Key=f'{run}/over-inline', UploadId=up['UploadId'], MultipartUpload={'Parts':[{'PartNumber':1,'ETag':part['ETag']}]})
assert pc.get_object(Bucket=gen, Key=f'{run}/over-inline')['Body'].read()==b'n'*1048576
up=pc.create_multipart_upload(Bucket=gen, Key=f'{run}/part-crc', ChecksumAlgorithm='SHA256', ChecksumType='COMPOSITE')
part=pc.upload_part(Bucket=gen, Key=f'{run}/part-crc', UploadId=up['UploadId'], PartNumber=1, Body=b'checksum part', ChecksumAlgorithm='SHA256')
pc.complete_multipart_upload(Bucket=gen, Key=f'{run}/part-crc', UploadId=up['UploadId'], MultipartUpload={'Parts':[{'PartNumber':1,'ETag':part['ETag'],'ChecksumSHA256':part['ChecksumSHA256']}]})
attrs=pc.get_object_attributes(Bucket=gen, Key=f'{run}/part-crc', ObjectAttributes=['Checksum','ObjectParts'])
assert attrs['ObjectParts']['Parts'][0]['ChecksumSHA256']==part['ChecksumSHA256']
pc.put_object(Bucket=gen, Key=f'{run}/shared-src', Body=b'shared')
pc.copy_object(Bucket=gen, Key=f'{run}/shared-copy', CopySource={'Bucket':gen,'Key':f'{run}/shared-src'})
pc.delete_objects(Bucket=gen, Delete={'Objects':[{'Key':f'{run}/shared-src'},{'Key':f'{run}/presigned'}]})
subprocess.run([os.environ['PGVS3_TEST_BIN'], 'reap', '--url', os.environ['PGVS3_TEST_DB_URL'], '--grace', '0s'], check=True, stdout=subprocess.DEVNULL)
assert pc.get_object(Bucket=gen, Key=f'{run}/shared-copy')['Body'].read()==b'shared'
PY

# --- RESP (Redis/Valkey) + compressed block storage on the same port --------
E2E_RUN="$RUN" E2E_ENDPOINT_A="$A" E2E_ENDPOINT_B="$B" E2E_BUCKET="$BUCKET" \
E2E_KEY="$PGVS3_ACCESS_KEY" E2E_SECRET="$PGVS3_SECRET_KEY" \
mise exec uv -- uv run --quiet --no-project --with redis --with boto3 python - <<'PY'
import os, socket, time, urllib.request, redis, boto3
from botocore.config import Config

ep_a = os.environ['E2E_ENDPOINT_A']
ep_b = os.environ['E2E_ENDPOINT_B']
host_a, port_a = ep_a.split('://')[1].split(':')
host_b, port_b = ep_b.split('://')[1].split(':')
ra = redis.Redis(host=host_a, port=int(port_a), decode_responses=False, protocol=2)
rb = redis.Redis(host=host_b, port=int(port_b), decode_responses=False, protocol=2)

assert ra.echo(b'hello') == b'hello'

# RESP3 is not available: HELLO 3 must fail without closing the connection,
# and HELLO 2 must still return the server banner in RESP2 form.
def resp_value(f):
    kind = f.read(1)
    if kind == b'$':
        n = int(f.readline())
        if n < 0:
            return None
        data = f.read(n)
        f.read(2)
        return data
    if kind == b':':
        return int(f.readline())
    if kind == b'+':
        return f.readline().rstrip(b'\r\n')
    if kind == b'-':
        return Exception(f.readline().rstrip(b'\r\n'))
    if kind == b'*':
        n = int(f.readline())
        return [resp_value(f) for _ in range(n)]
    raise AssertionError(f'unexpected RESP type {kind!r}')

raw = socket.create_connection((host_a, int(port_a)))
rawf = raw.makefile('rb')
raw.sendall(b'*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n')
hello3 = resp_value(rawf)
assert isinstance(hello3, Exception) and b'NOPROTO' in hello3.args[0], hello3
raw.sendall(b'*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n')
hello2 = resp_value(rawf)
assert isinstance(hello2, list) and len(hello2) == 14, hello2
banner = dict(zip(hello2[0::2], hello2[1::2]))
assert banner[b'server'] == b'pgvs3' and banner[b'proto'] == 2, banner
raw.close()

# Basic SET/GET across gateways A and B, plus HTTP read of the same key in `s3b_cache`.
assert ra.set('greeting', b'hello-from-resp') is True
assert rb.get('greeting') == b'hello-from-resp'
assert urllib.request.urlopen(f"{ep_b}/_cache/cache/greeting").read() == b'hello-from-resp'

# XX, GET, PX, PERSIST and TTL, read back through gateway B.
ra.set('greeting', b'updated-resp', xx=True, get=True)
assert rb.get('greeting') == b'updated-resp'

assert ra.set('ttl-key', b'ephemeral', px=3000) is True
pttl = rb.pttl('ttl-key')
assert 0 < pttl <= 3000, pttl
ra.persist('ttl-key')
assert rb.ttl('ttl-key') == -1

assert ra.set('short-ttl', b'bye', px=150) is True
deadline = time.monotonic() + 5
while rb.get('short-ttl') is not None:
    assert time.monotonic() < deadline, 'short-ttl did not expire on gateway B'
    time.sleep(0.05)
assert rb.ttl('short-ttl') == -2

# Atomic INCR / DECR / INCRBY.
ra.incr('counter')
assert rb.incrby('counter', 41) == 42

# MSET / MGET / EXISTS / DEL / KEYS / SCAN / INFO.
assert ra.mset({'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}) is True
assert rb.mget(['k1', 'missing', 'k3']) == [b'v1', None, b'v3']
assert rb.exists('k1', 'k2', 'missing') == 2
assert sorted(rb.keys('k*')) == [b'k1', b'k2', b'k3']
cursor, scanned = rb.scan(cursor=0, match='k*', count=10)
assert cursor == 0 and sorted(scanned) == [b'k1', b'k2', b'k3'], (cursor, scanned)
assert ra.delete('k1', 'k2', 'missing') == 2
assert rb.exists('k1', 'k2') == 0

# Cross-namespace and s3:// writes over RESP are rejected even when the bucket
# name matches the active cache namespace.
for bad_key in ('s3://cache/forbid', f"s3://{os.environ['E2E_BUCKET']}/forbid"):
    try:
        ra.set(bad_key, b'nope')
        raise AssertionError(f'write to {bad_key} should fail')
    except redis.ResponseError as exc:
        assert 'cross-namespace' in str(exc), exc

# Large compressible value (> 64 KiB -> compressed 8,120-byte blocks in `chunks`)
# and GETRANGE slicing across block boundaries.
big = (b'0123456789abcdef' * 16384) # 256 KiB
assert ra.set('big-blob', big) is True
assert rb.strlen('big-blob') == len(big)
assert rb.get('big-blob') == big
assert rb.getrange('big-blob', 8100, 16300) == big[8100:16301]

# Pipelined commands (coalesced GET batching).
pipe = ra.pipeline(transaction=False)
for i in range(64):
    pipe.set(f'p:{i}', f'val-{i}'.encode())
pipe.execute()
pipe = rb.pipeline(transaction=False)
for i in range(64):
    pipe.get(f'p:{i}')
assert pipe.execute() == [f'val-{i}'.encode() for i in range(64)]

# MULTI/WATCH are rejected explicitly instead of pretending to be atomic.
try:
    ra.pipeline(transaction=True).set('multi-key', b'x').execute()
    raise AssertionError('MULTI should be rejected')
except redis.ResponseError as exc:
    assert 'MULTI' in str(exc), exc

# Cross-protocol S3 <-> RESP unified bucket access.
s3c = boto3.client('s3', endpoint_url=ep_a,
    aws_access_key_id=os.environ['E2E_KEY'], aws_secret_access_key=os.environ['E2E_SECRET'],
    region_name='us-east-1', config=Config(signature_version='s3v4', s3={'addressing_style':'path'}))
bkt = os.environ['E2E_BUCKET']; run = os.environ['E2E_RUN']
s3c.put_object(Bucket=bkt, Key=f'{run}/from-s3', Body=b's3-payload-' * 1000)
assert rb.get(f's3://{bkt}/{run}/from-s3') == b's3-payload-' * 1000
PY

# --- OJS core job protocol: lifecycle on A, state visible on the read-only gateway
OQ="ojs-${RUN//_/-}"
ojs_post() {
  local base=$1 path=$2 body=$3
  curl -fsS -X POST -H 'Content-Type: application/openjobspec+json' -d "$body" "$base$path"
}
jget() {
  python3 -c 'import json,sys
d=json.load(sys.stdin)
for k in sys.argv[1].split("."):
    d=d[int(k)] if k.isdigit() else d[k]
print(d)' "$1"
}
ojs_code() {
  local method=$1 base=$2 path=$3 body=${4:-}
  if [ -n "$body" ]; then
    curl -sS -o "$TMP/ojs.json" -w '%{http_code}' -X "$method" \
      -H 'Content-Type: application/openjobspec+json' -d "$body" "$base$path"
  else
    curl -sS -o "$TMP/ojs.json" -w '%{http_code}' -X "$method" "$base$path"
  fi
}
enq=$(ojs_post "$A" /ojs/v1/jobs "{\"type\":\"e2e.job\",\"args\":[\"one\",{\"n\":1}],\"options\":{\"queue\":\"$OQ\",\"priority\":7}}")
id=$(printf '%s' "$enq" | jget job.id)
fr=$(ojs_post "$A" /ojs/v1/workers/fetch "{\"queues\":[\"$OQ\"],\"count\":1,\"worker_id\":\"e2e-worker\",\"visibility_timeout_ms\":2000}")
jid=$(printf '%s' "$fr" | jget job.id)
tok=$(printf '%s' "$fr" | jget job.lease_token)
[ "$jid" = "$id" ] || fail "OJS fetch returned $jid, want $id"
ack=$(ojs_post "$A" /ojs/v1/workers/ack "{\"job_id\":\"$jid\",\"lease_token\":\"$tok\",\"result\":{\"ok\":true}}")
[ "$(printf '%s' "$ack" | jget state)" = completed ] || fail "OJS ack: $ack"
ro_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
"$PGVS3_TEST_BIN" serve --url "$PGVS3_TEST_DB_URL" --addr "127.0.0.1:$ro_port" \
  --read-only --log-level error >"$PGVS3_TEST_RUN_DIR/ojs-ro.log" 2>&1 &
ro_pid=$!
ro_ready() { curl -fsS --max-time 1 "http://127.0.0.1:$ro_port/ojs/v1/health" >/dev/null 2>&1; }
poll 30 ro_ready || { cat "$PGVS3_TEST_RUN_DIR/ojs-ro.log" >&2; fail "read-only OJS gateway did not start"; }
RO="http://127.0.0.1:$ro_port"
[ "$(ojs_code GET "$RO" "/ojs/v1/jobs/$jid")" = 200 ] || fail "read-only OJS INFO"
[ "$(jget job.state < "$TMP/ojs.json")" = completed ] || fail "read-only OJS did not see the ack"
[ "$(ojs_code POST "$RO" /ojs/v1/jobs '{"type":"e2e.ro","args":[]}')" = 403 ] || fail "read-only OJS enqueue"
kill "$ro_pid" 2>/dev/null || true
wait "$ro_pid" 2>/dev/null || true
ro_pid=""
plan=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c "
  SET enable_seqscan = off;
  EXPLAIN (COSTS OFF) UPDATE \"s3q_$OQ\".jobs SET state='active' WHERE id = '$id';
")
echo "$plan" | grep -q "jobs_id_key" || fail "OJS lease update does not use jobs id index: $plan"
"$PGVS3_TEST_BIN" drop --url "$PGVS3_TEST_DB_URL" --kind queue --force "$OQ" >/dev/null

# --- S3 cache tier stored in PostgreSQL (no external gateway state) ---------
ORIGIN_DIR="$TMP/origin"
mkdir -p "$ORIGIN_DIR"
cat > "$TMP/origin.json" <<JSON
{"type":"filesystem","config":{"directory":"$ORIGIN_DIR"}}
JSON
"$PGVS3_TEST_BIN" origin --url "$PGVS3_TEST_DB_URL" --bucket edge \
  --cache-namespace "ocache-$RUN" --cache-ttl 0s set "$TMP/origin.json" | grep -q "origin_set=edge" || fail "pgvs3 origin set"
mkdir -p "$ORIGIN_DIR/edge/$RUN"
printf probe > "$ORIGIN_DIR/edge/$RUN/probe.txt"
origin_visible() { [ "$(curl -fsS "$1/edge/$RUN/probe.txt" 2>/dev/null)" = probe ]; }
poll 30 origin_visible "$A" || fail "origin config did not reach gateway A"
poll 30 origin_visible "$B" || fail "origin config did not reach gateway B"
OCACHE="$A"
OCACHE_ENDPOINT_A="$A" OCACHE_ENDPOINT_B="$B" OCACHE_DIR="$ORIGIN_DIR" OCACHE_RUN="$RUN" \
E2E_KEY="$PGVS3_ACCESS_KEY" E2E_SECRET="$PGVS3_SECRET_KEY" \
mise exec uv -- uv run --quiet --no-project --with boto3 python - <<'PY'
import os, time, boto3
from botocore.config import Config
cfg = Config(signature_version='s3v4', s3={'addressing_style': 'path'})
ca = boto3.client('s3', endpoint_url=os.environ['OCACHE_ENDPOINT_A'],
    aws_access_key_id=os.environ['E2E_KEY'], aws_secret_access_key=os.environ['E2E_SECRET'],
    region_name='us-east-1', config=cfg)
cb = boto3.client('s3', endpoint_url=os.environ['OCACHE_ENDPOINT_B'],
    aws_access_key_id=os.environ['E2E_KEY'], aws_secret_access_key=os.environ['E2E_SECRET'],
    region_name='us-east-1', config=cfg)
b = 'edge'
k = os.environ['OCACHE_RUN'] + '/obj.bin'
body = b'origin-payload-' + b'x' * 4000
ca.put_object(Bucket=b, Key=k, Body=body, ContentType='application/octet-stream', Metadata={'tier': 'write-through'})
get_res = cb.get_object(Bucket=b, Key=k)
assert get_res['Body'].read() == body
assert get_res['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', get_res['ResponseMetadata']['HTTPHeaders']
assert {mk.lower(): mv for mk, mv in get_res.get('Metadata', {}).items()} == {'tier': 'write-through'}, get_res.get('Metadata')
head = cb.head_object(Bucket=b, Key=k)
assert head['ContentLength'] == len(body) and head['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', head['ResponseMetadata']['HTTPHeaders']
try:
    cb.get_object(Bucket=b, Key=k, IfNoneMatch=head['ETag'])
    raise AssertionError('expected 304 Not Modified')
except cb.exceptions.ClientError as exc:
    assert exc.response['Error']['Code'] == '304', exc.response
assert cb.get_object(Bucket=b, Key=k, Range='bytes=0-15')['Body'].read() == body[:16]
delim = cb.list_objects_v2(Bucket=b, Prefix='', Delimiter='/')
assert any(cp['Prefix'] == os.environ['OCACHE_RUN'] + '/' for cp in delim.get('CommonPrefixes', [])), delim
copy_k = os.environ['OCACHE_RUN'] + '/copied.bin'
ca.copy_object(Bucket=b, Key=copy_k, CopySource={'Bucket': b, 'Key': k})
copy_get = cb.get_object(Bucket=b, Key=copy_k)
assert copy_get['Body'].read() == body
assert copy_get['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', copy_get['ResponseMetadata']['HTTPHeaders']
mp_k = os.environ['OCACHE_RUN'] + '/mp.bin'
mp_body = b'part-payload-' * 1000
up = ca.create_multipart_upload(Bucket=b, Key=mp_k)
part = ca.upload_part(Bucket=b, Key=mp_k, UploadId=up['UploadId'], PartNumber=1, Body=mp_body)
ca.complete_multipart_upload(Bucket=b, Key=mp_k, UploadId=up['UploadId'],
    MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
mp_get = cb.get_object(Bucket=b, Key=mp_k)
assert mp_get['Body'].read() == mp_body
assert mp_get['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', mp_get['ResponseMetadata']['HTTPHeaders']
mp_path = os.path.join(os.environ['OCACHE_DIR'], b, mp_k)
deadline = time.monotonic() + 15
while not os.path.exists(mp_path):
    assert time.monotonic() < deadline, 'multipart outbox did not reach the origin'
    time.sleep(0.05)
ca.delete_objects(Bucket=b, Delete={'Objects': [{'Key': copy_k}, {'Key': mp_k}]})
p = os.path.join(os.environ['OCACHE_DIR'], b, k)
open(p, 'wb').write(b'origin-updated')
assert cb.get_object(Bucket=b, Key=k)['Body'].read() == b'origin-updated'
keys = [o['Key'] for o in cb.list_objects_v2(Bucket=b, Prefix=os.environ['OCACHE_RUN'] + '/').get('Contents', [])]
assert k in keys, keys
ca.delete_object(Bucket=b, Key=k)
PY

quota_enforced() { ! curl -fsS -X PUT --data-binary 'quota' "$A/_cache/cache/quota-test" >/dev/null 2>&1; }
cache_write_allowed() { curl -fsS -X PUT --data-binary 'quota' "$A/_cache/cache/quota-test" >/dev/null 2>&1; }
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  "UPDATE s3p.namespaces SET quota_bytes = 1 WHERE kind = 'cache' AND name = 'cache'" >/dev/null
poll 15 quota_enforced || fail "quota was not enforced on a cache write"
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  "UPDATE s3p.namespaces SET quota_bytes = NULL WHERE kind = 'cache' AND name = 'cache'" >/dev/null
poll 15 cache_write_allowed || fail "cache write after quota reset"

# A ranged read on a large origin object is served from the origin; a full GET fills the cache.
python3 - "$ORIGIN_DIR" "$RUN" <<'PY'
import os, sys
root, run = sys.argv[1], sys.argv[2]
p = os.path.join(root, 'edge', run, 'big.bin')
os.makedirs(os.path.dirname(p), exist_ok=True)
with open(p, 'wb') as f:
    f.write(b'0123456789abcdef' * (1 << 20))  # 16 MiB
PY
curl -fsS -H 'Range: bytes=1048576-1049599' "$OCACHE/edge/$RUN/big.bin" -o "$TMP/range.part"
[ "$(wc -c < "$TMP/range.part")" = 1024 ] || fail "cache range length"
curl -fsS "$OCACHE/edge/$RUN/big.bin" -o /dev/null
curl -fsSI -H 'Range: bytes=1048576-1049599' "$B/edge/$RUN/big.bin" | grep -qi 'x-cache: HIT' || fail "cached 16 MiB range did not hit cache"
stat_out=$("$PGVS3_TEST_BIN" stat --url "$PGVS3_TEST_DB_URL" --kind cache --name "ocache-$RUN")
echo "$stat_out" | grep -Eq 'chunks=[1-9]' || fail "pgvs3 stat chunks: $stat_out"
"$PGVS3_TEST_BIN" origin --url "$PGVS3_TEST_DB_URL" --bucket edge clear | grep -q "origin_cleared=edge" || fail "pgvs3 origin clear"

# --- Maintained README Parquet/DuckLake scenario in both storage roles -------
export E2E_BUCKET=$BUCKET E2E_RUN=$RUN E2E_ENDPOINT=$A E2E_KEY=$PGVS3_ACCESS_KEY \
       E2E_SECRET=$PGVS3_SECRET_KEY E2E_PG_PORT=$PGVS3_TEST_PG_PORT
mise exec uv -- uv run --quiet --no-project --with duckdb==2.0.0 python test/data_plane.py analytics

# --- duckdb-object-storage (1.5.6) on the same gateway ---------------------
mise exec uv -- uv run --quiet --no-project --with duckdb==1.5.6 python - <<'PY'
import os, duckdb
c=duckdb.connect()
c.execute("INSTALL cache_httpfs FROM community; LOAD cache_httpfs;")
c.execute("INSTALL duckdb_object_storage FROM community; LOAD duckdb_object_storage;")
b=os.environ["E2E_BUCKET"]; run=os.environ["E2E_RUN"]; ep=os.environ["E2E_ENDPOINT"][7:]
c.execute(f"""CREATE OR REPLACE SECRET objfs_s3 (TYPE S3, PROVIDER CONFIG,
 KEY_ID '{os.environ['E2E_KEY']}', SECRET '{os.environ['E2E_SECRET']}',
 REGION 'us-east-1', ENDPOINT '{ep}', USE_SSL false, URL_STYLE 'path',
 SCOPE 's3://{b}/{run}/objfs')""")
c.execute("SET duckdb_objfs_backend='s3'")
c.execute(f"SET duckdb_objfs_bucket='{b}'")
c.execute(f"SET duckdb_objfs_root='{run}/objfs'")
c.execute("ATTACH 'duckdb_objfs://contract.db' AS o")
c.execute("CREATE TABLE o.items(i INTEGER, s VARCHAR)")
c.execute("INSERT INTO o.items VALUES (1,'one'),(2,'two'),(3,'three')")
c.execute("CHECKPOINT o")
assert c.execute("SELECT count(*), sum(i) FROM o.items").fetchone()==(3,6)
c.execute("DETACH o")
c.execute("ATTACH 'duckdb_objfs://contract.db' AS o (READ_ONLY)")
assert c.execute("SELECT count(*) FROM o.items").fetchone()[0]==3
PY
