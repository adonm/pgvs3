#!/usr/bin/env bash
# End-to-end S3 contract against two live gateways sharing one PostgreSQL.
# Requires: curl, python3, uv (DuckDB 2.0), docker (DuckDB 1.5.6 extension).
set -euo pipefail
: "${PGVS3_TEST_DB_URL:?}"
: "${PGVS3_TEST_ENDPOINT_A:?}"
: "${PGVS3_TEST_ENDPOINT_B:?}"
: "${PGVS3_ACCESS_KEY:?}"
: "${PGVS3_SECRET_KEY:?}"
: "${PGVS3_TEST_PG_PORT:?}"

A=$PGVS3_TEST_ENDPOINT_A
B=$PGVS3_TEST_ENDPOINT_B
BUCKET=pgvs3-contract
RUN="e2e-$(date +%s)-$$"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" "$@"; }
code() { s3 -o /dev/null -w '%{http_code}' "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
expect() { local want=$1; shift; local got; got=$(code "$@"); [ "$got" = "$want" ] || fail "expected HTTP $want, got $got: $*"; }

WEB=$A

# --- basic object, cross-gateway read, HEAD, ranges -------------------------
head -c 1048576 /dev/urandom > "$TMP/one.bin"
expect 200 -X PUT --data-binary @"$TMP/one.bin" "$A/$BUCKET/$RUN/one.bin"
s3 "$B/$BUCKET/$RUN/one.bin" -o "$TMP/one.out"
cmp "$TMP/one.bin" "$TMP/one.out" || fail "cross-gateway GET differs"
[ "$(s3 -I "$A/$BUCKET/$RUN/one.bin" | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2}')" = 1048576 ] || fail "HEAD length"
s3 -H 'Range: bytes=100-199' "$B/$BUCKET/$RUN/one.bin" -o "$TMP/range.out"
dd if="$TMP/one.bin" of="$TMP/range.want" bs=1 skip=100 count=100 status=none
cmp "$TMP/range.want" "$TMP/range.out" || fail "range bytes"
[ "$(s3 -H 'Range: bytes=-100' "$A/$BUCKET/$RUN/one.bin" | wc -c)" = 100 ] || fail "suffix range"
expect 416 -H 'Range: bytes=99999999-' "$A/$BUCKET/$RUN/one.bin"

# --- plain HTTP static web and unsigned S3 ----------------------------------
printf '<html>index</html>' > "$TMP/index.html"
expect 200 -X PUT --data-binary @"$TMP/index.html" "$A/$BUCKET/$RUN/web/index.html"
printf 'asset' > "$TMP/asset.txt"
expect 200 -X PUT -H 'Content-Type: text/plain' --data-binary @"$TMP/asset.txt" "$A/$BUCKET/$RUN/web/asset.txt"
[ "$(curl -fsS "$WEB/$BUCKET/$RUN/web/")" = '<html>index</html>' ] || fail "web index"
[ "$(curl -fsS "$WEB/$BUCKET/$RUN/web/asset.txt")" = asset ] || fail "web asset"
curl -fsSI "$WEB/$BUCKET/$RUN/web/" | grep -qi 'content-type: text/html' || fail "web content type"
[ "$(curl -fsS "$B/$BUCKET/$RUN/one.bin" | wc -c)" = 1048576 ] || fail "unsigned GET"
curl -fsS -X PUT --data-binary 'unsigned' "$B/$BUCKET/$RUN/noauth" >/dev/null || fail "unsigned PUT"
[ "$(s3 "$A/$BUCKET/$RUN/noauth")" = unsigned ] || fail "unsigned PUT body"
# Presigned URLs are rejected: auth is a network concern, not a gateway one.
expect 400 "$A/$BUCKET/$RUN/one.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak&X-Amz-Signature=abc"
expect 400 "$A/$BUCKET/$RUN/one.bin?AWSAccessKeyId=ak&Signature=abc&Expires=1"
expect 200 -X PUT --data-binary '' "$A/$BUCKET/$RUN/empty"
[ "$(s3 "$B/$BUCKET/$RUN/empty" | wc -c)" = 0 ] || fail "empty object body"
[ "$(s3 -I "$A/$BUCKET/$RUN/empty" | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2}')" = 0 ] || fail "empty object HEAD"

# --- conditional writes and reads ------------------------------------------
expect 200 -X PUT -H 'If-None-Match: *' --data-binary 'created' "$A/$BUCKET/$RUN/if.txt"
expect 412 -X PUT -H 'If-None-Match: *' --data-binary 'again' "$A/$BUCKET/$RUN/if.txt"
etag=$(s3 -I "$A/$BUCKET/$RUN/if.txt" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
expect 200 -X PUT -H "If-Match: $etag" --data-binary 'updated' "$A/$BUCKET/$RUN/if.txt"
etag2=$(s3 -I "$A/$BUCKET/$RUN/if.txt" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
[ "$etag2" != "$etag" ] || fail "conditional update did not change the ETag"
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
expect 200 -X PUT -H 'x-amz-server-side-encryption: AES256' --data-binary 'x' "$A/$BUCKET/$RUN/sse-aes"
expect 501 -X PUT -H 'x-amz-object-lock-mode: COMPLIANCE' --data-binary 'x' "$A/$BUCKET/$RUN/lock"
expect 501 -X PUT -H 'x-amz-tagging: a=b' --data-binary 'x' "$A/$BUCKET/$RUN/tag"
expect 501 -X PUT -H 'x-amz-acl: public-read' --data-binary 'x' "$A/$BUCKET/$RUN/public"

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
delete_resp=$(s3 -X POST --data-binary @"$TMP/delete.xml" "$A/$BUCKET?delete")
for key in "$RUN/page/0" "$RUN/page/1" "$RUN/page/2" "$RUN/page/missing"; do
  grep -q "<Deleted><Key>$key</Key></Deleted>" <<<"$delete_resp" || fail "DeleteObjects missing Deleted entry for $key"
done
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
[ "$(s3 -I "$A/$BUCKET/$RUN/multi.bin?partNumber=2" | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2}')" = 1048576 ] || fail "partNumber 2 HEAD length"
s3 -I "$A/$BUCKET/$RUN/multi.bin?partNumber=2" | tr -d '\r' | grep -qi 'x-amz-mp-parts-count: 2' || fail "part count header"
expect 416 "$A/$BUCKET/$RUN/multi.bin?partNumber=3"

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
sleep 2
[ "$(s3 "$ED/copy")" = 'copy me' ] || fail "reaper removed a shared file"

# --- indexed file_id reference checks and multi-pass reaping ----------------
.tmp/pgvs3/pgvs3 doctor --url "$PGVS3_TEST_DB_URL" >/dev/null || fail "pgvs3 doctor"
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
reap_out=$(.tmp/pgvs3/pgvs3 reap --url "$PGVS3_TEST_DB_URL" --grace 0s --all)
echo "$reap_out" | grep -q "reaped_rows=70000" || fail "multi-pass reap rows mismatch: $reap_out"
echo "$reap_out" | grep -q "reaped_files=1" || fail "multi-pass reap files mismatch: $reap_out"
left_chunks=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  'SELECT count(*) FROM "s3b_pgvs3-contract".chunks WHERE file_id = 999999999')
[ "$left_chunks" = 0 ] || fail "multi-pass reap left $left_chunks chunk rows"

# --- per-bucket schema isolation: clone, snapshot/restore, force-drop -------
SRC_B="src-$RUN"
CLONE_HTTP="clone-http-$RUN"
CLONE_CLI="clone-cli-$RUN"
expect 200 -X PUT --data-binary 'inline-val' "$A/$SRC_B/inline.txt"
expect 200 -X PUT --data-binary @"$TMP/one.bin" "$A/$SRC_B/chunked.bin"

# Whole-bucket clone via HTTP (`PUT /{dst}` with `x-amz-copy-source`) and CLI (`pgvs3 clone`).
expect 200 -X PUT -H "x-amz-copy-source: /$SRC_B" "$A/$CLONE_HTTP"
[ "$(s3 "$B/$CLONE_HTTP/inline.txt")" = 'inline-val' ] || fail "cloned bucket inline read"
s3 "$B/$CLONE_HTTP/chunked.bin" -o "$TMP/cloned.bin"
cmp "$TMP/one.bin" "$TMP/cloned.bin" || fail "cloned bucket chunked read"
expect 409 -X PUT -H "x-amz-copy-source: /$SRC_B" "$A/$CLONE_HTTP"

.tmp/pgvs3/pgvs3 clone --url "$PGVS3_TEST_DB_URL" "$SRC_B" "$CLONE_CLI" >/dev/null || fail "pgvs3 clone CLI"
[ "$(s3 "$A/$CLONE_CLI/inline.txt")" = 'inline-val' ] || fail "CLI cloned bucket read"

# Cross-bucket CopyObject (chunked and inline).
expect 200 -X PUT -H "x-amz-copy-source: /$SRC_B/chunked.bin" "$A/$CLONE_CLI/cross-copy.bin"
s3 "$B/$CLONE_CLI/cross-copy.bin" -o "$TMP/cross-copy.bin"
cmp "$TMP/one.bin" "$TMP/cross-copy.bin" || fail "cross-bucket CopyObject bytes"

# Non-empty standard DELETE fails with 409; force DELETE drops the schema immediately.
expect 409 -X DELETE "$A/$CLONE_HTTP"
expect 204 -X DELETE "$A/$CLONE_HTTP?force=true"
expect 404 "$A/$CLONE_HTTP/inline.txt"

# Single-bucket `pg_dump -n` snapshot and `pg_restore` after dropping the bucket.
docker exec "$PGVS3_TEST_PG_CONTAINER" pg_dump -U postgres -Fc -n "\"s3b_$CLONE_CLI\"" pgvs3 > "$TMP/bucket.dump"
.tmp/pgvs3/pgvs3 drop --url "$PGVS3_TEST_DB_URL" --force "$CLONE_CLI" >/dev/null || fail "pgvs3 drop --force"
expect 404 "$A/$CLONE_CLI/inline.txt"
docker exec -i "$PGVS3_TEST_PG_CONTAINER" pg_restore --single-transaction -U postgres -d pgvs3 < "$TMP/bucket.dump"
# Restored schemas are immediately live (no manual adoption step required) and
# carry their own metadata in <schema>.meta; reap/maintenance reconciles the
# catalog and file_ids sequence automatically.
expect 200 -I "$A/$CLONE_CLI"
[ "$(s3 "$B/$CLONE_CLI/inline.txt")" = 'inline-val' ] || fail "restored bucket inline read"
s3 "$B/$CLONE_CLI/cross-copy.bin" -o "$TMP/restored.bin"
cmp "$TMP/one.bin" "$TMP/restored.bin" || fail "restored bucket chunked read"
.tmp/pgvs3/pgvs3 reap --url "$PGVS3_TEST_DB_URL" --grace 0s >/dev/null
.tmp/pgvs3/pgvs3 drop --url "$PGVS3_TEST_DB_URL" --force "$SRC_B" >/dev/null
.tmp/pgvs3/pgvs3 drop --url "$PGVS3_TEST_DB_URL" --force "$CLONE_CLI" >/dev/null

# A catalog row whose schema vanished must not block layout checks, and
# maintenance (`reap`) prunes it automatically.
MISSING="missing-$RUN"
expect 200 -X PUT "$A/$MISSING"
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -q -c \
  "SET client_min_messages=warning; DROP SCHEMA \"s3b_$MISSING\" CASCADE"
doctor_out=$(.tmp/pgvs3/pgvs3 doctor --url "$PGVS3_TEST_DB_URL")
echo "$doctor_out" | grep -q "missing_schemas=1" || fail "missing schema not reported: $doctor_out"
.tmp/pgvs3/pgvs3 reap --url "$PGVS3_TEST_DB_URL" --grace 0s >/dev/null || fail "reap with a missing schema"
doctor_out=$(.tmp/pgvs3/pgvs3 doctor --url "$PGVS3_TEST_DB_URL")
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
import os, time, urllib.error, urllib.request, boto3
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
time.sleep(2)
assert pc.get_object(Bucket=gen, Key=f'{run}/shared-copy')['Body'].read()==b'shared'
for i in range(100):
    pc.put_object(Bucket=gen, Key=f'{run}/churn', Body=b'x'*1024)
    if i % 5 == 0:
        pc.delete_object(Bucket=gen, Key=f'{run}/churn')
print('boto3 ok')
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

assert ra.ping() is True
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

# NX, XX, GET, EX, PX, TTL, PERSIST.
assert ra.set('greeting', b'nope', nx=True) is None
assert ra.set('missing-xx', b'nope', xx=True) is None
old = ra.set('greeting', b'updated-resp', xx=True, get=True)
assert old == b'hello-from-resp'
assert rb.get('greeting') == b'updated-resp'

assert ra.set('ttl-key', b'ephemeral', px=3000) is True
pttl = rb.pttl('ttl-key')
assert 0 < pttl <= 3000, pttl
assert ra.persist('ttl-key') is True
assert rb.ttl('ttl-key') == -1

assert ra.set('short-ttl', b'bye', px=150) is True
time.sleep(0.25)
assert rb.get('short-ttl') is None
assert rb.ttl('short-ttl') == -2

# Atomic INCR / DECR / INCRBY.
assert ra.incr('counter') == 1
assert rb.incrby('counter', 41) == 42
assert ra.decr('counter') == 41

# MSET / MGET / EXISTS / DEL / KEYS / SCAN / INFO.
assert ra.mset({'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}) is True
assert rb.mget(['k1', 'missing', 'k3']) == [b'v1', None, b'v3']
assert rb.exists('k1', 'k2', 'missing') == 2
assert sorted(rb.keys('k*')) == [b'k1', b'k2', b'k3']
cursor, scanned = rb.scan(cursor=0, match='k*', count=10)
assert cursor == 0 and sorted(scanned) == [b'k1', b'k2', b'k3'], (cursor, scanned)
assert rb.info().get('db0', {}).get('keys', 0) >= 3
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
assert rb.getrange('big-blob', -32, -1) == big[-32:]

# Pipelined commands (coalesced GET batching).
pipe = ra.pipeline(transaction=False)
for i in range(64):
    pipe.set(f'p:{i}', f'val-{i}'.encode())
assert all(pipe.execute())
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
print('resp ok')
PY

# Verify that the 256 KiB compressible object in `s3b_cache.chunks` was stored compressed.
comp_bytes=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  'SELECT sum(octet_length(c.data)) FROM "s3c_cache".chunks c JOIN "s3c_cache".objects o ON o.file_id = c.file_id WHERE o.key = '\''big-blob'\''')
[ "$comp_bytes" -lt 4096 ] || fail "expected 256 KiB compressible chunks < 4096 B, got $comp_bytes"

# --- SQS queue protocol ----------------------------------------------------
E2E_ENDPOINT="$A" E2E_KEY="$PGVS3_ACCESS_KEY" E2E_SECRET="$PGVS3_SECRET_KEY" E2E_RUN="$RUN" \
mise exec uv -- uv run --quiet --no-project --with boto3 python - <<'PY'
import json, os, time, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
ep = os.environ['E2E_ENDPOINT']
sqs = boto3.client('sqs', endpoint_url=ep, aws_access_key_id=os.environ['E2E_KEY'],
    aws_secret_access_key=os.environ['E2E_SECRET'], region_name='us-east-1',
    config=Config(retries={'max_attempts': 0}))
name = 'q-' + os.environ['E2E_RUN'].lower().replace('_', '-')
url = sqs.create_queue(QueueName=name, Attributes={'VisibilityTimeout': '2'})['QueueUrl']
assert sqs.get_queue_url(QueueName=name)['QueueUrl'] == url
assert any(u.endswith('/' + name) for u in sqs.list_queues(QueueNamePrefix='q-').get('QueueUrls', []))
assert sqs.send_message(QueueUrl=url, MessageBody='hello-sqs')['MD5OfMessageBody']
batch = sqs.send_message_batch(QueueUrl=url, Entries=[
    {'Id': 'a', 'MessageBody': 'one'}, {'Id': 'b', 'MessageBody': 'two'}])
assert len(batch['Successful']) == 2, batch
msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=2)['Messages']
bodies = sorted(m['Body'] for m in msgs)
assert bodies == ['hello-sqs', 'one', 'two'], bodies
time.sleep(2.5)
again = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)['Messages']
assert again, 'messages did not become visible again'
for m in again:
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m['ReceiptHandle'])
attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=['All'])['Attributes']
assert int(attrs['ApproximateNumberOfMessages']) == 0, attrs

# A superseded receipt is rejected instead of silently acknowledging someone
# else's delivery; repeating the winning delete stays idempotent.
stale_name = 'stale-' + os.environ['E2E_RUN'].lower().replace('_', '-')
stale_url = sqs.create_queue(QueueName=stale_name, Attributes={'VisibilityTimeout': '1'})['QueueUrl']
sqs.send_message(QueueUrl=stale_url, MessageBody='stale-ack')
h1 = sqs.receive_message(QueueUrl=stale_url, MaxNumberOfMessages=1)['Messages'][0]['ReceiptHandle']
time.sleep(1.2)
h2 = sqs.receive_message(QueueUrl=stale_url, MaxNumberOfMessages=1)['Messages'][0]['ReceiptHandle']
assert h1 != h2, (h1, h2)
try:
    sqs.delete_message(QueueUrl=stale_url, ReceiptHandle=h1)
except ClientError as e:
    assert e.response['Error']['Code'] == 'ReceiptHandleIsInvalid', e.response
else:
    raise AssertionError('stale receipt was accepted')
dbatch = sqs.delete_message_batch(QueueUrl=stale_url, Entries=[
    {'Id': 'stale', 'ReceiptHandle': h1},
    {'Id': 'live', 'ReceiptHandle': h2},
])
assert [e['Id'] for e in dbatch.get('Successful', [])] == ['live'], dbatch
assert len(dbatch.get('Failed', [])) == 1 and dbatch['Failed'][0]['Id'] == 'stale', dbatch
assert dbatch['Failed'][0]['Code'] == 'ReceiptHandleIsInvalid', dbatch
sqs.delete_message(QueueUrl=stale_url, ReceiptHandle=h2)
sqs.delete_queue(QueueUrl=stale_url)

# RedrivePolicy moves a message to the dead-letter queue on the receive that
# exceeds maxReceiveCount, and that receive skips past it to any live message.
run = os.environ['E2E_RUN'].lower().replace('_', '-')
dl_name = 'dl-' + run
dl_url = sqs.create_queue(QueueName=dl_name)['QueueUrl']
src_name = 'src-' + run
redrive = json.dumps({'deadLetterTargetArn': 'arn:aws:sqs:us-east-1:000000000000:' + dl_name,
                      'maxReceiveCount': '1'})
src_url = sqs.create_queue(QueueName=src_name,
    Attributes={'VisibilityTimeout': '0', 'RedrivePolicy': redrive})['QueueUrl']
sqs.send_message(QueueUrl=src_url, MessageBody='doomed')
first = sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=1).get('Messages', [])
assert first and first[0]['Body'] == 'doomed', first
sqs.send_message(QueueUrl=src_url, MessageBody='survivor')
second = sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=1).get('Messages', [])
assert len(second) == 1 and second[0]['Body'] == 'survivor', second
sqs.delete_message(QueueUrl=src_url, ReceiptHandle=second[0]['ReceiptHandle'])
dead = sqs.receive_message(QueueUrl=dl_url, MaxNumberOfMessages=1).get('Messages', [])
assert dead and dead[0]['Body'] == 'doomed', dead
sqs.delete_message(QueueUrl=dl_url, ReceiptHandle=dead[0]['ReceiptHandle'])
sqs.delete_queue(QueueUrl=src_url)
sqs.delete_queue(QueueUrl=dl_url)

# FIFO: one message per group per receive, dedup IDs, sequence numbers, and
# content-based deduplication.
fifo_name = 'f-' + run + '.fifo'
fifo_url = sqs.create_queue(QueueName=fifo_name,
    Attributes={'FifoQueue': 'true', 'ContentBasedDeduplication': 'true'})['QueueUrl']
first = sqs.send_message(QueueUrl=fifo_url, MessageBody='a1', MessageGroupId='g',
    MessageDeduplicationId='d1')
dup = sqs.send_message(QueueUrl=fifo_url, MessageBody='a1-dup', MessageGroupId='g',
    MessageDeduplicationId='d1')
assert first['MessageId'] == dup['MessageId'], (first, dup)
assert first.get('SequenceNumber'), first
sqs.send_message(QueueUrl=fifo_url, MessageBody='a2', MessageGroupId='g',
    MessageDeduplicationId='d2')
sqs.send_message(QueueUrl=fifo_url, MessageBody='b1', MessageGroupId='h',
    MessageDeduplicationId='d3')
msgs = sqs.receive_message(QueueUrl=fifo_url, MaxNumberOfMessages=10,
    WaitTimeSeconds=1).get('Messages', [])
assert sorted(m['Body'] for m in msgs) == ['a1', 'b1'], msgs
for m in msgs:
    if m['Body'] == 'a1':
        sqs.delete_message(QueueUrl=fifo_url, ReceiptHandle=m['ReceiptHandle'])
nxt = sqs.receive_message(QueueUrl=fifo_url, MaxNumberOfMessages=10).get('Messages', [])
assert [m['Body'] for m in nxt] == ['a2'], nxt
for m in nxt:
    sqs.delete_message(QueueUrl=fifo_url, ReceiptHandle=m['ReceiptHandle'])
again = sqs.send_message(QueueUrl=fifo_url, MessageBody='a1-again', MessageGroupId='g',
    MessageDeduplicationId='d1')
assert again['MessageId'] == first['MessageId'], again
c1 = sqs.send_message(QueueUrl=fifo_url, MessageBody='content', MessageGroupId='c')
c2 = sqs.send_message(QueueUrl=fifo_url, MessageBody='content', MessageGroupId='c')
assert c1['MessageId'] == c2['MessageId'], (c1, c2)
fattrs = sqs.get_queue_attributes(QueueUrl=fifo_url, AttributeNames=['All'])['Attributes']
assert fattrs.get('FifoQueue') == 'true', fattrs
sqs.delete_queue(QueueUrl=fifo_url)

sqs.purge_queue(QueueUrl=url)
sqs.delete_queue(QueueUrl=url)
try:
    sqs.get_queue_url(QueueName=name)
    raise AssertionError('queue still exists')
except sqs.exceptions.QueueDoesNotExist:
    pass
print('sqs ok')
PY

# Acks must use the receipt index, never a sequential scan.
QIDX="qidx-$RUN"
curl -fsS -X POST -H 'Content-Type: application/x-amz-json-1.0' \
  -H 'X-Amz-Target: AmazonSQS.CreateQueue' \
  -d "{\"QueueName\":\"$QIDX\"}" "$A/" >/dev/null
plan=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c "
  SET enable_seqscan = off;
  EXPLAIN (COSTS OFF) DELETE FROM \"s3q_$QIDX\".messages WHERE receipt = 'x';
")
echo "$plan" | grep -q "messages_receipt" || fail "SQS ack does not use messages_receipt: $plan"
# Legacy Query/XML protocol (form-encoded POST body).
curl -fsS -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "Action=SendMessage&QueueUrl=$A/$QIDX&MessageBody=query-xml" "$A/" | grep -q '<SendMessageResponse' || fail "SQS Query SendMessage XML"
curl -fsS -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "Action=ReceiveMessage&QueueUrl=$A/$QIDX&MaxNumberOfMessages=1" "$A/" | grep -q '<Body>query-xml</Body>' || fail "SQS Query ReceiveMessage XML"
curl -fsS -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "Action=PurgeQueue&QueueUrl=$A/$QIDX" "$A/" | grep -q '<PurgeQueueResponse' || fail "SQS Query PurgeQueue XML"
.tmp/pgvs3/pgvs3 drop --url "$PGVS3_TEST_DB_URL" --kind queue --force "$QIDX" >/dev/null

# --- layout v10 -> v11 in-place migration -----------------------------------
V10_DB="pgvs3_v10_${RUN//-/_}"
V10_URL="${PGVS3_TEST_DB_URL%/*}/$V10_DB"
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
  -c "DROP DATABASE IF EXISTS \"$V10_DB\"" -c "CREATE DATABASE \"$V10_DB\"" >/dev/null
docker exec -i "$PGVS3_TEST_PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d "$V10_DB" >/dev/null <<'SQL'
CREATE SCHEMA s3p;
CREATE TABLE s3p.layout (version int4 NOT NULL);
INSERT INTO s3p.layout VALUES (10);
CREATE SEQUENCE s3p.file_ids CACHE 64;
CREATE TABLE s3p.buckets (
  name        text COLLATE "C" PRIMARY KEY,
  schema_name text COLLATE "C" NOT NULL UNIQUE,
  created_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bucket_name_shape CHECK (name ~ '^[a-z0-9._-]{1,58}$'));
CREATE SCHEMA "s3b_legacy";
CREATE TABLE "s3b_legacy".objects (
  key           text COLLATE "C" PRIMARY KEY,
  file_id       bigint,
  size          int8 NOT NULL,
  sha256        bytea NOT NULL,
  etag          text NOT NULL,
  user_metadata text[] NOT NULL DEFAULT '{}'::text[],
  content_type  text NOT NULL DEFAULT 'application/octet-stream',
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz,
  parts         int8[],
  part_ends     int8[],
  inline_data   bytea STORAGE EXTERNAL);
CREATE TABLE "s3b_legacy".chunks (
  file_id int8 NOT NULL, no int4 NOT NULL, data bytea STORAGE EXTERNAL NOT NULL,
  PRIMARY KEY (file_id, no));
CREATE TABLE "s3b_legacy".uploads (
  upload_id          text PRIMARY KEY,
  key                text COLLATE "C" NOT NULL,
  checksum_algorithm text,
  user_metadata      text[] NOT NULL DEFAULT '{}'::text[],
  content_type       text NOT NULL DEFAULT 'application/octet-stream',
  created_at         timestamptz NOT NULL DEFAULT now());
CREATE TABLE "s3b_legacy".upload_parts (
  upload_id text NOT NULL REFERENCES "s3b_legacy".uploads ON DELETE CASCADE,
  part_no   int4 NOT NULL, file_id int8 NOT NULL, size int8 NOT NULL,
  sha256 bytea NOT NULL, md5 bytea NOT NULL, checksum text,
  PRIMARY KEY (upload_id, part_no));
CREATE TABLE "s3b_legacy".object_part_checksums (
  key text COLLATE "C" NOT NULL, part_no int4 NOT NULL, file_id int8 NOT NULL,
  algorithm text NOT NULL, value text NOT NULL, PRIMARY KEY (key, part_no),
  FOREIGN KEY (key) REFERENCES "s3b_legacy".objects (key)
    ON UPDATE CASCADE ON DELETE CASCADE);
CREATE TABLE "s3b_legacy".garbage (
  file_id int8 PRIMARY KEY, queued_at timestamptz NOT NULL DEFAULT now());
INSERT INTO s3p.buckets (name, schema_name) VALUES ('legacy', 's3b_legacy');
INSERT INTO "s3b_legacy".objects (key, size, sha256, etag, inline_data)
  VALUES ('inline.txt', 5, decode(repeat('00', 32), 'hex'),
          '0123456789abcdef0123456789abcdef', 'hello');
INSERT INTO "s3b_legacy".objects (key, file_id, size, sha256, etag)
  VALUES ('chunked.bin', 42, 5, decode(repeat('11', 32), 'hex'),
          'fedcba9876543210fedcba9876543210');
INSERT INTO "s3b_legacy".chunks VALUES (42, 0, 'chunk');
SELECT setval('s3p.file_ids', 42);
SQL
migrate_out=$(.tmp/pgvs3/pgvs3 migrate --url "$V10_URL")
echo "$migrate_out" | grep -q "migrated_buckets=1 layout=11" || fail "migrate: $migrate_out"
.tmp/pgvs3/pgvs3 migrate --url "$V10_URL" | grep -q "migrated_buckets=0 layout=11" || fail "migrate is not idempotent"
v10_state=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d "$V10_DB" -At -c "
  SELECT (SELECT version FROM s3p.layout) || '|' ||
         (SELECT count(*) FROM s3p.namespaces WHERE kind = 'bucket' AND name = 'legacy') || '|' ||
         (SELECT count(*) FROM \"s3b_legacy\".chunks) || '|' ||
         (SELECT count(*) FROM information_schema.columns WHERE table_schema = 's3b_legacy'
            AND table_name = 'objects' AND column_name = 'origin_etag') || '|' ||
         (to_regclass('s3p.v10_buckets') IS NOT NULL) || '|' ||
         (SELECT string_agg(key || ':' || coalesce(encode(inline_data, 'escape'), '-') || ':' ||
                            coalesce(file_id::text, '-'), ',' ORDER BY key)
            FROM \"s3b_legacy\".objects);")
[ "$v10_state" = '11|1|1|1|true|chunked.bin:-:42,inline.txt:hello:-' ] || fail "v10 migration state: $v10_state"
doctor10=$(.tmp/pgvs3/pgvs3 doctor --url "$V10_URL")
echo "$doctor10" | grep -q "layout=11" || fail "migrated layout: $doctor10"
echo "$doctor10" | grep -q "buckets=1" || fail "migrated bucket count: $doctor10"
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d postgres -c "DROP DATABASE \"$V10_DB\"" >/dev/null

# --- S3 cache tier stored in PostgreSQL (no external gateway state) ---------
ORIGIN_DIR="$TMP/origin"
mkdir -p "$ORIGIN_DIR"
cat > "$TMP/origin.yaml" <<YAML
type: filesystem
config:
  directory: $ORIGIN_DIR
YAML
# Persist the origin config for bucket `edge` inside PostgreSQL; both running
# gateways ($A and $B, started without --origin-config) discover it from PG.
.tmp/pgvs3/pgvs3 origin --url "$PGVS3_TEST_DB_URL" --bucket edge \
  --cache-namespace "ocache-$RUN" --cache-ttl 0s set "$TMP/origin.yaml" | grep -q "origin_set=edge" || fail "pgvs3 origin set"
.tmp/pgvs3/pgvs3 origin --url "$PGVS3_TEST_DB_URL" list | grep -q "origin=edge" || fail "pgvs3 origin list"
sleep 1.2
OCACHE="$A"
OCACHE_ENDPOINT_A="$A" OCACHE_ENDPOINT_B="$B" OCACHE_DIR="$ORIGIN_DIR" OCACHE_RUN="$RUN" \
E2E_KEY="$PGVS3_ACCESS_KEY" E2E_SECRET="$PGVS3_SECRET_KEY" \
mise exec uv -- uv run --quiet --no-project --with boto3 python - <<'PY'
import os, boto3
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
put_res = ca.put_object(Bucket=b, Key=k, Body=body, ContentType='application/octet-stream', Metadata={'tier': 'write-through'})
assert put_res.get('ETag', '').strip('"'), put_res
# Write-through across gateways: written on A, immediately read on B as a cache HIT without re-pulling from the origin.
get_res = cb.get_object(Bucket=b, Key=k)
assert get_res['Body'].read() == body
assert get_res['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', get_res['ResponseMetadata']['HTTPHeaders']
assert {mk.lower(): mv for mk, mv in get_res.get('Metadata', {}).items()} == {'tier': 'write-through'}, get_res.get('Metadata')
head = cb.head_object(Bucket=b, Key=k)
assert head['ContentLength'] == len(body)
assert head['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', head['ResponseMetadata']['HTTPHeaders']
try:
    cb.get_object(Bucket=b, Key=k, IfNoneMatch=head['ETag'])
    raise AssertionError('expected 304 Not Modified')
except cb.exceptions.ClientError as exc:
    assert exc.response['Error']['Code'] == '304', exc.response
assert cb.get_object(Bucket=b, Key=k, Range='bytes=0-15')['Body'].read() == body[:16]
delim = cb.list_objects_v2(Bucket=b, Prefix='', Delimiter='/')
assert any(cp['Prefix'] == os.environ['OCACHE_RUN'] + '/' for cp in delim.get('CommonPrefixes', [])), delim
# CopyObject through the cache tier writes through to the origin and caches the destination immediately.
copy_k = os.environ['OCACHE_RUN'] + '/copied.bin'
ca.copy_object(Bucket=b, Key=copy_k, CopySource={'Bucket': b, 'Key': k})
copy_get = cb.get_object(Bucket=b, Key=copy_k)
assert copy_get['Body'].read() == body
assert copy_get['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', copy_get['ResponseMetadata']['HTTPHeaders']
assert os.path.exists(os.path.join(os.environ['OCACHE_DIR'], b, copy_k))
# Multipart upload through the cache tier writes through to the origin and is immediately a cache HIT on B.
mp_k = os.environ['OCACHE_RUN'] + '/mp.bin'
mp_body = b'part-payload-' * 1000
up = ca.create_multipart_upload(Bucket=b, Key=mp_k)
part = ca.upload_part(Bucket=b, Key=mp_k, UploadId=up['UploadId'], PartNumber=1, Body=mp_body)
ca.complete_multipart_upload(Bucket=b, Key=mp_k, UploadId=up['UploadId'],
    MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
mp_get = cb.get_object(Bucket=b, Key=mp_k)
assert mp_get['Body'].read() == mp_body
assert mp_get['ResponseMetadata']['HTTPHeaders'].get('x-cache') == 'HIT', mp_get['ResponseMetadata']['HTTPHeaders']
assert os.path.exists(os.path.join(os.environ['OCACHE_DIR'], b, mp_k))
# 0-byte object through the cache tier must report Content-Length: 0.
empty_k = os.environ['OCACHE_RUN'] + '/empty.bin'
ca.put_object(Bucket=b, Key=empty_k, Body=b'')
assert cb.head_object(Bucket=b, Key=empty_k)['ContentLength'] == 0
assert cb.get_object(Bucket=b, Key=empty_k)['Body'].read() == b''
ca.delete_objects(Bucket=b, Delete={'Objects': [{'Key': empty_k}, {'Key': copy_k}, {'Key': mp_k}]})
assert not os.path.exists(os.path.join(os.environ['OCACHE_DIR'], b, mp_k))
p = os.path.join(os.environ['OCACHE_DIR'], b, k)
open(p, 'wb').write(b'origin-updated')
assert cb.get_object(Bucket=b, Key=k)['Body'].read() == b'origin-updated'
keys = [o['Key'] for o in cb.list_objects_v2(Bucket=b, Prefix=os.environ['OCACHE_RUN'] + '/').get('Contents', [])]
assert k in keys, keys
ca.delete_object(Bucket=b, Key=k)
assert not os.path.exists(p)
print('s3cache ok')
PY

# Quota is enforced on the write path, within ~1s of a change.
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  "UPDATE s3p.namespaces SET quota_bytes = 1 WHERE kind = 'cache' AND name = 'cache'" >/dev/null
sleep 1.2
if curl -fsS -X PUT --data-binary 'quota' "$A/_cache/cache/quota-test" >/dev/null 2>&1; then
  fail "quota was not enforced on a cache write"
fi
docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  "UPDATE s3p.namespaces SET quota_bytes = NULL WHERE kind = 'cache' AND name = 'cache'" >/dev/null
sleep 1.2
curl -fsS -X PUT --data-binary 'quota' "$A/_cache/cache/quota-test" >/dev/null || fail "cache write after quota reset"

# A cold range on a large origin object must be passed through, not cached whole.
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
rows=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  "SELECT count(*) FROM \"s3c_ocache-$RUN\".objects WHERE key = 'edge/$RUN/big.bin'")
[ "$rows" = 0 ] || fail "cold range cached the whole object ($rows rows)"
curl -fsS "$OCACHE/edge/$RUN/big.bin" -o /dev/null
rows=$(docker exec "$PGVS3_TEST_PG_CONTAINER" psql -U postgres -d pgvs3 -At -c \
  "SELECT count(*) FROM \"s3c_ocache-$RUN\".objects WHERE key = 'edge/$RUN/big.bin'")
[ "$rows" = 1 ] || fail "full GET was not cached ($rows rows)"
curl -fsSI -H 'Range: bytes=1048576-1049599' "$B/edge/$RUN/big.bin" | grep -qi 'x-cache: HIT' || fail "cached 16 MiB range did not hit cache"
stat_out=$(.tmp/pgvs3/pgvs3 stat --url "$PGVS3_TEST_DB_URL" --kind cache --name "ocache-$RUN")
echo "$stat_out" | grep -Eq 'chunks=[1-9]' || fail "pgvs3 stat chunks: $stat_out"
.tmp/pgvs3/pgvs3 origin --url "$PGVS3_TEST_DB_URL" --bucket edge clear | grep -q "origin_cleared=edge" || fail "pgvs3 origin clear"

# --- DuckDB 2.0: httpfs, Parquet, DuckLake --------------------------------
export E2E_BUCKET=$BUCKET E2E_RUN=$RUN E2E_ENDPOINT=$A E2E_KEY=$PGVS3_ACCESS_KEY \
       E2E_SECRET=$PGVS3_SECRET_KEY E2E_PG_PORT=$PGVS3_TEST_PG_PORT
mise exec uv -- uv run --quiet --no-project --with duckdb==2.0.0 python - <<'PY'
import os, duckdb
c=duckdb.connect()
c.execute("INSTALL httpfs; LOAD httpfs;")
c.execute("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;")
b=os.environ["E2E_BUCKET"]; run=os.environ["E2E_RUN"]; ep=os.environ["E2E_ENDPOINT"]
c.execute(f"""CREATE SECRET s3s (TYPE S3, KEY_ID '{os.environ['E2E_KEY']}',
 SECRET '{os.environ['E2E_SECRET']}', REGION 'us-east-1', ENDPOINT '{ep[7:]}',
 URL_STYLE 'path', USE_SSL false)""")
c.execute(f"COPY (SELECT range AS i FROM range(10000)) TO 's3://{b}/{run}/duck.parquet' (FORMAT PARQUET)")
assert c.execute(f"SELECT count(*), sum(i) FROM 's3://{b}/{run}/duck.parquet'").fetchone()==(10000, 49995000)
c.execute(f"""CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT {os.environ['E2E_PG_PORT']},
 DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres')""")
c.execute(f"ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 's3://{b}/{run}/lake/')")
c.execute(f"CREATE TABLE lake.t_{run.replace('-','_')}(i INTEGER)")
c.execute(f"INSERT INTO lake.t_{run.replace('-','_')} SELECT range FROM range(1000)")
assert c.execute(f"SELECT count(*) FROM lake.t_{run.replace('-','_')}").fetchone()[0]==1000
print("duckdb 2.0 ok")
PY

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
print("duckdb_object_storage ok")
PY

echo "contract ok"
