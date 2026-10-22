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

s3 -X PUT "$A/$BUCKET" >/dev/null || fail "create bucket"

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
<Delete><Object><Key>$RUN/page/0</Key></Object><Object><Key>$RUN/page/1</Key></Object><Object><Key>$RUN/page/2</Key></Object></Delete>
XML
expect 200 -X POST --data-binary @"$TMP/delete.xml" "$A/$BUCKET?delete"
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

# --- S3 Express One Zone surface -------------------------------------------
EXPR="demo-$RUN--use1-az1--x-s3"
ED="$A/$EXPR"
cat > "$TMP/bucket.xml" <<'XML'
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location><Type>AvailabilityZone</Type><Name>use1-az1</Name></Location><Bucket><Type>Directory</Type><DataRedundancy>SingleAvailabilityZone</DataRedundancy></Bucket></CreateBucketConfiguration>
XML
expect 200 -X PUT --data-binary @"$TMP/bucket.xml" "$ED"
s3 -I "$ED" | tr -d '\r' | grep -qi 'x-amz-bucket-location-type: AvailabilityZone' || fail "directory bucket headers"

# CreateSession and short-lived session credentials.
session=$(s3 "$ED?session")
sak=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("AccessKeyId")][0])' <<<"$session")
ssk=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("SecretAccessKey")][0])' <<<"$session")
stok=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("SessionToken")][0])' <<<"$session")
sesscode() { local ak=$1 sk=$2 tok=$3; shift 3; curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$ak:$sk" -H "x-amz-s3session-token: $tok" -o /dev/null -w '%{http_code}' "$@"; }
[ "$(sesscode "$sak" "$ssk" "$stok" -X PUT --data-binary 'session' "$ED/session.txt")" = 200 ] || fail "session PUT"
[ "$(sesscode "$sak" "$ssk" "$stok" "$ED/session.txt")" = 200 ] || fail "session GET"
ro=$(s3 -H 'x-amz-create-session-mode: ReadOnly' "$ED?session")
rak=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("AccessKeyId")][0])' <<<"$ro")
rsk=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("SecretAccessKey")][0])' <<<"$ro")
rtok=$(python3 -c 'import sys,xml.etree.ElementTree as E; r=E.fromstring(sys.stdin.read()); print([e.text for e in r.iter() if e.tag.endswith("SessionToken")][0])' <<<"$ro")
[ "$(sesscode "$rak" "$rsk" "$rtok" -X PUT --data-binary 'no' "$ED/ro.txt")" = 403 ] || fail "read-only session wrote"
[ "$(sesscode "$rak" "$rsk" "$rtok" "$ED/session.txt")" = 200 ] || fail "read-only session read"

# Copy, delete the source, and let the reaper run; the shared file must survive.
expect 200 -X PUT --data-binary 'copy me' "$ED/src"
expect 200 -X PUT -H 'x-amz-copy-source: /src' "$ED/copy"
[ "$(s3 "$ED/copy")" = 'copy me' ] || fail "copy bytes"
expect 204 -X DELETE "$ED/src"
sleep 2
[ "$(s3 "$ED/copy")" = 'copy me' ] || fail "reaper removed a shared file"

# Rename and append.
expect 200 -X PUT --data-binary 'rename' "$ED/old"
expect 200 -X PUT -H 'x-amz-rename-source: /old' "$ED/new?renameObject"
[ "$(s3 "$ED/new")" = rename ] || fail "rename bytes"
expect 404 "$ED/old"
expect 200 -X PUT --data-binary 'token rename' "$ED/old-token"
expect 200 -X PUT -H 'x-amz-rename-source: /old-token' -H 'x-amz-client-token: token-1' "$ED/new-token?renameObject"
expect 200 -X PUT -H 'x-amz-rename-source: /old-token' -H 'x-amz-client-token: token-1' "$ED/new-token?renameObject"
expect 200 -X PUT --data-binary 'other' "$ED/other-token"
expect 400 -X PUT -H 'x-amz-rename-source: /other-token' -H 'x-amz-client-token: token-1' "$ED/other-dst?renameObject"
expect 200 -X PUT --data-binary 'base' "$ED/app"
expect 200 -X PUT -H 'x-amz-write-offset-bytes: 4' --data-binary ' appended' "$ED/app"
[ "$(s3 "$ED/app")" = 'base appended' ] || fail "append bytes"
expect 409 -X PUT -H 'x-amz-write-offset-bytes: 1' --data-binary 'x' "$ED/app"
expect 200 -X PUT --data-binary '' "$ED/empty-spill"
head -c 100000 /dev/zero > "$TMP/empty-append"
expect 200 -X PUT -H 'x-amz-write-offset-bytes: 0' --data-binary @"$TMP/empty-append" "$ED/empty-spill"
[ "$(s3 "$ED/empty-spill" | wc -c)" = 100000 ] || fail "empty inline append spill"

# CRC64NVME / FULL_OBJECT checksum on a single PUT.
crc64=$(python3 -c 'import base64,struct; print(base64.b64encode(struct.pack(">Q",0xAE8B14860A799888)).decode())')
expect 200 -X PUT -H "x-amz-checksum-crc64nvme: $crc64" -H 'x-amz-sdk-checksum-algorithm: CRC64NVME' -H 'x-amz-checksum-type: FULL_OBJECT' --data-binary '123456789' "$ED/crc64"
[ "$(s3 "$ED/crc64")" = 123456789 ] || fail "crc64 body"
expect 400 -X PUT -H "x-amz-checksum-crc64nvme: AAAAAAAAAAA=" -H 'x-amz-sdk-checksum-algorithm: CRC64NVME' --data-binary '123456789' "$ED/crc64-bad"

# Attributes, multipart copy and multipart listings.
s3 -H 'x-amz-object-attributes: ETag,ObjectSize,Checksum,ObjectParts,StorageClass' "$ED/app?attributes" | grep -q ObjectSize || fail "attributes"
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

# The same surface through an SDK: session auth, copy, rename, append.
EXPR="$EXPR" E2E_RUN="$RUN" E2E_ENDPOINT="$A" E2E_KEY="$PGVS3_ACCESS_KEY" E2E_SECRET="$PGVS3_SECRET_KEY" \
mise exec uv -- uv run --quiet --no-project --with boto3 python - <<'PY'
import os, time, urllib.request, boto3
from botocore.config import Config
s3=boto3.client('s3', endpoint_url=os.environ['E2E_ENDPOINT'],
    aws_access_key_id=os.environ['E2E_KEY'], aws_secret_access_key=os.environ['E2E_SECRET'],
    region_name='us-east-1', config=Config(s3={'addressing_style':'virtual'}, retries={'max_attempts':0}))
b=os.environ['EXPR']
s3.put_object(Bucket=b, Key='sdk.txt', Body=b'sdk')
assert s3.get_object(Bucket=b, Key='sdk.txt')['Body'].read()==b'sdk'
s3.copy_object(Bucket=b, Key='sdk.copy', CopySource={'Bucket':b,'Key':'sdk.txt'})
assert s3.get_object(Bucket=b, Key='sdk.copy')['Body'].read()==b'sdk'
s3.rename_object(Bucket=b, Key='sdk.renamed', RenameSource='/sdk.copy')
s3.put_object(Bucket=b, Key='sdk.renamed', Body=b' appended', WriteOffsetBytes=3)
assert s3.get_object(Bucket=b, Key='sdk.renamed')['Body'].read()==b'sdk appended'
assert s3.get_object_attributes(Bucket=b, Key='sdk.renamed', ObjectAttributes=['ObjectSize'])['ObjectSize']==12
pc=boto3.client('s3', endpoint_url=os.environ['E2E_ENDPOINT'],
    aws_access_key_id=os.environ['E2E_KEY'], aws_secret_access_key=os.environ['E2E_SECRET'],
    region_name='us-east-1', config=Config(signature_version='s3v4', s3={'addressing_style':'path'}))
run=os.environ['E2E_RUN']; gen='pgvs3-contract'
pc.put_object(Bucket=gen, Key=f'{run}/presigned', Body=b'presigned')
u=pc.generate_presigned_url('get_object', Params={'Bucket':gen,'Key':f'{run}/presigned'}, ExpiresIn=60)
assert urllib.request.urlopen(u).read()==b'presigned'
u=pc.generate_presigned_url('put_object', Params={'Bucket':gen,'Key':f'{run}/presigned-put'}, ExpiresIn=60)
assert urllib.request.urlopen(urllib.request.Request(u,data=b'put',method='PUT')).status==200
pc.put_object(Bucket=gen, Key=f'{run}/spill', Body=b'a'*60000)
pc.put_object(Bucket=gen, Key=f'{run}/spill', Body=b'b'*10000, WriteOffsetBytes=60000)
assert pc.get_object(Bucket=gen, Key=f'{run}/spill')['Body'].read()==b'a'*60000+b'b'*10000
pc.put_object(Bucket=gen, Key=f'{run}/over-inline', Body=b'old')
up=pc.create_multipart_upload(Bucket=gen, Key=f'{run}/over-inline')
part=pc.upload_part(Bucket=gen, Key=f'{run}/over-inline', UploadId=up['UploadId'], PartNumber=1, Body=b'n'*1048576)
pc.complete_multipart_upload(Bucket=gen, Key=f'{run}/over-inline', UploadId=up['UploadId'], MultipartUpload={'Parts':[{'PartNumber':1,'ETag':part['ETag']}]})
assert pc.get_object(Bucket=gen, Key=f'{run}/over-inline')['Body'].read()==b'n'*1048576
up=pc.create_multipart_upload(Bucket=gen, Key=f'{run}/part-crc', ChecksumAlgorithm='SHA256', ChecksumType='COMPOSITE')
body=b'checksum part'
part=pc.upload_part(Bucket=gen, Key=f'{run}/part-crc', UploadId=up['UploadId'], PartNumber=1, Body=body, ChecksumAlgorithm='SHA256')
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
print('boto3 express ok')
PY

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
