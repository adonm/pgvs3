#!/usr/bin/env bash
# Interoperability suite: rclone, restic, boto3, DuckLake and SlateDB.
# Requires the environment exported by deploy/bench/stack.sh.
set -euo pipefail
: "${PGVS3_TEST_ENDPOINT_A:?}"
: "${PGVS3_TEST_DB_URL:?}"
: "${PGVS3_ACCESS_KEY:?}"
: "${PGVS3_SECRET_KEY:?}"
: "${PGVS3_TEST_PG_PORT:?}"

A=$PGVS3_TEST_ENDPOINT_A
BUCKET=pgvs3-contract
RUN="interop-$(date +%s)-$$"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

RCLONE_BIN=$(mise which rclone 2>/dev/null || command -v rclone || true)
RESTIC_BIN=$(mise which restic 2>/dev/null || command -v restic || true)

if [ -n "$RCLONE_BIN" ]; then
  rclone() { "$RCLONE_BIN" "$@"; }
  export RCLONE_CONFIG_PGVS3_TYPE=s3
  export RCLONE_CONFIG_PGVS3_PROVIDER=Other
  export RCLONE_CONFIG_PGVS3_ENV_AUTH=false
  export RCLONE_CONFIG_PGVS3_ACCESS_KEY_ID="$PGVS3_ACCESS_KEY"
  export RCLONE_CONFIG_PGVS3_SECRET_ACCESS_KEY="$PGVS3_SECRET_KEY"
  export RCLONE_CONFIG_PGVS3_ENDPOINT="$A"
  export RCLONE_CONFIG_PGVS3_REGION=us-east-1
  export RCLONE_CONFIG_PGVS3_FORCE_PATH_STYLE=true
  head -c 12582912 /dev/urandom > "$TMP/rclone.bin"
  rclone copyto "$TMP/rclone.bin" "pgvs3:$BUCKET/$RUN/rclone.bin" \
    --s3-upload-cutoff 5M --s3-chunk-size 5M --s3-upload-concurrency 4
  rclone cat "pgvs3:$BUCKET/$RUN/rclone.bin" > "$TMP/rclone.out"
  cmp "$TMP/rclone.bin" "$TMP/rclone.out"
  rclone lsf "pgvs3:$BUCKET/$RUN" | grep -q 'rclone.bin'
  mkdir -p "$TMP/rclone-dir"
  echo one > "$TMP/rclone-dir/one.txt"
  echo two > "$TMP/rclone-dir/two.txt"
  rclone copy "$TMP/rclone-dir" "pgvs3:$BUCKET/$RUN/rclone-dir"
  rclone check "$TMP/rclone-dir" "pgvs3:$BUCKET/$RUN/rclone-dir"
  echo "rclone ok"
else
  echo "SKIP rclone: not installed" >&2
fi

if [ -n "$RESTIC_BIN" ]; then
  restic() { "$RESTIC_BIN" "$@"; }
  export AWS_ACCESS_KEY_ID="$PGVS3_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$PGVS3_SECRET_KEY"
  export AWS_DEFAULT_REGION=us-east-1
  export RESTIC_REPOSITORY="s3:$A/$BUCKET/restic-$RUN"
  export RESTIC_PASSWORD=pgvs3-restic-test
  echo "restic data" > "$TMP/restic.txt"
  restic init >/dev/null
  (cd "$TMP" && restic backup restic.txt >/dev/null)
  restic snapshots >/dev/null
  restic check >/dev/null
  restic restore latest --target "$TMP/restore" >/dev/null
  cmp "$TMP/restic.txt" "$TMP/restore/restic.txt"
  echo "restic ok"
else
  echo "SKIP restic: not installed" >&2
fi

# Normal boto3 path (the contract also exercises boto3 in detail).
PGVS3_ACCESS_KEY="$PGVS3_ACCESS_KEY" PGVS3_SECRET_KEY="$PGVS3_SECRET_KEY" \
PGVS3_ENDPOINT="$A" PGVS3_BUCKET="$BUCKET" PGVS3_RUN="$RUN" \
mise exec uv -- uv run --quiet --no-project --with boto3 python - <<'PY'
import os, boto3
from botocore.config import Config
c=boto3.client('s3', endpoint_url=os.environ['PGVS3_ENDPOINT'],
  aws_access_key_id=os.environ['PGVS3_ACCESS_KEY'],
  aws_secret_access_key=os.environ['PGVS3_SECRET_KEY'], region_name='us-east-1',
  config=Config(signature_version='s3v4', s3={'addressing_style':'path'}))
b=os.environ['PGVS3_BUCKET']; k=f"{os.environ['PGVS3_RUN']}/boto3"
c.put_object(Bucket=b, Key=k, Body=b'boto3')
assert c.get_object(Bucket=b, Key=k)['Body'].read()==b'boto3'
assert any(x['Key']==k for x in c.list_objects_v2(Bucket=b, Prefix=k).get('Contents', []))
c.delete_object(Bucket=b, Key=k)
print('boto3 ok')
PY

# DuckLake. DuckDB 2.0 currently ships DuckLake 1.1; set
# DUCKLAKE_VERSION=2.1 to make this fail until that extension is published.
PGVS3_DB_PORT="$PGVS3_TEST_PG_PORT" PGVS3_ENDPOINT="$A" PGVS3_BUCKET="$BUCKET" \
PGVS3_RUN="$RUN" DUCKLAKE_VERSION="${DUCKLAKE_VERSION:-}" \
mise exec uv -- uv run --quiet --no-project --with duckdb==2.0.0 python - <<'PY'
import os, duckdb
c=duckdb.connect()
c.execute("INSTALL ducklake; LOAD ducklake;")
ext=c.execute("select extension_version from duckdb_extensions() where extension_name='ducklake'").fetchone()
version=ext[0] if ext else 'unknown'
print('ducklake extension', version)
want=os.environ.get('DUCKLAKE_VERSION','')
if want and want not in version:
    raise SystemExit(f'DuckLake {want} required, found {version}')
c.execute("INSTALL postgres; LOAD postgres;")
b=os.environ['PGVS3_BUCKET']; run=os.environ['PGVS3_RUN']; endpoint=os.environ['PGVS3_ENDPOINT'][7:]
c.execute(f"""CREATE SECRET s3s (TYPE S3, KEY_ID '{os.environ['PGVS3_ACCESS_KEY']}',
 SECRET '{os.environ['PGVS3_SECRET_KEY']}', REGION 'us-east-1', ENDPOINT '{endpoint}',
 URL_STYLE 'path', USE_SSL false)""")
c.execute(f"""CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT {os.environ['PGVS3_DB_PORT']},
 DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres')""")
table='interop_'+run.replace('-','_')
c.execute(f"ATTACH 'ducklake:postgres:' AS il (DATA_PATH 's3://{b}/{run}/lake/')")
c.execute(f"CREATE TABLE il.{table}(i INTEGER, s VARCHAR)")
c.execute(f"INSERT INTO il.{table} VALUES (1,'one'),(2,'two')")
assert c.execute(f"SELECT count(*), sum(i) FROM il.{table}").fetchone()==(2,3)
print('ducklake ok')
PY

if mise exec rust -- cargo --version >/dev/null 2>&1; then
  S3_ENDPOINT="$A" S3_BUCKET="$BUCKET" S3_ACCESS_KEY="$PGVS3_ACCESS_KEY" \
  S3_SECRET_KEY="$PGVS3_SECRET_KEY" SLATEDB_PATH="$RUN/slatedb" \
  mise exec rust -- cargo run --quiet --manifest-path test/slatedb/Cargo.toml
  echo "slatedb ok"
else
  echo "SKIP slatedb: cargo not installed" >&2
fi

echo "interop ok"
