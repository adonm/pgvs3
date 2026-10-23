#!/usr/bin/env bash
# Runs a filtered subset of ceph/s3-tests against a running pgvs3 gateway.
# Requires: git, curl, mise (for uv/python). The gateway must be reachable at
# PGVS3_S3_HOST:PGVS3_S3_PORT (default 127.0.0.1:8014).
set -euo pipefail

HOST=${PGVS3_S3_HOST:-127.0.0.1}
PORT=${PGVS3_S3_PORT:-8014}
REPO=${S3TESTS_REPO:-https://github.com/ceph/s3-tests.git}
COMMIT=${S3TESTS_COMMIT:-5b3987116eca0b4b8fa57d6b10f3db57bab196b6}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

git clone --quiet "$REPO" "$TMP/s3-tests"
git -C "$TMP/s3-tests" checkout --quiet "$COMMIT"
# pgvs3 is not versioned; the upstream cleanup path uses ListObjectVersions.
git -C "$TMP/s3-tests" apply "$ROOT/test/s3-tests-noversion.patch"

cat > "$TMP/s3tests.conf" <<EOF
[DEFAULT]
host = $HOST
port = $PORT
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = pgvs3-{random}-

[s3 main]
display_name = pgvs3
user_id = pgvs3
email = pgvs3@example.invalid
api_name = default
access_key = test
secret_key = test

[s3 alt]
display_name = pgvs3-alt
user_id = pgvs3-alt
email = pgvs3-alt@example.invalid
access_key = test2
secret_key = test2

[s3 tenant]
display_name = pgvs3-tenant
user_id = pgvs3-tenant
access_key = test3
secret_key = test3
email = pgvs3-tenant@example.invalid
tenant = pgvs3

[iam]
email = pgvs3@example.invalid
user_id = pgvs3-iam
access_key = test4
secret_key = test4
display_name = pgvs3-iam

[iam root]
access_key = AAAAAAAAAAAAAAAAAAaa
secret_key = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
account_id = RGW11111111111111111
user_id = RGW11111111111111111
email = account1@example.invalid

[iam alt root]
access_key = BBBBBBBBBBBBBBBBBBbb
secret_key = bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
account_id = RGW22222222222222222
user_id = RGW22222222222222222
email = account2@example.invalid

[webidentity]
token = ignored
role_arn = arn:aws:iam::000000000000:role/ignored
EOF

mise exec uv -- uv venv --python 3.12 "$TMP/venv" >/dev/null
mise exec uv -- uv pip install --python "$TMP/venv/bin/python" -q -r "$TMP/s3-tests/requirements.txt"

tests=(
  test_bucket_list_empty
  test_bucket_listv2_many
  test_basic_key_count
  test_bucket_list_delimiter_basic
  test_bucket_listv2_delimiter_basic
  test_object_write_read_update_read_delete
  test_object_write_check_etag
  test_object_head_zero_bytes
  test_object_read_not_exist
  test_multi_object_delete
  test_multi_objectv2_delete_key_limit
  test_object_copy_zero_size
  test_object_copy_same_bucket
  test_object_copy_diff_bucket
  test_object_copy_verify_contenttype
  test_multipart_upload_contents
  test_multipart_upload_overwrite_existing_object
  test_multipart_upload_missing_part
  test_multipart_upload_incorrect_etag
  test_multipart_get_part
  test_list_multipart_upload
  test_abort_multipart_upload
  test_abort_multipart_upload_not_found
  test_ranged_request_response_code
  test_ranged_big_request_response_code
  test_ranged_request_skip_leading_bytes_response_code
  test_ranged_request_return_trailing_bytes_response_code
  test_ranged_request_invalid_range
  test_ranged_request_empty_object
  test_object_write_with_chunked_transfer_encoding
)
ids=()
for t in "${tests[@]}"; do
  ids+=("s3tests/functional/test_s3.py::$t")
done

cd "$TMP/s3-tests"
S3TEST_CONF="$TMP/s3tests.conf" "$TMP/venv/bin/python" -m pytest -q "${ids[@]}" \
  --tb=short --maxfail=10
