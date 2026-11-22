import io
import os
import sys
import time
import boto3
from botocore.config import Config
import duckdb

GW_A = os.environ["PGVS3_GW_A"]
GW_B = os.environ["PGVS3_GW_B"]
AK = os.environ["AWS_ACCESS_KEY_ID"]
SK = os.environ["AWS_SECRET_ACCESS_KEY"]
BUCKET = "smoke"


def s3_client(endpoint):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=AK,
        aws_secret_access_key=SK,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def main():
    a = s3_client(GW_A)
    b = s3_client(GW_B)

    a.create_bucket(Bucket=BUCKET)

    # 1. Put via A, fill on A, hit on B, ranges
    data = b"0123456789abcdef" * 4096  # 64 KiB
    a.put_object(Bucket=BUCKET, Key="hello.bin", Body=data)
    got_a = a.get_object(Bucket=BUCKET, Key="hello.bin")["Body"].read()
    assert got_a == data, "GET from A mismatch"
    got_b = b.get_object(Bucket=BUCKET, Key="hello.bin")["Body"].read()
    assert got_b == data, "GET from B mismatch"

    rng = b.get_object(Bucket=BUCKET, Key="hello.bin", Range="bytes=10-19")["Body"].read()
    assert rng == data[10:20], f"range mismatch: {rng!r}"
    suf = b.get_object(Bucket=BUCKET, Key="hello.bin", Range="bytes=-8")["Body"].read()
    assert suf == data[-8:], f"suffix mismatch: {suf!r}"

    # 2. Multipart upload via A, read via B
    mp = a.create_multipart_upload(Bucket=BUCKET, Key="mp.bin")
    uid = mp["UploadId"]
    part1 = b"A" * (5 * 1024 * 1024)
    part2 = b"B" * 1024
    e1 = a.upload_part(Bucket=BUCKET, Key="mp.bin", PartNumber=1, UploadId=uid, Body=part1)["ETag"]
    e2 = a.upload_part(Bucket=BUCKET, Key="mp.bin", PartNumber=2, UploadId=uid, Body=part2)["ETag"]
    a.complete_multipart_upload(
        Bucket=BUCKET,
        Key="mp.bin",
        UploadId=uid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": e1}, {"PartNumber": 2, "ETag": e2}]},
    )
    got_mp = b.get_object(Bucket=BUCKET, Key="mp.bin", Range="bytes=5242875-5242885")["Body"].read()
    assert got_mp == b"AAAAABBBBBB", f"multipart boundary mismatch: {got_mp!r}"

    # 3. ListObjectsV2 passthrough with delimiter + pagination
    a.put_object(Bucket=BUCKET, Key="dir/x.txt", Body=b"x")
    a.put_object(Bucket=BUCKET, Key="dir/y.txt", Body=b"y")
    a.put_object(Bucket=BUCKET, Key="top.txt", Body=b"top")
    p1 = a.list_objects_v2(Bucket=BUCKET, Delimiter="/", MaxKeys=2)
    assert p1["IsTruncated"], "expected truncated page 1"
    p2 = a.list_objects_v2(Bucket=BUCKET, Delimiter="/", MaxKeys=2, ContinuationToken=p1["NextContinuationToken"])
    names = [c["Key"] for c in p1.get("Contents", []) + p2.get("Contents", [])] + [
        cp["Prefix"] for cp in p1.get("CommonPrefixes", []) + p2.get("CommonPrefixes", [])
    ]
    assert "dir/" in names and "top.txt" in names, f"unexpected list: {names}"

    # 4. DeleteObjects via A invalidates B
    a.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": "mp.bin"}, {"Key": "top.txt"}]})
    try:
        b.get_object(Bucket=BUCKET, Key="mp.bin")
        raise AssertionError("expected NoSuchKey after DeleteObjects")
    except b.exceptions.NoSuchKey:
        pass

    # 5. DuckDB httpfs Parquet smoke (miss vs warm hit)
    pq_path = os.path.join(os.environ.get("TMPDIR", "/tmp"), "sample.parquet")
    con = duckdb.connect()
    con.execute(f"COPY (SELECT i, i*2 AS v, repeat('p', 32) AS pad FROM range(100000) t(i)) TO '{pq_path}' (FORMAT PARQUET)")
    with open(pq_path, "rb") as f:
        a.put_object(Bucket=BUCKET, Key="sample.parquet", Body=f.read())

    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint_host = GW_A.removeprefix("http://")
    con.execute(f"""
        CREATE SECRET s3_pgvs3 (
            TYPE S3,
            KEY_ID '{AK}',
            SECRET '{SK}',
            REGION 'us-east-1',
            ENDPOINT '{endpoint_host}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)

    t0 = time.perf_counter()
    c1, s1 = con.execute(f"SELECT count(*), sum(v) FROM read_parquet('s3://{BUCKET}/sample.parquet')").fetchone()
    miss_ms = (time.perf_counter() - t0) * 1000.0

    t1 = time.perf_counter()
    c2, s2 = con.execute(f"SELECT count(*), sum(v) FROM read_parquet('s3://{BUCKET}/sample.parquet')").fetchone()
    warm_ms = (time.perf_counter() - t1) * 1000.0

    assert (c1, s1) == (100000, 9999900000) and (c1, s1) == (c2, s2)
    print(f"versitygw+boto3+duckdb smoke OK: parquet_miss_ms={miss_ms:.2f} parquet_warm_hit_ms={warm_ms:.2f}")


if __name__ == "__main__":
    main()
