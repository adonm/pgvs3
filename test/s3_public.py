import os
import sys
import time
import urllib.request
import urllib.error
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import duckdb

GW_W1 = os.environ["PGVS3_GW_W1"]
GW_W2 = os.environ["PGVS3_GW_W2"]
GW_E1 = os.environ["PGVS3_GW_E1"]
GW_E2 = os.environ["PGVS3_GW_E2"]

MAX_ORIGIN_BYTES = 256 << 20  # 256 MiB hard politeness cap

BUCKETS = os.environ.get("PGVS3_PUBLIC_BUCKETS", "overturemaps-us-west-2,noaa-gsod-pds,tripdata").split(",")
REGIONS = os.environ.get("PGVS3_PUBLIC_REGIONS", "us-west-2,us-east-1,us-east-1").split(",")


def s3_gw(endpoint, region):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="anon",
        aws_secret_access_key="anon",
        region_name=region,
        config=Config(s3={"addressing_style": "path"}),
    )


def s3_direct(region):
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(signature_version=UNSIGNED),
    )


def scrape_metrics(endpoint):
    with urllib.request.urlopen(endpoint + "/metrics", timeout=5) as resp:
        text = resp.read().decode("utf-8")
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return out


def timed_get(client, bucket, key, rng=None):
    kw = {"Bucket": bucket, "Key": key}
    if rng:
        kw["Range"] = rng
    t0 = time.perf_counter()
    resp = client.get_object(**kw)
    data = resp["Body"].read()
    ms = (time.perf_counter() - t0) * 1000.0
    return data, resp.get("ETag", "").strip('"'), ms


def main():
    overture_b, gsod_b, trip_b = BUCKETS[0], BUCKETS[1], BUCKETS[2]
    overture_r, gsod_r, trip_r = REGIONS[0], REGIONS[1], REGIONS[2]

    gw_w1 = s3_gw(GW_W1, overture_r)
    gw_w2 = s3_gw(GW_W2, overture_r)
    gw_e1 = s3_gw(GW_E1, gsod_r)
    gw_e2 = s3_gw(GW_E2, gsod_r)

    dir_w = s3_direct(overture_r)
    dir_e = s3_direct(gsod_r)

    results = []

    # 1. Overture (us-west-2): list release/ with delimiter, cross-check direct AWS, ranged GETs, DuckDB.
    rel_gw = gw_w1.list_objects_v2(Bucket=overture_b, Prefix="release/", Delimiter="/")
    rel_dir = dir_w.list_objects_v2(Bucket=overture_b, Prefix="release/", Delimiter="/")
    gw_prefixes = sorted(p["Prefix"] for p in rel_gw.get("CommonPrefixes", []))
    dir_prefixes = sorted(p["Prefix"] for p in rel_dir.get("CommonPrefixes", []))
    assert gw_prefixes and gw_prefixes == dir_prefixes, f"release/ listing mismatch: gw={gw_prefixes} dir={dir_prefixes}"
    latest_rel = gw_prefixes[-1]

    places = gw_w1.list_objects_v2(Bucket=overture_b, Prefix=latest_rel + "theme=places/type=place/", MaxKeys=5)
    ov_objs = [o for o in places.get("Contents", []) if o["Key"].endswith(".parquet") and o["Size"] > 2 << 20]
    assert ov_objs, "no overture places parquet found"
    ov_key = ov_objs[0]["Key"]

    for label, rng in [("overture_suffix_64k", "bytes=-65536"), ("overture_mid_128k", "bytes=1048576-1179647")]:
        want_bytes, want_etag, _ = timed_get(dir_w, overture_b, ov_key, rng)
        m0 = scrape_metrics(GW_W1)
        got_miss, got_etag, miss_ms = timed_get(gw_w1, overture_b, ov_key, rng)
        m1 = scrape_metrics(GW_W1)
        assert got_miss == want_bytes, f"{label}: miss bytes mismatch"
        assert got_etag == want_etag, f"{label}: etag mismatch {got_etag} vs {want_etag}"
        assert m1["pgvs3_cache_misses_total"] > m0["pgvs3_cache_misses_total"], f"{label}: expected miss metric"

        got_warm, _, warm_ms = timed_get(gw_w1, overture_b, ov_key, rng)
        m2 = scrape_metrics(GW_W1)
        assert got_warm == want_bytes, f"{label}: warm bytes mismatch"
        assert m2["pgvs3_cache_hits_total"] > m1["pgvs3_cache_hits_total"], f"{label}: expected hit metric"

        mb0 = scrape_metrics(GW_W2)
        got_b, _, warm_b_ms = timed_get(gw_w2, overture_b, ov_key, rng)
        mb1 = scrape_metrics(GW_W2)
        assert got_b == want_bytes, f"{label}: cross-gw warm bytes mismatch"
        assert mb1["pgvs3_cache_hits_total"] > mb0["pgvs3_cache_hits_total"], f"{label}: expected cross-gw hit"
        assert mb1["pgvs3_cache_misses_total"] == mb0["pgvs3_cache_misses_total"], f"{label}: unexpected cross-gw miss"
        results.append(f"{label}:miss={miss_ms:.1f}ms,warm={warm_ms:.1f}ms,gwB={warm_b_ms:.1f}ms")

    # 2. DuckDB read_parquet on Overture through gwW1 twice (fresh connection so DuckDB doesn't skip HTTP).
    ep_w1 = GW_W1.removeprefix("http://")
    def run_duckdb():
        c = duckdb.connect()
        c.execute("INSTALL httpfs; LOAD httpfs;")
        c.execute(f"""
            CREATE SECRET s3_ov (
                TYPE S3,
                KEY_ID 'anon',
                SECRET 'anon',
                REGION '{overture_r}',
                ENDPOINT '{ep_w1}',
                URL_STYLE 'path',
                USE_SSL false
            );
        """)
        t_start = time.perf_counter()
        out = c.execute(f"""
            SELECT count(*), min(id)
            FROM read_parquet('s3://{overture_b}/{ov_key}')
            WHERE bbox.xmin BETWEEN -122.42 AND -122.40 AND bbox.ymin BETWEEN 37.77 AND 37.79
        """).fetchone()
        return out, (time.perf_counter() - t_start) * 1000.0

    res1, duck_miss_ms = run_duckdb()
    dm1 = scrape_metrics(GW_W1)

    res2, duck_warm_ms = run_duckdb()
    dm2 = scrape_metrics(GW_W1)

    assert res1 == res2, f"DuckDB query mismatch: {res1} vs {res2}"
    assert dm2["pgvs3_cache_hits_total"] > dm1["pgvs3_cache_hits_total"], "DuckDB warm run did not hit cache"
    assert dm2["pgvs3_cache_misses_total"] == dm1["pgvs3_cache_misses_total"], "DuckDB warm run had cache misses"
    results.append(f"duckdb_overture:miss={duck_miss_ms:.1f}ms,warm={duck_warm_ms:.1f}ms,rows={res1[0]}")

    # 3. NOAA GSOD (us-east-1): discover small CSV, full GET miss -> warm -> cross-gw warm.
    gsod_list = gw_e1.list_objects_v2(Bucket=gsod_b, Prefix="2024/", MaxKeys=5)
    gsod_objs = [o for o in gsod_list.get("Contents", []) if o["Key"].endswith(".csv") and o["Size"] > 0]
    assert gsod_objs, "no noaa-gsod-pds 2024 CSV found"
    gsod_key = gsod_objs[0]["Key"]

    want_csv, want_csv_etag, _ = timed_get(dir_e, gsod_b, gsod_key)
    mg0 = scrape_metrics(GW_E1)
    got_csv, got_csv_etag, gsod_miss_ms = timed_get(gw_e1, gsod_b, gsod_key)
    mg1 = scrape_metrics(GW_E1)
    assert got_csv == want_csv and got_csv_etag == want_csv_etag
    assert mg1["pgvs3_cache_misses_total"] > mg0["pgvs3_cache_misses_total"]

    got_csv2, _, gsod_warm_ms = timed_get(gw_e1, gsod_b, gsod_key)
    mg2 = scrape_metrics(GW_E1)
    assert got_csv2 == want_csv and mg2["pgvs3_cache_hits_total"] > mg1["pgvs3_cache_hits_total"]

    mge0 = scrape_metrics(GW_E2)
    got_csv_b, _, gsod_b_ms = timed_get(gw_e2, gsod_b, gsod_key)
    mge1 = scrape_metrics(GW_E2)
    assert got_csv_b == want_csv and mge1["pgvs3_cache_hits_total"] > mge0["pgvs3_cache_hits_total"]
    assert mge1["pgvs3_cache_misses_total"] == mge0["pgvs3_cache_misses_total"]
    results.append(f"gsod_csv:miss={gsod_miss_ms:.1f}ms,warm={gsod_warm_ms:.1f}ms,gwB={gsod_b_ms:.1f}ms")

    # 4. Tripdata (us-east-1): discover object, polite ranged GETs miss -> warm -> cross-gw warm.
    trip_list = gw_e1.list_objects_v2(Bucket=trip_b, MaxKeys=5)
    trip_objs = [o for o in trip_list.get("Contents", []) if o["Size"] > 128 << 10]
    assert trip_objs, "no tripdata object found"
    trip_key = trip_objs[0]["Key"]

    want_tr, want_tr_etag, _ = timed_get(dir_e, trip_b, trip_key, "bytes=0-65535")
    got_tr, got_tr_etag, tr_miss_ms = timed_get(gw_e1, trip_b, trip_key, "bytes=0-65535")
    assert got_tr == want_tr and got_tr_etag == want_tr_etag
    got_tr2, _, tr_warm_ms = timed_get(gw_e1, trip_b, trip_key, "bytes=0-65535")
    assert got_tr2 == want_tr
    got_tr_b, _, tr_b_ms = timed_get(gw_e2, trip_b, trip_key, "bytes=0-65535")
    assert got_tr_b == want_tr
    results.append(f"tripdata_64k:miss={tr_miss_ms:.1f}ms,warm={tr_warm_ms:.1f}ms,gwB={tr_b_ms:.1f}ms")

    # 5. Region mismatch (301): request an uncached us-west-2 object through us-east-1 gateway (GW_E1).
    probe_key = ov_objs[-1]["Key"] if len(ov_objs) > 1 and ov_objs[-1]["Key"] != ov_key else "wrong-region-probe.parquet"
    req = urllib.request.Request(f"{GW_E1}/{overture_b}/{probe_key}", method="GET")
    redirect_status = 0
    body = ""
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as e:
        redirect_status = e.code
        body = e.read().decode("utf-8", errors="replace")
    assert redirect_status == 301, f"expected 301 PermanentRedirect on wrong-region HEAD, got {redirect_status}"
    assert "PermanentRedirect" in body and "us-west-2" in body, f"unexpected 301 body: {body}"

    # 6. Politeness check: sum miss bytes across all gateways <= 256 MiB.
    total_origin_bytes = sum(int(scrape_metrics(u).get("pgvs3_cache_miss_bytes_total", 0)) for u in (GW_W1, GW_W2, GW_E1, GW_E2))
    assert total_origin_bytes <= MAX_ORIGIN_BYTES, f"origin bytes {total_origin_bytes} exceeded {MAX_ORIGIN_BYTES}"

    print(f"s3-public OK: origin_bytes={total_origin_bytes} redirect_301=PermanentRedirect(us-west-2) " + " ".join(results))


if __name__ == "__main__":
    main()
