#!/usr/bin/env python3
"""Real orthophoto RaQuet fixture: an N x N block of the LINZ Wellington 2021
0.075 m urban aerial sheets, mosaicked and converted with CARTO's raquet-io.

Imagery: Toitu Te Whenua Land Information New Zealand, AAM NZ and Wellington
City Council, CC BY 4.0. Each sheet is a 3200x4800 RGBA lossless-WebP COG of
about 23 MB; the converted RGBA gzip RaQuet with its overview pyramid is about
3x the source. Downloads are checked against each sheet's STAC
file:checksum, and sheets and converted files are cached under
$PGVS3_ORTHO_CACHE (default ~/.cache/pgvs3/ortho), so only the first run
downloads or converts. Set $PGVS3_ORTHO_FILE to use another RaQuet file.

    python test/raquet_ortho.py [--sheets 8]    # prints the converted path
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import random
import shutil
import subprocess
from urllib.parse import quote
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

import pyarrow.compute as pc
import pyarrow.parquet as pq

SURVEY = "https://nz-imagery.s3-ap-southeast-2.amazonaws.com/wellington/wellington_2021_0.075m/rgb/2193/"
ORIGIN = (1, 79)  # top-left sheet BQ31_500_001079; the survey covers every sheet up to 16x16
IMAGE = "pgvs3-raquet-io"
DOCKERFILE = Path(__file__).resolve().parents[1] / "deploy/bench/raquet-io"


def cache_dir():
    return Path(os.environ.get("PGVS3_ORTHO_CACHE") or Path.home() / ".cache/pgvs3/ortho")


def sheets(n):
    if not 1 <= n <= 16:
        raise ValueError("sheets must be 1..16")
    return [f"BQ31_500_{ORIGIN[0] + r:03d}{ORIGIN[1] + c:03d}" for r in range(n) for c in range(n)]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def fetch(name, dest):
    """Download one sheet unless the cached copy matches its STAC checksum."""
    with urlopen(SURVEY + name + ".json", timeout=60) as response:
        multihash = json.load(response)["assets"]["visual"]["file:checksum"]
    assert multihash.startswith("1220"), multihash
    want, path = multihash[4:], dest / f"{name}.tiff"
    if path.exists() and sha256(path) == want:
        return name, want
    part = path.with_suffix(".part")
    with urlopen(SURVEY + name + ".tiff", timeout=300) as response, part.open("wb") as f:
        shutil.copyfileobj(response, f, 1 << 20)
    assert sha256(part) == want, f"{name}: checksum mismatch"
    part.rename(path)
    return name, want


def build(n=8):
    """Return the cached N x N RaQuet file, downloading and converting on first
    use, or $PGVS3_ORTHO_FILE (any RaQuet file) when set."""
    if os.environ.get("PGVS3_ORTHO_FILE"):
        return Path(os.environ["PGVS3_ORTHO_FILE"])
    root = cache_dir()
    out = root / f"wellington-2021-{n}x{n}.parquet"
    if out.exists():
        return out
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(8) as pool:
        checksums = dict(pool.map(lambda name: fetch(name, src), sheets(n)))
    (root / f"{out.stem}.sheets.json").write_text(json.dumps(checksums, indent=1) + "\n")
    (root / f"{out.stem}.list").write_text("".join(f"src/{name}.tiff\n" for name in checksums))
    subprocess.run(["docker", "build", "-q", "-t", IMAGE, str(DOCKERFILE)], check=True, stdout=subprocess.DEVNULL)
    tmp = f"{out.stem}.tmp.parquet"
    script = (f"gdalbuildvrt -q -input_file_list {out.stem}.list {out.stem}.vrt"
              f" && raquet-io convert raster {out.stem}.vrt {tmp} --streaming"
              f" && raquet-io validate {tmp}")
    subprocess.run(["docker", "run", "--rm", "-u", f"{os.getuid()}:{os.getgid()}", "-v", f"{root}:/w", "-w", "/w",
                    IMAGE, "sh", "-c", script], check=True, stdout=subprocess.DEVNULL)
    (root / tmp).rename(out)
    return out


def metadata(path):
    """The RaQuet metadata JSON stored in the block-0 row."""
    table = pq.read_table(path, columns=["block", "metadata"], filters=[("block", "=", 0)])
    return json.loads(pc.drop_null(table["metadata"])[0].as_py())


def same(rows, expected):
    """Result equality, allowing float aggregates to differ by summation order."""
    flat = lambda r: [v for row in r for v in row]
    a, b = flat(rows), flat(expected)
    return len(rows) == len(expected) and len(a) == len(b) and all(
        math.isclose(x, y, rel_tol=1e-9) if isinstance(x, float) else x == y for x, y in zip(a, b))


def upload(endpoint, target, path, part=256 << 20):
    """Unsigned streaming multipart PUT of a local file to endpoint+target."""
    def call(url, data, method):
        with urlopen(Request(url, data=data, method=method), timeout=600) as response:
            assert response.status == 200, (method, url, response.status)
            return response.headers, response.read()

    url = endpoint + target
    upload_id = ET.fromstring(call(url + "?uploads", b"", "POST")[1]).findtext("{*}UploadId")
    parts = []
    with path.open("rb") as f:
        while chunk := f.read(part):
            number = len(parts) + 1
            headers, _ = call(f"{url}?uploadId={quote(upload_id)}&partNumber={number}", chunk, "PUT")
            parts.append(f"<Part><PartNumber>{number}</PartNumber><ETag>{headers['ETag']}</ETag></Part>")
    body = ("<CompleteMultipartUpload>" + "".join(parts) + "</CompleteMultipartUpload>").encode()
    return ET.fromstring(call(f"{url}?uploadId={quote(upload_id)}", body, "POST")[1]).findtext("{*}ETag")


def points(meta, count, seed=20261103):
    """Deterministic (lon, lat) points inside the raster's bounds."""
    west, south, east, north = meta["bounds"]
    rng = random.Random(seed)
    return [(rng.uniform(west, east), rng.uniform(south, north)) for _ in range(count)]


def rgb(lon, lat):
    return ", ".join(f"ST_RasterValue(block, band_{i}, ST_Point({lon}, {lat}), metadata)" for i in (1, 2, 3))


def queries(url, meta, count=8):
    """(name, sql) pairs: native RGB point lookups, one lookup per overview
    zoom (a zoomed-out map view), a region over the central 9% and a scan of
    band_1 across every zoom."""
    west, south, east, north = meta["bounds"]
    tiling = meta["tiling"]
    out = [(f"point {i}", f"SELECT {rgb(lon, lat)} FROM read_raquet_at('{url}', {lon}, {lat})")
           for i, (lon, lat) in enumerate(points(meta, count))]
    lon, lat = (west + east) / 2, (south + north) / 2
    out += [(f"overview z{z}", f"SELECT {rgb(lon, lat)} FROM read_raquet_at('{url}', {lon}, {lat}, {z})")
            for z in range(tiling["min_zoom"], tiling["max_zoom"])]
    x0, x1 = west + (east - west) * 0.35, west + (east - west) * 0.65
    y0, y1 = south + (north - south) * 0.35, south + (north - south) * 0.65
    region = f"'POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0}))'::GEOMETRY"
    return out + [
        ("region", f"SELECT unnest(ST_RegionStats(band_1, block, {region}, metadata)) FROM read_raquet('{url}', {region})"),
        ("scan", f"SELECT count(*), sum((ST_RasterSummaryStats(band_1, metadata)).sum) FROM read_raquet('{url}')"),
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sheets", type=int, default=8, help="block edge in sheets (1..16)")
    print(build(parser.parse_args().sheets))
