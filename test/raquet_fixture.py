#!/usr/bin/env python3
"""Deterministic RaQuet (https://www.raquet.io/) fixtures without GDAL.

One float32 band of 256x256 gzip tiles at QUADBIN zoom ZOOM, centred on
CENTRE. Every pixel holds an integer, so sums are exact and `value(lon, lat)`
is the oracle for point lookups. The metadata row is its own row group and
tiles are sorted by cell, so a point query can prune to one tile row group.

    python test/raquet_fixture.py out.parquet [--tiles 8] [--row-group 4]
"""
import argparse
import gzip
import json
import math

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import quadbin

BLOCK = 256
ZOOM = 8
PIXEL_ZOOM = ZOOM + 8
CENTRE = (-3.7038, 40.4168)


def pixels(gx, gy):
    """Value of global pixel (gx, gy) at PIXEL_ZOOM; integers below 1024."""
    noise = ((gx * 7919) ^ (gy * 104729)) & 7
    return ((gx * 31 + gy * 17) % 997 + noise).astype(np.float32)


def pixel_of(lon, lat):
    n = 1 << PIXEL_ZOOM
    lat = math.radians(lat)
    return (math.floor((lon + 180) / 360 * n),
            math.floor((1 - math.log(math.tan(lat) + 1 / math.cos(lat)) / math.pi) / 2 * n))


def value(lon, lat):
    gx, gy = pixel_of(lon, lat)
    return float(pixels(np.int64(gx), np.int64(gy)))


def lonlat(tx, ty):
    n = 1 << ZOOM
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * ty / n))))
    return tx / n * 360 - 180, lat


def tiles(count):
    cx, cy = pixel_of(*CENTRE)
    x0, y0 = cx // BLOCK - count // 2, cy // BLOCK - count // 2
    return [(x0 + i, y0 + j) for j in range(count) for i in range(count)]


def bounds(count):
    grid = tiles(count)
    (west, north), (east, south) = lonlat(*grid[0]), lonlat(grid[-1][0] + 1, grid[-1][1] + 1)
    return [west, south, east, north]


def write(path, count=8, row_group=4):
    """Write a count x count tile raster; returns the file's metadata dict."""
    rows = []
    total = total_sq = 0.0
    lo, hi = math.inf, -math.inf
    for tx, ty in tiles(count):
        gy, gx = np.mgrid[ty * BLOCK:(ty + 1) * BLOCK, tx * BLOCK:(tx + 1) * BLOCK].astype(np.int64)
        band = pixels(gx, gy)
        total += float(band.sum(dtype=np.float64))
        total_sq += float((band.astype(np.float64) ** 2).sum())
        lo, hi = min(lo, float(band.min())), max(hi, float(band.max()))
        rows.append((quadbin.tile_to_cell((tx, ty, ZOOM)), gzip.compress(band.astype("<f4").tobytes(), 6, mtime=0)))
    rows.sort()
    pixels_total = count * count * BLOCK * BLOCK
    mean = total / pixels_total
    metadata = {
        "file_format": "raquet", "version": "0.5.0",
        "width": count * BLOCK, "height": count * BLOCK,
        "crs": "EPSG:3857", "bounds": bounds(count), "bounds_crs": "EPSG:4326",
        "compression": "gzip",
        "tiling": {"scheme": "quadbin", "block_width": BLOCK, "block_height": BLOCK,
                   "min_zoom": ZOOM, "max_zoom": ZOOM, "pixel_zoom": PIXEL_ZOOM, "num_blocks": len(rows)},
        "bands": [{"name": "band_1", "type": "float32", "nodata": None,
                   "STATISTICS_MINIMUM": lo, "STATISTICS_MAXIMUM": hi, "STATISTICS_MEAN": mean,
                   "STATISTICS_STDDEV": math.sqrt(total_sq / pixels_total - mean * mean),
                   "STATISTICS_VALID_PERCENT": 100.0}],
    }
    schema = pa.schema([("block", pa.uint64()), ("band_1", pa.binary()), ("metadata", pa.string())])
    with pq.ParquetWriter(path, schema, compression="none", write_statistics=True) as writer:
        writer.write_table(pa.table({"block": [0], "band_1": [None], "metadata": [json.dumps(metadata)]}, schema=schema))
        for start in range(0, len(rows), row_group):
            chunk = rows[start:start + row_group]
            writer.write_table(pa.table({"block": [r[0] for r in chunk], "band_1": [r[1] for r in chunk],
                                         "metadata": [None] * len(chunk)}, schema=schema))
    return metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("path")
    parser.add_argument("--tiles", type=int, default=8)
    parser.add_argument("--row-group", type=int, default=4)
    args = parser.parse_args()
    print(json.dumps(write(args.path, args.tiles, args.row_group)["tiling"]))
