#!/usr/bin/env python3
"""Paired before/after runs using the existing loadgen, not a second load engine.

Requires disposable endpoints/databases: PUT measurements overwrite their own
benchmark keys. Seed through --seed-endpoint to measure a read-through tier
without accidentally warming its whole-object/write-back cache.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def validate_record(record, size, op, workers, count, requests, range_size):
    for key, expected in {"size": size, "op": op, "workers": workers, "count": count, "requests": requests, "errors": 0}.items():
        if record.get(key) != expected:
            raise RuntimeError(f"loadgen {key}={record.get(key)!r}, expected {expected!r}: {record}")
    for key in ("seconds", "requests_per_s", "p50_ms"):
        if not math.isfinite(record[key]) or record[key] <= 0:
            raise RuntimeError(f"unmeasurable {key}: {record}")
    length = size
    if op == "range":
        length = min(size, range_size)
    elif op == "head":
        length = 0
    expected_bytes = requests * length
    observed = record["mib_per_s"] * record["seconds"] * (1 << 20)
    if not math.isclose(observed, expected_bytes, rel_tol=1e-8, abs_tol=1):
        raise RuntimeError(f"transferred {observed} bytes, expected {expected_bytes}")


def summarize(raw):
    cases, failed = [], []
    for size, op, workers in sorted({(r["size"], r["op"], r["workers"]) for r in raw}):
        rows = {side: [r for r in raw if (r["size"], r["op"], r["workers"], r["label"]) == (size, op, workers, side)] for side in ("before", "after")}
        old, new = rows["before"], rows["after"]
        if not old or len(old) != len(new):
            raise RuntimeError("incomplete paired case")
        before = statistics.median(r["p50_ms"] for r in old)
        after = statistics.median(r["p50_ms"] for r in new)
        case = {"size": size, "op": op, "workers": workers, "before_p50_ms": before, "after_p50_ms": after,
                "p50_ratio": after / before,
                "throughput_ratio": statistics.median(r["requests_per_s"] for r in new) / statistics.median(r["requests_per_s"] for r in old)}
        cases.append(case)
        if case["p50_ratio"] > 1.20 or case["throughput_ratio"] < 0.80:
            failed.append(case)
    return cases, failed


def cache_path(args, op, size):
    if not (args.origin_directories or args.seed_endpoint):
        return "native"
    if op == "put":
        return "origin-write"
    if op == "head" and args.cache_state == "cold":
        return "metadata-probe"
    if op == "range" and size > 8 << 20 and args.range_size < size:
        if args.range_chunk:
            return "aligned-range-cache"
        if args.cache_state == "cold":
            return "pass-through-range"
        return "whole-object-cache-range"
    return "whole-object-cache"


class Fixtures:
    def __init__(self, args, endpoints, databases, run):
        self.args, self.endpoints, self.databases, self.run = args, endpoints, databases, run
        self.directories = dict(zip(endpoints, map(Path, args.origin_directories or [])))

    def sql(self, side, statement):
        if not self.databases[side]:
            return ""
        result = subprocess.run([self.args.psql, self.databases[side], "-XAt", "-v", "ON_ERROR_STOP=1", "-c", statement], check=True, capture_output=True, text=True)
        return result.stdout.strip()

    def configure(self):
        for side, directory in self.directories.items():
            with urllib.request.urlopen(urllib.request.Request(self.endpoints[side] + "/" + self.args.bucket, method="HEAD"), timeout=30):
                pass
            raw = self.sql(side, "SELECT attrs->'origin' FROM s3p.namespaces WHERE kind='bucket' AND name=" + quote(self.args.bucket))
            spec = json.loads(raw)
            config = json.loads(spec["config"])
            expected_range = min(max(self.args.range_chunk, 65536), 8 << 20) if self.args.range_chunk else 0
            if config["type"] != "filesystem" or config.get("prefix", "") != "" or Path(config["config"]["directory"]).resolve() != directory.resolve() or \
                    spec["namespace"] != self.args.origin_namespace or spec.get("range_chunk", 0) != expected_range or spec.get("ttl") in (None, "", "0s"):
                raise RuntimeError(f"{side} persisted origin spec does not match requested fixture: {spec}")

    def seed(self, side, size, payload, prefix, count):
        if side in self.directories:
            folder = self.directories[side] / self.args.bucket / prefix
            folder.mkdir(parents=True, exist_ok=False)
            for i in range(count):
                target = folder / f"obj-{i:06d}"
                try:
                    os.link(payload, target)
                except OSError:
                    shutil.copyfile(payload, target)
                if target.stat().st_size != size:
                    raise RuntimeError(f"incorrect seed size: {target}")
        else:
            self.run(self.args.seed_endpoint or self.endpoints[side], size, payload, prefix, "get", 1, 1, prefill=True, count=count)

    def cached(self, side, prefix):
        key = quote(self.args.bucket + "/" + prefix + "/")
        predicate = f"left(key,length({key}))={key}"
        ns = self.args.origin_namespace
        return int(self.sql(side, "SELECT " + "+".join(
            f"(SELECT count(*) FROM {identifier(schema)}.{table} WHERE {predicate})"
            for schema, table in (("s3c_" + ns, "objects"), ("s3w_" + ns, "objects"), ("s3c_" + ns, "origin_metadata")))))

    def confirm_cold_fill(self, side, size, prefix, count, op):
        length = min(size, self.args.range_size)
        fills = op == "get" or (op == "range" and (size <= 8 << 20 or length == size or (self.args.range_chunk and length <= 8 << 20)))
        if op == "head":
            return
        key = quote(self.args.bucket + "/" + prefix + "/")
        rows = self.sql(side, f"SELECT count(*) FROM {identifier('s3c_' + self.args.origin_namespace)}.objects WHERE left(key,length({key}))={key}")
        if fills and int(rows) < count:
            raise RuntimeError(f"cold {op} bypassed the intended cache fill: {rows} cached bodies for {count} distinct reads")
        if not fills and int(rows) != 0:
            raise RuntimeError("pass-through range unexpectedly cached a body")

    def verify(self, side, size, payload, prefix, op, warm, key=None, bucket=None):
        key = (bucket or self.args.bucket) + "/" + (key or f"{prefix}/obj-000000")
        headers = {}
        length = size
        start = 0
        if op == "range":
            length = min(size, self.args.range_size)
            start = size - length
            headers["Range"] = f"bytes={start}-{size-1}"
        method = "HEAD" if op == "head" else "GET"
        request = urllib.request.Request(self.endpoints[side] + "/" + urllib.parse.quote(key, safe="/"), headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=180) as response:
            if response.status != (206 if op == "range" else 200) or int(response.headers["Content-Length"]) != length:
                raise RuntimeError(f"wrong object/range size: {dict(response.headers)}")
            if op == "range" and response.headers.get("Content-Range") != f"bytes {start}-{size-1}/{size}":
                raise RuntimeError("wrong Content-Range")
            origin = bool(self.directories or self.args.seed_endpoint)
            if warm and ((origin and response.headers.get("X-Cache") != "HIT") or (not origin and response.headers.get("X-Cache") is not None)):
                raise RuntimeError(f"unexpected warm/native cache path: {dict(response.headers)}")
            if method == "GET":
                actual = hashlib.file_digest(response, "sha256").hexdigest()
                if op == "range":
                    with Path(payload).open("rb") as source:
                        source.seek(start)
                        expected = hashlib.sha256(source.read(length)).hexdigest()
                else:
                    expected = digest(payload)
                if actual != expected:
                    raise RuntimeError("fixture bytes differ from the requested payload")

    def verify_put(self, side, size, payload, prefix, bucket=None):
        query = urllib.parse.urlencode({"list-type": "2", "prefix": prefix + "/", "max-keys": 1})
        with urllib.request.urlopen(self.endpoints[side] + "/" + (bucket or self.args.bucket) + "?" + query, timeout=30) as response:
            key = ET.fromstring(response.read()).findtext(".//{*}Key")
        if not key:
            raise RuntimeError("successful PUT run published no objects")
        self.verify(side, size, payload, prefix, "get", False, key=key, bucket=bucket)

    def drain(self, side):
        if not self.directories:
            return
        deadline = time.monotonic() + 120
        while self.sql(side, "SELECT count(*) FROM s3p.pending_uploads WHERE namespace=" + quote(self.args.origin_namespace)) != "0":
            if time.monotonic() > deadline:
                raise RuntimeError("outbox did not drain before the next paired sample")
            time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--loadgen", required=True)
    parser.add_argument("--before-binary", help="optional frozen control binary for provenance hashing")
    parser.add_argument("--after-binary", help="optional frozen candidate binary for provenance hashing")
    parser.add_argument("--payload", action="append", default=[])
    parser.add_argument("--seed-endpoint")
    parser.add_argument("--ops", default="head,get,range,put")
    parser.add_argument("--workers", default="1,4")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--short-requests", type=int, default=200)
    parser.add_argument("--requests", type=int, default=0, help="override requests per sample (0 selects a size/state budget)")
    parser.add_argument("--postgres-before", help="disposable database URL; ANALYZE reads and CHECKPOINT writes")
    parser.add_argument("--postgres-after", help="disposable database URL; ANALYZE reads and CHECKPOINT writes")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--isolate-writes", action="store_true", help="create/drop a fresh bucket for each PUT run")
    parser.add_argument("--prefix", default="comparison")
    parser.add_argument("--bucket", default="bench")
    parser.add_argument("--cache-state", choices=("warm", "cold"), default="warm")
    parser.add_argument("--origin-directories", nargs=2, metavar=("BEFORE", "AFTER"))
    parser.add_argument("--origin-namespace", default="benchmark-origin")
    parser.add_argument("--range-chunk", type=int, default=0)
    parser.add_argument("--range-size", type=int, default=8192)
    parser.add_argument("--no-small", action="store_true", help="omit the default 4 KiB case")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("use at least three paired repetitions")
    if args.short_requests < 2:
        parser.error("--short-requests must be at least two")
    if args.requests < 0:
        parser.error("--requests must not be negative")
    if bool(args.postgres_before) != bool(args.postgres_after):
        parser.error("provide both disposable database URLs, or neither")
    if bool(args.before_binary) != bool(args.after_binary):
        parser.error("provide both provenance binaries, or neither")
    if args.origin_directories and not args.postgres_before:
        parser.error("origin fixtures require both disposable database URLs")
    if args.cache_state == "cold" and not args.origin_directories:
        parser.error("cold comparisons require explicit filesystem origin fixtures")
    if args.origin_directories and args.isolate_writes:
        parser.error("isolated native write buckets cannot use a per-bucket origin fixture")
    if args.origin_directories and args.seed_endpoint:
        parser.error("choose filesystem fixtures or a seed endpoint, not both")
    if args.range_chunk < 0 or args.range_size <= 0:
        parser.error("range sizes must be positive and range chunk must not be negative")
    if not re.fullmatch(r"[a-zA-Z0-9._-]+", args.prefix) or any(not re.fullmatch(r"[a-z0-9._-]{1,58}", n) for n in (args.bucket, args.origin_namespace)):
        parser.error("invalid fixture prefix, bucket or namespace")
    try:
        worker_counts = [int(w) for w in args.workers.split(",")]
    except ValueError:
        parser.error("workers must be comma-separated integers")
    if not set(args.ops.split(",")) <= {"get", "head", "range", "put"} or any(w < 1 for w in worker_counts):
        parser.error("unsupported operations or worker counts")
    if args.requests and args.requests < max(worker_counts):
        parser.error("--requests must cover every worker")
    out = Path(args.out)
    if out.exists():
        parser.error("output already exists; choose a fresh --out path")
    out.parent.mkdir(parents=True, exist_ok=True)
    raw = []
    endpoints = {"before": args.before, "after": args.after}
    databases = {"before": args.postgres_before, "after": args.postgres_after}
    fixture_dir = Path(tempfile.mkdtemp(prefix="fixtures-", dir=out.parent))
    small = fixture_dir / "4k.bin"
    small.write_bytes(os.urandom(4096))
    payloads = ([] if args.no_small else [(4096, str(small))]) + [(Path(p).stat().st_size, p) for p in args.payload]
    if not payloads or any(size <= 0 for size, _ in payloads):
        parser.error("provide at least one nonempty payload")
    if len({size for size, _ in payloads}) != len(payloads):
        parser.error("payload sizes must be distinct; use --no-small for a custom 4 KiB payload")
    payload_hashes = {str(p): digest(p) for _, p in payloads}
    binaries = {"before": args.before_binary, "after": args.after_binary} if args.before_binary else {}
    metadata = {"arguments": vars(args), "payload_sha256": payload_hashes,
                "binary_sha256": {side: digest(path) for side, path in binaries.items()}}

    def run(endpoint, size, payload, prefix, op, workers, requests, prefill=False, bucket=None, count=1, unique=False):
        bucket = bucket or args.bucket
        command = [args.loadgen, "-endpoint", endpoint, "-bucket", bucket,
                   "-unsigned", "-op", op, "-workers", str(workers),
                   "-size", str(size), "-count", str(count), "-prefix", prefix,
                   "-requests", str(requests), f"-prefill={str(prefill).lower()}"]
        if payload and (prefill or op == "put"):
            command += ["-payload-file", payload]
        if op == "range":
            command += ["-range-size", str(min(size, args.range_size)),
                        "-range-offset", str(max(0, size - args.range_size))]
        if unique:
            command += ["-unique-reads"]
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        record = json.loads(result.stdout)
        validate_record(record, size, op, workers, count, requests, args.range_size)
        return record

    fixtures = Fixtures(args, endpoints, databases, run)
    out.write_text(json.dumps(metadata | {"complete": False, "runs": []}, indent=2) + "\n")
    fixtures.configure()
    for size, payload in payloads:
        for op in args.ops.split(","):
            for workers in worker_counts:
                prefix = f"{args.prefix}-{fixture_dir.name}-{size}-{op}-{workers}"
                requests = max(workers, 3 if size >= 256 << 20 else 7)
                if op == "get" and size <= 128 << 20:
                    # A handful of concurrent short GETs mostly measures
                    # connection/plan startup, not steady-state throughput.
                    requests = max(requests, min(128, max(7, (1 << 30) // size)))
                if op in ("head", "range") or size == 4096:
                    requests = args.short_requests if op != "put" else args.short_requests // 2
                if args.cache_state == "cold" and op == "get" and size > 4096:
                    requests = min(requests, max(workers, 3, (256 << 20) // size))
                requests = max(workers, args.requests or requests)
                if op != "put" and args.cache_state == "warm":
                    for label in endpoints:
                        fixtures.seed(label, size, payload, prefix, 1)
                        warmup_op = "range" if op == "range" and args.range_chunk and size > 8 << 20 else "get"
                        run(endpoints[label], size, payload, prefix, warmup_op, 1, 1)
                        fixtures.verify(label, size, payload, prefix, op, True)
                        if not args.origin_directories and not args.seed_endpoint:
                            schema = identifier("s3b_" + args.bucket)
                            fixtures.sql(label, f"ANALYZE {schema}.objects; ANALYZE {schema}.chunks (file_id, no);")
                for repeat in range(args.repeats):
                    order = ["before", "after"] if repeat % 2 == 0 else ["after", "before"]
                    for label in order:
                        bucket = args.bucket
                        sample_prefix = f"{prefix}-{repeat}-{label}" if args.cache_state == "cold" or op == "put" else prefix
                        count = requests if args.cache_state == "cold" and op != "put" else 1
                        if op != "put" and args.cache_state == "cold":
                            fixtures.seed(label, size, payload, sample_prefix, count)
                            if fixtures.cached(label, sample_prefix) != 0:
                                raise RuntimeError("cold fixture contains cached objects or metadata")
                        if op == "put" and args.isolate_writes:
                            bucket = f"wcmp-{size}-{workers}-{repeat}"
                            fixtures.sql(label, "CHECKPOINT;")
                        try:
                            record = run(endpoints[label], size, payload, sample_prefix, op, workers, requests, bucket=bucket, count=count, unique=args.cache_state == "cold")
                            fixtures.drain(label)
                            if args.cache_state == "cold" and op != "put":
                                fixtures.confirm_cold_fill(label, size, sample_prefix, count, op)
                            if op == "put":
                                fixtures.verify_put(label, size, payload, sample_prefix, bucket=bucket)
                            else:
                                fixtures.verify(label, size, payload, sample_prefix, op, args.cache_state == "warm")
                        finally:
                            if bucket != "bench":
                                request = urllib.request.Request(f"{endpoints[label]}/{bucket}?force=true", method="DELETE")
                                with urllib.request.urlopen(request, timeout=120) as response:
                                    response.read()
                        record.update(label=label, repeat=repeat, payload=payload, cache_state=args.cache_state, cache_path=cache_path(args, op, size), prefix=sample_prefix)
                        raw.append(record)
                        out.write_text(json.dumps(metadata | {"complete": False, "runs": raw}, indent=2) + "\n")
                before = [r["p50_ms"] for r in raw if r["label"] == "before" and r["size"] == size and r["op"] == op and r["workers"] == workers]
                after = [r["p50_ms"] for r in raw if r["label"] == "after" and r["size"] == size and r["op"] == op and r["workers"] == workers]
                baseline, candidate = statistics.median(before), statistics.median(after)
                print(f"{size:>11} {op:>5} c={workers}: {baseline:.3f} -> {candidate:.3f} ms ({candidate / baseline - 1:+.1%})", flush=True)

    cases, failed = summarize(raw)
    if any(digest(p) != payload_hashes[str(p)] for _, p in payloads):
        raise RuntimeError("payload changed during the comparison")
    final = metadata | {"complete": True, "runs": raw, "cases": cases, "failed_gates": failed}
    out.write_text(json.dumps(final, indent=2) + "\n")
    if failed:
        print(f"{len(failed)} cases require investigation; see {out}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
