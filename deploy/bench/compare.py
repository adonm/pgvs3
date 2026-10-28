#!/usr/bin/env python3
"""Paired before/after runs using the existing loadgen, not a second load engine.

Requires disposable endpoints/databases: PUT measurements overwrite their own
benchmark keys. Seed through --seed-endpoint to measure a read-through tier
without accidentally warming its whole-object/write-back cache.
"""

import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--loadgen", required=True)
    parser.add_argument("--payload", action="append", default=[])
    parser.add_argument("--seed-endpoint")
    parser.add_argument("--ops", default="head,get,range,put")
    parser.add_argument("--workers", default="1,4")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--short-requests", type=int, default=200)
    parser.add_argument("--postgres-before", help="disposable database URL; ANALYZE reads and CHECKPOINT writes")
    parser.add_argument("--postgres-after", help="disposable database URL; ANALYZE reads and CHECKPOINT writes")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--isolate-writes", action="store_true", help="create/drop a fresh bucket for each PUT run")
    parser.add_argument("--prefix", default="comparison")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("use at least three paired repetitions")
    if args.short_requests < 2:
        parser.error("--short-requests must be at least two")
    if bool(args.postgres_before) != bool(args.postgres_after):
        parser.error("provide both disposable database URLs, or neither")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    raw = []
    endpoints = {"before": args.before, "after": args.after}
    databases = {"before": args.postgres_before, "after": args.postgres_after}
    payloads = [(4096, None)] + [(Path(p).stat().st_size, p) for p in args.payload]

    def sql(label, statement):
        if databases[label]:
            subprocess.run([args.psql, databases[label], "-v", "ON_ERROR_STOP=1", "-c", statement], check=True, capture_output=True)

    def run(endpoint, size, payload, prefix, op, workers, requests, prefill=False, bucket="bench"):
        command = [args.loadgen, "-endpoint", endpoint, "-bucket", bucket,
                   "-unsigned", "-op", op, "-workers", str(workers),
                   "-size", str(size), "-count", "1", "-prefix", prefix,
                   "-requests", str(requests), f"-prefill={str(prefill).lower()}"]
        if payload and (prefill or op == "put"):
            command += ["-payload-file", payload]
        if op == "range":
            command += ["-range-size", str(min(size, 8192)),
                        "-range-offset", str(max(0, size - 8192)), "-warmup"]
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        record = json.loads(result.stdout)
        if record["errors"] or record["requests"] != requests:
            raise RuntimeError(f"benchmark failed: {record}; {result.stderr}")
        return record

    for size, payload in payloads:
        prefix = f"{args.prefix}-{size}"
        for endpoint in ([args.seed_endpoint] if args.seed_endpoint else endpoints.values()):
            run(endpoint, size, payload, prefix, "get", 1, 1, prefill=True)
        if not args.seed_endpoint:
            for label in endpoints:
                sql(label, 'ANALYZE "s3b_bench".objects; ANALYZE "s3b_bench".chunks (file_id, no);')
        for op in args.ops.split(","):
            for workers in map(int, args.workers.split(",")):
                requests = max(workers, 3 if size >= 256 << 20 else 7)
                if op == "get" and size <= 128 << 20:
                    # A handful of concurrent short GETs mostly measures
                    # connection/plan startup, not steady-state throughput.
                    requests = max(requests, 128)
                if op in ("head", "range") or size == 4096:
                    requests = args.short_requests if op != "put" else args.short_requests // 2
                # Warm reads, but not writes: range warmup must not become GET.
                if op == "get":
                    for endpoint in endpoints.values():
                        run(endpoint, size, payload, prefix, op, 1, 1)
                for repeat in range(args.repeats):
                    order = ["before", "after"] if repeat % 2 == 0 else ["after", "before"]
                    for label in order:
                        bucket = "bench"
                        if op == "put" and args.isolate_writes:
                            bucket = f"wcmp-{size}-{workers}-{repeat}"
                            sql(label, "CHECKPOINT;")
                        try:
                            record = run(endpoints[label], size, payload, prefix, op, workers, requests, bucket=bucket)
                        finally:
                            if bucket != "bench":
                                request = urllib.request.Request(f"{endpoints[label]}/{bucket}?force=true", method="DELETE")
                                with urllib.request.urlopen(request, timeout=120) as response:
                                    response.read()
                        record.update(label=label, repeat=repeat, payload=payload)
                        raw.append(record)
                        out.write_text(json.dumps({"arguments": vars(args), "runs": raw}, indent=2) + "\n")
                before = [r["p50_ms"] for r in raw if r["label"] == "before" and r["size"] == size and r["op"] == op and r["workers"] == workers]
                after = [r["p50_ms"] for r in raw if r["label"] == "after" and r["size"] == size and r["op"] == op and r["workers"] == workers]
                baseline, candidate = statistics.median(before), statistics.median(after)
                print(f"{size:>11} {op:>5} c={workers}: {baseline:.3f} -> {candidate:.3f} ms ({candidate / baseline - 1:+.1%})", flush=True)

    failed = []
    cases = {(r["size"], r["op"], r["workers"]) for r in raw}
    for size, op, workers in sorted(cases):
        by_label = {label: [r for r in raw if (r["size"], r["op"], r["workers"], r["label"]) == (size, op, workers, label)] for label in endpoints}
        old, new = by_label["before"], by_label["after"]
        time_ratio = statistics.median(r["p50_ms"] for r in new) / statistics.median(r["p50_ms"] for r in old)
        rate_ratio = statistics.median(r["requests_per_s"] for r in new) / statistics.median(r["requests_per_s"] for r in old)
        if time_ratio > 1.20 or rate_ratio < 0.80:
            failed.append({"size": size, "op": op, "workers": workers, "p50_ratio": time_ratio, "throughput_ratio": rate_ratio})
    final = {"arguments": vars(args), "runs": raw, "failed_gates": failed}
    out.write_text(json.dumps(final, indent=2) + "\n")
    if failed:
        print(f"{len(failed)} cases require investigation; see {out}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
