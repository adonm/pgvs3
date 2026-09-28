#!/usr/bin/env python3
"""Compare storage-suite stacks from kind-bench JSONL: per-query latency.

    storage_report.py .tmp/pgvs3/kind-bench.jsonl

Columns per stack: first (pass 1, cold DuckDB cache, one connection), warm
(best of later passes) and fresh (best fresh-instance run: new DuckDB, empty
cache, storage connection already open).
"""

import json
import math
import sys


def best(passes: list[dict], q: str) -> float | None:
    times = [p["times"][q] for p in passes if q in p["times"] and q not in p["errors"]]
    return min(times) if times else None


def geomean(values: list[float]) -> float:
    values = [max(v, 1e-3) for v in values]
    return math.exp(sum(map(math.log, values)) / len(values))


records = [json.loads(line) for line in open(sys.argv[1]) if line.startswith("{")]
records = [r for r in records if "fresh" in r]
stacks = [r["stack"] for r in records]
queries = [f"Q{n}" for n in records[0]["queries"]]
cols = {}
for r in records:
    cols[r["stack"]] = {
        "first": {q: r["passes"][0]["times"].get(q) for q in queries},
        "warm": {q: best(r["passes"][1:], q) for q in queries} if len(r["passes"]) > 1 else {},
        "fresh": {q: best(r["fresh"], q) for q in queries} if r["fresh"] else {},
    }

kinds = [k for k in ("first", "warm", "fresh") if all(cols[s][k] for s in stacks)]
print(f"DuckDB {records[0]['duckdb']}; rows {records[0].get('rows')}; "
      f"load s: " + ", ".join(f"{r['stack']}={r.get('load_s') or 0:.0f}" for r in records))
print()
head = ["query"] + [f"{s} {k} ms" for k in kinds for s in stacks]
if len(stacks) == 2:
    head += [f"{k} {stacks[1]}/{stacks[0]}" for k in kinds]
print("| " + " | ".join(head) + " |")
print("|" + "---|" * len(head))
rows = queries + ["total", "geomean"]
for q in rows:
    cells = [q]
    ratios = []
    for k in kinds:
        for s in stacks:
            values = [v for v in cols[s][k].values() if v is not None]
            v = (sum(values) if q == "total" else geomean(values) if q == "geomean"
                 else cols[s][k][q])
            cells.append("-" if v is None else f"{v * 1000:.0f}")
        if len(stacks) == 2:
            a, b = (float(cells[-2]) if cells[-2] != "-" else None,
                    float(cells[-1]) if cells[-1] != "-" else None)
            ratios.append("-" if not a or b is None else f"{b / a:.2f}")
    print("| " + " | ".join(cells + ratios) + " |")
