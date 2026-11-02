#!/usr/bin/env python3
"""Check (or with PGVS3_UPDATE_REFERENCE=1 rewrite) the metrics block of docs/reference.md
against the names live gateways actually emit on /metrics."""
import os
import re
import sys
from pathlib import Path
from urllib.request import urlopen

REFERENCE = Path(__file__).resolve().parents[1] / "docs/reference.md"
BEGIN, END = "<!-- metrics:begin -->", "<!-- metrics:end -->"
SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s")


def scrape(urls):
    found = {}
    for url in urls:
        with urlopen(url.rstrip("/") + "/metrics", timeout=10) as response:
            for line in response.read().decode().splitlines():
                match = SAMPLE.match(line)
                if not match or line.startswith("#"):
                    continue
                labels = found.setdefault(match.group(1), set())
                for pair in re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="', match.group(3) or ""):
                    labels.add(pair)
    return found


def render(found):
    rows = ["| Metric | Type | Labels |", "| --- | --- | --- |"]
    histograms = {name[: -len("_bucket")] for name in found if name.endswith("_bucket")}
    for name in sorted(found):
        if any(name == base + suffix for base in histograms for suffix in ("_bucket", "_sum", "_count")):
            kind = "histogram"
        elif name.endswith("_total"):
            kind = "counter"
        else:
            kind = "gauge"
        labels = ", ".join(f"`{label}`" for label in sorted(found[name])) or "—"
        rows.append(f"| `{name}` | {kind} | {labels} |")
    return "\n".join(rows)


def documented(text):
    block = text.split(BEGIN, 1)[1].split(END, 1)[0]
    return set(re.findall(r"^\| `([a-zA-Z_:][a-zA-Z0-9_:]*)` \|", block, re.M))


def main(urls):
    text = REFERENCE.read_text()
    if BEGIN not in text or END not in text:
        raise SystemExit(f"{REFERENCE} lacks the {BEGIN}/{END} markers")
    found = scrape(urls)
    if not found:
        raise SystemExit("no metrics scraped")
    if os.environ.get("PGVS3_UPDATE_REFERENCE") == "1":
        head, rest = text.split(BEGIN, 1)
        REFERENCE.write_text(head + BEGIN + "\n" + render(found) + "\n" + END + rest.split(END, 1)[1])
        print(f"reference: wrote {len(found)} metrics")
        return
    missing = sorted(set(found) - documented(text))
    if missing:
        raise SystemExit("undocumented metrics (rerun the contract with PGVS3_UPDATE_REFERENCE=1):\n  " + "\n  ".join(missing))
    stale = sorted(documented(text) - set(found))
    print(f"reference: {len(found)} emitted metrics documented" + (f"; not emitted in this run: {', '.join(stale)}" if stale else ""))


if __name__ == "__main__":
    main(sys.argv[1:])
