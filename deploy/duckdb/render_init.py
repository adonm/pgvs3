#!/usr/bin/env python3
"""Render the baked SQL templates into a concrete init SQL file.

Hardcharger's embedded DuckDB deliberately omits ``getenv()`` (a statement must
never be able to read the process environment), and its init SQL is executed
verbatim with no shell-style expansion. The supported pattern is a concrete
init SQL file, so pgvs3 renders one from the environment before the container
starts and bind-mounts it over ``/opt/pgvs3-duckdb/init.sql``.

Sentinels:

* ``__DUCKDB_THREADS__`` outside quotes -> a validated bare SQL expression.
* ``'__PGVS3_URL__'`` inside quotes -> the value, SQL-escaped, between quotes.

The templates keep ``position('__PGVS3_' IN ...) = 1`` guards, so a template
that reaches the server unrendered fails at startup naming the variable. This
renderer fails earlier, naming the variable and never a value.

Usage:
    python3 deploy/duckdb/render_init.py --output FILE [--one-shot] [--mode 600]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULTS = {
    "PGVS3_PREFIX": "ducklake",
    "PGVS3_METADATA_SCHEMA": "ducklake_pgvs3",
    "PGVS3_ENDPOINT": "127.0.0.1:8014",
    "PGVS3_S3_ACCESS_KEY": "pgvs3",
    "PGVS3_S3_SECRET_KEY": "pgvs3",
    "PGVS3_S3_REGION": "us-east-1",
    "PGVS3_S3_USE_SSL": "",
    "DUCKDB_MEMORY_LIMIT": "2GB",
    "DUCKDB_THREADS": "4",
    "DUCKDB_TEMP_DIRECTORY": "/tmp/duckdb",
}
REQUIRED = ("PGVS3_URL", "PGVS3_BUCKET")
PLACEHOLDER = re.compile(r"__([A-Z][A-Z0-9_]*)__")
THREADS = re.compile(r"^[1-9][0-9]{0,3}$")
MEMORY = re.compile(r"^[0-9]+(\.[0-9]+)?(KB|MB|GB|TB)$", re.IGNORECASE)
BARE = ("DUCKDB_THREADS", "DUCKDB_MEMORY_LIMIT", "DUCKDB_TEMP_DIRECTORY",
        "PGVS3_S3_USE_SSL")
NO_SPACE = ("PGVS3_S3_ACCESS_KEY", "PGVS3_S3_SECRET_KEY", "PGVS3_S3_REGION",
            "PGVS3_METADATA_SCHEMA", "PGVS3_BUCKET", "PGVS3_PREFIX")


class RenderError(RuntimeError):
    pass


def validate(name: str, value: str) -> str:
    """Validate one value. Error text names the variable, never the value."""
    if any(character in value for character in ("\x00", "\n", "\r")):
        raise RenderError(f"{name} contains a control character")
    if name == "DUCKDB_THREADS" and not THREADS.fullmatch(value):
        raise RenderError("DUCKDB_THREADS must be a positive integer")
    if name == "DUCKDB_MEMORY_LIMIT" and not MEMORY.fullmatch(value):
        raise RenderError("DUCKDB_MEMORY_LIMIT must look like 2GB")
    if name == "DUCKDB_TEMP_DIRECTORY" and not value.startswith("/"):
        raise RenderError("DUCKDB_TEMP_DIRECTORY must be an absolute path")
    if name == "PGVS3_S3_USE_SSL" and value not in ("", "true", "false"):
        raise RenderError("PGVS3_S3_USE_SSL must be true, false or empty")
    if name in NO_SPACE and re.search(r"\s", value):
        raise RenderError(f"{name} must not contain whitespace")
    if "'" in value:
        raise RenderError(f"{name} must not contain a quote")
    return value


def values_from(environment: dict[str, str]) -> dict[str, str]:
    missing = [name for name in REQUIRED if not environment.get(name)]
    if missing:
        raise RenderError("missing required environment: " + ", ".join(missing))
    values = {name: validate(name, environment[name]) for name in REQUIRED}
    for name, default in DEFAULTS.items():
        values[name] = validate(name, environment.get(name, default))
    return values


def template(one_shot: bool) -> str:
    """extensions.sql then lake.sql: one pure-SQL document."""
    parts = [
        "-- Rendered by deploy/duckdb/render_init.py. Do not edit.\n",
        "-- Pure SQL: hardcharger-server runs init SQL through DuckDB's C++\n",
        "-- Connection::Query, which has no CLI dot-commands and no getenv().\n",
        "\n",
        (ROOT / "extensions.sql").read_text(),
        "\n",
        (ROOT / "lake.sql").read_text(),
    ]
    if one_shot:
        parts.extend(["\nSELECT 'PGVS3_NO_DAEMON_OK';\n"])
    return "".join(parts)


def render(text: str, values: dict[str, str]) -> str:
    def substitute(match: re.Match[str]) -> str:
        return values.get(match.group(1), match.group(0))

    rendered = PLACEHOLDER.sub(substitute, text)
    leftovers = sorted({match.group(1) for match in PLACEHOLDER.finditer(rendered)})
    if leftovers:
        raise RenderError("template needs unhandled sentinels: " + ", ".join(leftovers))
    return rendered


def render_text(environment: dict[str, str], one_shot: bool = False) -> str:
    values = values_from(environment)
    return render(template(one_shot), values)


def write(output: Path, text: str, mode: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + f".tmp-{os.getpid()}")
    try:
        temporary.write_text(text)
        temporary.chmod(mode)
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True,
                        help="rendered SQL file (created mode 0600)")
    parser.add_argument("--one-shot", action="store_true",
                        help="add the lake-only PGVS3_NO_DAEMON_OK marker")
    parser.add_argument("--mode", default="600",
                        help="octal mode for the rendered file (default 600: "
                             "owner only; mount it read-only into the server)")
    args = parser.parse_args(argv)
    try:
        mode = int(args.mode, 8)
        if mode & 0o444 == 0:
            raise RenderError("--mode must let the server read the file")
    except ValueError as error:
        print(f"render_init: invalid --mode: {error}", file=sys.stderr)
        return 1
    try:
        write(args.output, render_text(dict(os.environ), args.one_shot), mode)
        print(f"rendered {args.output}", flush=True)
        return 0
    except RenderError as error:
        print(f"render_init: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
