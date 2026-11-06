#!/usr/bin/env python3
"""Fetch and verify the pinned DuckDB 2.0.0 signed extensions.

Run this before ``docker build`` (``just duckdb-image`` does). The final image
uses the official DuckDB binary; this fetcher only prepares the nine extension
artifacts. The lock file is the source of truth: every archive, extracted
extension and every DuckDB ``.info`` installation-metadata sidecar is checked
before it is placed in the build context. The official CLI hash is retained in
the lock so host checks can verify the image binary.

The fetcher is deliberately boring: Python's standard library, pinned URLs and
SHA-256 hashes, atomic writes. A pre-populated ``--cache`` directory is accepted
for offline/repeat builds.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import urllib.request
from pathlib import Path
from urllib.error import URLError

ROOT = Path(__file__).resolve().parent
LOCK_PATH = ROOT / "extensions.lock"
SUMS_PATH = ROOT / "EXTENSIONS.sha256"
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
MODULES = (
    "spatial",
    "ducklake",
    "postgres_scanner",
    "fts",
    "vss",
    "quack",
    "httpfs",
    "raquet",
    "differential",
)


class FetchError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def check_hash(path: Path, expected: str, expected_size: int | None = None) -> bool:
    if not path.is_file():
        return False
    if sha256_file(path) != expected:
        return False
    if expected_size is not None and path.stat().st_size != expected_size:
        return False
    return True


def download(url: str, target: Path, expected: str, expected_size: int | None, network: bool) -> None:
    if check_hash(target, expected, expected_size):
        return
    if not network:
        raise FetchError(
            f"missing or mismatched artifact {target}; rerun with network enabled"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    print(f"download {url}", flush=True)
    request = urllib.request.Request(
        url, headers={"User-Agent": "pgvs3-duckdb-image/1"}
    )
    temporary = target.with_name(target.name + f".tmp-{os.getpid()}")
    try:
        with urllib.request.urlopen(request, timeout=300) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        if sha256_file(temporary) != expected:
            raise FetchError(f"downloaded artifact hash mismatch: {target.name}")
        if expected_size is not None and temporary.stat().st_size != expected_size:
            raise FetchError(
                f"downloaded artifact size mismatch: {target.name} "
                f"({temporary.stat().st_size} != {expected_size})"
            )
        temporary.replace(target)
    except URLError as error:
        raise FetchError(f"cannot download {url}: {error}") from error
    finally:
        temporary.unlink(missing_ok=True)


def place_bytes(target: Path, data: bytes, expected: str, length: int, write: bool = True) -> None:
    if check_hash(target, expected, length):
        return
    if not write:
        raise FetchError(f"missing or mismatched artifact {target}; verify-only made no changes")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + f".tmp-{os.getpid()}")
    try:
        temporary.write_bytes(data)
        if sha256_file(temporary) != expected:
            raise FetchError(f"generated .info hash mismatch: {target.name}")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def fetch_extensions(lock: dict, cache: Path, network: bool, write_info: bool = True) -> None:
    extension_dir = (
        cache
        / "extensions"
        / lock["duckdb"]["version"]
        / lock["platform"]
    )
    extension_dir.mkdir(parents=True, exist_ok=True)
    for module in lock["extensions"]:
        name = module["name"]
        extension = extension_dir / f"{name}.duckdb_extension"
        info = extension_dir / f"{name}.duckdb_extension.info"
        if not check_hash(extension, module["sha256"], module["bytes"]):
            archive = cache / "downloads" / Path(module["url"]).name
            download(
                module["url"],
                archive,
                module["sha256_gz"],
                module["bytes_gz"],
                network,
            )
            temporary = extension.with_name(extension.name + f".tmp-{os.getpid()}")
            try:
                with gzip.open(archive, "rb") as source, temporary.open("wb") as output:
                    shutil.copyfileobj(source, output)
                if sha256_file(temporary) != module["sha256"]:
                    raise FetchError(f"extracted extension hash mismatch: {name}")
                if temporary.stat().st_size != module["bytes"]:
                    raise FetchError(f"extracted extension size mismatch: {name}")
                temporary.replace(extension)
            finally:
                temporary.unlink(missing_ok=True)
        else:
            print(f"cached   {name} {module['version']}", flush=True)
        info_bytes = base64.b64decode(module["info_b64"], validate=True)
        place_bytes(
            info,
            info_bytes,
            module["sha256_info"],
            module["bytes_info"],
            write=write_info,
        )


def write_sums(lock: dict) -> None:
    lines = [
        f"{module['sha256']}  {module['name']}.duckdb_extension\n"
        for module in sorted(lock["extensions"], key=lambda item: item["name"])
    ]
    expected = "".join(lines)
    if SUMS_PATH.read_text() != expected:
        SUMS_PATH.write_text(expected)


def validate_lock(lock: dict, platform: str) -> None:
    if lock.get("schema") != 1:
        raise FetchError("unsupported extensions.lock schema")
    if lock["platform"] != platform:
        raise FetchError(
            f"lock is for {lock['platform']}; {platform} hashes were not computed"
        )
    required = {
        "name",
        "origin",
        "version",
        "url",
        "sha256_gz",
        "bytes_gz",
        "sha256",
        "bytes",
        "sha256_info",
        "bytes_info",
        "info_b64",
    }
    names = []
    for module in lock["extensions"]:
        missing = required - module.keys()
        if missing:
            raise FetchError(f"{module.get('name', '?')} lock entry misses {sorted(missing)}")
        names.append(module["name"])
        for field in ("sha256_gz", "sha256", "sha256_info"):
            if not HASH_RE.fullmatch(module[field]):
                raise FetchError(f"{module['name']} has an invalid {field}")
        info = base64.b64decode(module["info_b64"], validate=True)
        if hashlib.sha256(info).hexdigest() != module["sha256_info"]:
            raise FetchError(f"{module['name']} embedded .info hash mismatch")
        if len(info) != module["bytes_info"]:
            raise FetchError(f"{module['name']} embedded .info size mismatch")
    if tuple(sorted(names)) != tuple(sorted(MODULES)):
        raise FetchError(f"lock modules differ from the expected nine: {sorted(names)}")
    cli = lock["cli"]
    for field in ("sha256_tar", "sha256"):
        if not HASH_RE.fullmatch(cli[field]):
            raise FetchError(f"CLI has an invalid {field}")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", lock["base"]["digest"]):
        raise FetchError("base image digest is not pinned")
    if lock["base"]["image"] != "duckdb/duckdb:2.0.0":
        raise FetchError("base image is not the pinned official DuckDB image")
    expat = lock.get("libexpat", {})
    if not HASH_RE.fullmatch(expat.get("sha256", "")):
        raise FetchError("libexpat hash is not pinned")
    if expat.get("version") != "2.5.0-1+deb12u5":
        raise FetchError("libexpat package version is not pinned")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache",
        type=Path,
        default=ROOT / ".artifacts",
        help="artifact directory (default: deploy/duckdb/.artifacts)",
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="artifact platform; only linux_amd64 is pinned in this increment",
    )
    parser.add_argument(
        "--no-network",
        action="store_true",
        help="fail instead of downloading a missing artifact",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="verify an existing cache and EXTENSIONS.sha256 without writing",
    )
    args = parser.parse_args(argv)
    try:
        lock = json.loads(LOCK_PATH.read_text())
        platform = args.platform or lock["platform"]
        validate_lock(lock, platform)
        cache = args.cache.resolve()
        network = not (args.no_network or args.verify_only)
        fetch_extensions(lock, cache, network, write_info=not args.verify_only)
        if args.verify_only:
            expected = "".join(
                f"{module['sha256']}  {module['name']}.duckdb_extension\n"
                for module in sorted(lock["extensions"], key=lambda item: item["name"])
            )
            if SUMS_PATH.read_text() != expected:
                raise FetchError("EXTENSIONS.sha256 is out of sync with extensions.lock")
        else:
            write_sums(lock)
        modules = ", ".join(
            f"{module['name']}={module['version']}"
            for module in lock["extensions"]
        )
        total = sum(module["bytes"] for module in lock["extensions"])
        print(f"verified {len(lock['extensions'])} modules ({total} bytes)")
        print(modules)
        return 0
    except FetchError as error:
        print(f"fetch_artifacts: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
