#!/usr/bin/env python3
"""Host-side verification for the native official-DuckDB image.

This runs on the host and drives Docker. Nothing here is copied into the
image. It verifies the image entrypoint/user, the baked CLI and extension
bytes against the lock, and loads all nine modules with autoinstall/autoload
and unsigned loading disabled in a read-only, non-root container.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import subprocess
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOCK_PATH = ROOT / "extensions.lock"
SUMS_PATH = ROOT / "EXTENSIONS.sha256"
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
EXTENSION_DIR = "/opt/duckdb/.duckdb/extensions/v2.0.0/linux_amd64"


class CheckError(RuntimeError):
    pass


def docker(*args: str, input_text: str | None = None, timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args],
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def docker_inspect(image: str, template: str) -> str:
    result = docker("inspect", "--format", template, image)
    if result.returncode != 0:
        raise CheckError(f"docker inspect failed: {result.stderr.strip()}")
    return result.stdout.strip()


def read_tar_members(container: str, path: str) -> dict[str, tuple[int, str]]:
    """Hash regular files from ``docker cp container:path -`` without a temp copy."""
    process = subprocess.Popen(
        ["docker", "cp", f"{container}:{path}", "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    members: dict[str, tuple[int, str]] = {}
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                source = tar.extractfile(member)
                if source is None:
                    continue
                digest = hashlib.sha256()
                length = 0
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
                    length += len(block)
                members[Path(member.name).name] = (length, digest.hexdigest())
    finally:
        process.stdout.close()
    stderr = process.stderr.read().decode() if process.stderr else ""
    if process.wait() != 0:
        raise CheckError(f"docker cp {container}:{path} failed: {stderr.strip()}")
    return members


def lock_and_sums() -> dict:
    lock = json.loads(LOCK_PATH.read_text())
    expected_sums = "".join(
        f"{module['sha256']}  {module['name']}.duckdb_extension\n"
        for module in sorted(lock["extensions"], key=lambda item: item["name"])
    )
    if SUMS_PATH.read_text() != expected_sums:
        raise CheckError("EXTENSIONS.sha256 is out of sync with extensions.lock")
    return lock


def verify_bytes(image: str, lock: dict) -> None:
    helper = f"pgvs3-check-{Path.cwd().name}-{abs(hash(image)) % 100000}"
    docker("rm", "-f", helper)
    created = docker("create", "--name", helper, image)
    if created.returncode != 0:
        raise CheckError(f"cannot create inspection container: {created.stderr.strip()}")
    try:
        binaries = read_tar_members(helper, "/duckdb")
        extensions = read_tar_members(helper, EXTENSION_DIR + "/.")
    finally:
        docker("rm", "-f", helper)
    cli = lock["cli"]
    if "duckdb" not in binaries:
        raise CheckError("image has no /duckdb binary")
    length, digest = binaries["duckdb"]
    if length != cli["bytes"] or digest != cli["sha256"]:
        raise CheckError("image CLI hash or size differs from extensions.lock")
    for module in lock["extensions"]:
        name = module["name"]
        for suffix, field, length_field in (
            (".duckdb_extension", "sha256", "bytes"),
            (".duckdb_extension.info", "sha256_info", "bytes_info"),
        ):
            file_name = f"{name}{suffix}"
            if file_name not in extensions:
                raise CheckError(f"missing baked file: {file_name}")
            actual_length, actual_digest = extensions[file_name]
            if actual_length != module[length_field] or actual_digest != module[field]:
                raise CheckError(f"baked file hash or size mismatch: {file_name}")


def verify_config(image: str) -> None:
    entrypoint = json.loads(docker_inspect(image, "{{json .Config.Entrypoint}}"))
    command = json.loads(docker_inspect(image, "{{json .Config.Cmd}}"))
    user = docker_inspect(image, "{{.Config.User}}")
    workdir = docker_inspect(image, "{{.Config.WorkingDir}}")
    environment = docker_inspect(image, "{{json .Config.Env}}")
    if entrypoint != ["/duckdb"]:
        raise CheckError(f"unexpected entrypoint: {entrypoint!r}")
    if user != "65532:65532":
        raise CheckError(f"unexpected image user: {user!r}")
    if workdir != "/tmp":
        raise CheckError(f"unexpected working directory: {workdir!r}")
    if not isinstance(command, list) or "-init" not in command or "-bail" not in command:
        raise CheckError(f"unexpected command: {command!r}")
    if "HOME=/opt/duckdb" not in json.loads(environment):
        raise CheckError("HOME is not /opt/duckdb")
    for binary, args in (
        ("/bin/sh", ("-c", "true")),
        ("/usr/bin/python3", ("--version",)),
    ):
        result = docker("run", "--rm", "--entrypoint", binary, image, *args)
        if result.returncode == 0:
            raise CheckError(f"unexpected executable in final image: {binary}")


def check_sql(lock: dict) -> str:
    names = ",".join("'" + module + "'" for module in MODULES)
    return "\n".join(
        [
            "SET autoinstall_known_extensions=false;",
            "SET autoload_known_extensions=false;",
            "SET allow_unsigned_extensions=false;",
            *(f"LOAD {module};" for module in MODULES),
            "SELECT 'meta', version(), current_setting('allow_unsigned_extensions');",
            "SELECT 'ext', extension_name, extension_version, installed_from, loaded",
            "FROM duckdb_extensions()",
            f"WHERE extension_name IN ({names})",
            "ORDER BY extension_name;",
        ]
    )


def run_sql(image: str, sql: str) -> str:
    result = docker(
        "run",
        "--rm",
        "-i",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,size=1g",
        "--entrypoint",
        "/duckdb",
        image,
        "-no-agent",
        "-batch",
        "-noheader",
        "-csv",
        "-bail",
        input_text=sql,
        timeout=180,
    )
    if result.returncode != 0:
        raise CheckError((result.stderr or result.stdout).strip())
    return result.stdout


def verify_modules(image: str, lock: dict) -> None:
    rows = [row for row in csv.reader(io.StringIO(run_sql(image, check_sql(lock)))) if row]
    meta = [row for row in rows if row[0] == "meta"]
    extensions = [row for row in rows if row[0] == "ext"]
    if len(meta) != 1:
        raise CheckError(f"unexpected metadata rows: {meta!r}")
    if meta[0][1] != lock["duckdb"]["version"]:
        raise CheckError(f"version mismatch: {meta[0][1]} != {lock['duckdb']['version']}")
    if meta[0][2] != "false":
        raise CheckError("allow_unsigned_extensions is not false")
    expected = {module["name"]: module for module in lock["extensions"]}
    if len(extensions) != len(MODULES):
        raise CheckError(f"expected {len(MODULES)} loaded extensions, got {len(extensions)}")
    for kind, name, version, origin, loaded in extensions:
        del kind
        if name not in expected:
            raise CheckError(f"unexpected extension loaded: {name}")
        if loaded != "true":
            raise CheckError(f"extension not loaded: {name}")
        if version != expected[name]["version"]:
            raise CheckError(f"extension version mismatch: {name} {version}")
        if origin != expected[name]["origin"]:
            raise CheckError(f"extension origin mismatch: {name} {origin}")
    print(
        f"ok duckdb={meta[0][1]} modules={len(extensions)}/9 "
        "signed=required autoinstall=off autoload=off read-only=yes nonroot=65532"
    )


def check(image: str) -> int:
    lock = lock_and_sums()
    verify_config(image)
    verify_bytes(image, lock)
    verify_modules(image, lock)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check",))
    parser.add_argument("image")
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            return check(args.image)
    except (CheckError, OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"image check failed: {error}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
