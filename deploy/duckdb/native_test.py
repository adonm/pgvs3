#!/usr/bin/env python3
"""Focused stdlib tests for the native official-DuckDB image.

These tests do not start DuckDB, PostgreSQL, S3 or Docker. They pin the native
SQL bootstrap shape, the official base/entrypoint contract, the artifact lock
and the absence of an in-image Python/shell supervisor.
"""

from __future__ import annotations

import base64
import hashlib
import json
import unittest
from pathlib import Path

import fetch_artifacts
import image_tools

HERE = Path(__file__).resolve().parent
SECRET_URL = "postgresql://user:p%40ss@db.example:5433/pgvs3?sslmode=verify-full"
SECRET_TOKEN = "0123456789abcdef"


class SqlShapeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.extensions = (HERE / "extensions.sql").read_text()
        self.lake = (HERE / "lake.sql").read_text()
        self.init = (HERE / "init.sql").read_text()
        self.all_sql = "\n".join((self.extensions, self.lake, self.init))
        self.statements = "\n".join(
            line for line in self.all_sql.splitlines()
            if not line.lstrip().startswith("--")
        )

    def test_extensions_are_baked_and_unsigned_loading_is_off(self) -> None:
        self.assertIn("SET autoinstall_known_extensions=false;", self.extensions)
        self.assertIn("SET autoload_known_extensions=false;", self.extensions)
        self.assertIn("SET allow_unsigned_extensions=false;", self.extensions)
        for module in image_tools.MODULES:
            self.assertIn(f"LOAD {module};", self.extensions)
        self.assertNotIn("INSTALL", self.statements)

    def test_lake_sql_uses_only_getenv_secrets(self) -> None:
        self.assertTrue(self.lake.startswith("--"))
        self.assertIn(".read /opt/pgvs3-duckdb/extensions.sql", self.lake)
        self.assertIn("CREATE TEMP SECRET pgvs3_pg (", self.lake)
        self.assertIn("URI getenv('PGVS3_URL')", self.lake)
        self.assertIn("KEY_ID getenv('PGVS3_S3_ACCESS_KEY')", self.lake)
        self.assertIn("SECRET getenv('PGVS3_S3_SECRET_KEY')", self.lake)
        self.assertIn("METADATA_SCHEMA getenv('PGVS3_METADATA_SCHEMA')", self.lake)
        self.assertIn("ATTACH 'ducklake:pgvs3_lake' AS lake", self.lake)
        self.assertIn("AUTOMATIC_MIGRATION false", self.lake)
        self.assertIn("USE lake;", self.lake)
        self.assertNotIn("CREATE DATABASE", self.statements)
        self.assertNotIn("postgres://", self.statements)
        self.assertNotIn("postgresql://", self.statements)
        self.assertNotIn(SECRET_URL, self.all_sql)
        self.assertNotIn("p%40ss", self.all_sql)
        self.assertNotIn(SECRET_TOKEN, self.all_sql)

    def test_native_validation_covers_required_values_without_echoing(self) -> None:
        for variable in ("PGVS3_URL", "PGVS3_BUCKET", "QUACK_TOKEN", "QUACK_BIND"):
            self.assertIn(variable, self.all_sql)
        self.assertIn("error('PGVS3_URL is required", self.lake)
        self.assertIn("error('PGVS3_BUCKET is required", self.lake)
        self.assertIn("error('QUACK_TOKEN must be at least 4", self.init)
        self.assertIn("regexp_matches(getenv('PGVS3_URL'), '^postgres(ql)?://')", self.lake)
        self.assertIn("regexp_matches(getenv('PGVS3_BUCKET'), '^[a-z0-9._-]{1,58}$')", self.lake)
        # No validation error prints an env value.
        self.assertNotIn("error(getenv", self.all_sql)

    def test_server_suppresses_quack_output_and_prints_ready(self) -> None:
        self.assertTrue(self.init.startswith("--"))
        self.assertIn(".output /dev/null", self.init)
        self.assertLess(self.init.index(".read /opt/pgvs3-duckdb/lake.sql"), self.init.index("CALL quack_serve("))
        self.assertIn("token=getenv('QUACK_TOKEN')", self.init)
        self.assertIn("allow_other_hostname=CAST(getenv('QUACK_ALLOW_OTHER_HOSTNAME') AS BOOLEAN)", self.init)
        self.assertLess(self.init.index("CALL quack_serve("), self.init.index(".output\n"))
        self.assertLess(self.init.index(".output\n"), self.init.index(".print PGVS3_READY"))
        self.assertIn("warning Quack sessions start in the memory catalog", self.init)
        self.assertIn("lake.main.<table>", self.init)

    def test_lake_only_has_no_quack_server(self) -> None:
        self.assertNotIn("quack_serve", self.lake)
        self.assertNotIn("QUACK_TOKEN", self.lake)
        self.assertIn("SELECT 'PGVS3_LAKE_OK';", self.lake)
        lake_only = (HERE / "lake-only.sql").read_text()
        self.assertIn(".output /dev/null", lake_only)
        self.assertIn(".read /opt/pgvs3-duckdb/lake.sql", lake_only)
        self.assertIn("SELECT 'PGVS3_NO_DAEMON_OK';", lake_only)
        self.assertNotIn("quack_serve", lake_only)


class ImageContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dockerfile = (HERE / "Dockerfile").read_text()

    def test_official_base_and_native_entrypoint(self) -> None:
        self.assertIn(
            "duckdb/duckdb:2.0.0@sha256:"
            "453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221",
            self.dockerfile,
        )
        self.assertIn('ENTRYPOINT ["/duckdb"]', self.dockerfile)
        self.assertIn('"-init", "/opt/pgvs3-duckdb/init.sql"', self.dockerfile)
        self.assertIn('"-bail"', self.dockerfile)
        self.assertIn("USER 65532:65532", self.dockerfile)
        self.assertIn("STOPSIGNAL SIGTERM", self.dockerfile)

    def test_no_python_shell_or_supervisor_in_final_image(self) -> None:
        active = "\n".join(
            line for line in self.dockerfile.splitlines()
            if not line.lstrip().startswith("#")
        )
        for text in ("python3", "bootstrap.py", "smoke.py", "/bin/sh", "ENTRYPOINT [\"sh\""):
            self.assertNotIn(text, active)
        final = active.split(
            "FROM --platform=linux/amd64 duckdb/duckdb:2.0.0@sha256:"
            "453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221",
            -1,
        )[-1]
        self.assertNotIn("apt-get", final)
        self.assertNotIn("RUN ", final)
        self.assertIn("AS expat", self.dockerfile)
        self.assertIn("AS verify", self.dockerfile)

    def test_verified_extension_and_expat_bytes(self) -> None:
        self.assertIn("libexpat1=2.5.0-1+deb12u5", self.dockerfile)
        self.assertIn(
            "b342d94f6a71df596f19b1b0d4d3af9b883f0333027de6c3fc0dbc9ff8c45057",
            self.dockerfile,
        )
        self.assertIn("sha256sum -c /verify/EXTENSIONS.sha256", self.dockerfile)
        self.assertIn(
            "c41f957db5645d545a9caef430994cf262d24fad9297fef70c691395aadb43fb",
            self.dockerfile,
        )
        self.assertIn(
            "COPY --from=verify /verify/extensions/ "
            "/opt/duckdb/.duckdb/extensions/v2.0.0/${DUCKDB_PLATFORM}/",
            self.dockerfile,
        )


class ManifestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = json.loads((HERE / "extensions.lock").read_text())

    def test_lock_matches_sums_info_and_official_base(self) -> None:
        lock = self.lock
        self.assertEqual(lock["schema"], 1)
        self.assertEqual(lock["duckdb"]["version"], "v2.0.0")
        self.assertEqual(lock["duckdb"]["source_id"], "d54cbbe3df")
        self.assertEqual(lock["platform"], "linux_amd64")
        self.assertEqual(lock["base"]["image"], "duckdb/duckdb:2.0.0")
        self.assertEqual(
            lock["base"]["digest"],
            "sha256:453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221",
        )
        self.assertEqual(lock["libexpat"]["version"], "2.5.0-1+deb12u5")
        self.assertEqual(
            lock["libexpat"]["sha256"],
            "b342d94f6a71df596f19b1b0d4d3af9b883f0333027de6c3fc0dbc9ff8c45057",
        )
        self.assertEqual(
            sorted(module["name"] for module in lock["extensions"]),
            sorted(image_tools.MODULES),
        )
        for module in lock["extensions"]:
            info = base64.b64decode(module["info_b64"], validate=True)
            self.assertEqual(hashlib.sha256(info).hexdigest(), module["sha256_info"])
            self.assertEqual(len(info), module["bytes_info"])
            self.assertRegex(module["sha256"], r"^[0-9a-f]{64}$")
        expected = "".join(
            f"{module['sha256']}  {module['name']}.duckdb_extension\n"
            for module in sorted(lock["extensions"], key=lambda item: item["name"])
        )
        self.assertEqual((HERE / "EXTENSIONS.sha256").read_text(), expected)
        fetch_artifacts.validate_lock(lock, "linux_amd64")


if __name__ == "__main__":
    unittest.main()
