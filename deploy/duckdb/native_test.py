#!/usr/bin/env python3
"""Focused stdlib tests for the Hardcharger (ChargeWire) DuckDB image.

These tests do not start DuckDB, PostgreSQL, S3 or Docker. They pin the pure
SQL template shape, the host-side renderer, the Hardcharger base/entrypoint
contract and the artifact lock.
"""

from __future__ import annotations

import base64
import hashlib
import json
import unittest
from pathlib import Path

import fetch_artifacts
import image_tools
import render_init

HERE = Path(__file__).resolve().parent
SECRET_URL = "postgresql://user:p%40ss@db.example:5433/pgvs3?sslmode=verify-full"
SECRET_PASSWORD = "sup3r-secret-value"
DOT_COMMANDS = (".read", ".output", ".print", ".quit", ".mode", ".open", ".maxrows")
ENVIRONMENT = {
    "PGVS3_URL": SECRET_URL,
    "PGVS3_BUCKET": "lake-bucket",
    "PGVS3_S3_SECRET_KEY": SECRET_PASSWORD,
    "PGVS3_METADATA_SCHEMA": "ducklake_pgvs3",
    "DUCKDB_THREADS": "4",
    "DUCKDB_MEMORY_LIMIT": "1GB",
}


class TemplateShapeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.extensions = (HERE / "extensions.sql").read_text()
        self.lake = (HERE / "lake.sql").read_text()
        self.init = (HERE / "init.sql").read_text()
        self.lake_only = (HERE / "lake-only.sql").read_text()
        self.rendered = render_init.render_text(ENVIRONMENT)

    def test_init_and_one_shot_are_generated_from_the_two_templates(self) -> None:
        for name, text in (("init.sql", self.init), ("lake-only.sql", self.lake_only)):
            self.assertTrue(text.startswith("--"), name)
            for dot in DOT_COMMANDS:
                self.assertNotIn(f"\n{dot}", text, f"{name} contains a CLI dot-command {dot}")
        self.assertEqual(self.init.split("\n\n", 1)[1], self.extensions + "\n" + self.lake)
        self.assertEqual(self.lake_only.split("\n\n", 1)[1],
                         self.extensions + "\n" + self.lake + "\nSELECT 'PGVS3_NO_DAEMON_OK';\n")

    def test_templates_only_use_sql_hardcharger_supports(self) -> None:
        statements = "\n".join(
            line for line in (self.extensions + self.lake).splitlines()
            if not line.lstrip().startswith("--")
        )
        self.assertNotIn("getenv", statements)
        self.assertNotIn("INSTALL", statements)
        self.assertNotIn("LOAD quack;", statements)
        self.assertIn("SET autoinstall_known_extensions=false;", self.extensions)
        self.assertIn("SET autoload_known_extensions=false;", self.extensions)
        self.assertIn("SET allow_unsigned_extensions=false;", self.extensions)
        for module in image_tools.MODULES:
            self.assertIn(f"LOAD {module};", self.extensions)
        # The baked extensions live in /opt/pgvs3-duckdb/extensions inside the
        # image, where a read-only root filesystem can still find them.
        self.assertIn("SET extension_directories=['/opt/pgvs3-duckdb/extensions'];",
                      self.extensions)

    def test_renderer_substitutes_every_sentinel(self) -> None:
        for sentinel in ("__PGVS3_URL__", "__PGVS3_BUCKET__", "__PGVS3_PREFIX__",
                         "__PGVS3_ENDPOINT__", "__PGVS3_S3_ACCESS_KEY__",
                         "__PGVS3_S3_SECRET_KEY__", "__PGVS3_S3_REGION__",
                         "__PGVS3_S3_USE_SSL__", "__PGVS3_METADATA_SCHEMA__",
                         "__DUCKDB_THREADS__", "__DUCKDB_MEMORY_LIMIT__",
                         "__DUCKDB_TEMP_DIRECTORY__"):
            self.assertNotIn(sentinel, self.rendered, sentinel)
        self.assertIn(f"URI {SECRET_URL!r}", self.rendered)
        self.assertIn(f"SECRET {SECRET_PASSWORD!r}", self.rendered)
        self.assertIn("SET threads=4;", self.rendered)
        self.assertIn("SET memory_limit='1GB';", self.rendered)
        self.assertIn("ATTACH 'ducklake:pgvs3_lake' AS lake (AUTOMATIC_MIGRATION false);",
                      self.rendered)
        self.assertIn("SELECT 'PGVS3_LAKE_OK';", self.rendered)
        self.assertNotIn("PGVS3_NO_DAEMON_OK", self.rendered)
        one_shot = render_init.render_text(ENVIRONMENT, one_shot=True)
        self.assertIn("SELECT 'PGVS3_NO_DAEMON_OK';", one_shot)
        self.assertEqual(one_shot.count("ATTACH 'ducklake:pgvs3_lake'"), 1)

    def test_renderer_defaults_and_endpoint_scheme(self) -> None:
        rendered = render_init.render_text({"PGVS3_URL": SECRET_URL, "PGVS3_BUCKET": "b"})
        self.assertIn("SET threads=4;", rendered)
        self.assertIn("SET memory_limit='2GB';", rendered)
        self.assertIn("ENDPOINT regexp_replace('127.0.0.1:8014'", rendered)
        secure = render_init.render_text({
            "PGVS3_URL": SECRET_URL, "PGVS3_BUCKET": "b",
            "PGVS3_ENDPOINT": "https://s3.example:9000",
        })
        self.assertIn("'https://s3.example:9000' LIKE 'https://%'", secure)

    def test_renderer_refuses_bad_values_without_echoing_them(self) -> None:
        for name, value, expected in (
            ("PGVS3_URL", "", "PGVS3_URL"),
            ("PGVS3_BUCKET", "", "PGVS3_BUCKET"),
            ("PGVS3_BUCKET", "Bad Bucket", "PGVS3_BUCKET"),
            ("DUCKDB_THREADS", "four", "DUCKDB_THREADS"),
            ("DUCKDB_MEMORY_LIMIT", "lots", "DUCKDB_MEMORY_LIMIT"),
            ("DUCKDB_TEMP_DIRECTORY", "relative/dir", "DUCKDB_TEMP_DIRECTORY"),
            ("PGVS3_S3_USE_SSL", "yes", "PGVS3_S3_USE_SSL"),
            ("PGVS3_S3_SECRET_KEY", "a'b", "PGVS3_S3_SECRET_KEY"),
        ):
            environment = dict(ENVIRONMENT, **{name: value})
            with self.assertRaises(render_init.RenderError) as caught:
                render_init.render_text(environment)
            self.assertIn(expected, str(caught.exception))
            self.assertNotIn(SECRET_PASSWORD, str(caught.exception))
            self.assertNotIn(SECRET_URL, str(caught.exception))

    def test_unrendered_template_fails_loudly_instead_of_attaching(self) -> None:
        unrendered = render_init.template(one_shot=False)
        self.assertIn("position('__PGVS3_' IN '__PGVS3_URL__') = 1", unrendered)
        self.assertIn("error('PGVS3_URL is required", unrendered)
        self.assertIn("error('PGVS3_BUCKET is required", unrendered)
        # No guard echoes a value or the variable's content.
        self.assertNotIn("error(getenv", unrendered)
        self.assertNotIn("error('__PGVS3_", unrendered)


class ImageContractTest(unittest.TestCase):
    DIGEST = ("ghcr.io/gizmodata/hardcharger:2.10.0@sha256:"
              "35e7e1616b575ecf04f07b3308ca7435029fb26b24f0ec9ea38248375a27ed36")

    def setUp(self) -> None:
        self.dockerfile = (HERE / "Dockerfile").read_text()

    def test_hardcharger_final_stage_and_entrypoint(self) -> None:
        self.assertIn(f"FROM --platform=linux/amd64 {self.DIGEST}", self.dockerfile)
        self.assertIn('ENTRYPOINT ["hardcharger-server"]', self.dockerfile)
        self.assertIn("USER hardcharger", self.dockerfile)
        self.assertIn("EXPOSE 31337 31338", self.dockerfile)
        self.assertIn("STOPSIGNAL SIGTERM", self.dockerfile)
        self.assertIn("HARDCHARGER_SHUTDOWN_GRACE_PERIOD_SECONDS=15", self.dockerfile)
        self.assertIn("HARDCHARGER_DEFAULT_CATALOG=lake", self.dockerfile)
        self.assertIn("HARDCHARGER_INIT_SQL_COMMANDS_FILE=/opt/pgvs3-duckdb/init.sql",
                      self.dockerfile)
        self.assertIn("HARDCHARGER_ALLOW_UNSIGNED_EXTENSIONS=false", self.dockerfile)
        self.assertIn("HARDCHARGER_MEMORY_LIMIT=2GB", self.dockerfile)
        self.assertNotIn("HARDCHARGER_ADMIN_PASSWORD", self.dockerfile)
        self.assertNotIn("QUACK_", self.dockerfile)

    def test_no_python_shell_or_supervisor_in_final_image(self) -> None:
        active = "\n".join(
            line for line in self.dockerfile.splitlines()
            if not line.lstrip().startswith("#")
        )
        for text in ("python3", "bootstrap.py", "smoke.py", "/bin/sh", 'ENTRYPOINT ["sh"'):
            self.assertNotIn(text, active)
        final = active.split(self.DIGEST, -1)[-1]
        self.assertNotIn("apt-get", final)
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
        self.assertIn("COPY --from=verify /verify/extensions/ ${EXTENSION_ROOT}/",
                      self.dockerfile)
        self.assertIn("ARG EXTENSION_ROOT=/var/lib/hardcharger/.duckdb/extensions/v2.0.0/",
                      self.dockerfile)
        self.assertIn("COPY --from=official /duckdb /duckdb", self.dockerfile)
        self.assertIn("chown -R 999:999 /var/lib/hardcharger/.duckdb", self.dockerfile)


class ManifestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = json.loads((HERE / "extensions.lock").read_text())

    def test_lock_matches_sums_info_and_hardcharger_base(self) -> None:
        lock = self.lock
        self.assertEqual(lock["schema"], 1)
        self.assertEqual(lock["duckdb"]["version"], "v2.0.0")
        self.assertEqual(lock["duckdb"]["source_id"], "d54cbbe3df")
        self.assertEqual(lock["platform"], "linux_amd64")
        self.assertEqual(lock["server"]["image"], "ghcr.io/gizmodata/hardcharger:2.10.0")
        self.assertEqual(
            lock["server"]["digest"],
            "sha256:35e7e1616b575ecf04f07b3308ca7435029fb26b24f0ec9ea38248375a27ed36",
        )
        self.assertEqual(lock["server"]["entrypoint"], ["hardcharger-server"])
        self.assertEqual(lock["server"]["duckdb"], lock["duckdb"]["version"])
        self.assertEqual(lock["server"]["port"], 31337)
        self.assertEqual(lock["server"]["health_port"], 31338)
        self.assertEqual(
            lock["base"]["digest"],
            "sha256:453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221",
        )
        self.assertEqual(lock["libexpat"]["version"], "2.5.0-1+deb12u5")
        self.assertEqual(
            lock["libexpat"]["sha256"],
            "b342d94f6a71df596f19b1b0d4d3af9b883f0333027de6c3fc0dbc9ff8c45057",
        )
        self.assertEqual(sorted(module["name"] for module in lock["extensions"]),
                         sorted(image_tools.MODULES))
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
