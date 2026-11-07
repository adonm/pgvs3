import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
for key, value in {
    "PGVS3_TEST_RUN_DIR": str(ROOT / ".tmp/data-plane-test"),
    "PGVS3_TEST_BIN": str(ROOT / ".tmp/data-plane-test/pgvs3"),
    "PGVS3_TEST_DB_URL": "postgres://example.invalid/pgvs3",
    "PGVS3_TEST_ENDPOINT_A": "http://example.invalid/a",
    "PGVS3_TEST_ENDPOINT_B": "http://example.invalid/b",
    "PGVS3_TEST_ORIGIN_ENDPOINT": "http://example.invalid/origin",
    "PGVS3_TEST_PSQL": str(ROOT / "test/psql.py"),
}.items():
    os.environ.setdefault(key, value)
sys.path.insert(0, str(ROOT / "test"))
import data_plane


class ComputeHelperTests(unittest.TestCase):
    def test_compute_names_are_stable_unique_and_safe(self):
        first = data_plane.compute_names("compute.AbC123")
        self.assertEqual(first, data_plane.compute_names("compute.AbC123"))
        self.assertEqual(first["schema"], "compute_computeabc123")
        self.assertEqual(first["bucket"], "compute-computeabc123")
        self.assertEqual(first["prefix"], "compute/computeabc123/lake")
        for key in ("schema", "bucket"):
            self.assertLessEqual(len(first[key]), 63)
        self.assertRegex(first["schema"], r"^[a-z_][a-z0-9_]*$")
        self.assertRegex(first["bucket"], r"^[a-z0-9][a-z0-9-]*$")

    def test_parse_s3_keys_handles_namespace_and_suffix(self):
        body = b"""<?xml version="1.0"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Contents><Key>prefix/main/a.parquet</Key></Contents>
  <Contents><Key>prefix/main/b.txt</Key></Contents>
</ListBucketResult>"""
        keys = data_plane.parse_s3_keys(body)
        self.assertEqual(keys, ["prefix/main/a.parquet", "prefix/main/b.txt"])
        self.assertEqual([key for key in keys if key.endswith(".parquet")],
                         ["prefix/main/a.parquet"])
        sized = body.replace(
            b"<Key>prefix/main/a.parquet</Key>",
            b"<Key>prefix/main/a.parquet</Key><Size>1234</Size>")
        self.assertEqual(data_plane.parse_s3_objects(sized),
                         [("prefix/main/a.parquet", 1234), ("prefix/main/b.txt", 0)])

    def test_host_client_config_is_offline_signed_and_pinned(self):
        config = data_plane.duckdb_client_config(Path("/tmp/run/extensions"))
        self.assertEqual(config["extension_directory"], "/tmp/run/extensions")
        self.assertEqual(config["autoinstall_known_extensions"], "false")
        self.assertEqual(config["autoload_known_extensions"], "false")
        self.assertEqual(config["allow_unsigned_extensions"], "false")

    def test_sql_str_and_timing_summary_preserve_raw_samples(self):
        self.assertEqual(data_plane.sql_str("a'b"), "'a''b'")
        timings = data_plane.timing_summary([9, 4, 6, 5])
        self.assertEqual(timings["ms"], [9.0, 4.0, 6.0, 5.0])
        self.assertEqual(timings["first_ms"], 9.0)
        self.assertEqual(timings["repeat_ms"], [4.0, 6.0, 5.0])
        self.assertEqual(timings["median_ms"], 5.5)
        self.assertEqual(timings["min_ms"], 4.0)
        self.assertEqual(timings["max_ms"], 9.0)

    def test_empty_timing_summary_is_explicit(self):
        self.assertEqual(data_plane.timing_summary([]), {
            "ms": [], "first_ms": None, "repeat_ms": [],
            "median_ms": None, "min_ms": None, "max_ms": None,
        })


class QuackPoolHelperTests(unittest.TestCase):
    def test_application_name_preserves_existing_query(self):
        self.assertEqual(
            data_plane.with_application_name(
                "postgres://u:p@h/db?sslmode=disable", "pgvs3_pool"),
            "postgres://u:p@h/db?sslmode=disable&application_name=pgvs3_pool")
        self.assertEqual(
            data_plane.with_application_name("postgres://u:p@h/db", "ducklake_pool"),
            "postgres://u:p@h/db?application_name=ducklake_pool")

    def test_pool_percentile_is_linear_and_bounded(self):
        values = [10, 20, 30, 40]
        self.assertEqual(data_plane.pool_percentile(values, 0), 10)
        self.assertEqual(data_plane.pool_percentile(values, 0.5), 25)
        self.assertEqual(data_plane.pool_percentile(values, 0.99), 39.7)
        self.assertEqual(data_plane.pool_percentile(values, 1), 40)

    def test_pool_digest_and_metric_delta_keep_only_counters(self):
        rows = [(1, 2, 3.5), (4, 5, 6.5)]
        self.assertEqual(data_plane.pool_digest(rows, (1, 2)), [2, 7.0, 10.0])
        before = {"pgvs3_read_cache_hits_total": 1,
                  "pgvs3_read_cache_bytes": 999}
        after = {"pgvs3_read_cache_hits_total": 4,
                 "pgvs3_read_cache_bytes": 10}
        delta = data_plane.pool_metric_delta(before, after)
        self.assertEqual(delta["pgvs3_read_cache_hits_total"], 3)
        self.assertNotIn("pgvs3_read_cache_bytes", delta)


class TrinoReportTests(unittest.TestCase):
    def test_first_run_and_total_origin_methods_are_recorded(self):
        runs = [{"GET": 4, "HEAD": 1}, {"GET": 0, "HEAD": 2}, {"GET": 0, "HEAD": 2}]
        observed = data_plane.trino_observation([100, 80, 70], runs)
        self.assertEqual(observed["ms"], [100, 80, 70])
        self.assertEqual(observed["first_ms"], 100)
        self.assertEqual(observed["repeat_ms"], [80, 70])
        self.assertEqual(observed["origin_reads"], 5)
        self.assertEqual(observed["origin_reads_total"], 9)
        self.assertEqual(observed["origin_methods"], {
            "first": {"GET": 4, "HEAD": 1},
            "total": {"GET": 4, "HEAD": 5},
            "runs": runs,
        })

    def test_single_run_has_no_repeats(self):
        observed = data_plane.trino_observation([42], [{"GET": 2, "HEAD": 0}])
        self.assertEqual(observed["first_ms"], 42)
        self.assertEqual(observed["repeat_ms"], [])
        self.assertEqual(observed["origin_reads"], 2)
        self.assertEqual(observed["origin_reads_total"], 2)

    def test_head_only_repeats_remain_visible_per_run(self):
        runs = [{"GET": 0, "HEAD": 2} for _ in range(5)]
        observed = data_plane.trino_observation([50, 40, 41, 39, 42], runs)
        self.assertEqual(observed["origin_methods"]["first"], {"GET": 0, "HEAD": 2})
        self.assertEqual(observed["origin_methods"]["total"], {"GET": 0, "HEAD": 10})
        self.assertEqual(observed["origin_methods"]["runs"], runs)
        self.assertEqual(observed["origin_reads_total"], 10)

    def test_paired_blocks_alternate_abba_and_track_methods(self):
        class Proxy:
            def __init__(self):
                self.heads = 0

            def methods_under(self, prefix):
                return {"GET": 0, "HEAD": self.heads}

        proxy, calls, checkpoints = Proxy(), [], []
        expected = {"q": [1]}

        def execute(provider, query, kind, template):
            calls.append(provider)
            if provider == "alluxio":
                proxy.heads += 2
            return [1], 10

        paired = data_plane.trino_paired(execute, proxy, expected, 2, "/p/", checkpoints.append,
                                         queries=[("q", "lineitem", "SELECT {t}")])
        self.assertEqual(data_plane.TRINO_PAIRED_CATALOGS, {"pg": "via_a", "alluxio": "alluxio"})
        self.assertEqual(paired["providers"]["pg"]["catalog"], "via_a")
        self.assertEqual(paired["providers"]["alluxio"]["catalog"], "alluxio")
        self.assertEqual(calls, ["pg", "alluxio", "alluxio", "pg", "alluxio", "pg", "pg", "alluxio"])
        self.assertEqual([block["order"] for block in paired["queries"]["q"]["blocks"]],
                         [["pg", "alluxio", "alluxio", "pg"], ["alluxio", "pg", "pg", "alluxio"]])
        self.assertEqual(paired["queries"]["q"]["samples"][1]["origin_methods"], {"GET": 0, "HEAD": 2})
        self.assertTrue(paired["complete"])
        self.assertEqual(len(checkpoints), 3)

    def test_paired_rejects_bad_oracle_and_alluxio_get(self):
        class Proxy:
            def __init__(self):
                self.gets = 0

            def methods_under(self, prefix):
                return {"GET": self.gets, "HEAD": 0}

        expected = {"q": [1]}
        query = [("q", "lineitem", "SELECT {t}")]

        def bad_rows(provider, query, kind, template):
            return [0], 1

        checkpoints = []
        with self.assertRaises(AssertionError):
            data_plane.trino_paired(bad_rows, Proxy(), expected, 1, "/p/", checkpoints.append, queries=query)
        self.assertFalse(checkpoints[-1]["complete"])

        proxy = Proxy()

        def alluxio_get(provider, query, kind, template):
            if provider == "alluxio":
                proxy.gets += 1
            return [1], 1

        with self.assertRaises(AssertionError):
            data_plane.trino_paired(alluxio_get, proxy, expected, 1, "/p/", lambda _: None, queries=query)

    def test_effective_config_records_shared_pg_and_local_cache_budget(self):
        with patch.dict(os.environ, {"PGVS3_READ_CACHE_BYTES": "0", "PGVS3_POOL_MAX": "8",
                                     "PGVS3_FILL_CONCURRENCY": "16"}, clear=False):
            config = data_plane.trino_config(0.02, 3, "sf10", 1 << 20, "32")
            inherited = data_plane.trino_config(0.02, 3, "sf10", 1 << 20, None)
        self.assertEqual(config["gateway_read_cache_bytes"], 0)
        self.assertEqual(config["gateway_pool_max"], 8)
        self.assertEqual(config["gateway_fill_concurrency"], 16)
        self.assertEqual(config["origin_set_fill_concurrency"], 32)
        self.assertEqual(inherited["origin_set_fill_concurrency"], 16)
        self.assertEqual(config["repeat"], 3)
        self.assertEqual(config["range_chunk"], 1 << 20)
        self.assertEqual(config["origin_latency_ms"], 20)
        self.assertEqual(config["big_scale"], "sf10")


if __name__ == "__main__":
    unittest.main()
