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
