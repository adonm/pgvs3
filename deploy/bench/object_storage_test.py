import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from object_storage import CONFIGS, provision, sealed_summary, tuning_summary, validate


class ObjectStorageTests(unittest.TestCase):
    def test_cached_pin_mismatch_cannot_load_or_overwrite_user_binary(self):
        with tempfile.TemporaryDirectory() as directory, patch("object_storage.ROOT", Path(directory)), patch("object_storage.extension_platform", return_value="linux_amd64"):
            target = Path(directory) / ".tmp/deps/duckdb-object-storage/0.2.0/v2.0.0/linux_amd64/duckdb_object_storage.duckdb_extension"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"unrelated build")
            with self.assertRaises(RuntimeError):
                provision()
            self.assertEqual(target.read_bytes(), b"unrelated build")

    def test_destructive_namespace_and_directory_are_owned(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, PGVS3_TEST_RUN_DIR=directory):
            root = Path(directory) / "object-storage"
            root.mkdir()
            (root / "ownership.json").write_text(json.dumps({"run": "r0123456789ab"}))
            bucket = "os-r0123456789ab-m10-p0-objfs-default"
            job = {"owner": "r0123456789ab", "profile": "objfs-default", "folder": str(root / "worker"),
                   "bucket": bucket, "location": f"s3://{bucket}/state/"}
            validate(job)
            for changed in ({"folder": directory}, {"location": "s3://unowned/"}, {"owner": "another"}, {"bucket": bucket + "/../other"}):
                with self.subTest(changed=changed), self.assertRaises(RuntimeError):
                    validate(job | changed)

    def test_sealing_savings_do_not_hide_closing_cost_or_claim_live_semantics(self):
        def reader(seconds):
            return {"attach_schema_first_query_seconds": seconds}
        cohort = {"profile": "objfs-default", "target_mib": 100,
                  "remote_reads": {"loaded": {"scan": reader(0.1)}},
                  "sealed_reads": {"writer_close_seconds": 0.12, "readers": {"scan": reader(0.04)}}}
        summary = sealed_summary([cohort])[0]
        self.assertAlmostEqual(summary["close_amortization_readers"], 2)
        self.assertAlmostEqual(summary["saving_per_fresh_reader_seconds"], 0.06)
        self.assertIn("not a live-writer speedup", summary["contract"])
        cohort["sealed_reads"]["readers"]["scan"] = reader(0.2)
        self.assertIsNone(sealed_summary([cohort])[0]["close_amortization_readers"])

    def test_lean_budget_and_per_path_comparison_are_not_aggregate_wins(self):
        default, lean = CONFIGS["objfs-default"], CONFIGS["objfs-lean"]
        self.assertEqual(lean["data_cache"] + lean["metadata_cache"], (default["data_cache"] + default["metadata_cache"]) // 2)
        cases = [{"target_mib": 10, "operation": operation,
                  "median_seconds": {"objfs-default": 1, "objfs-lean": duration}}
                 for operation, duration in (("scan", 0.5), ("append", 1.21))]
        result = tuning_summary(cases)
        self.assertEqual([case["within_20pct_latency_budget"] for case in result], [True, False])


if __name__ == "__main__":
    unittest.main()
