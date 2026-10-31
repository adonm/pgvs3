import io
import json
import math
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from differential import PROFILES, delta, digest, epoch_limit_failure, extract, gateway_metrics, redact, successful_requests, summarize, validate_job


class DifferentialTests(unittest.TestCase):
    def test_pinned_extraction_verifies_bytes_and_never_overwrites_different_work(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory) / "cache"
            folder.mkdir()
            archive, target = folder / "upstream.zip", folder / "differential.duckdb_extension"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("differential.duckdb_extension", b"release")
                output.writestr("../unowned", b"must not extract")
            with self.assertRaises(RuntimeError):
                extract(archive, target, "0" * 64)
            self.assertFalse(target.exists())
            extract(archive, target, digest(archive))
            extract(archive, target, digest(archive))
            self.assertEqual(target.read_bytes(), b"release")
            self.assertFalse((folder.parent / "unowned").exists())
            target.write_bytes(b"user build")
            with self.assertRaises(RuntimeError):
                extract(archive, target, digest(archive))
            self.assertEqual(target.read_bytes(), b"user build")

    def test_comparisons_use_remote_ack_not_weaker_local_return_and_require_every_profile(self):
        cohorts = [{"complete": True, "profile": profile, "target_mib": 10, "remote_reads": {},
                    "events": [{"operation": "ddl", "statement_seconds": 0.001, "remote_ack_seconds": seconds}]}
                   for profile, seconds in zip(PROFILES, (0.01, 0.1, 0.005))]
        case = summarize(cohorts)[0]
        self.assertEqual(case["relative_to_ducklake"], {"snapshot": 10, "sync": 0.5})
        self.assertEqual(case["within_20pct_latency_budget"], {"snapshot": False, "sync": True})
        with self.assertRaises(RuntimeError):
            summarize(cohorts[:-1])
        cohorts[-1]["complete"] = False
        with self.assertRaises(RuntimeError):
            summarize(cohorts)
        cohorts[-1]["complete"] = True
        for invalid in (0, -1, math.nan, math.inf):
            cohorts[-1]["events"][0]["remote_ack_seconds"] = invalid
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                summarize(cohorts)

    def test_destructive_worker_actions_require_owned_paths_and_exact_provider_prefix(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, PGVS3_TEST_RUN_DIR=directory):
            owned = Path(directory) / "differential"
            owned.mkdir()
            owner = "r0123456789ab"
            (owned / "ownership.json").write_text(json.dumps({"run": owner}))
            bucket = "dd-" + owner + "-m10-p0-sync"
            job = {"owner": owner, "profile": "sync", "bucket": bucket, "location": f"s3://{bucket}/state/", "folder": str(owned / "cohort")}
            validate_job(job)
            for changes in ({"owner": "another-run"}, {"folder": directory}, {"bucket": bucket + "/../other"},
                            {"location": "s3://unowned/"}, {"repository": str(Path(directory) / "unowned")},
                            {"client_source": directory}, {"profile": "ducklake", "metadata_schema": "unowned"}):
                with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                    validate_job(job | changes)

    def test_real_gateway_counters_resets_and_credential_redaction(self):
        class Response(io.BytesIO):
            pass
        body = b'pgvs3_requests_total{method="GET",code="206"} 3\npgvs3_response_bytes_total 1024\n'
        with patch("differential.urlopen", return_value=Response(body)):
            measured = gateway_metrics("http://example.invalid")
        self.assertEqual(measured['pgvs3_requests_total{method="GET",code="206"}'], 3)
        self.assertEqual(delta({}, measured)["pgvs3_response_bytes_total"], 1024)
        successful_requests(measured)
        with self.assertRaises(RuntimeError):
            successful_requests({'pgvs3_requests_total{method="GET",code="503"}': 1})
        with self.assertRaises(RuntimeError):
            delta(measured, {})
        with patch("differential.urlopen", return_value=Response(b"unrelated_metric 1\n")), self.assertRaises(RuntimeError):
            gateway_metrics("http://example.invalid")
        with patch.dict(os.environ, PGVS3_SECRET_KEY="temporary-secret", PGVS3_CATALOG="host=local password=private"):
            self.assertEqual(redact("temporary-secret host=local password=private postgres://user:password@local/db"),
                             "<redacted> <redacted> postgres://<redacted>@local/db")

    def test_known_epoch_limit_remains_failed_ack_and_missing_samples_are_not_zero(self):
        failure = {"profile": "sync", "target_mib": 1024,
                   "error": {"message": "commit durable locally: [differential:busy] A shipping epoch exceeds 256 MiB"}}
        self.assertTrue(epoch_limit_failure("sync", failure))
        self.assertFalse(epoch_limit_failure("snapshot", failure))
        self.assertFalse(epoch_limit_failure("sync", {"error": {"message": "unrelated failure"}}))
        cohorts = [{"complete": True, "profile": profile, "target_mib": 1024, "remote_reads": {},
                    "events": [{"operation": "bulk_load", "remote_ack_seconds": 1}]}
                   for profile in PROFILES[:2]]
        case = summarize(cohorts, [failure])[0]
        self.assertIsNone(case["median_seconds"]["sync"])
        self.assertIsNone(case["relative_to_ducklake"]["sync"])
        self.assertEqual(case["samples"]["sync"], 0)
        self.assertFalse(case["within_20pct_latency_budget"]["sync"])
        self.assertEqual(case["unavailable_profiles"], ["sync"])


if __name__ == "__main__":
    unittest.main()
