import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from compare import Fixtures, cache_path, summarize, validate_record


class Response(io.BytesIO):
    def __init__(self, body, length):
        super().__init__(body)
        self.status = 200
        self.headers = {"Content-Length": str(length)}


class CompareTests(unittest.TestCase):
    def record(self, **changes):
        result = dict(size=4096, op="get", workers=1, count=1, requests=10, errors=0,
                      seconds=1, requests_per_s=10, p50_ms=100, mib_per_s=40960 / (1 << 20))
        return result | changes

    def test_loadgen_counts_and_transferred_bytes_are_checked(self):
        validate_record(self.record(), 4096, "get", 1, 1, 10, 8192)
        for changes in ({"size": 8192}, {"requests": 9}, {"errors": 1}, {"mib_per_s": 0}, {"p50_ms": float("nan")}):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                validate_record(self.record(**changes), 4096, "get", 1, 1, 10, 8192)

    def test_latency_and_rate_gates_are_per_case(self):
        for changes in ({}, {"p50_ms": 121}, {"requests_per_s": 7.9}):
            rows = [self.record(label=side, **(changes if side == "after" else {}))
                    for _ in range(3) for side in ("before", "after")]
            cases, failed = summarize(rows)
            self.assertEqual(len(cases), 1)
            self.assertEqual(bool(failed), bool(changes))

    def test_wrong_size_or_bytes_cannot_validate_a_fixture(self):
        args = SimpleNamespace(bucket="bench", range_size=8192, origin_directories=None, seed_endpoint=None)
        fixture = Fixtures(args, {"before": "http://example.invalid"}, {"before": None}, None)
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(directory) / "payload"
            payload.write_bytes(b"body")
            for body, length in ((b"body", 5), (b"oops", 4)):
                with self.subTest(body=body, length=length), patch("urllib.request.urlopen", return_value=Response(body, length)), self.assertRaises(RuntimeError):
                    fixture.verify("before", 4, payload, "prefix", "get", False)

    def test_persisted_origin_settings_must_match_the_fixture(self):
        args = SimpleNamespace(bucket="bench", origin_directories=["/before", "/after"], range_chunk=65536, origin_namespace="benchmark-origin")
        for changes in ({"range_chunk": 0}, {"namespace": "other"}, {"ttl": "0s"}):
            fixture = Fixtures(args, {"before": "http://example.invalid", "after": "http://example.invalid"}, {}, None)
            spec = dict(namespace="benchmark-origin", range_chunk=65536, ttl="1h0m0s",
                        config=json.dumps({"type": "filesystem", "config": {"directory": "/before"}})) | changes
            fixture.sql = lambda side, statement: json.dumps(spec)
            with self.subTest(changes=changes), patch("urllib.request.urlopen", return_value=Response(b"", 0)), self.assertRaises(RuntimeError):
                fixture.configure()

    def test_cache_paths_and_completed_fills_are_explicit(self):
        args = SimpleNamespace(bucket="bench", origin_directories=["/before", "/after"], range_chunk=65536,
                               origin_namespace="benchmark-origin", range_size=8192, cache_state="cold", seed_endpoint=None)
        fixture = Fixtures(args, {"before": "http://example.invalid", "after": "http://example.invalid"}, {}, None)
        self.assertEqual(cache_path(args, "range", 10 << 20), "aligned-range-cache")
        fixture.sql = lambda side, statement: "0"
        with self.assertRaises(RuntimeError):
            fixture.confirm_cold_fill("before", 4096, "prefix", 4, "get")
        args.range_chunk = 0
        self.assertEqual(cache_path(args, "range", 10 << 20), "pass-through-range")
        fixture.sql = lambda side, statement: "1"
        with self.assertRaises(RuntimeError):
            fixture.confirm_cold_fill("before", 10 << 20, "prefix", 4, "range")


if __name__ == "__main__":
    unittest.main()
