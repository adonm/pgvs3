from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "test"))
import reference


SCRAPE = {
    "pgvs3_requests_total": {"code", "method"},
    "pgvs3_request_duration_seconds_bucket": {"le"},
    "pgvs3_request_duration_seconds_sum": set(),
    "pgvs3_request_duration_seconds_count": set(),
    "pgvs3_spool_bytes": set(),
}


class ReferenceTests(unittest.TestCase):
    def test_render_types_and_labels(self):
        table = reference.render(SCRAPE)
        self.assertIn("| `pgvs3_requests_total` | counter | `code`, `method` |", table)
        self.assertIn("| `pgvs3_request_duration_seconds_count` | histogram | — |", table)
        self.assertIn("| `pgvs3_spool_bytes` | gauge | — |", table)

    def test_check_fails_on_undocumented_metric_and_update_fixes_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = Path(tmp) / "reference.md"
            doc.write_text("# Ref\n\n## Metrics\n\n" + reference.BEGIN + "\n| `pgvs3_spool_bytes` | gauge | — |\n" + reference.END + "\ntail\n")
            with patch.object(reference, "REFERENCE", doc), patch.object(reference, "scrape", return_value=SCRAPE):
                with self.assertRaises(SystemExit) as failure:
                    reference.main(["http://unused"])
                self.assertIn("pgvs3_requests_total", str(failure.exception))
                with patch.dict("os.environ", {"PGVS3_UPDATE_REFERENCE": "1"}):
                    reference.main(["http://unused"])
                reference.main(["http://unused"])
            self.assertTrue(doc.read_text().endswith("tail\n"))
            self.assertEqual(reference.documented(doc.read_text()), set(SCRAPE))

    def test_repository_reference_is_marked_and_populated(self):
        names = reference.documented(reference.REFERENCE.read_text())
        self.assertGreater(len(names), 40)
        self.assertIn("pgvs3_s3_admission_rejected_total", names)


if __name__ == "__main__":
    unittest.main()
