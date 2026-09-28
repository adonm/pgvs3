"""Lock vendored official query inputs to the revision used by both engines."""

import hashlib
import re
import unittest
from pathlib import Path

QUERIES = Path(__file__).resolve().parent / "queries"


class OfficialQueryInputs(unittest.TestCase):
    def test_clickbench_matches_pinned_upstream_duckdb_queries(self):
        lines = [line.strip() for line in (QUERIES / "clickbench.sql").read_text().splitlines()
                 if line.strip() and not line.lstrip().startswith("--")]
        self.assertEqual(len(lines), 43)
        self.assertEqual(
            hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest(),
            "274ffe1c4f83baad2fc177bbb6773bbdab0db779faeb2970de8cc531292a5dc6",
        )

    def test_spatialbench_has_all_twelve_query_inputs(self):
        text = (QUERIES / "spatialbench.sql").read_text()
        self.assertEqual([int(n) for n in re.findall(r"^-- @q(\d+)$", text, re.M)],
                         list(range(1, 13)))
        sections = re.split(r"^-- @q\d+\s*$", text, flags=re.M)[1:]
        queries = [re.sub(r"\s+", " ", re.sub(r"--[^\n]*", "", section)).strip().rstrip(";")
                   for section in sections]
        self.assertEqual(
            hashlib.sha256(("\n".join(queries) + "\n").encode()).hexdigest(),
            "ee084553ceecf4e8597fbe106579d52ce5cdd342205ff98f1995fd317ee8ed03",
        )


if __name__ == "__main__":
    unittest.main()
