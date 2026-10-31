import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "test"))
import documentation


class DocumentationTests(unittest.TestCase):
    def test_maintained_markdown_links_and_fences(self):
        self.assertGreater(documentation.check_links()["local_links"], 100)

    def test_named_python_and_json_are_real_code(self):
        snippets = documentation.snippets()
        self.assertGreater(len(snippets), 20)
        for name, block in snippets.items():
            with self.subTest(name=name):
                if block["language"] == "python":
                    compile(block["source"], block["path"], "exec")
                elif block["language"] == "json":
                    json.loads(block["source"])

    def test_duplicate_or_unbound_verification_marker_fails(self):
        scratch = documentation.ROOT / ".tmp"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory, patch("documentation.GUIDES", Path(directory)):
            path = Path(directory) / "test.md"
            block = '<!-- verify:repeated -->\n```json\n{}\n```\n'
            for text in (block + block, '<!-- verify:missing -->\n```js\n{}\n```\n'):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    documentation.snippets()

    def test_ojs_curl_examples_keep_array_arguments(self):
        checked = 0
        for name in ("README.md", "docs/operations.md"):
            text = (documentation.ROOT / name).read_text()
            for body in re.findall(r"-d '([^']+)'", text):
                value = json.loads(body)
                if isinstance(value, dict) and "args" in value:
                    self.assertIsInstance(value["args"], list, name)
                    checked += 1
        self.assertGreaterEqual(checked, 2)


if __name__ == "__main__":
    unittest.main()
