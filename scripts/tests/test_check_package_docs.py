import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check-package-docs.py"
SPEC = importlib.util.spec_from_file_location("check_package_docs", SCRIPT)
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


class PackageDocsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for name in checker.REQUIRED_DOCS:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Package docs.\n", encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_complete_package_passes(self):
        self.assertEqual([], checker.check(self.root))

    def test_missing_user_guide_fails(self):
        (self.root / "doc/user_guide.md").unlink()
        self.assertTrue(any("missing required package document" in e for e in checker.check(self.root)))

    def test_evidence_is_rejected(self):
        evidence = self.root / "doc/evidence/run.txt"
        evidence.parent.mkdir()
        evidence.write_text("evidence", encoding="utf-8")
        self.assertTrue(any("excluded review material" in e for e in checker.check(self.root)))

    def test_missing_link_in_guide_fails(self):
        (self.root / "doc/user_guide.md").write_text("[missing](absent.md)\n", encoding="utf-8")
        self.assertTrue(any("missing linked file" in e for e in checker.check(self.root)))

    def test_anchor_link_checks_path_before_anchor(self):
        (self.root / "doc/user_guide.md").write_text("[target](../README.md#start)\n", encoding="utf-8")
        self.assertEqual([], checker.check(self.root))

    def test_url_is_skipped(self):
        (self.root / "doc/user_guide.md").write_text("[web](https://example.com/no-file)\n", encoding="utf-8")
        self.assertEqual([], checker.check(self.root))

    def test_path_escape_fails(self):
        outside = self.root.parent / "outside.md"
        outside.write_text("outside", encoding="utf-8")
        try:
            (self.root / "doc/user_guide.md").write_text("[escape](../../outside.md)\n", encoding="utf-8")
            self.assertTrue(any("link escapes package root" in e for e in checker.check(self.root)))
        finally:
            outside.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
