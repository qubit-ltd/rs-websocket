import re
import unittest
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
RECORD = ROOT / "doc/review-followup-20261104T160902440Z-1b834208.md"


class ReviewFollowupLinksTests(unittest.TestCase):
    def test_relative_file_links_exist(self):
        for target in re.findall(r"\]\(([^)]+)\)", RECORD.read_text(encoding="utf-8")):
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            with self.subTest(target=target):
                self.assertTrue((RECORD.parent / unquote(parsed.path)).exists(), target)
