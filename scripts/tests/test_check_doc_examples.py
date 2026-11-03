import importlib.util
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "check-doc-examples.py"
spec = importlib.util.spec_from_file_location("check_doc_examples", SCRIPT)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


class MarkdownExtractionTests(unittest.TestCase):
    def test_extracts_only_marked_rust_blocks_with_unicode_line_numbers(self):
        markdown = "intro\n<!-- doc-example: id=one group=sample modes=default,config -->\n```rust,no_run\nfn 示例() {}\n```\n```python\nx = 1\n```\n"
        blocks = module.extract_blocks(markdown, "guide.md")
        self.assertEqual(len(blocks), 1)
        self.assertEqual((blocks[0].example_id, blocks[0].group, blocks[0].line), ("one", "sample", 3))
        self.assertIn("示例", blocks[0].source)

    def test_extracts_rust_fences_with_one_to_three_space_indentation(self):
        for indentation in (" ", "  ", "   "):
            with self.subTest(indentation=len(indentation)):
                markdown = f"<!-- doc-example: id=indented group=g modes=default -->\n{indentation}```rust,no_run\nfn example() {{}}\n{indentation}```\n"
                blocks = module.extract_blocks(markdown, "guide.md")
                self.assertEqual(len(blocks), 1)
                self.assertEqual(blocks[0].source, "fn example() {}\n")

    def test_longer_fence_keeps_triple_backticks_as_code(self):
        markdown = "<!-- doc-example: id=long group=g modes=default -->\n````rust,no_run\nlet text = \"```\";\n```\n````\n"
        blocks = module.extract_blocks(markdown, "guide.md")
        self.assertEqual(len(blocks), 1)
        self.assertIn("```\n", blocks[0].source)

    def test_closer_with_trailing_text_does_not_end_fence(self):
        markdown = "<!-- doc-example: id=invalid-close group=g modes=default -->\n```rust,no_run\nfn example() {}\n``` trailing text\n````\n"
        blocks = module.extract_blocks(markdown, "guide.md")
        self.assertEqual(len(blocks), 1)
        self.assertIn("``` trailing text\n", blocks[0].source)
        self.assertNotIn("````", blocks[0].source)

    def test_unterminated_non_rust_fence_fails_instead_of_hiding_rust(self):
        markdown = "```python\nvalue = 1\n```rust\nfn hidden_example() {}"
        with self.assertRaisesRegex(module.ExampleError, "unterminated fence"):
            module.extract_blocks(markdown, "guide.md")

    def test_rejects_unassigned_rust(self):
        with self.assertRaisesRegex(module.ExampleError, "unassigned"):
            module.extract_blocks("```rust\nfn main() {}\n```", "a.md")

    def test_rejects_duplicate_ids(self):
        text = "<!-- doc-example: id=x group=g modes=default -->\n```rust\na\n```\n<!-- doc-example: id=x group=g modes=default -->\n```rust\nb\n```"
        with self.assertRaisesRegex(module.ExampleError, "duplicate"):
            module.extract_blocks(text, "a.md")

    def test_rejects_missing_fence_and_unknown_rust_tags(self):
        with self.assertRaisesRegex(module.ExampleError, "unterminated"):
            module.extract_blocks("<!-- doc-example: id=x group=g modes=default -->\n```rust\nfn main() {}", "a.md")
        with self.assertRaisesRegex(module.ExampleError, "unknown"):
            module.extract_blocks("<!-- doc-example: id=x group=g modes=default -->\n```rust,compile\nfn main() {}\n```", "a.md")

    def test_rejects_incomplete_metadata(self):
        with self.assertRaises(module.ExampleError):
            module.extract_blocks("<!-- doc-example: id=x -->\n```rust\nx\n```", "a.md")

    def test_marker_cannot_attach_across_non_rust_fence(self):
        text = "<!-- doc-example: id=x group=g modes=default -->\n```python\nx = 1\n```\n```rust\nfn main() {}\n```"
        with self.assertRaisesRegex(module.ExampleError, "non-Rust"):
            module.extract_blocks(text, "a.md")

    def test_finds_config_checkout_in_normal_and_nested_worktree_layouts(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            normal = base / "rs-websocket"
            normal.mkdir()
            config = base / "rs-config"
            config.mkdir()
            (config / "Cargo.toml").write_text("[package]\n")
            self.assertEqual(module.find_config_path(normal), config.resolve())

            worktree = base / "project" / ".worktrees" / "rs-websocket" / "branch"
            worktree.mkdir(parents=True)
            nested_config = base / "project" / "rs-config"
            nested_config.mkdir()
            (nested_config / "Cargo.toml").write_text("[package]\n")
            self.assertEqual(module.find_config_path(worktree), nested_config.resolve())

    def test_config_checkout_error_lists_searched_paths(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(module.ExampleError, "searched:"):
                module.find_config_path(Path(td) / "rs-websocket")

    def test_config_mode_enables_client_feature_for_every_group(self):
        readme = module.extract_blocks("<!-- doc-example: id=r group=readme modes=default,config -->\n```rust,no_run\nfn readme_example() {}\n```", "README.md")
        private_ca = module.extract_blocks("<!-- doc-example: id=p group=guide_private_ca modes=default,config -->\n```rust,no_run\nfn private_ca_example() {}\n```", "doc/user_guide.md")
        config_example = module.extract_blocks("<!-- doc-example: id=c group=guide_config modes=config -->\n```rust,no_run\nfn config_example() {}\n```", "doc/user_guide.md")
        documents = {"README.md": readme, "README.zh_CN.md": [], "doc/user_guide.md": private_ca + config_example, "doc/user_guide.zh_CN.md": []}
        manifest = {"groups": {"readme": {}, "guide_private_ca": {}, "guide_config": {"config": True}}}

        def generated_manifests(mode):
            captured = {}

            def fake_cargo(command, **_kwargs):
                cargo_manifest = Path(command[command.index("--manifest-path") + 1]).read_text()
                package = next(line.split('"')[1] for line in cargo_manifest.splitlines() if line.startswith("name = "))
                captured[package] = cargo_manifest
                return SimpleNamespace(returncode=0, stdout="")

            with mock.patch.object(module.subprocess, "run", side_effect=fake_cargo):
                self.assertEqual(module.compile_mode(documents, manifest, mode, None), 0)
            return captured

        default_manifests = generated_manifests("default")
        self.assertEqual(len(default_manifests), 2)
        self.assertTrue(all('features = ["config"]' not in text for text in default_manifests.values()))

        config_manifests = generated_manifests("config")
        self.assertEqual(len(config_manifests), 3)
        self.assertTrue(all('features = ["config"]' in text for text in config_manifests.values()))
        for package, text in config_manifests.items():
            self.assertEqual("qubit-config =" in text, package == "doc-guide_config_en")

    def test_checks_bilingual_id_sets(self):
        en = [module.Example("x", "g", ("default",), "a.md", 1, "fn main() {}", "rust")]
        zh = [module.Example("y", "g", ("default",), "b.md", 1, "fn main() {}", "rust")]
        with self.assertRaisesRegex(module.ExampleError, "bilingual"):
            module.check_bilingual_sets({"README.md": en, "doc/user_guide.md": [], "README.zh_CN.md": zh, "doc/user_guide.zh_CN.md": []})


if __name__ == "__main__":
    unittest.main()
