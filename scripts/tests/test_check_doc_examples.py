import importlib.util
import contextlib
import io
import json
import re
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
    @staticmethod
    def write_config(directory, name="qubit-config", version="1.2.3"):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "Cargo.toml").write_text(
            f'[package]\nname = "{name}"\nversion = "{version}"\n',
            encoding="utf-8",
        )
        return directory

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

    def test_finds_config_checkout_inside_ci_workspace(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "rs-websocket"
            root.mkdir()
            config = root / "rs-config"
            config.mkdir()
            (config / "Cargo.toml").write_text("[package]\n")
            self.assertEqual(module.find_config_path(root), config.resolve())

    def test_config_checkout_error_lists_searched_paths(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(module.ExampleError, "searched:"):
                module.find_config_path(Path(td) / "rs-websocket")

    def test_explicit_config_path_overrides_discovery_and_resolves_relative_to_cwd(self):
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td)
            discovered = self.write_config(cwd / "rs-websocket" / "rs-config")
            explicit = self.write_config(cwd / "chosen-config")
            with mock.patch.object(module, "find_config_path", return_value=discovered.resolve()), \
                 mock.patch("os.getcwd", return_value=str(cwd)):
                self.assertEqual(module.resolve_config_path(cwd / "rs-websocket", Path("chosen-config")), explicit.resolve())

    def test_invalid_explicit_config_path_does_not_fall_back_to_discovery(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "rs-websocket"
            discovered = self.write_config(root / "rs-config")
            with mock.patch.object(module, "find_config_path", return_value=discovered.resolve()) as find:
                with self.assertRaisesRegex(module.ExampleError, "explicit.*config"):
                    module.resolve_config_path(root, Path("missing"))
            find.assert_not_called()

    def test_config_identity_validates_manifest_package_and_reports_non_git(self):
        with tempfile.TemporaryDirectory() as td:
            config = self.write_config(Path(td) / "config")
            with mock.patch.object(module.subprocess, "run", return_value=SimpleNamespace(returncode=128)):
                identity = module.config_identity(config)
            self.assertEqual(identity, {
                "path": str(config.resolve()), "package": "qubit-config", "version": "1.2.3",
                "git_head": None, "git_dirty": None,
            })
            wrong = self.write_config(Path(td) / "wrong", name="other")
            with self.assertRaisesRegex(module.ExampleError, "package.name.*qubit-config"):
                module.config_identity(wrong)

    def test_explicit_default_mode_conflict_is_rejected(self):
        with self.assertRaisesRegex(module.ExampleError, "--config-path.*config mode"):
            module.validate_config_mode("default", Path("config"))
        with mock.patch.object(sys, "argv", [str(SCRIPT), "--mode", "default", "--config-path", "config"]), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(module.main(), 2)
        self.assertIn("--config-path", output.getvalue())

    def test_config_mode_uses_one_explicit_source_and_identity_for_every_group(self):
        readme = module.extract_blocks("<!-- doc-example: id=r group=readme modes=config -->\n```rust,no_run\nfn readme_example() {}\n```", "README.md")
        guide = module.extract_blocks("<!-- doc-example: id=c group=guide_config modes=config -->\n```rust,no_run\nfn config_example() {}\n```", "doc/user_guide.md")
        documents = {"README.md": readme, "README.zh_CN.md": [], "doc/user_guide.md": guide, "doc/user_guide.zh_CN.md": []}
        manifest = {"groups": {"readme": {}, "guide_config": {"config": True}}}
        with tempfile.TemporaryDirectory() as td:
            config = self.write_config(Path(td) / "config")
            output = Path(td) / "evidence"
            seen = []
            real_run = module.subprocess.run

            def fake_cargo(command, **_kwargs):
                if command[0] == "git":
                    return real_run(command, **_kwargs)
                manifest_path = Path(command[command.index("--manifest-path") + 1])
                text = manifest_path.read_text(encoding="utf-8")
                seen.append(text)
                mapping = json.loads((output / "config" / manifest_path.parent.name / "mapping.json").read_text(encoding="utf-8"))
                self.assertEqual(mapping["config_identity"]["path"], str(config.resolve()))
                self.assertEqual(mapping["config_identity"]["package"], "qubit-config")
                if manifest["groups"][manifest_path.parent.name.rsplit("_", 1)[0]].get("config", False):
                    self.assertIn(f'path = "{config.resolve()}"', text)
                client = next(line for line in text.splitlines() if line.startswith("qubit-websocket = "))
                client_path = Path(re.search(r'path = "([^"]+)"', client).group(1))
                self.assertIn(f'path = "{config.resolve()}"', (client_path / "Cargo.toml").read_text(encoding="utf-8"))
                return SimpleNamespace(returncode=0, stdout="")

            with mock.patch.object(module.subprocess, "run", side_effect=fake_cargo), \
                 mock.patch.object(module, "resolve_config_path", wraps=module.resolve_config_path) as resolve, \
                 mock.patch.object(module, "config_identity", wraps=module.config_identity) as identity:
                self.assertEqual(module.compile_mode(documents, manifest, "config", output, config), 0)
            resolve.assert_called_once_with(module.ROOT, config)
            identity.assert_called_once_with(config.resolve())
            self.assertEqual(len(seen), 2)

    def test_resolver_preserves_automatic_discovery_when_no_path_is_given(self):
        expected = Path("/tmp/rs-config")
        with mock.patch.object(module, "find_config_path", return_value=expected) as find:
            self.assertEqual(module.resolve_config_path(Path("/tmp/project"), None), expected)
        find.assert_called_once_with(Path("/tmp/project"))

    def test_config_mode_enables_client_feature_for_every_group(self):
        readme = module.extract_blocks("<!-- doc-example: id=r group=readme modes=default,config -->\n```rust,no_run\nfn readme_example() {}\n```", "README.md")
        private_ca = module.extract_blocks("<!-- doc-example: id=p group=guide_private_ca modes=default,config -->\n```rust,no_run\nfn private_ca_example() {}\n```", "doc/user_guide.md")
        config_example = module.extract_blocks("<!-- doc-example: id=c group=guide_config modes=config -->\n```rust,no_run\nfn config_example() {}\n```", "doc/user_guide.md")
        documents = {"README.md": readme, "README.zh_CN.md": [], "doc/user_guide.md": private_ca + config_example, "doc/user_guide.zh_CN.md": []}
        manifest = {"groups": {"readme": {}, "guide_private_ca": {}, "guide_config": {"config": True}}}

        def generated_manifests(mode):
            captured = {}
            real_run = module.subprocess.run

            def fake_cargo(command, **_kwargs):
                if command[0] == "git":
                    return real_run(command, **_kwargs)
                cargo_manifest = Path(command[command.index("--manifest-path") + 1]).read_text()
                package = next(line.split('"')[1] for line in cargo_manifest.splitlines() if line.startswith("name = "))
                captured[package] = cargo_manifest
                if 'features = ["config"]' in cargo_manifest:
                    dependency_line = next(line for line in cargo_manifest.splitlines()
                                            if line.startswith("qubit-websocket = "))
                    client_path = re.search(r'path = "([^"]+)"', dependency_line).group(1)
                    client_manifest = (Path(client_path) / "Cargo.toml").read_text()
                    self.assertRegex(client_manifest,
                                     r'(?m)^qubit-config\s*=\s*\{[^}\n]*\bpath\s*=\s*"[^"]+"')
                return SimpleNamespace(returncode=0, stdout="")

            config_path = None
            if mode == "config":
                config_tmp = tempfile.TemporaryDirectory()
                config_path = self.write_config(Path(config_tmp.name) / "rs-config")
            try:
                with mock.patch.object(module.subprocess, "run", side_effect=fake_cargo):
                    self.assertEqual(module.compile_mode(documents, manifest, mode, None, config_path), 0)
            finally:
                if config_path is not None:
                    config_tmp.cleanup()
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
