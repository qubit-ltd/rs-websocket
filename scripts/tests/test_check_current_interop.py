"""Unit tests for current-source interop source validation and metadata assertions."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "check-current-interop.py"
SPEC = importlib.util.spec_from_file_location("check_current_interop", SCRIPT)
interop = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(interop)


def metadata_for(consumer_name: str, consumer_manifest: Path, dependency_name: str,
                 dependency_manifest: Path, source: str | None = None) -> dict:
    consumer_id, dependency_id = "consumer-id", "dependency-id"
    return {
        "packages": [
            {"id": consumer_id, "name": consumer_name, "manifest_path": str(consumer_manifest)},
            {"id": dependency_id, "name": dependency_name,
             "manifest_path": str(dependency_manifest), "source": source},
        ],
        "resolve": {"nodes": [{"id": consumer_id,
                               "deps": [{"name": dependency_name.replace("-", "_"), "pkg": dependency_id}]}]},
    }


class DirectDependencyTests(unittest.TestCase):
    def test_expected_direct_local_edge_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            consumer = root / "consumer/Cargo.toml"
            dependency = root / "dependency/Cargo.toml"
            self.assertEqual(interop.assert_direct_dependency(
                metadata_for("consumer", consumer, "qubit-web", dependency),
                "consumer", consumer, "qubit-web", dependency), dependency)

    def test_same_named_package_at_other_manifest_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            consumer, actual, expected = (root / name / "Cargo.toml" for name in ("c", "wrong", "right"))
            with self.assertRaisesRegex(interop.InteropError, "resolved to manifest"):
                interop.assert_direct_dependency(metadata_for("consumer", consumer, "qubit-web", actual),
                                                 "consumer", consumer, "qubit-web", expected)

    def test_git_source_residual_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            consumer, dependency = root / "c/Cargo.toml", root / "d/Cargo.toml"
            metadata = metadata_for("consumer", consumer, "qubit-web", dependency,
                                    "git+https://example.invalid/web#abc")
            with self.assertRaisesRegex(interop.InteropError, "source="):
                interop.assert_direct_dependency(metadata, "consumer", consumer, "qubit-web", dependency)

    def test_wrong_consumer_manifest_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            expected, dependency = root / "expected/Cargo.toml", root / "d/Cargo.toml"
            metadata = metadata_for("consumer", root / "other/Cargo.toml", "qubit-web", dependency)
            with self.assertRaisesRegex(interop.InteropError, "did not identify one"):
                interop.assert_direct_dependency(metadata, "consumer", expected, "qubit-web", dependency)

    def test_missing_direct_edge_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            consumer, dependency = root / "c/Cargo.toml", root / "d/Cargo.toml"
            metadata = metadata_for("consumer", consumer, "qubit-websocket", dependency)
            with self.assertRaisesRegex(interop.InteropError, "direct qubit-web edge"):
                interop.assert_direct_dependency(metadata, "consumer", consumer, "qubit-web", dependency)


def tungstenite_metadata(backend_source: str | None = None, include_backend: bool = True,
                        include_older_registry: bool = False) -> dict:
    client_manifest = "/tmp/current-client/Cargo.toml"
    packages = [
        {"id": "client", "name": "qubit-websocket", "version": "0.3.0",
         "manifest_path": client_manifest, "source": None},
        {"id": "tokio-ws-30", "name": "tokio-tungstenite", "version": "0.30.0",
         "manifest_path": "/registry/tokio-tungstenite-0.30/Cargo.toml", "source": "registry+index"},
    ]
    nodes = [
        {"id": "client", "deps": [{"name": "tokio_tungstenite", "pkg": "tokio-ws-30"}]},
        {"id": "tokio-ws-30", "deps": []},
    ]
    if include_backend:
        packages.append({"id": "tungstenite-git", "name": "tungstenite", "version": "0.30.0",
                         "manifest_path": "/git/tungstenite/Cargo.toml", "source": backend_source})
        nodes[1]["deps"].append({"name": "tungstenite", "pkg": "tungstenite-git"})
    if include_older_registry:
        packages.extend([
            {"id": "tokio-ws-29", "name": "tokio-tungstenite", "version": "0.29.0",
             "manifest_path": "/registry/tokio-tungstenite-0.29/Cargo.toml", "source": "registry+index"},
            {"id": "tungstenite-registry", "name": "tungstenite", "version": "0.29.0",
             "manifest_path": "/registry/tungstenite-0.29/Cargo.toml", "source": "registry+https://github.com/rust-lang/crates.io-index"},
        ])
        nodes.extend([
            {"id": "tokio-ws-29", "deps": [{"name": "tungstenite", "pkg": "tungstenite-registry"}]},
            {"id": "tungstenite-registry", "deps": []},
        ])
    return {"packages": packages, "resolve": {"nodes": nodes}}


class TungstenitePatchTests(unittest.TestCase):
    def test_expected_git_source_passes(self) -> None:
        manifest = Path("/tmp/current-client/Cargo.toml")
        metadata = tungstenite_metadata(interop.TUNGSTENITE_SOURCE)
        result = interop.assert_tungstenite_patch(metadata, manifest)
        self.assertEqual(result["tungstenite"]["source"], interop.TUNGSTENITE_SOURCE)
        self.assertIn("tokio-tungstenite 0.30", result["edge"])

    def test_unrelated_registry_029_can_coexist(self) -> None:
        manifest = Path("/tmp/current-client/Cargo.toml")
        metadata = tungstenite_metadata(interop.TUNGSTENITE_SOURCE, include_older_registry=True)
        result = interop.assert_tungstenite_patch(metadata, manifest)
        self.assertEqual(result["tungstenite"]["source"], interop.TUNGSTENITE_SOURCE)
        self.assertIn({"id": "tungstenite-registry", "version": "0.29.0",
                       "source": "registry+https://github.com/rust-lang/crates.io-index"},
                      result["coexisting_tungstenite"])

    def test_wrong_git_revision_fails(self) -> None:
        manifest = Path("/tmp/current-client/Cargo.toml")
        wrong_source = f"git+{interop.TUNGSTENITE_URL}?rev=wrong#wrong"
        with self.assertRaisesRegex(interop.InteropError, "exact Git source"):
            interop.assert_tungstenite_patch(tungstenite_metadata(wrong_source), manifest)

    def test_missing_git_backend_fails(self) -> None:
        manifest = Path("/tmp/current-client/Cargo.toml")
        metadata = tungstenite_metadata(include_backend=False, include_older_registry=True)
        with self.assertRaisesRegex(interop.InteropError, "exact Git source"):
            interop.assert_tungstenite_patch(metadata, manifest)


class PathSafetyTests(unittest.TestCase):
    def make_source(self, root: Path, name: str, package: str) -> Path:
        directory = root / name
        directory.mkdir()
        (directory / "Cargo.toml").write_text(f'[package]\nname = "{package}"\n', encoding="utf-8")
        return directory

    def test_overlapping_source_paths_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            web = self.make_source(root, "web", "qubit-web")
            websocket = self.make_source(web, "nested", "qubit-websocket")
            output = root / "evidence"
            with self.assertRaisesRegex(interop.InteropError, "overlap"):
                interop.validate_paths(str(web), str(websocket), str(output))

    def test_output_inside_source_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            web = self.make_source(root, "web", "qubit-web")
            websocket = self.make_source(root, "websocket", "qubit-websocket")
            with self.assertRaisesRegex(interop.InteropError, "outside both"):
                interop.validate_paths(str(web), str(websocket), str(web / "evidence"))

    @unittest.skipUnless(hasattr(Path, "symlink_to"), "symlinks unavailable")
    def test_output_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            web = self.make_source(root, "web", "qubit-web")
            websocket = self.make_source(root, "websocket", "qubit-websocket")
            real, link = root / "real", root / "link"
            real.mkdir()
            try:
                link.symlink_to(real, target_is_directory=True)
            except OSError as error:
                self.skipTest(str(error))
            with self.assertRaisesRegex(interop.InteropError, "symbolic link"):
                interop.validate_paths(str(web), str(websocket), str(link))


class ReadOnlySnapshotTests(unittest.TestCase):
    def test_tracked_target_files_are_excluded_from_source_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "repository"
            source.mkdir()
            (source / "Cargo.toml").write_text('[package]\nname="sample"\n', encoding="utf-8")
            target_file = source / "target" / "generated.txt"
            target_file.parent.mkdir()
            target_file.write_text("build output\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=source, check=True)
            subprocess.run(["git", "add", "Cargo.toml", "target/generated.txt"], cwd=source, check=True)

            paths = interop._source_paths(source)

            self.assertEqual([Path("Cargo.toml")], paths)
            snapshot = interop._snapshot(source, Path(temporary) / "copy")
            self.assertFalse((snapshot / "target" / "generated.txt").exists())

    def test_manifest_and_lockfile_hashes_are_unchanged_by_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "rs-websocket"
            source.mkdir()
            (source / "Cargo.toml").write_text('[package]\nname="qubit-websocket"\n', encoding="utf-8")
            (source / "Cargo.lock").write_text("lock input\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=source, check=True)
            subprocess.run(["git", "add", "Cargo.toml", "Cargo.lock"], cwd=source, check=True)
            before = interop.source_state(source)
            snapshot = interop._snapshot(source, root / "copies" / "rs-websocket")
            self.assertTrue((snapshot / "Cargo.toml").is_file())
            self.assertEqual(before, interop.source_state(source))


if __name__ == "__main__":
    unittest.main()
