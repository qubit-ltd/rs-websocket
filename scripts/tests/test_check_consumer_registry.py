"""Tests for the independent registry consumer resolution contract."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "check-consumer-registry.py"
SPEC = importlib.util.spec_from_file_location("check_consumer_registry", SCRIPT)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def write_handshake_sources(source_test: Path, test_content: str = "// probe test\n",
                            contract_content: str = "// shared contract\n") -> None:
    source_test.parent.mkdir(parents=True, exist_ok=True)
    source_test.write_text(test_content)
    contract = source_test.parent / "contract/handshake_contract.rs"
    contract.parent.mkdir(parents=True, exist_ok=True)
    contract.write_text(contract_content)


def metadata(*, tungstenite_version: str = "0.30.0",
             tungstenite_source: str | None = "registry+https://github.com/rust-lang/crates.io-index",
             tokio_version: str = "0.30.0",
             tokio_source: str | None = "registry+https://github.com/rust-lang/crates.io-index",
             client_manifest: str = "/source/Cargo.toml") -> dict:
    return {
        "packages": [
            {"id": "consumer", "name": "qubit-websocket", "version": "0.3.0",
             "manifest_path": client_manifest},
            {"id": "tokio", "name": "tokio-tungstenite", "version": tokio_version,
             "source": tokio_source},
            {"id": "tungstenite", "name": "tungstenite", "version": tungstenite_version,
             "source": tungstenite_source},
        ],
        "resolve": {"nodes": [
            {"id": "consumer", "deps": [{"name": "tokio_tungstenite", "pkg": "tokio"}]},
            {"id": "tokio", "deps": [{"name": "tungstenite", "pkg": "tungstenite"}]},
            {"id": "tungstenite", "deps": []},
        ]},
    }


class RegistryResolutionTests(unittest.TestCase):
    def test_baseline_030_is_reported_as_historical_limitation(self) -> None:
        resolved = probe.require_registry_backend(metadata(), mode="baseline")
        self.assertEqual(resolved["tungstenite"]["version"], "0.30.0")
        self.assertTrue(resolved["tungstenite"]["source"].startswith("registry+"))

    def test_git_backend_is_rejected_as_source_misclassification(self) -> None:
        graph = metadata(tungstenite_source="git+https://example.invalid/tungstenite#abc")
        with self.assertRaisesRegex(probe.ProbeError, "not from a registry"):
            probe.require_registry_backend(graph, mode="baseline")

    def test_abnormal_command_exit_is_never_success(self) -> None:
        with self.assertRaisesRegex(probe.ProbeError, "exited with status 17"):
            probe.require_success(17, "consumer handshake tests")

    def test_baseline_requires_exact_historical_versions(self) -> None:
        graph = metadata(tokio_version="0.30.0", tungstenite_version="0.30.0")
        resolved = probe.require_registry_backend(graph, mode="baseline")
        self.assertEqual(resolved["tungstenite"]["version"], "0.30.0")
        with self.assertRaisesRegex(probe.ProbeError, "baseline.*0.30.0"):
            probe.require_registry_backend(metadata(tungstenite_version="0.30.1"), mode="baseline")
        with self.assertRaisesRegex(probe.ProbeError, "baseline.*tokio-tungstenite.*0.30.0"):
            probe.require_registry_backend(metadata(tokio_version="0.30.1"), mode="baseline")

    def test_current_accepts_registry_versions_without_version_heuristic(self) -> None:
        resolved = probe.require_registry_backend(
            metadata(tokio_version="0.31.0", tungstenite_version="0.31.0"), mode="current")
        self.assertEqual(resolved["tungstenite"]["version"], "0.31.0")

    def test_current_still_rejects_non_registry_backend(self) -> None:
        graph = metadata(tungstenite_source="git+https://example.invalid/tungstenite#abc")
        with self.assertRaisesRegex(probe.ProbeError, "not from a registry"):
            probe.require_registry_backend(graph, mode="current")

    def test_current_rejects_non_registry_tokio_tungstenite(self) -> None:
        graph = metadata(tokio_source="git+https://example.invalid/tokio-tungstenite#abc")
        with self.assertRaisesRegex(probe.ProbeError, "tokio-tungstenite edge is not from a registry"):
            probe.require_registry_backend(graph, mode="current")

    def test_duplicate_backend_edges_are_rejected(self) -> None:
        graph = metadata()
        graph["resolve"]["nodes"][1]["deps"].append(
            {"name": "tungstenite", "pkg": "tungstenite"})
        with self.assertRaisesRegex(probe.ProbeError, "one direct tungstenite edge"):
            probe.require_registry_backend(graph, mode="baseline")

    def test_modes_generate_fixed_or_unconstrained_backend_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            websocket = Path(temporary) / "source"
            websocket.mkdir()
            (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            source_test = websocket / "tests/fixtures/consumer/tests/handshake_tests.rs"
            write_handshake_sources(source_test)
            commands: list[list[str]] = []
            outputs = [subprocess.CompletedProcess([], 0, "ok", "")]

            def fake_run(argv, **kwargs):
                commands.append(argv)
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, "abc123\n", "")
                if "metadata" in argv:
                    lockfile = Path(kwargs["cwd"]) / "Cargo.lock"
                    lockfile.write_text("# generated lock\n")
                    return subprocess.CompletedProcess(
                        argv, 0,
                        json.dumps(metadata(client_manifest=str(websocket.resolve() / "Cargo.toml"))), "")
                return outputs.pop(0)

            output = Path(temporary) / "evidence"
            with patch.object(probe.subprocess, "run", side_effect=fake_run):
                self.assertEqual(probe.run(str(websocket), str(output), mode="baseline"), 0)
            manifest = (output / "Cargo.toml").read_text()
            self.assertIn('tokio-tungstenite = "=0.30.0"', manifest)
            self.assertIn('tungstenite = "=0.30.0"', manifest)
            self.assertIn('"mode": "baseline"', (output / "consumer-registry.json").read_text())
            self.assertIn("--locked", commands[1])

    def test_current_manifest_does_not_pin_backend_versions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            websocket = Path(temporary) / "source"
            websocket.mkdir()
            (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            write_handshake_sources(websocket / "tests/fixtures/consumer/tests/handshake_tests.rs")
            output = Path(temporary) / "evidence"

            def fake_run(argv, **kwargs):
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, "abc123\n", "")
                if "metadata" in argv:
                    (Path(kwargs["cwd"]) / "Cargo.lock").write_text("# generated lock\n")
                    return subprocess.CompletedProcess(
                        argv, 0,
                        json.dumps(metadata(client_manifest=str(websocket.resolve() / "Cargo.toml"))), "")
                return subprocess.CompletedProcess(argv, 0, "ok", "")

            with patch.object(probe.subprocess, "run", side_effect=fake_run):
                self.assertEqual(probe.run(str(websocket), str(output), mode="current"), 0)
            manifest = (output / "Cargo.toml").read_text()
            self.assertNotIn('tokio-tungstenite = "=0.30.0"', manifest)
            self.assertNotIn('tungstenite = "=0.30.0"', manifest)
            self.assertIn('"mode": "current"', (output / "consumer-registry.json").read_text())

    def test_metadata_and_behavior_failures_propagate(self) -> None:
        for failure_phase in ("metadata", "test"):
            with self.subTest(phase=failure_phase), tempfile.TemporaryDirectory() as temporary:
                websocket = Path(temporary) / "source"
                websocket.mkdir()
                (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
                write_handshake_sources(websocket / "tests/fixtures/consumer/tests/handshake_tests.rs")
                output = Path(temporary) / "evidence"

                def fake_run(argv, **kwargs):
                    if argv[0] == "git":
                        return subprocess.CompletedProcess(argv, 0, "abc123\n", "")
                    if "metadata" in argv:
                        if failure_phase == "metadata":
                            return subprocess.CompletedProcess(argv, 9, "", "metadata failure")
                        (Path(kwargs["cwd"]) / "Cargo.lock").write_text("# generated lock\n")
                        return subprocess.CompletedProcess(argv, 0, json.dumps(metadata()), "")
                    return subprocess.CompletedProcess(argv, 11, "test failure", "")

                with patch.object(probe.subprocess, "run", side_effect=fake_run):
                    with self.assertRaises(probe.ProbeError):
                        probe.run(str(websocket), str(output), mode="current")

    def test_expect_fixed_is_current_alias_and_conflicts_are_rejected(self) -> None:
        args = probe.parse_args(["--websocket", "/source", "--output", "/out", "--expect-fixed"])
        self.assertEqual(args.mode, "current")
        with self.assertRaises(SystemExit):
            probe.parse_args(["--websocket", "/source", "--output", "/out",
                              "--mode", "baseline", "--expect-fixed"])


class PatchedResolutionTests(unittest.TestCase):
    def patched_metadata(self, *, source: str | None =
                         f"git+{probe.PATCH_URL}?rev={probe.PATCH_REV}#{probe.PATCH_REV}",
                         manifest_path: str = "/source/Cargo.toml") -> dict:
        graph = metadata(tungstenite_source=source, client_manifest=manifest_path)
        return graph

    def test_fixture_and_bilingual_guides_share_the_probe_patch(self) -> None:
        project = Path(__file__).resolve().parents[2]
        fixture = tomllib.loads((project / "tests/fixtures/consumer/Cargo.toml").read_text())
        manifests = [fixture]
        for guide in (project / "doc/user_guide.md", project / "doc/user_guide.zh_CN.md"):
            blocks = re.findall(r"```toml\s*\n(.*?)```", guide.read_text(), re.DOTALL)
            manifests.extend(tomllib.loads(block) for block in blocks if "[patch.crates-io]" in block)
        self.assertEqual(len(manifests), 3)
        for manifest in manifests:
            with self.subTest(manifest=manifest):
                patch = manifest["patch"]["crates-io"]["tungstenite"]
                self.assertEqual(patch["git"], probe.PATCH_URL)
                self.assertEqual(patch["rev"], probe.PATCH_REV)

    def test_patched_requires_exact_client_manifest_and_fork_revision(self) -> None:
        expected = probe.PATCH_REV
        graph = self.patched_metadata()
        resolved = probe.require_patched_backend(graph, websocket=Path("/source"))
        self.assertEqual(resolved["tungstenite"]["rev"], expected)
        with self.assertRaisesRegex(probe.ProbeError, "manifest_path"):
            probe.require_patched_backend(
                self.patched_metadata(manifest_path="/other/Cargo.toml"), websocket=Path("/source"))
        with self.assertRaisesRegex(probe.ProbeError, "revision"):
            probe.require_patched_backend(
                self.patched_metadata(source="git+https://github.com/qubit-ltd/tungstenite-rs?rev=branch#abc"),
                websocket=Path("/source"))

    def test_patched_rejects_registry_or_unrelated_git_backend(self) -> None:
        for source in ("registry+https://github.com/rust-lang/crates.io-index",
                       "git+https://example.invalid/tungstenite?rev=main#"
                       "b24a4442be0247a71fe9ee4e520eb483ac1b2873"):
            with self.subTest(source=source), self.assertRaises(probe.ProbeError):
                probe.require_patched_backend(
                    self.patched_metadata(source=source), websocket=Path("/source"))

    def test_patched_manifest_contains_path_and_pinned_fork(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            websocket = Path(temporary) / "source"
            websocket.mkdir()
            (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            source_test = websocket / "tests/fixtures/consumer/tests/handshake_tests.rs"
            write_handshake_sources(source_test, "// source test\n", "// source contract\n")
            output = Path(temporary) / "evidence"
            target_dirs: list[str] = []

            def fake_run(argv, **kwargs):
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, "abc123\n", "")
                if "metadata" in argv:
                    (Path(kwargs["cwd"]) / "Cargo.lock").write_text("# generated lock\n")
                    return subprocess.CompletedProcess(
                        argv, 0,
                        json.dumps(self.patched_metadata(
                            manifest_path=str(websocket / "Cargo.toml"))), "")
                target_dirs.append(kwargs["env"]["CARGO_TARGET_DIR"])
                return subprocess.CompletedProcess(argv, 0, "ok", "")

            with patch.object(probe.subprocess, "run", side_effect=fake_run):
                self.assertEqual(probe.run(str(websocket), str(output), mode="patched"), 0)
            manifest = (output / "Cargo.toml").read_text()
            self.assertIn(f'qubit-websocket = {{ path = {json.dumps(str(websocket))}', manifest)
            self.assertIn(f'rev = "{probe.PATCH_REV}"', manifest)
            evidence = json.loads((output / "consumer-registry.json").read_text())
            self.assertEqual(evidence["manifest_path"], str(websocket / "Cargo.toml"))
            self.assertEqual(evidence["test_source"]["path"], str(source_test))
            self.assertFalse(Path(target_dirs[0]).is_relative_to(output))

    def test_unpacked_crate_uses_explicit_test_source_and_does_not_claim_parent_git_head(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkout = root / "checkout"
            unpacked = checkout / "target/package/qubit-websocket-0.3.1"
            unpacked.mkdir(parents=True)
            (unpacked / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            test_source = checkout / "tests/handshake_tests.rs"
            write_handshake_sources(test_source, "// external packaged test\n",
                                    "// external packaged contract\n")
            output = root / "evidence"

            def fake_run(argv, **kwargs):
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, str(checkout) + "\n", "")
                if "metadata" in argv:
                    (Path(kwargs["cwd"]) / "Cargo.lock").write_text("# generated lock\n")
                    return subprocess.CompletedProcess(
                        argv, 0,
                        json.dumps(self.patched_metadata(
                            manifest_path=str(unpacked / "Cargo.toml"))), "")
                return subprocess.CompletedProcess(argv, 0, "ok", "")

            with patch.object(probe.subprocess, "run", side_effect=fake_run):
                self.assertEqual(probe.run(str(unpacked), str(output), mode="patched",
                                           test_source_arg=str(test_source)), 0)
            self.assertTrue((output / "tests/handshake_tests.rs").is_file())
            self.assertEqual((output / "tests/contract/handshake_contract.rs").read_text(),
                             "// external packaged contract\n")
            evidence = json.loads((output / "consumer-registry.json").read_text())
            self.assertIsNone(evidence["source"]["head"])
            self.assertEqual(evidence["test_sources"]["handshake_contract"]["path"],
                             str(test_source.parent / "contract/handshake_contract.rs"))
            self.assertEqual(evidence["test_sources"]["handshake_contract"]["sha256"],
                             probe.hashlib.sha256(
                                 (test_source.parent / "contract/handshake_contract.rs").read_bytes()
                             ).hexdigest())

    def test_patched_requires_test_source_and_propagates_subprocess_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            websocket = root / "source"
            websocket.mkdir()
            (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            with self.assertRaisesRegex(probe.ProbeError, "test source"):
                probe.run(str(websocket), str(root / "missing-test"), mode="patched")

            test_source = websocket / "tests/handshake_tests.rs"
            test_source.parent.mkdir(parents=True)
            test_source.write_text("// no shared file\n")
            with self.assertRaisesRegex(probe.ProbeError, "shared handshake contract"):
                probe.run(str(websocket), str(root / "missing-contract"), mode="patched",
                          test_source_arg=str(test_source))



if __name__ == "__main__":
    unittest.main()
