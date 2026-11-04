"""Tests for the independent registry consumer resolution contract."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "check-consumer-registry.py"
SPEC = importlib.util.spec_from_file_location("check_consumer_registry", SCRIPT)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def metadata(*, tungstenite_version: str = "0.30.0",
             tungstenite_source: str | None = "registry+https://github.com/rust-lang/crates.io-index",
             tokio_version: str = "0.30.0",
             tokio_source: str | None = "registry+https://github.com/rust-lang/crates.io-index") -> dict:
    return {
        "packages": [
            {"id": "consumer", "name": "qubit-websocket", "version": "0.3.0"},
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
            (websocket / "tests/fixtures/consumer/tests").mkdir(parents=True)
            (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            source_test = websocket / "tests/fixtures/consumer/tests/handshake_tests.rs"
            source_test.write_text("// probe test\n")
            commands: list[list[str]] = []
            outputs = [
                subprocess.CompletedProcess([], 0, json.dumps(metadata()), ""),
                subprocess.CompletedProcess([], 0, "ok", ""),
            ]

            def fake_run(argv, **kwargs):
                commands.append(argv)
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, "abc123\n", "")
                if "metadata" in argv:
                    lockfile = Path(kwargs["cwd"]) / "Cargo.lock"
                    lockfile.write_text("# generated lock\n")
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
            (websocket / "tests/fixtures/consumer/tests").mkdir(parents=True)
            (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            (websocket / "tests/fixtures/consumer/tests/handshake_tests.rs").write_text("// probe test\n")
            output = Path(temporary) / "evidence"

            def fake_run(argv, **kwargs):
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, "abc123\n", "")
                if "metadata" in argv:
                    (Path(kwargs["cwd"]) / "Cargo.lock").write_text("# generated lock\n")
                    return subprocess.CompletedProcess(argv, 0, json.dumps(metadata()), "")
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
                (websocket / "tests/fixtures/consumer/tests").mkdir(parents=True)
                (websocket / "Cargo.toml").write_text("[package]\nname='fixture'\n")
                (websocket / "tests/fixtures/consumer/tests/handshake_tests.rs").write_text("// probe test\n")
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


if __name__ == "__main__":
    unittest.main()
