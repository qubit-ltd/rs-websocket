"""Tests for the independent registry consumer resolution contract."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "check-consumer-registry.py"
SPEC = importlib.util.spec_from_file_location("check_consumer_registry", SCRIPT)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def metadata(*, tungstenite_version: str = "0.30.0",
             tungstenite_source: str | None = "registry+https://github.com/rust-lang/crates.io-index",
             tokio_version: str = "0.30.0") -> dict:
    return {
        "packages": [
            {"id": "consumer", "name": "qubit-websocket", "version": "0.3.0"},
            {"id": "tokio", "name": "tokio-tungstenite", "version": tokio_version,
             "source": "registry+https://github.com/rust-lang/crates.io-index"},
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
    def test_registry_030_is_reported_as_known_limitation(self) -> None:
        resolved = probe.require_registry_backend(metadata(), expect_fixed=False)
        self.assertEqual(resolved["tungstenite"]["version"], "0.30.0")
        self.assertTrue(resolved["tungstenite"]["source"].startswith("registry+"))

    def test_git_backend_is_rejected_as_source_misclassification(self) -> None:
        graph = metadata(tungstenite_source="git+https://example.invalid/tungstenite#abc")
        with self.assertRaisesRegex(probe.ProbeError, "not from a registry"):
            probe.require_registry_backend(graph, expect_fixed=False)

    def test_wrong_registry_version_is_rejected_for_baseline(self) -> None:
        with self.assertRaisesRegex(probe.ProbeError, "must pin registry tungstenite 0.30.0"):
            probe.require_registry_backend(metadata(tungstenite_version="0.31.0"), expect_fixed=False)

    def test_expect_fixed_rejects_known_limited_version(self) -> None:
        with self.assertRaisesRegex(probe.ProbeError, "requires a fixed tungstenite 0.30.x patch release"):
            probe.require_registry_backend(metadata(), expect_fixed=True)

    def test_expect_fixed_rejects_unverified_compatibility_line(self) -> None:
        graph = metadata(tungstenite_version="0.29.0")
        with self.assertRaisesRegex(probe.ProbeError, "requires a fixed tungstenite 0.30.x patch release"):
            probe.require_registry_backend(graph, expect_fixed=True)

    def test_abnormal_command_exit_is_never_success(self) -> None:
        with self.assertRaisesRegex(probe.ProbeError, "exited with status 17"):
            probe.require_success(17, "consumer handshake tests")


if __name__ == "__main__":
    unittest.main()
