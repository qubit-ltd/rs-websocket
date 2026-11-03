#!/usr/bin/env python3
"""Probe a consumer with tungstenite resolved from crates.io, without Cargo patches.

The currently available registry backend is tungstenite 0.30.0. Its handshake
validator only accepts the whole Connection value "Upgrade", so this probe
reports a known limitation unless --expect-fixed is selected for a future
compatible registry release.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

REGISTRY_TUNGSTENITE_VERSION = "0.30.0"
TOKIO_TUNGSTENITE_SERIES = "0.30."


class ProbeError(RuntimeError):
    """The independent consumer did not resolve or behave as required."""


def require_success(returncode: int, phase: str) -> None:
    if returncode:
        raise ProbeError(f"{phase} exited with status {returncode}")


def require_registry_backend(metadata: dict, *, expect_fixed: bool) -> dict:
    packages = {package.get("id"): package for package in metadata.get("packages", [])}
    clients = [package for package in packages.values() if package.get("name") == "qubit-websocket"]
    if len(clients) != 1:
        raise ProbeError(f"expected one qubit-websocket package, found {len(clients)}")
    nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}

    def direct(node_id: str, name: str) -> list[dict]:
        node = nodes.get(node_id)
        if node is None:
            raise ProbeError(f"metadata is missing resolved node {node_id}")
        ids = {edge.get("pkg") for edge in node.get("deps", [])
               if edge.get("name", "").replace("-", "_") == name.replace("-", "_")}
        return [packages[package_id] for package_id in ids if package_id in packages
                and packages[package_id].get("name") == name]

    client = clients[0]
    tokio = [package for package in direct(client["id"], "tokio-tungstenite")
             if str(package.get("version", "")).startswith(TOKIO_TUNGSTENITE_SERIES)]
    if len(tokio) != 1:
        raise ProbeError(f"client tokio-tungstenite edge must resolve to one 0.30.x package; found {tokio!r}")
    backend = direct(tokio[0]["id"], "tungstenite")
    if len(backend) != 1:
        raise ProbeError(f"tokio-tungstenite must have one direct tungstenite edge; found {backend!r}")
    actual = backend[0]
    source = actual.get("source")
    if not isinstance(source, str) or not source.startswith("registry+"):
        raise ProbeError(f"consumer tungstenite edge is not from a registry: {source!r}")
    if expect_fixed:
        version = str(actual.get("version", ""))
        if not version.startswith("0.30.") or version == REGISTRY_TUNGSTENITE_VERSION:
            raise ProbeError("--expect-fixed requires a fixed tungstenite 0.30.x patch release; "
                             f"found {version!r}")
    elif actual.get("version") != REGISTRY_TUNGSTENITE_VERSION:
        raise ProbeError(f"known-limitation probe must pin registry tungstenite {REGISTRY_TUNGSTENITE_VERSION}; "
                         f"found {actual.get('version')}")
    return {"tokio_tungstenite": {"version": tokio[0].get("version"), "source": tokio[0].get("source")},
            "tungstenite": {"version": actual.get("version"), "source": source},
            "edge": "qubit-websocket -> tokio-tungstenite -> tungstenite"}


def _run(argv: list[str], *, cwd: Path, env: dict[str, str], log_path: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    log_path.write_text(result.stdout, encoding="utf-8")
    print(result.stdout, end="")
    return result


def run(websocket_arg: str, output_arg: str, *, expect_fixed: bool) -> int:
    websocket, output = Path(websocket_arg), Path(output_arg)
    if not websocket.is_absolute() or not output.is_absolute():
        raise ProbeError("--websocket and --output must be absolute paths")
    websocket, output = websocket.resolve(), output.resolve()
    if not (websocket / "Cargo.toml").is_file():
        raise ProbeError(f"missing consumer crate manifest: {websocket / 'Cargo.toml'}")
    if output == websocket or websocket in output.parents or output in websocket.parents:
        raise ProbeError("output must be outside the source tree")
    if output.exists() and any(output.iterdir()):
        raise ProbeError(f"output directory is not empty; select a new evidence path: {output}")
    output.mkdir(parents=True, exist_ok=True)
    workspace = output / "registry-consumer"
    (workspace / "tests").mkdir(parents=True)
    manifest = f'''[package]\nname = "qubit-websocket-registry-probe"\nversion = "0.1.0"\nedition = "2024"\nrust-version = "1.94"\n\n[workspace]\n\n[dependencies]\nqubit-websocket = {{ path = {json.dumps(str(websocket))}, default-features = false }}\nbase64 = "^0.22"\nsha1 = "^0.10"\ntokio = {{ version = "^1.54", features = ["net", "rt-multi-thread", "macros", "time", "io-util"] }}\n'''
    (workspace / "Cargo.toml").write_text(manifest, encoding="utf-8")
    source_test = websocket / "tests/fixtures/consumer/tests/handshake_tests.rs"
    shutil.copy2(source_test, workspace / "tests/handshake_tests.rs")
    config = output / "cargo-config.toml"
    config.write_text("", encoding="utf-8")  # An empty config makes inheritance explicit.
    metadata_command = ["cargo", "--config", str(config), "metadata", "--manifest-path",
                       str(workspace / "Cargo.toml"), "--format-version", "1"]
    metadata = subprocess.run(metadata_command, cwd=workspace, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    (output / "metadata.stderr.log").write_text(metadata.stderr, encoding="utf-8")
    try:
        require_success(metadata.returncode, "cargo metadata")
    except ProbeError as error:
        raise ProbeError(f"{error}:\n{metadata.stderr}") from error
    metadata_json = json.loads(metadata.stdout)
    (output / "metadata.json").write_text(json.dumps(metadata_json, indent=2, sort_keys=True) + "\n",
                                           encoding="utf-8")
    backend = require_registry_backend(metadata_json, expect_fixed=expect_fixed)
    (output / "Cargo.toml").write_text(manifest, encoding="utf-8")
    lockfile = workspace / "Cargo.lock"
    if not lockfile.is_file():
        raise ProbeError("cargo metadata did not create the probe Cargo.lock")
    shutil.copy2(lockfile, output / "Cargo.lock")
    env = os.environ.copy()
    env["CONSUMER_EXPECT_FIXED"] = "1" if expect_fixed else "0"
    command = ["cargo", "--config", str(config), "test", "--manifest-path", str(workspace / "Cargo.toml"),
               "--locked", "--test", "handshake_tests"]
    result = _run(command, cwd=workspace, env=env, log_path=output / "tests.log")
    try:
        require_success(result.returncode, "consumer handshake tests")
    except ProbeError as error:
        raise ProbeError(f"{error}; see {output / 'tests.log'}") from error
    status = "fixed" if expect_fixed else "known limitation"
    evidence = {"status": status, "backend": backend, "expect_fixed": expect_fixed,
                "handshake_tests_exit_code": result.returncode,
                "registry_0_30_observed_behavior": {
                    "comma_separated_connection_tokens": "rejected",
                    "duplicate_connection_upgrade_first": "accepted (first value returned by HeaderMap.get)",
                    "duplicate_connection_upgrade_second": "rejected (first value returned by HeaderMap.get)",
                    "fixed_mode_requirement": "both duplicate orders and all legal token lists must be accepted",
                },
                "official_source": "https://github.com/snapview/tungstenite-rs/blob/v0.30.0/src/handshake/client.rs",
                "upstream_releases": "https://github.com/snapview/tungstenite-rs/releases",
                "crates_io_versions": "https://crates.io/api/v1/crates/tungstenite",
                "evidence_files": ["Cargo.toml", "Cargo.lock", "metadata.json", "tests.log"]}
    (output / "consumer-registry.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                                                    encoding="utf-8")
    print(f"consumer registry status: {status}; evidence: {output / 'consumer-registry.json'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--websocket", required=True, help="absolute rs-websocket source tree")
    parser.add_argument("--output", required=True, help="absolute evidence directory outside source tree")
    parser.add_argument("--expect-fixed", action="store_true",
                        help="require a future fixed registry release and all legal handshakes to pass")
    args = parser.parse_args(argv)
    try:
        return run(args.websocket, args.output, expect_fixed=args.expect_fixed)
    except (ProbeError, OSError, json.JSONDecodeError) as error:
        print(f"consumer registry check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
