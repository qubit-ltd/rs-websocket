#!/usr/bin/env python3
"""Probe registry resolution and behavior for an independent consumer."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

BASELINE_VERSION = "0.30.0"


class ProbeError(RuntimeError):
    """The independent consumer did not resolve or behave as required."""


def require_success(returncode: int, phase: str) -> None:
    if returncode:
        raise ProbeError(f"{phase} exited with status {returncode}")


def require_registry_backend(metadata: dict, *, mode: str) -> dict:
    packages = {package.get("id"): package for package in metadata.get("packages", [])}
    clients = [package for package in packages.values() if package.get("name") == "qubit-websocket"]
    if len(clients) != 1:
        raise ProbeError(f"expected one qubit-websocket package, found {len(clients)}")
    nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}

    def direct(node_id: str, name: str) -> list[dict]:
        node = nodes.get(node_id)
        if node is None:
            raise ProbeError(f"metadata is missing resolved node {node_id}")
        ids = [edge.get("pkg") for edge in node.get("deps", [])
               if edge.get("name", "").replace("-", "_") == name.replace("-", "_")]
        return [packages[package_id] for package_id in ids if package_id in packages
                and packages[package_id].get("name") == name]

    client = clients[0]
    tokio = direct(client["id"], "tokio-tungstenite")
    if len(tokio) != 1:
        raise ProbeError(f"client must resolve one direct tokio-tungstenite package; found {tokio!r}")
    if mode == "baseline" and tokio[0].get("version") != BASELINE_VERSION:
        raise ProbeError(f"baseline requires registry tokio-tungstenite {BASELINE_VERSION}; "
                         f"found {tokio[0].get('version')}")
    tokio_source = tokio[0].get("source")
    if not isinstance(tokio_source, str) or not tokio_source.startswith("registry+"):
        raise ProbeError(f"tokio-tungstenite edge is not from a registry: {tokio_source!r}")
    backend = direct(tokio[0]["id"], "tungstenite")
    if len(backend) != 1:
        raise ProbeError(f"tokio-tungstenite must have one direct tungstenite edge; found {backend!r}")
    actual = backend[0]
    source = actual.get("source")
    if not isinstance(source, str) or not source.startswith("registry+"):
        raise ProbeError(f"consumer tungstenite edge is not from a registry: {source!r}")
    if mode == "baseline" and actual.get("version") != BASELINE_VERSION:
        raise ProbeError(f"baseline requires registry tungstenite {BASELINE_VERSION}; found {actual.get('version')}")
    return {"tokio_tungstenite": {"version": tokio[0].get("version"), "source": tokio[0].get("source")},
            "tungstenite": {"version": actual.get("version"), "source": source},
            "edge": "qubit-websocket -> tokio-tungstenite -> tungstenite"}


def _run(argv: list[str], *, cwd: Path, env: dict[str, str], log_path: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    log_path.write_text(result.stdout, encoding="utf-8")
    print(result.stdout, end="")
    return result


def run(websocket_arg: str, output_arg: str, *, mode: str) -> int:
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
    backend_constraints = (f'tokio-tungstenite = "={BASELINE_VERSION}"\ntungstenite = "={BASELINE_VERSION}"\n'
                           if mode == "baseline" else "")
    manifest = f'''[package]\nname = "qubit-websocket-registry-probe"\nversion = "0.1.0"\nedition = "2024"\nrust-version = "1.94"\n\n[workspace]\n\n[dependencies]\nqubit-websocket = {{ path = {json.dumps(str(websocket))}, default-features = false }}\n{backend_constraints}base64 = "^0.22"\nsha1 = "^0.10"\ntokio = {{ version = "^1.54", features = ["net", "rt-multi-thread", "macros", "time", "io-util"] }}\n'''
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
    backend = require_registry_backend(metadata_json, mode=mode)
    (output / "Cargo.toml").write_text(manifest, encoding="utf-8")
    lockfile = workspace / "Cargo.lock"
    if not lockfile.is_file():
        raise ProbeError("cargo metadata did not create the probe Cargo.lock")
    shutil.copy2(lockfile, output / "Cargo.lock")
    env = os.environ.copy()
    env["CONSUMER_MODE"] = mode
    command = ["cargo", "--config", str(config), "test", "--manifest-path", str(workspace / "Cargo.toml"),
               "--locked", "--test", "handshake_tests"]
    result = _run(command, cwd=workspace, env=env, log_path=output / "tests.log")
    status = ("behavior verified" if result.returncode == 0 and mode == "current" else
              "historical limitation reproduced" if result.returncode == 0 else
              "handshake contract failed")
    source_head = subprocess.run(["git", "-C", str(websocket), "rev-parse", "HEAD"],
                                 text=True, capture_output=True, check=False)
    source_status = subprocess.run(["git", "-C", str(websocket), "status", "--short"],
                                   text=True, capture_output=True, check=False)
    evidence = {"mode": mode, "status": status, "backend": backend,
                "handshake_tests_exit_code": result.returncode,
                "source": {"head": source_head.stdout.strip() if source_head.returncode == 0 else None,
                           "dirty": bool(source_status.stdout.strip()) if source_status.returncode == 0 else None},
                "official_source": ("https://github.com/snapview/tungstenite-rs/blob/v"
                                    f"{backend['tungstenite']['version']}/src/handshake/client.rs"),
                "upstream_releases": "https://github.com/snapview/tungstenite-rs/releases",
                "crates_io_versions": "https://crates.io/api/v1/crates/tungstenite",
                "evidence_files": ["Cargo.toml", "Cargo.lock", "metadata.json", "tests.log"]}
    (output / "consumer-registry.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                                                    encoding="utf-8")
    try:
        require_success(result.returncode, "consumer handshake tests")
    except ProbeError as error:
        raise ProbeError(f"{error}; see {output / 'tests.log'} and {output / 'consumer-registry.json'}") from error
    print(f"consumer registry status: {status}; evidence: {output / 'consumer-registry.json'}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--websocket", required=True, help="absolute rs-websocket source tree")
    parser.add_argument("--output", required=True, help="absolute evidence directory outside source tree")
    parser.add_argument("--mode", choices=("baseline", "current"), default=None,
                        help="baseline reproduces tungstenite 0.30.0; current checks current resolution")
    parser.add_argument("--expect-fixed", action="store_true",
                        help="compatibility alias for --mode current")
    args = parser.parse_args(argv)
    if args.expect_fixed and args.mode == "baseline":
        parser.error("--expect-fixed conflicts with --mode baseline")
    args.mode = "current" if args.expect_fixed or args.mode is None else args.mode
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run(args.websocket, args.output, mode=args.mode)
    except (ProbeError, OSError, json.JSONDecodeError) as error:
        print(f"consumer registry check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
