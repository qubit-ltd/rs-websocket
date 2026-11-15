#!/usr/bin/env python3
"""Probe independent-consumer resolution and WebSocket handshake behavior."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

BASELINE_VERSION = "0.30.0"
PATCH_URL = "https://github.com/qubit-ltd/tungstenite-rs"
PATCH_REV = "b24a4442be0247a71fe9ee4e520eb483ac1b2873"


class ProbeError(RuntimeError):
    """The independent consumer did not resolve or behave as required."""


def require_success(returncode: int, phase: str) -> None:
    if returncode:
        raise ProbeError(f"{phase} exited with status {returncode}")


def direct_package(metadata: dict, package_id: str, dependency: str) -> dict:
    packages = {package.get("id"): package for package in metadata.get("packages", [])}
    nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}
    node = nodes.get(package_id)
    if node is None:
        raise ProbeError(f"metadata is missing resolved node {package_id}")
    ids = [edge.get("pkg") for edge in node.get("deps", [])
           if edge.get("name", "").replace("-", "_") == dependency.replace("-", "_")]
    matches = [packages[package_id] for package_id in ids if package_id in packages
               and packages[package_id].get("name") == dependency]
    if len(matches) != 1:
        raise ProbeError(f"{package_id} must have one direct {dependency} edge; found {matches!r}")
    return matches[0]


def client_package(metadata: dict, websocket: Path) -> dict:
    clients = [package for package in metadata.get("packages", [])
               if package.get("name") == "qubit-websocket"]
    if len(clients) != 1:
        raise ProbeError(f"expected one qubit-websocket package, found {len(clients)}")
    client = clients[0]
    expected_manifest = (websocket / "Cargo.toml").resolve()
    actual_manifest = Path(client.get("manifest_path", "")).resolve()
    if actual_manifest != expected_manifest:
        raise ProbeError(f"qubit-websocket manifest_path {actual_manifest} does not match "
                         f"the requested crate {expected_manifest}")
    return client


def require_registry_backend(metadata: dict, *, mode: str, websocket: Path | None = None) -> dict:
    clients = [package for package in metadata.get("packages", [])
               if package.get("name") == "qubit-websocket"]
    if len(clients) != 1:
        raise ProbeError(f"expected one qubit-websocket package, found {len(clients)}")
    client = clients[0] if websocket is None else client_package(metadata, websocket)
    tokio = direct_package(metadata, client["id"], "tokio-tungstenite")
    if mode == "baseline" and tokio.get("version") != BASELINE_VERSION:
        raise ProbeError(f"baseline requires registry tokio-tungstenite {BASELINE_VERSION}; "
                         f"found {tokio.get('version')}")
    tokio_source = tokio.get("source")
    if not isinstance(tokio_source, str) or not tokio_source.startswith("registry+"):
        raise ProbeError(f"tokio-tungstenite edge is not from a registry: {tokio_source!r}")
    actual = direct_package(metadata, tokio["id"], "tungstenite")
    source = actual.get("source")
    if not isinstance(source, str) or not source.startswith("registry+"):
        raise ProbeError(f"consumer tungstenite edge is not from a registry: {source!r}")
    if mode == "baseline" and actual.get("version") != BASELINE_VERSION:
        raise ProbeError(f"baseline requires registry tungstenite {BASELINE_VERSION}; "
                         f"found {actual.get('version')}")
    return {"tokio_tungstenite": {"version": tokio.get("version"), "source": tokio_source},
            "tungstenite": {"version": actual.get("version"), "source": source},
            "edge": "qubit-websocket -> tokio-tungstenite -> tungstenite"}


def require_patched_backend(metadata: dict, *, websocket: Path) -> dict:
    client = client_package(metadata, websocket)
    tokio = direct_package(metadata, client["id"], "tokio-tungstenite")
    tokio_source = tokio.get("source")
    if not isinstance(tokio_source, str) or not tokio_source.startswith("registry+"):
        raise ProbeError(f"tokio-tungstenite edge is not from a registry: {tokio_source!r}")
    actual = direct_package(metadata, tokio["id"], "tungstenite")
    source = actual.get("source")
    expected_source = f"git+{PATCH_URL}?rev={PATCH_REV}#{PATCH_REV}"
    if source != expected_source:
        raise ProbeError(f"tungstenite edge must use {PATCH_URL} at revision {PATCH_REV}; "
                         f"found source {source!r}")
    return {"tokio_tungstenite": {"version": tokio.get("version"), "source": tokio_source},
            "tungstenite": {"version": actual.get("version"), "source": source,
                            "url": PATCH_URL, "rev": PATCH_REV},
            "edge": "qubit-websocket -> tokio-tungstenite -> tungstenite"}


def run(websocket_arg: str, output_arg: str, *, mode: str,
        test_source_arg: str | None = None) -> int:
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
    workspace = output / "consumer"
    (workspace / "tests").mkdir(parents=True)
    if test_source_arg is None:
        source_test = websocket / "tests/fixtures/consumer/tests/handshake_tests.rs"
    else:
        source_test = Path(test_source_arg)
        if not source_test.is_absolute():
            raise ProbeError("--test-source must be an absolute path")
        source_test = source_test.resolve()
    if not source_test.is_file():
        raise ProbeError(f"test source file does not exist: {source_test}")
    source_contract = source_test.parent / "contract/handshake_contract.rs"
    if not source_contract.is_file():
        raise ProbeError(f"shared handshake contract does not exist: {source_contract}")

    backend_constraints = (f'tokio-tungstenite = "={BASELINE_VERSION}"\n'
                           f'tungstenite = "={BASELINE_VERSION}"\n'
                           if mode == "baseline" else "")
    patch_section = (f'\n[patch.crates-io]\ntungstenite = {{ git = {json.dumps(PATCH_URL)}, '
                     f'rev = {json.dumps(PATCH_REV)} }}\n' if mode == "patched" else "")
    manifest = (f'[package]\nname = "qubit-websocket-consumer-probe"\nversion = "0.1.0"\n'
                f'edition = "2024"\nrust-version = "1.94"\n\n[workspace]\n\n[dependencies]\n'
                f'qubit-websocket = {{ path = {json.dumps(str(websocket))}, default-features = false }}\n'
                f'{backend_constraints}base64 = "^0.22"\nsha1 = "^0.10"\n'
                f'tokio = {{ version = "^1.54", features = ["net", "rt-multi-thread", "macros", '
                f'"time", "io-util"] }}\n{patch_section}')
    (workspace / "Cargo.toml").write_text(manifest, encoding="utf-8")
    (output / "Cargo.toml").write_text(manifest, encoding="utf-8")
    consumer_tests = workspace / "tests"
    (consumer_tests / "contract").mkdir(parents=True)
    shutil.copy2(source_test, consumer_tests / "handshake_tests.rs")
    shutil.copy2(source_contract, consumer_tests / "contract/handshake_contract.rs")
    (output / "tests").mkdir()
    shutil.copy2(source_test, output / "tests/handshake_tests.rs")
    (output / "tests/contract").mkdir()
    shutil.copy2(source_contract, output / "tests/contract/handshake_contract.rs")
    test_digest = hashlib.sha256(source_test.read_bytes()).hexdigest()
    contract_digest = hashlib.sha256(source_contract.read_bytes()).hexdigest()
    config = output / "cargo-config.toml"
    config.write_text("", encoding="utf-8")  # Make config inheritance explicit.
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
    backend = (require_patched_backend(metadata_json, websocket=websocket) if mode == "patched" else
               require_registry_backend(metadata_json, mode=mode, websocket=websocket))
    lockfile = workspace / "Cargo.lock"
    if not lockfile.is_file():
        raise ProbeError("cargo metadata did not create the probe Cargo.lock")
    shutil.copy2(lockfile, output / "Cargo.lock")
    env = os.environ.copy()
    env["CONSUMER_MODE"] = mode
    env["CARGO_TARGET_DIR"] = str(output.with_name(f"{output.name}-target"))
    command = ["cargo", "--config", str(config), "test", "--manifest-path",
               str(workspace / "Cargo.toml"), "--locked", "--test", "handshake_tests"]
    result = subprocess.run(command, cwd=workspace, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    (output / "tests.log").write_text(result.stdout, encoding="utf-8")
    print(result.stdout, end="")
    status = ("historical limitation reproduced" if mode == "baseline" and result.returncode == 0 else
              "patched behavior verified" if mode == "patched" and result.returncode == 0 else
              "behavior verified" if mode == "current" and result.returncode == 0 else
              "handshake contract failed")
    git_root = subprocess.run(["git", "-C", str(websocket), "rev-parse", "--show-toplevel"],
                              text=True, capture_output=True, check=False)
    source_is_git_root = git_root.returncode == 0 and Path(git_root.stdout.strip()).resolve() == websocket
    if source_is_git_root:
        source_head = subprocess.run(["git", "-C", str(websocket), "rev-parse", "HEAD"],
                                     text=True, capture_output=True, check=False)
        source_status = subprocess.run(["git", "-C", str(websocket), "status", "--short"],
                                       text=True, capture_output=True, check=False)
        source_identity = {"git_root": str(websocket),
                           "head": source_head.stdout.strip() if source_head.returncode == 0 else None,
                           "dirty": bool(source_status.stdout.strip()) if source_status.returncode == 0 else None}
    else:
        source_identity = {"git_root": None, "head": None, "dirty": None}
    selected_client = next(package for package in metadata_json["packages"]
                           if package.get("name") == "qubit-websocket")
    evidence = {"mode": mode, "status": status, "backend": backend,
                "requested_crate": str(websocket),
                "manifest_path": str(Path(selected_client["manifest_path"]).resolve()),
                "metadata_command": metadata_command, "metadata_exit_code": metadata.returncode,
                "test_command": command, "handshake_tests_exit_code": result.returncode,
                "source": source_identity,
                "test_source": {"path": str(source_test), "sha256": test_digest},
                "test_sources": {
                    "handshake_tests": {"path": str(source_test), "sha256": test_digest},
                    "handshake_contract": {"path": str(source_contract),
                                           "sha256": contract_digest},
                },
                "official_source": (f"{PATCH_URL}/blob/{PATCH_REV}/src/handshake/client.rs"
                                    if mode == "patched" else
                                    "https://github.com/snapview/tungstenite-rs/blob/v"
                                    f"{backend['tungstenite']['version']}/src/handshake/client.rs"),
                "upstream_releases": "https://github.com/snapview/tungstenite-rs/releases",
                "crates_io_versions": "https://crates.io/api/v1/crates/tungstenite",
                "evidence_files": ["Cargo.toml", "Cargo.lock", "metadata.json",
                                   "metadata.stderr.log", "tests.log",
                                   "tests/handshake_tests.rs",
                                   "tests/contract/handshake_contract.rs"]}
    (output / "consumer-registry.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                                                    encoding="utf-8")
    try:
        require_success(result.returncode, "consumer handshake tests")
    except ProbeError as error:
        raise ProbeError(f"{error}; see {output / 'tests.log'} and {output / 'consumer-registry.json'}") from error
    print(f"consumer {mode} status: {status}; evidence: {output / 'consumer-registry.json'}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--websocket", required=True, help="absolute rs-websocket crate root")
    parser.add_argument("--output", required=True, help="absolute evidence directory outside source tree")
    parser.add_argument("--mode", choices=("baseline", "current", "patched"), default=None,
                        help="baseline reproduces 0.30.0; patched pins the fixed fork; current checks registry")
    parser.add_argument("--test-source", help="absolute handshake test source, for packaged crates")
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
        return run(args.websocket, args.output, mode=args.mode, test_source_arg=args.test_source)
    except (ProbeError, OSError, json.JSONDecodeError) as error:
        print(f"consumer registry check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
