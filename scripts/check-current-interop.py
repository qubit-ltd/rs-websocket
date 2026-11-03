#!/usr/bin/env python3
"""Verify WS and WSS interoperability using disposable copies of current source trees."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib

TUNGSTENITE_URL = "https://github.com/qubit-ltd/tungstenite-rs"
TUNGSTENITE_REV = "b24a4442be0247a71fe9ee4e520eb483ac1b2873"
TUNGSTENITE_SOURCE = f"git+{TUNGSTENITE_URL}?rev={TUNGSTENITE_REV}#{TUNGSTENITE_REV}"
WEB_GIT_URL = "https://github.com/qubit-ltd/rs-web"
WEBSOCKET_GIT_URL = "https://github.com/qubit-ltd/rs-websocket"


class InteropError(RuntimeError):
    """The supplied sources or resolved Cargo graph do not meet the contract."""


def _within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _read_manifest(manifest: Path) -> dict:
    try:
        with manifest.open("rb") as stream:
            return tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise InteropError(f"cannot read Cargo manifest {manifest}: {error}") from error


def validate_paths(web_arg: str, websocket_arg: str, output_arg: str) -> tuple[Path, Path, Path]:
    raw = [Path(value) for value in (web_arg, websocket_arg, output_arg)]
    if any(not path.is_absolute() for path in raw):
        raise InteropError("--web, --websocket, and --output must be absolute paths")
    web_raw, websocket_raw, output_raw = raw
    if output_raw.is_symlink():
        raise InteropError(f"output directory must not be a symbolic link: {output_raw}")
    web, websocket = web_raw.resolve(), websocket_raw.resolve()
    output = output_raw.resolve()
    if not web.is_dir() or not websocket.is_dir():
        raise InteropError("--web and --websocket must name existing directories")
    if web == websocket or _within(web, websocket) or _within(websocket, web):
        raise InteropError(f"source trees overlap: {web} ; {websocket}")
    if _within(output, web) or _within(output, websocket):
        raise InteropError("--output must be outside both source trees")
    for source in (web, websocket):
        manifest = source / "Cargo.toml"
        if not manifest.is_file():
            raise InteropError(f"Cargo.toml is missing: {manifest}")
    if _read_manifest(web / "Cargo.toml").get("package", {}).get("name") != "qubit-web":
        raise InteropError("--web Cargo.toml package.name must be qubit-web")
    if _read_manifest(websocket / "Cargo.toml").get("package", {}).get("name") != "qubit-websocket":
        raise InteropError("--websocket Cargo.toml package.name must be qubit-websocket")
    return web, websocket, output


def assert_direct_dependency(metadata: dict, consumer_name: str, consumer_manifest: Path,
                             dependency_name: str, expected_manifest: Path) -> Path:
    """Require exactly one direct dependency edge to the expected local package."""
    packages = {package.get("id"): package for package in metadata.get("packages", [])}
    consumers = [package for package in metadata.get("packages", [])
                 if package.get("name") == consumer_name
                 and Path(package.get("manifest_path", "")).resolve() == consumer_manifest.resolve()]
    if len(consumers) != 1:
        raise InteropError(f"metadata did not identify one {consumer_name} at {consumer_manifest}")
    consumer_id = consumers[0].get("id")
    nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}
    node = nodes.get(consumer_id)
    if node is None:
        raise InteropError(f"metadata resolve graph has no node for {consumer_name}")
    edge_ids = {dependency.get("pkg") for dependency in node.get("deps", [])
                if dependency.get("name", "").replace("-", "_") == dependency_name.replace("-", "_")}
    matches = [packages[package_id] for package_id in edge_ids
               if package_id in packages and packages[package_id].get("name") == dependency_name]
    if len(matches) != 1:
        raise InteropError(f"{consumer_name} direct {dependency_name} edge did not resolve uniquely")
    package = matches[0]
    actual = Path(package.get("manifest_path", "")).resolve()
    if actual != expected_manifest.resolve() or package.get("source") is not None:
        raise InteropError(f"{consumer_name} {dependency_name} resolved to manifest={actual}, "
                           f"source={package.get('source')!r}; expected local {expected_manifest.resolve()}")
    return actual


def assert_tungstenite_patch(metadata: dict, websocket_manifest: Path) -> dict:
    """Check the current client's tokio-tungstenite 0.30 edge reaches the pinned Git backend."""
    packages = {package.get("id"): package for package in metadata.get("packages", [])}
    clients = [package for package in metadata.get("packages", [])
               if package.get("name") == "qubit-websocket"
               and Path(package.get("manifest_path", "")).resolve() == websocket_manifest.resolve()]
    if len(clients) != 1:
        raise InteropError(f"metadata did not identify one qubit-websocket at {websocket_manifest}")
    nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}

    def edge_packages(node: dict, dependency_name: str) -> list[dict]:
        ids = {dependency.get("pkg") for dependency in node.get("deps", [])
               if dependency.get("name", "").replace("-", "_") == dependency_name.replace("-", "_")}
        return [packages[package_id] for package_id in ids if package_id in packages]

    client_id = clients[0].get("id")
    client_node = nodes.get(client_id)
    if client_node is None:
        raise InteropError("metadata resolve graph has no node for qubit-websocket")
    tokio_candidates = [package for package in edge_packages(client_node, "tokio-tungstenite")
                        if package.get("name") == "tokio-tungstenite"
                        and str(package.get("version", "")).startswith("0.30.")]
    if len(tokio_candidates) != 1:
        raise InteropError("qubit-websocket must have one direct tokio-tungstenite 0.30 edge; "
                           f"found {tokio_candidates!r}")
    tokio_package = tokio_candidates[0]
    tokio_node = nodes.get(tokio_package.get("id"))
    if tokio_node is None:
        raise InteropError("metadata resolve graph has no node for tokio-tungstenite 0.30")
    tungstenite_candidates = [package for package in edge_packages(tokio_node, "tungstenite")
                              if package.get("name") == "tungstenite"]
    matching = [package for package in tungstenite_candidates
                if package.get("source") == TUNGSTENITE_SOURCE]
    if len(matching) != 1:
        observed = [{"id": package.get("id"), "version": package.get("version"),
                     "source": package.get("source")} for package in tungstenite_candidates]
        raise InteropError("tokio-tungstenite 0.30 tungstenite edge must resolve to exact Git source "
                           f"{TUNGSTENITE_SOURCE!r}; found {observed}")
    backend = matching[0]
    return {"consumer": "qubit-websocket", "consumer_manifest": str(websocket_manifest.resolve()),
            "tokio_tungstenite": {"id": tokio_package.get("id"), "version": tokio_package.get("version")},
            "edge": "qubit-websocket -> tokio-tungstenite 0.30 -> tungstenite",
            "tungstenite": {"id": backend.get("id"), "version": backend.get("version"),
                            "source": backend.get("source")},
            "expected_source": TUNGSTENITE_SOURCE,
            "coexisting_tungstenite": [{"id": package.get("id"), "version": package.get("version"),
                                         "source": package.get("source")}
                                        for package in packages.values()
                                        if package.get("name") == "tungstenite"
                                        and package.get("id") != backend.get("id")]}


def _git(directory: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=directory, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def _source_paths(directory: Path) -> list[Path]:
    names = _git(directory, "ls-files", "-co", "--exclude-standard", "-z").split("\0")
    return sorted(Path(name) for name in names if name
                  and "target" not in Path(name).parts
                  and "__pycache__" not in Path(name).parts
                  and Path(name).suffix != ".pyc")


def source_state(directory: Path) -> dict:
    revision = _git(directory, "rev-parse", "HEAD")
    status = _git(directory, "status", "--porcelain", "--untracked-files=all")
    paths = _source_paths(directory)
    digest = hashlib.sha256()
    for relative in paths:
        path = directory / relative
        digest.update(relative.as_posix().encode("utf-8") + b"\0")
        if path.is_symlink():
            digest.update(b"symlink\0" + os.readlink(path).encode("utf-8"))
        elif path.is_file():
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
        else:
            digest.update(b"missing")
        digest.update(b"\0")
    return {"sha": revision, "dirty": bool(status), "content_sha256": digest.hexdigest()}


def _repo_label(repository: Path) -> str:
    manifest = _read_manifest(repository / "Cargo.toml")
    package = manifest.get("package", {}).get("name", repository.name)
    known = {"qubit-web": "rs-web", "qubit-websocket": "rs-websocket",
             "qubit-config": "rs-config", "qubit-redact": "rs-redact",
             "qubit-json": "rs-json", "qubit-budget": "rs-budget"}
    return known.get(package, repository.name)


def _manifest_path_dependencies(repository: Path, logical_root: Path):
    """Yield external local path dependencies referenced by manifests in a repository."""
    for manifest in repository.rglob("Cargo.toml"):
        if any(part in {"target", "build", ".git"} for part in manifest.parts):
            continue
        try:
            data = _read_manifest(manifest)
        except InteropError:
            continue
        sections = [data.get("dependencies", {}), data.get("dev-dependencies", {}),
                    data.get("build-dependencies", {})]
        sections.extend(data.get("workspace", {}).get(key, {})
                        for key in ("dependencies", "dev-dependencies"))
        for section in sections:
            if not isinstance(section, dict):
                continue
            for dependency in section.values():
                if isinstance(dependency, dict) and isinstance(dependency.get("path"), str):
                    relative_manifest_dir = manifest.parent.relative_to(repository)
                    target = (logical_root / relative_manifest_dir / dependency["path"]).resolve()
                    if target.is_dir() and not _within(target, logical_root.resolve()):
                        yield target


def _snapshot(repository: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    state = source_state(repository)
    for relative in _source_paths(repository):
        name = relative.as_posix()
        source = repository / name
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_symlink():
            resolved = source.resolve(strict=True)
            if not _within(resolved, repository.resolve()):
                raise InteropError(f"source contains escaping symlink: {source} -> {resolved}")
            target.symlink_to(os.readlink(source))
        elif source.is_file():
            shutil.copy2(source, target)
    if not (destination / "Cargo.toml").is_file():
        raise InteropError(f"snapshot is missing Cargo.toml: {destination}")
    return destination


def _prepare_copies(web: Path, websocket: Path, destination: Path) -> tuple[Path, Path]:
    common_root = web.parent
    repositories: dict[Path, Path] = {web.resolve(): web, websocket.resolve(): websocket}
    pending = list(repositories.values())
    while pending:
        repo = pending.pop()
        logical_repo = common_root / _repo_label(repo)
        for dependency in _manifest_path_dependencies(repo, logical_repo):
            # Dependencies are repository roots in the rust-common sibling layout.
            root = dependency
            while not (root / ".git").exists() and root.parent != common_root.parent:
                root = root.parent
            if root.parent != common_root:
                raise InteropError(f"path dependency is outside shared source root: {dependency}")
            if root.resolve() not in repositories:
                repositories[root.resolve()] = root
                pending.append(root)
    copies = {}
    for original in repositories.values():
        copies[original.resolve()] = _snapshot(original, destination / _repo_label(original))
    return copies[web.resolve()], copies[websocket.resolve()]


def _replace_dependency(manifest: Path, section_name: str, package_name: str,
                        dependency: dict) -> None:
    # Preserve the rest of the manifest byte-for-byte and replace only one TOML line.
    text = manifest.read_text(encoding="utf-8")
    section = None
    found = False
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1]
        if section == section_name and stripped.startswith(package_name + " ="):
            fields = []
            for key, value in dependency.items():
                if isinstance(value, bool):
                    rendered = str(value).lower()
                elif isinstance(value, list):
                    rendered = "[" + ", ".join(json.dumps(item) for item in value) + "]"
                else:
                    rendered = json.dumps(value)
                fields.append(f"{key} = {rendered}")
            lines[index] = package_name + " = { " + ", ".join(fields) + " }\n"
            found = True
            break
    if not found:
        raise InteropError(f"could not locate [{section_name}] {package_name} in {manifest}")
    manifest.write_text("".join(lines), encoding="utf-8")


def _metadata(cwd: Path, config: Path, command_log: list[dict]) -> dict:
    command = ["cargo", "--config", str(config), "metadata", "--format-version", "1"]
    result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, check=False)
    command_log.append({"argv": command, "cwd": str(cwd), "exit_code": result.returncode,
                        "stderr": result.stderr})
    if result.returncode:
        raise InteropError(f"dependency resolution failed (cargo metadata, exit {result.returncode}):\n{result.stderr}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise InteropError(f"cargo metadata returned invalid JSON: {error}") from error


def _run_test(command: list[str], cwd: Path, target: Path, config: Path,
              command_log: list[dict]) -> tuple[int, str | None]:
    full = ["cargo", "--config", str(config), *command]
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(target)
    result = subprocess.run(full, cwd=cwd, env=environment, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    combined = result.stdout + "\n" + result.stderr
    infrastructure_markers = ("failed to download", "failed to fetch", "failed to get",
                               "failed to load source", "could not resolve proxy", "network failure")
    classification = None
    if result.returncode:
        classification = ("dependency_infrastructure" if any(marker in combined.lower()
                          for marker in infrastructure_markers) else "interop_test_failure")
    command_log.append({"argv": full, "cwd": str(cwd), "exit_code": result.returncode,
                        "failure_classification": classification,
                        "stdout": result.stdout[-6000:], "stderr": result.stderr[-6000:]})
    return result.returncode, classification


def run(web_arg: str, websocket_arg: str, output_arg: str) -> int:
    web, websocket, output = validate_paths(web_arg, websocket_arg, output_arg)
    output.mkdir(parents=True, exist_ok=True)
    web_before, websocket_before = source_state(web), source_state(websocket)
    commands: list[dict] = []
    result_doc = {"web": {**web_before, "path": str(web)},
                  "websocket": {**websocket_before, "path": str(websocket)},
                  "protocol_base": {"tungstenite_git": TUNGSTENITE_URL, "revision": TUNGSTENITE_REV,
                                    "expected_metadata_source": TUNGSTENITE_SOURCE},
                  "commands": commands, "scenarios": {
                      "ws": ["authorization", "origin", "text_and_binary_exchange", "message_limits",
                             "client_close", "server_close", "graceful_shutdown"],
                      "wss": ["trusted_test_ca", "certificate_identity", "untrusted_ca_rejected",
                              "text_exchange", "session_close", "graceful_shutdown"]}}
    error = None
    exit_code = 1
    try:
        with tempfile.TemporaryDirectory(prefix="qubit-current-interop-") as temp:
            temp_root = Path(temp)
            copied_web, copied_websocket = _prepare_copies(web, websocket, temp_root / "sources")
            # Re-point only disposable manifests. The checked-out manifests and lockfiles stay read-only.
            client_manifest = copied_websocket / "Cargo.toml"
            _replace_dependency(client_manifest, "dev-dependencies", "qubit-web",
                                {"path": os.path.relpath(copied_web, copied_websocket),
                                 "default-features": False, "features": ["ws"]})
            fixture = copied_web / "tests/fixtures/wss_consumer"
            fixture_manifest = fixture / "Cargo.toml"
            _replace_dependency(fixture_manifest, "dependencies", "qubit-websocket",
                                {"path": os.path.relpath(copied_websocket, fixture),
                                 "default-features": False})
            with fixture_manifest.open("a", encoding="utf-8") as stream:
                stream.write(f'\n[patch.crates-io]\ntungstenite = {{ git = "{TUNGSTENITE_URL}", '
                             f'rev = "{TUNGSTENITE_REV}" }}\n')
            config = temp_root / "cargo-config.toml"
            config.write_text(
                f'[patch."{WEB_GIT_URL}"]\nqubit-web = {{ path = {json.dumps(str(copied_web))} }}\n'
                f'[patch."{WEBSOCKET_GIT_URL}"]\nqubit-websocket = {{ path = {json.dumps(str(copied_websocket))} }}\n',
                encoding="utf-8")
            for root, consumer, expected in ((copied_websocket, "qubit-websocket", copied_web),
                                               (fixture, "qubit-web-wss-consumer", copied_web)):
                metadata = _metadata(root, config, commands)
                assert_direct_dependency(metadata, consumer, root / "Cargo.toml", "qubit-web", expected / "Cargo.toml")
                if consumer == "qubit-web-wss-consumer":
                    assert_direct_dependency(metadata, consumer, root / "Cargo.toml", "qubit-websocket",
                                             copied_websocket / "Cargo.toml")
                protocol_check = assert_tungstenite_patch(metadata, copied_websocket / "Cargo.toml")
                result_doc.setdefault("protocol_checks", []).append({"root": str(root), **protocol_check})
                result_doc.setdefault("resolved_manifests", []).append({
                    "root": str(root), "consumer": consumer,
                    "qubit-web": str(expected / "Cargo.toml"),
                    "qubit-websocket": str(copied_websocket / "Cargo.toml") if consumer.endswith("consumer") else None})
                result_doc.setdefault("metadata", []).append({"root": str(root),
                    "packages": [{"name": p.get("name"), "manifest_path": p.get("manifest_path"),
                                  "source": p.get("source")} for p in metadata.get("packages", [])
                                 if p.get("name") in {"qubit-web", "qubit-websocket", "tungstenite"}]})
            target = temp_root / "target"
            test_results = []
            test_results.append(_run_test(["test", "--no-default-features", "--test", "web_interop_tests", "--locked"],
                                          copied_websocket, target, config, commands))
            test_results.append(_run_test(["test", "--manifest-path", str(fixture_manifest), "--locked"],
                                          copied_web, target, config, commands))
            codes = [code for code, _ in test_results]
            result_doc["test_exit_codes"] = codes
            failures = [classification for code, classification in test_results if code]
            if failures:
                result_doc["failure_classification"] = failures[0]
            exit_code = next((code for code in codes if code), 0)
    except (InteropError, OSError) as failure:
        error = str(failure)
        result_doc["error"] = {"classification": "dependency_or_source_resolution", "message": error}
        print(f"current interop check failed: {error}", file=sys.stderr)
    result_doc["web_after"] = source_state(web)
    result_doc["websocket_after"] = source_state(websocket)
    result_doc["input_trees_unchanged"] = (result_doc["web"] == {**result_doc["web_after"], "path": str(web)}
                                           and result_doc["websocket"] == {**result_doc["websocket_after"], "path": str(websocket)})
    evidence = output / "current-interop.json"
    evidence.write_text(json.dumps(result_doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"evidence: {evidence}")
    if not result_doc["input_trees_unchanged"]:
        print("input source tree changed during validation", file=sys.stderr)
        return 1
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--web", required=True, help="absolute current rs-web checkout")
    parser.add_argument("--websocket", required=True, help="absolute current rs-websocket checkout")
    parser.add_argument("--output", required=True, help="absolute evidence directory outside source trees")
    args = parser.parse_args(argv)
    try:
        return run(args.web, args.websocket, args.output)
    except InteropError as error:
        print(f"current interop check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
