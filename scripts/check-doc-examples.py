#!/usr/bin/env python3
"""Compile the Rust examples marked in the bilingual Markdown documentation."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ("README.md", "README.zh_CN.md", "doc/user_guide.md", "doc/user_guide.zh_CN.md")
MARKER = re.compile(r"^<!-- doc-example: id=([A-Za-z0-9_-]+) group=([A-Za-z0-9_-]+) modes=([a-z,]+) -->$")
OPEN_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
CLOSE_FENCE = re.compile(r"^ {0,3}(`+|~+)[ \t]*$")
ALLOWED_MODES = {"default", "config"}


class ExampleError(ValueError):
    pass


@dataclass(frozen=True)
class Example:
    example_id: str
    group: str
    modes: tuple[str, ...]
    path: str
    line: int
    source: str
    language: str


def extract_blocks(markdown: str, path: str) -> list[Example]:
    lines = markdown.splitlines()
    blocks: list[Example] = []
    marker = None
    in_fence = False
    fence_char = ""
    fence_length = 0
    language = ""
    start = 0
    contents: list[str] = []
    ids: set[str] = set()
    for number, line in enumerate(lines, 1):
        if not in_fence:
            match = MARKER.fullmatch(line)
            if match:
                if marker is not None:
                    raise ExampleError(f"{path}:{number}: metadata without a Rust fence")
                modes = tuple(match.group(3).split(","))
                if not modes or len(set(modes)) != len(modes) or set(modes) - ALLOWED_MODES:
                    raise ExampleError(f"{path}:{number}: unknown mode in metadata")
                marker = (match.group(1), match.group(2), modes)
                continue
            opening = OPEN_FENCE.fullmatch(line)
            if opening and not (opening.group(1).startswith("`") and "`" in opening.group(2)):
                fence_char = opening.group(1)[0]
                fence_length = len(opening.group(1))
                language = opening.group(2).strip()
                is_rust = language == "rust" or language.startswith("rust,")
                if marker is not None and not is_rust:
                    raise ExampleError(f"{path}:{number}: Rust metadata precedes non-Rust fence {language!r}")
                in_fence, start, contents = True, number, []
            elif marker is not None and line.strip():
                raise ExampleError(f"{path}:{number}: metadata is not immediately before a fence")
        else:
            closing = CLOSE_FENCE.fullmatch(line)
            is_closing = bool(
                closing
                and closing.group(1)[0] == fence_char
                and len(closing.group(1)) >= fence_length
            )
            if not is_closing:
                contents.append(line)
                continue
            if language == "rust" or language.startswith("rust,"):
                if language not in ("rust", "rust,no_run"):
                    raise ExampleError(f"{path}:{start}: unknown Rust fence tags {language!r}")
                if marker is None:
                    raise ExampleError(f"{path}:{start}: unassigned Rust block")
                example_id, group, modes = marker
                if example_id in ids:
                    raise ExampleError(f"{path}:{start}: duplicate ID {example_id!r}")
                ids.add(example_id)
                blocks.append(Example(example_id, group, modes, path, start, "\n".join(contents) + "\n", language))
                marker = None
            in_fence = False
    if in_fence:
        kind = "Rust " if language == "rust" or language.startswith("rust,") else ""
        raise ExampleError(f"{path}:{start}: unterminated {kind}fence")
    if marker is not None:
        raise ExampleError(f"{path}: metadata without a Rust fence")
    return blocks


def check_bilingual_sets(documents: dict[str, list[Example]]) -> None:
    english = {b.example_id for b in documents["README.md"] + documents["doc/user_guide.md"]}
    chinese = {b.example_id for b in documents["README.zh_CN.md"] + documents["doc/user_guide.zh_CN.md"]}
    if english != chinese:
        raise ExampleError(f"bilingual Rust block IDs differ: English-only={sorted(english-chinese)}, Chinese-only={sorted(chinese-english)}")


def load_manifest(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("documents") != list(DOCS) or not isinstance(data.get("groups"), dict):
        raise ExampleError("manifest must list the four required documents and a groups object")
    return data


def find_config_path(root: Path) -> Path:
    candidates = [root.parent / "rs-config"]
    if len(root.parents) >= 3:
        candidates.append(root.parents[2] / "rs-config")
    for candidate in candidates:
        if (candidate / "Cargo.toml").is_file():
            return candidate.resolve()
    searched = ", ".join(str(path.resolve()) for path in candidates)
    raise ExampleError(f"config examples require an rs-config checkout beside the project or workspace; searched: {searched}")


def check_manifest(documents: dict[str, list[Example]], manifest: dict) -> None:
    actual = {(b.path, b.example_id): b for blocks in documents.values() for b in blocks}
    declared = manifest.get("examples", [])
    expected = {(item["document"], item["id"]): item for item in declared}
    if actual.keys() != expected.keys():
        raise ExampleError(f"manifest block set differs: missing={sorted(actual.keys()-expected.keys())}, stale={sorted(expected.keys()-actual.keys())}")
    for key, block in actual.items():
        item = expected[key]
        if item["group"] != block.group or tuple(item["modes"]) != block.modes:
            raise ExampleError(f"{block.path}:{block.line}: ID {block.example_id} metadata differs from manifest")
        if block.group not in manifest["groups"]:
            raise ExampleError(f"{block.path}:{block.line}: unknown group {block.group}")
    if set(documents) != set(DOCS):
        raise ExampleError("missing required document")


def compile_mode(documents: dict[str, list[Example]], manifest: dict, mode: str, output: Path | None) -> int:
    groups: dict[str, list[Example]] = {}
    for language, paths in (("en", ("README.md", "doc/user_guide.md")), ("zh", ("README.zh_CN.md", "doc/user_guide.zh_CN.md"))):
        for path in paths:
            for block in documents[path]:
                if mode in block.modes:
                    groups.setdefault(f"{block.group}_{language}", []).append(block)
    failures = 0
    with tempfile.TemporaryDirectory(prefix="qubit-doc-examples-") as tmp:
        temp_root = Path(tmp)
        for group_name, blocks in groups.items():
            original_group = group_name.rsplit("_", 1)[0]
            options = manifest["groups"][original_group]
            crate = temp_root / group_name
            crate.mkdir()
            config_feature = mode == "config"
            needs_qubit_config = options.get("config", False)
            client_path = ROOT
            config_path = find_config_path(ROOT) if config_feature else None
            if config_feature:
                # This worktree sits below .worktrees, while the workspace sibling
                # dependency lives beside the source checkout. Stage current client
                # files and retarget only its local config path in the disposable copy.
                client_path = temp_root / f"qubit-websocket-config-source-{group_name}"
                shutil.copytree(ROOT, client_path, ignore=shutil.ignore_patterns(".git", "target"))
                client_manifest = client_path / "Cargo.toml"
                manifest_text = client_manifest.read_text(encoding="utf-8")
                manifest_text = manifest_text.replace('path = "../rs-config"', f'path = "{config_path}"')
                client_manifest.write_text(manifest_text, encoding="utf-8")
            dependencies = [f'qubit-websocket = {{ path = "{client_path}", features = ["config"] }}' if config_feature else f'qubit-websocket = {{ path = "{ROOT}" }}']
            for name, dependency in options.get("dependencies", {}).items():
                if isinstance(dependency, str):
                    dependencies.append(f'{name} = "{dependency}"')
                else:
                    features = dependency.get("features", [])
                    suffix = ", features = " + json.dumps(features) if features else ""
                    dependencies.append(f'{name} = {{ version = "{dependency["version"]}"{suffix} }}')
            if needs_qubit_config:
                dependencies.append(f'qubit-config = {{ path = "{config_path}" }}')
            (crate / "Cargo.toml").write_text('[package]\nname = "doc-' + group_name + '"\nversion = "0.0.0"\nedition = "2024"\n\n[dependencies]\n' + "\n".join(dependencies) + "\n", encoding="utf-8")
            source = "\n".join(f"// {b.path}:{b.line} [{b.example_id}]\n{b.source}" for b in blocks)
            (crate / "src").mkdir()
            (crate / "src/lib.rs").write_text(source, encoding="utf-8")
            if output:
                dest = output / mode / group_name
                dest.mkdir(parents=True, exist_ok=True)
                shutil.copytree(crate, dest, dirs_exist_ok=True)
                mapping = {"client_source": str(ROOT), "config_source": str(config_path) if config_feature else None, "examples": [b.__dict__ for b in blocks]}
                (dest / "mapping.json").write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
            cmd = ["cargo", "check", "--manifest-path", str(crate / "Cargo.toml"), "--target-dir", str(temp_root / "target")]
            result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
            print(f"[{mode}] group {group_name}: cargo exit {result.returncode}")
            if result.returncode:
                failures += 1
                for block in blocks:
                    print(f"  source {block.path}, block {block.example_id}, line {block.line}")
                print(result.stdout)
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("default", "config"), required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        docs = {doc: extract_blocks((ROOT / doc).read_text(encoding="utf-8"), doc) for doc in DOCS}
        manifest = load_manifest(ROOT / "scripts/doc-examples.json")
        check_manifest(docs, manifest)
        check_bilingual_sets(docs)
        output = args.output.resolve() if args.output else None
        if output:
            output.mkdir(parents=True, exist_ok=True)
        return int(bool(compile_mode(docs, manifest, args.mode, output)))
    except (OSError, json.JSONDecodeError, ExampleError) as error:
        print(f"doc example validation failed: {error}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
