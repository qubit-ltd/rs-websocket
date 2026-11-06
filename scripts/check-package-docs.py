#!/usr/bin/env python3
"""Check the published crate's user-facing documentation and local links."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


REQUIRED_DOCS = (
    "README.md",
    "README.zh_CN.md",
    "doc/user_guide.md",
    "doc/user_guide.zh_CN.md",
    "doc/architecture.md",
    "doc/architecture.zh_CN.md",
    "doc/design.zh_CN.md",
    "doc/prd.zh_CN.md",
)
LINK = re.compile(r"!?\[[^\]]*\]\(\s*(?:<([^>]+)>|([^\s)]+))[^)]*\)|!?\[[^\]]*\]\[\s*[^]]*\]")
REFERENCE = re.compile(r"^\s*\[[^]]+\]:\s*<?([^\s>]+)", re.MULTILINE)
TIMESTAMPED_DESIGN = re.compile(r"^design-\d{8}T\d{6,}(?:Z|[-_]).*\.md$")


def check(root: Path) -> list[str]:
    root = root.resolve()
    errors: list[str] = []
    for relative in REQUIRED_DOCS:
        if not (root / relative).is_file():
            errors.append(f"missing required package document: {relative}")

    doc = root / "doc"
    if doc.exists():
        for path in doc.rglob("*"):
            rel = path.relative_to(root).as_posix()
            parts = path.relative_to(root).parts
            excluded = (
                "evidence" in parts
                or any(part.startswith("review-") for part in parts)
                or any(part.startswith("implementation-plan-") for part in parts)
                or (path.is_file() and TIMESTAMPED_DESIGN.match(path.name))
            )
            if excluded:
                errors.append(f"excluded review material is present: {rel}")

    for md in sorted(root.rglob("*.md")):
        try:
            content = md.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            errors.append(f"cannot read {md.relative_to(root)}: {exc}")
            continue
        targets = [m.group(1) or m.group(2) for m in LINK.finditer(content) if m.group(1) or m.group(2)]
        targets.extend(m.group(1) for m in REFERENCE.finditer(content))
        for target in targets:
            target = target.strip().strip("\"'")
            if not target or target.startswith("#"):
                continue
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc:
                continue
            path_text = unquote(parsed.path)
            if not path_text:
                continue
            resolved = (md.parent / path_text).resolve()
            try:
                resolved.relative_to(root)
            except ValueError:
                errors.append(f"link escapes package root: {md.relative_to(root)} -> {target}")
                continue
            if not resolved.exists():
                errors.append(f"missing linked file: {md.relative_to(root)} -> {target}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path, help="unpacked crate root")
    args = parser.parse_args()
    errors = check(args.root)
    if errors:
        print("\n".join(f"ERROR: {error}" for error in errors), file=sys.stderr)
        return 1
    print("Package documentation and relative Markdown links are valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
