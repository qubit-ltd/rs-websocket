// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fs;
use std::path::Path;

fn current_version() -> String {
    format!(
        "{}.{}",
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR")
    )
}

fn assert_dependency_version(path: &Path, expected: &str) {
    let document = fs::read_to_string(path).expect("read current project documentation");
    let dependency_lines: Vec<&str> = document
        .lines()
        .filter(|line| line.contains("qubit-websocket ="))
        .collect();

    assert!(
        !dependency_lines.is_empty(),
        "{} has no dependency snippet",
        path.display()
    );
    for line in dependency_lines {
        assert!(
            line.contains(&format!("\"{expected}\"")),
            "{} declares qubit-websocket with a stale version: {line}",
            path.display()
        );
    }
}

fn assert_current_version(path: &Path, expected: &str) {
    let document = fs::read_to_string(path).expect("read current project documentation");
    let declaration = document
        .lines()
        .find(|line| line.contains("`qubit-websocket`"))
        .and_then(|line| {
            line.split("`qubit-websocket` ")
                .nth(1)
                .and_then(|version_and_text| version_and_text.split_whitespace().next())
        });

    assert!(
        declaration == Some(expected),
        "{} does not declare current qubit-websocket version {expected}; found {declaration:?}",
        path.display()
    );
}

fn markdown_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root).expect("read project root") {
        let path = entry.expect("read root entry").path();
        if path.is_file()
            && path.file_name().and_then(|name| name.to_str()).is_some_and(|name| {
                name.starts_with("README") && name.ends_with(".md")
            })
        {
            files.push(path);
        }
    }
    let doc = root.join("doc");
    for entry in fs::read_dir(&doc).expect("read doc directory") {
        let path = entry.expect("read doc entry").path();
        if path.is_file() && path.extension().is_some_and(|extension| extension == "md") {
            files.push(path);
        }
    }
    files.sort();
    files
}

// This project uses inline Markdown links (`[label](destination)`). Reference
// links are outside this check's supported syntax and are not used by the
// scanned README or `doc/*.md` files.
fn local_markdown_targets(line: &str) -> Vec<&str> {
    let mut targets = Vec::new();
    let mut cursor = 0;
    let mut in_code = false;
    while cursor < line.len() {
        let rest = &line[cursor..];
        if rest.starts_with('`') {
            in_code = !in_code;
            cursor += 1;
        } else if !in_code && rest.starts_with("](") {
            let start = cursor + 2;
            if let Some(end) = line[start..].find(')') {
                let raw = line[start..start + end].trim();
                let destination = raw
                    .strip_prefix('<')
                    .and_then(|value| value.split_once('>').map(|(path, _)| path))
                    .unwrap_or_else(|| raw.split_whitespace().next().unwrap_or(raw));
                let is_url = destination
                    .split_once(':')
                    .is_some_and(|(scheme, _)| {
                        !scheme.is_empty()
                            && scheme
                                .chars()
                                .enumerate()
                                .all(|(index, ch)| {
                                    ch.is_ascii_alphanumeric()
                                        || (index > 0 && matches!(ch, '+' | '.' | '-'))
                                })
                    });
                if !destination.is_empty() && !destination.starts_with('#') && !is_url {
                    targets.push(destination.split('#').next().unwrap_or(destination));
                }
                cursor = start + end + 1;
            } else {
                cursor += 2;
            }
        } else {
            cursor += rest.chars().next().expect("nonempty remainder").len_utf8();
        }
    }
    targets
}

#[test]
fn test_local_markdown_links_resolve() {
    assert_eq!(
        local_markdown_targets("[`label`](target.md)"),
        ["target.md"],
        "inline code in a link label must not suppress its destination"
    );

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut errors = Vec::new();
    for path in markdown_files(root) {
        let document = fs::read_to_string(&path).expect("read Markdown document");
        let mut fence: Option<char> = None;
        for (index, line) in document.lines().enumerate() {
            let trimmed = line.trim_start();
            if let Some(marker) = trimmed.chars().next().filter(|ch| *ch == '`' || *ch == '~') {
                let count = trimmed.chars().take_while(|ch| *ch == marker).count();
                if count >= 3 {
                    if fence == Some(marker) {
                        fence = None;
                    } else if fence.is_none() {
                        fence = Some(marker);
                    }
                    continue;
                }
            }
            if fence.is_some() {
                continue;
            }
            for target in local_markdown_targets(line) {
                if !path
                    .parent()
                    .expect("document parent")
                    .join(target)
                    .exists()
                {
                    errors.push(format!(
                        "{}:{}: missing local Markdown target {target}",
                        path.display(),
                        index + 1
                    ));
                }
            }
        }
    }
    assert!(
        errors.is_empty(),
        "broken local Markdown links:\n{}",
        errors.join("\n")
    );
}

#[test]
fn current_documentation_matches_package_version() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let version = current_version();

    for relative in [
        "README.md",
        "README.zh_CN.md",
        "doc/user_guide.md",
        "doc/user_guide.zh_CN.md",
    ] {
        assert_dependency_version(&root.join(relative), &version);
    }

    for relative in [
        "doc/user_guide.md",
        "doc/user_guide.zh_CN.md",
        "doc/architecture.md",
        "doc/architecture.zh_CN.md",
    ] {
        assert_current_version(&root.join(relative), &version);
    }
}
