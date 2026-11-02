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
