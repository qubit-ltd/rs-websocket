// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! WebSocket client infrastructure planned for Qubit Rust services.
//!
//! This crate currently contains only the repository scaffold. Its proposed
//! behavior and API are specified in the Chinese documents under `doc/`.

#[cfg(test)]
mod tests {
    #[test]
    fn test_repository_documents_define_unimplemented_scope() {
        let prd = include_str!("../doc/prd.zh_CN.md");
        let design = include_str!("../doc/design.zh_CN.md");

        assert!(prd.starts_with("# rs-websocket 产品需求文档"));
        assert!(design.starts_with("# rs-websocket 详细设计文档"));
        assert!(prd.contains("状态：待实现"));
        assert!(design.contains("状态：待实现"));
    }
}
