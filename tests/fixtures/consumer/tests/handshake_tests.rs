// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

#[path = "contract/handshake_contract.rs"]
mod handshake_contract;

fn mode() -> String {
    let mode = std::env::var("CONSUMER_MODE").unwrap_or_else(|_| "current".to_owned());
    match mode.as_str() {
        "baseline" | "current" | "patched" => mode,
        _ => panic!("CONSUMER_MODE must be baseline, current, or patched; found {mode:?}"),
    }
}

#[tokio::test]
async fn test_independent_consumer_observes_connection_token_contract() {
    handshake_contract::assert_connection_contract(mode() == "baseline").await;
}
