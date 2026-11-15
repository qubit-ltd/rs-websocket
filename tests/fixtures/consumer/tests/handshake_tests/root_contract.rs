// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod handshake_contract;

#[tokio::test]
async fn test_vendor_observes_full_connection_token_contract() {
    handshake_contract::assert_connection_contract(false).await;
}
