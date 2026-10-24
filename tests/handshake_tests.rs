// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use http::HeaderMap;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;

#[test]
fn test_builder_rejects_invalid_subprotocol_tokens() {
    for subprotocols in [
        vec!["chat".to_owned(), "chat".to_owned()],
        vec!["chat,superchat".to_owned()],
        vec!["chat protocol".to_owned()],
        vec!["chat\nprotocol".to_owned()],
    ] {
        assert!(
            WsClient::builder().subprotocols(subprotocols).build().is_err(),
            "invalid subprotocol tokens must be rejected"
        );
    }
}

#[tokio::test]
async fn test_connect_rejects_reserved_connection_headers_before_connecting() {
    let client = WsClient::builder().build().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap());

    let error = match client.connect_with("ws://127.0.0.1:1/", headers).await {
        Ok(_) => panic!("reserved connection header must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
}

#[tokio::test]
async fn test_connect_rejects_malformed_ipv6_authorities_as_invalid_url() {
    let client = WsClient::builder().build().unwrap();
    for url in ["ws://[not-ipv6]/", "ws://[::1", "ws://[::1]suffix/"] {
        let error = match client.connect(url).await {
            Ok(_) => panic!("malformed IPv6 authority must be rejected: {url}"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), WsErrorKind::InvalidUrl, "url: {url}");
    }
}
