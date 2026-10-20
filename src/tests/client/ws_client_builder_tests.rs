// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use http::HeaderMap;
use rustls::RootCertStore;

use crate::client::WsClient;
use crate::options::WsSessionLimits;
use crate::options::WsTrustRoots;

#[test]
fn test_builder_keeps_custom_session_limits() {
    let limits = WsSessionLimits {
        inbound_messages: 2,
        ..WsSessionLimits::default()
    };
    let client = WsClient::builder().limits(limits.clone()).build().unwrap();

    assert_eq!(client.options.limits, limits);
}

#[test]
fn test_builder_keeps_connection_settings() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer test".parse().unwrap());
    let roots = RootCertStore::empty();
    let client = WsClient::builder()
        .headers(headers.clone())
        .subprotocols(vec!["chat.v1".to_owned()])
        .connect_timeout(Duration::from_secs(3))
        .handshake_timeout(Duration::from_secs(4))
        .root_certificates(roots)
        .build()
        .unwrap();

    assert_eq!(client.options.headers, headers);
    assert_eq!(client.options.subprotocols, ["chat.v1"]);
    assert_eq!(client.options.connect_timeout, Duration::from_secs(3));
    assert_eq!(client.options.handshake_timeout, Duration::from_secs(4));
    assert!(matches!(client.options.trust_roots, WsTrustRoots::Additional(_)));
}

#[test]
fn test_last_trust_root_builder_call_wins() {
    let additional = RootCertStore::empty();
    let exclusive = RootCertStore::empty();
    let client = WsClient::builder()
        .exclusive_root_certificates(exclusive)
        .root_certificates(additional)
        .build()
        .unwrap();

    assert!(matches!(client.options.trust_roots, WsTrustRoots::Additional(_)));
}
