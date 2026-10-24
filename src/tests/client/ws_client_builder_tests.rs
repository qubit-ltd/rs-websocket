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
    let client = WsClient::builder()
        .limits(limits.clone())
        .build()
        .expect("default builder settings and custom limits are valid");

    assert_eq!(client.options.limits, limits);
}

#[test]
fn test_builder_keeps_connection_settings() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        "Bearer test".parse().expect("the test authorization header is valid"),
    );
    let roots = RootCertStore::empty();
    let client = WsClient::builder()
        .headers(headers.clone())
        .subprotocols(vec!["chat.v1".to_owned()])
        .connect_timeout(Duration::from_secs(3))
        .handshake_timeout(Duration::from_secs(4))
        .root_certificates(roots)
        .build()
        .expect("the configured connection settings are valid");

    assert_eq!(client.options.headers, headers);
    assert_eq!(client.options.subprotocols, ["chat.v1"]);
    assert_eq!(client.options.connect_timeout, Duration::from_secs(3));
    assert_eq!(client.options.handshake_timeout, Duration::from_secs(4));
    assert!(
        matches!(client.options.trust_roots, WsTrustRoots::Additional(_)),
        "root_certificates should select additional trust roots"
    );
}

#[test]
fn test_last_trust_root_builder_call_wins() {
    let additional = RootCertStore::empty();
    let exclusive = RootCertStore::empty();
    let client = WsClient::builder()
        .exclusive_root_certificates(exclusive)
        .root_certificates(additional)
        .build()
        .expect("both trust-root configurations are valid");

    assert!(
        matches!(client.options.trust_roots, WsTrustRoots::Additional(_)),
        "the last trust-root builder call should select additional roots"
    );
}
