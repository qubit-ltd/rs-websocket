// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use http::HeaderMap;
use qubit_websocket::WsClient;
use qubit_websocket::WsClientOptions;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsSessionLimits;

#[test]
fn test_builder_accepts_custom_session_limits() {
    let limits = WsSessionLimits {
        inbound_messages: 2,
        ..WsSessionLimits::default()
    };

    assert!(
        WsClient::builder().limits(limits).build().is_ok(),
        "custom session limits must be accepted"
    );
}

#[test]
fn test_builder_rejects_zero_session_limits() {
    let limits = WsSessionLimits {
        inbound_messages: 0,
        ..WsSessionLimits::default()
    };
    let error = match WsClient::builder().limits(limits).build() {
        Ok(_) => panic!("zero session limits must be rejected"),
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        WsErrorKind::InvalidConfig,
        "zero session limits must produce an invalid configuration error"
    );
}

#[test]
fn test_builder_rejects_reserved_default_headers() {
    for name in ["host", "connection", "upgrade", "sec-websocket-protocol"] {
        let mut headers = HeaderMap::new();
        headers.insert(name, "test".parse().unwrap());
        let error = match WsClient::builder().headers(headers).build() {
            Ok(_) => panic!("reserved header {name} must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), WsErrorKind::InvalidConfig, "header: {name}");
    }

    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer test".parse().unwrap());
    assert!(
        WsClient::builder().headers(headers).build().is_ok(),
        "authorization headers must be accepted"
    );
}

#[test]
fn test_builder_accepts_public_connection_settings() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer test".parse().unwrap());
    let client = WsClient::builder()
        .headers(headers)
        .subprotocols(vec!["chat.v1".to_owned()])
        .connect_timeout(Duration::from_secs(3))
        .handshake_timeout(Duration::from_secs(4))
        .root_certificates(rustls::RootCertStore::empty())
        .exclusive_root_certificates(rustls::RootCertStore::empty())
        .limits(WsSessionLimits::default())
        .build();

    assert!(client.is_ok(), "public connection settings must be accepted");
}

#[test]
fn test_builder_rejects_duplicate_subprotocols() {
    let error = match WsClient::builder()
        .subprotocols(vec!["chat.v1".to_owned(), "chat.v1".to_owned()])
        .build()
    {
        Ok(_) => panic!("duplicate subprotocols must be rejected"),
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        WsErrorKind::InvalidConfig,
        "duplicate subprotocols must produce an invalid configuration error"
    );
}

#[test]
fn test_client_options_defaults_validate() {
    assert!(
        WsClientOptions::default().validate().is_ok(),
        "default client options must validate"
    );
}
