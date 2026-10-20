// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

#![cfg(feature = "config")]

use std::time::Duration;

use qubit_config::Config;
use qubit_config::ConfigError;
use qubit_websocket::WsClientBuilder;
use qubit_websocket::WsErrorKind;
use qubit_websocket::config_keys;

#[test]
fn test_config_maps_limits_and_allows_later_builder_override() {
    let mut config = Config::new();
    config
        .set(format!("websocket.{}", config_keys::CONNECT_TIMEOUT_MS), 3000_u64)
        .unwrap();
    config
        .set(format!("websocket.{}", config_keys::HANDSHAKE_TIMEOUT_MS), 4000_u64)
        .unwrap();
    config
        .set(format!("websocket.{}", config_keys::INBOUND_MESSAGES), 8_u64)
        .unwrap();
    config
        .set(format!("websocket.{}", config_keys::CLOSE_TIMEOUT_MS), 600_u64)
        .unwrap();
    let builder = WsClientBuilder::from_config(&config, "websocket").unwrap();
    let client = builder.connect_timeout(Duration::from_secs(5)).build();
    assert!(client.is_ok());
}

#[test]
fn test_config_rejects_unknown_field() {
    let mut config = Config::new();
    config.set("websocket.connect_timeout_mss", 3000_u64).unwrap();
    let result = WsClientBuilder::from_config(&config, "websocket");
    assert!(matches!(result, Err(ConfigError::UnknownProperties { .. })));
}

#[test]
fn test_config_preserves_zero_for_builder_validation() {
    let mut config = Config::new();
    config
        .set(format!("websocket.{}", config_keys::OUTBOUND_MESSAGES), 0_u64)
        .unwrap();
    let builder = WsClientBuilder::from_config(&config, "websocket").unwrap();
    let error = match builder.build() {
        Ok(_) => panic!("zero outbound message limit must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
}

#[test]
fn test_config_rejects_inconsistent_limits_during_build() {
    let mut config = Config::new();
    config
        .set(format!("websocket.{}", config_keys::MAX_INBOUND_MESSAGE_BYTES), 5_u64)
        .unwrap();
    let builder = WsClientBuilder::from_config(&config, "websocket").unwrap();
    let error = match builder.build() {
        Ok(_) => panic!("inconsistent inbound limits must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
}

#[test]
fn test_config_requires_a_nonempty_selected_subtree() {
    let mut config = Config::new();
    config.set("service.name", "worker").unwrap();

    assert!(matches!(
        WsClientBuilder::from_config(&config, "websocket"),
        Err(ConfigError::PropertyNotFound(path)) if path == "websocket"
    ));
    assert!(matches!(
        WsClientBuilder::from_config(&Config::new(), ""),
        Err(ConfigError::PropertyNotFound(path)) if path.is_empty()
    ));
}

#[test]
fn test_config_accepts_empty_prefix_for_root_configuration() {
    let mut config = Config::new();
    config.set(config_keys::CONNECT_TIMEOUT_MS, 0_u64).unwrap();

    let builder = WsClientBuilder::from_config(&config, "").unwrap();
    let error = match builder.build() {
        Ok(_) => panic!("zero root connection timeout must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
}

#[test]
fn test_config_reports_sorted_root_paths_for_unknown_nested_keys() {
    let mut config = Config::new();
    config.set("websocket.zeta.value", true).unwrap();
    config.set("websocket.limits.extra", true).unwrap();

    assert!(matches!(
        WsClientBuilder::from_config(&config, "websocket"),
        Err(ConfigError::UnknownProperties { paths })
            if paths == ["websocket.limits.extra", "websocket.zeta.value"]
    ));
}

#[test]
fn test_config_conversion_error_contains_the_root_configuration_path() {
    let mut config = Config::new();
    config
        .set(format!("websocket.{}", config_keys::CONNECT_TIMEOUT_MS), "slow")
        .unwrap();

    assert!(matches!(
        WsClientBuilder::from_config(&config, "websocket"),
        Err(ConfigError::ConversionError { key, .. })
            if key == format!("websocket.{}", config_keys::CONNECT_TIMEOUT_MS)
    ));
}
