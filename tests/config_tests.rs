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
use qubit_websocket::WsConfigFailure;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSendError;
use qubit_websocket::WsSessionLimits;
use qubit_websocket::config_keys;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn test_config_outbound_capacity_applies_and_builder_override_wins() {
    let mut config = Config::new();
    config.set("websocket.limits.outbound_bytes", 4_u64).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        let mut websocket = timeout(TEST_TIMEOUT, accept_async(socket)).await.unwrap().unwrap();
        timeout(TEST_TIMEOUT, release_rx).await.unwrap().unwrap();
        let _ = timeout(TEST_TIMEOUT, websocket.close(None)).await;
    });

    let configured = WsClientBuilder::from_config(&config, "websocket")
        .unwrap()
        .build()
        .unwrap();
    let mut session = timeout(TEST_TIMEOUT, configured.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        session.try_send(WsMessage::Text("12345".into())),
        Err(WsSendError::MessageTooLarge)
    );
    release_tx.send(()).unwrap();
    timeout(TEST_TIMEOUT, session.close()).await.unwrap().unwrap();
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();

    // A second listener keeps the same configured client input while exercising
    // the later builder override with a message larger than the configured cap.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        let mut websocket = timeout(TEST_TIMEOUT, accept_async(socket)).await.unwrap().unwrap();
        timeout(TEST_TIMEOUT, release_rx).await.unwrap().unwrap();
        let _ = timeout(TEST_TIMEOUT, websocket.close(None)).await;
    });
    let overridden = WsClientBuilder::from_config(&config, "websocket")
        .unwrap()
        .limits(WsSessionLimits {
            outbound_bytes: 8,
            ..WsSessionLimits::default()
        })
        .build()
        .unwrap();
    let mut session = timeout(TEST_TIMEOUT, overridden.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session.try_send(WsMessage::Text("12345".into())), Ok(()));
    release_tx.send(()).unwrap();
    timeout(TEST_TIMEOUT, session.close()).await.unwrap().unwrap();
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_config_handshake_timeout_applies_and_builder_override_wins() {
    let mut config = Config::new();
    config.set("websocket.handshake_timeout_ms", 100_u64).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        timeout(TEST_TIMEOUT, async {
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        timeout(TEST_TIMEOUT, release_rx).await.unwrap().unwrap();
        drop(socket);
    });
    let client = WsClientBuilder::from_config(&config, "websocket")
        .unwrap()
        .build()
        .unwrap();
    let result = timeout(TEST_TIMEOUT, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap();
    let error = match result {
        Ok(_) => panic!("configured handshake timeout must reject the stalled peer"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Timeout);
    release_tx.send(()).unwrap();
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        let request = timeout(TEST_TIMEOUT, async {
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break request;
                }
            }
        })
        .await
        .unwrap();
        timeout(TEST_TIMEOUT, tokio::time::sleep(Duration::from_millis(500)))
            .await
            .unwrap();
        let request = String::from_utf8(request).unwrap();
        let key = request
            .lines()
            .find_map(|line| {
                line.strip_prefix("Sec-WebSocket-Key:")
                    .or_else(|| line.strip_prefix("sec-websocket-key:"))
            })
            .expect("client sends a websocket key")
            .trim();
        let accept = derive_accept_key(key.as_bytes());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        timeout(TEST_TIMEOUT, socket.write_all(response.as_bytes()))
            .await
            .unwrap()
            .unwrap();
        timeout(TEST_TIMEOUT, release_rx).await.unwrap().unwrap();
    });
    let client = WsClientBuilder::from_config(&config, "websocket")
        .unwrap()
        .handshake_timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let session = timeout(TEST_TIMEOUT, client.connect(&format!("ws://{address}/")))
        .await
        .unwrap()
        .unwrap();
    drop(session);
    release_tx.send(()).unwrap();
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
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
    assert_eq!(error.config_failure(), Some(WsConfigFailure::ZeroOutboundMessages));
    assert_eq!(error.config_failure().unwrap().field(), "limits.outbound_messages");
}

#[test]
fn test_config_rejects_inconsistent_limits_during_build() {
    let mut config = Config::new();
    config
        .set(
            format!("websocket.{}", config_keys::MAX_INBOUND_MESSAGE_BYTES),
            2_000_000_u64,
        )
        .unwrap();
    config
        .set(format!("websocket.{}", config_keys::INBOUND_BYTES), 1_500_000_u64)
        .unwrap();
    let builder = WsClientBuilder::from_config(&config, "websocket").unwrap();
    let error = match builder.build() {
        Ok(_) => panic!("inconsistent inbound limits must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
    assert_eq!(
        error.config_failure(),
        Some(WsConfigFailure::MessageExceedsInboundBudget)
    );
    assert_eq!(
        error.config_failure().unwrap().field(),
        "limits.max_inbound_message_bytes"
    );
}

#[test]
fn test_config_failure_field_is_relative_to_custom_prefix_and_override_can_repair_it() {
    let mut config = Config::new();
    config.set("service.websocket.connect_timeout_ms", 0_u64).unwrap();
    let error = match WsClientBuilder::from_config(&config, "service.websocket")
        .unwrap()
        .build()
    {
        Ok(_) => panic!("zero configured timeout must fail at build"),
        Err(error) => error,
    };
    assert_eq!(error.config_failure(), Some(WsConfigFailure::ZeroConnectTimeout));
    assert_eq!(error.config_failure().unwrap().field(), "connect_timeout_ms");

    let client = WsClientBuilder::from_config(&config, "service.websocket")
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .build();
    assert!(client.is_ok(), "builder override repairs the invalid config value");
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
fn test_config_accepts_nested_application_subtree() {
    let mut config = Config::new();
    config.set("service.name", "worker").unwrap();
    config.set("service.websocket.connect_timeout_ms", 3000_u64).unwrap();
    let client = WsClientBuilder::from_config(&config, "service.websocket")
        .unwrap()
        .build();
    assert!(client.is_ok(), "nested application configuration must build");
}

#[test]
fn test_config_reports_sorted_root_paths_for_unknown_nested_keys() {
    let mut config = Config::new();
    config.set("service.websocket.zeta.value", true).unwrap();
    config.set("service.websocket.limits.extra", true).unwrap();
    config.set("service.unrelated", true).unwrap();

    assert!(matches!(
        WsClientBuilder::from_config(&config, "service.websocket"),
        Err(ConfigError::UnknownProperties { paths })
            if paths == ["service.websocket.limits.extra", "service.websocket.zeta.value"]
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

#[test]
fn test_config_rejects_negative_capacity_without_losing_path() {
    let mut config = Config::new();
    config.set("websocket.limits.outbound_bytes", -1_i64).unwrap();
    let error = WsClientBuilder::from_config(&config, "websocket")
        .err()
        .expect("negative capacity must not become unsigned");
    assert_eq!(error.path(), Some("websocket.limits.outbound_bytes"));
}

#[test]
fn test_config_rejects_capacity_above_platform_usize() {
    let mut config = Config::new();
    config
        .set("websocket.limits.outbound_bytes", usize::MAX as u128 + 1)
        .unwrap();
    let error = WsClientBuilder::from_config(&config, "websocket")
        .err()
        .expect("capacity outside usize must not be truncated");
    assert!(matches!(error, ConfigError::PlatformIntegerOutOfRange { .. }));
    assert_eq!(error.kind(), qubit_config::ConfigErrorKind::Conversion);
    assert_eq!(error.path(), Some("websocket.limits.outbound_bytes"));
}

#[test]
fn test_config_rejects_collection_capacity_above_platform_usize_with_index() {
    let mut config = Config::new();
    config
        .set("websocket.limits.outbound_bytes", vec![usize::MAX as u128 + 1])
        .unwrap();
    let error = WsClientBuilder::from_config(&config, "websocket")
        .err()
        .expect("collection capacity outside usize must not be truncated");
    assert!(matches!(
        error,
        ConfigError::PlatformIntegerOutOfRange {
            source_index: Some(0),
            ..
        }
    ));
    assert_eq!(error.path(), Some("websocket.limits.outbound_bytes"));
}
