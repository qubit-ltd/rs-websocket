// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::time::Duration;

use qubit_websocket::WsClient;
use qubit_websocket::WsClientOptions;
use qubit_websocket::WsError;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSendError;
use qubit_websocket::WsSessionLimits;

#[test]
fn test_default_session_limits_match_the_documented_budget() {
    let limits = WsSessionLimits::default();
    assert_eq!(limits.max_inbound_frame_bytes, 1024 * 1024);
    assert_eq!(limits.max_inbound_message_bytes, 4 * 1024 * 1024);
    assert_eq!(limits.inbound_messages, 64);
    assert_eq!(limits.inbound_bytes, 4 * 1024 * 1024);
    assert_eq!(limits.outbound_messages, 64);
    assert_eq!(limits.outbound_bytes, 4 * 1024 * 1024);
    assert_eq!(limits.no_progress_timeout, Duration::from_secs(30));
    assert_eq!(limits.close_timeout, Duration::from_secs(5));
}

#[test]
fn test_options_have_documented_connection_timeouts() {
    let options = WsClientOptions::default();
    assert_eq!(options.connect_timeout, Duration::from_secs(10));
    assert_eq!(options.handshake_timeout, Duration::from_secs(10));
}

#[test]
fn test_validation_rejects_each_zero_limit() {
    let cases = [
        WsSessionLimits {
            max_inbound_frame_bytes: 0,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            max_inbound_message_bytes: 0,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            inbound_messages: 0,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            inbound_bytes: 0,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            outbound_messages: 0,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            outbound_bytes: 0,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            no_progress_timeout: Duration::ZERO,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            close_timeout: Duration::ZERO,
            ..WsSessionLimits::default()
        },
    ];
    for limits in cases {
        let options = WsClientOptions {
            limits,
            ..WsClientOptions::default()
        };
        assert_eq!(options.validate().unwrap_err().kind(), WsErrorKind::InvalidConfig);
    }
    assert_eq!(
        WsClientOptions {
            connect_timeout: Duration::ZERO,
            ..WsClientOptions::default()
        }
        .validate()
        .unwrap_err()
        .kind(),
        WsErrorKind::InvalidConfig
    );
    assert_eq!(
        WsClientOptions {
            handshake_timeout: Duration::ZERO,
            ..WsClientOptions::default()
        }
        .validate()
        .unwrap_err()
        .kind(),
        WsErrorKind::InvalidConfig
    );
}

#[test]
fn test_validation_rejects_inconsistent_inbound_frame_and_message_budgets() {
    let defaults = WsSessionLimits::default();
    let limits = WsSessionLimits {
        max_inbound_frame_bytes: defaults.max_inbound_message_bytes + 1,
        ..defaults.clone()
    };
    assert_eq!(
        WsClientOptions {
            limits,
            ..WsClientOptions::default()
        }
        .validate()
        .unwrap_err()
        .kind(),
        WsErrorKind::InvalidConfig
    );
    let limits = WsSessionLimits {
        max_inbound_message_bytes: defaults.inbound_bytes + 1,
        ..defaults.clone()
    };
    assert_eq!(
        WsClientOptions {
            limits,
            ..WsClientOptions::default()
        }
        .validate()
        .unwrap_err()
        .kind(),
        WsErrorKind::InvalidConfig
    );
}

#[test]
fn test_inbound_message_budget_is_independent_of_outbound_budget() {
    let limits = WsSessionLimits {
        max_inbound_frame_bytes: 1,
        max_inbound_message_bytes: 2,
        inbound_bytes: 2,
        outbound_bytes: 1,
        ..WsSessionLimits::default()
    };

    assert!(
        WsClientOptions {
            limits,
            ..WsClientOptions::default()
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn test_minimum_outbound_budget_is_valid() {
    let limits = WsSessionLimits {
        outbound_bytes: 1,
        ..WsSessionLimits::default()
    };
    assert!(
        WsClientOptions {
            limits,
            ..WsClientOptions::default()
        }
        .validate()
        .is_ok()
    );
    assert!(
        WsClient::builder()
            .limits(WsSessionLimits {
                outbound_bytes: 1,
                ..WsSessionLimits::default()
            })
            .build()
            .is_ok()
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn test_validation_rejects_byte_semaphore_limits_above_u32() {
    let limits = WsSessionLimits {
        inbound_bytes: u32::MAX as usize + 1,
        ..WsSessionLimits::default()
    };
    assert_eq!(
        WsClientOptions {
            limits,
            ..WsClientOptions::default()
        }
        .validate()
        .unwrap_err()
        .kind(),
        WsErrorKind::InvalidConfig
    );
    let limits = WsSessionLimits {
        outbound_bytes: u32::MAX as usize + 1,
        ..WsSessionLimits::default()
    };
    assert_eq!(
        WsClientOptions {
            limits,
            ..WsClientOptions::default()
        }
        .validate()
        .unwrap_err()
        .kind(),
        WsErrorKind::InvalidConfig
    );
}

#[test]
fn test_validation_rejects_message_semaphore_limits_above_max_permits() {
    let max_permits = tokio::sync::Semaphore::MAX_PERMITS;
    let cases = [
        WsSessionLimits {
            inbound_messages: max_permits + 1,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            outbound_messages: max_permits + 1,
            ..WsSessionLimits::default()
        },
    ];

    for limits in cases {
        assert_eq!(
            WsClientOptions {
                limits: limits.clone(),
                ..WsClientOptions::default()
            }
            .validate()
            .unwrap_err()
            .kind(),
            WsErrorKind::InvalidConfig
        );
        let error = match qubit_websocket::WsClient::builder().limits(limits).build() {
            Ok(_) => panic!("message capacity above MAX_PERMITS must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
    }

    let limits = WsSessionLimits {
        inbound_messages: max_permits,
        outbound_messages: max_permits,
        ..WsSessionLimits::default()
    };
    assert!(
        WsClientOptions {
            limits,
            ..WsClientOptions::default()
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn test_error_formatting_redacts_source_text_but_keeps_source_chain() {
    let error = WsError::with_source(WsErrorKind::Transport, std::io::Error::other("secret-token"));
    assert!(!format!("{error}").contains("secret-token"));
    assert!(!format!("{error:?}").contains("secret-token"));
    assert!(error.source().is_some());
}

#[test]
fn test_send_errors_have_stable_display_and_propagate_into_boxed_errors() {
    fn propagate_send_error(error: WsSendError) -> Result<(), Box<dyn Error + Send + Sync>> {
        Err(error)?;
        Ok(())
    }

    let cases = [
        (WsSendError::Backpressure, "WebSocket send queue is full"),
        (WsSendError::Closed, "WebSocket session is closed"),
        (
            WsSendError::MessageTooLarge,
            "WebSocket message exceeds the outbound limit",
        ),
    ];

    for (error, expected_message) in cases {
        assert_eq!(error.to_string(), expected_message);
        let propagated = propagate_send_error(error).unwrap_err();
        assert_eq!(propagated.to_string(), expected_message);
    }
}

#[test]
fn test_payload_len_counts_utf8_bytes_and_binary_bytes() {
    assert_eq!(WsMessage::Text("猫".to_owned()).payload_len(), 3);
    assert_eq!(WsMessage::Binary(bytes::Bytes::from_static(b"abc")).payload_len(), 3);
}

#[test]
fn test_message_debug_does_not_expose_payload_contents() {
    let message = WsMessage::Text("private-message".to_owned());
    assert!(!format!("{message:?}").contains("private-message"));
}

#[test]
fn test_options_debug_omits_header_and_subprotocol_values() {
    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", "secret-token".parse().unwrap());
    let options = WsClientOptions {
        headers,
        subprotocols: vec!["private-protocol".to_owned()],
        ..WsClientOptions::default()
    };

    let debug = format!("{options:?}");
    assert!(!debug.contains("secret-token"));
    assert!(!debug.contains("private-protocol"));
    assert!(debug.contains("header_count"));
    assert!(debug.contains("subprotocol_count"));
}
