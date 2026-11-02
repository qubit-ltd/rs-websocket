// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::io::Error as IoError;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use qubit_websocket::WsClient;
use qubit_websocket::WsClientOptions;
use qubit_websocket::WsConfigFailure;
use qubit_websocket::WsError;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSendError;
use qubit_websocket::WsSessionLimits;
use tokio::sync::Semaphore;

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
fn test_validation_reports_all_config_failure_variants_and_fields() {
    let defaults = WsSessionLimits::default();
    let cases = [
        (
            WsClientOptions {
                connect_timeout: Duration::ZERO,
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroConnectTimeout,
            "connect_timeout_ms",
        ),
        (
            WsClientOptions {
                handshake_timeout: Duration::ZERO,
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroHandshakeTimeout,
            "handshake_timeout_ms",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    max_inbound_frame_bytes: 0,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroMaxInboundFrameBytes,
            "limits.max_inbound_frame_bytes",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    max_inbound_message_bytes: 0,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroMaxInboundMessageBytes,
            "limits.max_inbound_message_bytes",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    inbound_messages: 0,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroInboundMessages,
            "limits.inbound_messages",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    inbound_bytes: 0,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroInboundBytes,
            "limits.inbound_bytes",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    outbound_messages: 0,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroOutboundMessages,
            "limits.outbound_messages",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    outbound_bytes: 0,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroOutboundBytes,
            "limits.outbound_bytes",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    no_progress_timeout: Duration::ZERO,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroNoProgressTimeout,
            "limits.no_progress_timeout_ms",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    close_timeout: Duration::ZERO,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::ZeroCloseTimeout,
            "limits.close_timeout_ms",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    max_inbound_frame_bytes: defaults.max_inbound_message_bytes + 1,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::FrameExceedsMessageLimit,
            "limits.max_inbound_frame_bytes",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    max_inbound_message_bytes: defaults.inbound_bytes + 1,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::MessageExceedsInboundBudget,
            "limits.max_inbound_message_bytes",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    outbound_bytes: usize::MAX,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::OutboundWriteBufferOverflow,
            "limits.outbound_bytes",
        ),
        (
            WsClientOptions {
                connect_timeout: Duration::MAX,
                ..WsClientOptions::default()
            },
            WsConfigFailure::UnrepresentableConnectTimeout,
            "connect_timeout_ms",
        ),
        (
            WsClientOptions {
                handshake_timeout: Duration::MAX,
                ..WsClientOptions::default()
            },
            WsConfigFailure::UnrepresentableHandshakeTimeout,
            "handshake_timeout_ms",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    close_timeout: Duration::MAX,
                    ..defaults.clone()
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::UnrepresentableCloseTimeout,
            "limits.close_timeout_ms",
        ),
        (
            WsClientOptions {
                limits: WsSessionLimits {
                    no_progress_timeout: Duration::MAX,
                    ..defaults
                },
                ..WsClientOptions::default()
            },
            WsConfigFailure::UnrepresentableNoProgressTimeout,
            "limits.no_progress_timeout_ms",
        ),
    ];

    for (options, expected, field) in cases {
        let error = options.validate().expect_err("invalid option must be rejected");
        assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
        let failure = error.config_failure().expect("validation reason is present");
        assert_eq!(failure, expected);
        assert_eq!(failure.field(), field);
    }
}

#[test]
fn test_validation_preserves_first_failure_order_and_equal_boundaries() {
    let error = WsClientOptions {
        connect_timeout: Duration::ZERO,
        handshake_timeout: Duration::ZERO,
        ..WsClientOptions::default()
    }
    .validate()
    .expect_err("zero connection timeout is first");
    assert_eq!(error.config_failure(), Some(WsConfigFailure::ZeroConnectTimeout));

    let error = WsClientOptions {
        limits: WsSessionLimits {
            max_inbound_frame_bytes: 5,
            max_inbound_message_bytes: 4,
            outbound_bytes: usize::MAX,
            ..WsSessionLimits::default()
        },
        ..WsClientOptions::default()
    }
    .validate()
    .expect_err("size relation precedes write-buffer overflow");
    assert_eq!(error.config_failure(), Some(WsConfigFailure::FrameExceedsMessageLimit));

    let error = WsClientOptions {
        limits: WsSessionLimits {
            outbound_bytes: usize::MAX,
            close_timeout: Duration::MAX,
            ..WsSessionLimits::default()
        },
        ..WsClientOptions::default()
    }
    .validate()
    .expect_err("write-buffer overflow precedes deadline validation");
    assert_eq!(
        error.config_failure(),
        Some(WsConfigFailure::OutboundWriteBufferOverflow)
    );

    let limits = WsSessionLimits {
        max_inbound_frame_bytes: 8,
        max_inbound_message_bytes: 8,
        inbound_bytes: 8,
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
fn test_config_failure_is_none_for_manual_and_non_config_errors() {
    assert_eq!(WsError::new(WsErrorKind::InvalidConfig).config_failure(), None);
    assert_eq!(WsError::new(WsErrorKind::Transport).config_failure(), None);
    assert_eq!(
        WsError::with_source(WsErrorKind::InvalidConfig, IoError::other("source-secret")).config_failure(),
        None
    );
}

#[test]
fn test_config_failure_formatting_contains_only_static_reason_and_field() {
    let options = WsClientOptions {
        connect_timeout: Duration::ZERO,
        headers: {
            let mut headers = HeaderMap::new();
            headers.insert("authorization", "header-secret".parse().expect("valid header"));
            headers
        },
        subprotocols: vec!["protocol-secret".to_owned()],
        ..WsClientOptions::default()
    };
    let error = options.validate().expect_err("zero timeout is invalid");
    for rendered in [error.to_string(), format!("{error:?}")] {
        assert!(rendered.contains("ZeroConnectTimeout"));
        assert!(rendered.contains("connect_timeout_ms"));
        assert!(!rendered.contains("header-secret"));
        assert!(!rendered.contains("protocol-secret"));
        assert!(!rendered.contains("source-secret"));
    }
}

#[test]
fn test_unrepresentable_session_deadline_is_rejected() {
    for limits in [
        WsSessionLimits {
            close_timeout: Duration::MAX,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            no_progress_timeout: Duration::MAX,
            ..WsSessionLimits::default()
        },
    ] {
        let options = WsClientOptions {
            limits: limits.clone(),
            ..WsClientOptions::default()
        };
        assert_eq!(options.validate().unwrap_err().kind(), WsErrorKind::InvalidConfig);
        let error = WsClient::builder()
            .limits(limits)
            .build()
            .err()
            .expect("builder rejects unrepresentable session deadlines");
        assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
    }
}

#[test]
fn test_unrepresentable_connection_deadline_is_rejected() {
    for options in [
        WsClientOptions {
            connect_timeout: Duration::MAX,
            ..WsClientOptions::default()
        },
        WsClientOptions {
            handshake_timeout: Duration::MAX,
            ..WsClientOptions::default()
        },
    ] {
        assert_eq!(options.validate().unwrap_err().kind(), WsErrorKind::InvalidConfig);
    }
    for builder in [
        WsClient::builder().connect_timeout(Duration::MAX),
        WsClient::builder().handshake_timeout(Duration::MAX),
    ] {
        let error = builder
            .build()
            .err()
            .expect("unrepresentable deadline must be rejected");
        assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
    }
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
fn test_validation_accepts_byte_budgets_above_u32() {
    let above_u32 = u32::MAX as usize + 1;
    for limits in [
        WsSessionLimits {
            inbound_bytes: above_u32,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            outbound_bytes: above_u32,
            ..WsSessionLimits::default()
        },
    ] {
        assert!(
            WsClientOptions {
                limits: limits.clone(),
                ..WsClientOptions::default()
            }
            .validate()
            .is_ok()
        );
        assert!(WsClient::builder().limits(limits).build().is_ok());
    }
}

#[test]
fn test_validation_accepts_message_counts_above_old_permit_limit() {
    let above_old_limit = Semaphore::MAX_PERMITS
        .checked_add(1)
        .expect("old permit limit has a representable successor");
    for limits in [
        WsSessionLimits {
            inbound_messages: above_old_limit,
            ..WsSessionLimits::default()
        },
        WsSessionLimits {
            outbound_messages: above_old_limit,
            ..WsSessionLimits::default()
        },
    ] {
        assert!(
            WsClientOptions {
                limits: limits.clone(),
                ..WsClientOptions::default()
            }
            .validate()
            .is_ok()
        );
        assert!(WsClient::builder().limits(limits).build().is_ok());
    }
}

#[test]
fn test_validation_rejects_write_buffer_overflow() {
    let invalid = WsSessionLimits {
        outbound_bytes: usize::MAX,
        ..WsSessionLimits::default()
    };
    assert_eq!(
        WsClientOptions {
            limits: invalid,
            ..WsClientOptions::default()
        }
        .validate()
        .unwrap_err()
        .kind(),
        WsErrorKind::InvalidConfig,
    );
    let valid = WsSessionLimits {
        outbound_bytes: usize::MAX - 145,
        ..WsSessionLimits::default()
    };
    assert!(
        WsClientOptions {
            limits: valid,
            ..WsClientOptions::default()
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn test_error_formatting_redacts_source_text_but_keeps_source_chain() {
    let error = WsError::with_source(WsErrorKind::Transport, IoError::other("secret-token"));
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
    assert_eq!(WsMessage::Binary(Bytes::from_static(b"abc")).payload_len(), 3);
}

#[test]
fn test_message_debug_does_not_expose_payload_contents() {
    let message = WsMessage::Text("private-message".to_owned());
    assert!(!format!("{message:?}").contains("private-message"));
}

#[test]
fn test_options_debug_omits_header_and_subprotocol_values() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        "secret-token"
            .parse()
            .expect("authorization header value should be valid"),
    );
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
