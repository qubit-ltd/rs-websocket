use std::error::Error;
use std::time::Duration;

use qubit_websocket::{WsClientOptions, WsError, WsErrorKind, WsMessage, WsSessionLimits};

#[test]
fn test_default_session_limits_match_the_documented_budget() {
    let limits = WsSessionLimits::default();
    assert_eq!(limits.max_frame_bytes, 1024 * 1024);
    assert_eq!(limits.max_message_bytes, 4 * 1024 * 1024);
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
    let mut cases = Vec::new();
    let mut limits = WsSessionLimits::default();
    limits.max_frame_bytes = 0;
    cases.push(limits);
    let mut limits = WsSessionLimits::default();
    limits.max_message_bytes = 0;
    cases.push(limits);
    let mut limits = WsSessionLimits::default();
    limits.inbound_messages = 0;
    cases.push(limits);
    let mut limits = WsSessionLimits::default();
    limits.inbound_bytes = 0;
    cases.push(limits);
    let mut limits = WsSessionLimits::default();
    limits.outbound_messages = 0;
    cases.push(limits);
    let mut limits = WsSessionLimits::default();
    limits.outbound_bytes = 0;
    cases.push(limits);
    let mut limits = WsSessionLimits::default();
    limits.no_progress_timeout = Duration::ZERO;
    cases.push(limits);
    let mut limits = WsSessionLimits::default();
    limits.close_timeout = Duration::ZERO;
    cases.push(limits);
    for limits in cases {
        let options = WsClientOptions {
            limits,
            ..WsClientOptions::default()
        };
        assert_eq!(
            options.validate().unwrap_err().kind(),
            WsErrorKind::InvalidConfig
        );
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
fn test_validation_rejects_inconsistent_frame_message_and_queue_budgets() {
    let mut limits = WsSessionLimits::default();
    limits.max_frame_bytes = limits.max_message_bytes + 1;
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
    let mut limits = WsSessionLimits::default();
    limits.max_message_bytes = limits.inbound_bytes + 1;
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
    let mut limits = WsSessionLimits::default();
    limits.max_message_bytes = limits.outbound_bytes + 1;
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
#[cfg(target_pointer_width = "64")]
fn test_validation_rejects_byte_semaphore_limits_above_u32() {
    let mut limits = WsSessionLimits::default();
    limits.inbound_bytes = u32::MAX as usize + 1;
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
    let mut limits = WsSessionLimits::default();
    limits.outbound_bytes = u32::MAX as usize + 1;
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
fn test_error_formatting_redacts_source_text_but_keeps_source_chain() {
    let error = WsError::with_source(WsErrorKind::Connect, std::io::Error::other("secret-token"));
    assert!(!format!("{error}").contains("secret-token"));
    assert!(!format!("{error:?}").contains("secret-token"));
    assert!(error.source().is_some());
}

#[test]
fn test_payload_len_counts_utf8_bytes_and_binary_bytes() {
    assert_eq!(WsMessage::Text("猫".to_owned()).payload_len(), 3);
    assert_eq!(
        WsMessage::Binary(bytes::Bytes::from_static(b"abc")).payload_len(),
        3
    );
}

#[test]
fn test_message_debug_does_not_expose_payload_contents() {
    let message = WsMessage::Text("private-message".to_owned());
    assert!(!format!("{message:?}").contains("private-message"));
}
