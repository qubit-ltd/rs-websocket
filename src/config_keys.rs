// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Stable relative configuration keys accepted by the optional config feature.

/// Relative key for the TCP connection timeout, expressed in milliseconds.
pub const CONNECT_TIMEOUT_MS: &str = "connect_timeout_ms";
/// Relative key for the opening handshake timeout, expressed in milliseconds.
pub const HANDSHAKE_TIMEOUT_MS: &str = "handshake_timeout_ms";
/// Relative key for the ordered WebSocket subprotocol offers.
pub const SUBPROTOCOLS: &str = "subprotocols";
/// Relative key for the maximum inbound frame payload, in bytes.
pub const MAX_INBOUND_FRAME_BYTES: &str = "limits.max_inbound_frame_bytes";
/// Relative key for the maximum reassembled inbound message, in bytes.
pub const MAX_INBOUND_MESSAGE_BYTES: &str = "limits.max_inbound_message_bytes";
/// Relative key for the inbound queue capacity, in messages.
pub const INBOUND_MESSAGES: &str = "limits.inbound_messages";
/// Relative key for the inbound queue payload budget, in bytes.
pub const INBOUND_BYTES: &str = "limits.inbound_bytes";
/// Relative key for the outbound queue capacity, in messages.
pub const OUTBOUND_MESSAGES: &str = "limits.outbound_messages";
/// Relative key for the outbound queue payload budget, in bytes.
pub const OUTBOUND_BYTES: &str = "limits.outbound_bytes";
/// Relative key for the no-progress timeout, expressed in milliseconds.
pub const NO_PROGRESS_TIMEOUT_MS: &str = "limits.no_progress_timeout_ms";
/// Relative key for the close-handshake timeout, expressed in milliseconds.
pub const CLOSE_TIMEOUT_MS: &str = "limits.close_timeout_ms";

/// Configuration keys recognized by the optional config loader.
pub(crate) const KNOWN_KEYS: &[&str] = &[
    CONNECT_TIMEOUT_MS,
    HANDSHAKE_TIMEOUT_MS,
    SUBPROTOCOLS,
    MAX_INBOUND_FRAME_BYTES,
    MAX_INBOUND_MESSAGE_BYTES,
    INBOUND_MESSAGES,
    INBOUND_BYTES,
    OUTBOUND_MESSAGES,
    OUTBOUND_BYTES,
    NO_PROGRESS_TIMEOUT_MS,
    CLOSE_TIMEOUT_MS,
];
