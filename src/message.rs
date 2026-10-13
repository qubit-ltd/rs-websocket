// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use bytes::Bytes;

/// An application data message exchanged with a WebSocket peer.
///
/// Text messages contain valid UTF-8, while binary messages preserve opaque
/// bytes. Use [`WsMessage::payload_len`] to inspect the payload size without
/// formatting or copying the payload.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsMessage;
///
/// let message = WsMessage::Text("hello".to_owned());
/// assert_eq!(message.payload_len(), 5);
/// ```
#[derive(Clone, Eq, PartialEq)]
pub enum WsMessage {
    /// UTF-8 text payload.
    Text(String),
    /// Opaque binary payload.
    Binary(Bytes),
}

impl fmt::Debug for WsMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => f.debug_struct("Text").field("payload_bytes", &text.len()).finish(),
            Self::Binary(bytes) => f.debug_struct("Binary").field("payload_bytes", &bytes.len()).finish(),
        }
    }
}

impl WsMessage {
    /// Returns the payload size in bytes.
    pub fn payload_len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Binary(bytes) => bytes.len(),
        }
    }
}
