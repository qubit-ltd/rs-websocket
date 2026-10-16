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
/// bytes. Its [`Debug`](fmt::Debug) implementation reports only the variant
/// and payload byte count, so message contents are not included in logs. Use
/// [`WsMessage::payload_len`] to inspect the payload size without formatting
/// or copying the payload.
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
    /// UTF-8 text payload. Its length and queue budget are measured in bytes.
    Text(String),
    /// Opaque binary payload, stored in a cheaply cloneable byte buffer.
    Binary(Bytes),
}

impl fmt::Debug for WsMessage {
    /// Formats the variant and payload byte count without exposing its
    /// contents.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => f.debug_struct("Text").field("payload_bytes", &text.len()).finish(),
            Self::Binary(bytes) => f.debug_struct("Binary").field("payload_bytes", &bytes.len()).finish(),
        }
    }
}

impl WsMessage {
    /// Returns the payload size in bytes.
    ///
    /// For text, this is the UTF-8 byte length, which can differ from the
    /// number of Unicode scalar values or grapheme clusters. For binary data,
    /// it is the number of stored bytes.
    #[inline]
    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Binary(bytes) => bytes.len(),
        }
    }
}
