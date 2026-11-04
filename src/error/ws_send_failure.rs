// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

use super::WsSendError;
use crate::ws_message::WsMessage;

/// A send-admission failure that retains the message which was not queued.
///
/// The message is available for an application-defined retry or recovery
/// policy. Queue admission does not guarantee delivery to the peer. This
/// value is not cloneable, and its formatting never includes message data.
///
/// # Examples
///
/// ```
/// use qubit_websocket::{WsMessage, WsSendError, WsSendFailure};
///
/// fn recover(failure: WsSendFailure) -> Result<WsMessage, WsSendError> {
///     let (kind, message) = failure.into_parts();
///     match kind {
///         WsSendError::Backpressure => Ok(message),
///         error => Err(error),
///     }
/// }
/// ```
#[must_use]
pub struct WsSendFailure {
    kind: WsSendError,
    message: WsMessage,
}

impl WsSendFailure {
    /// Creates a failure while retaining the message rejected by the queue.
    #[inline]
    pub(crate) fn new(kind: WsSendError, message: WsMessage) -> Self {
        Self { kind, message }
    }

    /// Returns the reason that queue admission failed.
    #[inline]
    pub fn kind(&self) -> WsSendError {
        self.kind
    }

    /// Borrows the message that was not queued.
    #[must_use]
    #[inline]
    pub fn message(&self) -> &WsMessage {
        &self.message
    }

    /// Returns the message that was not queued.
    #[must_use]
    #[inline]
    pub fn into_message(self) -> WsMessage {
        self.message
    }

    /// Returns the failure kind and the message that was not queued.
    #[inline]
    pub fn into_parts(self) -> (WsSendError, WsMessage) {
        (self.kind, self.message)
    }
}

impl fmt::Debug for WsSendFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (variant, payload_len) = match &self.message {
            WsMessage::Text(text) => ("Text", text.len()),
            WsMessage::Binary(bytes) => ("Binary", bytes.len()),
        };
        f.debug_struct("WsSendFailure")
            .field("kind", &self.kind)
            .field("message_variant", &variant)
            .field("payload_bytes", &payload_len)
            .finish()
    }
}

impl fmt::Display for WsSendFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match &self.message {
            WsMessage::Text(_) => "Text",
            WsMessage::Binary(_) => "Binary",
        };
        write!(
            f,
            "{} ({variant}, {} payload bytes)",
            self.kind,
            self.message.payload_len()
        )
    }
}

impl Error for WsSendFailure {}
