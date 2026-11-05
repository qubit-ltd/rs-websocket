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
    /// Admission error that prevented this message from entering the queue.
    kind: WsSendError,
    /// Original message retained for application-defined recovery after
    /// rejection.
    message: WsMessage,
}

impl WsSendFailure {
    /// Creates a failure while retaining the message rejected by the queue.
    ///
    /// The message is moved into this failure without cloning its payload. A
    /// rejected message does not imply that application work was rolled back
    /// or indicate whether any other message was delivered.
    ///
    /// # Parameters
    ///
    /// - `kind`: The admission error that prevented the message from entering
    ///   the queue.
    /// - `message`: The original message rejected by the queue, moved into this
    ///   failure for possible recovery.
    ///
    /// # Returns
    ///
    /// A failure containing `kind` and the original `message`.
    #[inline]
    pub(crate) fn new(kind: WsSendError, message: WsMessage) -> Self {
        Self { kind, message }
    }

    /// Returns the reason that queue admission failed.
    ///
    /// # Returns
    ///
    /// The admission error for this message's rejection. This says nothing
    /// about application rollback or the delivery of other messages.
    #[inline]
    pub fn kind(&self) -> WsSendError {
        self.kind
    }

    /// Borrows the message that was not queued.
    ///
    /// # Returns
    ///
    /// A reference to the original message, borrowed for a lifetime bounded
    /// by `self`. The message and its payload are not cloned. Its rejection
    /// does not imply application rollback or determine the delivery of other
    /// messages.
    #[must_use]
    #[inline]
    pub fn message(&self) -> &WsMessage {
        &self.message
    }

    /// Consumes this failure and returns the message that was not queued.
    ///
    /// # Returns
    ///
    /// The original message, moved out of this failure without cloning its
    /// payload. Rejection does not imply application rollback or indicate the
    /// delivery result of other messages.
    #[must_use]
    #[inline]
    pub fn into_message(self) -> WsMessage {
        self.message
    }

    /// Consumes this failure and returns its kind and the message that was not
    /// queued.
    ///
    /// # Returns
    ///
    /// A tuple containing `(kind, message)`, in that order. Both values are
    /// moved out of this failure; the original message payload is not cloned.
    /// Rejection does not imply application rollback or indicate the delivery
    /// result of other messages.
    #[inline]
    pub fn into_parts(self) -> (WsSendError, WsMessage) {
        (self.kind, self.message)
    }
}

/// Formats failure details without revealing the message payload.
impl fmt::Debug for WsSendFailure {
    /// Formats the error kind and message metadata without exposing payloads.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter that receives the redacted diagnostic representation.
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

/// Formats the rejection kind and non-sensitive message metadata.
impl fmt::Display for WsSendFailure {
    /// Formats the error kind and non-sensitive message metadata.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter that receives the non-sensitive error description.
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
