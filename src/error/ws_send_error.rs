// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason that an application send could not be queued.
///
/// This reports queue admission only. It does not describe a WebSocket
/// terminal failure or indicate whether the peer received a message.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsSendError;
///
/// let result: Result<(), WsSendError> = Err(WsSendError::Backpressure);
/// assert_eq!(result, Err(WsSendError::Backpressure));
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsSendError {
    /// The configured message-count or byte budget is currently full; retry
    /// policy and backoff remain the caller's responsibility.
    Backpressure,
    /// The session no longer accepts application messages.
    Closed,
    /// The message's payload bytes exceed the configured outbound byte limit.
    MessageTooLarge,
}

impl fmt::Display for WsSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Backpressure => "WebSocket send queue is full",
            Self::Closed => "WebSocket session is closed",
            Self::MessageTooLarge => "WebSocket message exceeds the outbound limit",
        };
        f.write_str(message)
    }
}

impl Error for WsSendError {}
