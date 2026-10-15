// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

/// Memory and timing limits applied to one WebSocket session.
///
/// Use `Default` as a baseline and override only the budgets needed by the
/// application. Every limit must remain non-zero and the inbound frame and
/// message budgets must fit the inbound queue budget.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsSessionLimits;
///
/// let limits = WsSessionLimits { inbound_messages: 16, ..WsSessionLimits::default() };
/// assert_eq!(limits.inbound_messages, 16);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WsSessionLimits {
    /// Maximum payload bytes accepted in one inbound frame.
    pub max_inbound_frame_bytes: usize,
    /// Maximum payload bytes accepted in one reassembled inbound message.
    pub max_inbound_message_bytes: usize,
    /// Maximum number of queued inbound application messages.
    pub inbound_messages: usize,
    /// Maximum payload bytes held by the inbound application queue.
    pub inbound_bytes: usize,
    /// Maximum number of queued outbound application messages.
    pub outbound_messages: usize,
    /// Maximum payload bytes held by the outbound application queue.
    pub outbound_bytes: usize,
    /// Maximum time without progress while a required read or write is blocked.
    pub no_progress_timeout: Duration,
    /// Maximum time to wait for the peer's close response.
    pub close_timeout: Duration,
}

impl Default for WsSessionLimits {
    fn default() -> Self {
        Self {
            max_inbound_frame_bytes: 1024 * 1024,
            max_inbound_message_bytes: 4 * 1024 * 1024,
            inbound_messages: 64,
            inbound_bytes: 4 * 1024 * 1024,
            outbound_messages: 64,
            outbound_bytes: 4 * 1024 * 1024,
            no_progress_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(5),
        }
    }
}
