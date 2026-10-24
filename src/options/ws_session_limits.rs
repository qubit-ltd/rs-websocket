// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

/// Queue and timing limits applied to one WebSocket session.
///
/// Defaults allow 1 MiB inbound frames, 4 MiB inbound messages and queue byte
/// budgets, 64 messages in each queue, a 30-second no-progress deadline, and a
/// 5-second close deadline. Override only the budgets needed by the
/// application. Every limit must remain non-zero, inbound frame and message
/// budgets must fit the inbound queue budget. Queue capacities are represented
/// by `usize` and are not restricted by a semaphore permit limit.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsSessionLimits;
///
/// let limits = WsSessionLimits {
///     inbound_messages: 16,
///     ..WsSessionLimits::default()
/// };
/// assert_eq!(limits.inbound_messages, 16);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WsSessionLimits {
    /// Maximum payload bytes accepted in one inbound frame; must not exceed
    /// `max_inbound_message_bytes`.
    pub max_inbound_frame_bytes: usize,
    /// Maximum payload bytes accepted in one reassembled inbound message; must
    /// not exceed `inbound_bytes`.
    pub max_inbound_message_bytes: usize,
    /// Maximum number of queued inbound application messages; must be non-zero.
    pub inbound_messages: usize,
    /// Maximum payload bytes held by the inbound application queue; must be
    /// non-zero and at least both inbound size limits.
    pub inbound_bytes: usize,
    /// Maximum number of queued outbound application messages; must be
    /// non-zero.
    pub outbound_messages: usize,
    /// Maximum payload bytes held by the outbound application queue; must be
    /// non-zero and fit the WebSocket write-buffer overhead calculation.
    pub outbound_bytes: usize,
    /// Maximum time without progress while a required read or write is blocked;
    /// must be non-zero.
    pub no_progress_timeout: Duration,
    /// Maximum time to wait for the peer's close response; must be non-zero.
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
