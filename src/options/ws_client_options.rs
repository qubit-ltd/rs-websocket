// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;
use std::time::Duration;

use http::HeaderMap;
use tokio::time::Instant;

use crate::error::WsConfigFailure;
use crate::error::WsError;
use crate::options::WsSessionLimits;
use crate::options::WsTrustRoots;

/// Immutable settings snapshot used to construct WebSocket clients.
///
/// Defaults use 10-second TCP and opening-handshake deadlines, no extra request
/// headers or subprotocol offers, standard WebPKI roots, and
/// [`WsSessionLimits::default`]. `validate()` checks timeout and session-limit
/// constraints, but does not validate headers or subprotocol tokens and does
/// not create a client. Construct a client with
/// [`crate::WsClientBuilder::build`] after configuring a
/// [`crate::WsClientBuilder`].
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsClientOptions;
///
/// let options = WsClientOptions::default();
/// assert!(options.validate().is_ok());
/// ```
#[derive(Clone)]
pub struct WsClientOptions {
    /// Deadline for DNS resolution and TCP connection establishment; defaults
    /// to 10 seconds and must be non-zero and representable by the runtime
    /// clock. An unrepresentable deadline is rejected as `InvalidConfig`.
    pub connect_timeout: Duration,
    /// Combined TLS and HTTP upgrade handshake deadline; defaults to 10 seconds
    /// and must be non-zero and representable by the runtime clock. An
    /// unrepresentable deadline is rejected as `InvalidConfig`.
    pub handshake_timeout: Duration,
    /// Default request headers; their values are omitted from debug output.
    /// Handshake-owned headers are rejected when building a client.
    pub headers: HeaderMap,
    /// Ordered WebSocket subprotocol offers; each must be a unique, non-empty
    /// HTTP token. When non-empty, the server must select one of these values
    /// or the handshake fails.
    pub subprotocols: Vec<String>,
    /// TLS trust root mode used by secure connections; certificate and
    /// hostname validation remain enabled in all modes.
    pub trust_roots: WsTrustRoots,
    /// Session memory and timing budgets, all required to be non-zero and
    /// internally consistent.
    pub limits: WsSessionLimits,
}

impl Default for WsClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(10),
            headers: HeaderMap::new(),
            subprotocols: Vec::new(),
            trust_roots: WsTrustRoots::Default,
            limits: WsSessionLimits::default(),
        }
    }
}

impl fmt::Debug for WsClientOptions {
    /// Formats configuration metadata while omitting request header values.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsClientOptions")
            .field("connect_timeout", &self.connect_timeout)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("header_count", &self.headers.len())
            .field("subprotocol_count", &self.subprotocols.len())
            .field("trust_roots", &self.trust_roots)
            .field("limits", &self.limits)
            .finish()
    }
}

impl WsClientOptions {
    /// Checks that connection and session deadlines are non-zero and
    /// representable by the runtime clock, queue budgets are valid, inbound
    /// frame/message limits fit the inbound byte budget, and outbound
    /// write-buffer arithmetic does not overflow.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when any timeout is zero or its deadline
    /// cannot be represented by the runtime clock, or when any other limit
    /// constraint is violated.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when all timeout and queue-limit constraints are
    /// satisfied.
    pub fn validate(&self) -> Result<(), WsError> {
        let limits = &self.limits;
        for (is_zero, failure) in [
            (self.connect_timeout.is_zero(), WsConfigFailure::ZeroConnectTimeout),
            (self.handshake_timeout.is_zero(), WsConfigFailure::ZeroHandshakeTimeout),
            (
                limits.max_inbound_frame_bytes == 0,
                WsConfigFailure::ZeroMaxInboundFrameBytes,
            ),
            (
                limits.max_inbound_message_bytes == 0,
                WsConfigFailure::ZeroMaxInboundMessageBytes,
            ),
            (limits.inbound_messages == 0, WsConfigFailure::ZeroInboundMessages),
            (limits.inbound_bytes == 0, WsConfigFailure::ZeroInboundBytes),
            (limits.outbound_messages == 0, WsConfigFailure::ZeroOutboundMessages),
            (limits.outbound_bytes == 0, WsConfigFailure::ZeroOutboundBytes),
            (
                limits.no_progress_timeout.is_zero(),
                WsConfigFailure::ZeroNoProgressTimeout,
            ),
            (limits.close_timeout.is_zero(), WsConfigFailure::ZeroCloseTimeout),
        ] {
            if is_zero {
                return Err(WsError::invalid_config(failure));
            }
        }
        if limits.max_inbound_frame_bytes > limits.max_inbound_message_bytes {
            return Err(WsError::invalid_config(WsConfigFailure::FrameExceedsMessageLimit));
        }
        if limits.max_inbound_message_bytes > limits.inbound_bytes {
            return Err(WsError::invalid_config(WsConfigFailure::MessageExceedsInboundBudget));
        }
        if Self::checked_write_buffer_limit(limits.outbound_bytes).is_none() {
            return Err(WsError::invalid_config(WsConfigFailure::OutboundWriteBufferOverflow));
        }
        let now = Instant::now();
        for (fits, failure) in [
            (
                now.checked_add(self.connect_timeout).is_some(),
                WsConfigFailure::UnrepresentableConnectTimeout,
            ),
            (
                now.checked_add(self.handshake_timeout).is_some(),
                WsConfigFailure::UnrepresentableHandshakeTimeout,
            ),
            (
                now.checked_add(limits.close_timeout).is_some(),
                WsConfigFailure::UnrepresentableCloseTimeout,
            ),
            (
                now.checked_add(limits.no_progress_timeout).is_some(),
                WsConfigFailure::UnrepresentableNoProgressTimeout,
            ),
        ] {
            if !fits {
                return Err(WsError::invalid_config(failure));
            }
        }
        Ok(())
    }

    /// Returns the write-buffer ceiling for an application payload budget.
    ///
    /// The extra capacity covers the largest data-frame header and one maximum
    /// client control frame, preserving Ping/Pong/Close progress independently
    /// of the application payload budget. `None` indicates arithmetic overflow.
    ///
    /// # Returns
    ///
    /// Returns the computed write-buffer ceiling, or `None` if adding the
    /// protocol overhead would overflow `usize`.
    #[inline]
    pub(crate) const fn checked_write_buffer_limit(outbound_bytes: usize) -> Option<usize> {
        match outbound_bytes.checked_add(14) {
            Some(value) => value.checked_add(131),
            None => None,
        }
    }
}
