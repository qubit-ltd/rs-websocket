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

use crate::error::WsError;
use crate::error::WsErrorKind;
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
    /// to 10 seconds and must be non-zero.
    pub connect_timeout: Duration,
    /// Combined TLS and HTTP upgrade handshake deadline; defaults to 10 seconds
    /// and must be non-zero.
    pub handshake_timeout: Duration,
    /// Default request headers; their values are omitted from debug output.
    /// Handshake-owned headers are rejected when building a client.
    pub headers: HeaderMap,
    /// Ordered WebSocket subprotocol offers; each must be a unique, non-empty
    /// HTTP token.
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
    /// Checks that queue budgets are non-zero, inbound frame/message limits
    /// fit the inbound byte budget, and outbound write-buffer arithmetic does
    /// not overflow.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when any timeout or limit constraint is
    /// violated.
    pub fn validate(&self) -> Result<(), WsError> {
        let limits = &self.limits;
        let nonzero = self.connect_timeout > Duration::ZERO
            && self.handshake_timeout > Duration::ZERO
            && limits.max_inbound_frame_bytes > 0
            && limits.max_inbound_message_bytes > 0
            && limits.inbound_messages > 0
            && limits.inbound_bytes > 0
            && limits.outbound_messages > 0
            && limits.outbound_bytes > 0
            && limits.no_progress_timeout > Duration::ZERO
            && limits.close_timeout > Duration::ZERO;
        let consistent = limits.max_inbound_frame_bytes <= limits.max_inbound_message_bytes
            && limits.max_inbound_message_bytes <= limits.inbound_bytes;
        let write_buffer_limit_fits = Self::checked_write_buffer_limit(limits.outbound_bytes).is_some();

        if nonzero && consistent && write_buffer_limit_fits {
            Ok(())
        } else {
            Err(WsError::new(WsErrorKind::InvalidConfig))
        }
    }
    /// Returns the write-buffer ceiling for an application payload budget.
    ///
    /// The extra capacity covers the largest data-frame header and one maximum
    /// client control frame, preserving Ping/Pong/Close progress independently
    /// of the application payload budget. `None` indicates arithmetic overflow.
    #[inline]
    pub(crate) const fn checked_write_buffer_limit(outbound_bytes: usize) -> Option<usize> {
        match outbound_bytes.checked_add(14) {
            Some(value) => value.checked_add(131),
            None => None,
        }
    }
}
