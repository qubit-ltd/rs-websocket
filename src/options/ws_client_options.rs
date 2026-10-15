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
/// `validate()` checks only numeric timeout and session-limit constraints; it
/// does not validate headers or subprotocol tokens and does not create a
/// client. Construct a client with [`crate::WsClientBuilder::build`] after
/// configuring a [`crate::WsClientBuilder`].
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
    /// Deadline for DNS resolution and TCP connection establishment.
    pub connect_timeout: Duration,
    /// Combined TLS and HTTP upgrade handshake deadline.
    pub handshake_timeout: Duration,
    /// Default request headers; their values are omitted from debug output.
    pub headers: HeaderMap,
    /// WebSocket subprotocols offered during the opening handshake.
    pub subprotocols: Vec<String>,
    /// TLS trust root mode used by secure connections.
    pub trust_roots: WsTrustRoots,
    /// Session memory and timing budgets.
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
    /// Checks that all limits are non-zero and that message budgets fit their
    /// queues.
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
        let semaphore_limits_fit = limits.inbound_bytes <= u32::MAX as usize
            && limits.outbound_bytes <= u32::MAX as usize
            && limits.inbound_messages <= tokio::sync::Semaphore::MAX_PERMITS
            && limits.outbound_messages <= tokio::sync::Semaphore::MAX_PERMITS;

        if nonzero && consistent && semaphore_limits_fit {
            Ok(())
        } else {
            Err(WsError::new(WsErrorKind::InvalidConfig))
        }
    }
}
