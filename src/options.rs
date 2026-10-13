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
use rustls::RootCertStore;

use crate::error::WsError;
use crate::error::WsErrorKind;

/// Selects the trusted certificate authorities used for secure WebSocket connections.
#[derive(Clone)]
pub enum WsTrustRoots {
    /// Use the standard WebPKI server roots.
    Default,
    /// Use the standard WebPKI roots plus the supplied roots.
    Additional(RootCertStore),
    /// Use only the supplied roots.
    Exclusive(RootCertStore),
}

impl fmt::Debug for WsTrustRoots {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("Default"),
            Self::Additional(roots) => f.debug_struct("Additional").field("root_count", &roots.len()).finish(),
            Self::Exclusive(roots) => f.debug_struct("Exclusive").field("root_count", &roots.len()).finish(),
        }
    }
}

/// Memory and timing limits applied to one WebSocket session.
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

/// Immutable settings snapshot used to construct WebSocket clients.
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
