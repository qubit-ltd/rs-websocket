use std::fmt;
use std::time::Duration;

use http::HeaderMap;
use rustls::RootCertStore;

use crate::error::{WsError, WsErrorKind};

/// Memory and timing limits applied to one WebSocket session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WsSessionLimits {
    pub max_frame_bytes: usize,
    pub max_message_bytes: usize,
    pub inbound_messages: usize,
    pub inbound_bytes: usize,
    pub outbound_messages: usize,
    pub outbound_bytes: usize,
    pub no_progress_timeout: Duration,
    pub close_timeout: Duration,
}

impl Default for WsSessionLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_message_bytes: 4 * 1024 * 1024,
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
    pub connect_timeout: Duration,
    pub handshake_timeout: Duration,
    pub headers: HeaderMap,
    pub subprotocols: Vec<String>,
    /// Optional additional trust roots, primarily for private PKI deployments and tests.
    pub root_certificates: Option<RootCertStore>,
    pub limits: WsSessionLimits,
}

impl Default for WsClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(10),
            headers: HeaderMap::new(),
            subprotocols: Vec::new(),
            root_certificates: None,
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
            .field("has_custom_roots", &self.root_certificates.is_some())
            .field("limits", &self.limits)
            .finish()
    }
}

impl WsClientOptions {
    /// Checks that all limits are non-zero and that message budgets fit their queues.
    pub fn validate(&self) -> Result<(), WsError> {
        let limits = &self.limits;
        let nonzero = self.connect_timeout > Duration::ZERO
            && self.handshake_timeout > Duration::ZERO
            && limits.max_frame_bytes > 0
            && limits.max_message_bytes > 0
            && limits.inbound_messages > 0
            && limits.inbound_bytes > 0
            && limits.outbound_messages > 0
            && limits.outbound_bytes > 0
            && limits.no_progress_timeout > Duration::ZERO
            && limits.close_timeout > Duration::ZERO;
        let consistent = limits.max_frame_bytes <= limits.max_message_bytes
            && limits.max_message_bytes <= limits.inbound_bytes
            && limits.max_message_bytes <= limits.outbound_bytes;
        let semaphore_limits_fit =
            limits.inbound_bytes <= u32::MAX as usize && limits.outbound_bytes <= u32::MAX as usize;

        if nonzero && consistent && semaphore_limits_fit {
            Ok(())
        } else {
            Err(WsError::new(WsErrorKind::InvalidConfig))
        }
    }
}
