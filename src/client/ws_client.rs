// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::client::internal::connect_socket;
use crate::error::WsError;
use crate::options::WsClientOptions;
use crate::session::WsSession;

/// An immutable WebSocket client configuration snapshot.
///
/// Build clients through [`WsClient::builder`] so timeouts, session budgets,
/// request headers, and protocol tokens are validated before a connection is
/// opened. `ws` endpoints use TCP directly; `wss` endpoints always verify the
/// peer certificate chain and hostname, including when custom trust roots are
/// configured.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsClient;
///
/// let result = WsClient::builder().build();
/// assert!(result.is_ok());
/// ```
#[derive(Clone)]
pub struct WsClient {
    /// Validated, immutable configuration used for each connection.
    pub(crate) options: WsClientOptions,
}

impl WsClient {
    /// Starts building a WebSocket client.
    #[inline]
    #[must_use]
    pub fn builder() -> super::WsClientBuilder {
        super::WsClientBuilder {
            options: WsClientOptions::default(),
        }
    }
}

impl WsClient {
    /// Opens a WebSocket using the client's default request headers.
    ///
    /// The URL must be an absolute `ws` or `wss` URL with a valid host and
    /// port. TCP connection and opening-handshake deadlines use the configured
    /// timeouts. Errors distinguish invalid URLs/configuration, transport,
    /// TLS, handshake, protocol, and timeout failures.
    pub async fn connect(&self, url: &str) -> Result<WsSession, WsError> {
        self.connect_with(url, http::HeaderMap::new()).await
    }

    /// Opens a WebSocket with per-connection request headers.
    ///
    /// Per-connection values replace default values of the same header name;
    /// headers owned by the WebSocket handshake are rejected as
    /// `InvalidConfig`. The URL and connection errors have the same conditions
    /// as [`WsClient::connect`].
    pub async fn connect_with(&self, url: &str, headers: http::HeaderMap) -> Result<WsSession, WsError> {
        let connected = connect_socket(url, &self.options, &headers).await?;
        Ok(WsSession::from_connected(connected, self.options.limits.clone()))
    }
}
