// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use http::HeaderMap;

use crate::client::internal::TlsConfigCache;
use crate::client::internal::connect_socket;
use crate::error::WsError;
use crate::options::WsClientOptions;
use crate::session::WsSession;

/// An immutable WebSocket client configuration snapshot.
///
/// Build clients through [`WsClient::builder`] so timeouts, session budgets,
/// request headers, and protocol tokens are validated before a connection is
/// opened. `ws` endpoints use TCP directly without initializing TLS. Secure
/// connections lazily build and reuse TLS configuration for this client;
/// clones share that configuration cache, while independently built clients
/// keep their trust policies and TLS session resumption state separate.
/// `wss` endpoints use an already installed rustls provider, or use a
/// per-client aws-lc fallback until a default provider is installed. WSS always
/// verifies the peer certificate chain and hostname, including when custom
/// trust roots are configured. Applications that require ring or a custom
/// provider should install it with `CryptoProvider::install_default` before
/// the first WSS connection.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsClient;
///
/// # async fn example() -> Result<(), qubit_websocket::WsError> {
/// let client = WsClient::builder().build()?;
/// let _session = client.connect("wss://events.example.com/ws").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct WsClient {
    /// Validated, immutable configuration used for each connection.
    pub(crate) options: WsClientOptions,
    /// Lazily built TLS configuration shared by this client and its clones.
    pub(crate) tls_config_cache: Arc<TlsConfigCache>,
}

impl WsClient {
    /// Starts building a WebSocket client.
    ///
    /// # Returns
    ///
    /// Returns a builder initialized with the client's default options.
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
    /// timeouts. For WSS, TLS configuration waiting and construction, TLS
    /// negotiation, and HTTP upgrade share the handshake timeout. Configuration
    /// construction runs on a blocking worker and may finish after timeout or
    /// cancellation; it never owns the TCP socket. Runtime scheduling means
    /// the configured timeout sum is not a strict wall-clock bound for the
    /// whole call. Errors distinguish invalid
    /// URLs/configuration, transport, TLS, handshake, protocol, and timeout
    /// failures. When no rustls provider is installed, WSS uses the
    /// client's cached aws-lc fallback. A provider installed later is used
    /// by the next WSS connection; an installed provider that cannot
    /// negotiate a safe protocol version produces `InvalidConfig`.
    ///
    /// # Parameters
    ///
    /// - `url`: Absolute `ws` or `wss` URL identifying the endpoint.
    ///
    /// # Returns
    ///
    /// Returns the established session after the opening handshake succeeds.
    ///
    /// # Errors
    ///
    /// Returns a [`WsError`] when the URL or client configuration is invalid,
    /// or when DNS, TCP, TLS, the handshake, the protocol, or a timeout fails.
    pub async fn connect(&self, url: &str) -> Result<WsSession, WsError> {
        self.connect_with(url, HeaderMap::new()).await
    }

    /// Opens a WebSocket with per-connection request headers.
    ///
    /// Per-connection values replace default values of the same header name;
    /// headers owned by the WebSocket handshake are rejected as
    /// `InvalidConfig`. The URL and connection errors have the same conditions
    /// as [`WsClient::connect`].
    ///
    /// # Parameters
    ///
    /// - `url`: Absolute `ws` or `wss` URL identifying the endpoint.
    /// - `headers`: Request headers that override defaults with matching names.
    ///
    /// # Returns
    ///
    /// Returns the established session after the opening handshake succeeds.
    ///
    /// # Errors
    ///
    /// Returns a [`WsError`] when the URL or client configuration is invalid,
    /// caller headers include handshake-owned fields, or a connection stage
    /// fails as described by [`WsClient::connect`], including an incompatible
    /// installed rustls provider for WSS.
    pub async fn connect_with(&self, url: &str, headers: HeaderMap) -> Result<WsSession, WsError> {
        let connected = connect_socket(url, &self.options, &self.tls_config_cache, &headers).await?;
        Ok(WsSession::from_connected(connected, self.options.limits.clone()))
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::WsClient;

    #[tokio::test]
    async fn test_client_clones_share_tls_configuration() {
        let client = WsClient::builder().build().expect("build client");
        let clone = client.clone();

        let first = client.tls_config_cache.get().await.expect("build client TLS config");
        let second = clone
            .tls_config_cache
            .get()
            .await
            .expect("reuse cloned client TLS config");

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn test_independent_clients_isolate_tls_trust_policy() {
        let first_client = WsClient::builder()
            .exclusive_root_certificates(rustls::RootCertStore::empty())
            .build()
            .expect("build first client");
        let second_client = WsClient::builder().build().expect("build second client");

        let first = first_client
            .tls_config_cache
            .get()
            .await
            .expect("build exclusive-trust TLS config");
        let second = second_client
            .tls_config_cache
            .get()
            .await
            .expect("build default-trust TLS config");

        assert!(!Arc::ptr_eq(&first, &second));
    }
}
