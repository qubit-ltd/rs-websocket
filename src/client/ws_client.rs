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
/// Build clients through [`WsClient::builder`] so the settings are validated
/// before any connection is opened.
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
    pub(crate) options: WsClientOptions,
}

impl WsClient {
    /// Starts building a WebSocket client.
    pub fn builder() -> super::WsClientBuilder {
        super::WsClientBuilder {
            options: WsClientOptions::default(),
        }
    }
}

impl WsClient {
    /// Opens a WebSocket using the client's default request headers.
    pub async fn connect(&self, url: &str) -> Result<WsSession, WsError> {
        self.connect_with(url, http::HeaderMap::new()).await
    }

    /// Opens a WebSocket with per-connection request headers.
    pub async fn connect_with(&self, url: &str, headers: http::HeaderMap) -> Result<WsSession, WsError> {
        let connected = connect_socket(url, &self.options, &headers).await?;
        Ok(WsSession::from_connected(connected, self.options.limits.clone()))
    }
}
