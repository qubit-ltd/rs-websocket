// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use http::{HeaderMap, HeaderName, HeaderValue};

use crate::options::WsClientOptions;
use crate::{WsError, WsErrorKind};

mod handshake;

/// An immutable WebSocket client configuration snapshot.
#[derive(Clone)]
pub struct WsClient {
    pub(crate) options: WsClientOptions,
}

/// Builder for a validated [`WsClient`] configuration.
pub struct WsClientBuilder {
    options: WsClientOptions,
}

impl WsClient {
    /// Starts building a WebSocket client.
    pub fn builder() -> WsClientBuilder {
        WsClientBuilder {
            options: WsClientOptions::default(),
        }
    }
}

impl Default for WsClientBuilder {
    fn default() -> Self {
        Self {
            options: WsClientOptions::default(),
        }
    }
}

impl WsClientBuilder {
    /// Sets the default request headers used by each connection.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.options.headers = headers;
        self
    }

    /// Sets the ordered WebSocket subprotocol offers.
    pub fn subprotocols(mut self, subprotocols: Vec<String>) -> Self {
        self.options.subprotocols = subprotocols;
        self
    }

    /// Sets the TCP connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.options.connect_timeout = timeout;
        self
    }

    /// Sets the TLS and HTTP opening handshake timeout.
    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.options.handshake_timeout = timeout;
        self
    }

    /// Builds a client after validating the configuration and protocol tokens.
    pub fn build(self) -> Result<WsClient, WsError> {
        self.options.validate()?;
        validate_subprotocols(&self.options.subprotocols)?;
        Ok(WsClient {
            options: self.options,
        })
    }
}

fn validate_subprotocols(protocols: &[String]) -> Result<(), WsError> {
    let mut seen = std::collections::HashSet::with_capacity(protocols.len());
    for protocol in protocols {
        if protocol.is_empty()
            || !protocol.bytes().all(is_http_token_byte)
            || !seen.insert(protocol)
        {
            return Err(WsError::new(WsErrorKind::InvalidConfig));
        }
    }
    Ok(())
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn is_reserved_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    name.eq_ignore_ascii_case("host")
        || name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("upgrade")
        || name.to_ascii_lowercase().starts_with("sec-websocket-")
}

fn apply_user_headers(target: &mut HeaderMap, headers: &HeaderMap) -> Result<(), WsError> {
    for (name, value) in headers {
        if is_reserved_header(name) {
            return Err(WsError::new(WsErrorKind::InvalidConfig));
        }
        target.insert(name.clone(), value.clone());
    }
    Ok(())
}

fn append_subprotocol_header(headers: &mut HeaderMap, protocols: &[String]) -> Result<(), WsError> {
    if protocols.is_empty() {
        return Ok(());
    }
    let value = HeaderValue::from_str(&protocols.join(", "))
        .map_err(|_| WsError::new(WsErrorKind::InvalidConfig))?;
    headers.insert("sec-websocket-protocol", value);
    Ok(())
}
