// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;

use crate::WsError;
use crate::WsErrorKind;
use crate::options::WsClientOptions;
use crate::options::WsSessionLimits;
use crate::options::WsTrustRoots;

pub(crate) mod handshake;

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

/// Builder for a validated [`WsClient`] configuration.
///
/// # Examples
///
/// ```
/// use qubit_websocket::{WsClient, WsSessionLimits};
///
/// let result = WsClient::builder()
///     .limits(WsSessionLimits { inbound_messages: 8, ..WsSessionLimits::default() })
///     .build();
/// assert!(result.is_ok());
/// ```
#[derive(Default)]
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

    /// Sets the limits applied to each established session.
    pub fn limits(mut self, limits: WsSessionLimits) -> Self {
        self.options.limits = limits;
        self
    }

    /// Adds explicit TLS trust roots while retaining certificate and hostname
    /// checks, in addition to the standard WebPKI roots.
    pub fn root_certificates(mut self, roots: rustls::RootCertStore) -> Self {
        self.options.trust_roots = WsTrustRoots::Additional(roots);
        self
    }

    /// Uses only the supplied TLS trust roots while retaining certificate and
    /// hostname checks.
    pub fn exclusive_root_certificates(mut self, roots: rustls::RootCertStore) -> Self {
        self.options.trust_roots = WsTrustRoots::Exclusive(roots);
        self
    }

    /// Builds a client after validating the configuration and protocol tokens.
    pub fn build(self) -> Result<WsClient, WsError> {
        self.options.validate()?;
        validate_subprotocols(&self.options.subprotocols)?;
        validate_user_headers(&self.options.headers)?;
        Ok(WsClient { options: self.options })
    }
}

fn validate_subprotocols(protocols: &[String]) -> Result<(), WsError> {
    let mut seen = std::collections::HashSet::with_capacity(protocols.len());
    for protocol in protocols {
        if protocol.is_empty() || !protocol.bytes().all(is_http_token_byte) || !seen.insert(protocol) {
            return Err(WsError::new(WsErrorKind::InvalidConfig));
        }
    }
    Ok(())
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
        )
}

fn is_reserved_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    name.eq_ignore_ascii_case("host")
        || name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("upgrade")
        || name.to_ascii_lowercase().starts_with("sec-websocket-")
}

fn validate_user_headers(headers: &HeaderMap) -> Result<(), WsError> {
    if headers.keys().any(is_reserved_header) {
        return Err(WsError::new(WsErrorKind::InvalidConfig));
    }
    Ok(())
}

/// Applies one caller-supplied header set, replacing each matching name as a
/// whole.
///
/// All values for a name in `headers` are appended in their original order, so
/// repeated values within this source are preserved. A later source therefore
/// replaces every earlier value for the same name without affecting other
/// names. Headers owned by the WebSocket protocol are rejected before `target`
/// changes.
fn apply_user_headers(target: &mut HeaderMap, headers: &HeaderMap) -> Result<(), WsError> {
    validate_user_headers(headers)?;
    for name in headers.keys() {
        target.remove(name);
        for value in headers.get_all(name) {
            target.append(name.clone(), value.clone());
        }
    }
    Ok(())
}

fn append_subprotocol_header(headers: &mut HeaderMap, protocols: &[String]) -> Result<(), WsError> {
    if protocols.is_empty() {
        return Ok(());
    }
    let value = HeaderValue::from_str(&protocols.join(", ")).map_err(|_| WsError::new(WsErrorKind::InvalidConfig))?;
    headers.insert("sec-websocket-protocol", value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http::HeaderMap;
    use rustls::RootCertStore;

    use super::WsClient;
    use super::append_subprotocol_header;
    use crate::error::WsErrorKind;
    use crate::options::WsSessionLimits;
    use crate::options::WsTrustRoots;

    #[test]
    fn builder_keeps_custom_session_limits() {
        let limits = WsSessionLimits {
            inbound_messages: 2,
            ..WsSessionLimits::default()
        };
        let client = WsClient::builder().limits(limits.clone()).build().unwrap();
        assert_eq!(client.options.limits, limits);
    }

    #[test]
    fn builder_rejects_zero_session_limits() {
        let limits = WsSessionLimits {
            inbound_messages: 0,
            ..WsSessionLimits::default()
        };
        let error = match WsClient::builder().limits(limits).build() {
            Ok(_) => panic!("zero session limits must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
    }

    #[test]
    fn builder_rejects_reserved_default_headers() {
        for name in ["host", "connection", "upgrade", "sec-websocket-protocol"] {
            let mut headers = HeaderMap::new();
            headers.insert(name, "test".parse().unwrap());

            let error = match WsClient::builder().headers(headers).build() {
                Ok(_) => panic!("reserved default header {name} must be rejected"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), WsErrorKind::InvalidConfig, "header: {name}");
        }

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test".parse().unwrap());
        assert!(WsClient::builder().headers(headers).build().is_ok());
    }

    #[test]
    fn builder_keeps_connection_settings() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test".parse().unwrap());
        let roots = RootCertStore::empty();
        let client = WsClient::builder()
            .headers(headers.clone())
            .subprotocols(vec!["chat.v1".to_owned()])
            .connect_timeout(Duration::from_secs(3))
            .handshake_timeout(Duration::from_secs(4))
            .root_certificates(roots)
            .build()
            .unwrap();

        assert_eq!(client.options.headers, headers);
        assert_eq!(client.options.subprotocols, ["chat.v1"]);
        assert_eq!(client.options.connect_timeout, Duration::from_secs(3));
        assert_eq!(client.options.handshake_timeout, Duration::from_secs(4));
        assert!(matches!(client.options.trust_roots, WsTrustRoots::Additional(_)));
    }

    #[test]
    fn last_trust_root_builder_call_wins() {
        let additional = RootCertStore::empty();
        let exclusive = RootCertStore::empty();
        let client = WsClient::builder()
            .exclusive_root_certificates(exclusive)
            .root_certificates(additional)
            .build()
            .unwrap();

        assert!(matches!(client.options.trust_roots, WsTrustRoots::Additional(_)));
    }

    #[test]
    fn subprotocol_header_rejects_an_invalid_value() {
        let mut headers = HeaderMap::new();
        let error = append_subprotocol_header(&mut headers, &["chat\nprotocol".to_owned()]).unwrap_err();

        assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
        assert!(!headers.contains_key("sec-websocket-protocol"));
    }
}
