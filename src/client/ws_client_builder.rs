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
use crate::client::WsClient;
use crate::options::WsClientOptions;
use crate::options::WsSessionLimits;
use crate::options::WsTrustRoots;

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
    /// Configuration being assembled and validated by
    /// [`WsClientBuilder::build`].
    pub(super) options: WsClientOptions,
}

impl WsClientBuilder {
    /// Sets the default request headers used by each connection.
    ///
    /// Handshake-owned headers are rejected by [`WsClientBuilder::build`].
    #[must_use]
    #[inline]
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.options.headers = headers;
        self
    }

    /// Sets the ordered WebSocket subprotocol offers.
    ///
    /// Each value must be a unique, non-empty HTTP token; invalid offers are
    /// rejected by [`WsClientBuilder::build`].
    #[must_use]
    #[inline]
    pub fn subprotocols(mut self, subprotocols: Vec<String>) -> Self {
        self.options.subprotocols = subprotocols;
        self
    }

    /// Sets the TCP connection timeout.
    ///
    /// A zero duration is rejected by [`WsClientBuilder::build`].
    #[must_use]
    #[inline]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.options.connect_timeout = timeout;
        self
    }

    /// Sets the TLS and HTTP opening handshake timeout.
    ///
    /// A zero duration is rejected by [`WsClientBuilder::build`].
    #[must_use]
    #[inline]
    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.options.handshake_timeout = timeout;
        self
    }

    /// Sets the limits applied to each established session.
    ///
    /// Zero or inconsistent limits are rejected by [`WsClientBuilder::build`].
    #[must_use]
    #[inline]
    pub fn limits(mut self, limits: WsSessionLimits) -> Self {
        self.options.limits = limits;
        self
    }

    /// Adds explicit TLS trust roots while retaining certificate and hostname
    /// checks, in addition to the standard WebPKI roots.
    #[must_use]
    #[inline]
    pub fn root_certificates(mut self, roots: rustls::RootCertStore) -> Self {
        self.options.trust_roots = WsTrustRoots::Additional(roots);
        self
    }

    /// Uses only the supplied TLS trust roots while retaining certificate and
    /// hostname checks.
    #[must_use]
    #[inline]
    pub fn exclusive_root_certificates(mut self, roots: rustls::RootCertStore) -> Self {
        self.options.trust_roots = WsTrustRoots::Exclusive(roots);
        self
    }

    /// Builds a client after validating timeouts, session budgets, headers,
    /// and protocol tokens.
    ///
    /// Returns `InvalidConfig` for zero or inconsistent limits, zero
    /// timeouts, invalid or duplicate subprotocol tokens, or handshake-owned
    /// request headers.
    pub fn build(self) -> Result<WsClient, WsError> {
        self.options.validate()?;
        validate_subprotocols(&self.options.subprotocols)?;
        validate_user_headers(&self.options.headers)?;
        Ok(WsClient { options: self.options })
    }
}

/// Validates that offered subprotocols are unique, non-empty HTTP tokens.
///
/// Returns `InvalidConfig` when any value is empty, invalid, or duplicated.
pub(super) fn validate_subprotocols(protocols: &[String]) -> Result<(), WsError> {
    let mut seen = std::collections::HashSet::with_capacity(protocols.len());
    for protocol in protocols {
        if protocol.is_empty() || !protocol.bytes().all(is_http_token_byte) || !seen.insert(protocol) {
            return Err(WsError::new(WsErrorKind::InvalidConfig));
        }
    }
    Ok(())
}

/// Returns whether a byte is allowed in an HTTP token, as used by subprotocols.
fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
        )
}

/// Identifies headers reserved for the WebSocket opening handshake.
///
/// The `Host`, `Connection`, `Upgrade`, and `Sec-WebSocket-*` fields are
/// reserved.
fn is_reserved_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    name.eq_ignore_ascii_case("host")
        || name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("upgrade")
        || name.to_ascii_lowercase().starts_with("sec-websocket-")
}

/// Rejects caller headers that would override fields owned by the handshake.
///
/// Returns `InvalidConfig` if a reserved header is present.
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
///
/// Returns `InvalidConfig` if a reserved header is present.
pub(super) fn apply_user_headers(target: &mut HeaderMap, headers: &HeaderMap) -> Result<(), WsError> {
    validate_user_headers(headers)?;
    for name in headers.keys() {
        target.remove(name);
        for value in headers.get_all(name) {
            target.append(name.clone(), value.clone());
        }
    }
    Ok(())
}

/// Appends the validated subprotocol offer as the handshake request header.
///
/// Returns `InvalidConfig` if the joined value cannot be represented as a
/// valid HTTP header value.
pub(super) fn append_subprotocol_header(headers: &mut HeaderMap, protocols: &[String]) -> Result<(), WsError> {
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

    use super::append_subprotocol_header;
    use crate::error::WsErrorKind;
    use crate::options::WsSessionLimits;
    use crate::options::WsTrustRoots;

    #[test]
    fn test_builder_keeps_custom_session_limits() {
        let limits = WsSessionLimits {
            inbound_messages: 2,
            ..WsSessionLimits::default()
        };
        let client = crate::client::WsClient::builder()
            .limits(limits.clone())
            .build()
            .unwrap();

        assert_eq!(client.options.limits, limits);
    }

    #[test]
    fn test_builder_keeps_connection_settings() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test".parse().unwrap());
        let roots = RootCertStore::empty();
        let client = crate::client::WsClient::builder()
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
    fn test_last_trust_root_builder_call_wins() {
        let additional = RootCertStore::empty();
        let exclusive = RootCertStore::empty();
        let client = crate::client::WsClient::builder()
            .exclusive_root_certificates(exclusive)
            .root_certificates(additional)
            .build()
            .unwrap();

        assert!(matches!(client.options.trust_roots, WsTrustRoots::Additional(_)));
    }

    #[test]
    fn test_subprotocol_header_rejects_an_invalid_value() {
        let mut headers = HeaderMap::new();
        let error = append_subprotocol_header(&mut headers, &["chat\nprotocol".to_owned()]).unwrap_err();

        assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
        assert!(!headers.contains_key("sec-websocket-protocol"));
    }
}
