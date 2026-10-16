// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::net::Ipv6Addr;
use std::sync::Arc;

use http::HeaderMap;
use http::Request;
use http::Uri;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::Connector;
use tokio_tungstenite::client_async_tls_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use super::ConnectedSocket;
use crate::client::ws_client_builder::append_subprotocol_header;
use crate::client::ws_client_builder::apply_user_headers;
use crate::error::WsError;
use crate::error::WsErrorKind;
use crate::error::WsHandshakeFailure;
use crate::options::WsClientOptions;
use crate::options::WsTrustRoots;

/// Opens a WebSocket connection with separate TCP and opening-handshake
/// deadlines.
///
/// The TCP connect is bounded by `connect_timeout`; TLS negotiation and the
/// HTTP upgrade are bounded together by `handshake_timeout`. Each call builds a
/// fresh request from the configured defaults and the headers supplied for this
/// connection. Timeout failures use `Timeout`, TCP failures use `Transport`,
/// ordinary I/O failures during TLS or HTTP upgrade use `Transport`, TLS
/// failures use `Tls`, and HTTP status or known upgrade-validation failures use
/// `Handshake`; other WebSocket protocol failures use `Protocol`.
pub(crate) async fn connect_socket(
    url: &str,
    options: &WsClientOptions,
    headers: &HeaderMap,
) -> Result<ConnectedSocket, WsError> {
    options.validate()?;
    super::super::ws_client_builder::validate_subprotocols(&options.subprotocols)?;
    let uri = parse_websocket_uri(url)?;
    let authority = uri.authority().ok_or_else(invalid_url)?;
    let host = normalize_tcp_host(authority.host())?;
    if host.is_empty() || authority.as_str().contains('@') {
        return Err(invalid_url());
    }
    let port = match authority.port_u16() {
        Some(port) => port,
        None if authority.port().is_none() => match uri.scheme_str() {
            Some("ws") => 80,
            Some("wss") => 443,
            _ => return Err(invalid_url()),
        },
        None => return Err(invalid_url()),
    };

    let request = build_request(uri, options, headers)?;

    let tcp = timeout(options.connect_timeout, TcpStream::connect((host, port)))
        .await
        .map_err(|_| WsError::new(WsErrorKind::Timeout))?
        .map_err(|error| WsError::with_source(WsErrorKind::Transport, error))?;

    let mut config = WebSocketConfig::default();
    config.max_frame_size = Some(options.limits.max_inbound_frame_bytes);
    config.max_message_size = Some(options.limits.max_inbound_message_bytes);
    config.write_buffer_size = 0;
    config.max_write_buffer_size = WsClientOptions::checked_write_buffer_limit(options.limits.outbound_bytes)
        .ok_or_else(|| WsError::new(WsErrorKind::InvalidConfig))?;

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(build_root_store(&options.trust_roots))
        .with_no_client_auth();
    let connector = Some(Connector::Rustls(Arc::new(tls_config)));

    let (stream, response) = timeout(
        options.handshake_timeout,
        client_async_tls_with_config(request, tcp, Some(config), connector),
    )
    .await
    .map_err(|_| WsError::new(WsErrorKind::Timeout))?
    .map_err(map_handshake_error)?;

    let status_code = response.status().as_u16();
    let selected_subprotocol = selected_subprotocol(response.headers(), &options.subprotocols)?;
    Ok(ConnectedSocket {
        stream,
        selected_subprotocol,
        status_code,
    })
}

/// Removes URL brackets from an IPv6 literal before creating the TCP target.
/// DNS names and IPv4 literals are passed through unchanged.
fn normalize_tcp_host(host: &str) -> Result<String, WsError> {
    if host.contains('[') || host.contains(']') {
        let literal = host
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .ok_or_else(invalid_url)?;
        let address = literal.parse::<Ipv6Addr>().map_err(|_| invalid_url())?;
        return Ok(address.to_string());
    }
    Ok(host.to_owned())
}

/// Builds the effective TLS trust store for the selected trust-root mode.
///
/// `Default` uses the standard WebPKI roots, `Additional` appends supplied
/// roots to those defaults, and `Exclusive` uses only the supplied roots.
/// Certificate and hostname verification remain enabled in every mode.
fn build_root_store(trust_roots: &WsTrustRoots) -> rustls::RootCertStore {
    match trust_roots {
        WsTrustRoots::Default => webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
        WsTrustRoots::Additional(additional) => {
            let mut roots: rustls::RootCertStore = webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect();
            roots.roots.extend(additional.roots.iter().cloned());
            roots
        }
        WsTrustRoots::Exclusive(roots) => roots.clone(),
    }
}

/// Builds a new opening request from the endpoint and header configuration.
///
/// The request starts with tungstenite's standard WebSocket headers, then
/// configured default headers are applied, followed by per-connection headers;
/// a per-connection value therefore replaces a default value with the same
/// name. Both user header sets reject `Host`, `Connection`, `Upgrade`, and
/// every `Sec-WebSocket-*` header so callers cannot override handshake-owned
/// fields. Offered subprotocols are added after user headers are validated.
/// Invalid header names or values return `InvalidConfig`.
fn build_request(uri: Uri, options: &WsClientOptions, headers: &HeaderMap) -> Result<Request<()>, WsError> {
    let mut request = uri.into_client_request().map_err(|_| invalid_url())?;
    apply_user_headers(request.headers_mut(), &options.headers)?;
    apply_user_headers(request.headers_mut(), headers)?;
    append_subprotocol_header(request.headers_mut(), &options.subprotocols)?;
    Ok(request)
}

/// Parses and validates an endpoint accepted by the WebSocket client.
///
/// Only absolute `ws` and `wss` URIs with a non-empty host are accepted. User
/// information, fragments, malformed ports, and ports outside the `u16` range
/// are rejected as `InvalidUrl`.
fn parse_websocket_uri(url: &str) -> Result<Uri, WsError> {
    if url.contains('#') {
        return Err(invalid_url());
    }
    let uri: Uri = url.parse().map_err(|_| invalid_url())?;
    if !matches!(uri.scheme_str(), Some("ws" | "wss")) {
        return Err(invalid_url());
    }
    let authority = uri.authority().ok_or_else(invalid_url)?;
    if authority.as_str().contains('@') || authority.host().is_empty() {
        return Err(invalid_url());
    }
    let authority_text = authority.as_str();
    if authority_text.contains('[') || authority_text.contains(']') {
        if !authority_text.starts_with('[') {
            return Err(invalid_url());
        }
        let closing_bracket = authority_text.find(']').ok_or_else(invalid_url)?;
        let literal = &authority_text[1..closing_bracket];
        let suffix = &authority_text[closing_bracket + 1..];
        if literal.contains('[') || suffix.contains('[') || suffix.contains(']') {
            return Err(invalid_url());
        }
        if !suffix.is_empty() && !suffix.starts_with(':') {
            return Err(invalid_url());
        }
        literal.parse::<Ipv6Addr>().map_err(|_| invalid_url())?;
    }
    if authority.port().is_none() && !authority.as_str().starts_with('[') && authority.as_str().contains(':') {
        return Err(invalid_url());
    }
    if authority.port().is_some() && authority.port_u16().is_none() {
        return Err(invalid_url());
    }
    Ok(uri)
}

/// Validates and returns the single subprotocol selected by the peer.
///
/// `None` means the response omitted the selection. A present value must
/// contain exactly one valid value that was offered by the client; duplicate
/// header fields, comma-separated values, malformed text, or an unoffered value
/// produce a structured `Handshake` error.
fn selected_subprotocol(headers: &HeaderMap, offered: &[String]) -> Result<Option<String>, WsError> {
    let values = headers.get_all("sec-websocket-protocol");
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(handshake_failure(WsHandshakeFailure::Subprotocol));
    }
    let value = value
        .to_str()
        .map_err(|_| handshake_failure(WsHandshakeFailure::Subprotocol))?;
    let selected = value.trim();
    if selected.is_empty() || selected.contains(',') || !offered.iter().any(|protocol| protocol == selected) {
        return Err(handshake_failure(WsHandshakeFailure::Subprotocol));
    }
    Ok(Some(selected.to_owned()))
}

/// Maps tungstenite opening failures into stable, safe WebSocket error classes.
///
/// HTTP status and known handshake failures are retained as structured values;
/// relevant underlying errors remain available only through `Error::source()`
/// for explicit diagnosis. `WsError`'s `Display` and `Debug` omit that source,
/// so formatting the mapped error does not expose transport or TLS details.
fn map_handshake_error(error: tokio_tungstenite::tungstenite::Error) -> WsError {
    use tokio_tungstenite::tungstenite::Error as TungsteniteError;
    use tokio_tungstenite::tungstenite::error::ProtocolError;
    match error {
        TungsteniteError::Http(response) => {
            let status = response.status().as_u16();
            WsError::new(WsErrorKind::Handshake)
                .with_status_code(status)
                .with_handshake_failure(WsHandshakeFailure::Status(status))
        }
        TungsteniteError::Tls(source) => WsError::with_source(WsErrorKind::Tls, source),
        TungsteniteError::Io(source) if io_error_contains_rustls_error(&source) => {
            WsError::with_source(WsErrorKind::Tls, source)
        }
        TungsteniteError::Url(_) => invalid_url(),
        TungsteniteError::Protocol(ProtocolError::MissingUpgradeWebSocketHeader) => {
            handshake_failure(WsHandshakeFailure::Upgrade)
        }
        TungsteniteError::Protocol(ProtocolError::MissingConnectionUpgradeHeader) => {
            handshake_failure(WsHandshakeFailure::Connection)
        }
        TungsteniteError::Protocol(ProtocolError::SecWebSocketAcceptKeyMismatch) => {
            handshake_failure(WsHandshakeFailure::Accept)
        }
        TungsteniteError::Protocol(ProtocolError::SecWebSocketSubProtocolError(_)) => {
            handshake_failure(WsHandshakeFailure::Subprotocol)
        }
        source @ TungsteniteError::Protocol(ProtocolError::HandshakeIncomplete) => {
            WsError::with_source(WsErrorKind::Transport, source)
        }
        source @ TungsteniteError::Protocol(_) => WsError::with_source(WsErrorKind::Protocol, source),
        TungsteniteError::Io(source) => WsError::with_source(WsErrorKind::Transport, source),
        source @ (TungsteniteError::ConnectionClosed | TungsteniteError::AlreadyClosed) => {
            WsError::with_source(WsErrorKind::Transport, source)
        }
        source @ (TungsteniteError::Capacity(_)
        | TungsteniteError::WriteBufferFull(_)
        | TungsteniteError::Utf8(_)
        | TungsteniteError::HttpFormat(_)) => WsError::with_source(WsErrorKind::Handshake, source),
        source @ TungsteniteError::AttackAttempt => WsError::with_source(WsErrorKind::Protocol, source),
    }
}

/// Reports whether an I/O error's source chain contains a rustls error.
///
/// This allows TLS failures wrapped by the transport layer to retain the `Tls`
/// category. It inspects types only and does not format or expose source text.
fn io_error_contains_rustls_error(error: &std::io::Error) -> bool {
    let mut source = error
        .get_ref()
        .map(|source| source as &(dyn std::error::Error + 'static));
    while let Some(error) = source {
        if error.downcast_ref::<rustls::Error>().is_some() {
            return true;
        }
        source = error.source();
    }
    false
}

/// Creates the source-free error used for rejected or malformed endpoint URLs.
fn invalid_url() -> WsError {
    WsError::new(WsErrorKind::InvalidUrl)
}

/// Creates a structured opening-handshake error without retaining peer text.
///
/// Only the stable failure category is exposed; response headers and other
/// peer-controlled details are not copied into the error's formatted output.
fn handshake_failure(failure: WsHandshakeFailure) -> WsError {
    WsError::new(WsErrorKind::Handshake).with_handshake_failure(failure)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use http::HeaderMap;
    use tokio::net::TcpListener;

    use super::ConnectedSocket;
    use super::build_request;
    use super::build_root_store;
    use super::connect_socket;
    use super::map_handshake_error;
    use super::normalize_tcp_host;
    use super::parse_websocket_uri;
    use crate::error::WsError;
    use crate::error::WsErrorKind;
    use crate::error::WsHandshakeFailure;
    use crate::options::WsClientOptions;
    use crate::options::WsTrustRoots;

    enum PeerResponse {
        Status(u16),
        Upgrade,
        Connection,
        Accept,
        UnproposedSubprotocol,
        DuplicateSubprotocol,
    }

    fn peer_response(response: PeerResponse, key: &str) -> String {
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        match response {
            PeerResponse::Status(status) => format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\n\r\n"),
            PeerResponse::Upgrade => format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"),
            PeerResponse::Connection => format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: keep-alive\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"),
            PeerResponse::Accept => "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: invalid\r\n\r\n".to_owned(),
            PeerResponse::UnproposedSubprotocol => format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: other\r\n\r\n"),
            PeerResponse::DuplicateSubprotocol => format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: chat\r\nSec-WebSocket-Protocol: chat\r\n\r\n"),
        }
    }

    async fn connect_to_raw_peer(response: PeerResponse, protocols: Vec<String>) -> Result<ConnectedSocket, WsError> {
        use std::io::Read;
        use std::io::Write;
        use std::net::TcpListener as StdTcpListener;
        use std::thread;

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let count = socket.read(&mut chunk).unwrap();
                request.extend_from_slice(&chunk[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            let key = request
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("sec-websocket-key")
                        .then_some(value.trim().to_owned())
                })
                .unwrap();
            socket.write_all(peer_response(response, &key).as_bytes()).unwrap();
        });
        let options = WsClientOptions {
            handshake_timeout: std::time::Duration::from_secs(2),
            subprotocols: protocols,
            ..WsClientOptions::default()
        };
        let result = connect_socket(&format!("ws://{address}/"), &options, &HeaderMap::new()).await;
        peer.join().unwrap();
        result
    }

    #[test]
    fn websocket_uri_validation_preserves_path_and_query() {
        let uri = parse_websocket_uri("wss://example.test/socket?room=one").unwrap();
        assert_eq!(uri.path_and_query().unwrap().as_str(), "/socket?room=one");
        for url in [
            "http://example.test/",
            "https://example.test/",
            "ws://user:secret@example.test/",
            "ws://example.test/#fragment",
            "ws:///missing-host",
            "ws://example.test:invalid/",
            "ws://[::1]suffix/",
            "ws://[[::1]]/",
            "ws://::1]/",
        ] {
            assert_eq!(parse_websocket_uri(url).unwrap_err().kind(), WsErrorKind::InvalidUrl);
        }
    }

    #[test]
    fn tcp_host_normalization_handles_ipv6_dns_and_ipv4() {
        assert_eq!(normalize_tcp_host("[::1]").unwrap(), "::1");
        assert_eq!(normalize_tcp_host("example.test").unwrap(), "example.test");
        assert_eq!(normalize_tcp_host("127.0.0.1").unwrap(), "127.0.0.1");
        for host in ["[not-ipv6]", "[::1", "::1]", "[fe80::1%eth0]"] {
            assert_eq!(normalize_tcp_host(host).unwrap_err().kind(), WsErrorKind::InvalidUrl);
        }
    }

    #[test]
    fn checked_write_buffer_limit_reserves_data_and_control_frames() {
        assert_eq!(WsClientOptions::checked_write_buffer_limit(1), Some(146));
        assert_eq!(WsClientOptions::checked_write_buffer_limit(usize::MAX), None);
    }

    #[test]
    fn tungstenite_handshake_failures_keep_their_specific_category() {
        use tokio_tungstenite::tungstenite::Error as TungsteniteError;
        use tokio_tungstenite::tungstenite::error::ProtocolError;

        let cases = [
            (
                ProtocolError::MissingUpgradeWebSocketHeader,
                WsHandshakeFailure::Upgrade,
            ),
            (
                ProtocolError::MissingConnectionUpgradeHeader,
                WsHandshakeFailure::Connection,
            ),
            (ProtocolError::SecWebSocketAcceptKeyMismatch, WsHandshakeFailure::Accept),
        ];
        for (cause, expected) in cases {
            let error = map_handshake_error(TungsteniteError::Protocol(cause));
            assert_eq!(error.kind(), WsErrorKind::Handshake);
            assert_eq!(error.handshake_failure(), Some(expected));
        }
    }

    #[test]
    fn ordinary_io_error_is_not_misclassified_as_tls() {
        let error = map_handshake_error(tokio_tungstenite::tungstenite::Error::Io(std::io::Error::other(
            "connection reset",
        )));

        assert_eq!(error.kind(), WsErrorKind::Transport);
    }

    #[test]
    fn request_headers_overlay_defaults_and_reject_protocol_owned_fields() {
        let mut defaults = HeaderMap::new();
        defaults.insert("authorization", "Bearer default".parse().unwrap());
        defaults.append("x-tag", "default-a".parse().unwrap());
        defaults.append("x-tag", "default-b".parse().unwrap());
        defaults.insert("x-trace", "trace-default".parse().unwrap());
        let options = WsClientOptions {
            headers: defaults,
            subprotocols: vec!["chat".to_owned(), "superchat".to_owned()],
            ..WsClientOptions::default()
        };
        let mut per_connection = HeaderMap::new();
        per_connection.insert("authorization", "Bearer once".parse().unwrap());
        per_connection.append("x-tag", "once-a".parse().unwrap());
        per_connection.append("x-tag", "once-b".parse().unwrap());

        let request_without_override = build_request(
            "ws://127.0.0.1:9000/chat?room=one".parse().unwrap(),
            &options,
            &HeaderMap::new(),
        )
        .unwrap();
        let default_values = request_without_override
            .headers()
            .get_all("x-tag")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(default_values, ["default-a", "default-b"]);

        let request = build_request(
            "ws://127.0.0.1:9000/chat?room=one".parse().unwrap(),
            &options,
            &per_connection,
        )
        .unwrap();
        assert_eq!(request.headers()["authorization"], "Bearer once");
        let effective_values = request
            .headers()
            .get_all("x-tag")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(effective_values, ["once-a", "once-b"]);
        assert_eq!(request.headers()["x-trace"], "trace-default");
        assert_eq!(request.headers()["sec-websocket-protocol"], "chat, superchat");

        for forbidden in ["Host", "Connection", "Upgrade", "Sec-WebSocket-Key"] {
            let mut headers = HeaderMap::new();
            headers.insert(forbidden, "caller-value".parse().unwrap());
            assert_eq!(
                build_request(
                    "ws://127.0.0.1/".parse().unwrap(),
                    &WsClientOptions::default(),
                    &headers,
                )
                .unwrap_err()
                .kind(),
                WsErrorKind::InvalidConfig
            );
        }
    }

    #[tokio::test]
    async fn local_peer_completes_a_websocket_handshake() {
        use std::io::Read;
        use std::io::Write;
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let count = socket.read(&mut chunk).unwrap();
                request.extend_from_slice(&chunk[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /chat?room=one HTTP/1.1\r\n"));
            assert!(request.to_ascii_lowercase().contains("authorization: bearer test\r\n"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("sec-websocket-protocol: chat\r\n")
            );
            let key = request
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("sec-websocket-key").then_some(value.trim())
                })
                .unwrap();
            let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
            write!(
                socket,
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: chat\r\n\r\n"
            )
            .unwrap();
        });

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test".parse().unwrap());
        let options = WsClientOptions {
            subprotocols: vec!["chat".to_owned()],
            ..WsClientOptions::default()
        };
        let connected = connect_socket(&format!("ws://{address}/chat?room=one"), &options, &headers)
            .await
            .unwrap();
        assert_eq!(connected.status_code, 101);
        assert_eq!(connected.selected_subprotocol.as_deref(), Some("chat"));
        drop(connected.stream);
        peer.join().unwrap();
    }

    #[tokio::test]
    async fn local_peer_handshake_failures_are_classified_precisely() {
        for (response, protocols, status, failure) in [
            (
                PeerResponse::Status(401),
                vec![],
                Some(401),
                Some(WsHandshakeFailure::Status(401)),
            ),
            (
                PeerResponse::Status(302),
                vec![],
                Some(302),
                Some(WsHandshakeFailure::Status(302)),
            ),
            (PeerResponse::Upgrade, vec![], None, Some(WsHandshakeFailure::Upgrade)),
            (
                PeerResponse::Connection,
                vec![],
                None,
                Some(WsHandshakeFailure::Connection),
            ),
            (PeerResponse::Accept, vec![], None, Some(WsHandshakeFailure::Accept)),
            (
                PeerResponse::UnproposedSubprotocol,
                vec!["chat".to_owned()],
                None,
                Some(WsHandshakeFailure::Subprotocol),
            ),
            (
                PeerResponse::DuplicateSubprotocol,
                vec!["chat".to_owned()],
                None,
                Some(WsHandshakeFailure::Subprotocol),
            ),
        ] {
            let result = connect_to_raw_peer(response, protocols).await;
            let error = result.err().expect("response should fail the opening handshake");
            assert_eq!(error.kind(), WsErrorKind::Handshake);
            assert_eq!(error.status_code(), status);
            assert_eq!(error.handshake_failure(), failure);
        }
    }

    #[tokio::test]
    async fn wss_connects_with_an_explicit_local_trust_root() {
        use rcgen::generate_simple_self_signed;
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::PrivateKeyDer;
        use rustls::pki_types::PrivatePkcs8KeyDer;
        use tokio_rustls::TlsAcceptor;

        let generated = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = CertificateDer::from(generated.cert.der().to_vec());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate.clone()).unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(tcp).await.unwrap();
            let _websocket = tokio_tungstenite::accept_async(tls).await.unwrap();
        });
        let options = WsClientOptions {
            trust_roots: WsTrustRoots::Exclusive(roots),
            handshake_timeout: std::time::Duration::from_secs(3),
            ..WsClientOptions::default()
        };
        let connected = connect_socket(
            &format!("wss://localhost:{}/", address.port()),
            &options,
            &HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(connected.status_code, 101);
        drop(connected.stream);
        server.await.unwrap();
    }

    #[test]
    fn default_trust_roots_include_webpki_roots() {
        let roots = build_root_store(&WsTrustRoots::Default);
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
    }

    #[test]
    fn additional_trust_roots_keep_webpki_roots_and_add_local_roots() {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let mut local_roots = rustls::RootCertStore::empty();
        local_roots.add(generated.cert.der().clone()).unwrap();

        let roots = build_root_store(&WsTrustRoots::Additional(local_roots));

        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len() + 1);
    }

    #[test]
    fn exclusive_trust_roots_only_include_local_roots() {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let mut local_roots = rustls::RootCertStore::empty();
        local_roots.add(generated.cert.der().clone()).unwrap();

        let roots = build_root_store(&WsTrustRoots::Exclusive(local_roots));

        assert_eq!(roots.len(), 1);
    }

    #[tokio::test]
    async fn untrusted_wss_certificate_is_classified_as_tls() {
        use rcgen::generate_simple_self_signed;
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::PrivateKeyDer;
        use rustls::pki_types::PrivatePkcs8KeyDer;
        use tokio_rustls::TlsAcceptor;

        let generated = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = CertificateDer::from(generated.cert.der().to_vec());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            acceptor.accept(tcp).await
        });

        let error = connect_socket(
            &format!("wss://localhost:{}/", address.port()),
            &WsClientOptions::default(),
            &HeaderMap::new(),
        )
        .await
        .err()
        .expect("an untrusted certificate must be rejected");

        assert_eq!(error.kind(), WsErrorKind::Tls);
        assert!(server.await.unwrap().is_err());
    }
}
