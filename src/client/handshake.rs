// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use http::HeaderMap;
use http::Request;
use http::Uri;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::Connector;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::client_async_tls_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::client::append_subprotocol_header;
use crate::client::apply_user_headers;
use crate::error::WsError;
use crate::error::WsErrorKind;
use crate::error::WsHandshakeFailure;
use crate::options::WsClientOptions;

/// A socket whose opening handshake has passed tungstenite validation.
pub(crate) struct ConnectedSocket {
    pub(crate) stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub(crate) selected_subprotocol: Option<String>,
    pub(crate) status_code: u16,
}

/// Opens a bounded WebSocket connection using a fresh request and timeout
/// scopes.
pub(crate) async fn connect_socket(
    url: &str,
    options: &WsClientOptions,
    headers: &HeaderMap,
) -> Result<ConnectedSocket, WsError> {
    options.validate()?;
    super::validate_subprotocols(&options.subprotocols)?;
    let uri = parse_websocket_uri(url)?;
    let authority = uri.authority().ok_or_else(invalid_url)?;
    let host = authority.host().to_owned();
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
        .map_err(|error| WsError::with_source(WsErrorKind::Connect, error))?;

    let mut config = WebSocketConfig::default();
    config.max_frame_size = Some(options.limits.max_frame_bytes);
    config.max_message_size = Some(options.limits.max_message_bytes);
    config.write_buffer_size = 0;
    config.max_write_buffer_size = options
        .limits
        .outbound_bytes
        .checked_add(14)
        .ok_or_else(|| WsError::new(WsErrorKind::InvalidConfig))?;

    let connector = options.root_certificates.as_ref().map(|roots| {
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_no_client_auth();
        Connector::Rustls(Arc::new(tls_config))
    });

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

fn build_request(uri: Uri, options: &WsClientOptions, headers: &HeaderMap) -> Result<Request<()>, WsError> {
    let mut request = uri.into_client_request().map_err(|_| invalid_url())?;
    apply_user_headers(request.headers_mut(), &options.headers)?;
    apply_user_headers(request.headers_mut(), headers)?;
    append_subprotocol_header(request.headers_mut(), &options.subprotocols)?;
    Ok(request)
}

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
    if authority.port().is_none() && !authority.as_str().starts_with('[') && authority.as_str().contains(':') {
        return Err(invalid_url());
    }
    if authority.port().is_some() && authority.port_u16().is_none() {
        return Err(invalid_url());
    }
    Ok(uri)
}

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
        source @ TungsteniteError::Protocol(_) => WsError::with_source(WsErrorKind::Protocol, source),
        source => WsError::with_source(WsErrorKind::Handshake, source),
    }
}

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

fn invalid_url() -> WsError {
    WsError::new(WsErrorKind::InvalidUrl)
}

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
    use super::connect_socket;
    use super::map_handshake_error;
    use super::parse_websocket_uri;
    use crate::error::WsError;
    use crate::error::WsErrorKind;
    use crate::error::WsHandshakeFailure;
    use crate::options::WsClientOptions;

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
        ] {
            assert_eq!(parse_websocket_uri(url).unwrap_err().kind(), WsErrorKind::InvalidUrl);
        }
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

        assert_eq!(error.kind(), WsErrorKind::Handshake);
    }

    #[test]
    fn request_headers_overlay_defaults_and_reject_protocol_owned_fields() {
        let mut defaults = HeaderMap::new();
        defaults.insert("authorization", "Bearer default".parse().unwrap());
        defaults.insert("x-trace", "trace-default".parse().unwrap());
        let options = WsClientOptions {
            headers: defaults,
            subprotocols: vec!["chat".to_owned(), "superchat".to_owned()],
            ..WsClientOptions::default()
        };
        let mut per_connection = HeaderMap::new();
        per_connection.insert("authorization", "Bearer once".parse().unwrap());
        let request = build_request(
            "ws://127.0.0.1:9000/chat?room=one".parse().unwrap(),
            &options,
            &per_connection,
        )
        .unwrap();
        assert_eq!(request.headers()["authorization"], "Bearer once");
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
            root_certificates: Some(roots),
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
