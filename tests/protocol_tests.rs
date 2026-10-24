// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error as StdError;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::StreamExt;
use http::HeaderMap;
use http::Request;
use qubit_websocket::WsClient;
use qubit_websocket::WsError;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsHandshakeFailure;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSessionLimits;
use rcgen::CertificateParams;
use rcgen::KeyPair;
use rcgen::SanType;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::ServerConfig;
use rustls::pki_types::PrivateKeyDer;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Message;

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

fn expect_error<T>(result: Result<T, WsError>) -> WsError {
    match result {
        Ok(_) => panic!("expected connection to fail"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn test_ws_connects_to_local_server_and_exchanges_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        let message = timeout(TEST_TIMEOUT, websocket.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(message.into_text().unwrap(), "hello");
        websocket.send(Message::Text("world".into())).await.unwrap();
        while let Some(Ok(message)) = websocket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let mut session = timeout(
        TEST_TIMEOUT,
        WsClient::builder()
            .build()
            .unwrap()
            .connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();
    session.send(WsMessage::Text("hello".into())).await.unwrap();
    assert!(matches!(
        timeout(TEST_TIMEOUT, session.receive()).await.unwrap(),
        Some(Ok(WsMessage::Text(text))) if text == "world"
    ));
    drop(session);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
// The callback must return tungstenite's full HTTP response on handshake failure.
#[allow(clippy::result_large_err)]
async fn test_connect_with_sends_all_connection_header_values_and_replaces_defaults() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        let mut websocket = accept_hdr_async(socket, |request: &Request<()>, response| {
            let tag_values = request
                .headers()
                .get_all("x-tag")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(tag_values, ["once-a", "once-b"]);
            assert_eq!(request.headers()["x-trace"], "trace-default");
            Ok(response)
        })
        .await
        .unwrap();
        let _ = timeout(TEST_TIMEOUT, websocket.next()).await;
    });

    let mut defaults = HeaderMap::new();
    defaults.append("x-tag", "default-a".parse().unwrap());
    defaults.append("x-tag", "default-b".parse().unwrap());
    defaults.insert("x-trace", "trace-default".parse().unwrap());
    let client = WsClient::builder().headers(defaults).build().unwrap();
    let mut per_connection = HeaderMap::new();
    per_connection.append("x-tag", "once-a".parse().unwrap());
    per_connection.append("x-tag", "once-b".parse().unwrap());

    let session = timeout(
        TEST_TIMEOUT,
        client.connect_with(&format!("ws://{address}/"), per_connection),
    )
    .await
    .expect("local handshake should finish")
    .expect("local server should accept the handshake");
    drop(session);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

fn localhost_test_tls() -> (TlsAcceptor, RootCertStore) {
    let certified = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = certified.cert;
    let private_key = certified.key_pair.serialize_der();
    let mut roots = RootCertStore::empty();
    roots.add(certificate.der().clone()).unwrap();
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone()],
            PrivateKeyDer::Pkcs8(private_key.into()),
        )
        .unwrap();
    (TlsAcceptor::from(Arc::new(server_config)), roots)
}

fn ipv6_test_tls(san_ip: &str) -> (TlsAcceptor, RootCertStore) {
    let key_pair = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![]).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress(san_ip.parse().unwrap()));
    let certificate = params.self_signed(&key_pair).unwrap();
    let private_key = key_pair.serialize_der();
    let mut roots = RootCertStore::empty();
    roots.add(certificate.der().clone()).unwrap();
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone()],
            PrivateKeyDer::Pkcs8(private_key.into()),
        )
        .unwrap();
    (TlsAcceptor::from(Arc::new(server_config)), roots)
}

async fn bind_ipv6_loopback_or_skip() -> Option<TcpListener> {
    match TcpListener::bind("[::1]:0").await {
        Ok(listener) => Some(listener),
        Err(error) if matches!(error.kind(), ErrorKind::AddrNotAvailable | ErrorKind::Unsupported) => None,
        Err(error) => panic!("failed to bind IPv6 loopback test listener: {error}"),
    }
}

async fn spawn_test_wss_peer(acceptor: TlsAcceptor) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        if let Ok(tls) = timeout(TEST_TIMEOUT, acceptor.accept(socket)).await.unwrap()
            && let Ok(mut websocket) = accept_async(tls).await
        {
            let _ = timeout(TEST_TIMEOUT, websocket.next()).await;
        }
    });
    (port, peer)
}

#[tokio::test]
async fn test_wss_connects_with_explicitly_trusted_test_ca() {
    let (acceptor, roots) = localhost_test_tls();
    let (port, peer) = spawn_test_wss_peer(acceptor).await;
    let client = WsClient::builder().root_certificates(roots).build().unwrap();
    let session = timeout(TEST_TIMEOUT, client.connect(&format!("wss://localhost:{port}/")))
        .await
        .expect("trusted local certificate handshake should finish")
        .expect("trusted CA and matching DNS name should connect");
    drop(session);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_wss_connects_with_exclusive_test_ca() {
    let (acceptor, roots) = localhost_test_tls();
    let (port, peer) = spawn_test_wss_peer(acceptor).await;
    let client = WsClient::builder().exclusive_root_certificates(roots).build().unwrap();
    let session = timeout(TEST_TIMEOUT, client.connect(&format!("wss://localhost:{port}/")))
        .await
        .expect("exclusive trusted local certificate handshake should finish")
        .expect("exclusive test CA and matching DNS name should connect");
    drop(session);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_wss_tls_handshake_stall_times_out_and_releases_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (mut socket, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("client should connect")
            .expect("listener should accept client");
        timeout(TEST_TIMEOUT, async {
            let mut buffer = [0; 1024];
            loop {
                let bytes = socket.read(&mut buffer).await.expect("socket read should succeed");
                if bytes == 0 {
                    break;
                }
            }
        })
        .await
        .expect("client should release the stalled TLS socket");
    });

    let client = WsClient::builder()
        .handshake_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let error = expect_error(
        timeout(TEST_TIMEOUT, client.connect(&format!("wss://{address}/")))
            .await
            .expect("stalled TLS handshake should not exceed the test deadline"),
    );
    assert_eq!(error.kind(), WsErrorKind::Timeout);
    timeout(TEST_TIMEOUT, peer)
        .await
        .expect("peer should observe socket release")
        .expect("peer task should succeed");
}

#[tokio::test]
async fn test_wss_connects_to_ipv6_literal_with_explicitly_trusted_ip_san() {
    let Some(listener) = bind_ipv6_loopback_or_skip().await else {
        return;
    };
    let port = listener.local_addr().unwrap().port();
    let (acceptor, roots) = ipv6_test_tls("::1");
    let peer = spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        let tls = timeout(TEST_TIMEOUT, acceptor.accept(socket)).await.unwrap().unwrap();
        let mut websocket = accept_async(tls).await.unwrap();
        let _ = timeout(TEST_TIMEOUT, websocket.next()).await;
    });

    let client = WsClient::builder().root_certificates(roots).build().unwrap();
    let session = timeout(TEST_TIMEOUT, client.connect(&format!("wss://[::1]:{port}/")))
        .await
        .expect("trusted IPv6 IP SAN TLS handshake should finish")
        .expect("explicitly trusted certificate with matching IP SAN should connect");
    drop(session);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_wss_rejects_ipv6_literal_with_mismatched_trusted_ip_san() {
    let Some(listener) = bind_ipv6_loopback_or_skip().await else {
        return;
    };
    let port = listener.local_addr().unwrap().port();
    let (acceptor, roots) = ipv6_test_tls("::2");
    let peer = spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        assert!(timeout(TEST_TIMEOUT, acceptor.accept(socket)).await.unwrap().is_err());
    });

    let client = WsClient::builder().root_certificates(roots).build().unwrap();
    let error = match timeout(TEST_TIMEOUT, client.connect(&format!("wss://[::1]:{port}/")))
        .await
        .expect("mismatched IPv6 IP SAN handshake should finish")
    {
        Ok(_) => panic!("a trusted certificate with a mismatched IP SAN must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Tls);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_wss_rejects_an_untrusted_test_ca_as_a_tls_error() {
    let (acceptor, _) = localhost_test_tls();
    let (port, peer) = spawn_test_wss_peer(acceptor).await;
    let error = expect_error(
        timeout(
            TEST_TIMEOUT,
            WsClient::builder()
                .build()
                .unwrap()
                .connect(&format!("wss://localhost:{port}/")),
        )
        .await
        .expect("untrusted TLS handshake should finish with an error"),
    );
    let source = StdError::source(&error);
    let source_variant =
        source
            .and_then(|source| source.downcast_ref::<TungsteniteError>())
            .map(|source| match source {
                TungsteniteError::Tls(_) => "Tls",
                TungsteniteError::Io(_) => "Io",
                TungsteniteError::Http(_) => "Http",
                TungsteniteError::Protocol(_) => "Protocol",
                TungsteniteError::Capacity(_) => "Capacity",
                _ => "Other",
            });
    assert_eq!(
        error.kind(),
        WsErrorKind::Tls,
        "sanitized error={error:?}, source_variant={source_variant:?}"
    );
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_wss_rejects_hostname_mismatch_for_explicitly_trusted_test_ca() {
    let (acceptor, roots) = localhost_test_tls();
    let (port, peer) = spawn_test_wss_peer(acceptor).await;
    let client = WsClient::builder().root_certificates(roots).build().unwrap();
    let error = expect_error(
        timeout(TEST_TIMEOUT, client.connect(&format!("wss://127.0.0.1:{port}/")))
            .await
            .expect("hostname validation should finish with an error"),
    );
    assert_eq!(error.kind(), WsErrorKind::Tls);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[derive(Clone, Copy)]
enum ResponseCase {
    Status(u16),
    MissingUpgrade,
    MissingConnection,
    BadAccept,
    UnproposedSubprotocol,
    DuplicateSubprotocol,
}

async fn serve_raw_response(case: ResponseCase) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        send_raw_response(socket, case).await.unwrap();
    });
    address
}

async fn send_raw_response(mut socket: TcpStream, case: ResponseCase) -> io::Result<()> {
    let mut request = Vec::new();
    let mut reader = BufReader::new(&mut socket);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "request ended"));
        }
        if line == "\r\n" {
            break;
        }
        request.extend_from_slice(line.as_bytes());
    }
    let key = String::from_utf8(request)
        .unwrap()
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("sec-websocket-key")
                .then_some(value.trim().to_owned())
        })
        .unwrap();
    let accept = derive_accept_key(key.as_bytes());
    let response = match case {
        ResponseCase::Status(status) => {
            format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\n\r\n")
        }
        ResponseCase::MissingUpgrade => format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        ),
        ResponseCase::MissingConnection => format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: keep-alive\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        ),
        ResponseCase::BadAccept => "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: wrong\r\n\r\n".to_owned(),
        ResponseCase::UnproposedSubprotocol => format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: other\r\n\r\n"
        ),
        ResponseCase::DuplicateSubprotocol => format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: chat\r\nSec-WebSocket-Protocol: chat\r\n\r\n"
        ),
    };
    reader.get_mut().write_all(response.as_bytes()).await
}

#[tokio::test]
async fn test_rejects_non_success_and_malformed_handshakes_by_category() {
    let cases = [
        (ResponseCase::Status(401), WsHandshakeFailure::Status(401)),
        (ResponseCase::Status(302), WsHandshakeFailure::Status(302)),
        (ResponseCase::MissingUpgrade, WsHandshakeFailure::Upgrade),
        (ResponseCase::MissingConnection, WsHandshakeFailure::Connection),
        (ResponseCase::BadAccept, WsHandshakeFailure::Accept),
        (ResponseCase::UnproposedSubprotocol, WsHandshakeFailure::Subprotocol),
        (ResponseCase::DuplicateSubprotocol, WsHandshakeFailure::Subprotocol),
    ];
    for (case, expected) in cases {
        let address = serve_raw_response(case).await;
        let client = WsClient::builder()
            .subprotocols(vec!["chat".to_owned()])
            .handshake_timeout(TEST_TIMEOUT)
            .build()
            .unwrap();
        let error = expect_error(
            timeout(TEST_TIMEOUT, client.connect(&format!("ws://{address}/")))
                .await
                .expect("malformed peer should not hang"),
        );
        assert_eq!(error.kind(), WsErrorKind::Handshake, "case: {expected:?}");
        assert_eq!(error.handshake_failure(), Some(expected));
        if let WsHandshakeFailure::Status(status) = expected {
            assert_eq!(error.status_code(), Some(status));
        }
    }
}

#[tokio::test]
async fn test_sensitive_credentials_are_omitted_from_error_debug_and_display() {
    const SECRET: &str = "Bearer protocol-test-secret";
    let address = serve_raw_response(ResponseCase::Status(401)).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", SECRET.parse().unwrap());
    let error = expect_error(
        timeout(
            TEST_TIMEOUT,
            WsClient::builder()
                .headers(headers)
                .handshake_timeout(TEST_TIMEOUT)
                .build()
                .unwrap()
                .connect(&format!("ws://{address}/?access_token=url-secret")),
        )
        .await
        .expect("rejected request should complete"),
    );
    assert_eq!(error.kind(), WsErrorKind::Handshake);
    let diagnostic = format!("{error:?} {error}");
    assert!(!diagnostic.contains(SECRET));
    assert!(!diagnostic.contains("url-secret"));
}

#[tokio::test]
async fn test_disconnect_during_upgrade_is_transport_error_without_leaking_request_secrets() {
    const SECRET: &str = "Bearer upgrade-test-secret";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("client should connect")
            .expect("listener should accept client");
        let mut reader = BufReader::new(socket);
        loop {
            let mut line = Vec::new();
            let bytes = timeout(TEST_TIMEOUT, reader.read_until(b'\n', &mut line))
                .await
                .expect("client should send handshake headers")
                .expect("handshake read should succeed");
            assert_ne!(bytes, 0, "client disconnected before completing request");
            if line == b"\r\n" {
                break;
            }
        }
        // Dropping the socket before writing any bytes simulates a peer that
        // accepts the upgrade request and then vanishes without an HTTP reply.
    });
    let mut headers = HeaderMap::new();
    headers.insert("authorization", SECRET.parse().unwrap());
    let error = expect_error(
        timeout(
            TEST_TIMEOUT,
            WsClient::builder()
                .headers(headers)
                .handshake_timeout(TEST_TIMEOUT)
                .build()
                .unwrap()
                .connect(&format!("ws://{address}/?access_token=url-secret")),
        )
        .await
        .expect("disconnect during upgrade should complete"),
    );
    timeout(TEST_TIMEOUT, peer)
        .await
        .expect("peer should finish reading the request")
        .expect("peer task should succeed");

    assert_eq!(error.kind(), WsErrorKind::Transport);
    assert_eq!(error.status_code(), None);
    assert_eq!(error.handshake_failure(), None);
    let diagnostic = format!("{error:?} {error}");
    assert!(!diagnostic.contains(SECRET));
    assert!(!diagnostic.contains("url-secret"));
}

#[tokio::test]
async fn test_tiny_protocol_limits_are_applied_to_real_incoming_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, sent_result) = oneshot::channel();
    let peer = spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        let send_result = websocket.send(Message::Binary(vec![0; 9].into())).await;
        let sent_ok = send_result.is_ok();
        let _ = sent.send(sent_ok);
        if sent_ok {
            let _ = timeout(TEST_TIMEOUT, websocket.next()).await;
        }
    });
    let limits = WsSessionLimits {
        max_inbound_frame_bytes: 8,
        max_inbound_message_bytes: 8,
        inbound_bytes: 8,
        outbound_bytes: 8,
        close_timeout: Duration::from_millis(250),
        ..WsSessionLimits::default()
    };
    let mut session = timeout(
        TEST_TIMEOUT,
        WsClient::builder()
            .limits(limits)
            .build()
            .unwrap()
            .connect(&format!("ws://{address}/")),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        timeout(TEST_TIMEOUT, sent_result)
            .await
            .expect("peer send should complete")
            .expect("peer send status should be reported")
    );
    let receive_result = timeout(TEST_TIMEOUT, session.receive()).await;
    assert!(
        receive_result.is_ok(),
        "receive timed out; termination snapshot: {:?}",
        session.termination()
    );
    let terminal = receive_result
        .unwrap()
        .expect("protocol error should be delivered once")
        .unwrap_err();
    assert_eq!(terminal.kind(), WsErrorKind::FrameTooLarge);
    assert!(timeout(TEST_TIMEOUT, session.receive()).await.unwrap().is_none());
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_ipv6_literal_connects_when_loopback_is_available() {
    let Some(listener) = bind_ipv6_loopback_or_skip().await else {
        return;
    };
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        let message = timeout(TEST_TIMEOUT, websocket.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(message.into_text().unwrap(), "ipv6");
        websocket.send(Message::Text("connected".into())).await.unwrap();
        while let Some(Ok(message)) = websocket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });
    let mut session = timeout(
        TEST_TIMEOUT,
        WsClient::builder()
            .build()
            .unwrap()
            .connect(&format!("ws://{address}/")),
    )
    .await
    .expect("IPv6 handshake deadline")
    .expect("IPv6 literal should connect");
    session.send(WsMessage::Text("ipv6".into())).await.unwrap();
    assert!(matches!(
        timeout(TEST_TIMEOUT, session.receive()).await.unwrap(),
        Some(Ok(WsMessage::Text(text))) if text == "connected"
    ));
    drop(session);
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}
