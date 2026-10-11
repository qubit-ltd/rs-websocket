// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::StreamExt;
use http::HeaderMap;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsHandshakeFailure;
use qubit_websocket::WsMessage;
use qubit_websocket::WsSessionLimits;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Message;

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

fn expect_error<T>(result: Result<T, qubit_websocket::WsError>) -> qubit_websocket::WsError {
    match result {
        Ok(_) => panic!("expected connection to fail"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn ws_connects_to_local_server_and_exchanges_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
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

fn localhost_test_tls() -> (TlsAcceptor, rustls::RootCertStore) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = certified.cert;
    let private_key = certified.key_pair.serialize_der();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate.der().clone()).unwrap();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(private_key.into()),
        )
        .unwrap();
    (TlsAcceptor::from(Arc::new(server_config)), roots)
}

async fn spawn_test_wss_peer(acceptor: TlsAcceptor) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept()).await.unwrap().unwrap();
        if let Ok(tls) = timeout(TEST_TIMEOUT, acceptor.accept(socket)).await.unwrap()
            && let Ok(mut websocket) = tokio_tungstenite::accept_async(tls).await
        {
            let _ = timeout(TEST_TIMEOUT, websocket.next()).await;
        }
    });
    (port, peer)
}

#[tokio::test]
async fn wss_connects_with_explicitly_trusted_test_ca() {
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
async fn wss_rejects_an_untrusted_test_ca_as_a_tls_error() {
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
    let source = std::error::Error::source(&error);
    let source_variant = source
        .and_then(|source| source.downcast_ref::<tokio_tungstenite::tungstenite::Error>())
        .map(|source| match source {
            tokio_tungstenite::tungstenite::Error::Tls(_) => "Tls",
            tokio_tungstenite::tungstenite::Error::Io(_) => "Io",
            tokio_tungstenite::tungstenite::Error::Http(_) => "Http",
            tokio_tungstenite::tungstenite::Error::Protocol(_) => "Protocol",
            tokio_tungstenite::tungstenite::Error::Capacity(_) => "Capacity",
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
async fn wss_rejects_hostname_mismatch_for_explicitly_trusted_test_ca() {
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

async fn serve_raw_response(case: ResponseCase) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
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
async fn rejects_non_success_and_malformed_handshakes_by_category() {
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
async fn sensitive_credentials_are_omitted_from_error_debug_and_display() {
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
async fn tiny_protocol_limits_are_applied_to_real_incoming_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, sent_result) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
        let send_result = websocket.send(Message::Binary(vec![0; 9].into())).await;
        let sent_ok = send_result.is_ok();
        let _ = sent.send(sent_ok);
        if sent_ok {
            let _ = timeout(TEST_TIMEOUT, websocket.next()).await;
        }
    });
    let limits = WsSessionLimits {
        max_frame_bytes: 8,
        max_message_bytes: 8,
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
    assert!(matches!(
        terminal.kind(),
        WsErrorKind::FrameTooLarge | WsErrorKind::MessageTooLarge | WsErrorKind::Protocol
    ));
    assert!(timeout(TEST_TIMEOUT, session.receive()).await.unwrap().is_none());
    timeout(TEST_TIMEOUT, peer).await.unwrap().unwrap();
}
