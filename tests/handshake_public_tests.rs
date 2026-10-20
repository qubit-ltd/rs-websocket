// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use http::HeaderMap;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsHandshakeFailure;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::ServerConfig;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::net::TcpListener as TokioTcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

enum PeerResponse {
    Success,
    Unauthorized,
}

fn read_request(socket: &mut TcpStream) -> String {
    socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let count = socket.read(&mut chunk).unwrap();
        request.extend_from_slice(&chunk[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(request).unwrap()
}

fn spawn_http_peer(response: PeerResponse) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let request = read_request(&mut socket);
        if matches!(response, PeerResponse::Success) {
            let request = request.to_ascii_lowercase();
            assert!(request.starts_with("get /chat?room=one http/1.1\r\n"));
            assert!(request.contains("authorization: bearer once\r\n"));
            assert!(request.contains("x-trace: trace-default\r\n"));
        }
        let response = match response {
            PeerResponse::Unauthorized => "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".to_owned(),
            PeerResponse::Success => {
                let key = request
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("sec-websocket-key").then_some(value.trim())
                    })
                    .unwrap();
                let accept = derive_accept_key(key.as_bytes());
                format!(
                    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: chat\r\n\r\n"
                )
            }
        };
        socket.write_all(response.as_bytes()).unwrap();
    });
    (address, peer)
}

#[tokio::test]
async fn test_public_client_completes_handshake_and_overlays_headers() {
    let (address, peer) = spawn_http_peer(PeerResponse::Success);
    let mut defaults = HeaderMap::new();
    defaults.insert("authorization", "Bearer default".parse().unwrap());
    defaults.insert("x-trace", "trace-default".parse().unwrap());
    let client = WsClient::builder()
        .headers(defaults)
        .subprotocols(vec!["chat".to_owned()])
        .build()
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer once".parse().unwrap());
    let session = client
        .connect_with(&format!("ws://{address}/chat?room=one"), headers)
        .await
        .unwrap();
    assert_eq!(session.response_status_code(), 101);
    assert_eq!(session.selected_subprotocol(), Some("chat"));
    peer.join().unwrap();
}

#[tokio::test]
async fn test_public_client_classifies_peer_handshake_status() {
    let (address, peer) = spawn_http_peer(PeerResponse::Unauthorized);
    let client = WsClient::builder().build().unwrap();
    let error = match client.connect(&format!("ws://{address}/")).await {
        Ok(_) => panic!("unauthorized response must fail the opening handshake"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Handshake);
    assert_eq!(error.status_code(), Some(401));
    assert_eq!(error.handshake_failure(), Some(WsHandshakeFailure::Status(401)));
    peer.join().unwrap();
}

#[tokio::test]
async fn test_public_client_connects_with_an_explicit_local_trust_root() {
    let generated = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = CertificateDer::from(generated.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
    let mut roots = RootCertStore::empty();
    roots.add(certificate.clone()).unwrap();
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .unwrap();
    let listener = TokioTcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let tls = acceptor.accept(tcp).await.unwrap();
        let _websocket = tokio_tungstenite::accept_async(tls).await.unwrap();
    });
    let client = WsClient::builder().exclusive_root_certificates(roots).build().unwrap();
    let session = client
        .connect(&format!("wss://localhost:{}/", address.port()))
        .await
        .unwrap();
    assert_eq!(session.response_status_code(), 101);
    server.await.unwrap();
}

#[tokio::test]
async fn test_public_client_classifies_an_untrusted_certificate_as_tls() {
    let generated = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = CertificateDer::from(generated.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .unwrap();
    let listener = TokioTcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        acceptor.accept(tcp).await
    });
    let client = WsClient::builder().build().unwrap();
    let error = match client.connect(&format!("wss://localhost:{}/", address.port())).await {
        Ok(_) => panic!("untrusted certificate must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Tls);
    assert!(server.await.unwrap().is_err());
}
