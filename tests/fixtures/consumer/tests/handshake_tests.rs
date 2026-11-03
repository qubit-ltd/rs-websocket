// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsHandshakeFailure;
use sha1::Digest;
use sha1::Sha1;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::Duration;
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum ResponseHeaders {
    Connection(&'static str),
    DuplicateConnection(&'static str, &'static str),
    MissingConnection,
}

#[tokio::test]
async fn independent_consumer_observes_connection_token_contract() {
    for (headers, should_connect) in [
        (ResponseHeaders::Connection("Upgrade"), true),
        (ResponseHeaders::Connection("Upgrade, keep-alive"), true),
        (ResponseHeaders::Connection("keep-alive, Upgrade"), true),
        (ResponseHeaders::Connection(" uPgRaDe\t"), true),
        (
            ResponseHeaders::DuplicateConnection("keep-alive", "Upgrade"),
            true,
        ),
        (ResponseHeaders::MissingConnection, false),
        (ResponseHeaders::Connection("keep-alive"), false),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(DEADLINE, listener.accept()).await.unwrap().unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let count = timeout(DEADLINE, socket.read(&mut chunk))
                    .await
                    .unwrap()
                    .unwrap();
                assert_ne!(
                    count, 0,
                    "client closed before sending complete HTTP headers"
                );
                request.extend_from_slice(&chunk[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                assert!(request.len() < 16 * 1024, "request headers exceeded limit");
            }
            let request = String::from_utf8(request).unwrap();
            let key = request
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("sec-websocket-key")
                        .then(|| value.trim().to_owned())
                })
                .expect("Sec-WebSocket-Key request header");
            let mut digest = Sha1::new();
            digest.update(key.as_bytes());
            digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
            let accept = STANDARD.encode(digest.finalize());
            let mut response = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n"
            );
            match headers {
                ResponseHeaders::Connection(value) => {
                    response.push_str(&format!("Connection: {value}\r\n"))
                }
                ResponseHeaders::DuplicateConnection(first, second) => {
                    response.push_str(&format!("Connection: {first}\r\nConnection: {second}\r\n"));
                }
                ResponseHeaders::MissingConnection => {}
            }
            response.push_str("\r\n");
            timeout(DEADLINE, socket.write_all(response.as_bytes()))
                .await
                .unwrap()
                .unwrap();
        });

        let result = timeout(
            DEADLINE,
            WsClient::builder()
                .build()
                .unwrap()
                .connect(&format!("ws://{address}/")),
        )
        .await
        .expect("consumer connection must finish within deadline");
        if should_connect {
            let session = result.expect("valid Connection tokens must be accepted");
            assert_eq!(session.response_status_code(), 101);
            drop(session);
        } else {
            let error = match result {
                Ok(session) => {
                    drop(session);
                    panic!("missing Upgrade token must fail");
                }
                Err(error) => error,
            };
            assert_eq!(error.kind(), WsErrorKind::Handshake);
            assert_eq!(
                error.handshake_failure(),
                Some(WsHandshakeFailure::Connection)
            );
        }
        timeout(DEADLINE, peer)
            .await
            .expect("peer must finish")
            .unwrap();
    }
}
