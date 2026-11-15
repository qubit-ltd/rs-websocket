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

#[derive(Clone, Copy, Debug)]
enum ResponseHeaders {
    Connection(&'static str),
    DuplicateConnection(&'static str, &'static str),
    RawConnectionFields(&'static [&'static [u8]]),
    MissingConnection,
}

pub(super) async fn assert_connection_contract(baseline: bool) {
    const INVALID_ONLY: &[&[u8]] = &[b"\xff"];
    const INVALID_THEN_UPGRADE: &[&[u8]] = &[b"\xff", b"Upgrade"];
    const UPGRADE_THEN_INVALID: &[&[u8]] = &[b"Upgrade", b"\xff"];

    for (headers, current_expectation, baseline_expectation) in [
        (ResponseHeaders::Connection("Upgrade"), true, true),
        (ResponseHeaders::Connection("Upgrade, keep-alive"), true, false),
        (ResponseHeaders::Connection("keep-alive, Upgrade"), true, false),
        (ResponseHeaders::Connection(" uPgRaDe\t"), true, true),
        (ResponseHeaders::Connection("x-upgrade"), false, false),
        (ResponseHeaders::Connection("upgrader"), false, false),
        (
            ResponseHeaders::DuplicateConnection("keep-alive", "Upgrade"),
            true,
            false,
        ),
        (
            ResponseHeaders::DuplicateConnection("Upgrade", "keep-alive"),
            true,
            true,
        ),
        (ResponseHeaders::MissingConnection, false, false),
        (ResponseHeaders::Connection("keep-alive"), false, false),
        (ResponseHeaders::RawConnectionFields(INVALID_ONLY), false, false),
        (ResponseHeaders::RawConnectionFields(INVALID_THEN_UPGRADE), false, false),
        (ResponseHeaders::RawConnectionFields(UPGRADE_THEN_INVALID), false, true),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(DEADLINE, listener.accept()).await.unwrap().unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let count = timeout(DEADLINE, socket.read(&mut chunk)).await.unwrap().unwrap();
                assert_ne!(count, 0, "client closed before sending complete HTTP headers");
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
            let mut response =
                format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n")
                    .into_bytes();
            match headers {
                ResponseHeaders::Connection(value) => {
                    response.extend_from_slice(format!("Connection: {value}\r\n").as_bytes());
                }
                ResponseHeaders::DuplicateConnection(first, second) => {
                    response.extend_from_slice(format!("Connection: {first}\r\nConnection: {second}\r\n").as_bytes());
                }
                ResponseHeaders::RawConnectionFields(fields) => {
                    for field in fields {
                        response.extend_from_slice(b"Connection: ");
                        response.extend_from_slice(field);
                        response.extend_from_slice(b"\r\n");
                    }
                }
                ResponseHeaders::MissingConnection => {}
            }
            response.extend_from_slice(b"\r\n");
            timeout(DEADLINE, socket.write_all(&response)).await.unwrap().unwrap();
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
        let should_connect = if baseline {
            baseline_expectation
        } else {
            current_expectation
        };
        if should_connect {
            let session = result.expect("valid Connection contract must be accepted");
            assert_eq!(session.response_status_code(), 101);
            drop(session);
        } else {
            let error = match result {
                Ok(session) => {
                    drop(session);
                    panic!("unexpected acceptance for Connection headers: {headers:?}");
                }
                Err(error) => error,
            };
            assert_eq!(error.kind(), WsErrorKind::Handshake);
            assert_eq!(error.handshake_failure(), Some(WsHandshakeFailure::Connection));
        }
        timeout(DEADLINE, peer).await.expect("peer must finish").unwrap();
    }
}
