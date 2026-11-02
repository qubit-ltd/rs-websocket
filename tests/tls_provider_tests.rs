// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::ServerConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const CASE_ENV: &str = "QUBIT_WEBSOCKET_PROVIDER_CASE";
const CASES: [&str; 5] = [
    "ws_without_provider",
    "wss_without_provider",
    "wss_with_provider",
    "wss_invalid_provider",
    "ws_with_invalid_provider",
];

#[test]
fn test_provider_scenarios_in_independent_processes() {
    for case in CASES {
        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .args(["--ignored", "--exact", "test_provider_child", "--nocapture"])
            .env(CASE_ENV, case)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start provider scenario child");
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if child.try_wait().expect("poll provider scenario child").is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().expect("collect timed out child output");
                panic!(
                    "provider scenario {case} timed out\nstdout: {}\nstderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            thread::sleep(Duration::from_millis(20));
        }
        let output = child.wait_with_output().expect("collect provider scenario output");
        assert!(
            output.status.success(),
            "provider scenario {case} failed with {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
#[ignore = "run only in an isolated provider scenario subprocess"]
async fn test_provider_child() {
    match std::env::var(CASE_ENV).as_deref() {
        Ok("ws_without_provider") => {
            assert!(CryptoProvider::get_default().is_none());
            connect_ws().await;
            assert!(CryptoProvider::get_default().is_none());
        }
        Ok("wss_without_provider") => {
            assert!(CryptoProvider::get_default().is_none());
            connect_wss().await;
            assert!(CryptoProvider::get_default().is_none());
        }
        Ok("wss_with_provider") => {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .expect("install explicit process provider");
            let before = CryptoProvider::get_default().expect("installed provider");
            connect_wss().await;
            let after = CryptoProvider::get_default().expect("provider remains installed");
            assert!(Arc::ptr_eq(before, after));
        }
        Ok("wss_invalid_provider") => {
            install_provider_without_cipher_suites();
            let error = match connect_wss_result().await {
                Ok(_) => panic!("invalid provider must fail WSS configuration"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
        }
        Ok("ws_with_invalid_provider") => {
            install_provider_without_cipher_suites();
            connect_ws().await;
        }
        _ => panic!("unknown or missing provider scenario"),
    }
}

fn install_provider_without_cipher_suites() {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    provider.cipher_suites.clear();
    provider
        .install_default()
        .expect("install deliberately invalid provider");
}

async fn connect_ws() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind WS peer");
    let address = listener.local_addr().expect("WS peer address");
    let peer = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept WS client");
        tokio_tungstenite::accept_async(tcp)
            .await
            .expect("complete WS handshake");
    });
    WsClient::builder()
        .build()
        .expect("build client")
        .connect(&format!("ws://{address}/"))
        .await
        .expect("connect WS");
    peer.await.expect("join WS peer");
}

async fn connect_wss() {
    connect_wss_result().await.expect("connect WSS");
}

async fn connect_wss_result() -> Result<qubit_websocket::WsSession, qubit_websocket::WsError> {
    let generated = generate_simple_self_signed(vec!["localhost".to_owned()]).expect("create test certificate");
    let certificate = CertificateDer::from(generated.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
    let mut roots = RootCertStore::empty();
    roots.add(certificate.clone()).expect("add test root");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let server_config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("server protocol versions")
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .expect("server certificate");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind WSS peer");
    let port = listener.local_addr().expect("WSS peer address").port();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let peer = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept WSS client");
        let tls = acceptor.accept(tcp).await.expect("complete TLS handshake");
        tokio_tungstenite::accept_async(tls)
            .await
            .expect("complete WSS handshake");
    });
    let client = WsClient::builder()
        .exclusive_root_certificates(roots)
        .build()
        .expect("build client");
    let result = client.connect(&format!("wss://localhost:{port}/")).await;
    if result.is_ok() {
        peer.await.expect("join WSS peer");
    }
    result
}
