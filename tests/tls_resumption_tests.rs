use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use qubit_websocket::{WsClient, WsMessage};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::PrivateKeyDer;
use rustls::{HandshakeKind, RootCertStore, ServerConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

const WATCHDOG: Duration = Duration::from_secs(10);

struct TestServer {
    address: std::net::SocketAddr,
    roots: RootCertStore,
    peer: JoinHandle<Vec<HandshakeKind>>,
}

async fn start_server(connection_count: usize) -> TestServer {
    let certified = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let config = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3 is supported")
    .with_no_client_auth()
    .with_single_cert(
        vec![certified.cert.der().clone()],
        PrivateKeyDer::Pkcs8(certified.key_pair.serialize_der().into()),
    )
    .expect("test certificate and key are valid");
    let mut config = config;
    config.send_tls13_tickets = 2;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let peer = tokio::spawn(async move {
        let mut handshakes = Vec::with_capacity(connection_count);
        for sequence in 0..connection_count {
            let (socket, _) = timeout(WATCHDOG, listener.accept())
                .await
                .expect("server accept watchdog")
                .expect("server accepts client");
            let tls = timeout(WATCHDOG, acceptor.accept(socket))
                .await
                .expect("TLS accept watchdog")
                .expect("TLS handshake succeeds");
            handshakes.push(
                tls.get_ref()
                    .1
                    .handshake_kind()
                    .expect("completed TLS handshake has a kind"),
            );
            let mut websocket = timeout(WATCHDOG, accept_async(tls))
                .await
                .expect("WebSocket upgrade watchdog")
                .expect("WebSocket upgrade succeeds");
            websocket
                .send(Message::Text(format!("sequence:{sequence}").into()))
                .await
                .expect("server sends sequence marker");
            match timeout(WATCHDOG, websocket.next())
                .await
                .expect("application acknowledgement watchdog")
                .expect("client keeps WebSocket open")
                .expect("client acknowledgement is valid")
            {
                Message::Text(text) if text == format!("ack:{sequence}") => {}
                other => panic!("unexpected acknowledgement: {other:?}"),
            }
            websocket
                .send(Message::Text(format!("confirmed:{sequence}").into()))
                .await
                .expect("server confirms application acknowledgement");
            timeout(WATCHDOG, websocket.close(None))
                .await
                .expect("server close watchdog")
                .expect("server closes WebSocket");
            // Consume the client's close response so each server connection is
            // fully shut down before accepting the next one.
            let close_response = timeout(WATCHDOG, websocket.next())
                .await
                .expect("client close response watchdog")
                .expect("client responds to close")
                .expect("client close frame is valid");
            assert!(matches!(close_response, Message::Close(_)));
        }
        handshakes
    });
    TestServer {
        address,
        roots,
        peer,
    }
}

async fn exchange_and_close(client: &WsClient, address: std::net::SocketAddr, sequence: usize) {
    let mut session = timeout(
        WATCHDOG,
        client.connect(&format!("wss://localhost:{}/", address.port())),
    )
    .await
    .expect("client connect watchdog")
    .expect("trusted local TLS and WebSocket connection succeeds");
    let message = timeout(WATCHDOG, session.receive())
        .await
        .expect("server marker receive watchdog")
        .expect("server sent application marker")
        .expect("server marker is valid");
    assert_eq!(message, WsMessage::Text(format!("sequence:{sequence}")));
    timeout(
        WATCHDOG,
        session.send(WsMessage::Text(format!("ack:{sequence}"))),
    )
    .await
    .expect("application acknowledgement send watchdog")
    .expect("client sends application acknowledgement");
    let confirmation = timeout(WATCHDOG, session.receive())
        .await
        .expect("application confirmation watchdog")
        .expect("server confirms application acknowledgement")
        .expect("server confirmation is valid");
    assert_eq!(
        confirmation,
        WsMessage::Text(format!("confirmed:{sequence}"))
    );
    timeout(WATCHDOG, session.close())
        .await
        .expect("client close watchdog")
        .expect("client close completes");
}

async fn finish_server(server: TestServer) -> Vec<HandshakeKind> {
    timeout(WATCHDOG, server.peer)
        .await
        .expect("server task watchdog")
        .expect("server task joins")
}

fn assert_handshakes(actual: Vec<HandshakeKind>, expected: &[HandshakeKind]) {
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_same_client_resumes_tls() {
    let server = start_server(2).await;
    let client = WsClient::builder()
        .exclusive_root_certificates(server.roots.clone())
        .build()
        .unwrap();
    exchange_and_close(&client, server.address, 0).await;
    exchange_and_close(&client, server.address, 1).await;
    assert_handshakes(
        finish_server(server).await,
        &[HandshakeKind::Full, HandshakeKind::Resumed],
    );
}

#[tokio::test]
async fn test_cloned_client_resumes_tls() {
    let server = start_server(2).await;
    let client = WsClient::builder()
        .exclusive_root_certificates(server.roots.clone())
        .build()
        .unwrap();
    exchange_and_close(&client, server.address, 0).await;
    let clone = client.clone();
    exchange_and_close(&clone, server.address, 1).await;
    assert_handshakes(
        finish_server(server).await,
        &[HandshakeKind::Full, HandshakeKind::Resumed],
    );
}

#[tokio::test]
async fn test_independent_clients_do_not_share_tls_resumption() {
    let server = start_server(3).await;
    let client_a = WsClient::builder()
        .exclusive_root_certificates(server.roots.clone())
        .build()
        .unwrap();
    let client_b = WsClient::builder()
        .exclusive_root_certificates(server.roots.clone())
        .build()
        .unwrap();
    exchange_and_close(&client_a, server.address, 0).await;
    exchange_and_close(&client_b, server.address, 1).await;
    exchange_and_close(&client_a, server.address, 2).await;
    assert_handshakes(
        finish_server(server).await,
        &[
            HandshakeKind::Full,
            HandshakeKind::Full,
            HandshakeKind::Resumed,
        ],
    );
}
