# Qubit WebSocket

[![Rust CI](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-websocket/coverage-badge.json)](https://qubit-ltd.github.io/rs-websocket/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-websocket.svg?color=blue)](https://crates.io/crates/qubit-websocket)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-websocket` is a bounded Tokio WebSocket client for Rust services. A worker can connect to an existing event endpoint, send a subscription, consume progress messages, and close its session during shutdown. The library owns transport and memory limits; the application owns event meaning, acknowledgements, and recovery policy.

## Install

Add the crate to a Rust 1.94+ application:

```toml
[dependencies]
qubit-websocket = "0.1"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread", "sync"] }
```

The quick start assumes a WS or WSS endpoint is already running and accepts the sample subscription. It does not start a server.

## Quick start

This worker connects, sends a subscription, consumes messages until the peer closes, an error arrives, or the application requests shutdown, then checks the terminal snapshot and performs the bounded close handshake. `send` succeeding means only that the outbound queue accepted the message; it does not prove delivery or event processing.

```rust,no_run
use qubit_websocket::{WsClient, WsMessage};

async fn consume_events(
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = WsClient::builder().build()?;
    let mut session = client.connect("wss://events.example.com/ws").await?;
    session
        .send(WsMessage::Text("subscribe:job-42".to_owned()))
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;

    loop {
        let next = tokio::select! {
            _ = &mut shutdown => break,
            next = session.receive() => next,
        };
        match next {
            Some(Ok(WsMessage::Text(event))) => println!("event: {event}"),
            Some(Ok(WsMessage::Binary(bytes))) => println!("binary: {} bytes", bytes.len()),
            Some(Err(error)) => { eprintln!("session: {:?}", error.kind()); break; }
            None => break,
        }
    }

    let close_result = session.close().await;
    println!("terminal: {:?}", session.termination());
    close_result?;
    Ok(())
}
```

The service owns the other end of the one-shot channel and the worker task. Pass the worker function above and a future that completes after producers stop and service shutdown begins: the sender stays with the service while the receiver moves into the worker.

```rust,no_run
use std::{error::Error, future::Future};
use tokio::sync::oneshot;

async fn run_worker<F, Fut, S>(
    start: F,
    stop_producers_and_wait: S,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: FnOnce(oneshot::Receiver<()>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'static,
    S: Future<Output = ()>,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker = tokio::spawn(start(shutdown_rx));

    // This future resolves after service shutdown begins and producers have stopped.
    stop_producers_and_wait.await;
    let _ = shutdown_tx.send(());
    worker.await??;
    Ok(())
}
```

If the sender is dropped without sending, the receiver also completes and the worker follows the same close path. The endpoint and subscription format belong to your service; `send` only reports queue admission, not business acknowledgement. The `close()` handshake is bounded by the session's configured timeout.

In the code above, the close operation happens after the event loop and before the terminal snapshot is printed. `close()` can return an existing terminal error or `Timeout`, so the worker result lets the service account for shutdown failure.

The endpoint URL, subscription format, and meaning of received events belong to your service. For a runnable command-line example, see [`examples/ws_client.rs`](examples/ws_client.rs) and [`examples/wss_client.rs`](examples/wss_client.rs); both expect an already running endpoint.

## Capabilities

- Connect to `ws://` and `wss://` using RFC 6455 over HTTP/1.1.
- Verify TLS with standard WebPKI roots, add trust roots, or select an exclusive trust store.
- Set default or per-connection HTTP headers and offer WebSocket subprotocols.
- Bound inbound frames, reassembled messages, and inbound/outbound queues by message count and payload bytes.
- Receive text and binary messages while the session driver handles Ping, Pong, Close, and terminal errors.
- Use `try_send` to observe full capacity immediately or `send` to wait for capacity. Neither confirms peer delivery or application processing.

The client does not provide a server, browser support, proxies, automatic reconnection, replay, compression, or HTTP/2/HTTP/3 WebSockets. Applications must decide whether and how to retry after a disconnect, including how to avoid duplicate business effects.

## Migrating from the first design

| Earlier design name | Current API | Change |
| --- | --- | --- |
| `max_frame_bytes` | `max_inbound_frame_bytes` | Explicitly limits an inbound frame payload. |
| `max_message_bytes` | `max_inbound_message_bytes` | Explicitly limits a reassembled inbound message. |
| `root_certificates: Option<RootCertStore>` | `trust_roots: WsTrustRoots` | Selects `Default`, `Additional`, or `Exclusive` trust behavior. |

`WsClientBuilder::root_certificates(roots)` now adds those roots to the default WebPKI roots. To trust only supplied roots, use `WsClientBuilder::exclusive_root_certificates(roots)`.

`WsErrorKind::Cancelled` and `WsErrorKind::Closed` were removed because the runtime has no path that produces them. Match `WsSendError::Closed` when the send queue no longer accepts messages; cancelling a future does not return a `WsError`.

## Learn more

- [User guide](doc/user_guide.md) · [用户指南](doc/user_guide.zh_CN.md)
- [Rust API documentation](https://docs.rs/qubit-websocket)
- [中文 README](README.zh_CN.md)
- [Product requirements](doc/prd.zh_CN.md) · [design record](doc/design.zh_CN.md)

## Limits

The first release does not provide a WebSocket server, browser support, proxies, automatic reconnection, message replay, compression, or HTTP/2 and HTTP/3 WebSockets. The application owns business acknowledgements, retries, and recovery after a disconnected session.

## Testing

```bash
# Run tests with the default feature set
cargo test --locked

# Run tests with all declared features
cargo test --all-features --locked

# Project CI checks
./.infra/bin/ci-check.sh

# Check code coverage
./.infra/bin/coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./.infra/bin/align-ci.sh` to
format code and `./.infra/bin/ci-check.sh` to satisfy CI requirements before
submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-websocket](https://github.com/qubit-ltd/rs-websocket)
