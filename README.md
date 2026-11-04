# Qubit WebSocket

[![Rust CI](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-websocket/coverage-badge.json)](https://qubit-ltd.github.io/rs-websocket/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-websocket.svg?color=blue)](https://crates.io/crates/qubit-websocket)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-websocket` is a bounded Tokio WebSocket client for Rust services. A worker can connect to an existing event endpoint, send a subscription, consume progress messages, and close its session during shutdown. The library manages the connection, protocol I/O, bounded queues for admitted messages, and the WebSocket write-buffer limit. Pending `send()` futures each retain their payload, so these limits are not a cap on total session or process memory. The application owns event meaning, acknowledgements, and recovery policy.

## Install

Add the crate to a Rust 1.94+ application:

```toml
[dependencies]
qubit-websocket = "0.3"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread", "sync"] }
```

The quick start assumes a WS or WSS endpoint is already running and accepts the sample subscription. It does not start a server.

Configuration support is optional. Enable it when the application wants to build a client from `qubit-config`; the application must also depend directly on `qubit-config` to create its `Config` value:

```toml
[dependencies]
qubit-websocket = { version = "0.3", features = ["config"] }
qubit-config = "0.15"
```

See the [configuration section in the user guide](doc/user_guide.md#load-client-settings-from-config) for the supported keys and boundaries.

Numeric option validation exposes a value-free `WsConfigFailure` through `WsError::config_failure()`; see the [error guide](doc/user_guide.md#errors-and-troubleshooting) for its scope and `None` cases.

The `config` feature uses the public `Config` and `ConfigError` types from `qubit-config` 0.15. When migrating from 0.2, update the direct dependency and any imports of those types; `WsClientBuilder::from_config` keeps the same call form and configuration keys.

**Handshake compatibility:** the pinned registry `tungstenite` 0.30.0 baseline rejects comma-separated `Connection` tokens and duplicate fields when `Upgrade` is not first; it accepts the duplicate order with `Upgrade` first. The current unpatched registry consumer contract is checked manually and every Monday at 06:23 UTC; see the corresponding Actions run's logs and `consumer-registry-current-evidence` artifact for the resolved version and result. A failed probe means the currently resolved consumer did not satisfy the contract; it can reflect an existing registry limitation or another failure. A passing probe does not by itself establish that an application can remove its patch. Applications needing the full valid token-list behavior should add the fixed-revision patch shown in the [consumer setup guide](doc/user_guide.md#consumer-dependency-setup) to their Cargo workspace root. This library's Cargo patch does not propagate to applications or published packages; verify the resolved source and consumer scenarios before removing an application patch.

## Quick start

This worker connects, sends a subscription, consumes messages until the peer closes, an error arrives, or the application requests shutdown, then checks the terminal snapshot and performs the bounded close handshake. `send` succeeding means only that the outbound queue accepted the message; it does not prove delivery or event processing.

<!-- doc-example: id=readme_consume group=readme modes=default,config -->
```rust,no_run
use qubit_websocket::{WsClient, WsMessage};

async fn consume_events(
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = WsClient::builder().build()?;
    let mut session = client.connect("wss://events.example.com/ws").await?;
    session
        .send(WsMessage::Text("subscribe:job-42".to_owned()))
        .await?;

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

The host service owns the external one-shot sender. After it stops producers and waits for them to finish, it sends the shutdown signal. The external shutdown receiver is passed to `serve` below; the host can also observe the worker task's result.

<!-- doc-example: id=readme_worker group=readme modes=default,config -->
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
    let mut worker = tokio::spawn(start(shutdown_rx));

    tokio::select! {
        result = &mut worker => {
            result??;
            return Ok(());
        }
        _ = stop_producers_and_wait => {}
    }
    let _ = shutdown_tx.send(());
    worker.await??;
    Ok(())
}
```

The helper uses `tokio::select!` to wait for either the worker or host shutdown. If the worker ends first, its result returns immediately; if the host signal arrives first, the helper signals the worker and waits for its close result. Dropping the host sender without sending also triggers shutdown. The endpoint and subscription format belong to your service; `send` only reports queue admission, not business acknowledgement. The `close()` handshake is bounded by the session's configured timeout, but that timeout does not bound total worker shutdown; handlers and task joins need an application shutdown policy. See [shutdown and recovery](doc/user_guide.md#shutdown-cancellation-and-recovery).

Start the worker by passing its function to the helper. The shutdown future should complete after your service has stopped producers:

<!-- doc-example: id=readme_serve group=readme modes=default,config -->
```rust,no_run
async fn serve(
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_worker(consume_events, async move {
        let _ = shutdown.await;
    })
    .await
}
```

The host keeps the matching sender, stops producers and waits for them to finish, then sends `()`. If that sender is dropped first, the receiver also completes and initiates the same shutdown path.

In the code above, the close operation happens after the event loop and before the terminal snapshot is printed. `close()` can return an existing terminal error or `Timeout`, so the worker result lets the service account for shutdown failure.

The endpoint URL, subscription format, and meaning of received events belong to your service. For a runnable command-line example, see [`examples/ws_client.rs`](examples/ws_client.rs) and [`examples/wss_client.rs`](examples/wss_client.rs); both expect an already running endpoint.

`termination()` reports `Closed { code }` when the protocol close handshake completes. The code is the peer's original status code, and the application must interpret it according to its service protocol; for example, `1009` reports a message-too-large close and does not mean that a subscription succeeded. `Error { kind }` reports a failed close or another terminal failure. The printed output in this example is diagnostic only; it does not represent persistence or business acknowledgement.

Dropping `WsSession` requests bounded local shutdown and closes both queues, even while cloned `WsSender`s remain alive; those senders then return `WsSendError::Closed`. Messages already queued but not written may be discarded. Drop does not let the caller observe peer confirmation or the close result, so explicitly await `close()` when shutdown outcome matters.

`send()` returns `WsSendError` when queue admission fails. For applications that need to retain a rejected message, `try_send_recoverable()` and `send_recoverable()` return `WsSendFailure`; the [user guide](doc/user_guide.md#recover-a-message-rejected-by-the-outbound-queue) shows how to wait once after backpressure. Handle `MessageTooLarge` and `Closed` according to service policy. Queue admission errors are separate from the session's terminal `WsError`; successful admission does not mean delivery or acknowledgement.

When the reader observes peer Close or ends with an error, the driver closes outbound admission before it waits for the writer's bounded close action. New sends and capacity waiters then receive `Closed`; messages accepted earlier but not yet written may be discarded. Inbound messages already accepted remain available before the terminal result.

## Capabilities

- Connect to `ws://` and `wss://` using RFC 6455 over HTTP/1.1.
- Use bracketed IPv6 URL literals such as `ws://[::1]:8080/events`; the TCP target parser validates the enclosed address. Scoped IPv6 literals are unsupported, and WSS continues to validate the certificate chain and server identity.
- Verify TLS with standard WebPKI roots, add trust roots, or select an exclusive trust store.
- Plain `ws://` connections do not initialize TLS. For `wss://`, an installed rustls provider is used; without one, aws-lc is used for that connection without changing the process default. Install ring or a custom provider explicitly during application startup if required; an incompatible installed provider returns `WsErrorKind::InvalidConfig`.
- Set default or per-connection HTTP headers and offer WebSocket subprotocols. A `HeaderMap` can carry multiple values for one name; per-connection headers replace the complete default value group for matching names, while unrelated defaults remain. Protocol-owned headers are rejected.
- Bound inbound frames, reassembled messages, and inbound/outbound queues by message count and payload bytes.
- The [outbound payload queue budget](doc/user_guide.md#set-budgets-and-respond-to-backpressure) is separate from the WebSocket write-buffer limit, which adds 14 bytes for the maximum data-frame header and 131 bytes for a maximum client control frame. Even a minimal valid payload budget retains Ping/Pong/Close processing capacity; this is not a total session memory bound or a delivery guarantee.
- Receive text and binary messages while the session driver handles Ping, Pong, Close, and terminal errors. When the inbound queue is full and another business message arrives before capacity returns, the session ends with `WsErrorKind::InboundBackpressure`; if no extra message arrives, expiry of `no_progress_timeout` ends it with `WsErrorKind::Timeout`. Both paths attempt a `1013` close, but a failed close write cannot guarantee that the peer sees it. These are session failures, not business acknowledgements; `WsSendError::Backpressure` separately means an outbound message was not admitted.
- Use `try_send` to observe full capacity immediately or `send` to wait for capacity. Neither confirms peer delivery or application processing.

The client does not provide a server, browser support, proxies, automatic reconnection, replay, compression, or HTTP/2/HTTP/3 WebSockets. Applications own business acknowledgements, retry decisions, and recovery after a disconnect, including how to avoid duplicate business effects.

Do not treat every `Closed` state as successful work: only your application protocol can decide whether a code such as `1000`, `1009`, or no code means completion, rejection, or requires reconciliation. `close()` returning `Ok(())` confirms protocol shutdown, not subscription success or durable processing.

## Learn more

- [User guide](doc/user_guide.md) · [用户指南](doc/user_guide.zh_CN.md)
- [Architecture](doc/architecture.md) · [架构说明](doc/architecture.zh_CN.md)
- [Rust API documentation](https://docs.rs/qubit-websocket)
- [中文 README](README.zh_CN.md)
- [Product requirements](doc/prd.zh_CN.md)

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

# Compile every marked Rust example in both languages
python3 -m unittest discover -s scripts/tests -p 'test_check_doc_examples.py'
python3 scripts/check-doc-examples.py --mode default
python3 scripts/check-doc-examples.py --mode config

# Verify interoperability with a local current rs-web checkout
python3 -m unittest discover -s scripts/tests -p 'test_check_current_interop.py'
python3 scripts/check-current-interop.py \
  --web /absolute/path/to/rs-web \
  --websocket "$PWD" \
  --output /tmp/rs-websocket-current-interop
```

These checks require Python 3.11 or later and Cargo. The Python scripts use only
the standard library. The config examples require the sibling `rs-config` checkout; run
`./.infra/bin/prepare-local-path-dependencies.sh` first when those local path
dependencies are not already available. The current-source interoperability
command uses disposable copies and records both repository revisions, dirty
state, and content digests. Its result applies only to that recorded pair of
source trees. The fixed-revision consumer and downstream workflows remain
separate regression checks. The consumer workflow runs the pinned historical
baseline on pushes, pull requests, and manual runs; its current registry probe
runs manually or every Monday at 06:23 UTC. A current-probe failure is evidence
that the resolved unpatched consumer did not meet the contract, not by itself
proof of a new regression. Inspect the run logs and
`consumer-registry-current-evidence` artifact. A passing probe does not alone
authorize removing an application patch; check the resolved version and
consumer scenarios first.

## License

Copyright (c) 2025 - 2026. Haixing Hu.

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
