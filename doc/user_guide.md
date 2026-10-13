# User guide

This guide covers `qubit-websocket` 0.1 for Rust service developers who need to consume events from an existing WebSocket endpoint. The running scenario is a worker subscribing to a job-progress stream, forwarding events to application logic, and stopping cleanly during shutdown. The library handles the connection, protocol I/O, bounded queues, and terminal state; your service defines authentication policy, event decoding, persistence, acknowledgements, and reconnect decisions.

Start with the [README quick start](../README.md#quick-start) for the shortest path. This guide expands that flow into production integration choices. A server must already be running; the repository's [`ws_client`](../examples/ws_client.rs) and [`wss_client`](../examples/wss_client.rs) are runnable client examples, not servers.

## Prepare a service client

Add the client and the types used to construct headers and TLS trust stores as direct dependencies. Use the same Rust toolchain as the crate (Rust 1.94 or newer):

```toml
[dependencies]
qubit-websocket = "0.1"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread", "sync", "time"] }
http = "1.5"
rustls = "0.23"
```

The following belongs in the worker module that owns the upstream subscription. The endpoint must accept `subscribe:job-42`; replace that application-specific value with your server's documented request. A successful connection returns HTTP status 101 and exposes the negotiated subprotocol, if any. A connection failure is returned as `WsError` before a session exists.

```rust,no_run
use http::header::AUTHORIZATION;
use http::HeaderMap;
use qubit_websocket::{WsClient, WsMessage};

async fn subscribe(
    token: &str,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
    let client = WsClient::builder()
        .headers(headers)
        .subprotocols(vec!["job-events.v1".to_owned()])
        .build()?;
    let mut session = client.connect("wss://events.example.com/ws").await?;
    println!("upgrade status: {}", session.response_status_code());
    println!("selected protocol: {:?}", session.selected_subprotocol());
    session
        .send(WsMessage::Text("subscribe:job-42".to_owned()))
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    // The receive_events helper below waits for either an event or shutdown.
    receive_events(&mut session, shutdown).await?;
    Ok(())
}

async fn receive_events(
    session: &mut qubit_websocket::WsSession,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), qubit_websocket::WsError> {
    loop {
        let next = tokio::select! {
            _ = &mut shutdown => break,
            next = session.receive() => next,
        };
        match next {
            Some(Ok(WsMessage::Text(text))) => println!("job event: {text}"),
            Some(Ok(WsMessage::Binary(bytes))) => println!("binary event: {} bytes", bytes.len()),
            Some(Err(error)) => { eprintln!("stream ended with {:?}", error.kind()); break; }
            None => break,
        }
    }
    let close_result = session.close().await;
    match session.termination() {
        Some(qubit_websocket::WsTermination::Normal { code }) => println!("normal close: {code:?}"),
        Some(qubit_websocket::WsTermination::Error { kind }) => eprintln!("terminal failure: {kind:?}"),
        None => eprintln!("no terminal state yet"),
    }
    close_result
}
```

Treat tokens as secrets: do not log headers or include credentials in endpoint URLs. `connect_with(url, headers)` supplies headers for one connection; these override same-named defaults set with `WsClientBuilder::headers`. The server may reject a missing or invalid token with a handshake status error. A subprotocol is an ordered offer; inspect `selected_subprotocol()` after connection and apply your service's policy if the server selected none.

## Choose TLS trust roots

`WsClientBuilder` defaults to `WsTrustRoots::Default`: standard WebPKI roots are used. This is the usual choice for public service certificates. For a private CA, prepare a `rustls::RootCertStore` from certificates your application has loaded and choose one of these policies:

| Policy | Builder call | Trust behavior |
| --- | --- | --- |
| Default | no certificate method | Standard WebPKI roots. |
| Additional | `.root_certificates(roots)` | Standard roots plus supplied roots. |
| Exclusive | `.exclusive_root_certificates(roots)` | Only supplied roots. |

The following is a builder fragment for the connection setup above. `roots` must already contain the CA certificate for the endpoint; load and validate that certificate using your application's certificate provisioning code. With `Additional`, either standard roots or the supplied root can validate the peer. With `Exclusive`, a peer chain outside `roots` fails with `WsErrorKind::Tls`; there is no fallback to plaintext.

```rust,no_run
use qubit_websocket::WsClient;
use rustls::RootCertStore;

fn client_with_private_ca(roots: RootCertStore) -> Result<WsClient, qubit_websocket::WsError> {
    WsClient::builder()
        .exclusive_root_certificates(roots)
        .build()
}
```

Use `.root_certificates(roots)` instead when public roots should continue to work alongside a private CA. The distinction matters during certificate rotation and for endpoints whose chain is issued by a public authority.

## Consume the event stream

The `receive_events` helper in the worker setup above handles the stream after the subscription send. Each returned `WsMessage` is an application event for your decoder. Persisting it, updating job state, and deciding whether to acknowledge it are application operations; a successful `receive()` only says a message was read from the bounded inbound queue.

The observable normal path is `receive()` returning `None` and `termination()` reporting `Normal`; an error is yielded once as `Some(Err(error))`, after which receive returns `None`, and the snapshot reports `Error { kind }`. `close()` initiates a bounded close handshake and is safe to call repeatedly. It can return a terminal error or `Timeout`; handle that result as part of worker shutdown.

The service creates the channel and owns its sender and the task handle. Pass the receiver into `subscribe`; on shutdown, stop producers first, send the one-shot signal, and await the worker so its `close()` result is observed:

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
    // This future resolves after producers stop and service shutdown begins.
    stop_producers_and_wait.await;
    let _ = shutdown_tx.send(());
    worker.await??;
    Ok(())
}
```

If the sender is dropped without sending, the receiver also completes and the worker takes the same close path. A normal peer close makes `receive()` return `None` with `WsTermination::Normal`; a terminal error is yielded once and recorded as `WsTermination::Error`. Local shutdown ends the receive wait, then `close()` starts the bounded handshake. If the peer does not complete it in time, the worker reports `WsErrorKind::Timeout`; a previously recorded terminal error may also be returned. Queue admission is not a peer or business acknowledgement.

Connect the subscription function to the worker helper by moving the token into the closure. The shutdown future represents stopping producers before signaling the worker:

```rust,no_run
async fn run_subscription(token: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_worker(
        move |shutdown| async move { subscribe(&token, shutdown).await },
        async {
            // Stop producers and wait for them to finish.
        },
    )
    .await
}
```

## Share outbound work safely

A worker may have several producers sending commands over one session. Clone `WsSender` and give each producer its own clone; all clones share the session's outbound message and byte budgets. The following belongs in the worker's producer setup, after `session` has connected. Both tasks need a Tokio runtime. Each successful send means queue admission only, so correlate important commands with application-level acknowledgements from the event stream.

```rust,no_run
use qubit_websocket::{WsMessage, WsSession};

async fn spawn_producers(session: &WsSession) -> Result<(), tokio::task::JoinError> {
    let sender = session.sender();
    let first = sender.clone();
    let one = tokio::spawn(async move {
        first.send(WsMessage::Text("pause:job-42".to_owned())).await
    });
    let two = tokio::spawn(async move {
        sender.send(WsMessage::Text("status:job-42".to_owned())).await
    });
    let first_result = one.await?;
    let second_result = two.await?;
    println!("enqueue results: {first_result:?}, {second_result:?}");
    Ok(())
}
```

Use `try_send` when a producer must not wait: `Backpressure` means a message-count or byte budget is currently full, `MessageTooLarge` means the message exceeds the configured outbound byte budget, and `Closed` means the session no longer accepts messages. With `send`, capacity pressure waits; if the send future is cancelled before it enqueues, its temporary capacity reservations are released. Neither method guarantees network delivery.

## Set budgets and respond to backpressure

`WsSessionLimits::default()` allows 1 MiB per inbound frame, 4 MiB per reassembled inbound message, 64 queued messages and 4 MiB of payload on each queue, a 30 second no-progress timeout, and a 5 second close timeout. Configure these limits from expected event sizes and the worker's memory budget. The frame limit cannot exceed the message limit, and the inbound message limit cannot exceed `inbound_bytes`; invalid or zero settings make `build()` return `WsErrorKind::InvalidConfig`.

This belongs in the worker's client construction. The values below reduce queue capacity for a small event consumer; they are examples to tune from service measurements, not universal recommendations. The observable result is that a full outbound queue makes `try_send` return `Backpressure`, while `send` waits until capacity becomes available or the session closes.

```rust,no_run
use std::time::Duration;
use qubit_websocket::{WsClient, WsSessionLimits};

fn bounded_client() -> Result<WsClient, qubit_websocket::WsError> {
    let limits = WsSessionLimits {
        max_inbound_frame_bytes: 256 * 1024,
        max_inbound_message_bytes: 1024 * 1024,
        inbound_messages: 16,
        inbound_bytes: 1024 * 1024,
        outbound_messages: 16,
        outbound_bytes: 256 * 1024,
        no_progress_timeout: Duration::from_secs(30),
        close_timeout: Duration::from_secs(5),
    };
    WsClient::builder().limits(limits).build()
}
```

Inbound capacity is released as the application removes messages through `receive`; if processing stalls, the bounded inbound queue fills and the driver stops accepting more application messages until progress resumes. Outbound capacity remains reserved while messages are queued or being written. On `Backpressure`, choose an application policy: wait with `send`, defer or coalesce replaceable status updates, or report overload upstream. Do not spin on `try_send`; it does not wait for capacity.

## Shutdown, cancellation, and recovery

During graceful service shutdown, stop producers, send the one-shot shutdown signal, then await the worker task. The worker exits its `tokio::select!` loop and calls `session.close().await`; that handshake waits no longer than the configured close timeout, and timeout aborts the driver with `WsErrorKind::Timeout`. A peer close is normal only when `termination()` reports `WsTermination::Normal`. A transport or protocol failure is recorded as `WsTermination::Error` and does not transparently reconnect. If the one-shot sender is dropped without sending, the receiver completes and also triggers local shutdown.

Dropping a pending `receive()` future only stops that caller's wait; it does not close the session. Cancelling a pending `send()` drops that enqueue attempt and releases any temporary queue reservations. Dropping a session or cancelling its owner does not make the library reconnect, replay queued work, or recover application state. On restart, the application must decide how to re-subscribe, reconcile missed events, and make repeated commands safe (for example, with an application-owned idempotency key or sequence cursor).

## Errors and troubleshooting

`WsError::kind()` gives a stable category; `status_code()` and `handshake_failure()` add handshake details when available. Categories include `InvalidUrl`, `Connect`, `Tls`, `Handshake`, `Protocol`, `FrameTooLarge`, `MessageTooLarge`, `Timeout`, `Internal`, and `InvalidConfig`. Cancelling a future drops that operation and does not return a `WsError`; `WsErrorKind::Cancelled` and `WsErrorKind::Closed` have no runtime-producing path and are not variants of the current API. Send queue failures use the separate `WsSendError` categories `Backpressure`, `MessageTooLarge`, and `Closed`; `WsSendError::Closed` means the sender no longer accepts messages.

| Symptom | Check |
| --- | --- |
| `Tls` on WSS | URL hostname, certificate validity, and whether the selected trust mode contains the issuing CA. |
| `Handshake` with status | Authentication and server route; inspect `status_code()` without logging credentials. |
| No selected subprotocol | Compare the offered values with server configuration and check `selected_subprotocol()`. |
| `FrameTooLarge` or `MessageTooLarge` | Compare server event shape with `max_inbound_frame_bytes` and `max_inbound_message_bytes`; raising limits also raises potential memory use. |
| `Backpressure` | Check producer rate and whether the outbound count or byte budget is undersized; choose an explicit wait, defer, or overload policy. |
| `Timeout` | Check DNS/connect/handshake progress, peer reads/writes, and the configured no-progress or close deadline. |

Never log bearer tokens or assume queue admission is a business acknowledgement. For exact signatures, see the [API docs](https://docs.rs/qubit-websocket).

## Further reading

- [README](../README.md) · [中文 README](../README.zh_CN.md)
- [API documentation](https://docs.rs/qubit-websocket)
- [Runnable WS example](../examples/ws_client.rs) · [runnable WSS example](../examples/wss_client.rs)
- [中文用户指南](user_guide.zh_CN.md)
