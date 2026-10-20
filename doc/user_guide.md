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
        .await?;
    // The receive_events helper below waits for either an event or shutdown.
    receive_events(&mut session, shutdown).await?;
    Ok(())
}

#[derive(Debug)]
struct AppError(&'static str);

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for AppError {}

async fn handle_job_event(event: &str) -> Result<(), AppError> {
    // Decode and persist the event, then send any application-level acknowledgement.
    // Return Ok(()) only after those business steps have completed successfully.
    let _ = event;
    Ok(())
}

async fn receive_events(
    session: &mut qubit_websocket::WsSession,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut processing_failure = None;
    loop {
        let next = tokio::select! {
            _ = &mut shutdown => break,
            next = session.receive() => next,
        };
        match next {
            Some(Ok(WsMessage::Text(text))) => {
                if let Err(error) = handle_job_event(&text).await {
                    eprintln!("application could not process the event: {error}");
                    processing_failure = Some(error);
                    break;
                }
            }
            Some(Ok(WsMessage::Binary(bytes))) => println!("binary event: {} bytes", bytes.len()),
            Some(Err(error)) => {
                eprintln!("stream ended with {:?}; apply the service's existing recovery policy", error.kind());
                break;
            }
            None => break,
        }
    }
    let close_result = session.close().await;
    match session.termination() {
        Some(qubit_websocket::WsTermination::Closed { code: Some(1000) }) => {
            println!("protocol close completed; application policy may mark the subscription complete");
        }
        Some(qubit_websocket::WsTermination::Closed { code: Some(1009) }) => {
            eprintln!("peer rejected a message as too large; record it, stop this subscription, and adjust the request or payload before deciding whether to retry");
        }
        Some(qubit_websocket::WsTermination::Closed { code: None }) => {
            eprintln!("peer omitted a close code; mark the outcome unknown and reconcile before treating the subscription as complete");
        }
        Some(qubit_websocket::WsTermination::Closed { code }) => {
            eprintln!("protocol close completed with application-defined code {code:?}");
        }
        Some(qubit_websocket::WsTermination::Error { kind }) => eprintln!("terminal failure: {kind:?}"),
        None => eprintln!("no terminal state yet"),
    }
    // A successful close result confirms protocol shutdown, not business completion.
    close_result?;
    if let Some(error) = processing_failure {
        return Err(Box::new(error));
    }
    Ok(())
}
```

Treat tokens as secrets: do not log headers or include credentials in endpoint URLs. `connect_with(url, headers)` supplies headers for one connection; these override same-named defaults set with `WsClientBuilder::headers`. A `HeaderMap` can contain multiple values for one name, and all values from the later source replace the complete default group for that name. The server may reject a missing or invalid token with a handshake status error. A subprotocol is an ordered offer; inspect `selected_subprotocol()` after connection and apply your service's policy if the server selected none.

The `send()` call above returns its original `WsSendError` to the worker caller if queue admission fails. Handle `Backpressure`, `MessageTooLarge`, or `Closed` according to the service policy; these queue errors are separate from a session's terminal `WsError`. A successful return means only that the queue accepted the message.

`handle_job_event(&str) -> Result<(), AppError>` is an application-owned boundary. Implement it with the service's decoding and persistence code, and return success only after the required business processing and any business acknowledgement are complete. This example stops processing when the handler returns `AppError`, closes the WebSocket, and returns that application error. A production worker may instead retry within a limit, quarantine the event, or stop the service; do not acknowledge an event before its required processing succeeds.

## Load client settings from config

The optional `config` feature lets the application create a builder from a `qubit_config::Config` subtree. Enable the feature and depend on `qubit-config` directly wherever the application constructs `Config`:

```toml
[dependencies]
qubit-websocket = { version = "0.1", features = ["config"] }
qubit-config = "0.14"
```

The application chooses the prefix. `websocket` is only an example; a service may use `service.websocket`, or an empty prefix to select the root configuration. The selected subtree must contain at least one property.

```rust,no_run
use http::HeaderMap;
use qubit_config::Config;
use qubit_websocket::{WsClientBuilder, config_keys};

fn build_client(runtime_headers: HeaderMap) -> Result<qubit_websocket::WsClient, Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.set(format!("websocket.{}", config_keys::CONNECT_TIMEOUT_MS), 3000_u64)?;
    config.set(format!("websocket.{}", config_keys::SUBPROTOCOLS), vec!["jobs.v1"])?;
    let client = WsClientBuilder::from_config(&config, "websocket")?
        .headers(runtime_headers)
        .build()?;
    Ok(client)
}
```

The stable relative key names are defined once in `qubit_websocket::config_keys`:

| Constant | Relative key | Value |
| --- | --- | --- |
| `CONNECT_TIMEOUT_MS` | `connect_timeout_ms` | `u64`, milliseconds |
| `HANDSHAKE_TIMEOUT_MS` | `handshake_timeout_ms` | `u64`, milliseconds |
| `SUBPROTOCOLS` | `subprotocols` | `Vec<String>` |
| `MAX_INBOUND_FRAME_BYTES` | `limits.max_inbound_frame_bytes` | `usize`, bytes |
| `MAX_INBOUND_MESSAGE_BYTES` | `limits.max_inbound_message_bytes` | `usize`, bytes |
| `INBOUND_MESSAGES` | `limits.inbound_messages` | `usize`, messages |
| `INBOUND_BYTES` | `limits.inbound_bytes` | `usize`, bytes |
| `OUTBOUND_MESSAGES` | `limits.outbound_messages` | `usize`, messages |
| `OUTBOUND_BYTES` | `limits.outbound_bytes` | `usize`, bytes |
| `NO_PROGRESS_TIMEOUT_MS` | `limits.no_progress_timeout_ms` | `u64`, milliseconds |
| `CLOSE_TIMEOUT_MS` | `limits.close_timeout_ms` | `u64`, milliseconds |

Omitted keys retain the builder's current defaults. Invalid paths, an absent or empty selected subtree, unknown properties, and value conversion errors are returned as `ConfigError` with configuration path context. A configured zero or inconsistent limits remain intact until `build()`, which reports the existing `WsErrorKind::InvalidConfig`. The adapter performs no network I/O. It does not load request headers, TLS trust roots, or a URL: provide headers and trust roots through the builder methods, and pass the URL to `connect` for each connection.

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

The `receive_events` helper in the worker setup above handles the stream after the subscription send. Each returned `WsMessage` is an application event for your decoder. The sample calls the application-owned `handle_job_event`; replace its placeholder body with the service's decoding and persistence logic. It must return `Ok(())` only after required business processing and any business acknowledgement have completed. A successful `receive()` only says a message was read from the bounded inbound queue.

When the close handshake completes, `receive()` returns `None` and `termination()` reports `Closed { code }`. The code is the peer's original status and is interpreted by the application: for this example, `Some(1000)` may mark the subscription complete; `Some(1009)` should record a size rejection and stop blind retries; `None` requires reconciliation against the service protocol. An error is yielded once as `Some(Err(error))`, after which receive returns `None`, and the snapshot reports `Error { kind }`. `close()` initiates a bounded close handshake and is safe to call repeatedly. `Ok(())` means protocol shutdown completed, including a completed `1009` handshake; it does not mean the subscription succeeded. A failed close reply returns an error, and `close()` can also return a terminal error or `Timeout`; handle that result as part of worker shutdown. These example prints do not represent persistence or business acknowledgement.

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

If the sender is dropped without sending, the receiver also completes and the worker takes the same close path. A completed peer close makes `receive()` return `None` with `WsTermination::Closed { code }`; the application interprets the code. A terminal error is yielded once and recorded as `WsTermination::Error`. Local shutdown ends the receive wait, then `close()` starts the bounded handshake. If the peer does not complete it in time, the worker reports `WsErrorKind::Timeout`; a previously recorded terminal error may also be returned. Queue admission is not a peer or business acknowledgement.

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

async fn spawn_producers(
    session: &WsSession,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sender = session.sender();
    let first = sender.clone();
    let one = tokio::spawn(async move {
        first.send(WsMessage::Text("pause:job-42".to_owned())).await
    });
    let two = tokio::spawn(async move {
        sender.send(WsMessage::Text("status:job-42".to_owned())).await
    });
    let (one, two) = tokio::join!(one, two);
    one??;
    two??;
    Ok(())
}
```

The helper waits for both producer tasks and returns either a task failure (`JoinError`) or a failed queue admission (`WsSendError`) to its caller. A `WsSendError` means the message was not enqueued; the caller must choose whether to retry, defer, or apply an overload policy.

Use `try_send` when a producer must not wait: `Backpressure` means a message-count or byte budget is currently full, `MessageTooLarge` means the message exceeds the configured outbound byte budget, and `Closed` means the session no longer accepts messages. With `send`, capacity pressure waits; if the send future is cancelled before it enqueues, its temporary capacity reservations are released. Neither method guarantees network delivery. Choose the response to `Backpressure` by message meaning: defer or coalesce replaceable status updates, but wait with `send()` or report overload for a command that must be delivered. Do not silently discard a non-repeatable command or spin on `try_send()`.

The `WsSession` owns session liveness. If it is dropped, it requests bounded local shutdown and closes both queues even if producer tasks still hold cloned `WsSender`s. Those clones then receive `WsSendError::Closed`; messages that were queued but not yet written may be discarded. Drop does not expose the close result or establish that the peer confirmed it. During planned shutdown, stop producers and await their completion first, then explicitly call `session.close().await`, record its result, and finally let the session drop. This ordering gives the service a close outcome without racing new sends against shutdown.

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

Inbound capacity is released as the application removes messages through `receive`. While a message is waiting for a full queue, the driver keeps polling control frames and retains at most that one waiting message. If capacity returns, the message is queued. If another application message arrives first, the driver can close early with WebSocket code `1013`; if no extra application data arrives, expiry of `no_progress_timeout` also triggers that close. If writing the Close frame fails, the peer is not guaranteed to see it. Outbound capacity remains reserved while messages are queued or being written. On `Backpressure`, choose an application policy: wait with `send`, defer or coalesce replaceable status updates, or report overload upstream. Do not spin on `try_send`; it does not wait for capacity.

## Shutdown, cancellation, and recovery

During graceful service shutdown, stop producers and wait for them to finish, send the one-shot shutdown signal, then await the worker task. The worker exits its `tokio::select!` loop and calls `session.close().await` before the session is dropped; this bounded handshake returns the close result to the worker, and timeout aborts the driver with `WsErrorKind::Timeout`. `WsTermination::Closed { code }` means the close handshake completed; the application interprets the code, so `1009` is not subscription success. Treat `1009` as a rejection to diagnose and adjust rather than blindly retrying; if the code is absent, reconcile service state before marking the subscription complete. A transport or protocol failure is recorded as `WsTermination::Error` and does not transparently reconnect. If the one-shot sender is dropped without sending, the receiver completes and the worker takes the same explicit close path.

Dropping a pending `receive()` future only stops that caller's wait; it does not close the session. Cancelling a pending `send()` drops that enqueue attempt and releases any temporary queue reservations. If cancellation or task exit drops the `WsSession`, local shutdown is requested and surviving sender clones become closed; queued-but-unwritten messages may be discarded, and Drop does not report whether the peer confirmed closure. The library does not reconnect, replay queued work, or recover application state. On restart, the application must decide how to re-subscribe, reconcile missed events, and make repeated commands safe (for example, with an application-owned idempotency key or sequence cursor).

## Errors and troubleshooting

`WsError::kind()` gives a stable category; `status_code()` and `handshake_failure()` add handshake details when available. Categories include `InvalidUrl`, `Transport`, `Tls`, `Handshake`, `Protocol`, `FrameTooLarge`, `MessageTooLarge`, `Timeout`, `Internal`, and `InvalidConfig`. `Transport` covers connection establishment and ordinary network I/O during the HTTP upgrade or an established session; task failures are `Internal`. HTTP rejection and structured WebSocket upgrade validation failures remain `Handshake` and can include a status code. Cancelling a future drops that operation and does not return a `WsError`; `WsErrorKind::Cancelled` and `WsErrorKind::Closed` have no runtime-producing path and are not variants of the current API. Send queue failures use the separate `WsSendError` categories `Backpressure`, `MessageTooLarge`, and `Closed`; propagate or handle these at the send call site. `WsSendError::Closed` means the sender no longer accepts messages, while `WsTermination::Error` records a session-level terminal failure.

| Symptom | Check |
| --- | --- |
| `Transport` | Check DNS/TCP connection setup or ordinary socket I/O. If the peer disconnects while the HTTP upgrade is in progress, this is also `Transport`; inspect network reachability and server availability. |
| `InvalidUrl` for an IPv6 endpoint | Write the literal with brackets, for example `ws://[::1]:8080/events`. Brackets are part of URL syntax; the TCP target is parsed as the enclosed IPv6 address. Scoped IPv6 literals are unsupported. |
| `Tls` on WSS | URL hostname, certificate validity, and whether the selected trust mode contains the issuing CA. |
| `Handshake` with status | HTTP rejection, authentication, or server route; inspect `status_code()` without logging credentials. `Handshake` without status can indicate an invalid upgrade response; check the endpoint's WebSocket configuration. |
| No selected subprotocol | Compare the offered values with server configuration and check `selected_subprotocol()`. |
| `FrameTooLarge` or `MessageTooLarge` | Compare server event shape with `max_inbound_frame_bytes` and `max_inbound_message_bytes`; raising limits also raises potential memory use. |
| `Backpressure` | Check producer rate and whether the outbound count or byte budget is undersized; choose an explicit wait, defer, or overload policy. |
| `Timeout` | Check DNS/connect/handshake progress, peer reads/writes, and the configured no-progress or close deadline. An inbound slow-consumer close also ends with this public category; it does not mean business processing completed or that the peer confirmed the Close frame. |

The `outbound_bytes` setting bounds queued application payload bytes. It is separate from the WebSocket driver's write-buffer limit: the latter adds 14 bytes for a maximum data-frame header and 131 bytes for a maximum client control frame. Even with a one-byte payload budget, Ping/Pong and Close processing retain buffer capacity. These queue and write-buffer budgets are not a precise total session-memory bound, and they cannot guarantee delivery when transport I/O cannot progress. WSS always validates the certificate chain and server identity; using an IPv6 literal does not disable TLS verification.

Never log bearer tokens or assume queue admission is a business acknowledgement. For exact signatures, see the [API docs](https://docs.rs/qubit-websocket).

## Further reading

- [README](../README.md) · [中文 README](../README.zh_CN.md)
- [Architecture](architecture.md) · [架构说明](architecture.zh_CN.md)
- [API documentation](https://docs.rs/qubit-websocket)
- [Runnable WS example](../examples/ws_client.rs) · [runnable WSS example](../examples/wss_client.rs)
- [中文用户指南](user_guide.zh_CN.md)
