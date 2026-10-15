# Architecture

This page describes the current `qubit-websocket` 0.1 architecture. The [Chinese version](architecture.zh_CN.md) is also available. The [session ownership, handshake classification, and source organization design](design-20261015T120440483Z-4bb31961.md) is **implemented**. The [close-coordination design](design-20261014T021031598Z-49c7bedd.md) is an implemented historical record; the [headers and backpressure design](design-20261014T043823504Z-0ceb2c88.md) records this completed repair, and source/API documentation remains authoritative for current behavior.

## Layers and ownership

The client and handshake layer validates the endpoint and request options, establishes the transport, and completes the HTTP/1.1 WebSocket upgrade. A successful handshake creates a `WsSession`; a failure is returned as `WsError` before a session exists. TLS trust selection and request headers are configured by the application through the client builder.

The session API gives the application a receive handle and a sender. `WsSession` owns session liveness; `WsSender` clones share outbound capacity but do not keep the session alive. Dropping the session requests bounded local shutdown and closes both queues, so surviving senders return `WsSendError::Closed`. Messages queued but not yet written may be discarded. Inbound and outbound queues are bounded by message count and payload bytes. `send()` waits for capacity and `try_send()` reports capacity immediately; both report `WsSendError` if the queue cannot accept the message. Successful enqueue is not a delivery or business acknowledgement.

The session driver owns protocol I/O and coordinates reader and writer progress. It handles Ping, Pong, and Close frames, while exposing text and binary messages to the application. Close coordination stops accepting new application messages, attempts the protocol reply, and waits within the configured close deadline. The driver records one terminal outcome for callers to inspect through `termination()`.

When the inbound queue is full, the driver can hold at most the message currently waiting for admission while it continues polling control frames. Another business message triggers the slow-consumer path immediately; if none arrives, `no_progress_timeout` bounds the wait. The driver attempts a `1013` Close, but a failed write cannot guarantee delivery to the peer. The resulting `Timeout` is a session failure classification, not a business-processing result or peer acknowledgement.

## Result boundaries

| Result | Meaning | Application responsibility |
| --- | --- | --- |
| `WsSendError` | An outbound message was not admitted to the session queue. | Decide whether to wait, defer, reject, or stop producing. |
| `WsTermination::Closed { code }` | The WebSocket close handshake completed; `code` preserves the peer's close code when present. | Interpret the code under the application protocol. A `1009` close is not business success. |
| `WsTermination::Error { kind }` | The session ended because close coordination or another terminal operation failed. | Diagnose and choose recovery; the library does not reconnect or replay work. |
| `WsError` from `receive()` or `close()` | A session-level operation reported a failure. A terminal receive error is delivered once. | Handle the error and account for shutdown results. |

`WsErrorKind::Transport` covers connection establishment, ordinary I/O failures during HTTP upgrade, and runtime transport I/O. Structured HTTP rejection and WebSocket upgrade validation failures remain `WsErrorKind::Handshake`; TLS errors retain `Tls`. `WsErrorKind::Internal` covers driver task failures. Queue admission errors remain `WsSendError` and do not become a session terminal error solely because a send call failed.

## Shutdown flow

For graceful shutdown, the service stops producers and waits for them to finish, signals the worker, then waits for the worker to call `close().await` and inspect the terminal state before dropping its session. The close handshake is bounded by the configured timeout; if the peer does not complete it, shutdown returns a timeout or an already recorded terminal error. Drop requests local bounded shutdown, but does not return a close result or establish peer confirmation; use explicit `close().await` when the caller needs to observe the outcome.

The application owns event decoding, persistence, business acknowledgements, and recovery after disconnect. In particular, a completed protocol close does not prove that queued commands were processed or consumed events were persisted.

## Further reading

- [User guide](user_guide.md) · [中文用户指南](user_guide.zh_CN.md)
- [README](../README.md) · [中文 README](../README.zh_CN.md)
- [Rust API documentation](https://docs.rs/qubit-websocket)
- [Session ownership, handshake classification, and source organization design (implemented)](design-20261015T120440483Z-4bb31961.md)
- [Implemented headers and backpressure design](design-20261014T043823504Z-0ceb2c88.md) · [implemented close-coordination history](design-20261014T021031598Z-49c7bedd.md) · [earlier design notes](design.zh_CN.md)
