# Architecture

This page describes the current `qubit-websocket` 0.1 architecture. For implementation rationale, see the [implementation design](design-20261014T021031598Z-49c7bedd.md); the current behavior is defined by source and API documentation. The [Chinese version](architecture.zh_CN.md) is also available.

## Layers and ownership

The client and handshake layer validates the endpoint and request options, establishes the transport, and completes the HTTP/1.1 WebSocket upgrade. A successful handshake creates a `WsSession`; a failure is returned as `WsError` before a session exists. TLS trust selection and request headers are configured by the application through the client builder.

The session API gives the application a receive handle and a sender. Inbound and outbound queues are bounded by message count and payload bytes. `send()` waits for capacity and `try_send()` reports capacity immediately; both report `WsSendError` if the queue cannot accept the message. Successful enqueue is not a delivery or business acknowledgement.

The session driver owns protocol I/O and coordinates reader and writer progress. It handles Ping, Pong, and Close frames, while exposing text and binary messages to the application. Close coordination stops accepting new application messages, attempts the protocol reply, and waits within the configured close deadline. The driver records one terminal outcome for callers to inspect through `termination()`.

## Result boundaries

| Result | Meaning | Application responsibility |
| --- | --- | --- |
| `WsSendError` | An outbound message was not admitted to the session queue. | Decide whether to wait, defer, reject, or stop producing. |
| `WsTermination::Closed { code }` | The WebSocket close handshake completed; `code` preserves the peer's close code when present. | Interpret the code under the application protocol. A `1009` close is not business success. |
| `WsTermination::Error { kind }` | The session ended because close coordination or another terminal operation failed. | Diagnose and choose recovery; the library does not reconnect or replay work. |
| `WsError` from `receive()` or `close()` | A session-level operation reported a failure. A terminal receive error is delivered once. | Handle the error and account for shutdown results. |

`WsErrorKind::Transport` covers connection establishment and runtime transport I/O. `WsErrorKind::Internal` covers driver task failures. Queue admission errors remain `WsSendError` and do not become a session terminal error solely because a send call failed.

## Shutdown flow

For graceful shutdown, the service stops producers, signals the worker, and waits for it. The worker stops consuming, calls `close().await`, and checks the terminal state. The close handshake is bounded by the configured timeout; if the peer does not complete it, shutdown returns a timeout or an already recorded terminal error. Dropping a session is not a substitute when the caller needs to observe shutdown completion.

The application owns event decoding, persistence, business acknowledgements, and recovery after disconnect. In particular, a completed protocol close does not prove that queued commands were processed or consumed events were persisted.

## Further reading

- [User guide](user_guide.md) · [中文用户指南](user_guide.zh_CN.md)
- [README](../README.md) · [中文 README](../README.zh_CN.md)
- [Rust API documentation](https://docs.rs/qubit-websocket)
- [Implementation design](design-20261014T021031598Z-49c7bedd.md) · [historical design record](design.zh_CN.md)
