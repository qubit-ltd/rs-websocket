# Qubit WebSocket

[![Rust CI](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-websocket/coverage-badge.json)](https://qubit-ltd.github.io/rs-websocket/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-websocket.svg?color=blue)](https://crates.io/crates/qubit-websocket)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-websocket` is a bounded Tokio WebSocket client for Rust services. For
example, a worker can connect to a `qubit-web` endpoint, send a command, receive
progress messages, and close the session during shutdown. The library manages
the transport and resource limits; the application defines message formats
and business delivery guarantees.

## Features

- Connect to `ws://` and `wss://` endpoints using RFC 6455 over HTTP/1.1.
- Validate TLS certificates and hostnames with system roots by default, or
  provide explicit trust roots.
- Set default or per-connection request headers and offer WebSocket
  subprotocols.
- Bound frames, reassembled messages, and inbound and outbound queues by both
  message count and payload bytes.
- Receive text and binary messages while the driver handles Ping, Pong, Close,
  and terminal errors.
- Use `try_send` for immediate backpressure or `send` to wait for queue
  capacity. Successful enqueue does not mean network delivery or processing.

## Example

The `ws_client` and `wss_client` examples accept a server URL as their first
argument. They read an optional bearer token from `WS_BEARER_TOKEN` and do not
print it.

```bash
cargo run --example ws_client -- ws://127.0.0.1:3001/ws
cargo run --example wss_client -- wss://localhost:3001/ws
```

The crate API and runnable examples are documented in [Rust API docs](https://docs.rs/qubit-websocket).
See the [product requirements](doc/prd.zh_CN.md) and
[design document](doc/design.zh_CN.md) for detailed behavior and limits.

## Limits

The first release does not provide a WebSocket server, browser support,
proxies, automatic reconnection, message replay, compression, or HTTP/2 and
HTTP/3 WebSockets. The application owns business acknowledgements, retries,
and recovery after a disconnected session.

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
