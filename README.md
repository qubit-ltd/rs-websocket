# Qubit WebSocket

[![Rust CI](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-websocket/coverage-badge.json)](https://qubit-ltd.github.io/rs-websocket/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-websocket.svg?color=blue)](https://crates.io/crates/qubit-websocket)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-websocket` is a planned WebSocket client library for Rust services that
need a long-lived, bidirectional connection with explicit resource and
lifecycle policies. This repository currently contains the project scaffold
and design documents; it does not yet provide a client API or a published
crate.

For example, a Rust service may connect to a `qubit-web` WebSocket endpoint,
send commands, receive progress messages, and stop the session when its host
shuts down. The proposed library would provide connection and session
management; the application would still define its command format and delivery
guarantees.

## Scope and status

The planned first release covers `ws://` and `wss://` connections, handshake
validation, bounded messages, explicit send backpressure, cancellation, and
graceful close. Reconnection, when added, will be opt-in and will not replay
application messages automatically. None of these runtime capabilities is
implemented in the current scaffold.

The [Chinese PRD](doc/prd.zh_CN.md) defines the requirements and acceptance
criteria. The [Chinese detailed design](doc/design.zh_CN.md) describes the
proposed API, protocol handling, failure behavior, and test strategy.

## Testing

```bash
cargo test --locked
cargo doc --no-deps --locked
./.infra/bin/ci-check.sh
```

These commands currently validate the scaffold only. Coverage collection will
be enabled when executable client code and behavior tests are added.

## License

Copyright (c) 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Keep public API documentation and tests current,
run `./.infra/bin/align-ci.sh` to format code, and run
`./.infra/bin/ci-check.sh` before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-websocket](https://github.com/qubit-ltd/rs-websocket)
