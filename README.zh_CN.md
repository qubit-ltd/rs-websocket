# Qubit WebSocket

[![Rust CI](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-websocket/coverage-badge.json)](https://qubit-ltd.github.io/rs-websocket/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-websocket.svg?color=blue)](https://crates.io/crates/qubit-websocket)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-websocket` 是面向 Rust 服务的有界 Tokio WebSocket 客户端。例如，
worker 可以连接 `qubit-web` 端点、发送命令、接收进度消息，并在服务停止时
关闭会话。库负责传输和资源限额；消息格式与业务交付保证由应用定义。

## 功能

- 使用 RFC 6455，通过 HTTP/1.1 连接 `ws://` 和 `wss://` 端点。
- 默认使用系统根证书验证 TLS 证书和主机名，也可提供显式信任根。
- 设置默认或单次连接请求头，并提议 WebSocket 子协议。
- 按消息数和 payload 字节数限制帧、重组消息以及入站、出站队列。
- 接收文本和二进制消息；驱动处理 Ping、Pong、Close 和终态错误。
- 使用 `try_send` 立即报告背压，或使用 `send` 等待队列容量。入队成功不代表
  网络送达或业务处理完成。

## 示例

`ws_client` 和 `wss_client` 示例将服务端 URL 作为第一个参数，并从
`WS_BEARER_TOKEN` 读取可选 bearer token；示例不会打印 token。

```bash
cargo run --example ws_client -- ws://127.0.0.1:3001/ws
cargo run --example wss_client -- wss://localhost:3001/ws
```

crate API 与可运行示例见 [Rust API 文档](https://docs.rs/qubit-websocket)。
详细行为与限额见[产品需求](doc/prd.zh_CN.md)和[设计文档](doc/design.zh_CN.md)。

## 限制

首版不提供 WebSocket 服务端、浏览器支持、代理、自动重连、消息重放、压缩，
也不支持 HTTP/2 或 HTTP/3 WebSocket。业务确认、重试和断线后的恢复由应用负责。

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test --locked

# 使用项目声明的全部 feature 运行测试
cargo test --all-features --locked

# 运行项目 CI 检查
./.infra/bin/ci-check.sh

# 检查代码覆盖率
./.infra/bin/coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./.infra/bin/align-ci.sh` 格式化代码，运行
`./.infra/bin/ci-check.sh` 对齐 CI 要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-websocket](https://github.com/qubit-ltd/rs-websocket)
