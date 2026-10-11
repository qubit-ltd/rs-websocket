# Qubit WebSocket

[![Rust CI](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-websocket/coverage-badge.json)](https://qubit-ltd.github.io/rs-websocket/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-websocket.svg?color=blue)](https://crates.io/crates/qubit-websocket)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-websocket` 计划为 Rust 服务提供 WebSocket 客户端基础设施，让应用能以明确的资源限额和生命周期管理长期双向连接。目前仓库只有项目骨架和设计文档，尚未实现客户端 API，也未发布 crate。

一个典型场景是：Rust 服务连接 `qubit-web` 的 WebSocket 端点，发送命令、接收进度，并在宿主服务停止时结束会话。计划中的公共库负责连接和会话管理；命令格式及业务消息的交付保证仍由应用定义。

## 范围与状态

首版计划支持 `ws://`、`wss://`、握手校验、消息限额、发送背压、取消和有界关闭。后续若增加重连，也必须由调用方显式启用，且不会自动重放业务消息。上述运行时能力目前均未实现。

[产品需求文档](doc/prd.zh_CN.md)说明需求和验收标准；[详细设计文档](doc/design.zh_CN.md)说明拟定的 API、协议处理、失败语义和测试策略。

## 测试

```bash
cargo test --locked
cargo doc --no-deps --locked
./.infra/bin/ci-check.sh
```

这些命令目前仅验证项目骨架。加入可执行的客户端代码和行为测试后，再启用覆盖率收集。

## 许可证

Copyright (c) 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅 [LICENSE](LICENSE)。

## 贡献

欢迎贡献。请同步维护公共 API 文档与测试，在提交 Pull Request 前运行 `./.infra/bin/align-ci.sh` 格式化代码，并运行 `./.infra/bin/ci-check.sh` 对齐 CI 要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-websocket](https://github.com/qubit-ltd/rs-websocket)
