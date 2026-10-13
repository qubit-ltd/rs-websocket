# Qubit WebSocket

[![Rust CI](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-websocket/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-websocket/coverage-badge.json)](https://qubit-ltd.github.io/rs-websocket/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-websocket.svg?color=blue)](https://crates.io/crates/qubit-websocket)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-websocket` 为 Rust 服务提供带资源上限的 Tokio WebSocket 客户端。后台 worker 可以订阅已有的事件端点、读取处理进度，并在服务退出时结束会话。库管理连接和内存边界；事件格式、业务确认及断线后的处理策略仍由应用决定。

## 安装

在 Rust 1.94 或更新版本的项目中加入：

```toml
[dependencies]
qubit-websocket = "0.1"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

下面的入门流程要求 WS 或 WSS 服务端已经启动，并能识别示例中的订阅内容；本库不会启动服务端。

## 快速开始

这段异步函数完整展示连接、发送订阅、持续接收直到对端关闭或出现错误、检查终态快照，以及发起有时限的关闭握手。`send` 返回成功只代表消息进入出站队列，不代表已经送达服务端或完成业务处理。

```rust,no_run
use qubit_websocket::{WsClient, WsMessage, WsTermination};

async fn consume_events() -> Result<(), Box<dyn std::error::Error>> {
    let client = WsClient::builder().build()?;
    let mut session = client.connect("wss://events.example.com/ws").await?;
    session
        .send(WsMessage::Text("subscribe:job-42".to_owned()))
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;

    while let Some(message) = session.receive().await {
        match message {
            Ok(WsMessage::Text(event)) => println!("event: {event}"),
            Ok(WsMessage::Binary(payload)) => println!("binary event: {} bytes", payload.len()),
            Err(error) => eprintln!("session ended: {:?}", error.kind()),
        }
    }

    match session.termination() {
        Some(WsTermination::Normal { code }) => println!("对端正常关闭：{code:?}"),
        Some(WsTermination::Error { kind }) => eprintln!("会话终止错误：{kind:?}"),
        None => eprintln!("会话尚未进入终态"),
    }
    session.close().await?;
    Ok(())
}
```

端点地址、订阅格式和事件含义由你的服务定义。要运行命令行示例，可查看 [`examples/ws_client.rs`](examples/ws_client.rs) 与 [`examples/wss_client.rs`](examples/wss_client.rs)；运行前同样要准备好服务端。

## 能力与边界

- 通过 HTTP/1.1 按 RFC 6455 连接 `ws://` 和 `wss://`。
- TLS 可使用标准 WebPKI 根证书、在其上追加证书，或只信任显式指定的证书。
- 支持默认及单次连接请求头，并可提议 WebSocket 子协议。
- 对入站帧、重组后的消息，以及双向队列分别设置消息数量和 payload 字节上限。
- 应用接收文本或二进制消息；会话驱动负责处理 Ping、Pong、Close 和终态错误。
- `try_send` 在容量不足时立即给出结果；`send` 等待可用容量。两者都不表示对端已收到或业务已经处理。

首版不包含服务端、浏览器支持、代理、自动重连、消息重放、压缩，也不支持 HTTP/2 或 HTTP/3 WebSocket。连接中断后是否重试、如何避免业务重复执行，需要应用自行决定。

## 从首版设计迁移

| 旧设计名称 | 当前 API | 变化 |
| --- | --- | --- |
| `max_frame_bytes` | `max_inbound_frame_bytes` | 明确限定单个入站帧的 payload。 |
| `max_message_bytes` | `max_inbound_message_bytes` | 明确限定重组后的入站消息。 |
| `root_certificates: Option<RootCertStore>` | `trust_roots: WsTrustRoots` | 用 `Default`、`Additional` 或 `Exclusive` 表达信任策略。 |

现在调用 `WsClientBuilder::root_certificates(roots)` 会把证书追加到默认 WebPKI 根。若连接必须只信任传入的根证书，请改用 `WsClientBuilder::exclusive_root_certificates(roots)`。

## 延伸阅读

- [English user guide](doc/user_guide.md) · [中文用户指南](doc/user_guide.zh_CN.md)
- [Rust API 文档](https://docs.rs/qubit-websocket)
- [English README](README.md)
- [产品需求](doc/prd.zh_CN.md) · [设计记录](doc/design.zh_CN.md)

## 限制

首版不提供 WebSocket 服务端、浏览器支持、代理、自动重连、消息重放、压缩，也不支持 HTTP/2 或 HTTP/3 WebSocket。业务确认、重试和断线后的恢复由应用负责。

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
