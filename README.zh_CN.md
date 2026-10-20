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
tokio = { version = "1.53", features = ["macros", "rt-multi-thread", "sync"] }
```

下面的入门流程要求 WS 或 WSS 服务端已经启动，并能识别示例中的订阅内容；本库不会启动服务端。

配置适配是可选功能。应用需要从 `qubit-config` 构造 `Config` 时，可启用该 feature；`qubit-config` 也应由应用直接依赖：

```toml
[dependencies]
qubit-websocket = { version = "0.1", features = ["config"] }
qubit-config = "0.14"
```

支持的键及适配边界见[用户指南中的配置章节](doc/user_guide.zh_CN.md#从配置读取客户端设置)。

## 快速开始

这个 worker 展示连接、发送订阅、持续接收直到对端关闭、出现错误或应用发出停机信号，再检查终态快照并执行有时限的关闭握手。`send` 返回成功只代表消息进入出站队列，不代表已经送达服务端或完成业务处理。

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
            Some(Ok(WsMessage::Text(event))) => println!("任务事件：{event}"),
            Some(Ok(WsMessage::Binary(bytes))) => println!("二进制事件：{} 字节", bytes.len()),
            Some(Err(error)) => { eprintln!("会话终止：{:?}", error.kind()); break; }
            None => break,
        }
    }

    let close_result = session.close().await;
    println!("会话终态：{:?}", session.termination());
    close_result?;
    Ok(())
}
```

服务持有一次性通道的发送端和 worker 任务句柄。将上面的 worker 函数，以及一个在生产者停止且服务开始停机后完成的 future，传给下方独立 helper：发送端由服务持有，接收端则交给 worker。

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

    // 此 future 在服务开始停机且生产者停止后完成。
    stop_producers_and_wait.await;
    let _ = shutdown_tx.send(());
    worker.await??;
    Ok(())
}
```

如果发送端未发送就被释放，接收端也会结束，worker 会走相同的关闭路径。端点地址和订阅格式由服务定义；`send` 只表示消息进入队列，不是业务确认。`close()` 握手受会话配置的超时限制。上面的代码在事件循环退出后先执行关闭，再打印终态；若 `close()` 返回已有终态错误或 `Timeout`，worker 结果会将停机失败交回服务处理。

将 worker 函数传给 helper 即可启动任务。传入的停机 future 应在服务停止生产者并等待其结束后完成：

```rust,no_run
async fn serve() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_worker(consume_events, async {
        // 停止生产者并等待它们结束。
    })
    .await
}
```

端点地址、订阅格式和事件含义由你的服务定义。要运行命令行示例，可查看 [`examples/ws_client.rs`](examples/ws_client.rs) 与 [`examples/wss_client.rs`](examples/wss_client.rs)；运行前同样要准备好服务端。

`termination()` 返回 `Closed { code }` 表示 WebSocket 关闭协议已完成，`code` 保留对端提供的状态码，具体含义由应用按服务协议判断。例如 `1009` 表示对端以消息过大为由关闭连接，不代表订阅成功；`Error { kind }` 则表示关闭或其他终止过程发生故障。快速开始中的打印只用于诊断展示，不代表数据已持久化或业务已确认。

释放 `WsSession` 会请求本地有界关闭，即使仍有克隆的 `WsSender`，它们也不能继续接纳消息；已入队但尚未写出的消息可能被丢弃。Drop 不会等待或报告对端关闭结果；需要检查结果时请显式调用并等待 `session.close().await`。升级期间发生的普通网络 I/O 故障归为 `WsErrorKind::Transport`；HTTP 拒绝或升级校验失败仍归为 `Handshake`。

`send()` 在消息无法进入队列时返回 `WsSendError`。调用处应按服务策略处理 `Backpressure`、`MessageTooLarge` 和 `Closed`；会话已关闭时，即使消息超限也返回 `Closed`，只有会话仍开放时才会因消息超限返回 `MessageTooLarge`。这些错误与会话终态中的 `WsError` 分开，也不表示对端已经响应。上面的 `?` 会把原始发送错误交给 worker 的调用方处理。

## 能力与边界

- 通过 HTTP/1.1 按 RFC 6455 连接 `ws://` 和 `wss://`。
- IPv6 URL 字面量要用方括号括起，例如 `ws://[::1]:8080/events`；TCP 目标解析会校验括号内的地址。带作用域标识的 IPv6 地址不受支持，WSS 仍会校验证书链和服务端身份。
- TLS 可使用标准 WebPKI 根证书、在其上追加证书，或只信任显式指定的证书。
- 支持默认及单次连接请求头，并可提议 WebSocket 子协议。一个 `HeaderMap` 可以为同名头保存多个值；单次连接请求头会按名称整组替换默认值，同名默认值不会与连接级值混合，其他默认头仍保留。协议保留头由库管理，调用方不能覆盖。
- 对入站帧、重组后的消息，以及双向队列分别设置消息数量和 payload 字节上限。
- 出站 payload 队列预算与 WebSocket 写缓冲上限是两项独立限制；写缓冲会额外增加 14 字节以容纳最大数据帧头，并增加 131 字节以容纳最大客户端控制帧。最小有效 payload 预算下仍可处理 Ping/Pong/Close；这不代表会话总内存上限，也不保证帧一定送达。
- 应用接收文本或二进制消息；会话驱动负责处理 Ping、Pong、Close 和终态错误。入站队列已满且容量恢复前又收到业务消息时，会话以 `WsErrorKind::InboundBackpressure` 结束；若没有额外消息，则 `no_progress_timeout` 到期后以 `WsErrorKind::Timeout` 结束。两条路径都会尝试发送 `1013` Close，但写入失败时无法保证对端收到。这些都是会话故障，不是业务确认；`WsSendError::Backpressure` 则表示出站消息未能入队。
- `try_send` 在容量不足时立即给出结果；`send` 等待可用容量。两者都不表示对端已收到或业务已经处理。

客户端不提供 WebSocket 服务端、浏览器支持、代理、自动重连、消息重放、压缩，也不支持 HTTP/2 或 HTTP/3 WebSocket。业务确认、重试决策和断线后的恢复由应用负责，包括避免重复业务操作。

不要把所有 `Closed` 都视为业务成功：`1000`、`1009` 或未提供状态码分别意味着什么，应由应用协议决定，必要时还要核对服务端状态。`close()` 返回 `Ok(())` 只说明协议收尾完成，不证明订阅成功或数据已可靠处理。

## 延伸阅读

- [English user guide](doc/user_guide.md) · [中文用户指南](doc/user_guide.zh_CN.md)
- [Architecture](doc/architecture.md) · [架构说明](doc/architecture.zh_CN.md)
- [Rust API 文档](https://docs.rs/qubit-websocket)
- [English README](README.md)
- [产品需求](doc/prd.zh_CN.md)

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

Copyright (c) 2025 - 2026. Haixing Hu.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./.infra/bin/align-ci.sh` 格式化代码，运行
`./.infra/bin/ci-check.sh` 对齐 CI 要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-websocket](https://github.com/qubit-ltd/rs-websocket)
