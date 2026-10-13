# 用户指南

本文面向 Rust 服务开发者，介绍如何使用 `qubit-websocket` 0.1 消费已有 WebSocket 服务推送的事件。贯穿场景是后台 worker 订阅任务进度，在业务处理中消费事件，并随服务停机结束连接。库负责传输、协议 I/O、有界队列和会话终态；认证策略、事件解码、持久化、业务确认以及断线恢复由应用负责。

可以先按[中文 README 的快速开始](../README.zh_CN.md#快速开始)跑通最短路径，再阅读本指南中的集成选择。服务端需要预先运行；仓库里的 [`ws_client`](../examples/ws_client.rs) 和 [`wss_client`](../examples/wss_client.rs) 是客户端示例，不会启动服务端。

## 为 worker 准备依赖

除了客户端 crate，构造请求头和 TLS 根证书时还需要将相应类型作为直接依赖。项目要求 Rust 1.94 或更新版本：

```toml
[dependencies]
qubit-websocket = "0.1"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread", "time"] }
http = "1.5"
rustls = "0.23"
```

下面的代码放在负责上游订阅的 worker 模块中。端点必须接受 `subscribe:job-42`；实际服务应替换为它定义的订阅格式。连接成功表示 HTTP 升级状态码为 101，并可查询协商出的子协议；握手失败则在创建会话前以 `WsError` 返回。

```rust,no_run
use http::header::AUTHORIZATION;
use http::HeaderMap;
use qubit_websocket::{WsClient, WsMessage};

async fn subscribe(token: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
    let client = WsClient::builder()
        .headers(headers)
        .subprotocols(vec!["job-events.v1".to_owned()])
        .build()?;
    let mut session = client.connect("wss://events.example.com/ws").await?;
    println!("升级状态码：{}", session.response_status_code());
    println!("服务端选用的子协议：{:?}", session.selected_subprotocol());
    session
        .send(WsMessage::Text("subscribe:job-42".to_owned()))
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    // 调用下方 receive_events，完成同一个 worker 工作流。
    receive_events(&mut session).await?;
    Ok(())
}
```

令牌应当按机密信息处理，不要写入日志，也不要拼进 URL。`connect_with(url, headers)` 可为单次连接提供请求头；同名请求头会覆盖 `WsClientBuilder::headers` 设置的默认值。凭据无效时服务端可能返回握手状态错误。子协议按给定顺序发送；连接后应检查 `selected_subprotocol()`，并按服务约定决定是否接受未选择子协议的连接。

## 选择 WSS 信任策略

默认的 `WsTrustRoots::Default` 使用标准 WebPKI 根证书，适用于常见的公有 CA 服务。若端点由私有 CA 签发，应用应先加载并校验其证书，再将对应的 `rustls::RootCertStore` 交给 Builder：

| 策略 | Builder 调用 | 信任范围 |
| --- | --- | --- |
| Default | 不调用证书方法 | 标准 WebPKI 根证书。 |
| Additional | `.root_certificates(roots)` | 标准根证书加上应用提供的根。 |
| Exclusive | `.exclusive_root_certificates(roots)` | 仅信任应用提供的根。 |

下面是上一节客户端初始化的 Builder 片段。`roots` 必须包含端点证书链对应的 CA；证书如何分发和载入属于应用配置。`Additional` 允许标准根或自定义根验证对端；`Exclusive` 遇到不在所给根证书链上的服务时返回 `WsErrorKind::Tls`，不会降级为明文连接。

```rust,no_run
use qubit_websocket::WsClient;
use rustls::RootCertStore;

fn client_with_private_ca(roots: RootCertStore) -> Result<WsClient, qubit_websocket::WsError> {
    WsClient::builder()
        .exclusive_root_certificates(roots)
        .build()
}
```

如果私有 CA 与公有 CA 服务都应继续连接，改用 `.root_certificates(roots)`。两种模式会影响证书轮换期间可接受的证书链，部署时要明确选定策略。

## 消费事件

把下面的循环接在 worker 发送订阅消息之后。收到的 `WsMessage` 是交给应用解码器的事件；写入存储、更新任务状态和决定是否确认，都属于业务逻辑。`receive()` 返回消息仅说明它已从有界入站队列取出。

```rust,no_run
use qubit_websocket::{WsMessage, WsTermination};

async fn receive_events(
    session: &mut qubit_websocket::WsSession,
) -> Result<(), qubit_websocket::WsError> {
    while let Some(event) = session.receive().await {
        match event {
            Ok(WsMessage::Text(text)) => {
                println!("任务事件：{text}"); // 在这里调用应用自己的解码和处理逻辑。
            }
            Ok(WsMessage::Binary(bytes)) => {
                println!("二进制事件：{} 字节", bytes.len());
            }
            Err(error) => {
                eprintln!("事件流以 {:?} 终止", error.kind());
                break;
            }
        }
    }

    match session.termination() {
        Some(WsTermination::Normal { code }) => println!("正常关闭：{code:?}"),
        Some(WsTermination::Error { kind }) => eprintln!("终态错误：{kind:?}"),
        None => eprintln!("会话还没有终态"),
    }
    session.close().await
}
```

正常关闭时，`receive()` 返回 `None`，`termination()` 为 `Normal`。错误终止会先通过一次 `Some(Err(error))` 交付给调用方，之后 `receive()` 返回 `None`，快照记录 `Error { kind }`。`close()` 发起有时限的关闭握手，可以重复调用；若会话已有终态错误或关闭超时，它会返回对应错误，worker 停机流程应处理该结果。

## 多个任务共享发送端

一个 worker 可以让多个生产者通过同一会话发送命令。连接成功后调用 `session.sender()`，再克隆 `WsSender` 分给各任务；所有副本共用同一组出站消息数和字节数预算。以下代码放在 worker 的生产者启动处，需要 Tokio runtime。发送成功只是入队；重要命令仍需通过事件流里的业务确认来关联结果。

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
    println!("入队结果：{first_result:?}、{second_result:?}");
    Ok(())
}
```

生产者不能等待时可用 `try_send`：`Backpressure` 表示当前消息数或字节预算已满；`MessageTooLarge` 表示单条消息超过出站字节上限；`Closed` 表示会话不再接受消息。`send` 会等待容量；若等待入队的 future 被取消，临时容量预留会释放。无论哪种方法都不承诺数据已通过网络送达。

## 设置资源预算并处理背压

`WsSessionLimits::default()` 的限制为：单个入站帧 1 MiB、重组后的入站消息 4 MiB、每个方向队列最多 64 条消息和 4 MiB payload、无进展等待 30 秒、关闭握手等待 5 秒。应根据事件大小和 worker 可用内存设定预算。帧上限不能大于消息上限，消息上限不能大于 `inbound_bytes`；任意值为零或组合不合法时，`build()` 返回 `WsErrorKind::InvalidConfig`。

下面代码放在 worker 创建客户端的位置，缩小了队列容量，目的是展示配置关系；实际数值应根据服务测量结果调整。出站队列满时，`try_send` 会返回 `Backpressure`；`send` 则等待空间释放或会话关闭。

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

应用通过 `receive` 取走消息后才释放对应入站容量。若业务处理跟不上，入站队列会填满，驱动会等待应用继续消费。出站消息在排队或写出期间占用预算。遇到背压时，应用可以等待、合并可替换的状态更新，或向上游报告过载；不要反复循环调用 `try_send`，它不会等待容量变化。

## 停机、取消与恢复

优雅停机时先停止创建新的生产者，再调用 `session.close().await`。关闭握手不会超过配置的 `close_timeout`；超时会终止驱动，并以 `WsErrorKind::Timeout` 表示。只有 `termination()` 为 `WsTermination::Normal` 才是正常终止；传输或协议故障不会触发自动重连。

丢弃等待中的 `receive()` future 只会结束当前调用方的等待，不会关闭会话。取消尚未完成的 `send()` 会放弃这次入队，并释放临时预算。丢弃会话或取消其所属任务，并不会让库自动重连、重放队列或恢复业务状态。服务重启后是否重新订阅、如何补齐遗漏事件、怎样避免重复命令产生副作用，应由应用决定；例如可在业务协议中使用幂等键或序列游标。

## 错误与排障

`WsError::kind()` 返回稳定的错误类别；握手错误还可查询 `status_code()` 和 `handshake_failure()`。常见类别有 `InvalidUrl`、`Connect`、`Tls`、`Handshake`、`Protocol`、`FrameTooLarge`、`MessageTooLarge`、`Timeout`、`Cancelled`、`Closed` 和 `InvalidConfig`。发送队列单独返回 `WsSendError`：`Backpressure`、`MessageTooLarge` 或 `Closed`。

| 现象 | 检查方向 |
| --- | --- |
| WSS 返回 `Tls` | 核对 URL 主机名、证书有效期，以及所选根证书策略是否含签发 CA。 |
| 握手状态错误 | 检查凭据和服务路由；可查询 `status_code()`，切勿记录认证头。 |
| 服务端未选择子协议 | 对照客户端提议值和服务配置，并检查 `selected_subprotocol()`。 |
| `FrameTooLarge` 或 `MessageTooLarge` | 对照服务端事件形态和两个入站上限；提高限额也会提高潜在内存占用。 |
| `Backpressure` | 检查生产速度以及出站消息数、字节数预算，明确等待、延后或过载处理策略。 |
| `Timeout` | 检查 DNS、连接和握手进度、对端读写情况，以及无进展或关闭期限。 |

不要记录 bearer token，也不要把入队成功当作业务确认。精确签名请查阅 [API 文档](https://docs.rs/qubit-websocket)。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [English README](../README.md)
- [Rust API 文档](https://docs.rs/qubit-websocket)
- [可运行 WS 示例](../examples/ws_client.rs) · [可运行 WSS 示例](../examples/wss_client.rs)
- [English user guide](user_guide.md)
