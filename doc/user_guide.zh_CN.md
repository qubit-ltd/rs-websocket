# 用户指南

本文面向 Rust 服务开发者，介绍如何使用 `qubit-websocket` 0.1 消费已有 WebSocket 服务推送的事件。贯穿场景是后台 worker 订阅任务进度，在业务处理中消费事件，并随服务停机结束连接。库负责传输、协议 I/O、有界队列和会话终态；认证策略、事件解码、持久化、业务确认以及断线恢复由应用负责。

可以先按[中文 README 的快速开始](../README.zh_CN.md#快速开始)跑通最短路径，再阅读本指南中的集成选择。服务端需要预先运行；仓库里的 [`ws_client`](../examples/ws_client.rs) 和 [`wss_client`](../examples/wss_client.rs) 是客户端示例，不会启动服务端。

## 为 worker 准备依赖

除了客户端 crate，构造请求头和 TLS 根证书时还需要将相应类型作为直接依赖。项目要求 Rust 1.94 或更新版本：

```toml
[dependencies]
qubit-websocket = "0.1"
tokio = { version = "1.53", features = ["macros", "rt-multi-thread", "sync", "time"] }
http = "1.5"
rustls = "0.23"
```

下面的代码放在负责上游订阅的 worker 模块中。端点必须接受 `subscribe:job-42`；实际服务应替换为它定义的订阅格式。连接成功表示 HTTP 升级状态码为 101，并可查询协商出的子协议；握手失败则在创建会话前以 `WsError` 返回。

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
    println!("升级状态码：{}", session.response_status_code());
    println!("服务端选用的子协议：{:?}", session.selected_subprotocol());
    session
        .send(WsMessage::Text("subscribe:job-42".to_owned()))
        .await?;
    // 下方 receive_events 同时等待事件与停机信号。
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
    // 在这里解码并持久化事件，再发送应用层确认（如服务协议要求）。
    // 只有这些业务步骤全部成功后才返回 Ok(())。
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
                    eprintln!("应用无法处理该事件：{error}");
                    processing_failure = Some(error);
                    break;
                }
            }
            Some(Ok(WsMessage::Binary(bytes))) => println!("二进制事件：{} 字节", bytes.len()),
            Some(Err(error)) => {
                eprintln!("事件流以 {:?} 终止；请按服务现有恢复策略处理", error.kind());
                break;
            }
            None => break,
        }
    }
    let close_result = session.close().await;
    match session.termination() {
        Some(qubit_websocket::WsTermination::Closed { code: Some(1000) }) => {
            println!("关闭协议已完成；应用可按业务约定将订阅标记为完成");
        }
        Some(qubit_websocket::WsTermination::Closed { code: Some(1009) }) => {
            eprintln!("对端以消息过大为由拒绝；记录原因并停止本次订阅，调整请求或负载后再决定是否重试");
        }
        Some(qubit_websocket::WsTermination::Closed { code: None }) => {
            eprintln!("对端未提供关闭码；将结果标为未知，核对服务状态后再判断订阅是否完成");
        }
        Some(qubit_websocket::WsTermination::Closed { code }) => {
            eprintln!("关闭协议已完成，应用自定义状态码：{code:?}");
        }
        Some(qubit_websocket::WsTermination::Error { kind }) => eprintln!("终态错误：{kind:?}"),
        None => eprintln!("会话还没有终态"),
    }
    // close 成功只说明协议收尾完成，不代表业务已完成。
    close_result?;
    if let Some(error) = processing_failure {
        return Err(Box::new(error));
    }
    Ok(())
}
```

令牌应当按机密信息处理，不要写入日志，也不要拼进 URL。`connect_with(url, headers)` 可为单次连接提供请求头；同名请求头会覆盖 `WsClientBuilder::headers` 设置的默认值。一个 `HeaderMap` 可以为同名头保存多个值；后一个来源会用自己的整组值替换前一个来源的同名值。凭据无效时服务端可能返回握手状态错误。子协议按给定顺序发送；连接后应检查 `selected_subprotocol()`，并按服务约定决定是否接受未选择子协议的连接。

如果上面的 `send()` 无法把消息放入队列，worker 调用方会收到原始 `WsSendError`。应用应按服务策略处理 `Backpressure`、`MessageTooLarge` 或 `Closed`。这些队列错误与会话终态 `WsError` 分属不同边界；发送成功也只说明队列已接纳消息。

`handle_job_event(&str) -> Result<(), AppError>` 是应用自行实现的处理边界。可在其中解析并持久化事件；只有业务处理及所需的业务确认都成功后，才返回 `Ok(())`。本例在处理函数返回 `AppError` 时停止消费、关闭 WebSocket，再把应用错误返回给调用方。生产环境也可按业务选择有限重试、隔离事件或停止服务；不得在必要处理成功前确认事件。

## 从配置读取客户端设置

可选的 `config` feature 支持从 `qubit_config::Config` 子树创建 builder。应用构造 `Config` 时应直接依赖 `qubit-config`：

```toml
[dependencies]
qubit-websocket = { version = "0.1", features = ["config"] }
qubit-config = "0.14"
```

配置前缀由应用决定。`websocket` 只是示例，也可以使用 `service.websocket`；空前缀表示选择根配置。选中的配置范围至少要包含一个属性。

```rust,no_run
use http::HeaderMap;
use qubit_config::Config;
use qubit_websocket::{config_keys, WsClientBuilder};

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

稳定的相对键名集中定义在 `qubit_websocket::config_keys`：

| 常量 | 相对键名 | 值类型 |
| --- | --- | --- |
| `CONNECT_TIMEOUT_MS` | `connect_timeout_ms` | `u64`，毫秒 |
| `HANDSHAKE_TIMEOUT_MS` | `handshake_timeout_ms` | `u64`，毫秒 |
| `SUBPROTOCOLS` | `subprotocols` | `Vec<String>` |
| `MAX_INBOUND_FRAME_BYTES` | `limits.max_inbound_frame_bytes` | `usize`，字节 |
| `MAX_INBOUND_MESSAGE_BYTES` | `limits.max_inbound_message_bytes` | `usize`，字节 |
| `INBOUND_MESSAGES` | `limits.inbound_messages` | `usize`，条数 |
| `INBOUND_BYTES` | `limits.inbound_bytes` | `usize`，字节 |
| `OUTBOUND_MESSAGES` | `limits.outbound_messages` | `usize`，条数 |
| `OUTBOUND_BYTES` | `limits.outbound_bytes` | `usize`，字节 |
| `NO_PROGRESS_TIMEOUT_MS` | `limits.no_progress_timeout_ms` | `u64`，毫秒 |
| `CLOSE_TIMEOUT_MS` | `limits.close_timeout_ms` | `u64`，毫秒 |

省略的键保留 builder 默认值。路径无效、所选配置范围不存在或为空、出现未知属性、值转换失败时会返回带配置路径上下文的 `ConfigError`。显式零值或相互冲突的 limits 会保留到 `build()`，由现有校验返回 `WsErrorKind::InvalidConfig`。适配器不执行网络 I/O，也不读取请求头、TLS 信任根或 URL：请求头和信任根通过 builder 方法设置，每次连接的 URL 仍传给 `connect`。

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

worker 示例中的 `receive_events` 会在发送订阅后处理事件流。收到的 `WsMessage` 是交给应用解码器的事件；写入存储、更新任务状态和决定是否确认，都属于业务逻辑。示例调用应用自行实现的 `handle_job_event`；应将函数体替换为服务实际的解码与持久化逻辑，并且只有必要业务处理和业务确认全部完成后才能返回 `Ok(())`。`receive()` 返回消息仅说明它已从有界入站队列取出。

关闭握手完成后，`receive()` 返回 `None`，`termination()` 为 `Closed { code }`，其中状态码保留对端原值，业务含义由应用判断。本例可将 `Some(1000)` 作为订阅完成条件；`Some(1009)` 应记录大小拒绝、停止本次订阅，并调整请求或负载后再决定是否重试；`None` 表示结果尚不能判定，应先核对服务状态。事件处理失败时，本例停止消费、执行关闭，再把 `AppError` 交给调用方；应用也可选择有限重试或隔离，但必须保证业务处理成功后才确认。会话错误通过一次 `Some(Err(error))` 交付，之后 `receive()` 返回 `None`，快照记录 `Error { kind }`。`close()` 发起有时限且可重复调用的关闭握手；对端以 `1009` 关闭但握手已完成时，`close()` 仍可返回 `Ok(())`，这只证明协议收尾成功，不代表订阅成功。关闭回包失败会返回错误；已有终态错误或超时也会从 `close()` 返回，worker 应处理结果。

服务负责创建通道，并持有发送端和任务句柄。将接收端交给 `subscribe`；服务停机时先停止生产者，再发送一次停机信号，并等待 worker 结束以取得 `close()` 结果：

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
    // 此 future 在生产者停止且服务开始停机后完成。
    stop_producers_and_wait.await;
    let _ = shutdown_tx.send(());
    worker.await??;
    Ok(())
}
```

如果发送端未发送就被释放，接收端也会结束，worker 会走相同的关闭路径。对端完成关闭握手后，`receive()` 返回 `None`，快照为 `WsTermination::Closed { code }`，应用再按状态码决定业务处理；终态错误会先交付一次，并记录为 `WsTermination::Error`。本地主动停机则结束接收等待，随后由 `close()` 发起有界握手。若对端未能及时完成握手，worker 返回 `WsErrorKind::Timeout`；如果会话已有终态错误，也可能由 `close()` 返回该错误。队列接纳消息不等于对端收到或业务确认。

可以将订阅函数连接到 worker helper，并把 token 移入闭包。停机 future 表示先停止生产者，再通知 worker：

```rust,no_run
async fn run_subscription(token: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_worker(
        move |shutdown| async move { subscribe(&token, shutdown).await },
        async {
            // 停止生产者并等待它们结束。
        },
    )
    .await
}
```

## 多个任务共享发送端

一个 worker 可以让多个生产者通过同一会话发送命令。连接成功后调用 `session.sender()`，再克隆 `WsSender` 分给各任务；所有副本共用同一组出站消息数和字节数预算。以下代码放在 worker 的生产者启动处，需要 Tokio runtime。发送成功只是入队；重要命令仍需通过事件流里的业务确认来关联结果。

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

这个 helper 会等两个生产者任务都结束，并把任务失败（`JoinError`）或入队失败（`WsSendError`）交给调用方。`WsSendError` 表示消息没有进入队列；调用方需要决定是否重试、延后处理或执行过载策略。

生产者不能等待时可用 `try_send`：`Backpressure` 表示当前消息数或字节预算已满；`MessageTooLarge` 表示单条消息超过出站字节上限；`Closed` 表示会话不再接受消息。`send` 会等待容量；若等待入队的 future 被取消，临时容量预留会释放。无论哪种方法都不承诺数据已通过网络送达。遇到 `Backpressure` 时按消息语义处理：可替换的状态更新可以延后或合并；必须送达的命令应等待 `send()` 或向调用方报告过载。不要静默丢弃不可重复的命令，也不要循环调用 `try_send()`。

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

应用通过 `receive` 取走消息后才释放对应入站容量。若队列已满，驱动会在继续处理控制帧的同时，最多保留一条等待入队的业务消息。容量及时恢复时，该消息即可入队；若容量恢复前又收到额外业务数据，驱动可提前发送 WebSocket `1013` 并关闭；若没有额外业务数据，则 `no_progress_timeout` 到期也会触发关闭。写入 Close 帧失败时，不保证对端收到它。出站消息在排队或写出期间占用预算。遇到背压时，应用可以等待、合并可替换的状态更新，或向上游报告过载；不要反复循环调用 `try_send`，它不会等待容量变化。

## 停机、取消与恢复

优雅停机时先停止生产者，通知 worker 结束接收循环，再等待 worker 调用 `session.close().await` 并记录结果，最后释放会话。握手不超过配置的 `close_timeout`，超时会终止驱动并返回 `WsErrorKind::Timeout`。`WsTermination::Closed { code }` 表示关闭握手完成，状态码由应用按协议解释；`1009` 应作为拒绝结果诊断，不能盲目重试；若没有状态码，应先核对服务状态再判断订阅是否完成。事件处理失败时由应用决定有限重试、隔离或停止；无论采取哪种策略，都应在所需业务处理成功后再确认事件。传输或协议故障记录为 `WsTermination::Error`，不会触发自动重连。若发送端未发送就被释放，接收端结束也会触发本地停机。

`WsSession` 是接收权和会话存活权的持有者。若 worker 被取消或提前退出而直接释放 session，即使生产者还留有 `WsSender`，Drop 也会请求本地有界关闭并关闭发送队列；等待中的发送以及后续发送会得到 `WsSendError::Closed`。尚未写出的排队消息可能被丢弃。Drop 不等待对端确认，也不提供关闭结果；需要观察关闭是否成功时，应按上面的停机顺序显式等待 `close().await`。如果任务取消导致无法执行显式关闭，应用应按自身协议决定如何恢复未确认的命令和遗漏事件。

丢弃等待中的 `receive()` future 只会结束当前调用方的等待，不会关闭会话。取消尚未完成的 `send()` 会放弃这次入队，并释放临时预算。丢弃会话或取消其所属任务，并不会让库自动重连、重放队列或恢复业务状态。服务重启后是否重新订阅、如何补齐遗漏事件、怎样避免重复命令产生副作用，应由应用决定；例如可在业务协议中使用幂等键或序列游标。

## 错误与排障

`WsError::kind()` 返回稳定的错误类别；握手错误还可查询 `status_code()` 和 `handshake_failure()`。错误类别包括 `InvalidUrl`、`Transport`、`Tls`、`Handshake`、`Protocol`、`FrameTooLarge`、`MessageTooLarge`、`Timeout`、`Internal` 和 `InvalidConfig`。`Transport` 涵盖连接建立、运行期 I/O，以及 WebSocket HTTP 升级期间的普通网络 I/O 故障；HTTP 拒绝或结构化升级校验失败归为 `Handshake`。驱动任务异常归为 `Internal`。取消 future 只会丢弃当前操作，不会返回 `WsError`；`WsErrorKind::Cancelled` 与 `WsErrorKind::Closed` 没有运行时生成路径，已不属于当前 API。发送队列单独返回 `WsSendError`：`Backpressure`、`MessageTooLarge` 或 `Closed`，应在发送调用处传播或处理；其中 `WsSendError::Closed` 表示发送端不再接收消息，`WsTermination::Error` 则记录会话级终态故障。

| 现象 | 检查方向 |
| --- | --- |
| `Transport` | 根据错误发生阶段检查 DNS/TCP 建连、升级期间的网络断开或运行期网络 I/O；若对端在返回 HTTP 升级响应前断开，也属于此类。 |
| IPv6 端点返回 `InvalidUrl` | URL 中的 IPv6 字面量要用方括号括起，例如 `ws://[::1]:8080/events`。方括号属于 URL 语法，TCP 目标会按括号内的 IPv6 地址解析；带作用域标识的 IPv6 地址不受支持。 |
| `Handshake` | 检查 HTTP 拒绝状态和升级响应是否合法；可查看 `status_code()`、`handshake_failure()`，切勿记录认证头。 |
| WSS 返回 `Tls` | 核对 URL 主机名、证书有效期，以及所选根证书策略是否含签发 CA。 |
| 服务端未选择子协议 | 对照客户端提议值和服务配置，并检查 `selected_subprotocol()`。 |
| `FrameTooLarge` 或 `MessageTooLarge` | 对照服务端事件形态和两个入站上限；提高限额也会提高潜在内存占用。 |
| `Backpressure` | 检查生产速度以及出站消息数、字节数预算，明确等待、延后或过载处理策略。 |
| `Timeout` | 检查 DNS、连接和握手进度、对端读写情况，以及无进展或关闭期限。入站慢消费者关闭也以此公开类别结束；它不表示业务处理已完成，也不表示对端确认了 Close 帧。 |

`outbound_bytes` 限制的是队列中应用 payload 的字节数，与 WebSocket 驱动的写缓冲上限不同：写缓冲会额外增加 14 字节来容纳最大数据帧头，并增加 131 字节来容纳最大客户端控制帧。即使 payload 预算只有 1 字节，Ping/Pong 和 Close 仍有处理所需的缓冲空间。这两项预算都不是整个会话精确的内存上限；传输 I/O 停滞时，也不能保证帧已送达对端。WSS 始终校验证书链和服务端身份，使用 IPv6 字面量不会关闭 TLS 验证。

不要记录 bearer token，也不要把入队成功当作业务确认。精确签名请查阅 [API 文档](https://docs.rs/qubit-websocket)。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [English README](../README.md)
- [架构说明](architecture.zh_CN.md) · [Architecture](architecture.md)
- [Rust API 文档](https://docs.rs/qubit-websocket)
- [可运行 WS 示例](../examples/ws_client.rs) · [可运行 WSS 示例](../examples/wss_client.rs)
- [English user guide](user_guide.md)
