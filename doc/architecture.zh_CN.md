# 架构说明

本文说明 `qubit-websocket` 0.3 的现行结构。也可阅读 [English version](architecture.md)。早期设计记录未包含在当前检出中；当前行为以源码和 API 文档为准。

2026-11-02 较早时，在临时外部消费者中针对本地 `rs-web` 工作树提交 `7bd9a4f1f10f4de6051d771f2b5516fc5f193ef2` 运行了现有五项互通场景，全部通过。复制的测试源码 SHA-256 为 `f1288dce9088c73eba8b6bb73fa904968a9db4b0f6ba739f06a2de9829fcf73d`。本轮实施又使用同一测试源码对当时最新的本地检出 `74331ed0697042e16e60d3ba6b845f5bfc4937ad` 重跑这五项场景，全部通过。本 crate 默认的 `qubit-web` Git 固定版本仍为 `3046fa0fbc46c6c4584fdf9710b45649dfebd70a`；这两次本地工作树结果都不能证明对已发布版本的兼容性。

## 分层与职责

客户端与握手层校验端点和请求选项，建立传输连接并完成 HTTP/1.1 WebSocket 升级。握手成功后创建 `WsSession`；若失败，会在会话创建前返回 `WsError`。TLS 信任策略和请求头由应用通过 client builder 配置。普通 `ws://` 连接不初始化 TLS。对于 WSS，每个构建好的客户端会延迟缓存 TLS 配置和会话恢复状态；该客户端的克隆共享缓存，分别构建的客户端保持隔离。客户端使用应用已安装的 rustls provider；若未安装，则使用该客户端自己的 aws-lc fallback，且不修改进程默认 provider。之后安装的 provider 会在后续 WSS 连接中生效。

IPv6 URL 字面量必须带方括号，例如 `ws://[::1]:8080/events`。握手解析器只在生成 TCP 目标时去掉方括号，并校验其中的 IPv6 地址；带作用域标识的 IPv6 地址不受支持。WSS 仍会校验证书链和服务端身份。

会话 API 为应用提供接收接口和发送端。入站、出站队列都受消息数量和 payload 字节数限制。`send()` 会等待出站两项预算同时可用，等待期间不预留任一预算；已接纳消息在驱动写出期间仍持有两项预算。`try_send()` 根据已接纳消息立即报告容量情况。成功接纳的消息按 FIFO 排列，但并发等待者不保证严格 FIFO，也不保证不会饥饿，因此较小的消息可以先于较早等待的大消息入队。队列无法接纳消息时，两者都返回 `WsSendError`。关闭状态优先：观察到队列关闭后发送会返回 `Closed`，即使消息超限也一样；只有队列仍开放时，超限消息才返回 `MessageTooLarge`。消息成功入队不代表已经送达或得到业务确认；等待中的 future 自身持有 payload，不计入队列预算。

出站 payload 队列预算与 WebSocket 驱动的写缓冲上限是两项独立限制。写缓冲额外增加 14 字节以容纳最大数据帧头，并增加 131 字节以容纳最大客户端控制帧，因此最小有效 payload 预算下仍能处理 Ping/Pong/Close。这两项预算都不是整个会话精确的内存上限；如果传输 I/O 无法继续，预留空间也不能保证对端收到帧。

`WsSession` 持有接收权和会话存活权。释放它会同步请求驱动执行有界本地关闭，并关闭两侧队列；仍被克隆持有的 `WsSender` 会因发送队列关闭而返回 `WsSendError::Closed`，不能延长会话寿命。已入队但尚未写出的消息可能被丢弃。Drop 不等待关闭握手，因此不能据此观察对端是否确认；要取得关闭结果，应用必须显式调用 `close().await`。

单个驱动任务持有协议 I/O，以及读写两个 future。它处理 Ping、Pong 和 Close 控制帧，将文本与二进制消息交给应用。正常发布终态前，读写 future 均已完成或已被释放。关闭协调会停止接纳新的业务消息，尝试回复协议 Close，并在配置的关闭期限内等待结束。驱动只记录一个终态，应用可通过 `termination()` 查询。

入站队列满时，驱动最多暂存一条等待入队的消息，同时继续轮询控制帧。容量恢复前若又收到业务消息，会话以 `WsErrorKind::InboundBackpressure` 结束；若没有额外业务消息，则 `no_progress_timeout` 到期后以 `WsErrorKind::Timeout` 结束。两条路径都会尝试发送 `1013` Close，但写入失败时无法保证对端收到。这些都是会话故障，不代表业务处理完成或对端确认关闭。

## 结果边界

| 结果 | 含义 | 应用职责 |
| --- | --- | --- |
| `WsSendError` | 出站消息未能进入会话队列。 | 决定等待、延后、拒绝请求或停止生产。 |
| `WsTermination::Closed { code }` | WebSocket 关闭握手已完成；若对端提供状态码，则原样保留。 | 按应用协议解释状态码。`1009` 不代表业务成功。 |
| `WsTermination::Error { kind }` | 关闭协调或其他终止操作失败，会话以错误结束。 | 诊断并选择恢复策略；库不会自动重连或重放消息。 |
| `WsErrorKind::InboundBackpressure` | 已有一条消息等待入队时，又收到额外入站业务消息。 | 排查消费滞后，并由应用决定重连、重放或补偿。 |
| `receive()` 或 `close()` 返回的 `WsError` | 会话级操作报告故障；终态接收错误只交付一次。 | 处理错误，并确认停机结果。 |

`WsErrorKind::Transport` 覆盖连接建立、运行期传输 I/O，以及 HTTP 升级期间的普通网络 I/O；HTTP 拒绝或结构化升级校验失败归为 `WsErrorKind::Handshake`。数值选项校验会给 `InvalidConfig` 附加不含配置值的 `WsConfigFailure`；手动构造的错误及 TLS provider 配置错误可能没有该原因。TLS 错误仍单独归类。驱动任务故障归为 `WsErrorKind::Internal`。发送队列接纳失败属于 `WsSendError`，不会仅因一次发送失败就变成会话终态错误。

## 依赖与消费者边界

握手协议栈使用 `tokio-tungstenite` 0.30.0。其 registry 依赖 `tungstenite` 0.30.0 尚不能接受所有合法的逗号分隔或重复 `Connection` token 字段。本 workspace 将已知修复固定到 Qubit fork 提交 `b24a4442be0247a71fe9ee4e520eb483ac1b2873`。Cargo 根 patch 不会向下游应用继承，因此在上游 registry 发布修正版之前，受影响的消费者需要在自己的根清单中加入同一 patch。独立消费者夹具和 workflow 验证的是应用该 patch 后的依赖路径；这属于有条件兼容，并不代表未加 patch 的 registry 消费者已修复。

## 停机流程

优雅停机时，服务先停止生产者，再通知 worker 并等待它结束。worker 停止消费后调用 `close().await`，并检查终态。关闭握手受配置的超时限制；若对端未及时完成，停机操作会返回超时或已记录的终态错误。期限到达时，`WsSession::close()` 会请求中止驱动并在期限内返回，但不保证外层驱动任务在返回瞬间已经完成 join。需要确认关闭结果时，应显式等待关闭流程，不能用丢弃会话代替。

事件解码、持久化、业务确认和断线恢复均由应用负责。尤其是，协议关闭完成不能证明队列中的命令已被处理，也不能证明已消费事件已持久化。

## 延伸阅读

- [中文用户指南](user_guide.zh_CN.md) · [English user guide](user_guide.md)
- [中文 README](../README.zh_CN.md) · [English README](../README.md)
- [Rust API 文档](https://docs.rs/qubit-websocket)
- [更早的设计说明](design.zh_CN.md)
