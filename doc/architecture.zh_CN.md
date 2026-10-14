# 架构说明

本文说明 `qubit-websocket` 0.1 的现行结构。实现取舍见[本轮实施设计文档](design-20261014T021031598Z-49c7bedd.md)；当前行为以源码和 API 文档为准。也可阅读 [English version](architecture.md)。

## 分层与职责

客户端与握手层校验端点和请求选项，建立传输连接并完成 HTTP/1.1 WebSocket 升级。握手成功后创建 `WsSession`；若失败，会在会话创建前返回 `WsError`。TLS 信任策略和请求头由应用通过 client builder 配置。

会话 API 为应用提供接收接口和发送端。入站、出站队列都受消息数量和 payload 字节数限制。`send()` 等待队列容量，`try_send()` 立即报告容量情况；队列无法接纳消息时，两者都返回 `WsSendError`。消息成功入队不代表已经送达或得到业务确认。

会话驱动负责协议 I/O，并协调读写任务。它处理 Ping、Pong 和 Close 控制帧，将文本与二进制消息交给应用。关闭协调会停止接纳新的业务消息，尝试回复协议 Close，并在配置的关闭期限内等待结束。驱动记录单一终态，应用可通过 `termination()` 查询。

## 结果边界

| 结果 | 含义 | 应用职责 |
| --- | --- | --- |
| `WsSendError` | 出站消息未能进入会话队列。 | 决定等待、延后、拒绝请求或停止生产。 |
| `WsTermination::Closed { code }` | WebSocket 关闭握手已完成；若对端提供状态码，则原样保留。 | 按应用协议解释状态码。`1009` 不代表业务成功。 |
| `WsTermination::Error { kind }` | 关闭协调或其他终止操作失败，会话以错误结束。 | 诊断并选择恢复策略；库不会自动重连或重放消息。 |
| `receive()` 或 `close()` 返回的 `WsError` | 会话级操作报告故障；终态接收错误只交付一次。 | 处理错误，并确认停机结果。 |

`WsErrorKind::Transport` 覆盖连接建立和运行期传输 I/O；驱动任务故障归为 `WsErrorKind::Internal`。发送队列接纳失败属于 `WsSendError`，不会仅因一次发送失败就变成会话终态错误。

## 停机流程

优雅停机时，服务先停止生产者，再通知 worker 并等待它结束。worker 停止消费后调用 `close().await`，并检查终态。关闭握手受配置的超时限制；若对端未及时完成，停机操作会返回超时或已记录的终态错误。需要确认关闭结果时，应显式等待关闭流程，不能用丢弃会话代替。

事件解码、持久化、业务确认和断线恢复均由应用负责。尤其是，协议关闭完成不能证明队列中的命令已被处理，也不能证明已消费事件已持久化。

## 延伸阅读

- [中文用户指南](user_guide.zh_CN.md) · [English user guide](user_guide.md)
- [中文 README](../README.zh_CN.md) · [English README](../README.md)
- [Rust API 文档](https://docs.rs/qubit-websocket)
- [本轮实施设计](design-20261014T021031598Z-49c7bedd.md) · [历史设计记录](design.zh_CN.md)
