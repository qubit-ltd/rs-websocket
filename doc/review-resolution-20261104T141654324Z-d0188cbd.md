# WS-001/WS-002/WS-003 实施与复核记录

日期：2026-11-04
项目：`rs-websocket`（本轮 Git worktree：`/data/working/qubit/rust-common/.worktrees/rs-websocket/ws-recoverable-20261104`）
设计：`doc/design-20261104T141654324Z-d0188cbd.md`（原件未随本记录归档；本次定向核查未找到可访问原件）
计划：`doc/implementation-plan-20261104T141654324Z-d0188cbd.md`（原件未随本记录归档；本次定向核查未找到可访问原件）

### 设计与计划原件核查

2026-11-04 定向核查了当前可见的 `git log --all` 历史，以及 `/data/working/qubit/rust-common` 下相关现有目录；未找到上述设计和实施计划原件。HEAD 可见的历史记录曾将它们描述为“源仓库文档，保留且未提交”，但这不能证明原件现可访问或已随记录归档。本记录不以其他文档或当前方案替代原件；因此无法据原件核对本次实施与设计/计划依据的一致性。

## WS-001：可回收发送结果

新增并从 crate 根导出 `WsSendFailure`，提供 `kind()`、`message()`、`into_message()`、`into_parts()`。`WsSender` 与 `WsSession` 均增加同步 `try_send_recoverable` 和异步 `send_recoverable`。原 `WsSendError`、`try_send`、`send` 的签名与错误类别保持不变；旧方法转发到同一队列接纳路径，失败时投影错误类别并销毁消息。失败对象持有原消息，不克隆或重编码；文本 `String` 的指针/容量及二进制 `Bytes` 的共享指针由测试核对。Debug/Display 只输出类别、变体和 payload 长度。关闭仍优先于超限；取消待处理的异步 future 会销毁消息，成功仅表示排队，不表示网络或业务交付。

核心验证：

- `cargo test --locked --lib recoverable`：退出 0，3 项队列回收测试通过。
- `cargo test --locked --test session_tests recoverable`：退出 0，1 项公开句柄 WS 测试通过；覆盖两个句柄的同步/异步成功和关闭失败。
- `cargo test --locked --lib session`：退出 0，94 passed、1 ignored；包含失败类别、优先级、所有权、脱敏、取消预算及队列行为测试。
- `cargo test --locked`、`cargo test --locked --all-features`、`cargo test --locked --doc --all-features`：均退出 0；新增 Rustdoc 示例通过。
- 独立消费者：`cargo test --manifest-path tests/fixtures/consumer/Cargo.toml --locked --test handshake_tests` 退出 0，1 passed。

## WS-002：广播唤醒测量与条件优化

### 测量环境和方法

- 日期：2026-11-04；Linux x86_64；`rustc 1.94.0 (4a4ef493e 2026-03-02)`；Tokio 1.54.1；项目锁文件依赖；debug profile。
- 命令（基线、初版候选重跑、单锁候选完全相同）：`cargo test --locked --lib wake_probe -- --ignored --nocapture`。基线、成功候选重跑、单锁最终候选退出码均为 0；初版候选原始日志还保留一次失败尝试（退出 101，`iter_mut` 绑定缺少 `mut`），修复后重跑成功。表格采用单锁最终候选日志，与保留的 baseline/single-lock 附件逐项对应。
- 每组 5 轮；等待者数 1/16/128；均匀 128-byte 与混合 65/128-byte 消息；分别由消息数或字节预算限制。屏障计数确认所有 push future 首次 Pending 后逐次释放一个容量。记录每轮 poll 总数、逐次接纳延迟样本、聚合 p50/p95/p99 与 5 轮总墙钟耗时。探针没有 profiler，CPU 时间未测。
- 完整原始输出作为本仓库证据保留：基线 [`wake-probe-baseline`](evidence/wake-probe-baseline-20261104T141654324Z-d0188cbd.txt)、初版候选 [`wake-probe-candidate`](evidence/wake-probe-candidate-20261104T141654324Z-d0188cbd.txt)、单锁最终候选 [`wake-probe-single-lock`](evidence/wake-probe-single-lock-20261104T141654324Z-d0188cbd.txt)。这些文件由本轮原始 `/tmp` 日志复制；完成后清理 `/tmp` 原件，仓库证据附件保留供复核。

### 基线与候选结果

| 等待者 | 负载 | 瓶颈 | 基线 poll/轮 | 候选 poll/轮 | 基线接纳 p50/p95/p99 (µs) | 单锁候选 p50/p95/p99 (µs) | 基线/候选 5 轮耗时 (ms) |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 均匀 | 消息数 | 2 | 2 | 24/36/36 | 17/35/35 | 0/0 |
| 1 | 均匀 | 字节数 | 2 | 2 | 23/26/26 | 16/17/17 | 0/0 |
| 1 | 混合 | 消息数 | 2 | 2 | 23/25/25 | 16/16/16 | 0/0 |
| 1 | 混合 | 字节数 | 2 | 2 | 21/22/22 | 16/17/17 | 0/0 |
| 16 | 均匀 | 消息数 | 152 | 32 | 49/77/84 | 14/18/21 | 5/2 |
| 16 | 均匀 | 字节数 | 152 | 32 | 47/80/90 | 14/18/28 | 5/2 |
| 16 | 混合 | 消息数 | 152 | 32 | 49/74/81 | 13/18/26 | 5/2 |
| 16 | 混合 | 字节数 | 152 | 32 | 48/82/91 | 14/18/35 | 5/2 |
| 128 | 均匀 | 消息数 | 8384 | 256 | 223/277/303 | 16/18/29 | 167/17 |
| 128 | 均匀 | 字节数 | 8384 | 256 | 225/278/294 | 16/19/24 | 174/18 |
| 128 | 混合 | 消息数 | 8384 | 256 | 219/280/292 | 15/18/24 | 171/16 |
| 128 | 混合 | 字节数 | 8384 | 256 | 211/276/287 | 16/20/28 | 164/18 |

poll 计数包含每个等待者首次 Pending 和成功接纳时的最终 poll。128 等待者基线每轮有 `8384 - 128 - 128 = 8128` 次无可用容量的重复 poll；单锁候选为 `256 - 128 - 128 = 0`，减少 100%。128 等待者 p50 从 211–225 µs 降到 15–16 µs（约改善 92.4%–93.3%）。16 等待者无效 poll 从每轮 120 降至 0。单等待者各配置 p50 均改善（21–24 µs 降至 16–17 µs），没有观察到稳定超过 10% 的退化。候选通过至少减少 50% 无效 poll、至少改善目标中位指标 20%、单等待者不稳定退化超过 10% 的门槛，因此保留优化。p95/p99 是每配置 5 轮内逐次接纳样本的分位数；单等待者少量样本存在偶发尾值，不作为稳定退化证据。

### 候选实现和正确性

每个容量等待者登记需求字节数并持有独立通知。等待者集合、队列用量快照、登记、取消和唤醒选择均在同一个 `QueueState` mutex 下协调。容量释放后只通知一个符合双预算条件的等待者；被通知 waiter 若竞争失败会重置登记并交接唤醒。关闭会唤醒所有登记 waiter。没有引入 FIFO 或无饥饿承诺。

`cargo test --locked --lib session` 退出 0（94 passed、1 ignored），覆盖混合大小下符合容量者前进、被选 waiter 取消后转交唯一唤醒、释放与新注册竞态、关闭唤醒全部等待者。单锁协调是在第一次独立审查后按计划约束收紧；最终单锁版本的探针重新运行并达到上述门槛。

## WS-003：控制帧驱动进展

### 受控组合驱动

- `cargo test --locked --lib control_flood`：退出 0，3 passed。
- `cargo test --locked --lib driver`：退出 0，52 passed。
- `run_parts` 使用单次手工 poll、最多 1024 帧后 Pending 的 stream、记录 `start_send` 的可替换 sink。普通混合 Ping/Pong 与已发布本地 close 情形均在 64 帧预算处让出并在帧流耗尽前推进 writer；入站队列满等待情形在 65 帧内推进。测试避免将单线程 timeout 作为唯一看门狗。
- reader 普通热循环与入站容量等待热循环都在累计 64 条立即处理帧后调用 `yield_now`。当前帧所有权及原操作 deadline 保持不变，未拆分读写任务。

### 本地真实传输

- `cargo test --locked --test protocol_tests control_flood`：退出 0，2 passed。
- 单线程本地 WS 和可信本地 WSS peer 各发送有限的 256 个 Ping；两场景均观察到业务消息、Pong 和完成的 Close handshake。
- 结论仅限于这些有限本地传输批次通过，不声称真实网络重现长期饥饿或拒绝服务；有界 poll 证据来自受控组合驱动。

## 文档、门禁和复核

- 中英文 README/指南/架构与 `scripts/doc-examples.json` 已同步；新增 `guide_recoverable_send` 独立组，说明失败回收、一次性等待路径、Closed/MessageTooLarge 的应用策略、future 取消边界及成功只表示入队。
- `python3 scripts/check-doc-examples.py --mode default` 与 `--mode config` 均退出 0；所有 16 个 default 和 18 个 config 示例组均显示 `cargo exit 0`。
- `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` 退出 0，40 passed。
- 当前源码互操作：`python3 scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/rust-common/.worktrees/rs-websocket/ws-recoverable-20261104 --output /tmp/rs-websocket-interop-20261104T141654324Z-d0188cbd` 退出 0；WS 5 passed，WSS 3 passed。临时证据输出目录由本轮创建，验证后清理。
- `./.infra/bin/align-ci.sh` 最终重跑退出 0。此前中间尝试曾因并行编写中的 T3 测试 Clippy 风格问题及新类型双重 `must_use` 报错退出 1；均已按诊断修正后重跑。
- `./.infra/bin/ci-check.sh` 最终退出 0；项目 CI 的 style、build、test、strict-doc、README、release build、feature matrix、package、audit 和 dependency 检查完成。coverage 门禁实际执行并通过：functions 97.83%（阈值 ≥95%）、lines 94.61%（阈值 >90%）、regions 92.47%（阈值 >85%）。`coverage-cfg-clippy` 在仓库 CI 配置中为 `false`，故该可选子项明确未配置；常规 Clippy 与 coverage 均已运行。
- 最终独立审查未发现 Critical/Important 问题；审查提出的同锁协调要求已按计划修正并重新验证，测量原始输出也已作为仓库证据附件保留。变更限于本计划写入集合；本轮设计与计划文档未进入 worktree 提交集合。

## 中间失败与处理

- T1 红阶段 `cargo test --locked --lib recoverable` 退出 101，编译器明确报告 `BoundedQueue::try_push_recoverable` 尚不存在；新增失败对象和共享接纳路径后同命令退出 0，3 passed。
- 公开句柄测试首轮 `cargo test --locked --test session_tests recoverable` 退出 101，Close handshake 在 deadline 超时；诊断发现测试 peer 读取 Close 后未 flush 自动响应，加入 flush 后重跑退出 0，1 passed。
- 初版 wake probe `cargo test --locked --lib wake_probe -- --ignored --nocapture` 退出 101，测试 task 中 push future 借用了 task 外局部 queue；将 future 构造移入持有 queue 的任务后基线探针退出 0。
- T2 候选首次 Clippy 编译退出 101，waiter vector 需要可变绑定；加入 `mut` 后候选探针和 session 测试退出 0。
- 中间 `align-ci.sh` 曾因 T3 测试代码的未使用变量/手写取模及 T1 访问器上的冗余 `must_use` 退出 1。分别移除未使用绑定、改用 `is_multiple_of`、移除冗余属性；最终 `align-ci.sh` 在单锁版本上退出 0。
- 第一轮独立审查提出 T2 等待者登记和容量状态应由同一锁协调。将 waiters 移入 `QueueState` 后，重新运行单锁版本的 wake probe、session 测试、`align-ci.sh` 和 `ci-check.sh`，结果均通过。

## 限制

唤醒探针没有使用 CPU profiler，因此没有 CPU 时间数据；当前记录了 poll 数、接纳延迟分位数和墙钟耗时。有限本地 WS/WSS 测试不是公网压力或长期公平性证明。消息成功入队不等于送达，应用仍须自行处理关闭和业务恢复策略。
