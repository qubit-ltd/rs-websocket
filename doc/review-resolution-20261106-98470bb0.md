# WS-20261106-001 结案报告

## 结论

已补齐不同 waiter 登记顺序的性能验证。基线测量确认，在 K=4096 且队首 waiter 不可容纳、后续 waiter 可容纳的排列中，原选择器遍历多个字节 bucket 会产生稳定的额外选择成本。最终保留了一个局部候选：索引启用且 waiter 数超过 128、`free_bytes > 0` 时，先按登记顺序取前 8 个 waiter（已通知项也占用窗口），选择窗口内首个未通知且可容纳的 waiter；窗口内没有符合项时，沿用字节索引回退。候选不增加索引状态、不改变公共 API，也不改变最小 waiter ID、容量预留、关闭广播、取消交接或索引滞回语义。

这项结论描述探针所测的合成队列排列，不等同于生产端到端延迟保证。

## 变更

- `src/session/tests/internal_tests.rs`：将 ignored 性能探针扩展为 K=1、16、128、1024、4096，升序、降序、固定 seed 洗牌和不可容纳前缀排列，以及 zero/quarter/all 释放矩阵。每格执行 3 次预热和 30 个计时样本，轮次轮换场景顺序，计时仅包围释放占位消息。
- 新增非 ignored K=129 降序与固定 seed 洗牌语义回归，断言唯一唤醒登记序列中首个可容纳 waiter、其 poll 成功且最终 usage 为 `(0, 0)`。
- 新增 indexed zero-byte waiter 回归，验证无可用字节时仍能正确选择零字节 waiter。
- 新增 Tokio 双 worker 竞争探针，竞争操作数据单独呈现，不与单线程释放延迟混算。
- `src/session/internal/queue_inner.rs`：索引启用且 waiter 数超过 128、`free_bytes > 0` 时，先按登记顺序取前 8 个 waiter（已通知项也占用窗口），选择窗口内首个未通知且可容纳的 waiter；窗口内没有符合项时，回退至字节索引。

没有公共语义变化，因此未改双语 README 或用户指南。

## 性能证据

单线程探针基线的 K=4096 blocked-prefix/all p50 三轮为 40,736、39,142、37,882 ns；升序/all 控制为 2,564、1,170、1,243 ns。最终候选三组独立交替配对结果如下：

| 场景 | Pair 1 baseline → 候选 | Pair 2 baseline → 候选 | Pair 3 baseline → 候选 |
| --- | --- | --- | --- |
| K=4096 blocked-prefix/all（各值为三轮 p50，ns） | 36,354/36,298/36,215 → 1,580/1,565/1,505 | 37,420/37,232/37,293 → 1,433/1,445/1,432 | 37,407/37,391/37,377 → 1,434/1,473/1,419 |
| K=4096 blocked-prefix/quarter（各值为三轮 p50，ns） | 10,020/9,997/9,925 → 1,526/1,579/1,526 | 10,428/10,350/10,253 → 1,413/1,413/1,400 | 10,295/10,267/10,263 → 1,458/1,498/1,477 |

目标场景 all 每轮改善约 94–96%，quarter 每轮改善约 85–87%。zero-free K=4096 控制以绝对纳秒差分析：最终候选在 ascending/zero 九个配对样本的 candidate-minus-baseline 差值中位数为 0 ns；blocked-prefix/zero 中位数为 +31 ns、均值约 +12 ns，未见跨轮次稳定超过 10% 的退化。低 K 控制差异为数纳秒量级。

最终 unfit-prefix 控制中，16,384 waiter 的 release p50 为基线 2,474 ns、候选 1,520 ns；cancellation p50 为 751/460 ns，close p50 为 686,450/665,487 ns。阈值 churn 的 64 waiter cycle p50 为 34,114/34,326 ns；双 worker 补充测量为 40,906/40,621 ns。控制数据没有显示稳定超过 10% 的回退。

竞争探针覆盖 3 轮 × K=1024/4096 × quarter/all，共观察 17 次竞争尝试、12 次成功、5 次 backpressure。竞争线程未在所有释放计时窗口稳定抢占容量；该数据仅说明调度与锁竞争，不用于推导单线程延迟。

各次原始样本、失败候选、环境、命令和退出结果见 [性能证据目录](evidence/review-20261106-98470bb0/) 与其中的 [T1/T2 运行记录](evidence/review-20261106-98470bb0/run-record.md)。最终生产候选 SHA-256 为 `99a913073120c6b68c5a8852c5a22daf84bab96ecd1f6b1c188f82069680f09b`。

## 最终验证

所有命令均在 `/data/working/qubit/.worktrees/rs-websocket/ws-20261106-001-implementation` 中运行；命令日志位于 `doc/evidence/review-20261106-98470bb0/`。

| 命令 | 结果 |
| --- | --- |
| `./.infra/bin/align-ci.sh` | 退出码 0；对齐和仓库 CI 检查通过 |
| `./.infra/bin/style-check.sh` | 退出码 0 |
| `rustfmt --config-path .infra/style/rustfmt.toml --config skip_children=true --check src/session/internal/queue_inner.rs src/session/tests/internal_tests.rs` | 退出码 0 |
| `cargo test --locked --lib test_capacity_index_selects_oldest_fitting_waiter_across_orders` | 退出码 0，1 passed |
| `cargo test --locked --lib test_indexed_waiter_zero_bytes_progress_when_no_byte_capacity_is_free` | 退出码 0，1 passed |
| `cargo test --locked --lib session::` | 退出码 0，104 passed，10 ignored |
| `cargo test --locked` | 退出码 0；所有单元、集成和 doctest 目标通过 |
| `cargo test --all-features --locked` | 退出码 0；所有单元、集成和 doctest 目标通过 |
| `cargo test --all-features --locked --doc` | 退出码 0，16 doctests passed |
| `env 'RUSTDOCFLAGS=-D warnings' cargo doc --all-features --no-deps --locked` | 退出码 0 |
| `./.infra/bin/coverage.sh` | 退出码 0；函数 98.08%（307/313）、行 95.23%（2578/2707）、regions 93.51%（4204/4496），达到现有门槛 |
| `./.infra/bin/ci-check.sh` | 退出码 0；CI、coverage、dependency/audit 等配置任务通过 |
| `python3 scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/.worktrees/rs-websocket/ws-20261106-001-implementation --output /tmp/rs-websocket-interop-98470bb0` | 退出码 0；5 个 WS 与 3 个 WSS 互操作用例通过 |

额外诊断 `cargo fmt --all -- --check` 退出码为 1，因为它使用默认 rustfmt 配置，不符合本仓库的 `.infra/style/rustfmt.toml`，并报告大量其他格式差异。使用仓库配置的 package 检查和本轮两个 Rust 文件检查均退出码 0；配置化 workspace 诊断只发现准备好的本地依赖 checkout 中的格式差异，没有报告目标项目文件。没有为此修改无关源码或依赖仓库。

独立复审未发现实现语义或性能结论方面的实质问题；证据复审指出的环境快照与日志描述问题已修正，并经复审确认准确。设计文档与实施计划未修改，均留在原路径。无依赖升级，无 API 迁移。
