# 验证命令记录

目标仓库候选 worktree：`/data/working/qubit/rust-common/.worktrees/rs-websocket-registration-20261104`。原始 checkout 为 `dev-starfish`，HEAD `316a1d56966db60aa40044f9ceb7a7fd98c44bfd`。工具链为 rustc/cargo 1.94.0，Linux x86_64。逐轮性能原始输出保存在本目录的 registration、wake 与 queue 日志。

## WS-A01 基线和候选

| 命令 | 退出码与结果 |
| --- | --- |
| 原实现：`CARGO_TARGET_DIR=/tmp/rs-websocket-registration-head-target cargo test --locked --manifest-path /data/working/qubit/rust-common/.worktrees/rs-websocket-head-probe/Cargo.toml --release --lib registration_probe -- --ignored --nocapture --test-threads=1` | 0；2 个 probe 通过。由临时 HEAD worktree 加入相同 probe 测量代码，生产实现保持未修改。 |
| 候选：`cargo test --locked --release --lib registration_probe -- --ignored --nocapture --test-threads=1` | 0；2 个 probe 通过。包含 warmup、7 轮 byte/count/zero-byte 数据、heartbeat、双 worker 补测、释放后有界进展与预算回收断言。 |
| `cargo test --locked --lib session` | 0；97 passed、5 ignored。 |
| `cargo test --locked --release --lib wake_probe -- --ignored --nocapture --test-threads=1`（HEAD 与候选） | 两侧均为 0；wake 行为及 poll 总数一致。 |
| `cargo test --locked --release --lib queue_followup_probe -- --ignored --nocapture --test-threads=1`（HEAD 与候选） | 两侧均为 0；2 个 probe 通过，正常 fast path、取消、close/drain 和预算清理均有结果。 |
| `git diff --check` | 0。 |

主 byte-limited registration p50/p95（HEAD → 候选）：128 waiter `58,424/74,478 → 39,169/43,638 ns`；1,024 waiter `1,753,260/1,781,995 → 354,508/373,039 ns`；4,096 waiter `34,810,148/35,565,065 → 1,647,571/1,878,199 ns`；16,384 waiter `638,937,171/667,023,941 → 7,327,764/7,444,587 ns`。4,096 和 16,384 waiter 的 p50 分别下降约 95.3% 和 98.9%。7 轮全部保留；未剔除高尾。Count-limited 与 zero-byte fast-path 也各有七轮同条件对照。候选正常 Text/Bytes push/pop p50/p95 为 `168/168 ns`；HEAD Text 为 `168/169 ns`、Bytes 为 `168/168 ns`。128-waiter mixed byte-limited wake p50/p95 为 HEAD `1/1 µs`、候选 `1/2 µs`，poll 总数相同。双 worker 16k cancel 候选 p50 比 HEAD 高约 8.9%，但候选有两轮高尾（p95 `22.586 ms` 对 HEAD `8.004 ms`）；其余 current-thread cancel/close 中位数与预算/进展断言没有稳定回退。该补测样本有限，按压力测量观察，不作为业务 SLA。

新增零字节竞争回归在旧实现和候选上均通过；释放消息槽后 waiter 在 2 秒 timeout 内完成，FIFO 与预算归零断言通过。该回归保护容量耗尽时零字节消息仍可直接入队的语义。

## WS-A02 历史来源与链接

历史原件查找使用精确名称。`git log --all --name-only --pretty=format: -- doc | rg '(^|/)(design-20261104T160902440Z-1b834208\.md|implementation-plan-20261104T160902440Z-1b834208\.md)$'` 按无匹配退出 1；`git ls-tree -r --name-only HEAD doc` 本身退出 0，再按 160902 精确名称筛选退出 1；`find /data/working/qubit/rust-common -type f` 按原件准确文件名搜索退出 0、无匹配。也核对了相关记录提及的 141654 文件名，仅找到 resolution 记录，没有设计/计划原件。原始 evidence 目录仍存在。

| 命令 | 退出码与结果 |
| --- | --- |
| `python3 -m unittest discover -s scripts/tests -p 'test_review_followup_links.py'`（旧文档） | 1；预期 RED，准确指出缺失设计与计划两条相对文件链接。 |
| `python3 -m unittest discover -s scripts/tests -p 'test_review_followup_links.py'`（修正文档后） | 0；1 项通过。 |
| `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | 0；41 项通过。 |

## 集成与完整验证

| 命令 | 退出码与结果 |
| --- | --- |
| `./.infra/bin/align-ci.sh` | 0；格式/Clippy 对齐检查通过，diff 未引入无关格式改动。 |
| `./.infra/bin/style-check.sh` | 0；Rust style checks passed。 |
| `cargo test --locked` | 0；默认 feature 全套通过。 |
| `cargo test --locked --all-features` | 0；all-features 全套通过。 |
| `cargo test --locked --doc --all-features` | 0；16 个 doctests 通过。 |
| `python3 scripts/check-doc-examples.py --mode default` | 0；英文/中文 README 与指南示例各组编译通过。 |
| `python3 scripts/check-doc-examples.py --mode config` | 0；默认文档及 config 示例各组编译通过。 |
| `cargo test --manifest-path tests/fixtures/consumer/Cargo.toml --locked --test handshake_tests` | 0；独立 consumer handshake 1 项通过。 |
| `python3 scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/rust-common/.worktrees/rs-websocket-registration-20261104 --output /tmp/rs-websocket-review-c217986f-current-interop` | 0；从 current source 编译并通过 WS 5 项、WSS 3 项；元数据在 `current-interop.json`。 |
| `./.infra/bin/ci-check.sh` | 0；Rust style、build/test/doc/readme、package、feature matrix、Clippy、coverage 与 dependency checks 通过。Coverage：functions 97.86%（门槛 ≥95%）、lines 94.77%（门槛 >90%）、regions 92.80%（门槛 >85%）。`coverage-cfg-clippy` 由仓库配置 `coverage_cfg_clippy = false` 明确禁用，故脚本报告未配置并跳过。 |

开发期间的第一次独立 `rustfmt --check` 检查还报告了未修改的既有 `queue_inner.rs:121` 长行及测试文件其他原有格式差异，退出 1；新增行之后已按格式要求整理，且最终 `align-ci.sh` 与 `style-check.sh` 均退出 0。CI 只跳过仓库明确配置为 false 的 `coverage-cfg-clippy`，未降低或屏蔽任何门槛。
