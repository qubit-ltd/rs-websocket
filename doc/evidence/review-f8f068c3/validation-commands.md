# WS-R01–WS-R03 验证记录

所有目标仓库命令均在 `/data/working/qubit/rust-common/.worktrees/rs-websocket-ws-f8f068c3-20261105` 执行。该 worktree 基于起始 HEAD `0548724629f7defb3b6db4947b3f9419b0a2965b`；本轮不涉及公共 API 变化。

## 完成并通过

| 命令 | 结果 |
| --- | --- |
| `python3 -m unittest discover -s scripts/tests -p 'test_check_package_docs.py'` | exit 0，7 项通过；见 `package-doc-tests.txt` |
| `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | exit 0，48 项通过；见 `python-tests.txt` |
| `cargo package --list --allow-dirty` | exit 0；清单含双语 README、指定的六份文档；见 `package-list.txt` |
| 按 Cargo package list 在临时目录重建入包文件树，再运行 `python3 scripts/check-package-docs.py --root <临时根目录>` | checker exit 0；75 个清单文件、8 份 Markdown、6 份 `doc/` Markdown，链接及排除规则通过。此结果是清单驱动的临时树检查，不代表 `cargo package` 成功；见 `package-doc-stage-check.txt` |
| `cargo test --locked` | exit 0；默认测试及 16 个 doctest 通过；见 `cargo-test-default.txt` |
| `cargo test --all-features --locked` | exit 0；all-features 测试和 doctest 通过；见 `cargo-test-all-features.txt` |
| `cargo test --all-features --locked --doc` | exit 0，16 个 doctest 通过；见 `cargo-test-docs.txt` |
| `env 'RUSTDOCFLAGS=-D warnings' cargo doc --all-features --no-deps --locked` | exit 0；见 `rustdoc-strict.txt` |
| `./.infra/bin/align-ci.sh` | 首轮在严格 Clippy 中发现本轮探针的 redundant closure，按提示改为 `.map(Waker::from)` 后重跑 exit 0；见 `align-ci-final.txt` |
| `./.infra/bin/style-check.sh` | exit 0；见 `style-check-final.txt` |
| `./.infra/bin/coverage.sh` | exit 0；函数 98.02%、行 95.14%、区域 93.34%，均达到原门槛；见 `coverage.txt`。最后一轮 `ci-check.sh` 又运行了集成覆盖率 |
| `./.infra/bin/ci-check.sh` | 修正 Clippy 问题后的首轮 exit 0；扩展三轮边界回归后最终重跑也 exit 0，含 coverage 和 dependency audit；见 `ci-check.txt`、`ci-check-final.txt` |
| `cargo test --locked --lib session::tests::internal_tests` | 最终 post-align exit 0，26 通过、9 个 ignored；见 `internal-tests-post-align.txt` |
| `cargo test --locked --lib session::internal::bounded_queue` | 最终 post-align exit 0，5 通过；见 `bounded-queue-post-align.txt` |
| `python3 scripts/check-doc-examples.py --mode default --output <仓库外目录>` | exit 0，默认模式示例通过；见 `doc-examples-default.txt` |
| `python3 scripts/check-doc-examples.py --mode config --output <仓库外目录>` | exit 0，config 模式示例通过；见 `doc-examples-config.txt` |
| `python3 scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket <本 worktree> --output <仓库外目录>` | 最终运行 exit 0，WS 5/5、WSS 3/3，通过且输入树前后未变；见 `current-interop-final.txt` |
| release 阈值、payload 多样性及 65/63 前缀探针，及三轮基线/候选交替比较 | 全部有效采样命令 exit 0；R02 周期中位数改善 88%，R03 高 K 目标场景改善 80–97%，控制场景没有稳定超过 10% 的退化；见 `performance/summary.md` 和同目录原始样本 |
| `git diff --check` | exit 0 |

## 失败、诊断和修正

- 初次 R03 payload 探针 exit 101：断言错把“最大可容纳 waiter”当作应选者。队列语义要求最老可容纳 waiter；修正断言并为每个 future 使用独立 waker 后，探针三轮均通过。首次错误记录在 `performance/payload-diversity-first-attempt.txt`。
- 滞回回归在旧实现上按预期失败（128 waiter 时索引已停用）；加入停用低水位 64 后通过。测试还验证 129→128 反复三轮、回落到 64、重新激活时索引大小一致。
- fast-path 测试首编译曾因从外层测试模块访问私有 `BoundedQueue::inner` 而失败；移除不合法的内部状态访问，保留对可观察唤醒次序的断言后通过。
- R03 快路径 v1 在 K=4096、0% eligible 控制场景中位数增加约 34%，未保留。将快路径限定为索引已激活、waiter 数大于 128 且存在可用字节后，v2 的该控制变化为 +9.7%，单轮高值方向不一致，未稳定超过 10%；见性能汇总。
- 第一次 `align-ci.sh` 因本轮 redundant closure 失败；针对性修改后完整重跑成功，未降低 lint 门槛。
- 第一次 current-interop 命令把输出日志写在 websocket 源树中，触发自有输入哈希变化；第二次运行时 `rs-web` 原仓库在并行任务中由 `a5153f6` 前进到 `d3d89fe`，校验器正确检测到变化。把日志移到仓库外并对当前稳定源重跑后通过。`rs-web` 的并行提交不是本轮变更。

## 按用户后续指示跳过的 Cargo package 准备验证

用户明确要求忽略 `cargo package` 准备验证。以下是此前为定位行为执行的诊断命令及原始结果；这些命令失败，不记为通过，也不再作为本轮完成条件。`qubit-config` 在项目 manifest 中使用本地 `../rs-config` 路径，版本约束为 `0.15`，本地版本为 `0.15.0`。Cargo package list 和上方的清单驱动文档检查仍通过。未生成或发布 `.crate`，未检查实际 crate 解包内容和压缩体积。

| 命令 | 原始结果及诊断 |
| --- | --- |
| `cargo package --allow-dirty --no-verify` | exit 101； crates.io 当前只提供 `qubit-config` 0.14.3 及更早版本，而目标清单要求 `^0.15`；本地 `../rs-config` 源码版本是 0.15.0。没有生成 `.crate` |
| `cargo package --allow-dirty` | 同一解析失败，exit 101；见 `package-final.txt` |
| 临时在 `[patch.crates-io]` 添加本地 `qubit-config` 路径后重试 `cargo package --allow-dirty --no-verify` | 仍在 package 发布版本解析时以相同原因 exit 101；临时编辑已恢复，未纳入 diff；见 `package-local-patch-experiment.txt` |

因此无法核对真实 `.crate` 解包内容及压缩后大小。`package-list.txt` 和清单驱动的临时树链接检查只证明白名单及文档链接符合预期，不能替代实际 Cargo 打包。
