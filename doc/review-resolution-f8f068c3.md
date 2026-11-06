# WS-R01–WS-R03 实施结案记录

## 基线及变更保护

- 目标仓库实施开始时位于 `dev-starfish`，HEAD 为 `0548724629f7defb3b6db4947b3f9419b0a2965b`。其工作树有 6 份未跟踪设计/计划文档；原目录未编辑。
- 相关 `rs-web` 原仓库开始时位于 `dev-starfish`、HEAD `a5153f641d7bee308ba53937d5d3abd47c50717d`，有两份未跟踪设计/计划文件。实施期间该仓库的并行任务将 `dev`、`dev-starfish` 和 `codex/connection-diagnostics-20261105` 推进到 `d3d89fefbdfb90fc718206e3ccc32d3de2d58f06`；本轮未修改它。
- 本轮只在 Git worktree `/data/working/qubit/rust-common/.worktrees/rs-websocket-ws-f8f068c3-20261105` 操作，基于目标原分支的起始提交。另一个已有 worktree 未触碰。

## 计划目标

| 目标 | 结果 |
| --- | --- |
| WS-R01 发布包文档白名单及包内链接校验 | 已实现六项文档白名单和 checker；7 项 checker 单测通过，Cargo package list 与清单驱动的临时解包树检查通过。按用户后续指示，跳过 Cargo package 准备验证；因此不生成 `.crate`，也不报告实际解包内容或压缩体积。 |
| WS-R02 128↔129 waiter 阈值抖动核查 | 已复现索引重建/释放开销；采用启用 128、停用 64 的滞回候选。边界反复循环、回落与重新激活的确定性回归通过。三轮配对候选周期 p50 从 395.984 µs 降至 45.807 µs；普通 waiter 数控制无稳定退化。 |
| WS-R03 多 payload bucket 扫描核查 | 已复现高 K 时可容纳 bucket 扫描开销；保留最老未通知 waiter 已可容纳时的快路径，其他情况沿用既有选择。K=1024/4096 的 25%/100% eligible 目标场景改善 80–97%；K=4096、0% eligible 中位数 +9.7%，未稳定超过 10%。未引入复杂索引或依赖。 |
| T4 集成验收 | 格式、风格、默认/all-features 测试、doctest、严格 Rustdoc、覆盖率、完整 ci-check、Python 测试、双模式文档示例和当前源码互通均通过。按用户后续指示，Cargo package 准备验证不适用并跳过。 |

## 实现摘要

- `Cargo.toml` 的文档 include 从 `/doc/**` 改为六份用户文档白名单，其余既有 include 保留。
- 新增 `scripts/check-package-docs.py`，要求 README 和六份文档存在，拒绝包内历史 review/evidence、带时间戳设计及实施计划文件，扫描包内 Markdown 相对文件链接并防止路径逃逸；新增 7 项单测。
- 队列索引停用水位从 128 降至 64，启用水位仍为 128；索引在 65..128 个 waiter 时保持同步，64 及以下释放且在解锁后析构。
- 当索引活跃、waiter 数大于 128、存在空闲字节且最老未通知 waiter 可容纳时，直接通知该 waiter；否则仍执行原 bucket 选择，保持最小 waiter ID 顺序。
- 性能探针保持 ignored，不把计时断言加入共享 CI。候选对比和主机负载、第一版快路径拒绝理由、内存代价限制见 [`performance/summary.md`](evidence/review-f8f068c3/performance/summary.md)。
- 更新了队列内部注释，没有公共 API、配置默认值或架构级行为说明变化；无需修改双语 architecture 文档或下游调用方。

## 验证与独立审查

所有命令、退出状态、失败诊断和处理记录见 [`validation-commands.md`](evidence/review-f8f068c3/validation-commands.md)。最终 `ci-check.sh`、格式与风格检查、默认/all-features 测试、Rustdoc、覆盖率、Python 脚本、文档示例和互通检查通过。性能对比达到计划的收益/控制门槛；实验受共享主机并发负载影响，完整限制已记录。

最终独立审查没有实质问题。曾诊断尝试 `cargo package`，但本地路径依赖的发布准备仍会要求注册表版本；用户随后明确要求忽略 Cargo package 准备验证，因此该验证按用户调整跳过。Cargo package 清单与文档链接检查仍通过。

## 等待项和 Git 状态

`qubit-config` 依赖已配置为本地 `../rs-config` 路径及 `0.15` 版本约束，本地源码版本为 0.15.0。根据用户后续调整，本轮不要求 Cargo package 准备验证；没有在本轮发布任何 crate。

本报告记录用户调整后的验证范围。Git 提交、worktree 整合、分支同步和推送按本轮授权继续执行。起始分支、其他已有更改及本轮设计/计划文档均未覆盖。
