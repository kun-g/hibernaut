# Hibernaut 架构设计文档（Git 版本化最新基线）

本文档整合原基准设计方案与已落地的全部架构演进，作为 `kun-g/hibernaut` 仓库内唯一随代码同步演进的最新架构说明（Google Drive 原基准文档《Always-on Agent 整洁架构设计方案（Task 交接文档）》保持只读存档）。参考原文：[Building always-on agents (Anthropic)](https://claude.com/resources/articles/building-always-on-agents)。

## 1. 核心运行心智：休眠—唤醒周期

常驻型 Agent 绝大多数时间处于休眠状态（**Asleep, no tokens spent**），仅在外部事件或到期定时器到达时短暂唤醒执行一轮工作（**Run**），结束后状态落盘（**Stored state**）继续休眠。

单轮 Run 四步循环：

1. **An event arrives**：外部事件（用户消息、Webhook、定时触发、审批回复）写入全局 Event Inbox 并快速 ACK。
2. **A session starts**：加载跨 Run 持久化状态（基线 Memory + Session 断点/历史），决定新开或续接 `session_id`。
3. **The agent loop runs**：在 `PermissionPort` 门禁下执行推理、工具调用、按需读取记忆与设置后续定时器。
4. **The run writes back**：持久化新状态与记忆，产出强类型终态 `RunOutcome = Silent | Emit | Suspend` 后释放租约重新休眠。

## 2. 整洁架构分层与依赖方向（L0–L3）

依赖箭头严格由外向内单向指向：**`L2 engine/`（调用方）与 `L3 adapters/`（实现方）并列指向 `L1 core/ports.py` 与 `L0 core/models.py`**，`app/bootstrap.py` 与 `app/main.py` 为唯一依赖注入组装根（Composition Root）。

| 层级 | 路径 | 职责定位 | 依赖规则 |
| :--- | :--- | :--- | :--- |
| **L0 领域契约层** | `core/models.py` | 不可变领域数据结构与强类型终态 `RunOutcome`（`Silent` / `Emit` / `Suspend`）、`NewEventPolicy` | 零外部依赖，仅 Python 标准库 |
| **L1 核心端口层** | `core/ports.py` | 定义 9 个纯抽象 `Protocol` 端口及 `RunCapabilities` 控制钩子 | 仅依赖 Python 标准库 + L0 |
| **L2 生命周期编排层** | `engine/` | `EventHandler`、`AgentWorker`、`TimerScheduler`、`OutboxDispatcher` | 仅依赖 L0 + L1，严禁 `import adapters/` 或 `app/` |
| **L3 基础设施与组装层** | `adapters/` + `app/` | SQLite 存储、分区文件系统 Memory、Auto Mode + Credential Proxy、白盒/黑盒执行器、Webhook/Console 通道、`bootstrap.py`、`main.py` | 实现 L1 端口，仅在 `app/` 组装注入 |

### L1 全部 9 个核心端口与 RunCapabilities

| 端口 / 契约 | 职责 |
| :--- | :--- |
| `InboxPort` | 统一事件缓冲与事实源；按 `(inbox_id, source, external_id)` 幂等去重（重复标 `dropped`） |
| `TimerPort` | 持久化定时器存储（单次 `follow_up` 与周期 `schedule`） |
| `MemoryPort` | 跨 Run 目录记忆（必载基线 `instructions.md` / `index.md` / `reported.md` + 按需 `notes/*` / `progress/*`） |
| `PermissionPort` | 每次工具调用前置三态门禁（`allow` / `block` / `defer`） |
| `OutboxPort` | 待发消息持久队列，解耦推理与外部通道投递 |
| `LockPort` | 以 `inbox_id` 为主键的单活租约锁（`acquire` / `renew` / `release`），支持进程崩溃后租约到期自动接管 |
| `SessionStorePort` | 跨 Run 会话元数据与 `Suspend` 审批断点存取（支持 `find_by_request_id` 精准续接） |
| `ChannelPort` | 外部投递通道（Console、Webhook、IM 或审批交互卡片） |
| `ExecutorPort` | Agent 单轮推理边界（白盒或黑盒实现），输入批次事件与 `RunCapabilities`，输出 `RunOutcome` |
| `RunCapabilities` | 注入单轮 Run 的受控能力集合：`memory`、`timers`、`permissions`、`poll_folded_events()`、`is_interrupted()` |

## 3. 五大核心原语与运行终态

1. **Event Inbox**：所有触发源统一落盘、幂等去重后立即返回 ACK，异步唤醒 Worker；批量消费同 `session_id` 事件，Run 结束后统一 `mark_done()`。
2. **Timers**：独立于 Event Inbox 的持久化表；`TimerScheduler` 定期扫描到期项，更新状态/下次触发时间后转为标准 `Event` 写入 Event Inbox 并唤醒 Worker。
3. **Memory & Context Window**：每轮启动必载 `instructions.md`、`index.md`、`reported.md`（纳入可启用 Prompt Caching 的固定前缀）；`notes/` 与 `progress/` 运行中按需读取；长会话触发 Compact 压缩前关键状态必须写回 Memory。
4. **Permissions & Credential Proxy**：沙箱/Agent 进程零真实密钥，对外鉴权经由外置 `CredentialProxyClient` 注入；Auto Mode 分类器拦截高危调用并反馈给 Agent；高影响动作触发 `defer` 后保存断点并立刻返回 `Suspend` 放锁，人类审批结果作为新 `Event` 回到 Event Inbox 唤醒恢复。
5. **Outbox & RunOutcome**：默认静默（`Silent`）；需回报结果时经 `Emit` 写入 Outbox，由 `OutboxDispatcher` 做单 `inbox_id` 节流防刷屏与失败重试；需人工确认时输出 `Suspend`。

## 4. 多 `inbox_id` Actor 分区并发与隔离架构

采用 **一套共享底座 + 按 `inbox_id` 分区单活** 的 Actor / Partition 并发模型：同一 `inbox_id` 任意时刻最多 1 个活跃 Worker，不同 `inbox_id` 之间由 `AsyncWorkerPool` 完全并行执行。

- **三种分区粒度天然共存**：个人助理（`user:main`）、多项目并行（`proj:<name>`）、同项目多任务（`<project>:<issue>`）。
- **并发红线**：**严禁两个不同 `inbox_id` 同时挂载同一个本地代码目录**；`WorkspacePartitionRegistry.register()` 在注册时强制校验解析后路径唯一性，冲突即抛 `ValueError`（同项目拆任务分区必须绑定独立 `git worktree`）。

| 架构层 / 组件 | 部署形态 | 多项目并发方式 |
| :--- | :--- | :--- |
| Inbox / Timers / Outbox / Leases / Sessions | 全局共享 1 套数据库（SQLite WAL / Postgres） | 核心表按 `inbox_id` 逻辑分区，`worker_leases` 主键即 `inbox_id` |
| Handler / Scheduler / Dispatcher | 全局共享 1 套常驻服务 | 统一接入、定时扫描与节流投递，按 `inbox_id` 唤醒路由 |
| Permissions & Credential Proxy | 全局共享 1 套安全网关 | 集中策略与外置密钥注入，进程内零真实密钥 |
| `AsyncWorkerPool` & `AlwaysOnDaemon` (`app/main.py`) | 共享异步 Worker 池 | 跨 `inbox_id` 并行持锁执行；同 `inbox_id` 单活互斥；`Suspend` 立即放锁不阻塞 |
| `PartitionedMemoryAdapter` & 代码工作区 | 按 `inbox_id` 强隔离 | 独立项目记忆目录 `memory/projects/<id>/` 与独立代码工作目录；全局 `global/instructions.md` 只读共享 |

## 5. 运行中新事件到达策略（`NewEventPolicy`）

在 `AgentWorker`（及 `app/main.py --policy`）中支持三种策略：

| 策略 | 行为约定 | 适用场景 |
| :--- | :--- | :--- |
| `defer`（默认） | 当前 Run 正常跑完并 `mark_done()`，随后在同一次持锁循环内拉取新事件开启下一轮 | 通用稳态场景（默认推荐） |
| `fold` | 在当前 Run 的 LLM/工具回合间隙通过 `caps.poll_folded_events()` 探测同 `inbox_id` + 同 `session_id` 的新 `pending` 事件，增量注入当前上下文继续推理，本轮结束后与原批次一并 `mark_done()` | 长推理/多步工具循环内吸收增量补充信息 |
| `interrupt` | 同一 `inbox_id` 到达新事件时立即触发取消信号中止当前 Run；被中止的 Run **绝不向 Outbox 写入半成品消息**，原批次保持 `pending` 不标 `done`，Worker 在同一租约内将原批次与新事件合并后重启新一轮 Run | 高优先级用户实时纠偏或急停 |

## 6. 执行器选型与三道防腐化门禁

- **白盒执行器（当前默认 `adapters/executor_whitebox/`）**：框架管理 Tool-use 循环、Prompt Cache 前缀、Compact 钩子、权限拦截与 `fold`/`interrupt` 控制点，只需注入任意 `LLMBackend` 即可对接 Claude / Gemini / OpenAI。
- **黑盒执行器（可随时插拔 `adapters/executor_blackbox/`）**：实现 `ExecutorPort` 并将外部 Agent CLI/SDK 的 Hook 或 MCP 桥接到 `RunCapabilities`，`core/` 与 `engine/` 零改动。
- **三道自动化门禁**（本地提交前与 `.github/workflows/ci.yml` 强制执行）：
  1. `lint-imports`：验证 `.importlinter` 分层与反向依赖禁令。
  2. `pytest -v`：运行 AST 静态依赖检查、In-Memory / SQLite / PartitionedMemory 契约测试、并发隔离与 `NewEventPolicy` 端到端测试。
  3. `pyright`：对 `core/` 与 `engine/` 执行 strict 静态类型检查（含 `RunOutcome` `assert_never` 穷尽检查）。

## 7. 核心词汇表（Glossary）

| 术语 | 精确定义 | 范围 / 约束 |
| :--- | :--- | :--- |
| **Event Inbox** | 全局共享的物理事件表（如 SQLite `event_inbox`） | 全局 1 套，存放所有 `inbox_id` 的事件 |
| **Inbox** | Event Inbox 内的逻辑队列与并发隔离域（Actor Mailbox） | 按需划分（1 至 N 个），同分区串行、跨分区并发 |
| **`inbox_id`** | 标识单个逻辑 Inbox 的一等路由键与 `worker_leases` 主键 | 决定并发锁边界、项目 Memory 与代码工作区隔离 |
| **Event** | 进入 Event Inbox 的不可变输入信封，状态为 `pending` / `done` / `dropped` | 必属某个 `inbox_id`，可附带 `session_id` |
| **`session_id`** | 隶属于某个 `inbox_id` 的跨 Run 对话转录与 `Suspend` 断点上下文单元 | 一个 `inbox_id` 下可有多个 `session_id` |
| **Run / Lease** | 单次唤醒执行的推理循环 / 以 `inbox_id` 为主键的限时互斥租约 | `Suspend` 立即释放租约；进程崩溃后租约到期自动接管 |
