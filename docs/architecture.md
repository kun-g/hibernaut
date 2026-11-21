# Hibernaut 架构设计文档（Git 版本化最新基线）

本文档整合原基准设计方案与已落地的全部架构演进，作为 `kun-g/hibernaut` 仓库内唯一随代码同步演进的最新架构说明（Google Drive 原基准文档《Always-on Agent 整洁架构设计方案（Task 交接文档）》保持只读存档）。参考原文：[Building always-on agents (Anthropic)](https://claude.com/resources/articles/building-always-on-agents)。

## 1. 核心运行心智：休眠—唤醒周期

常驻型 Agent 绝大多数时间处于休眠状态（**Asleep, no tokens spent**），仅在外部事件或到期定时器到达时短暂唤醒执行一轮工作（**Run**），结束后状态落盘（**Stored state**）继续休眠。

单轮 Run 四步循环：

1. **An event arrives**：外部事件（用户消息、GitHub/飞书 Webhook、GitHub Issue 轮询、MCP 控制面调用、定时触发、审批回复）写入全局 Event Inbox 并快速 ACK。
2. **A session starts**：按 4 级优先级解析会话（`approval_request_id` → 显式 `session_id` → `(inbox_id, correlation_key)` → 新建），加载跨 Run 持久化状态（基线 Memory + Session 断点/历史）。
3. **The agent loop runs**：在 `PermissionPort` 门禁下执行推理、工具调用或本地 CLI 子进程，按需读取记忆与设置后续定时器。
4. **The run writes back**：持久化新状态、记忆与 `(inbox_id, correlation_key) → session_id` 映射，产出强类型终态 `RunOutcome = Silent | Emit | Suspend` 后释放租约重新休眠。

## 2. 整洁架构分层与依赖方向（L0–L3）

依赖箭头严格由外向内单向指向：**`L2 engine/`（调用方）与 `L3 adapters/`（实现方）并列指向 `L1 core/ports.py` 与 `L0 core/models.py`**，`app/bootstrap.py` 与 `app/main.py` 为唯一依赖注入组装根（Composition Root）。

| 层级 | 路径 | 职责定位 | 依赖规则 |
| :--- | :--- | :--- | :--- |
| **L0 领域契约层** | `core/models.py` | 不可变领域数据结构（含 `Event.correlation_key`、`SessionRef.correlation_key`）与强类型终态 `RunOutcome`、`NewEventPolicy` | 零外部依赖，仅 Python 标准库 |
| **L1 核心端口层** | `core/ports.py` | 定义 9 个纯抽象 `Protocol` 端口及 `RunCapabilities` 控制钩子 | 仅依赖 Python 标准库 + L0 |
| **L2 生命周期编排层** | `engine/` | `EventHandler`、`AgentWorker`、`TimerScheduler`、`OutboxDispatcher` | 仅依赖 L0 + L1，严禁 `import adapters/` 或 `app/` |
| **L3 基础设施与组装层** | `adapters/` + `app/` | SQLite 存储（含 `workspaces` 表）、分区 Memory、Auto Mode + Credential Proxy、白盒执行器、`LocalCLIExecutor` + `OrcaWorktreeManager`、GitHub/飞书入站与轮询、`MCPServerAdapter`、飞书出站通道、`config.py`、`bootstrap.py`、`main.py` | 实现 L1 端口或驱动 L2 `EventHandler`，仅在 `app/` 组装注入 |

### L1 全部 9 个核心端口与 RunCapabilities

| 端口 / 契约 | 职责 |
| :--- | :--- |
| `InboxPort` | 统一事件缓冲与事实源；按 `(inbox_id, source, external_id)` 幂等去重（重复标 `dropped`） |
| `TimerPort` | 持久化定时器存储（单次 `follow_up` 与周期 `schedule`） |
| `MemoryPort` | 跨 Run 目录记忆（必载基线 `instructions.md` / `index.md` / `reported.md` + 按需 `notes/*` / `progress/*`） |
| `PermissionPort` | 每次工具调用前置三态门禁（`allow` / `block` / `defer`） |
| `OutboxPort` | 待发消息持久队列，解耦推理与外部通道投递 |
| `LockPort` | 以 `inbox_id` 为主键的单活租约锁（`acquire` / `renew` / `release`），支持进程崩溃后租约到期自动接管 |
| `SessionStorePort` | 跨 Run 会话元数据、`Suspend` 审批断点（`find_by_request_id`）及外部关联键映射（`find_by_correlation_key`，支持会话轮换重绑） |
| `ChannelPort` | 外部投递通道（`FeishuChannelAdapter`、`ConsoleChannelAdapter`、`WebhookChannelAdapter`） |
| `ExecutorPort` | Agent 单轮推理边界（白盒 `WhiteboxExecutor` 或黑盒 `LocalCLIExecutor`），输入批次事件与 `RunCapabilities`，输出 `RunOutcome` |
| `RunCapabilities` | 注入单轮 Run 的受控能力集合：`memory`、`timers`、`permissions`、`poll_folded_events()`、`is_interrupted()` |

## 3. 外部关联键 `correlation_key` 与 L3 入站/出站通道

- **4 级会话解析优先级**（`AgentWorker._resolve_session()`）：`approval_request_id` → 显式 `session_id` → `(inbox_id, correlation_key)` → 新建 `session_id`。
- **同键批处理与会话轮换重绑**：`_group_into_batches()` 按解析后的键（`session_id` → `correlation_key`）稳定分组，同键全部 `pending` 事件并入一批（批内保持 `seq` 顺序），批次按各组首条事件的 `seq` 执行。无键事件彼此无关，`defer`/`fold` 下各自单独成批且不被 `fold` 注入，仅 `interrupt` 重启时合并。本键当前批次遇到两种情况会结束：带 `approval_request_id` 的审批事件（同键更早的事件与它同批，挂起会话会把不含审批结果的 Run 视为拒绝），或执行目标（payload 的 `explicit_workdir`、`executor`）与本批不同的事件。同键后续事件留到下一轮重新解析会话后再执行。快照里某键有待处理的审批时，该键的第一批必须带上审批：审批前执行目标不兼容的事件暂缓到审批之后，不提前结束批次。`LocalCLIExecutor` 的工作目录和模板取自批内普通事件，只有审批单独成批时才用审批自带的目标。`fold` 只注入同键、同执行目标的事件，遇到同键审批事件即停止，含审批的批次不注入；`interrupt` 以本轮调度快照判断新事件，含审批的批次不可中断（避免重复执行已批准的工具）；重置开新 Session 时向 `SessionStorePort.save()` 写入同一 `(inbox_id, correlation_key)` 自动解绑旧会话并指向新活跃会话。
- **L3 入站驱动适配器**（直接调用 `EventHandler.ingest()`）：
  - `POST /events`：通用 JSON 事件接入（202 ACK）。
  - `POST /webhooks/github`（`GitHubWebhookAdapter`）：HMAC-SHA256 `X-Hub-Signature-256` 验签、`X-GitHub-Delivery` 幂等键、`repo -> inbox_id` 映射、Issue/PR 自动提取 `correlation_key = f"github:{repo}#{number}"`。
  - `GitHubIssuePoller`（`adapters/channels/github_poller.py`）：无公网 Webhook 时的本机定时轮询入口（支持 `gh issue list` 或 GitHub REST API 及 label 过滤），生成 `external_id = f"github-poll:{repo}#{number}:{updated_at}"` 与完全一致的 `correlation_key = f"github:{repo}#{number}"`。
  - `POST /webhooks/feishu`（`FeishuWebhookAdapter`）：支持飞书 v2.0 `url_verification` Challenge 握手（200 OK）、`verification_token` 校验、`header.event_id` 幂等键、话题 `correlation_key` 提取与审批卡片回调恢复 `Suspend`。
  - `MCPServerAdapter`（`adapters/channels/mcp_server.py`）：纯标准库 MCP JSON-RPC 2.0 入站控制面（`initialize`、`notifications/initialized`、`ping`、`tools/list`、`tools/call`），暴露 5 个最小核心 Tool（`hibernaut_ingest_event`、`hibernaut_list_sessions`、`hibernaut_list_workspaces`、`hibernaut_resolve_approval`、`hibernaut_reclaim_workspace`），同时支持本机 `--mcp-stdio` 行帧直连与远程 `POST /mcp` Streamable HTTP（强制 `Authorization: Bearer <HIBERNAUT_MCP_TOKEN>` 校验，未配置 token 时默认禁用返回 404）。
- **L3 飞书出站通道**（`FeishuChannelAdapter`，`adapters/channels/feishu_channel.py`）：仅用标准库 `urllib.request`，基于 `FEISHU_APP_ID` / `FEISHU_APP_SECRET` 自动缓存刷新 `tenant_access_token`，发送普通文本通知与 `Suspend` 交互审批卡片；未配置凭据时自动降级为 `ConsoleChannelAdapter`。

## 4. 本机自举闭环：`OrcaWorktreeManager`、`LocalCLIExecutor` 与声明式配置

- **极简可读 Worktree 命名**（不含 repo 名与哈希）：GitHub Issue `#N` → `hb-issue-<N>` / `hb/issue-<N>`；PR `#N` → `hb-pr-<N>` / `hb/pr-<N>`；其他任务 → `hb-task-<seq>` / `hb/task-<seq>`；同仓库重名自动顺延 `-2`。
- **工作区决策优先级**：`explicit_workdir`（只读覆盖口）→ SQLite `workspaces` 表已落盘路径（按 `correlation_key` 复用）→ 动态创建（默认 `orca worktree create`，可选 `orca terminal create`，支持切换纯 `git worktree add` 后端）。
- **三道安全保险**：
  1. **意图占位与对账**：创建前先在 SQLite `workspaces` 表写入 `creating` 占位，崩溃重入时先对账本地目录再决定复用或清理。
  2. **路径落盘**：创建后按主仓库和分支查询 `git worktree list --porcelain -z`，确认唯一且可用的独立工作区后再标记 `ready`。复用时用同一登记表校正 SQLite 的旧路径。执行器重试耗尽后，Worker 池记录 inbox 和异常，保留该批次为 `pending`，继续处理其他 inbox。同 inbox 下次唤醒时重试。
  3. **三关安全回收**（`reclaim()`）：双重归属校验（`hb-` 前缀 + `workspaces` 表记录）+ 三关检查（无开着 PR / 无未提交改动 / 无未推送提交），任一不通过即保留目录并经 Outbox 告警。
- **可配置本地 CLI 执行器**（`LocalCLIExecutor`）：实现 `ExecutorPort`，内置 `claude`（默认）、`cursor-agent`、`grok`、`dsh` 模板，支持透传环境变量对接本地模型网关（如 `yetone/magpie`），响应 `caps.is_interrupted()` 安全终止子进程。
- **声明式配置**（`hibernaut.example.toml` + `app/config.py` + `app/main.py --config`）：使用标准库 `tomllib` 解析，仅存静态主检出、执行器模板与凭据环境变量名，绝不存动态 worktree 路径。HTTP daemon 热加载时先组装新配置，待旧 Worker 池排空后再切换，沿用同一条 SQLite 连接及未变更的监听 socket。

## 5. 多 `inbox_id` Actor 分区并发与 `NewEventPolicy`

- **多 `inbox_id` 并发**：`AsyncWorkerPool` 跨 `inbox_id` 并行、同 `inbox_id` 单活互斥（`worker_leases` 主键即 `inbox_id`）、`Suspend` 立即放锁；`WorkspacePartitionRegistry` 强制禁止不同 `inbox_id` 挂载同一本地代码目录，全局 `global/instructions.md` 只读共享。
- **HTTP 入站与 SQLite 并发**：`WebhookHTTPServer`（`ThreadingHTTPServer`，`request_queue_size=128`）每请求一线程；`shutdown()` 等待在途请求全部写完 Inbox 后返回，Worker 停止与配置热加载据此排空。`SQLiteDatabase` 只有一条连接，所有语句经同一把可重入锁串行；写操作走 `transaction()`（`BEGIN IMMEDIATE`），去重、租约等先读后写在线程间和共享同一文件的进程间保持原子；租约时钟在拿到写锁后读取，等锁时间不计入 TTL；`mark_done()` 按 SQLite 参数上限分段更新；文件库开启 WAL 与 `busy_timeout`。回收 worktree 时，`claim_for_reclaim()` 以读到的 `updated_at` 做比较并交换：进行中的 `reclaiming` 认领是排他的，只有超过 `reclaim_stale_after_seconds`（默认 600 秒，认领方已崩溃）的认领才可被接管；删除期间心跳线程定期续期认领，慢删除不会被当作过期而被接管；删除成功后 `finish_reclaim()` 同样按认领做比较并交换，认领已失效时不删除被他人重新登记的记录；删除失败时 `release_reclaim()` 恢复原状态，可直接重试。
- **`NewEventPolicy` 三策略**：`defer`（默认，推迟到下一轮）、`fold`（回合间隙通过 `poll_folded_events()` 增量注入同会话/同关联键事件并统一 `mark_done()`）、`interrupt`（安全取消当前 Run/CLI 子进程，绝不向 Outbox 写半成品，原批次保持 `pending` 与新事件同租约合并重启）。

## 6. 三道防腐化门禁与核心词汇表（Glossary）

提交前与 `.github/workflows/ci.yml` 强制执行：`lint-imports`、`pytest -v`、`pyright`（strict）。

| 术语 | 精确定义 | 范围 / 约束 |
| :--- | :--- | :--- |
| **Event Inbox** | 全局共享的物理事件表（SQLite `event_inbox`） | 全局 1 套，存放所有 `inbox_id` 的事件 |
| **Inbox / `inbox_id`** | 逻辑并发队列（Actor Mailbox）及其一等路由键 / `worker_leases` 主键 | 同分区串行、跨分区并发；禁跨 `inbox_id` 共享同一本地代码目录 |
| **Event** | 进入 Event Inbox 的不可变输入信封（`pending` / `done` / `dropped`） | 必属某个 `inbox_id`，可附带 `session_id` 或 `correlation_key` |
| **`session_id`** | 隶属于某个 `inbox_id` 的跨 Run 对话转录与 `Suspend` 断点上下文单元 | 一个 `inbox_id` 下可有多个 `session_id` |
| **`correlation_key`** | 外部业务实体的确定性关联键（如 `github:kun-g/hibernaut#42`、`feishu:<chat>:<thread>`） | 经 `SessionStorePort.find_by_correlation_key()` 映射到当前活跃 `session_id`，同时作为 `workspaces` 表动态 worktree 复用键 |
| **Run / Lease** | 单次唤醒执行周期 / 以 `inbox_id` 为主键的限时互斥租约 | `Suspend` 立即释放租约；进程崩溃后租约到期自动接管 |
