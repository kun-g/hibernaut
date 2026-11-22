# Hibernaut 架构设计文档（Git 版本化最新基线）

本文档整合原基准设计方案与已落地的全部架构演进，作为 `kun-g/hibernaut` 仓库内唯一随代码同步演进的最新架构说明（Google Drive 原基准文档《Always-on Agent 整洁架构设计方案（Task 交接文档）》保持只读存档）。参考原文：[Building always-on agents (Anthropic)](https://claude.com/resources/articles/building-always-on-agents)。

## 1. 核心运行心智：休眠—唤醒周期

常驻型 Agent 绝大多数时间处于休眠状态（**Asleep, no tokens spent**），仅在外部事件或到期定时器到达时短暂唤醒执行一轮工作（**Run**），结束后状态落盘（**Stored state**）继续休眠。

单轮 Run 四步循环：

1. **An event arrives**：外部事件（用户消息、GitHub/飞书 Webhook、GitHub Issue 轮询、MCP 控制面调用、定时触发、审批回复）写入全局 Event Inbox 并快速 ACK。
2. **A session starts**：按 4 级优先级解析会话（`approval_request_id` → 显式 `session_id` → `(inbox_id, correlation_key)` → 新建），加载跨 Run 持久化状态（基线 Memory + Session 断点/历史及绑定的 `executor_id` / `executor_session_id`），并按 4 级粘滞路由（`resolve_executor_id()`）选定目标 `ExecutorPort`。
3. **The agent loop runs**：在 `PermissionPort` 门禁下执行推理、工具调用或本地 CLI 子进程，按需读取记忆与设置后续定时器。
4. **The run writes back**：持久化新状态、记忆、`(inbox_id, correlation_key) → session_id` 映射以及执行器绑定（`selected_executor.executor_id`、`outcome.executor_session_id`），产出强类型终态 `RunOutcome = Silent | Emit | Suspend` 后释放租约重新休眠。

## 2. 整洁架构分层与依赖方向（L0–L3）

依赖箭头严格由外向内单向指向：**`L2 engine/`（调用方）与 `L3 adapters/`（实现方）并列指向 `L1 core/ports.py` 与 `L0 core/models.py`**，`app/bootstrap.py` 与 `app/main.py` 为唯一依赖注入组装根（Composition Root）。

| 层级 | 路径 | 职责定位 | 依赖规则 |
| :--- | :--- | :--- | :--- |
| **L0 领域契约层** | `core/models.py` | 不可变领域数据结构（含 `Event.correlation_key`、`SessionRef.correlation_key / executor_id / executor_session_id`）、强类型终态 `RunOutcome`（仅携带 `executor_session_id`）、`NewEventPolicy` 与 4 级粘滞路由纯函数 `resolve_executor_id()` | 零外部依赖，仅 Python 标准库 |
| **L1 核心端口层** | `core/ports.py` | 定义 9 个纯抽象 `Protocol` 端口（含 `ExecutorPort.executor_id`）及 `RunCapabilities` 控制钩子 | 仅依赖 Python 标准库 + L0 |
| **L2 生命周期编排层** | `engine/` | `EventHandler`、`AgentWorker`（支持 `executors: Mapping[str, ExecutorPort]` 查表路由）、`TimerScheduler`、`OutboxDispatcher` | 仅依赖 L0 + L1，严禁 `import adapters/` 或 `app/` |
| **L3 基础设施与组装层** | `adapters/` + `app/` | SQLite 存储（含 `workspaces` 表与旧库兼容迁移）、分区 Memory、Auto Mode + Credential Proxy、白盒执行器、`LocalCLIExecutor` + `OrcaWorktreeManager`、GitHub/飞书入站与轮询、`MCPServerAdapter`、飞书出站通道、`config.py`、`bootstrap.py`、`main.py` | 实现 L1 端口或驱动 L2 `EventHandler`，仅在 `app/` 组装注入 |

### L1 全部 9 个核心端口与 RunCapabilities

| 端口 / 契约 | 职责 |
| :--- | :--- |
| `InboxPort` | 统一事件缓冲与事实源（4 态生命周期 `pending` / `processing` / `done` / `failed`，重复幂等标 `dropped`）；支持 `claim_pending`、`fetch_processing`、`bind_session`、`requeue_processing`、`mark_done`、`mark_failed` |
| `TimerPort` | 持久化定时器存储（单次 `follow_up` 与周期 `schedule`） |
| `MemoryPort` | 跨 Run 目录记忆（必载基线 `instructions.md` / `index.md` / `reported.md` + 按需 `notes/*` / `progress/*`） |
| `PermissionPort` | 每次工具调用前置三态门禁（`allow` / `block` / `defer`） |
| `OutboxPort` | 待发消息持久队列，解耦推理与外部通道投递 |
| `LockPort` | 以 `inbox_id` 为主键的单活租约锁（`acquire` / `renew` / `release`），支持进程崩溃后租约到期自动接管 |
| `SessionStorePort` | 跨 Run 会话元数据（4 态 `active` / `suspended` / `archived` / `closed`、`work_key`）、执行器绑定（`executor_id` / `executor_session_id`）、`Suspend` 审批断点（`find_by_request_id`）、活跃引用计数（`count_active_by_work_key`）、`archive_session` / `close_session` 及外部关联键映射（`find_by_correlation_key` 仅命中 `active`/`suspended`） |
| `ChannelPort` | 外部投递通道（`FeishuChannelAdapter`、`ConsoleChannelAdapter`、`WebhookChannelAdapter`） |
| `ExecutorPort` | Agent 单轮推理边界（声明只读属性 `executor_id`；白盒 `WhiteboxExecutor` 或黑盒 `LocalCLIExecutor`），输入批次事件与 `RunCapabilities`，输出 `RunOutcome` |
| `RunCapabilities` | 注入单轮 Run 的受控能力集合：`memory`、`timers`、`permissions`、`poll_folded_events()`、`is_interrupted()` |

## 3. 外部关联键 `correlation_key`、多执行器粘滞路由与 L3 通道

- **4 级会话解析优先级**（`AgentWorker._resolve_session()`）：`approval_request_id` → 显式 `session_id` → `(inbox_id, correlation_key)` → 新建 `session_id`。
- **同键批处理与会话轮换重绑**：`_group_into_batches()` 按解析后的键（`session_id` → `correlation_key`）稳定分组，同键全部 `pending` 事件并入一批（批内保持 `seq` 顺序），批次按各组首条事件的 `seq` 执行。无键事件彼此无关，`defer`/`fold` 下各自单独成批且不被 `fold` 注入，仅 `interrupt` 重启时合并。本键当前批次遇到两种情况会结束：带 `approval_request_id` 的审批事件（同键更早的事件与它同批，挂起会话会把不含审批结果的 Run 视为拒绝），或执行目标（payload 的 `executor_id`、`explicit_workdir`、`executor`）与本批不同的事件。同键后续事件留到下一轮重新解析会话后再执行。快照里某键有待处理的审批时，该键的第一批必须带上审批：审批前执行目标不兼容的事件暂缓到审批之后，不提前结束批次。`AgentWorker._select_executor()` 选择执行器、`LocalCLIExecutor` 选择工作目录和模板，都取自批内普通事件，只有审批单独成批时才用审批自带的目标。审批命中挂起会话的 `pending_request_id` 时，该批固定交给会话绑定的 `executor_id`（持有断点的执行器）处理；执行器绑定上线前挂起的旧会话没有 `executor_id`，当前默认执行器不能证明归属（旧版 CLI 已可按事件选模板），只接受审批事件显式指定的 `executor_id`（唯一注册的执行器也可能是替代者，不作为依据）；无法判定或持有者未注册时本批报 `ExecutorFailed`，不确认事件、不清断点，补写 `sessions.executor_id` 后原审批即可恢复；审批待处理期间，同会话里指定其他 `executor_id` 的普通事件留到审批之后执行。`fold` 只注入同键、同执行目标的事件，遇到同键审批事件即停止，含审批的批次不注入；`interrupt` 以本轮调度快照判断新事件，含审批的批次不可中断（避免重复执行已批准的工具）；重置开新 Session 时向 `SessionStorePort.save()` 写入同一 `(inbox_id, correlation_key)` 自动解绑旧会话并指向新活跃会话。
- **多执行器粘滞路由与原生会话续接闭环**：
  - `resolve_executor_id()` 按 4 级优先级决定目标执行器：① 事件显式指定 `event.payload.get("executor_id")`（允许主动切换）→ ② 已有会话的 `session.executor_id`（默认粘滞，避免后续事件不指定时静默回退 `default_executor`）→ ③ 项目级默认 `projects.<name>.executor`（可选）→ ④ 全局 `runtime.default_executor`。
  - `AgentWorker` 持有全量注册的 `executors: Mapping[str, ExecutorPort]`，每轮查表执行后统一将 `selected_executor.executor_id` 与 `outcome.executor_session_id` 写入 `SessionStorePort`；`RunOutcome` 保持简洁不携带 `executor_id`。
  - `LocalCLIExecutor` 仅在 `session.executor_id == template.name` 且 `session.executor_session_id` 非空并配置了 `resume_args` 时启用原生会话续接；切换执行器时自动回退全新 `args`，绝不跨执行器错传原生会话 ID。
- **L3 入站驱动适配器**（直接调用 `EventHandler.ingest()`）：
  - `POST /events`：通用 JSON 事件接入（202 ACK）。
  - `POST /webhooks/github`（`GitHubWebhookAdapter`）：HMAC-SHA256 `X-Hub-Signature-256` 验签、`X-GitHub-Delivery` 幂等键、`repo -> inbox_id` 映射、Issue/PR 自动提取 `correlation_key = f"github:{repo}#{number}"`。
  - `GitHubIssuePoller`（`adapters/channels/github_poller.py`）：无公网 Webhook 时的本机定时轮询入口（支持 `gh issue list` 或 GitHub REST API 及 label 过滤），生成 `external_id = f"github-poll:{repo}#{number}:{updated_at}"` 与完全一致的 `correlation_key = f"github:{repo}#{number}"`。
  - `POST /webhooks/feishu`（`FeishuWebhookAdapter`）：支持飞书 v2.0 `url_verification` Challenge 握手（200 OK）、`verification_token` 校验、`header.event_id` 幂等键、话题 `correlation_key` 提取与审批卡片回调恢复 `Suspend`。
  - `MCPServerAdapter`（`adapters/channels/mcp_server.py`）：纯标准库 MCP JSON-RPC 2.0 入站控制面（`initialize`、`notifications/initialized`、`ping`、`tools/list`、`tools/call`），暴露 7 个核心 Tool（`hibernaut_ingest_event`、`hibernaut_list_sessions`（含 `status` / `work_key` / `executor_id` / `executor_session_id`）、`hibernaut_list_events`（含 `session_id` / `status` / `attempts` / `last_error`）、`hibernaut_list_workspaces`、`hibernaut_resolve_approval`、`hibernaut_reclaim_workspace`、`hibernaut_close_session`），同时支持本机 `--mcp-stdio` 行帧直连与远程 `POST /mcp` Streamable HTTP（强制 `Authorization: Bearer <HIBERNAUT_MCP_TOKEN>` 校验，未配置 token 时默认禁用返回 404）。
- **L3 飞书出站通道**（`FeishuChannelAdapter`，`adapters/channels/feishu_channel.py`）：仅用标准库 `urllib.request`，基于 `FEISHU_APP_ID` / `FEISHU_APP_SECRET` 自动缓存刷新 `tenant_access_token`，发送普通文本通知与 `Suspend` 交互审批卡片；未配置凭据时自动降级为 `ConsoleChannelAdapter`。

## 4. 本机自举闭环：`OrcaWorktreeManager`、`LocalCLIExecutor` 与声明式配置

- **极简可读 Worktree 命名**（不含 repo 名与哈希）：GitHub Issue `#N` → `hb-issue-<N>` / `hb/issue-<N>`；PR `#N` → `hb-pr-<N>` / `hb/pr-<N>`；其他任务 → `hb-task-<seq>` / `hb/task-<seq>`；同仓库重名自动顺延 `-2`。
- **工作区决策优先级**：`explicit_workdir`（只读覆盖口）→ SQLite `workspaces` 表已落盘路径（按 `correlation_key` 复用）→ 动态创建（默认 `orca worktree create`，可选 `orca terminal create`，支持切换纯 `git worktree add` 后端）。
- **三道安全保险**：
  1. **意图占位与对账**：创建前先在 SQLite `workspaces` 表写入 `creating` 占位，崩溃重入时先对账本地目录再决定复用或清理。
  2. **路径落盘**：分配名称时检查所有本地 `hb/` 分支，包括未挂载的分支。Orca 创建时可能再加数字后缀，执行器读取 `--json` 回执里的实际分支和路径，与主仓库的 `git worktree list --porcelain -z` 核对，确认是唯一且可用的独立工作区后登记为 `ready`。复用时用同一登记表校正 SQLite 的旧路径。执行器重试耗尽后，批次标记为 `failed` 并记录 `last_error`，Worker 池记录异常后继续处理其他 inbox。
  3. **引用计数前置门禁 + 三关安全回收**（`reclaim()`）：先校验 `count_active_by_work_key(work_key) == 0`（若有 `active`/`suspended` 会话仍引用则拒绝回收并返回占用会话 ID），再执行双重归属校验（`hb-` 前缀 + `workspaces` 表记录）+ 三关检查（无开着 PR / 无未提交改动 / 无未推送提交），任一不通过即保留目录并经 Outbox 告警。
- **可配置本地 CLI 执行器**（`LocalCLIExecutor`）：实现 `ExecutorPort`，内置 `claude`（默认）、`cursor-agent`、`grok`、`dsh` 模板，支持可选 `resume_args` 原生会话续接（自动从 JSON 输出提取 `executor_session_id` / `session_id`）及透传环境变量对接本地模型网关（如 `yetone/magpie`），响应 `caps.is_interrupted()` 安全终止子进程。
- **Claude Code 共用启动适配器**（`adapters/executor_cli/claude_code.py`）：各角色（评审与 SDE）共用的启动配置与调用。`[launchers.<name>]` 声明可信启动器（`harness`、`command`、非凭据 `env`；内置 `claude`），只能来自部署配置，事件无法指定；启动器可自管配置目录、账号、网关或代理（如 `claude-sg`），原样接收参数。`ClaudeCodeProfile` 把角色以外的五项分开：`harness`、`launcher`、`billing`、`model`、`effort`（另有 `timeout_seconds`、`env`），构造时校验，启动器 harness 必须与角色一致。`harness_args()` 生成 `-p --output-format stream-json --verbose --model --effort --permission-mode <角色指定>` 加角色参数，权限模式必须显式传入，避免启动器的默认 `bypassPermissions` 生效。`child_env()` 统一剥离付费凭据、`CLAUDE_CODE_OAUTH_TOKEN`、`ANTHROPIC_BASE_URL` 与云厂商开关，叠加启动器与角色 `env`；`api` 计费把 `ANTHROPIC_BASE_URL` 指向 `[credential_proxy] url` 的 `/proxy/<anthropic_service>`，只给占位 key，真实 key 由外置凭据代理注入（与 `CredentialProxyClient` 共用 `proxy_service_url()`）。`run_claude_code()` 经 stdin 投递 prompt、读 stream-json、在 `init` 消息校验 `apiKeySource`（订阅须为 `none`，api 须为代理占位的 `ANTHROPIC_API_KEY`）与模型，并经 `process.supervise()` 处理中断与超时；`LocalCLIExecutor` 共用同一个 `supervise()`/`terminate()` 与事件取值工具（`process.py`）。
- **独立评审执行器**（`CoderReviewerExecutor`，`adapters/executor_cli/reviewer.py`）：`[executors.<id>]` 写 `role = "coder-reviewer"` 时注册为评审角色（`HibernautConfig.roles`），派单用 `payload.executor_id = "<id>"`。启动配置见上条共用适配器，默认 `claude-code / claude / subscription / claude-opus-5-5 / high`；非法值、`env` 中的凭据或计费开关、未声明的启动器、不适用的模板字段（`command`、`args`、`orca_agent` 等）均报 `ValueError`；热加载失败时打印原因并保留旧配置，成功后下一次评审即用新值（含切换启动器）。
  - **输入**：任务要求（`requirements` 或 `text`）、测试证据（`test_evidence`）、代码差异（执行器自己计算：`base_ref`，缺省依次尝试 `origin/HEAD`、`main` 的 merge-base 到工作区的 diff、提交列表与未跟踪文件，超 20 万字符截断）。缺要求、找不到基准或无改动直接报错，不启动模型。
  - **工作区**：`explicit_workdir` → `review_of`（开发任务的 `correlation_key`）或本事件 `correlation_key` 在 `workspaces` 表中 `ready` 的路径；评审从不创建 worktree。建议评审用独立的 `correlation_key`（如 `review:<key>`），避免开发会话的执行器粘滞绑定被切到评审者。
  - **只读**：`-p --permission-mode dontAsk`（经所选启动器），只放行 Read/Grep/Glob 与只读 git 命令，禁用 Edit/Write 类工具；运行前后用临时 index 对工作区（HEAD + 含未跟踪文件的树）取指纹，不一致则丢弃评审结论并报错，不回滚。
  - **计费**：见共用适配器；`subscription` 用启动器自己的登录，`init` 报告任何付费来源即在会话开始时终止；`api` 只经外置凭据代理，未配置 `[credential_proxy]` 时解析即报错。`init` 报告的模型与配置全名不符同样终止。
  - **输出**：`--json-schema` 约束结构化结论，`verdict = pass | changes_requested`；阻塞项必含 `location`（文件:行）、`trigger`、`impact`，结论与阻塞项数量矛盾视为无效。结果作为一条 `Emit` 写入 Outbox；每次评审都是新会话（`--no-session-persistence`，不回填 `executor_session_id`），开发者修复后重新派单即为独立复审。权限门禁工具名 `coder_reviewer.launch`。
- **SDE 执行器**（`SDEExecutor`，`adapters/executor_cli/sde.py`）：`[executors.<id>]` 写 `role = "sde"` 时注册，派单用 `payload.executor_id = "<id>"`。启动配置同共用适配器，默认 `claude-code / claude / subscription / claude-opus-5-5 / high`；另有 `review`（布尔，默认 `true`，非布尔报错）、`reviewer`（默认 `coder-reviewer`，`review = true` 时必须指向评审角色，否则解析报错）、`max_review_rounds`（正整数，默认 3）。热加载重建执行器，下一次运行使用新值。
  - **工作区**：同 `LocalCLIExecutor`：`explicit_workdir` → `workspaces` 表按 `correlation_key` 复用 → 动态创建。
  - **开发**：`--permission-mode dontAsk`，放行 Read/Grep/Glob/Edit/Write/MultiEdit/NotebookEdit/Bash，禁止 `git push`、`gh pr create/merge`；提示词要求先写失败测试并保存原始输出、最小修改、跑仓库全套检查、在任务分支提交。`--json-schema` 约束报告（`status = done | needs_input`、`summary`、`test_evidence`、`question`），`done` 无测试证据视为无效。`needs_input` 返回 `Suspend`，回答可来自同任务的跟进消息或批准时的 `comment`，经 `--resume` 回到同一开发会话；拒绝则输出 `[sde] stopped: question declined`（含问题、工作区与已有进展），不算交付。权限门禁 `sde.launch` 在每次开发启动前检查（含修复轮）：`defer` 以 `launch_pending` 挂起并把完整状态与待执行提示词写入检查点，批准即执行该次启动，拒绝输出 `stopped: launch declined`；`block` 输出 `stopped: launch blocked`。
  - **挂起期间的跟进**：同 `correlation_key` 的非审批事件记入检查点 `followups`，以原请求 ID 重新挂起；恢复时并入需求，先交开发会话处理，再送评审者。引擎规则：`Suspend` 沿用会话当前 `pending_request_id` 时只更新检查点，不重发审批卡（`AgentWorker._apply_outcome`）。
  - **评审回路**（`review = true`）：SDE 在进程内调用同一配置代生成的 `CoderReviewerExecutor.review()`（独立只读会话，保留评审者自己的启动器、计费、模型与门禁 `coder_reviewer.launch`），输入为需求、执行器自算的实际 diff 与测试证据；复审额外附上轮阻塞项和两次工作区指纹（`worktree_fingerprint` 的树对象）之间的增量 diff。`changes_requested` 把阻塞项经 `--resume` 交回同一开发会话修复后再送审；`pass` 交付。评审无法完成（评审者报错、结论无效、门禁未放行）或超过 `max_review_rounds` 时返回 `Suspend`，检查点 `{"sde": {...}}` 保存工作区、需求、证据、轮次历史与阻塞项；批准则重试（有阻塞项时再修一轮），拒绝则以 `review=pending` 交付，不标为已评审。
  - **关闭评审**（`review = false`）：开发完成即交付（`review=off`），不启动评审者。
  - **交付**：一条 `Emit`，含状态、五项启动设置、工作区、摘要、测试证据、评审历史与最后一次评审结论；`executor_session_id` 回填开发会话 ID。中断沿用 `supervise()`，不写半成品。
- **Orca 可见执行**：模板的 `orca_agent` 指定 Orca 管理的 agent。项目使用 Orca worktree 时，同一 Hibernaut 会话的后续事件回到已绑定的对话（`OrcaVisibleRunner`，`adapters/executor_cli/orca.py`）。绑定存在 SQLite `terminal_bindings` 表（键为 `session_id`，记录 `executor_id`、`agent`、`workdir`、终端 `handle`、进程 `incarnation_id` 与原生会话 ID），原生会话 ID 同时写入 `sessions.executor_session_id`，会话换了 `session_id` 也能按它找回绑定。每轮按顺序选择：
  1. **新开**：会话没有本执行器的绑定（且未带原生会话 ID）时 `orca agent launch --surface terminal` 在后台标签页提交，按回执记录终端句柄与 `incarnationId`。
  2. **复用**：绑定终端仍在 `orca terminal list` 中、`incarnationId` 与 `agentIdentity` 都一致时，用 `orca terminal send --enter` 投递到原对话（`terminal show` 的 `orphaned` 只表示尚无可见窗格，不作存活依据；同句柄换了进程视为另一个对话）。
  3. **恢复**：终端已关闭但有原生会话 ID 时，`orca terminal create --command "<command> --resume <id>"` 新开终端，等 `tui-idle` 并确认 `agentIdentity` 后再投递。
  4. **无法确认**：会话有绑定却既无存活终端也无原生会话 ID、工作目录变了、恢复没能启动 agent 或终端拒收时，什么都不发送，返回 `Suspend`（检查点 `{"orca_continuation": "unconfirmed"}`）说明原因；批准后才新开对话，拒绝则跳过本轮。建表迁移时，已有执行器的旧会话各补一条无终端、无原生 ID 的绑定，因此升级前跑过的会话走这一步而不会被当成新会话。
  原生会话 ID 不靠 agent 自报：每轮结果文件所在的临时目录名唯一且写在投递的 prompt 里，执行器在 Claude Code 转录目录（模板 `env.CLAUDE_CONFIG_DIR`、daemon 的 `CLAUDE_CONFIG_DIR` / `ORCA_CLAUDE_CONFIG_DIR`、`~/.claude` 下的 `projects/*/<uuid>.jsonl`）里找含该标记的会话，恰好一个（或与已绑定 ID 相同）才确认，否则不写入并记警告。完成以每轮唯一的临时结果文件为准。投递后中断或超时则先确认原生会话 ID、再关闭本轮终端，下一轮走恢复；启动回执或投递回执不确定时转为 `Suspend` 供人工确认，不重复投递。
- **声明式配置与禁止静默回退 Demo**（`hibernaut.example.toml` + `app/config.py` + `app/main.py --config`）：使用标准库 `tomllib` 解析，仅存静态主检出、执行器模板（全部 `[executors.<id>]` 注册进 `AgentWorker`，`[projects.<name>]` 支持可选 `executor`）与凭据环境变量名，绝不存动态 worktree 路径。以 `--config` 正式启动时严禁静默回退 `RuleBasedDemoLLM`：若未配置 `[executors]` 或缺少 `default_executor`，则向 `stderr` 打印清晰提示并以 **`ingest-only`（仅入站控制面）模式**启动，不启动消费 Worker 池，所有新事件安全保持 `pending` 等待配置修复（仅无 `--config` 或显式传 `--demo` 时才允许使用 Demo 执行器）。HTTP daemon 热加载时先组装新配置，待旧 Worker 池排空后再切换，沿用同一条 SQLite 连接及未变更的监听 socket。

## 5. 多 `inbox_id` Actor 分区并发与 `NewEventPolicy`

- **多 `inbox_id` 并发**：`AsyncWorkerPool` 跨 `inbox_id` 并行、同 `inbox_id` 单活互斥（`worker_leases` 主键即 `inbox_id`）、`Suspend` 立即放锁；`WorkspacePartitionRegistry` 强制禁止不同 `inbox_id` 挂载同一本地代码目录，全局 `global/instructions.md` 只读共享。
- **HTTP 入站与 SQLite 并发**：`WebhookHTTPServer`（`ThreadingHTTPServer`，`request_queue_size=128`）每请求一线程；`shutdown()` 等待在途请求全部写完 Inbox 后返回，Worker 停止与配置热加载据此排空。`SQLiteDatabase` 只有一条连接，所有语句经同一把可重入锁串行；写操作走 `transaction()`（`BEGIN IMMEDIATE`），去重、租约等先读后写在线程间和共享同一文件的进程间保持原子；租约时钟在拿到写锁后读取，等锁时间不计入 TTL；`mark_done()` 按 SQLite 参数上限分段更新；文件库开启 WAL 与 `busy_timeout`。回收 worktree 时，`claim_for_reclaim()` 以读到的 `updated_at` 做比较并交换：进行中的 `reclaiming` 认领是排他的，只有超过 `reclaim_stale_after_seconds`（默认 600 秒，认领方已崩溃）的认领才可被接管；删除期间心跳线程定期续期认领（遇到临时的 `SQLITE_BUSY`/`SQLITE_LOCKED` 缩短间隔重试，不退出），慢删除不会被当作过期而被接管；删除成功后 `finish_reclaim()` 同样按认领做比较并交换，认领已失效时不删除被他人重新登记的记录；删除失败时 `release_reclaim()` 恢复原状态，可直接重试。
- **`NewEventPolicy` 三策略**：`defer`（默认，推迟到下一轮）、`fold`（回合间隙通过 `poll_folded_events()` 增量注入同会话/同关联键事件并统一 `mark_done()`）、`interrupt`（安全取消当前 Run/CLI 子进程，绝不向 Outbox 写半成品，原批次保持 `pending` 与新事件同租约合并重启）。

## 6. 三道防腐化门禁与核心词汇表（Glossary）

提交前与 `.github/workflows/ci.yml` 强制执行：`lint-imports`、`pytest -v`、`pyright`（strict）。

| 术语 | 精确定义 | 范围 / 约束 |
| :--- | :--- | :--- |
| **Event Inbox** | 全局共享的物理事件表（SQLite `event_inbox`） | 全局 1 套，存放所有 `inbox_id` 的事件 |
| **Inbox / `inbox_id`** | 逻辑并发队列（Actor Mailbox）及其一等路由键 / `worker_leases` 主键 | 同分区串行、跨分区并发；禁跨 `inbox_id` 共享同一本地代码目录 |
| **Event** | 进入 Event Inbox 的输入信封（4 态 `pending` / `processing` / `done` / `failed`，重复标 `dropped`；含 `attempts` / `last_error`） | 必属某个 `inbox_id`，执行前立即 `bind_session()`；抢锁先恢复孤儿 `processing` |
| **`session_id`** | 隶属于某个 `inbox_id` 的跨 Run 对话转录与 `Suspend` 断点上下文单元（4 态 `active` / `suspended` / `archived` / `closed`） | 一个 `inbox_id` 下可有多个 `session_id`，绑定 `executor_id` / `executor_session_id` / `work_key`；轮换标 `archived`，结案标 `closed` 且引用归零触发回收 |
| **`correlation_key`** | 外部业务实体的确定性关联键（如 `github:kun-g/hibernaut#42`、`feishu:<chat>:<thread>`） | 经 `SessionStorePort.find_by_correlation_key()` 映射到当前活跃 `session_id`，同时作为 `workspaces` 表动态 worktree 复用键 |
| **Run / Lease** | 单次唤醒执行周期 / 以 `inbox_id` 为主键的限时互斥租约 | `Suspend` 立即释放租约；进程崩溃后租约到期自动接管 |
