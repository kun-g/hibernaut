# Hibernaut 核心词汇表（Glossary）

本词汇表为 Hibernaut（Always-on Agent）架构设计、文档与代码实现的术语单一事实源（Source of Truth），用于统一物理存储、逻辑分区、生命周期与生态分域命名，消除歧义。

## 1. 核心调度与分区概念

| 术语 | 精确定义 | 范围 / 粒度 | 与其他概念的关系 |
|---|---|---|---|
| **Event Inbox** | 全局共享的事件缓冲存储（SQLite `event_inbox` 表），所有外部触发源与定时器的唯一事实源 | 全局 1 套 | 物理容器，存放所有 `inbox_id` 的事件 |
| **Inbox** | Event Inbox 内的一个逻辑队列与并发隔离域（Actor Mailbox） | 按需划分（1 至 N 个） | 同一个 Inbox 内串行消费，不同 Inbox 之间并发 |
| **inbox_id** | 唯一标识单个逻辑 Inbox 的路由键（如 `user:main`、`proj:hibernaut`、`hibernaut:issue-42`） | 全链路一等字段 | `worker_leases` 租约锁主键；L3 据此路由项目 Memory 与仓库；与 Project 严格保持 **1 个 Project → 多个 inbox_id（1:N）** |
| **Event** | 进入 Event Inbox 的单条不可变输入信封（经 Switchboard 转入的 GitHub 和飞书事件、Drive 轮询任务、用户消息、定时触发、审批回复） | 单次触发 | 必属某个 `inbox_id`，确定会话后即时回填 `session_id`；状态为 `pending / processing / done / failed`（去重记 `dropped`） |
| **correlation_key** | 外部业务实体的确定性关联键（如 `github:kun-g/hibernaut#42`、`gdrive:<file_id>`、`feishu:<chat>:<thread>`），由 L3 入站适配器提取 | 隶属于某个 `inbox_id` | 通过 `SessionStorePort.find_by_correlation_key()` 映射到当前活跃/挂起 `session_id`，跨外部源自动续接并支持会话轮换 |
| **Session / session_id** | 跨多次 Run 的逻辑会话与挂起断点上下文单元 | 隶属于某个 `inbox_id` | 显式 4 态（`active / suspended / archived / closed`）；绑定 `executor_id`、`executor_session_id`、`work_key` 与可选 `parent_session_id` |
| **work_key** | 托管动态 `hb-*` worktree 的复用与引用计数键（默认沿用 `correlation_key`） | 单件活（Task / Issue） | 仅当某 `work_key` 下 `active / suspended` 会话引用数归零（`ref=0`）且过三关检查才允许安全回收目录 |
| **Run** | Worker 被唤醒后执行的一轮短生命周期推理与工具循环（一次 `ExecutorPort.execute` 调用） | 单次唤醒周期 | 消费同一 `inbox_id` 的一批事件，结束时产出 `RunOutcome` 并落盘休眠 |
| **Open Run / 脱离（detach）** | 进行中、可在进程重启后接回的 Run：`open_runs` 表里的登记加 `<state_dir>/runs/<session_id>/<run_id>/` 目录（执行器的 `state.json`）。停机时正在等 Orca 终端的 Run 被脱离：旧进程不等它、不关终端，新进程用 `resume()` 接着等结果 | 单个 Run | 正常结束时登记与目录一起删除；同一会话至多 1 个登记 |
| **Worker / Lease** | 消费事件并拉起 Run 的无状态调度协程 / 以 `inbox_id` 为主键的限时互斥租约锁（`worker_leases`） | 每 `inbox_id` 单活 | 保证同一 `inbox_id` 同一时刻最多 1 个活跃 Run；进程崩溃后租约到期自动释放接管 |

## 2. 五大原语与运行终态

| 术语 | 精确定义 | 共享 / 隔离属性 |
|---|---|---|
| **Timers（Follow-up / Schedule）** | 独立于 Event Inbox 的持久化定时器表，到期由 Scheduler 转为标准 Event 写入 Event Inbox | 全局共享 1 套表，按 `inbox_id` 分区 |
| **Memory** | 跨 Run 持久化记忆目录，分为每轮必载基线（`instructions.md`、`index.md`、`reported.md`）与按需读取目录（`notes/`、`progress/`） | 按 `inbox_id` 隔离项目记忆；全局 `instructions.md` 只读共享 |
| **Permissions & Credential Proxy** | 工具调用前置三态门禁（`allow / block / defer`）与外置凭证代理（沙箱内零真实密钥） | 全局共享 1 套安全网关，按项目/工具维度路由策略与凭据 |
| **Outbox & Dispatcher** | 待发消息持久队列与后台节流投递引擎，负责去噪、防刷屏与失败重试 | 全局共享 1 套表与投递服务，按 `inbox_id` 标记归属 |
| **RunOutcome（Silent / Emit / Suspend）** | 单轮 Run 的强类型三选一终态：静默退出（`Silent`）、发消息入 Outbox（`Emit`）、保存断点并立刻放锁等待人工审批（`Suspend`） | L0 领域契约，强制耗尽匹配 |
| **执行账本（Ledger）/ RunRecord / AuditRecord** | 独立包 `src/ledger/` 与独立 SQLite 文件 `ledger.db`：每个结束的 Run 追加一条 `RunRecord`（结局、执行器、耗时、错误类别、日志引用 `log_ref`、token 与费用），各服务的点状审计追加为 `AuditRecord`；只追加、只读查询，写失败不影响主路径 | 全局 1 套，与主库分开；`run_id` 即 Outbox 元数据里的 `run_id`，重放事件的新 Run 以 `replay_of_run_id` 指向被重放的失败 Run |
| **文件 Outbox** | `[channels] outbox_dir` 下每条投递的 Outbox 消息一个 Markdown 文件，开头是任务上下文头（任务、来源、状态、需要人做什么等） | 本机给人看的副本，与现有通道并存，不替代它们 |

## 3. 生态分域与易混概念澄清

- **Event Inbox vs. Inbox vs. `inbox_id`**：Event Inbox 指物理共享表（全局 1 套）；Inbox 指其中的逻辑并发分区（可并存多个）；`inbox_id` 是标识逻辑分区的字符串键与租约锁主键。
- **`inbox_id` vs. `session_id`**：`inbox_id` 管**并发锁边界与项目/工作区隔离**；`session_id` 管**分区内部的对话上下文连续性与断点恢复**。同一个 `inbox_id` 内可以先后或交替运行多个 `session_id`。
- **多粒度共存与防写冲突底线**：单用户（`user:main`）、按项目（`proj:<name>`）、按任务（`<project>:<issue>`）三种 `inbox_id` 粒度天然在同一套底座中共存；但严禁两个不同 `inbox_id` 同时挂载同一个本地代码目录（可写任务必须走按 `work_key` 隔离的动态 `hb-*` worktree）。
- **`correlation_key` vs. `session_id`**：`correlation_key` 是外部业务实体的稳定标识（如某个 GitHub Issue），`session_id` 是 Hibernaut 内部的逻辑对话实例；二者解耦后，同一工单既可跨事件自动续接，又可在上下文过长时归档旧会话并重绑到新的 `session_id`。
- **Switchboard（事件归一化交换机）**：生态最外层的统一通信总线，负责外部平台（GitHub / 飞书 / MCP / 定时器 / 邮件）的入站验签、幂等去重、异构事件归一化为 `CanonicalEvent`、按 `event_type/scope` 路由分发以及统一出站投递。零业务依赖，不 import Taskboard 或 Hibernaut。
- **RelayHub vs. Relay Agent（Switchboard 长连接推送）**：RelayHub 是 Switchboard 公网侧的 WebSocket 端点（`/ws/relay`），持有在线路由表并把投递推给已连接的消费方；Relay Agent 是运行在私网或本机消费方进程里的客户端，主动外连 RelayHub、收 `push` 后调用本地处理函数再回 `ack` / `nack`。节点离线时投递保持 `pending`，不计重试次数。
- **`conn_epoch`**：RelayHub 给同一 `consumer_id` 每次握手成功分配的单调递增连接代号。新连接握手后旧连接以 Close 4009 关闭，任何携带非当前 `conn_epoch` 的 `ack` / `nack` 都被丢弃，防止两条连接同时确认（防脑裂）。
- **ccpeer 接收端 vs. Claude Code 会话**：Claude Code 会话是 Orca 终端等处运行的交互式 `claude` 进程，在 `<配置目录>/sessions/` 登记 `messagingSocketPath`；ccpeer 接收端是以某个名字登记成 `standalone` 会话的常驻进程，只收不推理，把对方 `SendMessage(<名字>)` 的回复写进收件箱（`inbox.jsonl`，控制回执在 `control.jsonl`）。消息里的 `from-name`、`from-mode` 是发送方自称，不鉴别身份。
- **交接方式（`handoff`）：`mcp` vs. `peer`**：Claude Code 角色会话取任务、交结果的通道，按项目配置。`mcp` 由启动器挂的 `hibernaut_handoff` MCP 服务读写 hb 进程所建的轮次目录；`peer` 把任务以 ccpeer 消息发给会话、会话 `SendMessage` 把结果 JSON 发回常驻接收端，hb 按持久化游标读收件箱。`task_id` 标识一次任务（取 Hibernaut `session_id`），`attempt_id` 标识其中一轮（首次开发、返工、跟进各一个），结果必须带上当前轮的两者才被接受。
- **通审 vs. 增量复核**：SDE 内置评审的两种范围。通审看整份改动，任何问题都能阻断（第 1 轮，或没有上一轮阻断与交付交回可对照时）；增量复核（其余各轮，挂起期间有人跟进也一样）只能阻断上一轮没修好的项和上一轮之后改动引入的新问题，范围外的阻断由程序降级为跟进。
- **跟进（评审）**：评审 `notes` 里不阻断的意见，含被降级的阻断；交付时列在 PR 正文与 Outbox 消息，不打回开发。与挂起期间人发来的 follow-up（并入需求）不是一回事。
- **Taskboard（多源任务与 DAG 排期）**：负责归集来自多源的任务（`TaskItem`），维护任务状态与 DAG 先后依赖关系（`TaskDependency`，含环检测）；上游结案后自动解锁下游并经 Switchboard 发送 `work.ready` 事件唤醒 Hibernaut。不直接触碰 Session、Worktree 或执行器。
- **Runner（执行面 / 执行节点）**：独立部署在本机（如 Mac）或远端 VPS 上的执行宿主服务，负责管理所在机器的磁盘工作区（Orca / `git worktree` 与三关安全回收）、子进程生命周期、日志流落盘及 `yetone/magpie` 环境对接，并在其内部托管多个具体 Executor。与 Hibernaut 控制面通过 `ExecutorPort` / `RunnerPort` 解耦，可跨项目复用。
- **Executor vs. Runner 区分**：Runner 是「执行机器与工位（含 worktree 与运行环境，面向分布式部署，对应未来 `runner_id`）」，Executor 是「工位上干活的具体推理引擎/CLI（如 `claude` / `grok` / `cursor-agent` / `dsh`，对应 `executor_id` 与 `executor_session_id`）」。一台 Runner 节点可同时托管多个 Executor。
