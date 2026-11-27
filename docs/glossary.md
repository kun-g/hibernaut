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
| **trace_id / `_trace_id`** | 一条因果链的标识：事件 payload 的 `trace_id`，否则 `_trace_id`，都没有时为该事件自己的 `event_id` | 跨 Inbox、跨 Run | 由 L0 `resolve_trace_id()` 读取，不改写事件；转投、回投与取消事件原样继承，Run 的结果写入 `RunReport.trace_id`、Outbox 元数据 `_trace_id` 与账本 |
| **parent_event_id** | 事件 payload 里指向直接来源事件的 `event_id`（新事件指旧事件） | 单条事件 | 转投、回投与取消事件写发出它的 Run 的主事件；`hibernaut_list_events` 透出；旧事件不因有了后继而改写 |
| **Session / session_id** | 跨多次 Run 的逻辑会话与挂起断点上下文单元 | 隶属于某个 `inbox_id` | 显式 4 态（`active / suspended / archived / closed`）；绑定 `executor_id`（即 `agent_id`，见第 3 节 Agent）、`executor_session_id`、`work_key` 与可选 `parent_session_id` |
| **work_key** | 托管动态 `hb-*` worktree 的复用与引用计数键（默认沿用 `correlation_key`） | 单件活（Task / Issue） | 仅当某 `work_key` 下 `active / suspended` 会话引用数归零（`ref=0`）且过三关检查才允许安全回收目录 |
| **Run** | Worker 被唤醒后执行的一轮短生命周期推理与工具循环（一次 `ExecutorPort.execute` 调用） | 单次唤醒周期 | 消费同一 `inbox_id` 的一批事件，结束时产出 `RunOutcome` 并落盘休眠 |
| **Open Run / 脱离（detach）** | 进行中、可在进程重启后接回的 Run：`open_runs` 表里的登记加 `<state_dir>/runs/<session_id>/<run_id>/` 目录（执行器的 `state.json`）。停机时正在等 Orca 终端的 Run 被脱离：旧进程不等它、不关终端，新进程用 `resume()` 接着等结果 | 单个 Run | 正常结束时登记与目录一起删除；同一会话至多 1 个登记 |
| **Inbox 绑定（`InboxBinding`）** | 项目配置里一条「inbox 正则 → 默认 agent、agent 白名单、选路规则」，写在 `[[projects.<name>.inbox_bindings]]`；第一个整串匹配 `inbox_id` 的生效 | 项目内 | Inbox 与 Agent 是 M:N：同一 Inbox 绑多个 Agent 时共用一把租约串行接力，同一 Agent 服务多个 Inbox 时彼此并发、worktree 隔离 |
| **非项目 Inbox 绑定（`WorkdirBinding`）** | 不属于任何项目的 Inbox（`sys:*` / `user:*`）的绑定，写在 `[[runtime.inbox_bindings]]`：同 Inbox 绑定，另须显式写启动目录 `workdir`，可写 `checkout_access` | 部署级 | 可写的 `workdir` 只属于一个 Inbox；没有绑定也不匹配项目的 Inbox，事件保持 `pending`。使用规范见 [`inbox-conventions.md`](inbox-conventions.md) |
| **转投（forward）/ 回投（return）** | 一个 Agent 经 `RunCapabilities.forward_to_inbox` 把工作排进另一个 Inbox 的事件（`agent:forward`）/ 该工作带 `return_to` 时把结果排回指定 Inbox 的事件（`agent:return`） | 同一项目内；另有一个方向例外：非项目绑定 → 其 `forward_to` 列出的项目主 Inbox 或系统 Inbox，及其结果回投 | `hop_count` 记这件工作被主动转投的次数，上限 3（`MAX_FORWARD_HOPS`），回投不计；两种事件都过 `PermissionPort`。被拒的回投不外发，在单独的挂起会话里等人决定（held return），审批请求不带正文；不能回去的由 `agent:return_cancelled` 结束上级的这一项等待。内部来源名只由 Hibernaut 入队，公开入口拒绝 |
| **委派标识（`delegation_id`）** | 一项带 `return_to` 的转投的稳定标识：首次转出它的 `agent:forward` 的 `external_id`，随回投与取消事件走 | 上级会话的 `pending_returns` | 每项只有一个责任方：欠结果的会话把整件交给别人后就不再欠；发出方会话被关闭，它就结束，还没开始的不再开始；每项只结清一次，晚到的第二份结果丢弃；两项不同的委派不进同一批，还欠着或等着的会话不接另一项 |
| **任务键（`task_key`）** | 从非项目 Inbox 转投进一个项目的一件活的会话键：`forward:<来源 inbox>:<转投事件 external_id>:<原键>` | 目标项目内的这条链 | 同一个外部对话派出的每件活各有一个，所以各有会话、`work_key` 与 worktree；转投回执、`list_sessions` / `list_events` 的 `correlation_key`、回投的 `from_task` 都给出它；`continues` 填它即往那件活里补充。对外答复仍按 `origin_key` |
| **补充（supplement）** | 带 `continues` 的跨范围转投：不另立委派，并进任务键所指的会话；那个会话还欠着结果就由一份结果一起答 | 同一外部对话、同一来源 Inbox | 不带 `return_to`、不指定 agent；那件活已结束则被拒，按新活另投 |
| **Worker / Lease** | 消费事件并拉起 Run 的无状态调度协程 / 以 `inbox_id` 为主键的限时互斥租约锁（`worker_leases`） | 每 `inbox_id` 单活 | 保证同一 `inbox_id` 同一时刻最多 1 个活跃 Run；进程崩溃后租约到期自动释放接管 |

## 2. 五大原语与运行终态

| 术语 | 精确定义 | 共享 / 隔离属性 |
|---|---|---|
| **Timers（Follow-up / Schedule）** | 独立于 Event Inbox 的持久化定时器表，到期由 Scheduler 转为标准 Event 写入 Event Inbox | 全局共享 1 套表，按 `inbox_id` 分区。**配置定时器**是由 `[[timers]]` 声明、以唯一 `name` 标识的周期任务，事件来源为 `timer:<name>` |
| **Memory** | 跨 Run 持久化记忆目录，分为每轮必载基线（`instructions.md`、`index.md`、`reported.md`）与按需读取目录（`notes/`、`progress/`） | 按 `project_key` 隔离项目记忆，同一项目的 Inbox 共用；全局 `instructions.md` 只读共享；项目 `instructions.md` 的改动经人批准后生效 |
| **project_key** | 项目记忆的分区键，由 `inbox_id` 归并得到（`proj:<key>[:…]`、`<key>:<任务>`，或 `[memory_hub.inbox_projects]` 指定）；`sys:*`、`user:*` 与绑定了 `workdir` 的 Inbox 各自独立 | Memory Hub 的目录名与 `proj://<project_key>/` 的范围；不是队列或租约的键 |
| **SDE 的记忆工具** | SDE 会话只读本项目记忆的 `memory_search` 与 `memory_read`，按会话令牌经 daemon 的固定 Unix socket 调用，任务结案前一直可用 | 范围与权限同该会话 `inbox_id` 的 Run；评审会话没有 |
| **任务记录 / 目标名（`attempt_notes`）** | 派活时用 `attempt_notes` 写上目标名的 SDE 任务，结束时由 hb 记下的一条「试了什么、结果如何」，同一目标的记在同一个 `notes/attempts/<目标名>.md` | 一次任务一条，只写本项目记忆；是记录不是指令；没写该字段就不记 |
| **Memory 提案（Proposal）** | 对 `instructions.md` 等受控记忆的待批写入，编号 `prop_<hex>` | 人批准后才写入；过期自动失效 |
| **Permissions & Credential Proxy** | 工具调用前置三态门禁（`allow / block / defer`）与外置凭证代理（沙箱内零真实密钥） | 全局共享 1 套安全网关，按项目/工具维度路由策略与凭据 |
| **Outbox & Dispatcher** | 待发消息持久队列与后台节流投递引擎，负责去噪、防刷屏与失败重试 | 全局共享 1 套表与投递服务，按 `inbox_id` 标记归属 |
| **RunOutcome（Silent / Emit / Suspend）** | 单轮 Run 的强类型三选一终态：静默退出（`Silent`）、发消息入 Outbox（`Emit`）、保存断点并立刻放锁等待人工审批（`Suspend`） | L0 领域契约，强制耗尽匹配 |
| **执行账本（Ledger）/ RunRecord / AuditRecord** | 独立包 `src/ledger/` 与独立 SQLite 文件 `ledger.db`：每个结束的 Run 追加一条 `RunRecord`（结局、执行器、耗时、错误类别、日志引用 `log_ref`、token 与费用），各服务的点状审计追加为 `AuditRecord`；只追加、只读查询，写失败不影响主路径 | 全局 1 套，与主库分开；`run_id` 即 Outbox 元数据里的 `run_id`，重放事件的新 Run 以 `replay_of_run_id` 指向被重放的失败 Run |
| **文件 Outbox** | `[channels] outbox_dir` 下每条投递的 Outbox 消息一个 Markdown 文件，开头是任务上下文头（任务、来源、状态、需要人做什么等） | 本机给人看的副本，与现有通道并存，不替代它们 |
| **共享专家（expert）/ 专家池** | `[experts.<role>]` 声明、不属于任何项目的 Agent；项目用 `experts` 抽调后，在 `proj:<project>:<role>[:<task>]` 里工作 | 跨项目共享 Agent 与 `role://` 记忆；每个 Inbox 只挂一个项目的读写记忆 |
| **角色记忆（`role://<role>/`）** | 一个角色在所有项目里共用的技能记忆，存于 `<memory_root>/roles/<role>/` | Run 只能提案，人在 hub 上批准后生效，批准时做 CAS 校验 |
| **收口卡（`CloseoutCard`）** | 一次与专家的直接会话的结论：摘要、决定、后续任务、记忆提案。`draft → confirmed → synced`，或 `discarded` | 人确认后才回流到幕僚长 Inbox（`agent:closeout` 事件）、Taskboard 与提案队列，幂等 |
| **幕僚长（Chief of Staff）** | 服务 `user:main`、带 `builtin:chief-of-staff` Skill 的全局 Steward，只澄清、分派、汇总 | 与各项目 Steward 分工；不拆项目内的活 |
| **Hub（协作看板）** | `[collaboration.hub]` 起的 HTTP 页面，按需拉取简报、审批提案、确认收口卡 | 与只读的 Dashboard（`src/dashboard/`）是两个服务；与 Switchboard 的 RelayHub 无关 |

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
- **评审候选**：SDE 的 `reviewer` 写成有序列表时列表里的每一个。每个任务第一次评审前按列表取第一个可用的，选定后各轮都用它；它报额度用完才换下一个。`reviewer` 写一个名字就是固定的评审者，不叫候选，不看可用表。
- **可用表**：外面的程序写、Hibernaut 只读的一份 JSON（`[availability] file`），说哪些执行器现在不该用和为什么；没列到的算可用。与「额度标记」分开：额度标记是 Hibernaut 在评审报额度用完时自己记的，到期失效，两者任一说不可用就不选。
- **跟进（评审）**：评审 `notes` 里不阻断的意见，含被降级的阻断；交付时列在 PR 正文与 Outbox 消息，不打回开发。与挂起期间人发来的 follow-up（并入需求）不是一回事。
- **Taskboard（多源任务与 DAG 排期）**：负责归集来自多源的任务（`TaskItem`），维护任务状态与 DAG 先后依赖关系（`TaskDependency`，含环检测）；上游结案后自动解锁下游并经 Switchboard 发送 `work.ready` 事件唤醒 Hibernaut。不直接触碰 Session、Worktree 或执行器。
- **Runner → Executor → Agent 三层边界**：Runner 是执行工位，Executor 是工位上的驱动，Agent 是用驱动干活的业务主体；**Inbox 绑定的是 Agent**，不是 Executor 或 Runner。
  - **Runner（执行面 / 执行节点）**：独立部署在本机（如 Mac）或远端 VPS 上的执行宿主服务，负责管理所在机器的磁盘工作区（Orca / `git worktree` 与三关安全回收）、子进程生命周期、日志流落盘及 `yetone/magpie` 环境对接，并在其内部托管多个具体 Executor。与 Hibernaut 控制面通过 `ExecutorPort` / `RunnerPort` 解耦，可跨项目复用；面向分布式部署，对应未来 `runner_id`。
  - **Executor（驱动）**：没有业务含义的底层 CLI / LLM 驱动（如 `claude`、`grok`、`cursor-agent`、`dsh`，或一个 Claude Code 启动器加计费方式），在 `[executors.<id>]` 声明。一台 Runner 可托管多个 Executor。
  - **Agent（业务主体）**：绑定 Role 与 Skills、权限边界、记忆视角和底层 Executor，在 `[agents.<id>]` 声明，标识为 `agent_id`。同一 Executor 可供多个 Agent 使用。
  - **`agent_id` vs. `executor_id`**：代码与存储沿用的 `executor_id`（`ExecutorPort.executor_id`、`sessions.executor_id`、事件 payload 的旧键）取值就是 `agent_id`；`[executors.<id>]` 写了 `role` 或 `command` 时同时是同名 Agent（旧写法）。
- **Steward（项目管家）**：`role = "steward"` 的 Agent，项目主 Inbox 的前台：读事件后自己答复、转投给别的 Agent，或汇总回投的结果。在项目主检出（或配置的 `workdir`）里只读运行，每轮在 Orca 终端里一次性执行，不建 worktree，不能编辑文件或执行命令，没有内置工具，读文件也经逐次过门禁的 MCP 工具；可读写所在 Inbox 的 Memory、只读查询本项目的会话与事件。能路由到哪些 Inbox 和 Agent 由 Runtime 每次唤醒时现拼的路由清单（routing manifest）给出。
- **全局 Steward**：绑在非项目 Inbox（`[[runtime.inbox_bindings]]`，`checkout_access = "readonly"`）上的 `steward` Agent，不属于任何项目。在绑定的 `workdir` 里只读运行；可转投到绑定 `forward_to`（转投白名单）列出的项目主 Inbox，只读查询自己的 Inbox 与这些项目的会话和事件，看不到项目的文件。
- **系统 Steward**：绑在系统 Inbox 上的 `steward` Agent（`sys:ops`），由全局 Steward 转交或由群入口的名单成员直接说，用系统工具（`check_projects`、`onboard_project`、`complete_project`）接入项目、检查和补齐项目的 Steward 绑定、说明与转投白名单，用只读的 `runtime_status` 回答 hb 运转得怎样。同样只读、没有命令行，不能再转投；需求见 [`requirements/system-steward.md`](requirements/system-steward.md)。
- **系统 Inbox**：写了 `system_tools = true` 的 `[[runtime.inbox_bindings]]`：单个精确 `sys:<name>`、`checkout_access = "readonly"`、Agent 全是 `steward`。只收全局 Steward 替人转来的、群入口名单上的人发的、本机运营者投的，以及它自己审批的答复。
- **运维 Inbox**：写了 `ops_tools = true` 的 `[[runtime.inbox_bindings]]`（如 `sys:infra`，agent `infra`，不要与系统 Steward 的 `sys:ops` 混淆），限制和来路同系统 Inbox。它的 Steward 只有 `dokploy` 一个能动手的工具，在 Dokploy 上建项目的预发布和生产环境；没有通用命令行，不能转投。
- **运维 agent**：绑在运维 Inbox 上的 `steward` Agent。预发布直接部署；生产上线、删除由代码判为 `deploy_production` / `delete_resource`，挂起等人批准。
- **扣住的调用**（held call）：运维 Steward 的 `dokploy` 调用被门禁判为 `defer` 时不执行，记进本轮的 `deferred`，随挂起一起送审；批准后由 Hibernaut 按原参数重判再执行，Steward 再跑一轮报告结果。
- **`create_repo`**：全局 Steward 在绑定写 `repo_tools = true` 时多出的工具：在配置允许的账号或组织下建私有 GitHub 仓库，未指定时用 `[repo_tools] owner`，首次提交是来源文件夹里该项目最新的文件；仓库已存在就停。不是命令行。
- **来源文件夹**（sources folder）：Drive 凭据代理配置的 `sources_folder_id`，下面每个项目一个子文件夹放各个版本；`create_repo` 按项目名查找时使用。代理显式开启 `allow_any_source_folder` 后，也可用共享的项目文件夹链接直接取其文件。
- **一句话开新项目**：单聊里一句话，由全局 Steward 依次 `create_repo`、转交系统 Steward 接入、转交运维 agent 配环境，每步结果回同一单聊；前一步回信 `done` 才能转下一步（回信带 `status`：`done` / `failed`）。
- **群入口**：`[[feishu_groups]]` 把一个飞书群（`chat_id`）绑到一个系统 Inbox，并列出算数的人（`members`，飞书 open_id）。名单上的人在群里发的消息直接进该 Inbox，答复回群；名单外的人的消息丢弃不回。一个系统 Inbox 只绑一个群，它的审批卡发到这个群，只认名单上的人在这个群里的点击。
- **运行状态**（`runtime_status`）：系统 Steward 和全局 Steward 共用的只读工具（同一份实现），给出一段时间内失败和挂起的任务（挂起带等什么：`approval` 审批、`answer` 人回答、`user_away` 用户离开电脑、`retry` 到点重试）、交付结果、发不出去的 Outbox 消息、各 `[[timers]]` 最近的触发和结果，以及查不到的范围；只有摘要，不带 payload 和密钥。
- **Runner Relay**：Hibernaut 进程内的 Runner 反向 WebSocket 端点（`RunnerRelayHub`），Runner 主动连入，凭共享令牌鉴权后接单。`[runner]` 的 `relay` 模式与试点路由各开各的。与 Switchboard 的 relay 无关。
- **Runner 试点路由**：`[runner_pilot]` 配置的一条默认关闭的路由：只把一个任务级 Inbox 交给一台单槽位的独立 Runner，执行器固定为 `runner-protocol-check`（无工具的协议文件校验）。关闭时该 Inbox 按原路由执行。
- **远端 Agent 节点**：经 `[runner]` 的 `relay` 模式连入的单槽位、单项目、只读 Runner（`python -m runner.agent_service`），只带一个 CLI 执行器，只执行显式指名该 agent 的事件。与只带协议校验程序的试点节点是两种服务，配置互不通用。
- **候选提交（`candidate_commit`）**：试点任务载荷里要核验的 40 位完整提交号。与 SDE 交付中的候选分支无关。
- **汇总摘要（manifest digest）**：试点核验的五份 Runner 协议文件按路径排序的 `<sha256>  <路径>` 清单（`shasum -a 256` 格式）的 SHA-256，用 git 与 `shasum` 即可独立复算。

## 测试分层

- **单元测试**：从准备夹具到清理都在当前进程内完成。
- **集成测试**：启动 Git、守护进程、shell 或模拟 CLI 等外部进程，外部服务使用模拟实现。
- **端到端测试**：从用户入口发起操作，使用真实服务验证结果。此层预留。
