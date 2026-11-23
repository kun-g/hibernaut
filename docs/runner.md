# Runner 执行面架构设计文档（v1.0 定稿）

本文档记录 Always-on Agent 生态中 **Runner（执行面 / 执行节点）** 模块的定稿设计（Q0–Q4 共 15 项架构决策），配套交互式双向透视蓝图见 [`docs/runner_blueprint.html`](runner_blueprint.html)，跨节点 JSON Schema 契约见 [`schemas/runner/v1/`](../schemas/runner/v1/)。

## 1. 核心定位与四条边界（Q0）

- **定位（0A 方案 A）**：无业务语义的通用代码执行宿主（Execution Host），部署在 Mac 本机或远端 VPS，独占磁盘工作区与子进程；一台 Runner 托管多个 Executor CLI（`claude` / `grok` / `cursor-agent` / `dsh`）。
- **代码拆分线（0B 方案 A）**：底层原子原语（Worktree 隔离回收、进程监管、CLI 启动、只读指纹校验）下沉至 `src/runner/`；多轮业务编排（`sde.py` 评审回路、`PRDelivery`）留在 Hibernaut 控制面。
- **三关回收 PR 核验归属（0C 方案 A）**：控制面先核验 `count_active_by_work_key == 0` 及远端无开着 PR（squash 合并附带 `merged_head_sha`），Runner 零 `gh` 凭据依赖，纯本地核验树干净 + `HEAD` 已推送或匹配 `merged_head_sha`。

## 2. `RunnerPort` 协议与双模传输（Q1）

遵循 [`ADR-0001`](adr/0001-cross-project-protocol-schema.md)（JSON Schema Draft 2020-12，`schema_version = "runner.v1"`）：
- **终态语义（1A / 1C 方案 A）**：Runner 仅返回执行事实（`succeeded | failed | interrupted | timed_out | refused` + `structured_output` + `error_kind` + `log_ref`），控制面通过薄适配层（如 `src/agent_runtime/adapters/runner_bridge.py`）翻译回业务终态。
- **4 个核心方法**：`capabilities()`、`run(RunRequest, authorizer=None)`（`run_id` 幂等键；`authorizer` 是可选的 `ToolAuthorizer` 回调，见 §6「逐次工具授权」）、`interrupt(InterruptRequest)`、`reclaim_workspace(ReclaimRequest)`。
- **双模传输（1B 方案 A）**：Stage 1 `InProcessRunnerClient` 同进程直调；Stage 2 `RunnerRelayHub` + `RunnerRelayAgent` 纯 stdlib 反向 WebSocket（RFC 6455，`conn_epoch` 防脑裂 + Close 4009 + `run.submit/accepted/completed/query` + `tool.authorize/decision`）。

## 3. 多节点注册、心跳、并发槽位与亲和性（Q2）

- **亲和与离线策略（2A 方案 A）**：写任务严格硬亲和建目录的原节点，原节点离线保 `pending` 不计失败；仅 `readonly=True` 且 `remote_readonly_ok=True` 允许漂移至其他在线节点。
- **槽位防超卖（2B 方案 A）**：控制面内存预扣（`reserved_slots`）+ Runner 本地并发上限终裁（`slots_full` 退回换机）+ 每 20s 心跳实测校准。
- **持久化与握手对账（2C 方案 A）**：`NodeRegistry` 纯内存，`WorkspaceAffinityStore` 落控制面 SQLite，Runner 握手 `hello` 上报 `ready_work_keys` 自动清理脏映射。

## 4. Worktree 驱动与三道保险抽离（Q3）

- **单库复合主键（3A 方案 A）**：每台 Runner 维护单库 `runner_workspaces.db`（WAL），主键 `(namespace, project_key, owner_scope, work_key)`。
- **强前缀与 `name_hint`（3B 方案 A）**：强前缀（`hb-` / `ws-`）由 `runner.toml` 白名单 + 调用方身份鉴权钉死，调用方仅提议经清洗的 `name_hint`，撞名自动顺延 `-2`。
- **共享三道保险与禁静默降级（3C 方案 A）**：`WorktreeManager` 统一承载 `creating` 占位、写归属锁与 CAS 本地安全回收；`interactive=True` 必须匹配 `interactive_surface="orca"`，严禁静默降级 headless。

## 5. 多 CLI 驱动、防挂死与流式扩展口（Q4）

- **双保险看门狗（4A 方案 A）**：`ProcessSupervisor` 独立进程组（`start_new_session=True`）+ 总硬超时 + 按驱动可配静默看门狗 `idle_timeout_seconds` + `os.killpg` 整树清理 + 实时 `.log` 落盘。
- **原生续接防呆（4B 方案 A）**：回传 `<driver>:<native_id>`，驱动自检 `supports_resume` 与前缀匹配才走 `resume_args`，切执行器自动新开。
- **流式扩展口（4C 方案 A）**：Phase 1 保持单轮 `RunResult` 并内置 `StreamPipe` 扩展口；Phase 2 沿同一条反向 WebSocket 增量扩展 `run.chunk` / `run.stdin` 帧。

## 6. 执行安全不变量

- **进程组随 Run 结束**：`ProcessSupervisor.execute` 不论正常退出、超时、`interrupt` 还是协程被取消，都先对子进程所在进程组（`start_new_session=True`，组号即子进程 pid）发 `SIGTERM`，超过 `grace_seconds` 再 `SIGKILL`，并在首进程退出后对整组补发 `SIGKILL`，直到组内没有进程才返回或继续抛出取消。没有 PID-1 reaper 的 Linux 容器里，被杀的孙进程会滞留为僵尸（`killpg` 对它仍成功）：组内可见成员在 `/proc` 里全是 `Z`（或 `X`）态时即视为已退出，不再等满 `grace_seconds`；看不到任何成员或没有 `/proc` 时不作此判断。被取消的 Run 记为 `interrupted`，以同一 `run_id` 等待它的调用方也拿到这个终态。
- **一个工作区同时只有一个 Run**：`RunnerDaemon` 按 `(namespace, project_key, owner_scope, work_key, readonly)` 占位，占位在 worktree 解析之前取得，并保持到进程组回收完毕；第二个 `run_id` 落到同一工作区时返回 `refused`（`error_kind = "rate_limit"`，`error_summary` 以 `workspace_busy` 开头）。仍有 Run 占用时 `reclaim_workspace` 返回 `blocked`（`run_active`）；回收本身也占住该四元组的写、读两个位置，直到回收线程跑完，期间到达的 Run 收到 `workspace_busy`。
- **只读 Run 不挂主检出**：`readonly=True` 的 Run 使用按上述四元组派生的独立 detached worktree（`<worktree 根>/.readonly/<prefix>-ro-<hash>`，与写工作区不在同一目录，不进 `workspaces` 表，也不上报为亲和；建出、还原与删除前都核对该目录在只读根下、不是任何已登记或 git 登记在分支上的写 worktree，否则以 `permission_denied` 拒绝），每次运行前从任务 worktree 已提交的 `HEAD`（没有任务 worktree 时为 `default_base_ref`）建出，只存在于这次 Run：运行后指纹变化即返回 `failed`（`permission_denied`），不论成败都在释放占位前删除该目录，主检出与任务 worktree 不受影响。
- **HB 适配层**：`RunnerExecutorBridge.execute` 被 Worker 取消（`interrupt` 策略或停机）时，向 Runner 发 `interrupt` 并等到 Run 出终态再继续抛出取消；Runner 尚未登记该 `run_id`（`not_found`）时按轮询间隔重发。`translate_run_result` 给每个结果带上 `RunTelemetry`（`stdout_excerpt`、`error_summary` 作 stderr、`exit_code`、Runner 的结构化 `error_kind`、执行节点 `runner_id`），Worker 的执行账本据此写运行日志与计费；`RunRecord.error_kind` 直接用 Runner 给的分类（如 `slots_full` 记 `rate_limit`），没有时才按 stderr 与退出码推断；`RunRecord.runner_id` 记实际执行的节点。Run 被中断时 Bridge 抛出带同样 telemetry 的 `RunCancelled`（`CancelledError` 子类），Worker 在执行器调用处取下它，`interrupted` 记录因此也落在实际节点上；原批次照常重新入队，不写 Outbox。
- **反向 WebSocket 换连接**：Hub 按提交时的 `conn_epoch` 记录每个在途 Run。同一 `runner_id` 的新连接顶替旧连接（Close 4009）或旧连接断开时，旧连接上的在途 Run 以 `RunnerConnectionLostError`（`ConnectionError` 子类）结束并释放预扣槽位；Runner 侧随连接取消这些 Run 并回收进程组，调用方换新 `run_id` 重试，进程组回收前会收到 `workspace_busy`。
- **`run_id` 是跨等待者的幂等键**：Hub 对同一 `run_id` 只提交一次、只预扣一个槽位，后来的调用者等待同一次提交；某个调用者超时或被取消不影响其余调用者，也不撤销提交。`run.submit` 发出后不设 ack 期限（Runner 可能已在执行），只有连接断开或被顶替才以 `RunnerConnectionLostError` 结束。Agent 收到本机已在执行或已完成的 `run_id` 时直接接受并交给 `RunnerDaemon.run` 挂接已有执行，不因槽位满回 `slots_full`。Hub 把已结束的非 `refused` 结果按 `run_id` 缓存（LRU，`FINISHED_RUN_CACHE_SIZE = 4096` 条），重试同一 `run_id` 直接返回该结果，不再派发。`run_id` 只对同一请求复用：Hub 与 `RunnerDaemon` 都按 `(caller_id, namespace, project_key, owner_scope, work_key, executor_id, readonly, 执行字段指纹)` 比对（`runner.protocol.run_identity`；指纹是除 `run_id`、`credential_lease`、`metadata` 外全部请求字段的 SHA-256，prompt、原生会话 ID、`interactive` 等任何一项不同都算另一个请求），不同请求复用同一 `run_id` 时 Hub 抛 `RunIdConflictError`，Daemon 返回 `refused`（`permission_denied`）；Daemon 先做调用方授权再查重，不会把别人的结果交给未授权的调用方。两端都不缓存 `refused`（如 `workspace_busy`）：它没有执行，同一请求在占用解除后以同一 `run_id` 重试会真正执行。`interrupt` 与 `run.query` 也只对提交该 Run 的调用方生效（同进程客户端带自己的 `caller_id`，Relay 帧带 Hub 转来的 `caller_id`），其他调用方得到 `not_found` / 空结果。
- **git / orca 子进程不占事件循环**：`RunnerDaemon` 经 `asyncio.to_thread` 调用 worktree 解析、写入校验、指纹、只读还原与回收，Agent 的 `hello` 读取 `ready_work_keys` 也在线程里，其他 Run 的看门狗、中断与进程组回收照常推进。这些线程调用被取消时仍等线程跑完才继续抛出取消（`_to_thread_to_completion`），工作区占位因此不会早于 git 操作释放。不同 `work_key` 的解析因此可能并发，分配目录名与分支名时把表里已登记（含 `creating`）的名字视为占用。
- **子进程环境只放白名单**：`build_child_env` 只从 Runner 自身环境带入 `PATH`、`HOME`、用户与 shell、临时目录、时区、`LANG` / `LC_*`、终端、`XDG_*` 与代理变量（`CHILD_ENV_ALLOWLIST`），名字像凭据的（含 `TOKEN`、`SECRET`、`PASSWORD`、`CREDENTIAL`、`API_KEY`、`ACCESS_KEY`、`AUTH` 等）即使在白名单前缀下也去掉。驱动配置的附加变量可以设其他项，但名字像凭据或与租约变量同名时整次 Run 以 `refused`（`permission_denied`）结束。名字允许、值却是带用户信息的 URL（如 `http://user:pass@proxy:8080` 这样的代理；名字以 `_PROXY` 结尾的变量只要值里有 `@` 就算，不论是否省略协议前缀、密码里是否有未转义的 `#` `/` `?`）时，继承环境与驱动附加变量同样整次拒绝（不静默丢弃，免得节点在无代理下跑），错误信息只写变量名。外部鉴权只经 `credential_lease`（`RUNNER_BROKER_URL` / `RUNNER_LEASE_TOKEN`）。
- **工作区命令同一份受限环境**：`worktree.py` 的每条 git / orca 命令（创建、解析、指纹、只读还原、校验、回收）都以 `build_child_env` 的结果启动，git 拉起的 hook 与 filter 随之继承；Runner 自身的 `GH_TOKEN`、`SSH_AUTH_SOCK`、`GIT_ASKPASS`、`GIT_CONFIG_*`、`ORCA_*` 等不传入。环境带凭据（同上）时以 `ChildEnvError` 在启动任何命令前拒绝：Run 为 `refused`（`permission_denied`），回收为 `blocked`（`child_env_refused`，工作区记录退回 `ready`，配置改好后可再回收），文案只含变量名。
- **输入写入不挡看门狗**：经 stdin 传 prompt 的驱动，写入在独立任务里进行，与输出读取、硬超时、静默看门狗和 `interrupt` 检查并发；子进程不读输入时超时与中断照常生效，结束时先取消写入再回收进程组。
- **交互 Run 不降级**：内置 `TemplateCLIDriver` 只会起后台子进程，能力里的 `interactive_surface` 恒为 `none`；`interactive=True` 的请求在节点上以 `refused`（`config_error`）结束，Hub 路由也要求执行器本身声明 `orca`。在 Orca 可见终端里启动并监管 CLI 的驱动尚未实现。
- **断线后先收尾再重连**：Agent 的连接断开时，取消该连接上的全部 Run 并等它们结束（含 worktree 线程与进程组回收），之后才重连并读取 `ready_work_keys`；仍在创建的写工作区因此不会从 `hello` 里漏掉，Hub 不会删掉它的亲和绑定。
- **日志先于进程、输出按块读取**：`ProcessSupervisor` 先打开运行日志再启动子进程，打不开日志就不启动；启动后的每条退出路径都经进程组回收。输出按 64 KiB 块持续排空并写日志，逐行回调另有 4 MiB 的行缓冲上限（`MAX_STREAM_LINE_BYTES`），超长行不会中断读取；读取失败时 Run 记为 `failed`，`stderr` 以 `runner output read failed` 开头。
- **回复只认发出请求的连接**：`run.accepted` / `run.completed` 只在来自该 `run_id` 提交时的 `runner_id` 与 `conn_epoch` 时生效，`rpc.reply` 只在来自请求发往的连接时生效；别的节点或旧连接发来的同名回复被忽略。
- **写工作区先认领后发送、按工作区串行派发**：同一四元组的写请求从选节点到收到 ACK 这一段逐个进行，后来的请求等前一个的节点确认后再派发。选定节点后、第一次 `await` 之前，若该四元组还没有亲和绑定就先写入绑定，同一工作区随后的写请求（不同 `run_id`）因此只会派到同一节点（槽位满时报 `NoEligibleRunnerError`，不换机）。节点明确回 `accepted = false` 才说明没执行；因为派发是串行的，此时没有别的请求依赖这次认领，删除这次新建的绑定是安全的（多槽节点上不会出现“首单被拒、次单仍在途”）；连接断开等无回答的情形保留绑定，由节点重连时 `hello` 的 `ready_work_keys` 对账清理。
- **心跳超时即断开**：Hub 每个心跳周期扫一次，超过两个周期没有 `pong` 的节点除了标离线，还按当前连接代次关闭连接并让其在途 `run.submit` / 结果等待以 `RunnerConnectionLostError` 结束，与连接断开同一路径；握手后不再回应、也不断开的节点不会让调用方一直等。
- **逐次工具授权**：CLI 里的每一次工具调用先经控制面 `PermissionPort.check()` 判定。
  - **链路**：`RunnerExecutorBridge`（`authorize_tools=True`）给 `runner.run()` 挂 `ToolAuthorizer`；`RunnerDaemon` 为该 Run 开一个 `ToolGate`（0700 私有目录里的 Unix socket，随 Run 建删），由驱动把 `runner/tool_hook.py` 装成 CLI 的工具前置钩子；钩子把工具名与参数交给 `ToolGate`（载荷里的账号邮箱、路径等不转发；会话开始时另报 CLI 的原生会话 ID，供还没有输出就被中断的 Run 续接），得到 `allow` / `block` / `defer`。Relay 模式下 Agent 用 `tool.authorize` 帧（`ToolCall`）问 Hub，Hub 只认来自该 Run 提交连接的帧，问过 `ToolAuthorizer` 后以 `tool.decision` 帧（`ToolDecision`）按 `call_id` 回答；契约见 `runner.tool_authorize.schema.json`。
  - **三态**：`allow` 放行；`block` 拒绝这次调用并把原因交给执行器，Run 继续；`defer` 拒绝这次调用并中断整个 Run（之后到达的调用一律拒绝），`RunResult.status = "interrupted"`，Bridge 返回 `Suspend`：检查点（`kind = "runner_tool_defer"`）记下工具名、参数、原 prompt、只读与交互标记，正文写清要执行什么。
  - **批准后恢复**：`approval_request_id` 的决定到达后，Bridge 用原 prompt 加审批结果续跑（CLI 支持原生续接时续同一会话）。批准只放行一次参数完全相同的调用（`Bash` / `Shell` / `run_terminal_command` 的 `description` 说明文字不计），参数变了重新问人，没用上的批准随下一个检查点保留；拒绝则本次 Run 内拦下该调用并附审批意见；策略已改为 `block` 时批准无效。执行器出错重试时，用过的批准不再生效（记在进程内存里，进程重启后重放同一批事件会再次生效）。只读 Run 恢复后仍是只读。
  - **各 CLI 的钩子**：`claude` 用 `--settings`（`PreToolUse` 匹配全部工具，`SessionStart` 报到，`disableAllHooks=false`）加 `--permission-mode dontAsk`（钩子没放行的一律拒绝）；`cursor-agent` 用 `--plugin-dir`（`preToolUse` `failClosed`）加 `--force --trust`；`grok` 用 `--agent` 定义文件（`PreToolUse` `failClosed`）加 `--permission-mode bypassPermissions --no-subagents --disallowed-tools workflow`（子代理与 workflow 不走本 agent 的钩子）。会替换或关掉这些设置的参数（如 `--settings`、`--permission-mode`、`--bare`）不得出现在驱动的 `args` / `resume_args` 里，构造驱动时即报错。`dsh` 没有可用钩子。
  - **fail-closed**：声明会调工具（`calls_tools=True`）的驱动没有 `ToolAuthorizer` 时，`RunnerDaemon` 以 `refused`（`permission_denied`）拒绝启动；要求授权（`authorize_tools`）但驱动没有钩子时同样拒绝，Hub 路由跳过没有声明 `tool_authorization` 的节点（旧节点视为没有）；Bridge 对工具能力未知或会调工具的执行器，未设 `authorize_tools` 时在提交前就返回 `Emit(status="refused")`。钩子 60 秒内没有报到则中断该 Run，跑完都没报到则结果作废（`failed` / `permission_denied`）。钩子读不懂载荷、参数被截断或超过 512 KiB、连不上 `ToolGate`、回调报错或 60 秒无回答，都按拒绝处理（退出码 2）。
  - **不在覆盖范围**：仓库或用户自己给 CLI 配的其他钩子由 CLI 直接执行，不经 `PermissionPort`；被放行的 `Bash` 命令内部做什么由策略对参数的判定负责。
- **回复文本**：驱动解析到 `type = "result"` 的 JSON 行时，其 `result` 文本记为 `RunResult.final_text`（上限 65536 字符），Bridge 优先用它作回信；`stdout_excerpt` 只保留输出开头 256 KiB，流更长时仍能取到最后的结果行。
- **Orca 后端**：`orca worktree create` 带 `--repo path:<主检出>`、`--base-branch <解析后的提交>`、`--no-parent`、`--setup skip`、`--json`，以回执里的绝对路径与分支（须为请求的分支或其 `-N` 后缀）为准登记，回收时按该路径 `orca worktree rm`。

## 7. 组装根接入（`[runner]`）

`hibernaut.toml` 的 `[runner]` 默认 `enabled = false`，此时不创建任何 Runner 对象，执行器、权限策略与环境变量处理都和没有这一段时相同。`enabled = true` 时，`build_daemon_from_config` 为每个项目把 `executors` 列出的 CLI agent 从 `LocalCLIExecutor` 换成 `RunnerExecutorBridge`（`authorize_tools=True`）；角色执行器（`SDEExecutor` 等）、未列出的 CLI agent、不属于任何项目的 Inbox 不变。

- **`executors`**：不填时是所有命令名或执行器名为 `claude` / `grok` / `cursor-agent` 的 CLI agent。`in_process` 模式按 `[executors.<id>]` 的 `command` / `args` / `resume_args` / `env` 构造驱动，参数与钩子冲突、没有钩子的执行器在解析配置时报错；`relay` 模式的命令由各节点自己的驱动决定。
- **权限策略**：`allow_tools` / `block_tools` 并入该进程 Worker 的 `AutoModePermissionAdapter`（优先级不变：block、危险参数、defer、allow）。两者和内置名单都没有的工具一律 `defer`，所以只读工具（如 `Read`、`Grep`、`Glob`）要显式列出，否则第一次调用就会挂起问人。
- **`mode = "in_process"`**：进程内 `RunnerDaemon` + `InProcessRunnerClient(caller_id="hibernaut", bound_namespace="hb")`；项目取自 `[projects.*]`（基线分支为 `origin/<delivery.base_branch>`，缺省 `origin/main`），状态库与日志在 `state_dir`（缺省 `db_path` 同目录的 `runner/`）。
- **`mode = "relay"`**：进程内 `RunnerRelayHub` 监听 `listen_host:listen_port`，亲和库为 `WorkspaceAffinityStore`（缺省 `runner/affinity.db`），共享 token 从 `token_env` 指定的环境变量取出后即从 `os.environ` 删除，执行器子进程看不到。Hub 只说明文 ws，跨网络时前面要有 TLS。节点掉线、未连上或没有空槽（含 `workspace_busy`）时，Run 最多等 `node_wait_seconds`，期间可被新事件中断；超时后按执行失败处理（不是无限期保 `pending`）。
- **生命周期**：`RunnerHandle`（`app/bootstrap.py`）每进程一个，`AlwaysOnDaemon.run()` / `serve_configured()` 在第一个 Run 之前 `start()`、最后一个 Run 结束并回收进程组之后 `stop()`。热加载各代共用同一个 handle：项目与执行器的变化对之后的 Run 生效，`[runner]` 本身的变化要重启（`executors` 除外）。`--mcp-stdio` 伴随进程不打开 Runner。
- **尚未包含**：会话结案时自动 `reclaim_workspace`（Runner 的 worktree 目前由人回收）；常驻 Relay 节点的启动入口（`RunnerRelayAgent` 需自行装配，`python -m runner.service` 只接受 §9 的试点形状）；Orca 可见终端驱动；Bridge 的 prompt 只含事件正文，不含 Inbox Memory 的 `instructions.md` 等。

## 8. 架构门禁

- `.importlinter` 契约 `runner-zero-business-deps` 禁止 `runner` 导入 `agent_runtime`、`taskboard`、`switchboard`、`workshop`、`memory_hub`、`ledger`；
- `pyrightconfig.json` 将 `src/runner` 纳入 strict 类型检查；
- `tests/test_runner_stage1.py` 与 `tests/test_runner_stage2.py` 覆盖协议、AST 隔离、Worktree 三道保险、看门狗、跨网反向 WS 调度与 `conn_epoch` 防脑裂；`tests/test_runner_safety.py` 覆盖第 6 节的不变量：取消后进程组（含忽略 `SIGTERM` 的孙进程）被回收、`workspace_busy`、Bridge 被取消时中断并等待、只读 Run 的独立 worktree 与还原、顶替连接时在途 Run 失败并释放槽位、Orca 回执路径与非法回执、槽位满时重复提交同一 `run_id` 仍被接受且只执行一次、ack 迟到仍返回结果、同一 `run_id` 的重叠等待只提交一次、worktree 创建不阻塞事件循环、并发解析同名提示得到不同目录；`tests/test_runner_stage1.py` 另覆盖 Bridge 在 `block` 与 `defer` 策略下对可调工具或工具能力未知的执行器不调用 `runner.run()`；`tests/test_runner_hardening.py` 覆盖子进程环境不带凭据（含驱动附加变量）、输入背压下超时与中断仍生效、交互请求被拒且不启动 CLI、握手后静默的节点被断开且在途 Run 失败、未绑定工作区在首个 ACK 前已认领（延迟 ACK 的双节点）且被拒后释放、多槽节点拒绝首单时次单不会派到别的节点、带凭据的代理 URL（继承与驱动两条路径）被拒且错误信息不含密码、重试已结束的 `run_id` 返回缓存结果不再执行、Hub 与 Daemon 拒绝把 `run_id` 复用于别的调用方或工作区、别的连接发来的回复被忽略、回收与被取消的 Run 在 worktree 线程跑完前不放开工作区、只读 Run 结束后删除其 detached worktree；名称撞上只读目录的写工作区与被指向写目录的只读 Run 都不受影响（后者被拒）、断线时仍在创建工作区的节点重连后亲和不变、改了 prompt / `interactive` / 原生会话 ID 的同一 `run_id` 在 Hub 与 Daemon 都被拒、`workspace_busy` 后同一 `run_id` 重试会执行、其他调用方不能中断或查询别人的 Run（同进程与 Relay）、日志打不开时不留进程、超过流读取上限的单行完整读出、输出读取失败时 Run 失败。`tests/test_runner_tool_gate.py` 覆盖逐次工具授权：三态经 Bridge、Daemon 与 CLI 钩子（同进程与 Relay）、批准 / 拒绝 / 改参 / 重试后的恢复、未挂授权与钩子未加载时拒绝、`ToolGate` 与钩子脚本的异常路径、三种 CLI 的启动参数；`tests/test_runner_composition.py` 覆盖组装根：缺省关闭时不变、两种模式经 `AgentWorker` 端到端、审批恢复、停机与热加载、配置校验。`tests/test_runner_workspace_env.py` 以记录环境的 git / orca 垫片与合成凭据覆盖工作区命令：白名单、hook、正常的创建 / 校验 / 回收、带凭据代理的拒绝与恢复。`tests/test_runner_ledger.py` 经 Bridge、`AgentWorker` 与真实 `RunLedger` 端到端校验：真实 `slots_full` 拒绝记为 `rate_limit` 且仍按已交付结果处理，`RunRecord.runner_id` 是执行节点，新事件中断的 Run 也记在执行节点上、原批次与新事件合并重跑且被中断的 Run 不产生交付消息。
- `tests/test_runner_service.py` 覆盖第 9 节的 Runner 侧：校验程序的结果可用 git 与 SHA-256 独立复算、异常提交与请求一律失败、服务配置与令牌文件的限制、单实例锁、Relay 重启后重连、`SIGTERM` 退出、握手中断不终止重连、节点接纳与错误令牌；`tests/test_runner_pilot.py` 覆盖 Hibernaut 侧，并以 Hibernaut 与 Runner 两个真实进程端到端验证：账本追溯、受控中断、强制停止、Runner 停止、热加载关闭与恢复、遗留事件、Relay 不可用时的原路径。

## 9. 只读试点（默认关闭）

一条真实任务经本机独立 Runner 核验候选提交里的五份 `schemas/runner/v1` 协议文件。用途是验证「派发 → 执行 → 账本 → 中断 → 回退」整条链路，不引入会调工具的执行器。部署、验证、回退步骤与异常处理见 [`deploy/runner/README.md`](../deploy/runner/README.md)，均由人执行。与 §7 的 `[runner]` 互不依赖：各有各的 Relay、令牌与节点，可只开其一；`[runner]` 为 `relay` 时两个端口不能相同。

- **路由**：`hibernaut.toml` 的 `[runner_pilot]`，`enabled` 默认 `false`。打开后只有 `inbox_id` 指定的任务级 Inbox（`<项目>:<任务>`，不能是 `proj:` / `user:`、任一项目的主 Inbox 或 Drive 默认 Inbox）走试点；该 Inbox 里指名其他执行器、或会话仍绑定其他执行器的事件保持 `pending`，其他 Inbox 指名 `runner-protocol-check` 的事件以「未执行」结束，所在 Inbox 的绑定有白名单时按白名单拒绝。`runner-protocol-check` 是保留名，`[executors]` 与 `[agents]` 都不能声明。
- **与 Inbox 绑定、转投**：打开期间试点 Inbox 不按 `[[projects.<name>.inbox_bindings]]`（不属任何项目时为 `[[runtime.inbox_bindings]]`）选 Agent，只有 `runner-protocol-check` 一个。能转投到它的 Inbox 与关闭时相同（同项目或同一条 runtime 绑定），不被任何项目或绑定覆盖时不接受转投。别的 Agent 转投到该 Inbox 时指名其他 Agent 会被当场拒绝；转投的任务不带 `candidate_commit`，以失败结束并按 `return_to` 回投给上级。关闭后恢复原绑定。主机地址不可配置。Inbox 仍有其他执行器被停机摘下的 Run 时，试点 Worker 拒绝开工，等那些 Run 了结。
- **任务输入**：`payload.candidate_commit`（40 位小写提交号）与可选的 `payload.hold_seconds`（0–60 的整数，只用于中断演练，不影响结果）。同批事件只能指向一个提交，`hold_seconds` 以最后一条为准。发给 Runner 的 prompt 只由这两项构成；载荷里的正文、工作目录、执行器参数、`readonly`、`interactive` 都不采用，派发固定为只读、非交互、不续接。
- **校验程序**：`src/runner/protocol_check.py`，以 `python -I -S -B` 运行，只用标准库，文件清单写死；从指定提交的 git 对象读取（`--no-replace-objects`、`GIT_NO_LAZY_FETCH=1`，不联网，不读工作区文件）。提交须存在且是 commit；五个文件齐全、模式 `100644`、不超过 1 MiB、合法 JSON 对象且无重复键、声明 Draft 2020-12、`$id` 指向自身。任何一项不符即退出码 1、不输出摘要。五个文件是 `runner.capabilities`、`runner.interrupt`、`runner.reclaim`、`runner.run_request`、`runner.run_result`；目录里的其他文件（如 `runner.tool_authorize`，试点不用逐次工具授权）不在校验与摘要内。
- **结果可复算**：报告含提交号、逐文件 SHA-256 与汇总摘要（清单格式同 `shasum -a 256`，按路径排序）。Hibernaut 收到后核对提交号、五个路径，并重算汇总摘要，不符记 `bad_output`；Outbox 消息附复算命令。
- **Runner 服务**：`python -m runner.service --config runner.toml`，由 launchd 监管。配置只接受试点形状：一个槽位、`readonly_only = true`、git 后端、无交互、只认 `hibernaut` 调用、恰好一个项目、Relay 地址固定为 `ws://127.0.0.1:<端口>/ws/runner`；唯一的驱动是校验程序，没有配置项能加第二个。`readonly_only` 的节点拒绝写工作区（`permission_denied`）。`state_dir/runner.lock` 保证单实例。`SIGTERM` 先结束在途 Run 的进程组并删除只读 worktree。子进程环境按 §6 的白名单。
- **Relay**：Hibernaut 进程内的 `RunnerPilotRelay`，绑定 `127.0.0.1`。两端从同一份 0600 令牌文件读取共享令牌（不经环境变量）；令牌不符、`runner_id` 不符、槽位不是 1、执行器不是恰好校验程序、命名空间或项目不符的连接以 4001 关闭，心跳里改报也会断开。令牌不写入日志。只读 Run 不产生亲和绑定，Relay 不落盘。
- **权限**：每次派发前经 `PermissionPort.check("runner.protocol_check.launch")`，非 `allow` 不派发。`[runner]` 启用时，其 `block_tools` 对这次检查同样生效。
- **账本**：`runner_id` 为节点；终态为 `emit` / `done`、`emit` / `failed` 或 `interrupted`；`log_ref` 指向本机账本日志，其中 `=== runner log ===` 一行记录节点上的日志位置、Runner `run_id`、SHA-256 与字节数。
- **中断**：试点 Worker 用 `interrupt` 策略。新事件到达或强制停机时，Bridge 让节点结束 Run 并等到终态（进程组已回收、只读 worktree 已删）才放手；账本记 `interrupted`，Outbox 不写任何内容，原批次回到 `pending` 与新事件合并重跑。尚未派发（在等节点）的 Run 直接取消，账本不写节点。
- **失败不会卡住**：节点 30 秒内不可用、连接中断、校验不通过、结果不符都以 `Emit(status="failed")` 结束并关闭会话；不自动重试，由人重放。
- **关闭与回退**：热加载改 `enabled = false` 后，在途 Run 用完 Relay 再关，新事件回到原路由；会话已随每个结果关闭，同一 `correlation_key` 的后续事件走原执行器。遗留的试点事件由占位执行器了结。Relay 打不开（令牌、端口）时只影响试点 Inbox：冷启动保持 `pending`，热加载开启则拒绝本次配置。
- **不做**：写工作区、会调工具的 CLI、多节点、非回环地址、自动 fetch 候选提交。
