# Runner 执行面架构设计文档（v1.0 定稿）

本文档记录 Always-on Agent 生态中 **Runner（执行面 / 执行节点）** 模块的定稿设计（Q0–Q4 共 15 项架构决策），配套交互式双向透视蓝图见 [`docs/runner_blueprint.html`](runner_blueprint.html)，跨节点 JSON Schema 契约见 [`schemas/runner/v1/`](../schemas/runner/v1/)。

## 1. 核心定位与四条边界（Q0）

- **定位（0A 方案 A）**：无业务语义的通用代码执行宿主（Execution Host），部署在 Mac 本机或远端 VPS，独占磁盘工作区与子进程；一台 Runner 托管多个 Executor CLI（`claude` / `grok` / `cursor-agent` / `dsh`）。
- **代码拆分线（0B 方案 A）**：底层原子原语（Worktree 隔离回收、进程监管、CLI 启动、只读指纹校验）下沉至 `src/runner/`；多轮业务编排（`sde.py` 评审回路、`PRDelivery`）留在 Hibernaut 控制面。
- **三关回收 PR 核验归属（0C 方案 A）**：控制面先核验 `count_active_by_work_key == 0` 及远端无开着 PR（squash 合并附带 `merged_head_sha`），Runner 零 `gh` 凭据依赖，纯本地核验树干净 + `HEAD` 已推送或匹配 `merged_head_sha`。

## 2. `RunnerPort` 协议与双模传输（Q1）

遵循 [`ADR-0001`](adr/0001-cross-project-protocol-schema.md)（JSON Schema Draft 2020-12，`schema_version = "runner.v1"`）：
- **终态语义（1A / 1C 方案 A）**：Runner 仅返回执行事实（`succeeded | failed | interrupted | timed_out | refused` + `structured_output` + `error_kind` + `log_ref`），控制面通过薄适配层（如 `src/agent_runtime/adapters/runner_bridge.py`）翻译回业务终态。
- **4 个核心方法**：`capabilities()`、`run(RunRequest)`（`run_id` 幂等键）、`interrupt(InterruptRequest)`、`reclaim_workspace(ReclaimRequest)`。
- **双模传输（1B 方案 A）**：Stage 1 `InProcessRunnerClient` 同进程直调；Stage 2 `RunnerRelayHub` + `RunnerRelayAgent` 纯 stdlib 反向 WebSocket（RFC 6455，`conn_epoch` 防脑裂 + Close 4009 + `run.submit/accepted/completed/query`）。

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

- **进程组随 Run 结束**：`ProcessSupervisor.execute` 不论正常退出、超时、`interrupt` 还是协程被取消，都先对子进程所在进程组（`start_new_session=True`，组号即子进程 pid）发 `SIGTERM`，超过 `grace_seconds` 再 `SIGKILL`，并在首进程退出后对整组补发 `SIGKILL`，直到组内没有进程才返回或继续抛出取消。被取消的 Run 记为 `interrupted`，以同一 `run_id` 等待它的调用方也拿到这个终态。
- **一个工作区同时只有一个 Run**：`RunnerDaemon` 按 `(namespace, project_key, owner_scope, work_key, readonly)` 占位，占位在 worktree 解析之前取得，并保持到进程组回收完毕；第二个 `run_id` 落到同一工作区时返回 `refused`（`error_kind = "rate_limit"`，`error_summary` 以 `workspace_busy` 开头）。仍有 Run 占用时 `reclaim_workspace` 返回 `blocked`（`run_active`）。
- **只读 Run 不挂主检出**：`readonly=True` 的 Run 使用按上述四元组派生的独立 detached worktree（`<prefix>-ro-<hash>`，不进 `workspaces` 表，也不上报为亲和），每次运行前重置到任务 worktree 已提交的 `HEAD`（没有任务 worktree 时为 `default_base_ref`）。运行后指纹变化即 `checkout --force` + `clean -ffdx` 还原，再返回 `failed`（`permission_denied`）。回收该四元组时一并删除。
- **HB 适配层**：`RunnerExecutorBridge.execute` 被 Worker 取消（`interrupt` 策略或停机）时，向 Runner 发 `interrupt` 并等到 Run 出终态再继续抛出取消；Runner 尚未登记该 `run_id`（`not_found`）时按轮询间隔重发。`translate_run_result` 给每个结果带上 `RunTelemetry`（`stdout_excerpt`、`error_summary` 作 stderr、`exit_code`、Runner 的结构化 `error_kind`、执行节点 `runner_id`），Worker 的执行账本据此写运行日志与计费；`RunRecord.error_kind` 直接用 Runner 给的分类（如 `slots_full` 记 `rate_limit`），没有时才按 stderr 与退出码推断；`RunRecord.runner_id` 记实际执行的节点。Run 被中断时 Bridge 抛出带同样 telemetry 的 `RunCancelled`（`CancelledError` 子类），Worker 在执行器调用处取下它，`interrupted` 记录因此也落在实际节点上；原批次照常重新入队，不写 Outbox。
- **反向 WebSocket 换连接**：Hub 按提交时的 `conn_epoch` 记录每个在途 Run。同一 `runner_id` 的新连接顶替旧连接（Close 4009）或旧连接断开时，旧连接上的在途 Run 以 `RunnerConnectionLostError`（`ConnectionError` 子类）结束并释放预扣槽位；Runner 侧随连接取消这些 Run 并回收进程组，调用方换新 `run_id` 重试，进程组回收前会收到 `workspace_busy`。
- **`run_id` 是跨等待者的幂等键**：Hub 对同一 `run_id` 只提交一次、只预扣一个槽位，后来的调用者等待同一次提交；某个调用者超时或被取消不影响其余调用者，也不撤销提交。`run.submit` 发出后不设 ack 期限（Runner 可能已在执行），只有连接断开或被顶替才以 `RunnerConnectionLostError` 结束。Agent 收到本机已在执行或已完成的 `run_id` 时直接接受并交给 `RunnerDaemon.run` 挂接已有执行，不因槽位满回 `slots_full`。
- **git / orca 子进程不占事件循环**：`RunnerDaemon` 经 `asyncio.to_thread` 调用 worktree 解析、写入校验、指纹、只读还原与回收，Agent 的 `hello` 读取 `ready_work_keys` 也在线程里，其他 Run 的看门狗、中断与进程组回收照常推进。不同 `work_key` 的解析因此可能并发，分配目录名与分支名时把表里已登记（含 `creating`）的名字视为占用。
- **子进程环境只放白名单**：`build_child_env` 只从 Runner 自身环境带入 `PATH`、`HOME`、用户与 shell、临时目录、时区、`LANG` / `LC_*`、终端、`XDG_*` 与代理变量（`CHILD_ENV_ALLOWLIST`），名字像凭据的（含 `TOKEN`、`SECRET`、`PASSWORD`、`CREDENTIAL`、`API_KEY`、`ACCESS_KEY`、`AUTH` 等）即使在白名单前缀下也去掉。驱动配置的附加变量可以设其他项，但名字像凭据或与租约变量同名时整次 Run 以 `refused`（`permission_denied`）结束。名字允许、值却是带用户信息的 URL（如 `http://user:pass@proxy:8080` 这样的代理；名字以 `_PROXY` 结尾的变量即使省略协议前缀写成 `user:pass@proxy:8080` 也按代理地址解析）时，继承环境与驱动附加变量同样整次拒绝（不静默丢弃，免得节点在无代理下跑），错误信息只写变量名。外部鉴权只经 `credential_lease`（`RUNNER_BROKER_URL` / `RUNNER_LEASE_TOKEN`）。
- **输入写入不挡看门狗**：经 stdin 传 prompt 的驱动，写入在独立任务里进行，与输出读取、硬超时、静默看门狗和 `interrupt` 检查并发；子进程不读输入时超时与中断照常生效，结束时先取消写入再回收进程组。
- **交互 Run 不降级**：内置 `TemplateCLIDriver` 只会起后台子进程，能力里的 `interactive_surface` 恒为 `none`；`interactive=True` 的请求在节点上以 `refused`（`config_error`）结束，Hub 路由也要求执行器本身声明 `orca`。在 Orca 可见终端里启动并监管 CLI 的驱动尚未实现。
- **写工作区先认领后发送、按工作区串行派发**：同一四元组的写请求从选节点到收到 ACK 这一段逐个进行，后来的请求等前一个的节点确认后再派发。选定节点后、第一次 `await` 之前，若该四元组还没有亲和绑定就先写入绑定，同一工作区随后的写请求（不同 `run_id`）因此只会派到同一节点（槽位满时报 `NoEligibleRunnerError`，不换机）。节点明确回 `accepted = false` 才说明没执行；因为派发是串行的，此时没有别的请求依赖这次认领，删除这次新建的绑定是安全的（多槽节点上不会出现“首单被拒、次单仍在途”）；连接断开等无回答的情形保留绑定，由节点重连时 `hello` 的 `ready_work_keys` 对账清理。
- **心跳超时即断开**：Hub 每个心跳周期扫一次，超过两个周期没有 `pong` 的节点除了标离线，还按当前连接代次关闭连接并让其在途 `run.submit` / 结果等待以 `RunnerConnectionLostError` 结束，与连接断开同一路径；握手后不再回应、也不断开的节点不会让调用方一直等。
- **会调工具的执行器不经 Bridge 运行**：逐次工具授权（CLI 内每一次工具调用先经 `PermissionPort.check()`）尚未接到 Runner，因此 `RunnerExecutorBridge` 默认把执行器视为“工具能力未知”，在调用 `runner.run()` 之前就返回 `Emit(status="refused")`，不论 Inbox 策略是 `allow`、`block` 还是 `defer`；只有构造时显式声明 `executor_calls_tools=False` 的执行器才会提交。`claude` 等可调工具的 CLI 要等逐次授权接通后才能经 Bridge 运行。
- **Orca 后端**：`orca worktree create` 带 `--repo path:<主检出>`、`--base-branch <解析后的提交>`、`--no-parent`、`--setup skip`、`--json`，以回执里的绝对路径与分支（须为请求的分支或其 `-N` 后缀）为准登记，回收时按该路径 `orca worktree rm`。

## 7. 架构门禁

- `.importlinter` 契约 `runner-zero-business-deps` 禁止 `runner` 导入 `agent_runtime`、`taskboard`、`switchboard`、`workshop`、`memory_hub`、`ledger`；
- `pyrightconfig.json` 将 `src/runner` 纳入 strict 类型检查；
- `tests/test_runner_stage1.py` 与 `tests/test_runner_stage2.py` 覆盖协议、AST 隔离、Worktree 三道保险、看门狗、跨网反向 WS 调度与 `conn_epoch` 防脑裂；`tests/test_runner_safety.py` 覆盖第 6 节的不变量：取消后进程组（含忽略 `SIGTERM` 的孙进程）被回收、`workspace_busy`、Bridge 被取消时中断并等待、只读 Run 的独立 worktree 与还原、顶替连接时在途 Run 失败并释放槽位、Orca 回执路径与非法回执、槽位满时重复提交同一 `run_id` 仍被接受且只执行一次、ack 迟到仍返回结果、同一 `run_id` 的重叠等待只提交一次、worktree 创建不阻塞事件循环、并发解析同名提示得到不同目录；`tests/test_runner_stage1.py` 另覆盖 Bridge 在 `block` 与 `defer` 策略下对可调工具或工具能力未知的执行器不调用 `runner.run()`；`tests/test_runner_hardening.py` 覆盖子进程环境不带凭据（含驱动附加变量）、输入背压下超时与中断仍生效、交互请求被拒且不启动 CLI、握手后静默的节点被断开且在途 Run 失败、未绑定工作区在首个 ACK 前已认领（延迟 ACK 的双节点）且被拒后释放、多槽节点拒绝首单时次单不会派到别的节点、带凭据的代理 URL（继承与驱动两条路径）被拒且错误信息不含密码。`tests/test_runner_ledger.py` 经 Bridge、`AgentWorker` 与真实 `RunLedger` 端到端校验：真实 `slots_full` 拒绝记为 `rate_limit` 且仍按已交付结果处理，`RunRecord.runner_id` 是执行节点，新事件中断的 Run 也记在执行节点上、原批次与新事件合并重跑且被中断的 Run 不产生交付消息。
