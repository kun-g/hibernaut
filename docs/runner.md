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

## 6. 架构门禁

- `.importlinter` 契约 `runner-zero-business-deps` 禁止 `runner` 导入 `agent_runtime`、`taskboard`、`switchboard`、`workshop`、`memory_hub`；
- `pyrightconfig.json` 将 `src/runner` 纳入 strict 类型检查；
- `tests/test_runner_stage1.py` 与 `tests/test_runner_stage2.py` 覆盖协议、AST 隔离、Worktree 三道保险、看门狗、跨网反向 WS 调度与 `conn_epoch` 防脑裂。
