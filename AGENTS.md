# AGENTS.md — Hibernaut 开发与架构规范

本文档是 Hibernaut（Always-on Agent Runtime）代码仓库的统一开发与编程 Agent 入口规范。任何修改在提交前必须满足以下 6 项硬约束；完整架构说明见 [`docs/architecture.md`](docs/architecture.md)。

## 1. L0–L3 单向依赖铁律

依赖箭头严格由外向内单向指向：**`L2 engine/`（调用方）与 `L3 adapters/`（实现方）并列指向 `L1 core/ports.py` 与 `L0 core/models.py`**，`app/bootstrap.py`（及 `app/main.py`）是唯一的组装根（Composition Root）。

- **L0 (`core/models.py`)**：零外部依赖，仅允许 Python 标准库；禁止导入 `core/ports.py` 或任何外层模块。
- **L1 (`core/ports.py`)**：仅允许 Python 标准库 + `core/models.py`。
- **L2 (`engine/`)**：仅允许 Python 标准库 + `core/`；**严禁反向 `import adapters/` 或 `app/`**。
- **L3 (`adapters/` + `app/`)**：实现 L1 `Protocol` 端口；禁止绕过 L1 端口让 L2 直接耦合具体数据库、LLM SDK 或外部通道。
- **`src/switchboard/`**：独立包，只依赖标准库与自身；禁止导入 `agent_runtime`、`taskboard`、`runner`、`memory_hub`（`.importlinter` 契约 `switchboard-zero-business-deps`）。
- **`src/ccpeer/`**：独立包，只依赖标准库与自身；禁止导入 `agent_runtime`、`switchboard`、`taskboard`、`runner`、`memory_hub`（`.importlinter` 契约 `ccpeer-zero-business-deps`）。
- **`src/runner/`**：独立包，只依赖标准库与自身；禁止导入 `agent_runtime`、`taskboard`、`switchboard`、`workshop`、`memory_hub`（`.importlinter` 契约 `runner-zero-business-deps`）。控制面经 `agent_runtime/adapters/runner_bridge.py` 单向依赖它。

## 2. 核心词汇表与并发红线

术语以 [`docs/glossary.md`](docs/glossary.md) 为唯一基准（新增或修改术语只改该文件），核心区分如下：

| 术语 | 精确定义 | 作用域 / 约束 |
| :--- | :--- | :--- |
| **Event Inbox** | 全局共享的物理事件表（如 SQLite `event_inbox`） | 全局 1 套，存放所有 `inbox_id` 的事件 |
| **Inbox** | Event Inbox 内的逻辑并发队列（Actor Mailbox） | 同 Inbox 串行单活，不同 Inbox 并发 |
| **`inbox_id`** | 逻辑 Inbox 的一等路由键与租约锁主键（`worker_leases`） | L3 据此路由独立的项目 Memory 与代码工作区 |
| **`session_id`** | 分区内部的对话转录与 `Suspend` 断点上下文单元 | 隶属某个 `inbox_id`，一个 `inbox_id` 下可有多个 `session_id` |
| **Run / Lease** | 单次唤醒执行周期 / 以 `inbox_id` 为主键的限时互斥租约 | `Suspend` 必须保存断点并立刻释放租约，不得阻塞 Worker |

- **多粒度共存**：单用户（`user:main`）、按项目（`proj:<name>`）、按任务（`<project>:<issue>`）三种 `inbox_id` 粒度在同一套共享底座中共存。
- **并发红线**：**严禁两个不同 `inbox_id` 同时挂载同一个本地代码目录**。同一项目拆分为多个任务级 `inbox_id` 并行时，必须为每个分区绑定独立的 `git worktree`；全局 `global/instructions.md` 仅允许只读共享。

## 3. 安全与幂等契约

- **零真实密钥**：Agent 运行进程严禁直接持有外部服务真实密钥/Token，对外鉴权请求必须经由外置 `CredentialProxyClient` 转发注入。
- **全量权限门禁**：执行器内的每一次工具调用均须先经 `PermissionPort.check()` 判定（`allow` / `block` / `defer`），不可绕过。
- **路径防穿越**：所有文件系统 `MemoryPort` 实现必须校验解析后路径位于分区根目录内，拒绝 `../` 越权读写；全局 `instructions.md` 对分区运行态只读。
- **入队幂等去重**：`InboxPort.append()` 必须按 `(inbox_id, source, external_id)` 幂等去重，重复事件标记为 `dropped` 且不得进入 `fetch_pending()`。
- **显式三态终态与中断安全**：`ExecutorPort.execute()` 必须返回 `RunOutcome = Silent | Emit | Suspend`，L2 编排层必须使用 `assert_never` 做穷尽匹配；`interrupt` 中止的 Run 严禁向 Outbox 写入半成品消息，原批次必须保持 `pending` 与新事件合并重启。

## 4. 精简代码注释规范

- **禁止无信息量 docstring**：不要为自解释的模块、类、字段或方法编写复述名称的 `"""..."""` 文档字符串。
- **空类写法**：无字段的数据类或标记类使用 `pass`（如 `class Silent: pass`）。
- **只留高信号注释**：仅在需要解释非显而易见的约束、并发不变量、安全边界或因果顺序时写简短行内注释。

## 5. 提交前必跑的三道门禁命令

每次提交代码前，必须在仓库根目录依次运行并全绿通过：

```bash
lint-imports
pytest -v
pyright
```

- `lint-imports`：验证 `.importlinter` 全部架构分层与反向依赖契约（0 broken）。
- `pytest -v`：验证 AST 静态边界测试、Port 契约测试、多 `inbox_id` 并发隔离及 `NewEventPolicy`（`defer`/`fold`/`interrupt`）端到端测试。
- `pyright`：对 `src/agent_runtime/core`、`src/agent_runtime/engine`、`src/switchboard`、`src/ccpeer`、`src/taskboard` 与 `src/runner` 执行 strict 静态类型检查（0 errors, 0 warnings）。

## 6. 文档增量协作闭环规范

- **详细流程文档**：登记在 [`guidance.toml`](guidance.toml)，按任务的 `activities` / 标签选取（如版本规则 [`docs/process/versioning.md`](docs/process/versioning.md)：`versioning`、`release`）。SDE 不改 `__version__`，交付回执报 `change_kind` 与理由，版本由 Hibernaut 交付步骤决定。

- **Drive 基准只读**：Google Drive 原基准设计文档《Always-on Agent 整洁架构设计方案（Task 交接文档）》保持只读不动，不直接覆写。
- **增量追踪**：新增架构讨论与待落地改动先记录在 Google Drive《Always-on Agent 架构增量变更（待落地）》。
- **Git 文档同步与闭环清理**：**代码落地时必须同步更新仓库内的 `docs/architecture.md` 与 `README.md`**；待三道门禁全绿、推送至 `main` 并经确认后，再从 Drive《Always-on Agent 架构增量变更（待落地）》中清空已落地条目。
