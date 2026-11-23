# Taskboard 多源任务归集与 DAG 依赖编排架构设计方案（定稿版）

本文是人 × Agent 协作平台中 Taskboard（TB）子系统的定稿设计，继承以下基线文档的约定与数据结构：

- 《Always-on Agent 生态分域架构蓝图（Switchboard · Taskboard · Hibernaut 及扩展模块）》（Google Drive）
- 《Always-on Agent 核心词汇表（Glossary）》（Google Drive；仓库内术语以 [`glossary.md`](glossary.md) 为准）
- 《Switchboard 外部事件归一化交换机架构设计方案》（Google Drive；仓库内实现说明见 [`architecture.md` §7](architecture.md#7-switchboardphase-1--phase-2-relay--stage-ab-接入外部事件归一化交换机)）

Phase 1 代码由 PR #58（`db0c9b4`）合入：包在 `src/taskboard/`，事件 Schema 在 `schemas/taskboard/`，测试在 `tests/taskboard/`。本文中的路径、类名、契约名、取值范围与 DDL 均按该实现核对；与 Drive 定稿附件不一致之处一律以代码为准，差异列在文末 [附录：与 Drive 定稿附件的差异](#附录与-drive-定稿附件的差异)。实现层面的逐条行为说明见 [`architecture.md` §10](architecture.md#10-taskboardphase-1多源任务归集与-dag-依赖编排)。

---

## 一、定位、职责边界与依赖铁律

### 1.1 一句话定位

**Taskboard（TB）是人 × Agent 协作平台的「多源任务事实源 + DAG 依赖裁判」**——只管把任务收齐、把先后依赖判准、在前置清零那一刻发出开工信号；发完即交棒，不做调度队列、不做工作流引擎、不替下游操心执行。

### 1.2 职责边界

| 架构维度 | TB 管什么（边界以内） | TB 不管什么（责任归属） |
| :--- | :--- | :--- |
| **任务入库** | • 1:N `task_source_links` 多源事件归并<br>• 基于时间戳与乱序水位线的幂等更新<br>• 维护 `manual_override_fields` 保护手工修改的字段 | • 入站 Webhook 验签、解包（**Switchboard**）<br>• 外部平台 SDK 交互与 API 速率限制（**Switchboard**）<br>• 跨系统消息传输去重与 ACK（**Switchboard**） |
| **状态模型** | • 5 态：`backlog` \| `ready` \| `in_progress` \| `done` \| `cancelled`<br>• 单字段 `assignee` 前缀（`agent:*` 与 `human:*`）<br>• 透传展示字段 `task_type`、`stage` | • 业务工种的子状态机（**执行方 / Runner**）<br>• 运行中途挂起审批 `Session.suspended`（**Hibernaut**）<br>• 人工协同界面渲染（**UI / Frontend**） |
| **DAG 编排** | • `blocks` / `related` 边的建立与严格存在性校验<br>• SQLite 写事务内基于 DFS 的实时判环<br>• `ready → in_progress` 依赖守卫<br>• 上游 `done` 后扫描解锁下游（CAS 防双派）<br>• 上游取消、返工时的依赖失效旁路告警 | • 父子状态自动级联聚合（禁止，由业务层处理）<br>• 自动创建虚拟占位单或未决节点（禁止）<br>• 运行时动态分支评估（**Runner / Hibernaut**） |
| **派发与执行** | • 前置清零时按 `(priority ASC, ready_at ASC)` 排序<br>• `agent:*` 立即发出 `work.ready`<br>• `human:*` 推送人工待办通知<br>• 事件投递到 Switchboard 事件总线即止 | • 在途限流、并发排队（**Hibernaut**）<br>• Agent 分配锁与租约（**Hibernaut**）<br>• 执行超时重派、失败重试（**Hibernaut / Runner**）<br>• Worktree 隔离与 CLI 执行（**Runner**） |
| **横向底座** | • 旁路上报状态迁移审计日志（`AuditSinkPort`）<br>• 审计故障静默降级，不影响核心写事务 | • 凭据保管与注入（**Credential Broker**）<br>• 跨会话长文本与向量检索（**Memory Hub**）<br>• 禁止直接 `import` Hibernaut / Runner 内部代码 |

### 1.3 内部整洁分层

Taskboard 按整洁架构分 L0–L3 四层，最外是组装根 `app/`，依赖只能由外向内：

```text
app（组装根）→ L3 adapters → L2 engine → L1 ports → L0 core
```

```text
+---------------------------------------------------------------------------+
|  L3 Adapters   SQLiteTaskStore · SwitchboardAdapter · SafeAuditAdapter    |
|  +---------------------------------------------------------------------+  |
|  |  L2 Engine   IngestionService · UnlockDispatcher · FlowExpander     |  |
|  |  +---------------------------------------------------------------+  |  |
|  |  |  L1 Ports  TaskStorePort · EventPublisherPort · AuditSinkPort |  |  |
|  |  |  +---------------------------------------------------------+  |  |  |
|  |  |  |  L0 Core  TaskItem · 5 态状态机 · 字段合并 · DFS 判环   |  |  |  |
|  |  |  +---------------------------------------------------------+  |  |  |
|  |  +---------------------------------------------------------------+  |  |
|  +---------------------------------------------------------------------+  |
+---------------------------------------------------------------------------+
```

1. **L0 Core（`src/taskboard/core/`）**：纯 Python，零 I/O。`TaskItem` 等实体、领域错误、5 态迁移规则、三层字段合并、纯内存 DFS 判环与 Flow 编译。
2. **L1 Ports（`src/taskboard/ports.py`）**：`typing.Protocol` 端口：`TaskStorePort`、`EventPublisherPort`、`AuditSinkPort`。
3. **L2 Engine（`src/taskboard/engine/`）**：`IngestionService`（入站事件与 7 个 `tb_*` 工具方法）、`UnlockDispatcher`（解锁扫描与派发）、`FlowExpander`（Flow 展开），编排内核与端口。
4. **L3 Adapters（`src/taskboard/adapters/`）**：基于 `sqlite3` 的 `SQLiteTaskStore`、对接 Switchboard `CanonicalEvent` / `EventBusPort` 的 `SwitchboardAdapter`、审计适配器（`SafeAuditAdapter`、`NoopAuditSink`、`RecordingAuditSink`）。
5. **组装根（`src/taskboard/app/`）**：配置解析与服务入口（`python -m taskboard.app`），装配以上各层；任何一层都不得导入它。

过渡期以单仓独立包 `src/taskboard/` 组织，`.importlinter` 约束单向依赖，禁止逆向或跨域调用（见 [7.2.2](#722-importlinter-门禁)）。

---

## 二、5 态调度机、双层状态模型与多源幂等归集

### 2.1 5 态核心状态机

Taskboard 不设传统看板里的 `waiting_human` 与 `blocked` 状态：

- **不要 `blocked`**：前置未完成导致的等待是**派生状态**，查 `task_dependencies` 里 `kind='blocks'` 且上游 `status != 'done'` 的记录即可算出。把 `blocked` 冗余进字段，会在加删边、上游返工时带来状态同步与竞态问题。
- **不要 `waiting_human`**：人工验收、决策本质是执行资源的归属。TB 用 `human:*` 前缀的 `assignee` 表达人工节点；运行中途的挂起审批或等待外部 PR 由 `stage` 字符串与 Hibernaut 的 `Session.suspended` 表达，调度层保持 5 态。

```text
                     +---------------------------------------+
                     |                                       | (Reopen)
                     v                                       |
              +--------------+  Unblocked   +--------------+ |
(work.opened) |              |------------> |              | |
------------->|   backlog    |              |    ready     |-+
              |              |<------------ |              |
              +--------------+   Re-block   +--------------+
                     |                             |
                     |                             | (Dispatch & Claim)
                     |                             v
                     |                      +--------------+
                     |                      |              |
                     |                      | in_progress  |
                     |                      |              |
                     |                      +--------------+
                     |                             |
                     |                             | (execution.reported: complete)
                     v                             v
              +--------------+              +--------------+
              |  cancelled   |              |     done     |
              +--------------+              +--------------+
```

图中只画主路径；完整迁移见下表（`core/state_machine.py` 的 `ALLOWED_TRANSITIONS` 与 `REOPEN_TRANSITIONS`）。

### 2.2 合法状态迁移与守卫

| 起始状态 | 目标状态 | 触发事件 / 条件 | 守卫与系统行为 |
| :--- | :--- | :--- | :--- |
| 无 | `backlog` | `work.opened` / `tb_create_task` | 初始状态未指定或为 `backlog`；指定 `ready` 但有未完成上游时也落入 `backlog`。 |
| 无 | `ready` | `work.opened` / `tb_create_task` | 显式指定 `ready` 且没有未完成的 `blocks` 上游；创建后立即派发。 |
| `backlog` | `ready` | 依赖解开 / 显式迁移 | 必须 `count_incomplete_blockers(task_id) == 0`，否则抛 `BlockedByDependencyError`；通过后写 `ready_at`。 |
| `backlog` | `cancelled` | `work.closed` / 显式迁移 | 任务废弃，不触发下游解锁。 |
| `ready` | `in_progress` | `execution.reported`（`task_action=claim`） | 再次核验无未完成上游，否则抛 `BlockedByDependencyError`；可同时写入 `assignee`、`stage`。 |
| `ready` | `backlog` | 新增未完成的 `blocks` 上游 / 上游重开 | 运行期插入了未完成上游，回退 `backlog`。 |
| `ready` | `cancelled` | `work.closed` / 显式迁移 | 取消就绪任务。 |
| `in_progress` | `in_progress` | 重复的 `claim` 等运行中上报 | 幂等空操作，不算非法迁移。 |
| `in_progress` | `done` | `execution.reported`（`complete`）/ `work.closed`（`state=closed`） | 完成后**当场**触发下游解锁扫描。 |
| `in_progress` | `ready` | `execution.reported`（`release`） | 执行方放弃或人工释放：回到 `ready` 并刷新 `ready_at`，**不派发**（不发 `work.ready`，也不发 `human_todo`）。之后只有作为下游遇到上游完成或删边时才会再派发，届时领新的 `dispatch_seq`。 |
| `in_progress` | `cancelled` | `work.closed`（`state=cancelled`）/ 显式迁移 | 强行终止正在执行的任务。 |
| `done` | `in_progress` | 显式重开（`reopen=True`） | 上游返工；`ready` 的下游退回 `backlog`，`in_progress` / `done` 的下游收到 `dependency_invalidated` 通知。 |
| `done` | `ready` | 显式重开（`reopen=True`） | 返工并回到待派发；下游处理同上。 |
| `cancelled` | `backlog` | 显式重开（`reopen=True`） | 重新激活已取消的任务。 |

注：

- 终态（`done` / `cancelled`）只能经 `reopen=True` 离开；终态上迟到的运行中上报（`claim`、`update_stage`）一律拒绝（`IllegalTransitionError`）。
- 不带 `reopen` 重复进入同一终态（`done→done`、`cancelled→cancelled`）视为重放：状态不变，重新执行完成或取消的扇出，用于补完因发布失败而中断的扇出（见 [4.2](#42-不做自动重派与超时检测)）。
- `work.closed` 的 `state=closed` 在任务处于 `in_progress` 时落 `done`，其他非终态落 `cancelled`；`state=cancelled` 一律落 `cancelled`。

### 2.3 双层状态模型与单字段 `assignee` 前缀

TB 用两层解耦底层调度的简单与上层业务的多样：

1. **第 1 层：核心调度**，由 5 态 `status` 与单字段 `assignee` 决定。`assignee` 用前缀划分命名空间（校验正则 `^(agent|human):[a-z0-9.:_-]+$`，默认 `agent:default`）：
   - `agent:{agent_id}` 或 `agent:role:{role_name}`：派发时（触发路径见 [4.1](#41-不做调度策略没有-dispatchpolicyport)）发出 `work.ready`。
   - `human:{user_id}` 或 `human:group:{group_name}`：派发时发人工待办通知，**不唤醒** Agent 调度。
2. **第 2 层：业务多样性**，由自由字符串 `task_type` 与 `stage` 表达。
   - `task_type`：任务工种（如 `code_review`、`deployment`、`data_analysis`；默认 `general`，Flow 父任务为 `flow_group`）。
   - `stage`：单任务内部的小阶段（如 `syntax_checking`、`building`、`waiting_pr_merge`）。
   - TB 对这两个字段零配置、零校验、零状态跳转，只在接口与事件里原样透传，供 UI 展示与 Hibernaut 感知上下文。跨角色的多步骤流程必须拆成 DAG 节点串联。

### 2.4 多源幂等归集与三层字段所有权

#### 2.4.1 多源归并结构与乱序水位线

外部系统（GitHub Issue、飞书任务、Jira 等）经 Switchboard 把事件投给 TB。TB 用 `task_source_links` 实现一个任务对多个来源（1:N）：

```text
+-------------------------------------------------------+
|                       tasks                           |
|-------------------------------------------------------|
| task_id (PK)        : "tb_tsk_01HJ8Z..."              |
| source_ref (冗余主来源，可空，非空时唯一)              |
|                     : "github://org/repo/issues/42"   |
| last_source_event_at: 1718900000000                   |
+-------------------------------------------------------+
                           ^
                           | 1:N
+-------------------------------------------------------+
|                 task_source_links                     |
|-------------------------------------------------------|
| source_ref (PK) : "github://org/repo/issues/42"       |
| task_id (FK)    : "tb_tsk_01HJ8Z..."                  |
| source          : "github"                            |
| is_primary      : 1                                   |
| last_synced_at  : 1718900000000                       |
+-------------------------------------------------------+
```

- `find_by_source_ref()` 先查 `task_source_links`，再查 `tasks.source_ref`。
- **乱序水位线**：入站事件带 `occurred_at`（Switchboard 信封字段，换算为毫秒）。若早于 `task.last_source_event_at`，TB 视为迟到事件，整条忽略（NO-OP），防止旧状态覆盖新状态；`work.closed` 同样受此约束。

#### 2.4.2 三层字段所有权

```text
+-------------------------------------------------------------------+
|                        Tasks Field Layers                         |
+-------------------------------------------------------------------+
| [Layer 3: State Machine Exclusive]                                |
|   status, dispatch_seq, version, ready_at, claimed_ready_at,      |
|   blocked_reason, last_source_event_at                            |
|   --> 只由状态机与存储写入；手工 patch 直接报错。                 |
+-------------------------------------------------------------------+
| [Layer 2: Manual Override Protected]                              |
|   priority, assignee, preferred_executor, scope, task_type        |
|   --> 被手工修改后记入 manual_override_fields。                   |
|   --> 之后外部 work.updated 自动跳过这些字段。                    |
+-------------------------------------------------------------------+
| [Layer 1: External Mirror]                                        |
|   title, description, artifact_ref, reply_target                  |
|   --> 随外部 work.opened / work.updated 同步。                    |
+-------------------------------------------------------------------+
```

字段集合定义在 `core/merge.py`（`LAYER1_EXTERNAL_MIRROR`、`LAYER2_MANUAL_PROTECTED`、`LAYER3_STATE_MACHINE_EXCLUSIVE`）。另有 `stage`、`parent_task_id` 允许手工修改但不加锁。

人通过 `tb_update_task` 修改第 2 层字段（例如 `priority`）时，TB 把字段名追加进该任务的 `manual_override_fields`。之后 Switchboard 传来 `work.updated`：

- 字段在 `manual_override_fields` 中的，跳过该字段的更新；
- 跳过的字段名作为 `MergeResult.skipped_override_fields` 返回并记入审计；
- `tb_update_task(unlock_fields=[...])` 显式解除字段锁，恢复外部同步。

---

## 三、DAG 依赖图、并发解锁与异常传播

### 3.1 边类型与写入前硬校验

`task_dependencies` 支持两种边类型（`kind`）：

1. `blocks`：**强阻塞依赖**。参与判环、阻塞计数与解锁门禁；所有 `blocks` 上游都 `done` 后，下游才能进入 `ready`。
2. `related`：**弱关联**。只作上下文参考与 UI 展示，可双向，**不判环、不阻塞**调度。

```text
       blocks (Strict DAG)               related (Context Only)
  [Task A] ----------> [Task B]       [Task C] <----------> [Task D]
  (Must be done)     (Blocked)        (Informational link, no blocking)
```

#### 硬校验与并发安全判环

`SQLiteTaskStore.add_dependency(upstream_id, downstream_id, kind)` 在一个 `BEGIN IMMEDIATE` 写事务内依次完成：

1. **严格存在性校验**：上游不存在抛 `UpstreamTaskNotFoundError`，下游不存在抛 `TaskNotFoundError`；直接拒绝，**不建幽灵占位单**。
2. **自环校验**：`upstream_id == downstream_id` 抛 `CycleDetectedError`（`cycle_path` 为 `(id, id)`）。
3. **DFS 判环**（仅 `blocks`）：从 `downstream_id` 出发，沿已有 `kind='blocks'` 的出边做深度优先搜索；能回到 `upstream_id` 则加边会成环，抛 `CycleDetectedError`（携带 `cycle_path`）并回滚事务。

判环算法在 L0（`core/dag.py`），不依赖存储；L3 只把「查某节点的 `blocks` 出边」作为回调传进来：

```python
def detect_cycle(
    upstream_id: str,
    downstream_id: str,
    get_downstream: Callable[[str], Iterable[str]],
) -> tuple[str, ...] | None:
    if upstream_id == downstream_id:
        return (upstream_id, upstream_id)

    visited: set[str] = set()
    stack: list[tuple[str, tuple[str, ...]]] = [(downstream_id, (upstream_id, downstream_id))]

    while stack:
        curr, path = stack.pop()
        if curr == upstream_id:
            return path
        if curr in visited:
            continue
        visited.add(curr)
        for nxt in get_downstream(curr):
            if nxt == upstream_id:
                return (*path, upstream_id)
            if nxt not in visited:
                stack.append((nxt, (*path, nxt)))
    return None
```

`assert_no_cycle()` 在返回路径非空时抛 `CycleDetectedError(cycle_path)`；`SQLiteTaskStore` 传入的回调在同一写事务里执行：

```sql
SELECT downstream_task_id
FROM task_dependencies
WHERE upstream_task_id = ? AND kind = 'blocks';
```

### 3.2 零冗余状态同步与删边联动

- **加边联动**（`UnlockDispatcher.on_dependency_added`，仅对未完成的 `blocks` 上游）：下游在 `backlog` 则不动；在 `ready` 则因新增未完成上游退回 `backlog`；在 `in_progress` 则不中断运行，只落边并发 `dependency_invalidated` 通知。
- **删边联动**（`UnlockDispatcher.on_dependency_removed`）：一条 `blocks` 边被删除后，立即对该下游做未完成阻塞检查：

```sql
SELECT COUNT(*) FROM task_dependencies d
JOIN tasks t ON d.upstream_task_id = t.task_id
WHERE d.downstream_task_id = :downstream_id
  AND d.kind = 'blocks'
  AND t.status != 'done';
```

计数为 `0` 时：下游在 `backlog` 则提升为 `ready` 并派发；已经是 `ready` 的直接派发。Phase 1 没有删除任务的接口；表上的外键 `ON DELETE CASCADE` 保证删除任务时连带删除它的边与来源链接。

### 3.3 上游完成解锁与 CAS 防双派

上游完成（`in_progress → done`）时，系统找出因此解除阻塞的全部下游，并保证每个下游最多解锁、派发一次。

#### 步骤 1：找出所有前置 `blocks` 均已完成的下游

`list_unblocked_downstream()` 用 `NOT EXISTS` 反连接检索：

```sql
SELECT DISTINCT t_down.*
FROM task_dependencies d
JOIN tasks t_down ON d.downstream_task_id = t_down.task_id
WHERE d.upstream_task_id = :completed_task_id
  AND d.kind = 'blocks'
  AND t_down.status = 'backlog'
  AND NOT EXISTS (
      SELECT 1
      FROM task_dependencies d_sub
      JOIN tasks t_sub ON d_sub.upstream_task_id = t_sub.task_id
      WHERE d_sub.downstream_task_id = d.downstream_task_id
        AND d_sub.kind = 'blocks'
        AND t_sub.status != 'done'
  )
ORDER BY t_down.priority ASC, t_down.created_at ASC;
```

#### 步骤 2：CAS 原子提升

对每个候选下游，`try_promote_to_ready()` 在独立的 `BEGIN IMMEDIATE` 事务里按版本比较并交换，并在**同一条** `UPDATE` 里再次确认没有未完成上游：

```sql
UPDATE tasks
SET status = 'ready',
    ready_at = :now,
    blocked_reason = '',
    version = version + 1,
    updated_at = :now
WHERE task_id = :target_task_id
  AND status = 'backlog'
  AND version = :expected_version
  AND NOT EXISTS (
      SELECT 1
      FROM task_dependencies d
      JOIN tasks u ON d.upstream_task_id = u.task_id
      WHERE d.downstream_task_id = tasks.task_id
        AND d.kind = 'blocks'
        AND u.status != 'done'
  );
```

影响行数为 `1` 表示由当前调用解锁，随后派发；为 `0` 表示另一并发调用已处理、或扫描与提升之间新增了未完成上游，当前调用直接跳过（任务留在 `backlog`，等新上游完成再解锁）。`ready_at` 由应用层以 UTC ISO 8601（`%Y-%m-%dT%H:%M:%S.%fZ`）写入。

#### 步骤 3：领取派发序号

派发前 `try_claim_dispatch()` 同样按版本 CAS，把 `dispatch_seq` 加一并记下 `claimed_ready_at = ready_at`。`claimed_ready_at == ready_at` 表示本次 `ready` 已领过序号，重放时不再领，按原 `dispatch_seq` 重发同一 `external_id`，由 Switchboard 去重。

### 3.4 异常传播规则

1. **上游 `cancelled`**：
   - TB **不级联取消下游**（不自动破坏用户定义的任务图）。
   - 下游停留在 `backlog`（上游是 `cancelled` 而非 `done`，未完成计数不归零）。
   - TB 给未终结的下游写 `blocked_reason = "upstream_cancelled"`，并发 `execution.reported` 通知（`outcome_kind="emit"`、`notify_kind="upstream_cancelled"`），由人决定删边、换上游还是取消下游。
2. **上游重开（`done → in_progress` 或 `done → ready`）**：
   - **下游在 `ready`**：未完成上游数从 `0` 变为大于 `0`，下游退回 `backlog`，**当场失去派发资格**。
   - **下游在 `in_progress` 或 `done`**：TB 不强杀执行、不回滚已完成状态，而是发 `notify_kind="dependency_invalidated"` 通知，由 Hibernaut 或 Runner 决定是否取消上下文或重跑下游。
3. **父子任务**：
   - `tasks` 表只保留可选字段 `parent_task_id`，供 UI 看板树形折叠显示。
   - TB **不做父子状态聚合**（子任务全部完成不代表父任务自动完成）。
   - 「所有子步骤完成后再做汇总」必须用 DAG 汇聚显式表达（`Subtask A -> Summary`、`Subtask B -> Summary`）。

---

## 四、就绪直发与零背压

### 4.1 不做调度策略（没有 `DispatchPolicyPort`）

Taskboard 不承担流控、限流与并发管理：

- 没有 `DispatchPolicyPort`、并发度限制或 Agent 忙闲检测。
- **零背压直发**：下列路径让任务进入（或停在）`ready` 后会调用 `UnlockDispatcher.dispatch_ready_task()`，只要无未完成上游就立即派发；一次解锁多个下游时按 `(priority ASC, ready_at ASC)` 排序（`priority` 取值 0–3，数值小者优先）：
  - 新建即为 `ready`（`tb_create_task`、`work.opened`），以及 `work.opened` 重投时任务已是 `ready`；
  - `backlog → ready` 的显式迁移；
  - `done` 经 `reopen=True` 回到 `ready`；
  - 上游完成后解锁的下游，连同该上游下已经是 `ready` 的下游；
  - 删边后前置归零的下游（从 `backlog` 提升，或本就是 `ready`）；
  - Flow 展开后 `ready` 的根节点。

  派发时 `assignee` 为 `agent:*` 发 `work.ready`；为 `human:*` 发 `notify_kind="human_todo"` 的 `execution.reported`。**并非任意 `ready` 任务都会直发**：`in_progress → ready`（`execution.reported` 的 `release`，或 `tb_transition_task(target_status="ready")`）只把状态改回 `ready` 并刷新 `ready_at`，不派发，TB 也没有扫描 `ready` 任务补派发的轮询。
- **排队与限流归属**：Agent 是否空闲、单机并发度、`worker_leases` 租约竞争与 `event_inbox` 缓冲，全部交给下游 **Hibernaut**。TB 发完即交棒。

```text
+------------------+         Ready Event        +-----------------------+
|    Taskboard     | -------------------------> |      Switchboard      |
|  (State & DAG)   |  (No rate-limiting/queue)  |     (Event Router)    |
+------------------+                            +-----------------------+
                                                            |
                                                            v
                                                +-----------------------+
                                                |       Hibernaut       |
                                                | (Queue, Leases, Flow) |
                                                +-----------------------+
```

### 4.2 不做自动重派与超时检测

- TB 没有定时轮询（Cron）检查 `in_progress` 是否超时，也不做超时自动重派。
- **投递即止**：TB 保证 `work.ready` 投到 Switchboard 事件总线，幂等键 `external_id = "tb:{task_id}:d{dispatch_seq}"`。
- **可靠投递**：重发、持久化 ACK、离线 Agent 补推与死信由 **Switchboard**（`delivery_log` + `RelayHub`）保证；执行期间的超时、心跳与断线重试由 **Hibernaut / Runner** 负责。
- **发布失败**：终态先提交、扇出后发布。发布异常包成 `PublishFailedError`，`SwitchboardAdapter.handle_canonical_event()` 返回 `nack`；Switchboard 重投同一入站事件时，TB 按 [2.2](#22-合法状态迁移与守卫) 的终态重放补完扇出，`external_id` 不变。

---

## 五、轻量流程展开（Ad-hoc Flow → DAG）

### 5.1 设计动机

用户或 Coordinator Agent 常需要临时创建一串有序步骤（例如「先生成代码，再跑单元测试，最后人工 Review」）。轻量流程展开（`tb_instantiate_flow`）让用户像写待办清单一样声明一次性流程（Ad-hoc Flow），系统把它编译成 TB 原生的 DAG 节点与 `blocks` 边：不用预先注册静态模板，又完全复用 TB 的解锁与状态机。

### 5.2 声明式 Flow Plan 结构

Flow Plan 的结构由 [`schemas/taskboard/flow_plan.schema.json`](../schemas/taskboard/flow_plan.schema.json) 定义，对应 L0 的 `FlowPlan` / `FlowStepSpec`。步骤省略 `after` 时默认串行（依赖上一步）；显式写 `after: [...]` 表达并行分叉与多前置汇聚（`after: []` 表示无前置）。Phase 1 的入口是 `IngestionService.tb_instantiate_flow(plan: FlowPlan)`，尚未提供 YAML / JSON 文本解析入口；下面用 YAML 展示字段：

```yaml
title: "Feature Code & Review Flow"
scope: "repo:org/project#102"
initial_status: "ready"          # 根节点的初始状态，默认 ready
steps:
  - id: "step_checkout"
    title: "拉取分支与准备环境"
    assignee: "agent:runner-1"
    task_type: "setup"

  - id: "step_codegen"
    title: "生成核心功能代码"
    assignee: "agent:coder"
    task_type: "coding"
    after: ["step_checkout"]

  - id: "step_unittest"
    title: "运行自动化单元测试"
    assignee: "agent:tester"
    task_type: "testing"
    after: ["step_codegen"]

  - id: "step_docgen"
    title: "同步更新 API 文档"
    assignee: "agent:writer"
    task_type: "documentation"
    after: ["step_codegen"]

  - id: "step_human_review"
    title: "人工代码审核与合并"
    assignee: "human:group:tech_leads"
    task_type: "code_review"
    after: ["step_unittest", "step_docgen"]
```

步骤字段的默认值：`assignee="agent:default"`、`task_type="general"`、`priority=2`。

### 5.3 单事务编译与展开后解绑

`core/dag.py` 的 `compile_flow_plan()` 先在内存里编译，`FlowExpander` 再经 `TaskStorePort.save_expanded_flow()` 在一个 SQLite 事务里写入：

1. **校验**：至少一个步骤，步骤 `id` 非空且不重复；`after` 引用不存在的步骤抛 `UpstreamTaskNotFoundError`。
2. **生成父任务**：新建一个 `task_type="flow_group"`、状态 `backlog` 的父任务，所有步骤的 `parent_task_id` 指向它（仅用于 UI 折叠，不参与状态聚合）。
3. **批量创建 `TaskItem`**：每个步骤生成 `tb_tsk_<ULID>`，映射 `title`、`description`、`assignee`、`task_type`、`priority`，`scope` 取自 Plan。
4. **建边并判环**：`after` 转为 `kind='blocks'` 的边；每加一条边都在内存图上做一次 DFS 判环。
5. **根节点直发**：没有前置的根节点按 `initial_status` 起步，其余为 `backlog`；事务提交后，`ready` 的根节点立即派发。
6. **展开后解绑**：编译完成后所有节点都是独立的原生任务。之后可以随时插入新步骤、删除步骤或修改 `assignee`，不受原 Flow 约束。

---

## 六、与 Switchboard / Hibernaut 的双向 JSON Schema 契约

### 6.1 协议规范

- **单一事实源**：双向消息的 `metadata` 结构定义在仓库根目录 [`schemas/taskboard/`](../schemas/taskboard)，遵循 JSON Schema Draft 2020-12。
- **契约校验**：`SwitchboardAdapter` 在入站（`work.opened`、`execution.reported`）与出站（`work.ready`、通知）时按 Schema 校验 metadata（内置标准库子集校验器）；CI 中 `tests/taskboard/test_m4_architecture_and_schemas.py` 对四份 Schema 做生产、消费双向契约测试。
- **轻信封重引用**：消息保持轻量，大块上下文（代码快照、日志等）不嵌入 payload，用 `artifact_ref` URI 引用。
- **全链路追溯**：metadata 必须带 `trace_id` 与 `hop_count`，每经过一次 TB 推导转发 `hop_count + 1`。入站 `hop_count >= 8` 拒收，出站 `hop_count > 8` 拒发，均抛 `MaxHopsExceededError` 并记审计，防止死循环。
- **防自环**：`source="taskboard"` 的入站事件直接忽略。

### 6.2 双向事件契约

只使用 Switchboard 已定稿的 `event_type`，不新增类型。`title` / `description` 取 Switchboard 信封的 `subject` / `body`；`source_ref` 依次取 `metadata.source_ref`、信封 `correlation_key`、`external_id`；乱序水位用信封的 `occurred_at`。

| 方向 | event_type | 触发场景 | 关键字段（`metadata`） |
| :--- | :--- | :--- | :--- |
| **消费** | `work.opened` | 外部系统创建任务（飞书立项、GitHub Issue、API 调用） | • `work_kind`（`issue` \| `pr` \| `task`，必填）<br>• `trace_id`、`hop_count`（必填）<br>• `initial_status`（`backlog` \| `ready`，默认 `backlog`）<br>• `assignee`、`task_type`、`preferred_executor`（可选）<br>• `priority`（整数 0–3，默认 2）<br>• `depends_on`（`task_id` 或 `source_ref` 数组，前置必须已存在）<br>• `artifact_ref`、`reply_target`（可选）<br>已存在同一 `source_ref` 时按更新处理（幂等） |
| **消费** | `work.updated` | 外部镜像信息变更（如 Issue 标题修改） | • 信封 `subject` / `body` 更新 `title` / `description`<br>• `priority`、`assignee`、`preferred_executor`、`scope`、`task_type`（受 `manual_override_fields` 保护）<br>• `artifact_ref`、`reply_target` |
| **消费** | `work.closed` | 外部显式关闭或取消 | • `state`（`closed` \| `cancelled`，默认 `cancelled`）<br>• `close_reason`（可选） |
| **消费** | `execution.reported` | Runner / Agent 汇报执行进度与结果 | • `outcome_kind`（`emit` \| `suspend` \| `error` \| `done`，必填）<br>• `trace_id`、`hop_count`（必填）<br>• `task_action`（`claim` \| `update_stage` \| `complete` \| `release`；缺省则 TB 不处理）<br>• `task_id`（可选，缺省按 `source_ref` 定位）<br>• `stage`、`assignee`、`artifact_ref`（可选） |
| **产出** | `work.ready` | 任务就绪且 `assignee` 为 `agent:*` | • 信封 `external_id`：`tb:{task_id}:d{dispatch_seq}`<br>• `work_kind="task"`、`state="ready"`<br>• `task_id`、`dispatch_seq`（≥ 1）、`assignee`（`agent:*`）、`task_type`、`priority`<br>• `trace_id`、`hop_count`（1–8）<br>• `preferred_executor`、`artifact_ref`、`reply_target`、`parent_task_id`（有值时） |
| **产出** | `execution.reported` | ① 人工待办（`human:*`）<br>② DAG 异常告警 | • 信封 `source="taskboard"`，`external_id`：`tb:notify:{notify_kind}:{task_id}:{key}`<br>• `outcome_kind="emit"`<br>• `notify_kind`：`human_todo` \| `upstream_cancelled` \| `dependency_invalidated`<br>• `task_id`、`target_assignee`、`reason`<br>• `run_id="tb_run_{task_id}"`、`session_id="tb_sess_{task_id}"`<br>• `trace_id`、`hop_count` |

通知 `external_id` 中的 `key`：`human_todo` 为 `d{dispatch_seq}`；`upstream_cancelled` 为 `u{上游 task_id}:v{上游 version}`（重放不变，上游重开后再取消则换新）；其余为 `v{version}:d{dispatch_seq}`。

---

## 七、SQLite 持久化、目录结构与 import-linter 门禁

### 7.1 SQLite 建表 DDL

`SQLiteTaskStore` 使用单连接加线程锁，启用 `PRAGMA journal_mode=WAL`、`PRAGMA busy_timeout=5000` 与外键约束，写操作一律 `BEGIN IMMEDIATE`。时间字段由应用层以 UTC ISO 8601 写入。以下 DDL 与 `adapters/sqlite_store.py` 的 `_SCHEMA_SQL` 一致：

```sql
-- 1. 主任务表
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,                          -- tb_tsk_<ULID>
    source_ref TEXT DEFAULT NULL,                      -- 冗余主来源（如 github://org/repo/issues/1），可空
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'backlog',            -- backlog, ready, in_progress, done, cancelled
    assignee TEXT NOT NULL DEFAULT 'agent:default',    -- agent:* 或 human:*
    task_type TEXT NOT NULL DEFAULT 'general',
    stage TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 2,               -- 0（最高）到 3（最低）
    preferred_executor TEXT NOT NULL DEFAULT '',
    scope TEXT NOT NULL DEFAULT '',
    parent_task_id TEXT DEFAULT NULL,                  -- 仅用于 UI 树形展示
    manual_override_fields TEXT NOT NULL DEFAULT '[]', -- JSON 数组
    blocked_reason TEXT NOT NULL DEFAULT '',           -- 如 upstream_cancelled
    dispatch_seq INTEGER NOT NULL DEFAULT 0,           -- 每次派发 +1
    version INTEGER NOT NULL DEFAULT 1,                -- CAS 乐观锁
    last_source_event_at INTEGER NOT NULL DEFAULT 0,   -- 来源事件水位（毫秒）
    ready_at TEXT DEFAULT NULL,
    claimed_ready_at TEXT DEFAULT NULL,                -- 领到 dispatch_seq 时的 ready_at
    artifact_ref TEXT DEFAULT NULL,
    reply_target TEXT DEFAULT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (parent_task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_source_ref
    ON tasks(source_ref)
    WHERE source_ref IS NOT NULL AND source_ref != '';

CREATE INDEX IF NOT EXISTS idx_tasks_status_priority
    ON tasks(status, priority ASC, ready_at ASC);

CREATE INDEX IF NOT EXISTS idx_tasks_assignee
    ON tasks(assignee)
    WHERE assignee != '';

CREATE INDEX IF NOT EXISTS idx_tasks_parent
    ON tasks(parent_task_id)
    WHERE parent_task_id IS NOT NULL;

-- 2. 多源绑定表
CREATE TABLE IF NOT EXISTS task_source_links (
    source_ref TEXT PRIMARY KEY,                       -- 外部全局唯一引用 URI
    task_id TEXT NOT NULL,
    source TEXT NOT NULL,                              -- github, feishu, jira ...
    is_primary INTEGER NOT NULL DEFAULT 0,
    last_synced_at INTEGER NOT NULL DEFAULT 0,         -- 毫秒
    created_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_source_links_task_id
    ON task_source_links(task_id);

-- 3. DAG 依赖表
CREATE TABLE IF NOT EXISTS task_dependencies (
    upstream_task_id TEXT NOT NULL,                    -- 被依赖方（上游）
    downstream_task_id TEXT NOT NULL,                  -- 依赖方（下游）
    kind TEXT NOT NULL DEFAULT 'blocks',               -- blocks 或 related
    created_at TEXT NOT NULL,
    PRIMARY KEY (upstream_task_id, downstream_task_id, kind),
    FOREIGN KEY (upstream_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (downstream_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_deps_downstream
    ON task_dependencies(downstream_task_id, kind);

CREATE INDEX IF NOT EXISTS idx_deps_upstream
    ON task_dependencies(upstream_task_id, kind);
```

### 7.2 目录结构与 import-linter 门禁

#### 7.2.1 代码目录

```text
src/taskboard/
├── __init__.py
├── core/                        # L0 领域内核，零 I/O
│   ├── __init__.py
│   ├── models.py                # TaskItem、TaskSourceLink、TaskDependency、TaskView、FlowPlan、ExpandedFlow
│   ├── errors.py                # 领域错误（见 8.2）
│   ├── state_machine.py         # 5 态迁移矩阵与守卫
│   ├── merge.py                 # 三层字段所有权合并与乱序水位
│   └── dag.py                   # DFS 判环与 Flow 编译
├── ports.py                     # L1：TaskStorePort、EventPublisherPort、AuditSinkPort（Protocol）
├── engine/                      # L2 应用编排
│   ├── __init__.py
│   ├── ingestion.py             # IngestionService：入站事件处理与 7 个 tb_* 工具方法
│   ├── dispatcher.py            # UnlockDispatcher：解锁扫描、派发与异常通知
│   └── flow_expander.py         # FlowExpander：Flow 展开
├── adapters/                    # L3 适配器
│   ├── __init__.py
│   ├── sqlite_store.py          # SQLiteTaskStore：DDL、WAL、CAS
│   ├── switchboard_adapter.py   # SwitchboardAdapter：事件收发与 Schema 校验
│   └── audit_adapter.py         # SafeAuditAdapter、NoopAuditSink、RecordingAuditSink
└── app/                         # 组装根，最上层
    ├── __init__.py
    ├── __main__.py              # python -m taskboard.app
    ├── config.py                # taskboard.toml 解析与校验
    └── main.py                  # 服务装配与启停

schemas/taskboard/               # 仓库根目录，JSON Schema 契约
├── work_opened.metadata.schema.json
├── work_ready.metadata.schema.json
├── execution_reported.metadata.schema.json
└── flow_plan.schema.json

tests/taskboard/
├── test_m1_core_kernel.py
├── test_m2_sqlite_store.py
├── test_m3_engine_and_switchboard.py
├── test_m4_architecture_and_schemas.py
└── test_m5_publish_replay.py
```

#### 7.2.2 `.importlinter` 门禁

仓库根目录 `.importlinter` 的 `root_packages` 包含 `taskboard`，其中 Taskboard 相关的三条契约如下（CI 与提交前的 `lint-imports` 强制执行）：

```ini
[importlinter:contract:taskboard-no-runtime-deps]
name = Taskboard must not depend on execution runtime or memory/broker internals
type = forbidden
source_modules =
    taskboard
forbidden_modules =
    agent_runtime
    runner
    hibernaut
    broker
    memory
    memory_hub

[importlinter:contract:taskboard-clean-layers]
name = Taskboard Clean Architecture L3 -> L2 -> L1 -> L0 unidirectional layers
type = layers
layers =
    taskboard.app
    taskboard.adapters
    taskboard.engine
    taskboard.ports
    taskboard.core

[importlinter:contract:taskboard-core-pure]
name = Taskboard L0 core must not depend on switchboard or sqlite3
type = forbidden
source_modules =
    taskboard.core
forbidden_modules =
    sqlite3
    switchboard
    taskboard.adapters
    taskboard.engine
    taskboard.ports
```

另外，Switchboard 一侧的 `switchboard-zero-business-deps`、`ccpeer-zero-business-deps` 契约禁止这两个包反向导入 `taskboard`。`tests/taskboard/test_m4_architecture_and_schemas.py` 另用 AST 检查分层边界（含下层不得导入 `taskboard.app`），并断言 `taskboard-clean-layers` 的层次。

---

## 八、Phase 1 编码契约清单

### 8.1 JSON Schema 文件

四份 Schema 均为 Draft 2020-12，位于 [`schemas/taskboard/`](../schemas/taskboard)，以文件为准：

| 文件 | 校验对象 | 必填字段 | 关键约束 |
| :--- | :--- | :--- | :--- |
| [`work_opened.metadata.schema.json`](../schemas/taskboard/work_opened.metadata.schema.json) | 入站 `work.opened` 的 metadata | `work_kind`、`trace_id`、`hop_count` | `work_kind` ∈ `issue`/`pr`/`task`；`hop_count` 0–8；`initial_status` ∈ `backlog`/`ready`；`assignee` 匹配 `^(agent\|human):[a-z0-9.:_-]+$`；`priority` 0–3 |
| [`work_ready.metadata.schema.json`](../schemas/taskboard/work_ready.metadata.schema.json) | 出站 `work.ready` 的 metadata | `task_id`、`dispatch_seq`、`assignee`、`task_type`、`priority`、`trace_id`、`hop_count` | `additionalProperties: false`；`state` 只能是 `ready`；`assignee` 匹配 `^agent:[a-z0-9.:_-]+$`；`dispatch_seq` ≥ 1；`hop_count` 1–8；`priority` 0–3 |
| [`execution_reported.metadata.schema.json`](../schemas/taskboard/execution_reported.metadata.schema.json) | 入站执行上报与出站通知的 metadata | `outcome_kind`、`trace_id`、`hop_count` | `outcome_kind` ∈ `emit`/`suspend`/`error`/`done`；`task_action` ∈ `claim`/`update_stage`/`complete`/`release`；`notify_kind` ∈ `human_todo`/`upstream_cancelled`/`dependency_invalidated`；`hop_count` 0–8 |
| [`flow_plan.schema.json`](../schemas/taskboard/flow_plan.schema.json) | `FlowPlan` | `title`、`scope`、`steps` | `steps` 至少 1 项，每项必填 `id`、`title`；`initial_status` 默认 `ready`；步骤 `assignee` 默认 `agent:default`；`priority` 0–3 |

### 8.2 L0 领域错误（`src/taskboard/core/errors.py`）

所有错误继承 `TaskboardError`。

| 异常 | 触发条件 | 调用方处理 |
| :--- | :--- | :--- |
| `TaskNotFoundError` | 按 `task_id` / `source_ref` 未命中；加边时下游不存在 | 返回 404 / MCP 错误 |
| `UpstreamTaskNotFoundError` | 加边、`depends_on` 或 Flow `after` 引用的前置不存在 | 拒绝写入，不建幽灵单 |
| `CycleDetectedError` | 新增 `blocks` 边成环或自环（携带 `cycle_path: tuple[str, ...]`） | 回滚事务并提示环路径 |
| `IllegalTransitionError` | 违反 5 态迁移矩阵、终态未带 `reopen`、终态上迟到的运行中上报 | 入站事件记审计后 `acked`（不重投）；手工调用返回 409 |
| `BlockedByDependencyError` | `backlog → ready` 或 `ready → in_progress` 时仍有未完成上游 | 拒绝开工，返回阻塞上游 ID 列表（`blocking_task_ids`） |
| `OptimisticLockError` | CAS `WHERE version = ?` 影响行数为 0 | 引擎内最多重试 3 次；并发解锁、领取派发时静默跳过 |
| `SchemaValidationError` | 入站或出站 metadata 不符合 Schema | 入站：记审计后 `acked`；出站：包成 `PublishFailedError` |
| `PublishFailedError` | 出站发布的任何异常（含出站 Schema 校验失败） | 记 `outbound_publish_failed`，入站事件返回 `nack` 等待重投补完 |
| `MaxHopsExceededError` | 入站 `hop_count >= 8`，或出站 `hop_count > 8` | 丢弃并记审计告警；入站事件 `acked` |

### 8.3 `tb_*` 工具方法（7 个）

定义在 `IngestionService`（`engine/ingestion.py`），供后续 MCP / API 入口调用；Phase 1 尚未暴露为 MCP 工具。

| 方法 | 入参 / 出参 | 关键副作用 |
| :--- | :--- | :--- |
| `tb_create_task` | 入参：`scope`, `title`, `description=""`, `initial_status="backlog"`, `assignee="agent:default"`, `task_type="general"`, `priority=2`, `source_ref?`, `parent_task_id?`, `preferred_executor=""`, `artifact_ref?`, `reply_target?`, `source="mcp"`<br>出参：`TaskItem` | `initial_status=="ready"` 时立即派发（`work.ready` 或人工待办） |
| `tb_update_task` | 入参：`task_id`, `patch`, `unlock_fields=()`<br>出参：`TaskItem` | 手改第 2 层字段时自动加入 `manual_override_fields`；`unlock_fields` 解除字段锁；改第 3 层字段报错 |
| `tb_transition_task` | 入参：`task_id`, `target_status`, `reason=""`, `reopen=False`<br>出参：`TaskItem` | 走 5 态守卫：`backlog → ready`、`ready → in_progress` 查前置；`done` 触发下游解锁；`cancelled` 给下游标 `upstream_cancelled` 并告警；重开触发下游回退或 `dependency_invalidated` |
| `tb_link_source` | 入参：`task_id`, `source_ref`, `source`, `set_primary=False`, `last_synced_at=0`<br>出参：`TaskSourceLink` | 写入 `task_source_links`（1:N 跨源合并） |
| `tb_manage_dependency` | 入参：`op`（`add` \| `remove`）, `upstream_task_id`, `downstream_task_id`, `kind="blocks"`<br>出参：`bool` | `add`：存在性校验 + DFS 判环，并执行加边联动；`remove`：前置归零时把下游提升为 `ready` 并派发 |
| `tb_instantiate_flow` | 入参：`plan: FlowPlan`<br>出参：`ExpandedFlow`（`parent_task_id`, `tasks`, `edges`） | 单事务写入父任务、N 个子任务与 `blocks` 边，随后派发 `ready` 的根节点 |
| `tb_list_tasks` | 入参：`scope?`, `status?`, `assignee_prefix?`, `dispatchable_only=False`, `parent_task_id?`<br>出参：`list[TaskView]` | `TaskView` 带派生字段 `incomplete_blocker_count`，看板据此显示「等待前置中」 |

### 8.4 分步编码顺序与验收测试

| 里程碑 | 范围 | 验收测试（`tests/taskboard/`） |
| :--- | :--- | :--- |
| **M1** | `src/taskboard/core/` 纯内核，不碰 SQLite：5 态迁移全覆盖、`ready→in_progress` 依赖拦截、`manual_override_fields` 锁字段与乱序水位、DFS 自环与多跳环检测、`FlowPlan` 默认串行与显式 `after` 并行汇聚 | `test_m1_core_kernel.py` |
| **M2** | `adapters/sqlite_store.py`：WAL 与单写锁、局部唯一索引 `uq_tasks_source_ref`、1:N `task_source_links`、依赖存在性与判环、两个上游并发 `done` 时 `NOT EXISTS + CAS` 最多解锁一次、删边即解锁与 CAS 冲突 | `test_m2_sqlite_store.py` |
| **M3** | `engine/` + `SwitchboardAdapter` + Schema 契约：`work.opened(initial_status)` 与幂等、`execution.reported(task_action=claim/update_stage/complete/release)` 与优先级排序、`work.ready` 幂等键 `tb:{task_id}:d{dispatch_seq}`、人工待办与 DAG 异常复用 `execution.reported(notify_kind)`、`hop_count` 防环、`tb_*` 工具、审计降级 | `test_m3_engine_and_switchboard.py` |
| **M4** | CI 架构门禁：`lint-imports` 跑通 `.importlinter` 中三条 Taskboard 契约；pytest 对四份 Schema 做生产 / 消费双向契约校验，并用 AST 检查分层边界 | `test_m4_architecture_and_schemas.py` |
| **M5** | 发布失败与重放：`complete`、`work.closed`、`work.opened` 重投补完扇出且 `external_id` 不变；出站失败回 `nack`；扫描与提升之间新增上游时不提升；多个上游取消各发一条通知 | `test_m5_publish_replay.py` |

五个文件共 45 个用例（含参数化）。`scripts/select_tests.py` 把 `schemas/taskboard/**` 的改动映射到 m3、m4、m5。

---

## 附录：与 Drive 定稿附件的差异

以下各处 Drive 定稿附件与 PR #58 的实现不一致，本文已按实现改写：

| 位置 | 附件写法 | 实现（本文采用） |
| :--- | :--- | :--- |
| 1.3 L1 端口 | 抽象基类（ABC） | `typing.Protocol` |
| 1.3 类名 | `TaskIngestionService`、`AuditSinkAdapter` | `IngestionService`、`SafeAuditAdapter`（另有 `NoopAuditSink`、`RecordingAuditSink`） |
| 2.4.2 第 1 层字段 | `title`、`description`、`external_snapshot` | `title`、`description`、`artifact_ref`、`reply_target` |
| 2.4.2 第 3 层字段 | `status`、`dispatch_seq`、`version` | 另含 `ready_at`、`claimed_ready_at`、`blocked_reason`、`last_source_event_at` |
| 3.1 判环 | 在存储层直接用游标 DFS | L0 `core/dag.py` 的 `detect_cycle()`，存储层传入查询出边的回调；返回 `cycle_path` |
| 3.1 上游不存在 | `TaskNotFoundError` | `UpstreamTaskNotFoundError`（下游不存在才是 `TaskNotFoundError`） |
| 3.3 CAS 提升 | `UPDATE … WHERE status='backlog' AND version=?` | 同一条 `UPDATE` 里再加 `NOT EXISTS` 未完成上游检查，并清空 `blocked_reason`；另有 `try_claim_dispatch()` 领派发序号 |
| 5.2 Flow 字段 | `flow_name`、`context_scope` | `title`、`scope`（另有 `initial_status`、`template_name`）；暂无 YAML / JSON 文本入口 |
| 5.3 父任务 | 虚拟组节点或首节点 | 新建 `task_type="flow_group"` 的父任务 |
| 6.1 跳数上限 | `hop_count > 8` 终止 | 入站 `>= 8` 拒收，出站 `> 8` 拒发 |
| 6.2 `priority` | 整数 1–5，默认 3 | 整数 0–3，默认 2（数值小者优先） |
| 6.2 `work.opened` 必填 | `source_ref`、`title` | metadata 必填 `work_kind`、`trace_id`、`hop_count`；`title` 取信封 `subject`，`source_ref` 可取 `correlation_key` |
| 7.1 DDL | `source_ref NOT NULL` 全局唯一、`assignee` 默认 `''`、`task_type` 默认 `default`、`priority` 默认 3、时间由 SQLite 默认值生成 | `source_ref` 可空、非空时局部唯一；`assignee` 默认 `agent:default`、`task_type` 默认 `general`、`priority` 默认 2；新增 `claimed_ready_at`、`artifact_ref`、`reply_target`；时间由应用层写入 |
| 7.2.1 目录 | `core/entities.py`、`dag_engine.py`；`engine/ingestion_service.py`、`unlock_dispatcher.py`；`adapters/audit_sink_adapter.py`；Schema 在 `src/taskboard/schemas/` | `core/models.py`、`errors.py`、`merge.py`、`dag.py`；`engine/ingestion.py`、`dispatcher.py`；`adapters/audit_adapter.py`；Schema 在仓库根目录 `schemas/taskboard/`，文件名带 `.metadata` |
| 7.2.2 契约 | `contract:1`–`contract:3`，`root_package = taskboard` | `taskboard-no-runtime-deps`（另禁 `memory`）、`taskboard-clean-layers`（最上层另有 `taskboard.app`）、`taskboard-core-pure`（另禁 `taskboard.ports`、`taskboard.engine`） |
| 8.1 Schema | 正文内联 JSON | 链接到仓库文件并列关键约束；实现另含 `state`、`preferred_executor`、`target_assignee`、`reason` 等字段，`assignee` 正则允许 `:` |
| 8.2 错误 | 8 个 | 另有基类 `TaskboardError` 与 `PublishFailedError` |
| 8.3 `tb_list_tasks` 出参 | `list[TaskItem]` | `list[TaskView]` |
| 8.4 里程碑 | M1–M4 | 另有 M5（`test_m5_publish_replay.py`） |
