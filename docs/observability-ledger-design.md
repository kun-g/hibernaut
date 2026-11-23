# Observability / Ledger 架构设计基线（v2.0 · Q0–Q5 全部 13 项决策已锁定）

> 同步自 Google Drive《Always-on Agent 可观测与复盘层设计方案（Observability & Post-Mortem）》，作为 `kun-g/hibernaut` 仓库内版本化设计基线。

## 1. 定位与硬边界（Q0）
- **定位**：全生态的旁路只追加事实账本与复盘索引面（Fail-Open、Append-Only、轻索引+重外置）。
- **边界 0.1**：`hibernaut_replay_event` 归属 Hibernaut 控制面；`src/ledger/` 严格保持纯旁路只追加与只读查询，零反向写耦合。
- **边界 0.2**：同进程嵌入式运行，但物理隔离为独立 SQLite 文件 `ledger.db`（WAL），与主库 `worker_leases` / `event_inbox` 零写锁争用。

## 2. 双层数据契约（Q1 · ADR-0001 JSON Schema Draft 2020-12）
- `schemas/v1/audit_record.schema.json`：全生态共享的点状审计事件（`audit_events` 表）。
- `schemas/v1/run_record.schema.json`：HB + Runner 单次执行的区间档案（`runs` + `run_events` M:N 桥接表）。
- **1.1 Switchboard 兼容**：保留 `switchboard/ports.py` 现有关键字签名，增补可选 `correlation_key` / `scope` / `trace_id` / `severity`，经 `SwitchboardLedgerAuditAdapter` 脱敏转写。
- **1.2 `trace_id` 传播**：Schema 预留可选 `trace_id`，初期靠 `correlation_key` + `event_id` 串联，后续沿 `CanonicalEvent.metadata._trace_id` 透传。
- **1.3 `Emit` 终态**：保留 `outcome_kind="emit"` 并单列 `emit_status`（`done | failed | stopped | refused`）；成功定义 = `silent` 或 (`emit` 且 `emit_status == "done"`)。

## 3. `ErrorKind` 三级纯函数分类器与分流（Q2）
- **2.1 分类优先级**：① HB 内部异常 → ② `yetone/magpie` / CLI JSON 结构化错误码 → ③ 按 `executor_id` 专属正则扫 `stderr` 尾 4KB → 兜底信号/退出码/`unknown`。
- **2.2 三路分流**：瞬时故障（`rate_limit`/`network`/`crash`）按 `5s → 15s → 45s` 指数退避；确定性故障（`config_error`/`permission_denied`）第 1 次快停告警；执行故障（`tool_error`/`bad_output`/`timeout`/`unknown`）默认 0 次重试，保留 worktree 与 `.log` 待复盘。

## 4. 分布式外置日志与成本账本（Q3）
- **3.1 日志路由**：统一 `log_ref` URI（`local://YYYY-MM-DD/<run_id>.log` 直读磁盘带路径穿越防护；`runner://<id>/...` 按需反向拉取 tail，离线降级返回 `error_summary`）；本地 `.log` 按 14 天或 500 MB 双阈值滚动清理。
- **3.2 双源计费**：走 `yetone/magpie` 时透传 `X-Run-Id` / `X-Correlation-Key` / `X-Trace-Id` 为权威源（`usage_source="magpie"`），直连 CLI 从 stream-json 兜底（`usage_source="cli"`）。

## 5. 复盘 → 幂等重放 → Memory 防污染闭环（Q4）
- **4.1 重放防并发冲突**：HB 侧 `InboxPort.replay()` 仅允许重置 `failed` 事件，写入 `_replay_of_run_id` 后正常排队，绝不抢占在途租约，旧 `RunRecord` 完整保留。
- **4.2 Memory 提纯闸门**：瞬时故障禁入 Memory；复盘结论先入 `notes/postmortem/<id>.md` 候选池（必带 `source_run_id` 与失效条件），仅当复现 ≥ 2 次或人工审批后才幂等晋升进 `instructions.md`。

## 6. 工程交付形态（Q5）
- 独立顶级包 `src/ledger/` + `schemas/v1/` + `tests/test_ledger_all.py`，通过 `.importlinter` 锁死零业务反向依赖；由云端一次性实现测试打包，本机 Agent 仅需完成 4 处薄接线即可合入 `main`。
