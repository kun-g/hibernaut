# ADR-0001：跨项目协议采用 JSON Schema（暂不引入 Protobuf）

- **状态**：Accepted
- **日期**：2026-11-22
- **关联范围**：Hibernaut 多 `[projects.*]` 联动，以及 Hibernaut 与外部独立系统（如 Workshop、个人知识库、CLI/IDE MCP 客户端）之间的跨项目协议边界

## 1. 背景（Context）

随着 Hibernaut 进入多项目常驻运行与跨系统协同阶段，需要为以下跨项目边界建立**程序化、可机器校验的契约**，避免联调退化为口头或文档约定：

1. **跨项目事件信封**（`inbox_id`、`source`、`external_id`、`correlation_key`、`payload._meta` 因果追踪 / `reply_to` / `hop_count` 防环）；
2. **结构化交付物引用**（`artifact_ref`：PR、commit SHA、文档/知识库条目摘要，轻信封、重引用）；
3. **控制面观测与结案视图**（MCP `list_events` / `list_sessions` / `close_session` / `reclaim_workspace`）。

在选型时，主要备选方案为 **JSON Schema（Draft 2020-12）** 与 **Protocol Buffers（`.proto` / Protobuf）**。

## 2. 决策（Decision）

**v1 阶段统一采用 JSON Schema（Draft 2020-12）作为跨项目协议的程序化单一事实源**，不引入 Protobuf 工具链与运行时依赖：

- 在版本化目录（如 `schemas/v1/` 或独立协议仓库）维护 `.schema.json` 文件，并通过 `schema_version` 与 `additionalProperties` 策略管理向前兼容；
- 在各项目的 CI（`pytest`）中引入双向契约测试（Producer 校验真实产出，Consumer 校验测试夹具与解析逻辑），将契约漂移拦截在提交前；
- `src/agent_runtime/core/`（L0/L1）继续严格保持零第三方运行时依赖。

## 3. 决策理由（Rationale）

| 评估维度 | JSON Schema（选定） | Protobuf（暂缓） |
| :--- | :--- | :--- |
| **传输层原生契合度** | 现有全部外部边界（HTTP `/events`、Webhook、MCP `inputSchema`、SQLite `payload_json`、CLI JSON 输出）均为 JSON，一份定义可直接复用 | 全链路为 JSON/MCP，需额外走 `protojson` 桥接，且 MCP `inputSchema` 仍需转换为 JSON Schema |
| **依赖与构建复杂度** | 运行时零依赖，无需 `protoc`/`buf` 代码生成流水线，不破坏 Hibernaut L0 纯标准库铁律 | 需在多仓库引入 `protoc`/`buf` 生成流程与 `protobuf` 运行时依赖，维护成本高 |
| **LLM / Agent 生态友好度** | Claude Code / Grok / Cursor 与 MCP 客户端原生理解 JSON Schema，可直接用于结构化输出约束 | 模型与 MCP 客户端均需二次映射 |
| **演进与排障体验** | 明文 JSON 可直接观测、grep、落盘 SQLite，定位问题零解码成本 | 二进制线协议需专用工具解码，对低频控制面事件收益极低 |

## 4. 后果与权衡（Consequences）

### 正面收益
- 零额外运行时依赖，完整保留 Hibernaut 与 Workshop 的轻量零配置部署体验；
- MCP 工具定义与跨项目事件信封共享同一套校验语义，消除“文档与代码两张皮”。

### 已知代价与缓解措施
- **缺少编译器级跨语言强类型生成**：通过 CI 双向契约测试 + 可选开发期代码生成（如 `datamodel-code-generator`）缓解；
- **缺少内置 `buf breaking` 式字段编号约束**：在协议演化规范中约定“v1 只允许新增可选字段，禁止删除或改类型；破坏性变更升级 `v2/`”。

## 5. 触发重评条件（When to Revisit for Protobuf）

当以下任一条件成立时，重新评估引入 Protobuf（配合 `buf breaking` 做顶层 IDL，并自动导出 JSON Schema 给 MCP）：

1. 出现 **非 Python 强类型语言客户端/服务**（如 Go、Rust 或 Swift macOS 原生客户端）需要与运行时进行高频 RPC 互通；
2. 出现 **双向流式通信（gRPC Streaming）** 或高吞吐低延迟二进制传输需求，JSON 序列化成为实测瓶颈；
3. 跨团队/多语言仓库数量超过 3 个，手工维护 JSON Schema 的破坏性变更检查成本高于引入 `buf` 工具链的成本。
