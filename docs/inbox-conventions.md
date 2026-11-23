# Inbox 使用规范

Inbox 是串行队列：同一个 `inbox_id` 一次只跑一件，不同 `inbox_id` 并发。命名、分桶、依赖怎么选是软规范，写在这里和 `builtin:steward` Skill；[硬约束](#硬约束由代码执行)由代码执行，Skill 与事件都绕不开。术语见 [`glossary.md`](glossary.md)，实现见 [`architecture.md`](architecture.md) §3–§5。

## 命名

| 写法 | 用途 | 例 |
|---|---|---|
| `proj:<p>` | 项目主入口，外部来的事件默认到这里，由 `steward` 分诊 | `proj:hibernaut` |
| `proj:<p>:<role>` | 固定角色队列（三段），同一角色的活串行做 | `proj:hibernaut:sde`、`proj:hibernaut:review` |
| `proj:<p>:<role>:<slug>` | 动态队列（四段），为要并发的一件独立的活另开 | `proj:hibernaut:sde:issue-42` |
| `sys:*`、`user:*` | 不属于任何项目的顶层 Inbox：系统运维、个人事务 | `sys:hb-ops`、`user:main` |

- 项目 Inbox 须被该项目的 `inbox_id` 规则匹配；项目写了绑定的，还须被某条绑定覆盖，否则转投被拒。
- `<slug>` 取任务的稳定短名（Issue 号、主题），同一件活的后续事件沿用同一个，不用随机值。
- `inbox_id` 同时是 Memory 分区名，不能含 `/`、`\`、`..`。

## 串行复用还是并发分桶

- **复用 `proj:<p>:<role>`（默认）**：几件活改同一批文件、后一件要用前一件的结果，或不急。一条队列，先到先做，不产生合并冲突。
- **另开 `proj:<p>:<role>:<slug>`**：几件活互不依赖、改动基本不重叠，而且并发确实能省时间。每个 Inbox 有自己的会话与 `hb-*` worktree。
- **同一件活的后续**（返工、追问、评审意见）回到原来的 Inbox 和会话，不另开桶。
- 分桶不是加快的办法：总并发仍受 `[runtime]`、`[projects.*]`、`[agents.*]`、`[executors.*]` 的 `max_concurrency` 限制。

## 依赖

- **两步以内的顺序依赖**（先做 A 再做 B）：投同一个 Inbox，靠队列保证顺序；要看 A 的结果再决定 B，就带 `return_to` 转投 A，结果回来后再派 B。
- **更复杂的依赖**（多节点、扇入扇出、跨天、要看状态或重排）：交给 Taskboard 建任务和 DAG 依赖，由它在上游完成后派发；不要用多级转投或在 Memory 里手工维护依赖图。

## 非项目 Inbox 的 `workdir`

`sys:*` / `user:*` 没有项目检出，启动目录必须显式写在部署文件里：

```toml
[[runtime.inbox_bindings]]
pattern = "sys:hb-ops"
default_agent = "ops"
agents = ["ops"]
workdir = "/abs/path/to/ops-workdir"
checkout_access = "readwrite"   # readonly | readwrite，默认 readwrite
```

- `workdir` 必填；`~` 与相对路径（以配置文件所在目录为准）都可以。目录不会自动创建，不存在时事件保持 `pending`（含 `review_of`、审批回复、关闭请求、挂起会话的后续和指向该目录的 `explicit_workdir`，换一种写法也算）；只有 `explicit_workdir` 指向别的目录的事件照样在本轮拒绝。
- 可写的 `workdir` 只给一个 Inbox 用：`pattern` 须是单个精确 ID，两条绑定不能指向同一个可写目录（已存在的按 inode 比较；尚不存在的部分不分大小写比较，区分大小写的卷上也一样），也不能落在某个项目的检出里。`checkout_access = "readonly"` 可以共享目录、可以用正则。
- `readonly` 对 CLI agent 是声明：运行时据此放行共享，并在提示里写明不改文件，但不拦 CLI 的写操作。真正只读由该 CLI 自己的参数保证（如 `--permission-mode plan`）。
- 这些 Inbox 总是在 Hibernaut 进程本机起 CLI，`[runner]` 的 `executors` 只作用于项目 Inbox，Runner 的逐次工具授权在这里不生效。
- 只能用不带 `role` 的 CLI agent。`sde`、`code-reviewer`、`steward` 依赖项目检出，加载时拒绝。
- 一条绑定自成一个转投范围：可以在本 Inbox 内换 agent 接力，不能与项目或别的绑定互投。

## 硬约束（由代码执行）

| 约束 | 不满足时 |
|---|---|
| 每次转投、回投过 `PermissionPort` 的 `inbox.forward` | `block` 拒绝，`defer` 等人批准 |
| Agent 在 Inbox 绑定的 `agents` 白名单里 | 明确拒绝，不换别的 agent |
| 非项目 Inbox 有显式 `workdir`，且不与项目 Inbox 重叠、不共享可写目录 | 配置拒绝加载；热加载保留旧配置 |
| `inbox_id` 匹配某个项目或某条 `[[runtime.inbox_bindings]]` | 事件保持 `pending`，记 `WARNING` |
| `inbox_id` 能作 Memory 分区名（不含 `/`、`\`、`..`，且目录建得出来） | 事件保持 `pending`，每次唤醒记一条 `WARNING`，不占 Worker |
| 事件里的 `explicit_workdir` 不能把有绑定的非项目 Inbox 换到别的目录 | 本轮拒绝 |
| 一件外部事件最多主动转投 3 次（`MAX_FORWARD_HOPS = 3`）；回投是对转投的答复，不计数 | 转投被拒，当前 agent 就地答复 |
| 不能转投给同一 `(inbox_id, agent_id)` | 拒绝 |
