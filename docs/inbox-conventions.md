# Inbox 使用规范

Inbox 是串行队列：同一个 `inbox_id` 一次只跑一件，不同 `inbox_id` 并发。命名、分桶、依赖怎么选是软规范，写在这里和 `builtin:steward` Skill；[硬约束](#硬约束由代码执行)由代码执行，Skill 与事件都绕不开。术语见 [`glossary.md`](glossary.md)，实现见 [`architecture.md`](architecture.md) §3–§5。

## 命名

| 写法 | 用途 | 例 |
|---|---|---|
| `proj:<p>` | 项目主入口，外部来的事件默认到这里，由 `steward` 分诊 | `proj:hibernaut` |
| `proj:<p>:<role>` | 固定角色队列（三段），同一角色的活串行做 | `proj:hibernaut:sde`、`proj:hibernaut:review` |
| `proj:<p>:<role>:<slug>` | 动态队列（四段），为要并发的一件独立的活另开 | `proj:hibernaut:sde:issue-42` |
| `sys:*`、`user:*` | 不属于任何项目的顶层 Inbox：系统运维、个人事务 | `sys:ops`、`user:main` |

- 项目 Inbox 须被该项目的 `inbox_id` 规则匹配；项目写了绑定的，还须被某条绑定覆盖，否则转投被拒。
- `<slug>` 取任务的稳定短名（Issue 号、主题），同一件活的后续事件沿用同一个，不用随机值。
- Memory 分区名由 `inbox_id` 得出，所以它不能含 `/`、`\`、`..`。同一项目的 Inbox 共用项目记忆；`sys:*`、`user:*` 与其他绑定了 `workdir` 的 Inbox 各有各的记忆（[`architecture.md`](architecture.md) §14）。

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
pattern = "sys:ops"
default_agent = "ops"
agents = ["ops"]
workdir = "/abs/path/to/hibernaut"
checkout_access = "readonly"    # readonly | readwrite，默认 readwrite
system_tools = true             # 可选，默认 false；见「系统 Steward」
```

- `workdir` 必填；`~` 与相对路径（以配置文件所在目录为准）都可以。目录不会自动创建，不存在时事件保持 `pending`（含 `review_of`、审批回复、关闭请求、挂起会话的后续和指向该目录的 `explicit_workdir`，换一种写法也算）；只有 `explicit_workdir` 指向别的目录的事件照样在本轮拒绝。
- 可写的 `workdir` 只给一个 Inbox 用：`pattern` 须是单个精确 ID，两条绑定不能指向同一个可写目录（已存在的按 inode 比较；尚不存在的部分不分大小写比较，区分大小写的卷上也一样），也不能落在某个项目的检出或某个 Steward 的 `workdir` 里。`checkout_access = "readonly"` 可以共享目录、可以用正则。
- `readonly` 对 CLI agent 是声明：运行时据此放行共享，并在提示里写明不改文件，但不拦 CLI 的写操作。真正只读由该 CLI 自己的参数保证（如 `--permission-mode plan`）。
- 这些 Inbox 总是在 Hibernaut 进程本机起 CLI，`[runner]` 的 `executors` 只作用于项目 Inbox，Runner 的逐次工具授权在这里不生效。
- 可用不带 `role` 的 CLI agent；`checkout_access = "readonly"` 的绑定还可用 `steward`（见下节）。`sde`、`code-reviewer` 依赖项目检出，加载时拒绝。`agents` 省略时指全部 CLI agent，加上本绑定 `default_agent`、`rules` 点名的 steward。
- 一条绑定自成一个转投范围：可以在本 Inbox 内换 agent 接力，不能与别的绑定互投；写了 `forward_to` 才能转投到列出的项目主 Inbox 或系统 Inbox。

## 全局 Steward 与转投白名单

不属于项目的 `steward` 接个人对话（如飞书单聊）：能答的自己答，要动手的派给对的项目。

```toml
[agents.ash]
role = "steward"
executor = "claude-code"
skills = ["builtin:steward"]

[[runtime.inbox_bindings]]
pattern = "user:main"
default_agent = "ash"
agents = ["ash"]
workdir = "~/workspace"
checkout_access = "readonly"
forward_to = ["proj:hibernaut", "proj:sure"]
```

- **目录**：以绑定的 `workdir` 为准，须是 Orca 已登记的 Git 检出；agent 自己也写了 `workdir` 的须是同一个目录，`branch` 照旧每次核对。没有 Orca 终端时挂起等人批准重试，与项目 Steward 相同。
- **`forward_to`**：每项是某个项目的主 Inbox（`inbox_id` 的第一个精确 ID），或一个系统 Inbox 的精确 ID（见「系统 Steward」）；写错哪一项，加载和热加载的报错就点名哪一项。省略即不能出本绑定。
- **只有一个方向**：绑定 → 列出的主 Inbox。不能指定 `target_agent`，由项目的规则选；`return_to` 只能是来源 Inbox 自己。项目不能主动转投到 `user:*` / `sys:*`，只有结果能回投。其余约束（跳数、白名单 agent、逐次过 `inbox.forward`）不变。
- **按当时的配置判**：转投申请、审批后补发、会话恢复、Run 收尾入队前都重判。热加载删掉一项后，还在跑的旧 Run 也发不出去：暂存的转投丢弃，并在原对话里说明（`kind = "forward_refused"`）；已派出的活照做，但结果被扣下等人决定，不会自动回投。已经入队的转投不收回。
- **提示词**：同一个 `builtin:steward`，在非项目 Inbox 里自动换成不提项目检出的那一份（`skills/steward-global/`），配置里没有单独的名字。路由清单只列白名单项目的名字、主 Inbox、默认 agent 与 `[projects.<name>] description`。
- **查询**：`list_sessions` / `list_events` 可看自己的 Inbox 与白名单项目的全部 Inbox；读文件和 `git` 只限 `workdir`。
- **答复**：接手的 agent 直接答复原对话；带 `return_to` 的由全局 Steward 汇总后答复同一处。

### 一个对话里的几件活

同一个飞书单聊的消息共用一个会话键，所以每次跨范围转投另立**任务键**，在项目里各有会话、`work_key` 与 worktree，项目 Steward 再派给 `sde` 也不合并；带不带 `return_to` 都一样。

补充怎么回到原来那件：

1. 转投的回执给出任务键；`list_sessions` / `list_events` 的 `correlation_key`、回投事件的 `from_task` 也是它。
2. 向同一个主 Inbox 转投，`continues` 填该键，不带 `return_to`。还没开始的与原活同批处理；已开始的进原会话。
3. 那个会话还欠着结果时，一份结果同时答原活和补充；不欠时直接答复原对话。项目 Steward 把补充转给正在做这件活的 agent 时同样并进它的会话，不另立委派。
4. 只认本对话、本 Inbox 派出且还开着的活；已结束的被拒，连同原请求与结果按新活另投。
5. 全局 Steward 的会话被关闭时，它派出的活一并关闭。

## 系统 Steward

接入项目、检查和补齐项目配置。需求与编号见 [`requirements/system-steward.md`](requirements/system-steward.md)。人照旧跟全局 Steward 说，由它转交；结果按普通转投的回信方式回到原单聊。

绑定写 `system_tools = true` 就是系统 Inbox（写法见上「非项目 Inbox 的 `workdir`」），须是单个精确的 `sys:<name>`、`checkout_access = "readonly"`、agent 全是 `steward`、不写 `forward_to`。要转到它的绑定在自己的 `forward_to` 里列出它，须是精确 ID、只读、agent 全是 `steward`。

| 工具（也是 `PermissionPort` 里的名字，默认放行） | 参数 | 做什么 |
|---|---|---|
| `check_projects` | 无 | 按项目列出缺的：Steward、说明、派活名单 |
| `onboard_project` | `repo`，可选 `name`、`description` | 克隆、在 Orca 登记、建标签、加项目段与 Steward 绑定、加进全局 Steward 的 `forward_to`；已接入的只列出缺什么 |
| `complete_project` | `name`，可选 `description` | 补上缺的，不动已有的值 |

- **默认值由代码定**，命令行 `scripts/onboard_project.py` 用同一套：主 Inbox 归 `steward`，`proj:<p>:.+` 归 `sde`，`open_pr = true`、`auto_merge = false`。具体哪个 agent 取别的项目用得最多的；没有合适的就拒绝（命令行则照加项目，并注明没配 Steward）。没给说明时写中性的「GitHub 仓库 owner/repo」。主 Inbox 已有手写绑定、默认 agent 不是 Steward 的，列为待人做，不改。
- **写配置**：只认上表的参数；整份能解析才写，先留备份（配置旁的 `<名字>.bak-<时间>`，0600）、原子替换；写前文件被人改过就放弃；写后等热加载生效，没生效就恢复原文件并说明。回执带 `added`、`backup` 和 `undo`。没有开热加载的进程里这些工具拒绝执行。
- **建标签**：只在 `[onboarding] label_owners` 列出的 owner 上建，默认空。服务器入口路由（Switchboard）永远只列为待人做。
- **谁能到它**：全局 Steward 替人转来的、本机运营者投的（本机 `POST /events`、CLI）、它自己的审批。GitHub、群聊、Drive、定时器来的，项目转来的，以及远端或经代理的 `POST /events`（403）、`POST /mcp` 都到不了；后两者投给全局 Steward 的也不算人说的。全局 Steward 也只有在本轮处理的是人的单聊消息或本机运营者的事件时才能转给它；只因为一条回投醒来的那轮不行。
- **看不到的**：`hibernaut.toml` 与它的备份不给 `read_file`、`search_files`、`git` 读，答复里没有 `env` 和令牌的值。

## 硬约束（由代码执行）

| 约束 | 不满足时 |
|---|---|
| 每次转投、回投过 `PermissionPort` 的 `inbox.forward` | `block` 拒绝，`defer` 等人批准 |
| 转投目标在同一项目（或同一绑定）内，或是绑定 `forward_to` 列出的项目主 Inbox、系统 Inbox；跨范围时不指定 agent，`return_to` 只能是来源 Inbox | 拒绝；审批放行、Run 收尾时按当时的配置重判 |
| 回投到别的范围时，等结果的 Inbox 现在的 `forward_to` 仍列着本项目 | 结果扣下，等人决定 |
| Agent 在 Inbox 绑定的 `agents` 白名单里 | 明确拒绝，不换别的 agent |
| 非项目 Inbox 有显式 `workdir`，且不与项目 Inbox 重叠、不共享可写目录 | 配置拒绝加载；热加载保留旧配置 |
| `inbox_id` 匹配某个项目或某条 `[[runtime.inbox_bindings]]` | 事件保持 `pending`，记 `WARNING` |
| `inbox_id` 能作 Memory 分区名（不含 `/`、`\`、`..`，且目录建得出来） | 事件保持 `pending`，每次唤醒记一条 `WARNING`，不占 Worker |
| 事件里的 `explicit_workdir` 不能把有绑定的非项目 Inbox 换到别的目录 | 本轮拒绝 |
| 一件外部事件最多主动转投 3 次（`MAX_FORWARD_HOPS = 3`）；回投是对转投的答复，不计数 | 转投被拒，当前 agent 就地答复 |
| 不能转投给同一 `(inbox_id, agent_id)` | 拒绝 |
| 系统 Inbox 不往外转投；转给它的那一轮须有人的单聊消息或本机运营者的事件 | 拒绝；审批放行、Run 收尾时重判 |
| 系统 Inbox 只收全局 Steward 转来的、本机运营者投的和自己的审批 | 事件标记 `failed`；远端或经代理的 `POST /events` 返回 403 |
| 系统 Inbox 是单个精确 `sys:<name>`、只读、agent 全是 `steward`、没有 `forward_to` | 配置拒绝加载；热加载保留旧配置 |
