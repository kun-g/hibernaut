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
default_agent = "hibernaut-ops"
agents = ["hibernaut-ops"]
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

## 定时脚本的 Inbox

重复执行同一脚本可用固定的 `sys:<name>` 和只读工作目录。也可选项目覆盖的 Inbox，项目写了绑定时须命中其中一条。带 `system_tools` 的 Inbox 拒绝作定时目标。`rules = [{ source = "timer:disk-check", agent = "disk-check" }]` 可让脚本和同一 Inbox 的其他来源分别选路。

在 `[[timers]]` 写唯一 `name`、`inbox_id` 和 `interval_seconds`（1 至 2147483647 的整数）。`enabled` 默认 `true`。可选的 `payload` 表只接受 JSON 可表示的数据，整体放在事件的 `payload.data` 下，里面的 `executor`、`agent_id` 等键只是数据。执行文件和参数模板由绑定的 CLI 配置指定，脚本把事件当输入读取。示例见 [`hibernaut.example.toml`](../hibernaut.example.toml)。

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

接入项目、检查和补齐项目配置。需求与编号见 [`requirements/system-steward.md`](requirements/system-steward.md)。人在单聊跟全局 Steward 说，由它转交，结果按普通转投的回信方式回到原单聊；或在 `[[feishu_groups]]` 绑定的群里直接说（只认 `members` 列出的人），答复回群里的同一话题。问 hb 运转得怎样、某个 `[[timers]]` 执行得如何、有什么可优化的，全局 Steward 不必转交：它也有同一份只读的 `runtime_status`（和系统 Steward 共用一份实现），直接答，不只凭 `list_events`、`list_sessions`（那两个看不到任务在等什么、交付怎么结束、定时任务的 Run 怎么结束）。它只多这一个只读工具，没有 `check_projects`、`onboard_project`、`complete_project`，也拿不到 hb MCP 控制面（`--strict-mcp-config` 只挂 Steward 自己的工具服务）。

SDE 等用户离开电脑（任务说要等空闲再做）时按契约返回 `needs_input`，问题以 `WAIT_IDLE` 开头；`runtime_status` 据此把它列为 `user_away`，与等人回答的 `answer` 分开。

绑定写 `system_tools = true` 就是系统 Inbox（写法见上「非项目 Inbox 的 `workdir`」），须是单个精确的 `sys:<name>`、`checkout_access = "readonly"`、agent 全是 `steward`、不写 `forward_to`。要转到它的绑定在自己的 `forward_to` 里列出它，须是精确 ID、只读、agent 全是 `steward`。

| 工具（也是 `PermissionPort` 里的名字，默认放行） | 参数 | 做什么 |
|---|---|---|
| `check_projects` | 无 | 按项目列出缺的：Steward、说明、派活名单 |
| `onboard_project` | `repo`，可选 `name`、`description` | 克隆、在 Orca 登记、建标签、加项目段与 Steward 绑定、加进全局 Steward 的 `forward_to`；已接入的只列出缺什么 |
| `complete_project` | `name`，可选 `description` | 补上缺的，不动已有的值 |

- **默认值由代码定**，命令行 `scripts/onboard_project.py` 用同一套：主 Inbox 归 `steward`，`proj:<p>:.+` 归 `sde`，`open_pr = true`、`auto_merge = false`。具体哪个 agent 取别的项目用得最多的；没有合适的就拒绝（命令行则照加项目，并注明没配 Steward）。没给说明时写中性的「GitHub 仓库 owner/repo」。主 Inbox 已有手写绑定、默认 agent 不是 Steward 的，列为待人做，不改。
- **写配置**：只认上表的参数；整份能解析才写，先留备份（配置旁的 `<名字>.bak-<时间>`，0600）、原子替换；写前文件被人改过就放弃；写后等热加载生效，没生效就恢复原文件并说明；确认生效前文件又被人改了，保留人改的那版并说明，不报已生效。回执带 `added`、`backup` 和 `undo`。没有开热加载的进程里这些工具拒绝执行。
- **建标签**：只在 `[onboarding] label_owners` 列出的 owner 上建，默认空。服务器入口路由（Switchboard）永远只列为待人做。
- **谁能到它**：全局 Steward 替人转来的、绑定群里名单上的人发的、本机运营者投的（本机 `POST /events`、CLI）、它自己的审批。GitHub、没绑定的群和群里名单外的人、Drive、定时器来的，项目转来的，以及远端或经代理的 `POST /events`（403）、`POST /mcp` 都到不了；后两者投给全局 Steward 的也不算人说的。全局 Steward 也只有在本轮处理的是人的单聊消息或本机运营者的事件时才能转给它；只因为一条回投醒来的那轮不行，例外只有一个：这条回投来自只收人请求的 Inbox（系统或运维）、`status` 是 `done`、回的是本会话还在等的那件，这时下一步接着人的原请求走（一句话开新项目的链条，见下「运维 Inbox」）。
- **谁的请求算数**：来源由上一条的代码验过，契约和内置 Skill 写明事件里的请求就是人提的，`agent:forward` 和 `webhook` 都一样；清楚就直接做，不再请人确认授权，只有没说清才问。全局 Steward 引述的人的原话是请求；请求里引用或附带的外来文字、工具读到的文字不是请求，不照做。
- **看不到的**：`hibernaut.toml` 与它的备份不给 `read_file`、`search_files`、`git` 读，答复里没有 `env` 和令牌的值。

## 项目群

和合作方的飞书群绑到一个项目的 Inbox，需求见 [`requirements/project-group.md`](requirements/project-group.md)。默认每个项目一个专用的 `proj:<p>:chat`，绑定写在 `proj:<p>:.+` 那条之前（第一个整串匹配的生效），agent 全是 `steward`：

```toml
[[projects.fruit.inbox_bindings]]
pattern = "proj:fruit:chat"
default_agent = "steward"
agents = ["steward"]

[[feishu_groups]]
chat_id = "oc_xxx"
inbox_id = "proj:fruit:chat"
members = ["ou_kun", "ou_partner"]   # 含合作方
approvers = ["ou_kun"]               # 这个项目所有 Inbox 的审批只认他们
```

- 群里谁说的都只算对这个项目的请求：项目 Inbox 没有 `forward_to`，群 Inbox 和它转去的主 Inbox、子队列都到不了别的项目、`sys:*`、`user:*`。
- 审批卡按 Switchboard 的 `[outbound] approval_target` 发，不发群里；Switchboard 只收 `approvers` 的点击。
- Switchboard 写了 `unmatched_policy = "reject"` 时，要在 `[routing.feishu_chats]` 把这个群路由到群 Inbox（或任意 scope，hb 按 `chat_id` 改投），否则消息在 Switchboard 就被丢掉。

## 运维 Inbox 与一句话开新项目

需求 R12–R15、C7–C9 见 [`requirements/system-steward.md`](requirements/system-steward.md)。人在单聊对全局 Steward 说一句「新建项目 X，从 Drive 下最新版本作初始提交，再配好预发布和生产」，全局 Steward 依次：

1. `create_repo`（绑定写 `repo_tools = true`）：在 `[repo_tools] allowed_owners` 允许的账号或组织下建私有仓库，未指定时用 `owner`，旧配置仅允许 `owner`，首次提交是凭据代理 `sources_folder_id` 下同名文件夹（或人给的链接）里最新修改的文件，压缩包解开；仓库已存在、文件夹空、下载或推送失败都停下说明。
2. 转给系统 Inbox 接入（`return_to` 自己），等回信。
3. 回信 `done` 才转给运维 Inbox 配环境（`return_to` 自己）；回信 `failed` 时停下，代码也拒绝这次转投。
4. 汇总：仓库、hb 接入、两个环境的地址和状态。

运维 Inbox 是写了 `ops_tools = true` 的绑定（限制同系统 Inbox），Steward 只有一个 `dokploy(action, args)` 工具，在绑定的 `workdir` 里跑 `[ops_tools] dokploy_script`（令牌由脚本从钥匙串取、输出打码，不能 `--reveal`），没有通用命令行，也不能转投。每次调用先由代码分类再过 `PermissionPort`：

| 分类（`PermissionPort` 里的名字） | 什么调用 | 默认 |
|---|---|---|
| `dokploy.read` | 查询 | 放行 |
| `dokploy.change` | 建项目、环境、应用、域名，改配置（不上线） | 放行 |
| `dokploy.deploy` | 部署、启动、重新部署到 `staging_environments` 里的环境 | 放行 |
| `deploy_production` | 部署、启动、重新部署、回滚到别的环境或看不出环境的，开了自动部署的来源 | 挂起等批准 |
| `delete_resource` | `remove` / `delete` | 挂起等批准 |
| `dokploy.admin` | `post` 到未登记的服务 | 挂起等批准 |

挂起的调用不执行；审批卡列出每个调用和环境，以及 Steward 扣住的答复（预发布的状态）。批准后 Hibernaut 按原参数重新分类、过门禁再执行，然后再开一轮 Steward 报告结果；拒绝则只再开一轮，让它说生产没上线。系统和运维 Steward 做不成时答复带 `status: failed`，回信据此标失败。

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
| 系统 Inbox 不往外转投；转给它的那一轮须有人的单聊消息或本机运营者的事件，或一条来自系统 Inbox、`done`、本会话还在等的回投 | 拒绝；审批放行、Run 收尾时重判 |
| 系统 Inbox 只收全局 Steward 转来的、绑定群里名单上的人发的、本机运营者投的和自己的审批 | 事件标记 `failed`；远端或经代理的 `POST /events` 返回 403；绑定群里名单外的人的消息在进 Event Inbox 前丢弃、不回 |
| `[[feishu_groups]]` 的 `inbox_id` 是带 `system_tools` 的绑定的精确 pattern，或被一条 agent 全是 `steward` 的项目绑定覆盖的项目 Inbox（这时 `approvers` 必填）；一个 Inbox 只绑一个群、`chat_id` 不重复、`members` 非空 | 配置拒绝加载；热加载保留旧配置 |
| 项目群的 Inbox 只收这个群名单上的人的消息；这个项目所有 Inbox 的审批只认 `approvers` | 别的聊天的消息、别人的点击丢弃不回；远端 `POST /events` 403 |
| 系统 Inbox（`system_tools` 或 `ops_tools`）是单个精确 `sys:<name>`、只读、agent 全是 `steward`、没有 `forward_to`，两个键不写在同一条 | 配置拒绝加载；热加载保留旧配置 |
| `repo_tools` 只在精确 ID、只读、全 `steward`、不是 `sys:` 的绑定上，且有 `[repo_tools] owner` 与 `[gdrive]` 代理；`ops_tools` 要有 `[ops_tools] dokploy_script` | 配置拒绝加载；热加载保留旧配置 |
| 运维 Inbox 部署到不是预发布的环境（含看不出环境的、开自动部署的来源）、删除、未登记的 Dokploy 服务 | 不执行，挂起等人批准；批准后由 Hibernaut 执行 |
