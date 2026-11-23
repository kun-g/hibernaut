# Hibernaut

Hibernaut 是常驻型 Agent（Always-on Agent）的运行时：平时休眠、不花 token；GitHub Issue、飞书消息、MCP 调用、Google Drive 任务文件或定时器到达时唤醒，在独立的 git worktree 里调用 Claude Code 等 CLI 干活，把结果写回后继续休眠。每轮执行只有三种结局：`Silent`（静默结束）、`Emit`（发一条消息）、`Suspend`（保存断点、等人审批）。

纯 Python 标准库实现（3.11+），运行不需要第三方依赖。

## 功能一览

| 功能 | 做什么 |
| :--- | :--- |
| 多入口接单 | `POST /events`、经 Switchboard 转入的 GitHub 和飞书事件、审批卡片回调、MCP、Google Drive 任务队列、定时器；重复事件只执行一次 |
| 按 Inbox 并发 | 每个 `inbox_id` 一条串行队列，不同 Inbox 并行；同一个 Issue、Drive 文件或飞书话题的事件接着同一会话和 worktree 做 |
| 多执行器 | claude / cursor-agent / grok / dsh 等 CLI 和角色执行器；按事件、会话、项目、全局四级选择，会话粘在首次选定的执行器上 |
| SDE 开发角色 | 在任务 worktree 里测试先行开发，送评审，按评审意见返工；派单写 `executor_id = "sde"`（Drive 任务 frontmatter 写 `executor_id: sde`），项目也可设为默认执行器；GitHub Issue 以 `# 标题` + 正文为需求、标题为 PR 标题 |
| 代码评审 | 评审者可以是只读的 Claude Code 角色 `code-reviewer`，也可以是任意 CLI 执行器（如 grok，模板写 `terminal = "orca"` 可在 Orca 终端里旁观评审）；按需求、实际 diff 和测试证据给出 `pass` / `changes_requested`；SDE 内置评审第 1 轮通审，第 2 轮起只做增量复核：只有上一轮没修好的项和之后改动引入的新问题能打回，其余由程序降为跟进，交付时列在 PR 正文「跟进」小节和完成消息里 |
| PR 交付 | SDE 完成后由 Hibernaut 合入主干、按改动类型定版本号、跑检查、推分支、开 PR，可选在评审通过且 CI 绿后自动合并；合入主干冲突（版本号以外）或合入后检查失败时交回同一开发会话解决，已通过评审且只解了冲突的（逐块核对解法只由两侧内容组成）直接重跑检查续交付，另有改动的只评审开发者自己的增量（不含主干带进来的改动），超过次数上限才挂起等人；合并后删除源分支并结案；与任务来源无关 |
| 可见终端 | 运行在 Orca 终端里打开，人可以旁观、回答提问，后续事件回到同一对话；项目设 `handoff = "peer"` 时任务、返工与结果经 ccpeer 消息往来，结果落在 launchd 常驻接收端的收件箱，hb 重启不丢 |
| 工作区管理 | 按任务自动建 `hb-*` worktree、复用，证据齐全（PR 已合并、无未提交改动等）后才回收；交付合并 PR 后自动关闭会话并回收 worktree 和本地分支；当时被排队事件或在线会话挡住的，由后台清扫（启动时一轮，之后默认每 5 分钟，`[runtime] worktree_sweep_interval_seconds`）在其结束后回收；目录已被人手或 Orca 删掉的记录，在没有排队事件、未结束的 Run 等占用时清除，并关闭引用它的会话、发一条告警 |
| 项目规范注入 | 读取被改仓库的 `AGENTS.md` 与 `guidance.toml`，按任务类型挑选规范文档，从固定 commit 读取 |
| 人工审批 | 权限门禁返回 `defer`、agent 需要人回答或交付受阻时挂起，审批后从断点继续 |
| 安全 | agent 进程不持有真实密钥（运营者在 `env` 里显式写的除外）：API key 由外置代理注入，Drive 密钥由独立系统用户保管，GitHub 走宿主机凭据 |
| 运维 | 配置热加载、跟随 `origin/main` 自动更新；Outbox 上报失败按 5/15/45/135/405 秒持久化退避，重启不丢进度 |
| 执行账本 | 同仓的独立包 `src/ledger/`：每个 Run 记一条档案（结局、执行器、耗时、错误类别、日志、token 与费用），同进程 Switchboard 的审计也记入，存在独立的 `ledger.db`；执行器失败按类别分流（瞬时故障 5/15/45 秒退避重试，配置或权限错误第 1 次即停，执行故障不重试），最终失败发告警；MCP 只读工具查询 Run、日志、审计与执行器统计，`hibernaut_replay_event` 重放失败事件。设计见 [`docs/observability-ledger-design.md`](docs/observability-ledger-design.md) |
| Switchboard | 同仓的独立包：外部事件归一、去重、路由、统一出站；可与 Hibernaut 同进程运行，或经 Relay 连到公网的 Switchboard；`--check` 在发版前无副作用地校验配置，停机时先结清在途请求、投递和发送 |
| Taskboard | 同仓的独立包：多来源任务归集（一个任务可挂多个来源）、5 态状态机、DAG 依赖与环检测、上游完成后自动解锁下游，经 Switchboard 发 `work.ready` 或给人发待办通知；SQLite 存储。独立常驻服务 `python -m taskboard.app` 经 Relay 消费 Switchboard 事件、经 Switchboard `/mcp` 上行，并以 HTTP `/mcp`（可挂同域子路径，如 `/taskboard/mcp`）或 `--mcp-stdio` 提供 7 个 `tb_*` MCP 工具；Hibernaut 接 `work.ready` 执行，并以 `execution.reported` 回报 `claim` / `update_stage` / `complete` / `release`（审批挂起后恢复仍回写同一任务）。设计见 [`docs/taskboard.md`](docs/taskboard.md) |
| Runner | 同仓的独立包，只依赖标准库：代码执行宿主，负责 worktree 隔离与安全回收、子进程监管（硬超时与静默看门狗）、多 CLI 驱动与会话续接；本机同进程直调，或经反向 WebSocket 由控制面按工作区亲和调度多个节点。`agent_runtime.adapters.runner_bridge.RunnerExecutorBridge` 把它适配为 `ExecutorPort`，目前未接入 `hibernaut.toml`，默认执行器不变。设计见 [`docs/runner.md`](docs/runner.md) |
| Dashboard | 同仓的独立包 `src/dashboard/`：只读本地 Web 看板，用于实时查看 Hibernaut、Taskboard 与 Switchboard 运转状态（拓扑与延迟、活跃 Run、挂起/异常告警、事件队列、PR 交付与 worktree、Taskboard 任务、Switchboard 路由与流量、执行账本、`ccpeer` 与 Outbox 报告）。运行 `PYTHONPATH=src python3 -m dashboard --config hibernaut.toml` 访问 `http://127.0.0.1:8877` |

## 工作方式

| 概念 | 含义 |
| :--- | :--- |
| Event Inbox | 全局唯一的事件表（SQLite），所有入口的事件都写进这里 |
| Inbox / `inbox_id` | 逻辑队列。同一 `inbox_id` 同时只有一个 Run，不同 `inbox_id` 并行。常见粒度：`user:main`、`proj:<name>`、`<project>:<issue>` |
| `correlation_key` | 外部业务实体的稳定键，如 `github:kun-g/hibernaut#42`、`gdrive:<file_id>`；同键事件进入同一会话 |
| Session | 跨多次 Run 的对话与断点，记录绑定的执行器和 worktree |
| Run | 一次唤醒执行：处理一批事件，返回 `Silent` / `Emit` / `Suspend` |
| Executor | 干活的引擎：CLI（`claude`、`grok` 等）或角色执行器（`sde`、`code-reviewer`） |

一轮 Run：事件写入 Event Inbox 并立即 ACK → 找到或新建会话、选定执行器 → 执行，每次工具调用先过权限门禁 → 写回状态与结果，释放租约后休眠。术语定义见 [`docs/glossary.md`](docs/glossary.md)。

## 快速开始

```bash
git clone https://github.com/kun-g/hibernaut.git
cd hibernaut

# 不需要 API key 的端到端演示：Silent -> Suspend -> 人工批准 -> 恢复 -> Emit
PYTHONPATH=src python3 -m agent_runtime.app.demo

# 正式运行：复制配置，填写主检出路径、执行器和环境变量名
cp hibernaut.example.toml hibernaut.toml
PYTHONPATH=src python3 -m agent_runtime.app.main --config hibernaut.toml
```

没有配置执行器时，daemon 以 ingest-only 模式启动：照常接单，事件保持待处理，等配置补好再执行。

派一个开发任务（模板见 [`docs/dispatch/`](docs/dispatch)）：

```bash
curl -s localhost:8080/events -H 'Content-Type: application/json' -d @docs/dispatch/sde-task.json
```

在 Claude Code 等客户端里接入 MCP：`python3 -m agent_runtime.app.main --mcp-stdio --config <hibernaut.toml>`；远程接入设 `mcp_enabled = true`，用 `Authorization: Bearer <HIBERNAUT_MCP_TOKEN>` 访问 `POST /mcp`。

## 配置

Hibernaut 的配置在 `hibernaut.toml`，带注释的模板是 [`hibernaut.example.toml`](hibernaut.example.toml)。保存后自动热加载：入口、消息发送和 Drive 轮询继续运行，进行中的任务用旧配置跑完，新事件用新配置；非法配置保留当前设置；改存储路径或 `[switchboard]` 需要重启。

| 配置段 | 管什么 |
| :--- | :--- |
| `[runtime]` | 数据库与记忆目录、全局并发上限 `max_concurrency`、新事件策略（`defer` / `fold` / `interrupt`）、全局默认执行器；`ledger_db_path` 是执行账本的独立数据库（默认 `db_path` 同目录的 `ledger.db`，运行日志在它旁边的 `logs/runs/`） |
| `[channels]` | HTTP 端口、MCP 的密钥环境变量名；`outbox_dir` 设置后每条 Outbox 消息另存为该目录下一个带任务上下文的 Markdown 文件；配置了 `auto_merge` 却停在 PR 未合并时，状态为「待合并」并写明 PR 链接与未合并原因 |
| `[executors.<id>]` | CLI 模板；写 `role = "sde"` 或 `role = "code-reviewer"` 则是角色执行器。SDE 的 `review`、`reviewer` 决定评审开关和评审者；可选 `max_concurrency` 限制同时跑该执行器的 Inbox 数（跨项目合计） |
| `[launchers.<name>]`、`[credential_proxy]` | Claude Code 启动命令；`billing = "api"` 时经外置凭据代理。启动器与角色执行器的 `env` 原样传给 Claude Code，`model` 可写任意模型名，例如经本机网关跑非 Claude 模型（示例见模板里的 `gpt-reviewer`） |
| `[projects.<name>]` | 仓库、检出路径、服务的 `inbox_id`（字符串或正则数组）、默认 `executor`、worktree 与终端后端；可选 `max_concurrency` 限制本项目同时在跑的 Inbox 数 |
| `[projects.<name>.delivery]` | PR 交付：开 PR、自动合并、版本文件、检查命令、是否要求评审和 CI、冲突与检查失败交回开发的次数上限（`max_handbacks`）、合并后是否删除源分支（`delete_branch`）、来自 GitHub Issue 的任务在 PR 正文里怎么写这个 Issue（`issue_link`：`closes` 合并后关闭 / `refs` 只关联，默认 / `none` 不写） |
| `[gdrive]` | Drive 任务队列，部署见 [`deploy/gdrive-proxy/`](deploy/gdrive-proxy/README.md)。任务可引用队列根下 `附件/` 里的文件（frontmatter `attachments` 写文件 ID 或原名，逗号分隔、`[a, b]` 或逐行 `- a` 均可，或正文里写该文件的 Drive ID），Run 开始时下载到 `attachments_dir`（默认数据库旁的 `attachments/`，在 worktree 之外），任务正文末尾附上路径、大小和 sha256，会话结案后删除。上传 `.md` 任务文件时关掉 Drive「将上传的文件转换为 Google 文档格式」 |
| `[switchboard]` | 同进程运行 Switchboard 或经 Relay 连接远程 Switchboard；公网 VPS 容器部署见 [`deploy/switchboard/`](deploy/switchboard/README.md)，与 Taskboard 同域部署见 [`deploy/taskboard/`](deploy/taskboard/README.md) |

并发上限分三级：全局、项目、执行器，后两级不写就不单独限制，大于全局值时以全局为准。满额的一级只让受它限制的 Inbox 按到达顺序排队，其他 Inbox 照常运行。

GitHub 入站只经 Switchboard。把 GitHub Webhook 指向 Switchboard 的 `/webhooks/github`，在 `switchboard.toml` 里设置 `[github] require_label` 和仓库路由，使路由目标匹配 Hibernaut 项目的 `inbox_id`，再启用同进程桥接或 Relay。未部署 Taskboard 时，按 `switchboard.example.toml` 的 `[routing.event_consumers]` 示例把 `work.opened`、`work.updated`、`work.closed` 路由到 Hibernaut。部署 Taskboard 时保持默认路由，按 [`taskboard.example.toml`](taskboard.example.toml) 写 `taskboard.toml`，设置 `SWITCHBOARD_RELAY_TOKEN`、`SWITCHBOARD_MCP_TOKEN`、`TASKBOARD_MCP_TOKEN` 后运行 `python -m taskboard.app --config taskboard.toml`；`[intake] auto_ready_sources`（默认 `["github"]`）来源的 Issue / PR（包括后贴触发标签的旧 Issue）直接进入 `ready`，由 Taskboard 发 `work.ready` 给 Hibernaut，Hibernaut 执行时把状态回写 Taskboard（这些内部同步不发评论）。人工待办与依赖异常通知没有可回复的来源时发往 `switchboard.toml` 的 `[outbound] notify_target`，未设置则发往 `approval_target`。Switchboard 与 Taskboard 共用一个域名时，`[http] base_path`（如 `/switchboard`）让 Switchboard 在带前缀和不带前缀的路径上都可访问。升级前使用本机 Issue 轮询或 daemon Webhook 的部署需要先切换入口。

飞书事件订阅和卡片回调都指向 Switchboard 的 `/webhooks/feishu`。在 `switchboard.toml` 设置聊天路由和应用凭据环境变量名。要为无来源会话发审批卡，请配置 `[outbound]` 的 `approval_channel = "feishu"` 和 `approval_target`（接收卡片的聊天 ID）。

旧 `hibernaut.toml` 仍可加载。以下键各自触发 `UserWarning` 并被忽略，可从对应配置段删除：

| 配置段 | 已停用的键 |
| :--- | :--- |
| `[runtime]` | `poll_mode`、`poll_interval_seconds` |
| `[channels]` | `github_token_env`、`github_webhook_secret_env`、`feishu_app_id_env`、`feishu_app_secret_env`、`feishu_verification_token_env`、`feishu_default_chat_id` |
| `[projects.<name>]` | `github_poll`、`poll_labels`、`feishu_chat_id` |

`[gdrive] poll_interval_seconds` 继续控制 Drive 任务队列的轮询周期。

Switchboard 自己的配置在 `switchboard.toml`（模板 [`switchboard.example.toml`](switchboard.example.toml)）。发版或重启前先校验：`PYTHONPATH=src python3 -m switchboard.app --config switchboard.toml --check`，不建目录和数据库、不监听端口、不访问网络；通过时退出码 0，否则在 stderr 给出配置路径和字段名并以 1 退出。顶层 `shutdown_timeout_seconds`（默认 10 秒，正数，写在第一个 `[表]` 之前）是停机等待在途工作的上限，`docker stop`、systemd 等的强杀时限要比它长。细节见 [`deploy/switchboard/`](deploy/switchboard/README.md)。

## 运行

长期运行用自动更新监督进程（需在一个专用的、位于 `main` 的检出里）：

```bash
python3 scripts/run_updating.py --config hibernaut.toml
```

除 `--interval` 与 `--wait-log-interval` 外的参数原样传给 daemon。它每 300 秒检查一次 `origin/main`，能快进时让旧 daemon 停机排空，旧 daemon 退出后再更新代码并重启；检出不干净、不在 `main` 或历史分叉时保持现状，排空期间检出被改动则跳过这次更新。origin 必须是你信任其提供可执行代码的来源。

停机（SIGINT / SIGTERM，自动更新也走这条路）分两步：先只停止认领新事件，进行中的 Run 照常跑完并写回结果（正在等 Orca 终端里 agent 结果的 Run 例外：旧进程不等它、不关终端，登记和 `runs/` 下的状态目录留给新进程接着等，agent 不会收到重复任务），期间 HTTP 入口（`/events`、`/mcp`）照常接收入库并返回 202、留给下一个进程处理，Outbox 投递、定时器和 Drive 轮询照常运行（worktree 回收清扫不再开始新一轮）；Run 全部结束后才关闭 HTTP（等已接收的请求写完）、停后台服务并退出。新进程启动后先接回这些 Run，再处理同一 Inbox 的新事件。

经 `run_updating.py` 重启时 HTTP 入口不拒绝连接：旧 daemon 停止接收后把监听 socket 交还监督进程，监督进程带着它快进、`execv`，再交给新 daemon 直接使用；这段时间到达的连接在内核 backlog（128）里排队，新 daemon 开始监听后按序处理。实测排队最长约 0.3 秒（不跑 Run 的测试实例；生产上取决于新进程的导入与组装耗时），客户端超时须长于这段时间。Switchboard 中继（`mode = "relay"`）在旧 daemon 退出前主动断开，新 daemon 组装时立即重连（实测断开约 0.3 秒），断开期间的事件由 Switchboard 暂存，重连后立即重投。不经监督进程直接运行 daemon、或第一次启动时，daemon 照旧自己绑定端口；进程内 Switchboard（`mode = "in_process"`）自己的 HTTP 端口不在交接范围内，重启期间仍会拒绝连接；它在 daemon 停机的最后排空在途工作（上限 `shutdown_timeout_seconds`）再关闭。

再发一次 SIGINT / SIGTERM（直接发给 daemon，或发给 `run_updating.py`，它会转给 daemon）则不再等待：进行中的 Run 按中断处理，不写半成品，事件回到 `pending`，由下一个进程重跑。

`run_updating.py` 把 daemon 放在独立会话里运行，终端的 Ctrl-C 只到监督进程、由它转给 daemon。监督进程收到 SIGHUP（如关闭终端、断开 SSH）也按停机处理，排空后连同 daemon 一起退出。SIGKILL 无法捕获：监督进程被 `kill -9` 时 daemon 不会收到信号，会继续运行，需另行停止（`pgrep -f agent_runtime.app.main` 查找）。监督进程等 daemon 退出不设上限（不会因排空太久而强杀），但每 `--wait-log-interval` 秒（默认 60）在 stderr 记一行监督进程 PID、daemon PID 与已等待秒数；daemon 卡住不退时据此找到它，再发一次信号或手动处理。

查看版本：`PYTHONPATH=src python3 -m agent_runtime.app.main --version`。版本规则见 [`docs/process/versioning.md`](docs/process/versioning.md)。

## 文档

| 文档 | 内容 |
| :--- | :--- |
| [`docs/architecture.md`](docs/architecture.md) | 完整设计：生命周期（§1）、分层（§2）、会话与路由及各入口（§3）、执行器、评审、SDE、PR 交付、worktree、多 inbox 项目、热加载（§4）、并发与新事件策略（§5）、Switchboard（§7）、ccpeer（§9）、Taskboard（§10）、执行账本（§11）、Runner（§12）、Dashboard（§13） |
| [`docs/taskboard.md`](docs/taskboard.md) | Taskboard 完整设计方案：职责边界、5 态状态机、DAG 解锁与异常传播、Flow 展开、与 Switchboard 的 Schema 契约、DDL 与门禁 |
| [`docs/observability-ledger-design.md`](docs/observability-ledger-design.md) | 执行账本（Observability / Ledger）设计基线：定位与边界、双层数据契约、错误分类与分流、日志与计费、重放与 Memory 闸门；接线见架构文档 §11 |
| [`docs/runner.md`](docs/runner.md) | Runner 执行面设计（v1.0 定稿）：`RunnerPort` 协议与双模传输、多节点注册与亲和、worktree 三道保险、CLI 驱动与看门狗；交互蓝图 [`docs/runner_blueprint.html`](docs/runner_blueprint.html) |
| [`docs/glossary.md`](docs/glossary.md) | 术语表（术语以它为准） |
| [`AGENTS.md`](AGENTS.md) | 开发规范与提交门禁 |
| [`docs/dispatch/`](docs/dispatch) | 派单模板：SDE 任务（`/events`、Drive）、独立评审 |
| [`deploy/gdrive-proxy/`](deploy/gdrive-proxy/README.md) | Drive 队列凭据代理的安装与维护 |
| [`deploy/switchboard/`](deploy/switchboard/README.md) | Switchboard 公网 VPS 容器部署（Docker Compose + Caddy 自动 TLS，或接入 Dokploy / Traefik 等已有反向代理）、GitHub App 收事件与回写评论、`scripts/onboard_project.py` 一键接入新仓库、Hibernaut 配对、冒烟验收、灰度切流与回滚 |
| [`deploy/taskboard/`](deploy/taskboard/README.md) | Switchboard + Taskboard 同域子路径联合部署（`/switchboard`、`/taskboard`，Dokploy / Traefik 或 Caddy）、各服务独立 ref 与配置哈希的按需更新、路由切流与一键回滚、旧域名平滑下线 |
| [`scripts/prepare_taskboard_deploy.py`](scripts/prepare_taskboard_deploy.py) | `render`：校验两份配置、生成 `TASKBOARD_MCP_TOKEN`（0600 文件，默认打码）、算配置哈希与切流状态；`smoke --base-url https://<域名>`：两个健康检查、`/taskboard/mcp` 无令牌 401、带令牌列出 7 个 `tb_*` 工具 |
| [`deploy/ccpeer/`](deploy/ccpeer/README.md) | `ccpeer` 接收端的 launchd 常驻部署（登录自启、崩溃拉起）、运行时文件、同名接收端规则与手工排查命令；`handoff = "peer"` 的项目依赖它 |
| [`docs/adr/`](docs/adr) | 架构决策记录 |

## 开发

代码按整洁架构分四层，依赖只能由外向内：

```text
src/agent_runtime/
├── core/       # L0 领域模型 + L1 端口，只依赖标准库
├── engine/     # L2 编排：入站、Worker、定时器、Outbox 投递；只依赖 core/
├── adapters/   # L3 实现：SQLite、记忆、权限、执行器、交付、各通道
└── app/        # 配置加载与组装根（daemon、demo）
src/switchboard/  # 独立包，不依赖 agent_runtime
src/ccpeer/       # 独立包：与本机 Claude Code 会话收发消息（发送、会话发现、常驻接收端），只依赖标准库
src/taskboard/    # 独立包：多源任务归集与 DAG 编排（core/ L0、ports.py L1、engine/ L2、adapters/ L3、app/ 配置与入口），只依赖标准库与 switchboard
src/runner/       # 独立包：代码执行宿主（worktree、进程监管、CLI 驱动、反向 WebSocket 多节点调度），只依赖标准库
src/ledger/       # 独立包：执行账本与审计（独立 ledger.db、运行日志、错误分类、MCP 只读查询），只依赖标准库
src/dashboard/    # 独立包：只读本地 Web 看板（HB / TB / SB 实时总览与详情页），只依赖标准库
schemas/taskboard/  # Taskboard 与 Switchboard 之间事件 metadata 的 JSON Schema（Draft 2020-12）
schemas/runner/v1/  # Runner 协议（capabilities、run_request、run_result、interrupt、reclaim）的 JSON Schema（Draft 2020-12）
schemas/v1/         # 执行账本的 AuditRecord / RunRecord JSON Schema（ADR-0001，Draft 2020-12）
```

提交前三道门禁必须全绿：

```bash
pip install -e ".[dev]"
lint-imports   # 分层与反向依赖检查
pytest -v      # 契约、并发、端到端测试（本地快检可用 pytest -n auto $(python scripts/select_tests.py)）
pyright        # core/、engine/、switchboard/、ccpeer/、taskboard/、runner/、ledger/、dashboard/ 的 strict 类型检查
```
