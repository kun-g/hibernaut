# 远端只读 Agent 节点部署（macOS）

Hibernaut 指名一个专用执行器（下文 `remote-claude`）时，这一单由另一台 macOS 上的 Runner 执行：一个槽位、一个项目、只读、只有 `Read` / `Grep` / `Glob` 三个工具，每次调用先经 Hibernaut 的 `PermissionPort`。设计与边界见 [`docs/runner.md`](../../docs/runner.md) §10。**默认关闭**；下面每一步都由人在部署机上做，代码合入不会改线上配置，也不会启动服务。

与本机只读试点（[`README.md`](README.md)）并行运行、互不依赖：

| | 本机只读试点 | 远端 Agent 节点 |
| :--- | :--- | :--- |
| Hibernaut 配置 | `[runner_pilot]` | `[runner]`，`mode = "relay"` |
| 谁会走 | 指定的一个任务级 Inbox | 指名 `remote-claude` 的事件 |
| Relay 端口、令牌文件 | `8791`、`runner-relay.token` | `8790`、`runner-agent.token`（端口或令牌文件相同时拒绝启动） |
| 节点服务 | `python -m runner.service` | `python -m runner.agent_service` |
| 节点配置、launchd 标签 | `runner.toml`、`com.hibernaut.runner` | `agent-runner.toml`、`com.hibernaut.runner-agent` |
| 改配置 | 热加载 | 重启 Hibernaut |

下文 `/Users/YOU`、`hibernaut-runner-node` 是占位，实际的机器名、账号、地址只写在各机器自己的配置里，不进仓库。

## 启用

### 节点

1. **专用 macOS 账号**（非管理员）。CLI 与 Runner 同一个账号，账号里只放 CLI 自己的登录和下面的 Relay 令牌；不放 GitHub、云服务、飞书等凭据，也不放能登录 Hibernaut 主机的 SSH 私钥。账号须保持图形登录：launchd 的用户服务和 CLI 的钥匙串登录都依赖它。在该账号里登录 CLI 并确认 `claude -p 'hi'` 可用；令牌类环境变量不会传给 CLI，登录只能是 CLI 自己保存的登录。CLI 版本须支持 `--restricted`（`claude --help` 里有）。

2. **两份检出**：运行 Runner 的代码，和供查询的专用克隆。

   ```bash
   mkdir -p ~/runner-agent && cd ~/runner-agent
   git clone <仓库地址> src-checkout      # Runner 代码；Python ≥ 3.11，只用标准库
   git clone <仓库地址> hibernaut         # 供查询；看到的是它的 origin/main，需要新提交时由人 git fetch
   ```

   私有仓库需要凭据时由人临时克隆，不把凭据留在这个账号里（`git remote -v` 里也不能有）。

3. **Relay 令牌**：在 Hibernaut 主机生成（见下），经已有的安全通道放到节点，属主为该账号、权限 0600：`~/.config/hibernaut/runner-agent.token`。

4. **配置与检查**（不连接 Relay，打印能力后退出；不合格时退出码 2）：

   ```bash
   cp src-checkout/deploy/runner/agent-runner.example.toml agent-runner.toml   # 改路径
   PYTHONPATH=src-checkout/src python3 -m runner.agent_service --config agent-runner.toml --check
   ```

   应只有一个执行器，`tool_authorization` 为 `true`，`slots.max_concurrent` 为 1。

5. **允许隧道**：打开「远程登录」，只允许这个账号。把 Hibernaut 主机隧道专用公钥写进 `~/.ssh/authorized_keys`，它只能在节点的 `127.0.0.1:8790` 上开监听，不能登录、不能转发到别处：

   ```
   restrict,port-forwarding,permitlisten="127.0.0.1:8790",permitopen="127.0.0.1:1",command="/usr/bin/false" ssh-ed25519 <公钥>
   ```

   由管理员在 `/etc/ssh/sshd_config.d/` 加一份，只对这个账号生效：断网后约 45 秒释放端口，否则旧连接会占着 8790、隧道重连失败；并关掉密码登录。

   ```
   Match User <节点账号>
     ClientAliveInterval 15
     ClientAliveCountMax 3
     PasswordAuthentication no
     KbdInteractiveAuthentication no
   ```

6. **交给 launchd**：

   ```bash
   cp src-checkout/deploy/runner/com.hibernaut.runner-agent.plist.example ~/Library/LaunchAgents/com.hibernaut.runner-agent.plist   # 改路径
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.hibernaut.runner-agent.plist
   ```

   隧道没通时服务按退避重试，每次中断只记一条日志。

### Hibernaut 主机

7. **令牌与隧道密钥**：

   ```bash
   mkdir -p -m 700 ~/.config/hibernaut
   (umask 077; python3 -c 'import secrets; print(secrets.token_urlsafe(32))' > ~/.config/hibernaut/runner-agent.token)
   ssh-keygen -t ed25519 -N '' -f ~/.ssh/hibernaut-runner-node
   ```

   在 `~/.ssh/config` 加 `Host hibernaut-runner-node`（`HostName`、`User`、`IdentityFile`），先由人连一次并核对主机指纹（隧道用 `StrictHostKeyChecking=yes`，不会自动接受）。

8. **隧道**：`com.hibernaut.runner-tunnel.plist.example` 复制到 `~/Library/LaunchAgents/com.hibernaut.runner-tunnel.plist`，改路径后 `launchctl bootstrap`。流量由 SSH 加密，两端都只监听 `127.0.0.1`；Relay 再用共享令牌鉴权。节点上 `lsof -nP -iTCP:8790 -sTCP:LISTEN` 应只有 `127.0.0.1:8790`。

9. **开路由**：`hibernaut.toml` 加（已有 `[runner]` 时合并）。`[runner_pilot]` 不用改，这里列出是为了说明两者并存：

   <!-- hibernaut.toml -->
   ```toml
   [executors.remote-claude]
   command = "/usr/bin/false"           # 必须：这个执行器从不在本机执行（不属任何项目的 Inbox、[runner] 关闭时会用到）
   max_concurrency = 1                  # 与节点的槽位一致：多出的 Inbox 排队，而不是等节点超时

   [runner]
   enabled = true
   mode = "relay"
   executors = ["remote-claude"]        # 必须显式列出，不填会把全部 CLI agent 都交给节点
   readonly_only = true
   allow_tools = ["Read", "Grep", "Glob"]
   timeout_seconds = 600
   node_wait_seconds = 60
   listen_host = "127.0.0.1"
   listen_port = 8790
   token_file = "~/.config/hibernaut/runner-agent.token"

   [runner_pilot]
   enabled = true
   inbox_id = "hibernaut:runner-pilot"
   runner_id = "mac-pilot"
   project_key = "hibernaut"
   port = 8791
   token_file = "~/.config/hibernaut/runner-relay.token"
   ```

   派单的 Inbox 须属于节点上的那个项目（`[projects.<name>] inbox_id` 能匹配到，且 `<name>` 与节点 `[projects.<键>]` 相同）；`[projects.<name>.inbox_bindings]` 对它有 `agents` 白名单时，把 `remote-claude` 加进去。

10. **重启前先查**（`[runner]` 打不开会让整个 Hibernaut 起不来，影响所有路由）：

    ```bash
    cp hibernaut.toml hibernaut.toml.before-runner-agent     # 改配置之前做，回退用
    PYTHONPATH=src python -c 'import sys; from pathlib import Path; from agent_runtime.app.config import load_config; from runner.transport.auth import read_relay_token; r = load_config(sys.argv[1]).runner; read_relay_token(Path(r.token_file)); print(r.mode, r.listen_host, r.listen_port, r.executors, r.readonly_only)' hibernaut.toml
    lsof -nP -iTCP:8790 -sTCP:LISTEN                         # 应无输出
    ```

    然后重启 Hibernaut。日志应有 `relay hub listening on ws://127.0.0.1:8790/ws/runner`，节点日志应有 `relay connected`。改好配置到重启之间，其他配置改动的热加载都会被拒绝（日志 `[runner] changes require a restart`）。

## 验证（首单）

```bash
curl -sS http://127.0.0.1:8080/events -H 'Content-Type: application/json' -d '{
  "inbox_id": "hibernaut:runner-agent", "external_id": "runner-agent-1", "correlation_key": "runner-agent:1",
  "payload": {"agent_id": "remote-claude", "title": "只读仓库查询", "body": "读 README.md，列出提交前要跑的检查命令，并写明依据的文件与行。"}}'
```

1. **账本**（默认与 `db_path` 同目录）：

   ```bash
   sqlite3 ledger.db "SELECT run_id, executor_id, runner_id, outcome_kind, emit_status, log_ref FROM runs
     WHERE inbox_id = 'hibernaut:runner-agent' ORDER BY started_at"
   ```

   应为 `remote-claude` / 节点的 `runner_id` / `emit` / `done`。`log_ref` 指向账本旁的 `logs/runs/<日期>/<run_id>.log`，其中 `=== runner log ===` 一行是 `runner://<节点>/<日期>/<Runner run_id>.log run_id=… sha256=… bytes=…`。
2. **节点核对同一单**：节点服务日志有 `run <Runner run_id> accepted (executor remote-claude, owner hibernaut:runner-agent)` 和 `ended: succeeded`；`shasum -a 256 ~/runner-agent/logs/<日期>/<Runner run_id>.log` 与账本里的 `sha256` 相同，`wc -c` 与 `bytes` 相同。
3. **只读与隔离**：该日志首行（`"subtype":"init"`）的 `tools` 只有三个，`cwd` 在节点的只读 worktree（路径含 `.readonly`）；结束后 `git -C ~/runner-agent/hibernaut worktree list` 只剩主检出，`git -C ~/runner-agent/hibernaut status --short` 为空。
4. **越界被拒**：再发一条要求读 `~/.config/hibernaut/runner-agent.token` 的查询。节点日志应有 `Read call refused on this node`，回复里没有内容。
5. **原路由**：给其他 Inbox 发不写 `agent_id` 的普通任务，账本 `runner_id` 为 `local`；给试点 Inbox 发一条协议校验，`runner_id` 为试点节点；给 `hibernaut:runner-agent` 换一个 `correlation_key`、不写 `agent_id`，仍由项目默认 Agent 执行（同一 `correlation_key` 的会话会继续用上次的 Agent）。
6. **节点缺席**：停节点服务，再发一条查询：等 `node_wait_seconds` 后该事件失败，账本 `outcome_kind = error`、`error_summary` 写明没有在线节点；期间其他路由照常。

## 回退

按影响从小到大：

1. **不再派单**：不再发 `agent_id = remote-claude` 的事件，从绑定白名单里去掉它。不用重启。
2. **立刻断开**：`launchctl bootout gui/$(id -u)/com.hibernaut.runner-tunnel`（Hibernaut 主机）或 `…/com.hibernaut.runner-agent`（节点）。节点先结束在途 Run 的进程组、删掉其只读 worktree。在途的单按瞬时故障退避重试（5 / 15 / 45 秒），仍无节点则失败；不会改由本机执行。
3. **关闭**：`hibernaut.toml` 把 `[runner]` 改 `enabled = false`（或恢复 `hibernaut.toml.before-runner-agent`）后重启 Hibernaut。`lsof` 查不到 8790；试点与其他路由不变。**保留 `[executors.remote-claude]`**：遗留的、仍指名它的事件在本机只会以 `[CLI:remote-claude exit=1]` 失败，不执行任何查询；确认没有遗留事件后再删。
4. **清理**：节点删 `authorized_keys` 那一行、两端删令牌文件与隧道密钥、节点退出 CLI 登录。

恢复时按「启用」第 8–10 步重做。换令牌：两端同时换文件，重启 Hibernaut 与节点服务。升级节点：`git -C ~/runner-agent/src-checkout pull` 后 `launchctl kickstart -k gui/$(id -u)/com.hibernaut.runner-agent`；Hibernaut 的自动更新不管节点。

## 异常

| 现象 | 处理 |
| :--- | :--- |
| Hibernaut 重启后起不来，报 `relay token file …` 或 8790 被占 | 修好令牌文件或端口；来不及时按「回退」第 3 步关闭 `[runner]` 再启动 |
| 账本 `outcome_kind = error`、`runner_id = local` | 没有节点完成这一单（`local` 是没有节点回报时的默认值，不表示在本机执行过）；`error_summary` 是 `no online runner matches…` 时看隧道与节点服务，`connection lost` / `disconnected` 时是执行中断线 |
| 隧道日志 `remote port forwarding failed for listen port 8790` | 节点上还有旧的 sshd 会话占着端口（等它超时，或管理员结束它），或 `permitlisten` 不符 |
| 节点日志反复 `relay connection lost` | 隧道没通、Hibernaut 没开 Relay，或令牌不符（Hibernaut 日志 `relay refused a connection: invalid token`） |
| 结果 `failed (permission_denied): … did not load its tool authorization hook` | CLI 没有加载工具钩子，结果已作废；核对 CLI 版本 |
| 结果 `failed (tool_error): … unknown option` | CLI 太旧，不认 `--restricted` 等参数 |
| 结果 `refused: … carries URL credentials` | 节点服务环境里的代理地址带账号密码，改用不带凭据的代理 |
| 事件挂起问人 | `allow_tools` 少了 `Read` / `Grep` / `Glob` 之一；按正常审批处理或补上后重启 |
| Runner 退出码 1 | 另一个同 `runner_id` 的连接顶替了它；确认只有一个服务 |
| 残留只读 worktree（节点断电或 `kill -9`） | `git -C ~/runner-agent/hibernaut worktree list` 查看，人确认后 `git worktree remove`；CLI 子进程须另外确认已退出 |
