# 只读试点 Runner 部署（macOS launchd）

（另一台机器上的只读 Agent 节点见 [`REMOTE.md`](REMOTE.md)，可与本试点并行。）

一条任务级 Inbox 的协议文件校验由这台独立 Runner 执行，设计与边界见 [`docs/runner.md`](../../docs/runner.md) §9。**默认关闭**；下面每一步都由人在部署机上做，代码合入不会改线上配置，也不会启动服务。

## 启用

1. **专用克隆**（Hibernaut 项目的主检出与任务 worktree 不与 Runner 共用目录）：

   ```bash
   git clone <仓库地址> ~/runner-pilot/hibernaut
   git -C ~/runner-pilot/hibernaut fetch origin <候选分支>
   git -C ~/runner-pilot/hibernaut cat-file -t <40 位提交号>      # 应输出 commit
   ```

   校验程序不联网，候选提交须先由人 fetch 到这个克隆里；没有时任务以失败结束，文案写明提交不在仓库中。

2. **Relay 令牌**（两端读同一份文件；须为本用户所有、权限 0600、不少于 32 个字符）：

   ```bash
   mkdir -p -m 700 ~/.config/hibernaut
   (umask 077; python3 -c 'import secrets; print(secrets.token_urlsafe(32))' > ~/.config/hibernaut/runner-relay.token)
   ```

3. **Runner 配置**：`cp deploy/runner/runner.example.toml ~/runner-pilot/runner.toml`，改路径，然后检查（不连接 Relay，打印能力后退出；配置或令牌不合格、或服务已在运行时退出码 2）：

   ```bash
   PYTHONPATH=src python -m runner.service --config ~/runner-pilot/runner.toml --check   # Python ≥ 3.11，只用标准库
   ```

4. **交给 launchd**：

   ```bash
   cp deploy/runner/com.hibernaut.runner.plist.example ~/Library/LaunchAgents/com.hibernaut.runner.plist   # 改 /Users/YOU
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.hibernaut.runner.plist
   launchctl print gui/$(id -u)/com.hibernaut.runner | grep -E 'state|pid'
   ```

   Hibernaut 还没开路由时服务会按退避重试连接，日志每次中断只记一条。

5. **开路由**：`hibernaut.toml` 加（模板见 `hibernaut.example.toml`），热加载即生效。`inbox_id` 用一个没有别的任务在用的任务级 Inbox：

   ```toml
   [runner_pilot]
   enabled = true
   inbox_id = "hibernaut:runner-pilot"
   runner_id = "mac-pilot"
   project_key = "hibernaut"
   port = 8791
   token_file = "~/.config/hibernaut/runner-relay.token"
   ```

   Hibernaut 日志应有 `[config] Reloaded`，Runner 日志应有 `relay connected`；`lsof -nP -iTCP:8791 -sTCP:LISTEN` 只有 `127.0.0.1:8791`。

## 验证

```bash
C=<40 位候选提交号>
curl -sS http://127.0.0.1:8080/events -H 'Content-Type: application/json' -d '{
  "inbox_id": "hibernaut:runner-pilot", "external_id": "runner-pilot-1", "correlation_key": "runner-pilot:1",
  "payload": {"title": "核验 Runner 协议文件", "candidate_commit": "'$C'"}}'
```

1. **结果**：Outbox 收到「Runner 协议文件校验通过」，含候选提交、五个文件的 SHA-256、汇总摘要、运行节点与 Runner `run_id`。
2. **复算**：在任一含该提交的克隆里跑消息末尾的命令，输出应等于汇总摘要；单个文件用 `git cat-file blob $C:<路径> | shasum -a 256`。
3. **账本**（默认与 `db_path` 同目录的 `ledger.db`）：

   ```bash
   sqlite3 ledger.db "SELECT run_id, runner_id, outcome_kind, emit_status, error_kind, log_ref FROM runs
     WHERE inbox_id = 'hibernaut:runner-pilot' ORDER BY started_at"
   ```

   `runner_id` 是试点节点。`log_ref` 指向账本旁的 `logs/runs/<日期>/<run_id>.log`，其中 `=== runner log ===` 一行给出 Runner 日志的位置（`runner://<节点>/<日期>/<Runner run_id>.log`，对应 Runner 的 `logs_dir`）、`sha256` 与字节数，可用 `shasum -a 256` 核对。
4. **受控中断**：先发一条 `"hold_seconds": 60`（换 `external_id`），看到 Runner 日志 `run … accepted` 后用同一 `correlation_key` 再发一条不带 `hold_seconds` 的。应见：

   - `pgrep -f protocol_check.py` 里原进程已退出；
   - 账本先是一行 `outcome_kind = interrupted`（带 `runner_id` 与 `log_ref`），再是一行 `emit` / `done`；
   - Outbox 只有最终一条结果，两个事件都是 `done`。

   同一 `correlation_key` 的事件合成一批，`candidate_commit` 必须相同，`hold_seconds` 以最后一条为准。
5. **原路径**：给其他 Inbox 发普通任务，仍由原执行器处理；给试点 Inbox 发 `agent_id`（旧键 `executor_id`）为其他执行器的事件，保持 `pending`（不执行）。

## 回退

1. `hibernaut.toml` 改 `enabled = false`（或删掉该表）并保存。在途校验照常跑完并写出结果；结束后 Relay 关闭，日志出现 `[runner_pilot] Relay closed.`，`lsof` 查不到 8791。想立刻结束在途校验，先发一条同 `correlation_key` 的新事件（受控中断）再改配置。
2. 试点 Inbox 按原路由执行：发一条普通任务确认走原执行器。
3. 仍指向 `runner-protocol-check` 的遗留事件（被强制停止留下的、显式写了该 `agent_id` 的）以「试点路由未对本 Inbox 开放，未执行」结束并关闭会话（显式指名、而该 Inbox 的绑定白名单没有它的，按白名单拒绝），不会改由其他执行器跑；重新启用后用 `hibernaut_replay_event(force=true)` 重放。
4. 停服务：`launchctl bootout gui/$(id -u)/com.hibernaut.runner`。SIGTERM 会先结束在途校验的进程组、删掉其只读 worktree 再退出。
5. 恢复：看到 `Relay closed.` 后再改回 `enabled = true`（端口未放开时这次热加载会被拒绝，届时再保存一次改动）。运行中改 `[runner_pilot]` 其他项会被拒绝并提示先关闭。

升级 Runner 代码后 `launchctl kickstart -k gui/$(id -u)/com.hibernaut.runner`；自动更新只重启 Hibernaut。

## 异常

| 现象 | 处理 |
| :--- | :--- |
| 试点事件一直 `pending` | Relay 没开（令牌文件不合格、端口被占），Hibernaut 日志有 `[runner_pilot]` 一行；修好后任一次配置热加载或重启会重开并执行。Relay 正常时是事件指名了其他执行器，或同一 `correlation_key` 的会话仍绑定在其他执行器上：换一个 `correlation_key` |
| 结果「未派发到 Runner」 | 30 秒内没有可用节点；看 Runner 服务是否在跑、`runner_id` / `project_key` 是否一致（不符时 Hibernaut 日志有 `relay refused runner`）。重放该事件 |
| 结果「连接中断，Run 未完成」 | Runner 服务被停或重启，进程已由它回收；重放该事件 |
| Runner 以 `kill -9` 或断电结束 | 校验进程至多再跑完一次（`hold_seconds` 上限 60 秒）。残留只读 worktree 用 `git -C ~/runner-pilot/hibernaut worktree list` 查看，人确认后 `git worktree remove` |
| Runner 退出码 1 | 另一个同 `runner_id` 的连接顶替了它；确认只有一个服务 |
