# ccpeer 接收端常驻部署（macOS launchd）

`ccpeer` 接收端以一个名字（如 `hibernaut`）登记成可被 Claude Code `SendMessage(<名字>)` 寻址的会话，把收到的消息写进收件箱。它在前台运行、不 fork、不绑定宿主进程、没有空闲超时，所以要交给 launchd 监督：登录后自启，退出或崩溃后自动拉起。

## 安装

1. 复制模板并替换占位：

   ```bash
   cp deploy/ccpeer/com.hibernaut.ccpeer.plist.example ~/Library/LaunchAgents/com.hibernaut.ccpeer.plist
   ```

   - `/Users/YOU` 换成自己的家目录；`PYTHONPATH` 指向本仓库的 `src`；解释器需 Python ≥ 3.11。
   - `--as` 是收信名字；对方回复时写 `SendMessage(to="<名字>")`。
   - **每个**需要互通的 Claude Code 配置目录都写一组 `--config-dir`。launchd 不带 shell 环境，`CLAUDE_CONFIG_DIR` 在这里为空：用 `CLAUDE_CONFIG_DIR=~/.claude-x claude` 起的会话登记在 `~/.claude-x/sessions`，漏写这个目录，那些会话就找不到这个接收端。

2. 加载并检查：

   ```bash
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.hibernaut.ccpeer.plist
   launchctl print gui/$(id -u)/com.hibernaut.ccpeer | grep -E 'state|pid'
   PYTHONPATH=src python3 -m ccpeer list | grep standalone   # 能看到 hibernaut [standalone]
   tail -f ~/Library/Logs/ccpeer.log
   ```

## 运行时文件

| 路径 | 内容 |
| :--- | :--- |
| `<config-dir>/sessions/<pid>.json` | 登记（`name`、`messagingSocketPath`、`peerProtocol`、`standalone: true`），权限 0600 |
| `<config-dir>/sessions/<pid>.<sha256(socket)>.key` | 会话密钥 `peerToken`，权限 0600 |
| `/tmp/cc-socks/<pid>.sock` | 接收端监听的 Unix socket，权限 0600 |
| `~/.ccpeer/<名字>/inbox.jsonl` | 收到的消息，每行 `msg_id`、`ts`、`type`、`from_addr`、`from_name`、`from_mode`、`body` |
| `~/.ccpeer/<名字>/control.jsonl` | 控制回执（如 `held`：对方会话把消息扣下等人工批准） |
| `~/.ccpeer/<名字>/receiver.json` | 运行中的接收端 pid 与回信地址；`send` 用它填 `from` |

登记每 60 秒重写一次（`--heartbeat`），被别的进程清掉后会自己补回。SIGTERM / SIGINT / SIGHUP 时删除登记、密钥、socket 与 `receiver.json` 后退出码 0。

## 同名接收端

启动时若发现同名登记：对应进程存活且 socket 可连，拒绝启动、退出码 2（launchd 按 `ThrottleInterval` 10 秒后重试，旧实例退出后新实例接管）；进程已死或 socket 不通、且登记是 `standalone`（ccpeer 或旧 `cc-peer` 脚本留下的），删掉残留登记后接管。同一状态目录还有文件锁 `receiver.lock`，防止两个实例同时写同一个收件箱。

## 停止、升级

```bash
launchctl bootout gui/$(id -u)/com.hibernaut.ccpeer        # 停止并卸载
launchctl kickstart -k gui/$(id -u)/com.hibernaut.ccpeer   # 代码更新后重启
```

收件箱不会因重启丢失；读取方保存游标（`inbox` 的 `cursor=` 字节偏移或最后一条 `msg_id`），重启后从原位置续读。

## 手工排查

```bash
export PYTHONPATH=src
python3 -m ccpeer list                                   # 存活会话
python3 -m ccpeer send --as hibernaut --to <会话名|pid> "请用 SendMessage 回复 hibernaut"
python3 -m ccpeer send --as hibernaut --to <会话名> - < body.txt   # 正文原样发送，不 strip
python3 -m ccpeer inbox --as hibernaut --cursor 0        # 打印记录，下一游标写到 stderr
python3 -m ccpeer inbox --as hibernaut --control         # 控制回执
```

协议是逆向所得，已验证的 Claude Code 版本见 `ccpeer.VERIFIED_CLAUDE_CODE_VERSIONS`。会话登记的 `peerProtocol` 不在 `ccpeer.SUPPORTED_PEER_PROTOCOLS` 里时，`send` 报 `UnsupportedProtocol` 并拒绝发送；Claude Code 升级后先重新验证帧格式，再放开新版本。

## Hibernaut 的 peer 交接

项目在 `hibernaut.toml` 里设 `handoff = "peer"` 后，Hibernaut 以 `[peer] receiver`（默认 `hibernaut`）的名义给 Orca 终端里的 Claude Code 会话发任务，会话用 `SendMessage(to="hibernaut")` 回结果。Hibernaut 只读 `~/.ccpeer/<名字>/` 下的收件箱（读到哪里记在自己的 SQLite `peer_cursors`），不启动接收端；接收端不在时 Hibernaut 启动失败。Hibernaut 起会话时清掉 `CLAUDE_CONFIG_DIR`，会话登记在 `~/.claude/sessions`，所以接收端至少要带 `--config-dir ~/.claude`。
