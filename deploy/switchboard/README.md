# Switchboard 容器部署（公网 VPS）

把 Switchboard 作为两个容器部署到公网 VPS：

- `switchboard`：只含 `src/switchboard/`（纯标准库，`python:3.12-slim`，UID 10001 的非 root 用户），监听容器网络内的 8790，不映射到宿主机。SQLite（含 WAL 文件）存在命名卷 `switchboard_data`（容器内 `/data`）。
- `caddy`：唯一对外的容器，占用 80/443，自动申请和续期 TLS 证书，把 HTTPS 与 `wss://<域名>/ws/relay` 的 WebSocket 升级转发到 `switchboard:8790`。

对外入口（下文以占位域名 `sb.example.com` 为例）：

| 路径 | 用途 |
| :--- | :--- |
| `/webhooks/github` | GitHub Webhook |
| `/webhooks/feishu` | 飞书事件订阅 |
| `/mcp` | MCP（`Authorization: Bearer <SWITCHBOARD_MCP_WEBHOOK_SECRET>`），Hibernaut 经它上报运行结果 |
| `/ws/relay` | Relay 长连接，本机 Hibernaut 主动外连，Switchboard 经它推送事件 |
| `/healthz` | 健康检查 |

仓库只保存模板。`.env`、`switchboard.toml`、`Caddyfile` 是每台机器的实例文件，已在 `.gitignore` 中忽略。

## 前提

- VPS 已安装 Docker Engine 与 Compose 插件（`docker compose version` 可用）。
- 域名的 A/AAAA 记录指向 VPS，防火墙放行 80 与 443（Caddy 用 80 完成 ACME 校验）。不要放行 8790。

## 部署

在 VPS 上的 Hibernaut 检出里执行（只构建 `src/switchboard/`，不需要安装 Hibernaut）：

```bash
cd deploy/switchboard
cp switchboard.docker.toml switchboard.toml
cp Caddyfile.example Caddyfile
cp .env.example .env
chmod 600 .env
```

1. `Caddyfile`：把 `sb.example.com` 换成你的域名。
2. `switchboard.toml`：按需改 `[routing]`（仓库、飞书群到 `inbox_id` 的映射）、`[github]`、`[feishu]`、`[outbound]`。`[github] require_label`（默认 `hibernaut`）决定哪些 Issue/PR 及其评论会入站，公开仓库不要清空；`[routing.event_consumers]` 在没有 Taskboard 时把 Issue/PR 事件直接投给 Hibernaut，部署了 Taskboard 再删掉。不要改 `[http] host/port`、`[sqlite] path` 和 `[relay] path`，它们与 Dockerfile、Compose、Caddyfile 对应；不要设置 `[relay] listen_port`，Caddy 只转发 8790。
3. `.env`：填六个变量，令牌可用 `openssl rand -hex 32` 生成。

   | 变量 | 内容 |
   | :--- | :--- |
   | `SWITCHBOARD_RELAY_WEBHOOK_SECRET` | Relay 令牌；本机 Hibernaut 的 `SWITCHBOARD_RELAY_TOKEN` 填同一个值 |
   | `SWITCHBOARD_MCP_WEBHOOK_SECRET` | `/mcp` 的 Bearer 令牌；本机 Hibernaut 的 `SWITCHBOARD_MCP_TOKEN` 填同一个值 |
   | `SWITCHBOARD_GITHUB_WEBHOOK_SECRET` | GitHub Webhook 的 Secret |
   | `SWITCHBOARD_GITHUB_TOKEN` | 发 GitHub 评论用的 Token |
   | `SWITCHBOARD_FEISHU_WEBHOOK_SECRET` | 飞书事件订阅的 Verification Token |
   | `SWITCHBOARD_FEISHU_TOKEN` | 飞书 tenant access token |

4. 构建并启动：

   ```bash
   docker compose up -d --build
   docker compose ps          # switchboard 显示 healthy 后 caddy 才启动
   docker compose logs -f     # 出现 "[switchboard] relay endpoint ... (transport=hybrid)"
   ```

改 `switchboard.toml` 或 `.env` 后执行 `docker compose up -d --force-recreate switchboard`（Switchboard 不热加载）。改 `Caddyfile` 后执行 `docker compose restart caddy`。

## 本机 Hibernaut 配对

本机（如 Mac）的 `hibernaut.toml`：

```toml
[switchboard]
enabled = true
mode = "relay"
relay_url = "wss://sb.example.com/ws/relay"
mcp_url = "https://sb.example.com/mcp"
consumer_id = "hibernaut"
```

Hibernaut 的运行环境里设置两个变量（缺任一个启动失败）：

```bash
export SWITCHBOARD_RELAY_TOKEN=<VPS .env 里的 SWITCHBOARD_RELAY_WEBHOOK_SECRET>
export SWITCHBOARD_MCP_TOKEN=<VPS .env 里的 SWITCHBOARD_MCP_WEBHOOK_SECRET>
```

`[switchboard]` 段改动需要重启 Hibernaut。`consumer_id` 必须与 VPS 上 `[routing]` 投递的消费方一致（默认 `hibernaut`），同一个 `consumer_id` 同时只能有一个连接，后连上的会顶掉前一个（Close 4009）。

## 冒烟验收

在本机执行，`sb.example.com` 换成你的域名。

1. TLS 与健康检查：

   ```bash
   curl -fsS https://sb.example.com/healthz
   # {"status": "ok"}
   ```

2. WebSocket 透传（Caddy → `/ws/relay`）：

   ```bash
   curl -sS -o /dev/null -w '%{http_code}\n' --http1.1 --max-time 5 \
     -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
     -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
     https://sb.example.com/ws/relay
   # 101（握手成功后连接被 --max-time 截断，curl 退出码 28 属正常）
   ```

3. Relay 鉴权：用一个不会收到事件的 `consumer_id` 外连，约 10 秒内没有任何输出即通过，Ctrl+C 退出；故意填错令牌应打印 `relay rejected authentication`。

   ```bash
   SWITCHBOARD_RELAY_TOKEN=<relay 令牌> PYTHONPATH=src \
     python3 -m switchboard.relay_agent --url wss://sb.example.com/ws/relay --consumer-id smoke-test
   ```

   不要用 `--consumer-id hibernaut`，否则会顶掉正在运行的 Hibernaut。

4. MCP 鉴权：

   ```bash
   curl -sS https://sb.example.com/mcp \
     -H "Authorization: Bearer <mcp 令牌>" -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
   # 返回 submit_work、post_message、decide_approval、report_execution、replay_delivery
   ```

   不带或带错令牌应返回 401。

5. 端口检查：从外网 `curl -m 5 http://<VPS IP>:8790/healthz` 应连接失败。

6. 与 Hibernaut 联调（灰度切流第 2 步后）：有事件进来后查看投递状态，`hibernaut` 行应为 `acked`：

   ```bash
   docker compose exec switchboard python3 -c "import sqlite3; print(sqlite3.connect('/data/switchboard.db').execute('SELECT consumer_id, status, count(*) FROM delivery_log GROUP BY 1, 2').fetchall())"
   ```

## 灰度切流

Hibernaut 现有的 GitHub / 飞书 / MCP 通道在 Stage B 仍然保留，可以逐个入口切换，每步都能单独回退。

1. 部署 VPS 并完成冒烟验收 1–5。此时还没有外部事件发到 VPS。
2. 本机 Hibernaut 改为 `mode = "relay"`（见上文）并重启。确认启动日志无报错、没有明文告警；用冒烟第 6 步确认 `delivery_log` 中 `hibernaut` 的投递为 `acked`、没有积压的 `pending` / `retry`。
3. 切一个入口：先在一个仓库的 GitHub Webhook 里把 Payload URL 改为 `https://sb.example.com/webhooks/github`（Content type `application/json`，Secret 用 `SWITCHBOARD_GITHUB_WEBHOOK_SECRET`），在 Recent Deliveries 确认 2xx，再发一个带 `hibernaut` 标签的测试 Issue 确认 Hibernaut 收到并回写评论（回写的评论末尾有隐藏标记 `<!-- switchboard:outbound -->`，回流时被丢弃，不会再触发 Run）。同时把该项目 `hibernaut.toml` 的 `poll_labels` 改为 `[]`：轮询和 Webhook 的 `external_id` 不同，两路都开时同一个 Issue 会跑两次。
4. 观察一段时间（建议至少一天）没有 `dead_letter` 后，再切飞书事件订阅地址到 `https://sb.example.com/webhooks/feishu`，以及其余仓库。

## 回滚

- **单个入口**：把 GitHub Webhook / 飞书事件订阅地址改回原来的 Hibernaut 地址。
- **Hibernaut 侧**：`hibernaut.toml` 的 `[switchboard]` 改回原来的 `mode = "in_process"` 或 `enabled = false`，重启 Hibernaut。切换前先确认 VPS 上没有发给 `hibernaut` 的 `pending` 投递，否则这些事件要等重新连上才会推送。
- **VPS 版本**：升级前先备份数据卷，回滚时检出上一个版本重新构建：

  ```bash
  docker compose stop switchboard
  docker run --rm -v switchboard_switchboard_data:/data -v "$PWD":/backup python:3.12-slim \
    tar czf /backup/switchboard-data-$(date +%Y%m%d%H%M).tgz -C /data .
  git checkout <上一个可用的 commit>
  docker compose up -d --build switchboard
  ```

  新版本改过表结构时，回滚代码要连同数据卷一起恢复到升级前的备份。
- **整体停用**：`docker compose down`（保留数据卷）；`docker compose down -v` 会删除数据库与 Caddy 证书，谨慎使用。
