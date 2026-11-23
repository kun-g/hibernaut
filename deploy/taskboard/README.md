# Switchboard + Taskboard 同域部署（容器）

把 Taskboard 与 Switchboard 部署在同一个 compose 栈里，对外共用一个域名（下文以占位域名 `hub.example.com` 为例），按子路径区分：

| 公网路径 | 转发到 | 用途 |
| :--- | :--- | :--- |
| `/switchboard/webhooks/github` | `switchboard:8790` | GitHub Webhook |
| `/switchboard/webhooks/feishu` | `switchboard:8790` | 飞书事件订阅与卡片回调 |
| `/switchboard/mcp` | `switchboard:8790` | Hibernaut 上报运行结果（`SWITCHBOARD_MCP_WEBHOOK_SECRET`） |
| `/switchboard/ws/relay` | `switchboard:8790` | 本机 Hibernaut 的 Relay 长连接 |
| `/taskboard/mcp` | `taskboard:8791` | `tb_*` MCP 工具（`Authorization: Bearer <TASKBOARD_MCP_TOKEN>`） |
| `/switchboard/healthz`、`/taskboard/healthz` | 各自服务 | 健康检查 |

- 两个服务都按 `[http] base_path`（`/switchboard`、`/taskboard`）直接处理带前缀的路径，反向代理保留前缀转发即可，不需要 StripPrefix 中间件。不带前缀的路径同样可用，所以旧域名在迁移期间继续有效。
- Taskboard 在 compose 内网直连 `ws://switchboard:8790/ws/relay` 与 `http://switchboard:8790/mcp`，不经过公网。启动日志里这两个地址的 `sends tokens in plaintext` 告警因此属预期。
- `taskboard` 镜像只含 `src/taskboard`、`src/switchboard`、`schemas/taskboard`（纯标准库，`python:3.12-slim`，UID 10001 的非 root 用户 `taskboard`），SQLite 在命名卷 `taskboard_data`（容器内 `/data`），配置挂到 `/etc/taskboard/taskboard.toml`。

| 文件 | 用途 |
| :--- | :--- |
| `Dockerfile`、`Dockerfile.dockerignore` | Taskboard 镜像，构建上下文是仓库根，只送上面三个目录 |
| `taskboard.docker.toml` | 容器配置模板，复制为 `taskboard.toml` |
| `.env.example` | Taskboard 的三个令牌，复制为 `.env` |
| `docker-compose.yml` | 标准 Compose 的联合栈（`switchboard` + `taskboard`，可选 `caddy` profile） |
| `Caddyfile.example` | 可选 Caddy 的同域子路径配置，复制为 `Caddyfile` |
| [`scripts/prepare_taskboard_deploy.py`](../../scripts/prepare_taskboard_deploy.py) | `render`：校验配置、生成令牌、算配置哈希；`smoke`：对线上域名做冒烟验收 |

仓库只保存模板；`.env`、`taskboard.toml`、`Caddyfile` 是实例文件，已在 `.gitignore` 中忽略。

## 前提

- Switchboard 已按 [`deploy/switchboard/README.md`](../switchboard/README.md) 部署过，或本次一起部署。
- 本机 Hibernaut 是能处理 `work.ready` 并回报 `task_action` 的版本（见 `docs/architecture.md` §10.2）；切流前必须满足。
- `hub.example.com` 的 A/AAAA 记录已指向服务器并生效，再去绑定域名（见下文「证书坑」）。

## 1. 准备配置（render）

```bash
cp deploy/taskboard/taskboard.docker.toml deploy/taskboard/taskboard.toml
```

在现有的 `switchboard.toml` 里加上 `base_path`，切流前先保留 `[routing.event_consumers]`（事件仍直连 Hibernaut，Taskboard 部署好了也收不到事件）：

```toml
[http]
host = "0.0.0.0"
port = 8790
base_path = "/switchboard"
```

`[outbound]` 要有 `notify_target`（或 `approval_target`），否则 Taskboard 发给人的 `human_todo` 通知没有去处。然后运行：

```bash
mkdir -p ~/hub && chmod 700 ~/hub
python3 scripts/prepare_taskboard_deploy.py render \
  --template deploy/taskboard/taskboard.toml \
  --switchboard-toml deploy/switchboard/switchboard.toml \
  --token-file ~/hub/taskboard.env \
  --taskboard-ref <commit> --switchboard-ref <commit>
```

- 生成高熵的 `TASKBOARD_MCP_TOKEN`，以 `TASKBOARD_MCP_TOKEN=<令牌>` 一行写入 `--token-file`（权限 0600）。目标文件已存在、或位于仓库里未被 git 忽略的路径时拒绝写入。输出里令牌默认打码，`--show-token` 才打印明文；不给 `--token-file` 也不给 `--show-token` 时直接报错，免得令牌生成了却没人拿到。只想重算哈希时用 `--no-token`。
- 打印要填进 Dokploy Environment（或 `deploy/taskboard/.env`）的变量：`TASKBOARD_MCP_TOKEN`、`TASKBOARD_REF`、`TASKBOARD_CONFIG_SHA`、`SWITCHBOARD_REF`、`SWITCHBOARD_CONFIG_SHA`。配置哈希是 toml 文件 SHA-256 的前 12 位。
- 检查项：`[ok]` 通过；`[fail]` 必须修（`base_path` 不对、Relay 未开启或单开了端口），此时退出码为 1；`[todo] switchboard routing ...` 表示还没切流，切流前属正常；`[warn]` 提示风险。

之后每次改动某个 toml，重新 `render --no-token`，只更新那一个服务的 `*_CONFIG_SHA`。

## 2a. Dokploy / Traefik 同域子路径部署

1. 在现有 Switchboard 的 Dokploy Compose 服务里改 compose（沿用同一个服务，Switchboard 的数据卷和项目名不变），类型 raw compose：

   ```yaml
   services:
     switchboard:
       build:
         context: https://github.com/kun-g/hibernaut.git#${SWITCHBOARD_REF}
         dockerfile: deploy/switchboard/Dockerfile
       image: hibernaut-switchboard:${SWITCHBOARD_REF}
       restart: unless-stopped
       stop_grace_period: 20s
       labels: ["switchboard.config-sha=${SWITCHBOARD_CONFIG_SHA}"]
       environment:
         SWITCHBOARD_RELAY_WEBHOOK_SECRET: ${SWITCHBOARD_RELAY_WEBHOOK_SECRET}
         SWITCHBOARD_MCP_WEBHOOK_SECRET: ${SWITCHBOARD_MCP_WEBHOOK_SECRET}
         SWITCHBOARD_GITHUB_WEBHOOK_SECRET: ${SWITCHBOARD_GITHUB_WEBHOOK_SECRET}
         SWITCHBOARD_GITHUB_TOKEN: ${SWITCHBOARD_GITHUB_TOKEN:-}
         SWITCHBOARD_GITHUB_APP_ID: ${SWITCHBOARD_GITHUB_APP_ID:-}
         SWITCHBOARD_GITHUB_APP_PRIVATE_KEY: ${SWITCHBOARD_GITHUB_APP_PRIVATE_KEY:-}
         SWITCHBOARD_FEISHU_WEBHOOK_SECRET: ${SWITCHBOARD_FEISHU_WEBHOOK_SECRET}
         SWITCHBOARD_FEISHU_APP_ID: ${SWITCHBOARD_FEISHU_APP_ID:-}
         SWITCHBOARD_FEISHU_APP_SECRET: ${SWITCHBOARD_FEISHU_APP_SECRET:-}
       configs:
         - source: switchboard_toml
           target: /etc/switchboard/switchboard.toml
       volumes:
         - switchboard_data:/data

     taskboard:
       build:
         context: https://github.com/kun-g/hibernaut.git#${TASKBOARD_REF}
         dockerfile: deploy/taskboard/Dockerfile
       image: hibernaut-taskboard:${TASKBOARD_REF}
       restart: unless-stopped
       labels: ["taskboard.config-sha=${TASKBOARD_CONFIG_SHA}"]
       environment:
         # Taskboard 连 Switchboard 用的就是 Switchboard 自己的两个令牌
         SWITCHBOARD_RELAY_TOKEN: ${SWITCHBOARD_RELAY_WEBHOOK_SECRET}
         SWITCHBOARD_MCP_TOKEN: ${SWITCHBOARD_MCP_WEBHOOK_SECRET}
         TASKBOARD_MCP_TOKEN: ${TASKBOARD_MCP_TOKEN}
       depends_on:
         switchboard:
           condition: service_healthy
       configs:
         - source: taskboard_toml
           target: /etc/taskboard/taskboard.toml
       volumes:
         - taskboard_data:/data

   configs:
     switchboard_toml:
       content: |
         # 粘贴 switchboard.toml 全文，整体缩进到这一层
     taskboard_toml:
       content: |
         # 粘贴 taskboard.toml 全文，整体缩进到这一层

   volumes:
     switchboard_data:
     taskboard_data:
   ```

   - 两个服务各用自己的 `*_REF` 与 `*_CONFIG_SHA`。只改 `TASKBOARD_REF` 或 `TASKBOARD_CONFIG_SHA` 再部署时，Switchboard 的镜像命中构建缓存、label 不变，容器不会重建；反过来也一样。构建缓存被清理过时，同一 commit 重建出的镜像 ID 会变，Switchboard 也会跟着重建一次（数据在卷里，不丢）。
   - `labels` 必须写成列表。写成映射时 Dokploy 不再注入它的 Traefik label，两个域名都会返回 404。
   - `configs.content` 的变化 compose 察觉不到，所以靠 `*_CONFIG_SHA` label 触发重建。内容里的 `$` 要写成 `$$`。
   - 不写 `ports:`。

2. Environment 里填 Switchboard 原有的变量（见 `deploy/switchboard/README.md`），再加 `render` 打印的 `TASKBOARD_MCP_TOKEN`（令牌文件里的明文）、`TASKBOARD_REF`、`TASKBOARD_CONFIG_SHA`、`SWITCHBOARD_REF`、`SWITCHBOARD_CONFIG_SHA`。
3. Domains 里在同一个 Host 上加两条：

   | Service Name | Host | Path | Container Port | Strip Path |
   | :--- | :--- | :--- | :--- | :--- |
   | `switchboard` | `hub.example.com` | `/switchboard` | `8790` | 关 |
   | `taskboard` | `hub.example.com` | `/taskboard` | `8791` | 关 |

   两条都打开 HTTPS，证书选 Let's Encrypt。Traefik 按前缀转发并保留前缀，由 `base_path` 处理，不需要手写 StripPrefix middleware。旧域名（如 `sb.example.com` → `switchboard:8790`）先保留，见第 5 节。
4. Deploy，然后按第 3 节冒烟。

**证书坑**：Traefik 只在路由首次加载时申请证书。DNS 晚于绑定域名才生效时，证书申请失败并一直用默认自签证书；DNS 生效后改一下域名配置（比如把 HTTPS 关掉再打开）并重新部署才会重新申请。所以先建 DNS，再在 Dokploy 绑域名。

## 2b. 标准 Compose（自带 Caddy）

主机的 80/443 没被别的反向代理占用时，在服务器的检出里：

```bash
cd deploy/taskboard
cp Caddyfile.example Caddyfile        # 把 hub.example.com 换成你的域名
cp .env.example .env && chmod 600 .env
```

- `.env`：`SWITCHBOARD_RELAY_TOKEN` 与 `SWITCHBOARD_MCP_TOKEN` 分别填 `../switchboard/.env` 里的 `SWITCHBOARD_RELAY_WEBHOOK_SECRET` 与 `SWITCHBOARD_MCP_WEBHOOK_SECRET`；`TASKBOARD_MCP_TOKEN` 填令牌文件里的值；再取消注释并填上 `render` 打印的 `*_REF` 与 `*_CONFIG_SHA`。
- Switchboard 仍读 `../switchboard/.env` 与 `../switchboard/switchboard.toml`。
- 两个服务都从 GitHub 上对应的 commit 构建（`HIBERNAUT_GIT_URL` 可改成别的仓库地址），不用本地检出里的代码。

先停掉 `deploy/switchboard` 的旧栈（不加 `-v`，保留数据卷；本栈的 `switchboard_data` 就是那个卷 `switchboard_switchboard_data`），再启动：

```bash
(cd ../switchboard && docker compose down)
docker compose --profile caddy up -d --build
docker compose ps    # switchboard、taskboard 都 healthy 后 caddy 才启动
```

已有反向代理时去掉 `--profile caddy`，由它把两段子路径转发到 `switchboard:8790` 与 `taskboard:8791`（保留前缀）。

**按需更新单个服务**：改 `.env` 里一个服务的 `*_REF` 或 `*_CONFIG_SHA` 后，只更新它：

```bash
docker compose up -d --build --no-deps taskboard
docker compose up -d --build --no-deps switchboard
```

`--no-deps` 不碰依赖的服务。改 `Caddyfile` 后执行 `docker compose restart caddy`。

**Caddy 子路径**：`Caddyfile.example` 用 `handle /switchboard/*` 与 `handle /taskboard/*` 分别 `reverse_proxy` 到两个服务。`handle` 保留前缀，与 `base_path` 配合；换成 `handle_path`（剥离前缀）也能用，因为两个服务都接受裸路径。WebSocket 升级由 `reverse_proxy` 直接透传。

## 3. 冒烟验收

在本机执行（令牌只从环境变量读，不出现在命令行参数里）：

```bash
set -a; . ~/hub/taskboard.env; set +a
python3 scripts/prepare_taskboard_deploy.py smoke --base-url https://hub.example.com
```

依次检查：

1. `GET /switchboard/healthz` 返回 `200 {"status": "ok"}`；
2. `GET /taskboard/healthz` 返回 `200 {"status": "ok"}`；
3. 不带令牌 `POST /taskboard/mcp` 返回 `401`（fail-closed）；
4. 带 Bearer 令牌 `initialize` 成功，`tools/list` 列出全部 7 个 `tb_*` 工具。

全部通过退出码 0；任一项失败打印 `[fail]` 及原因（状态码、响应片段或连接错误），退出码 1。子路径不同时用 `--switchboard-prefix`、`--taskboard-prefix`，令牌放在别的变量里时用 `--token-env`。

通过后把本机 `hibernaut.toml` 指到新地址并重启 Hibernaut：

```toml
[switchboard]
enabled = true
mode = "relay"
relay_url = "wss://hub.example.com/switchboard/ws/relay"
mcp_url = "https://hub.example.com/switchboard/mcp"
consumer_id = "hibernaut"
```

再按 `deploy/switchboard/README.md`「冒烟验收」第 3、4 步，把其中的地址换成 `https://hub.example.com/switchboard/...`，确认 Relay 与 Switchboard `/mcp` 鉴权正常。

## 4. 路由切流与一键回滚

切流前 `switchboard.toml` 的 `[routing.event_consumers]` 把 `work.*` 直接投给 Hibernaut、`execution.reported` 只给出站网关，Taskboard 收不到事件。

**切流**（冒烟全绿、Hibernaut 已连上新地址后）：

1. 删掉 `switchboard.toml` 里的整个 `[routing.event_consumers]` 表，回到默认路由：`work.opened` / `work.updated` 给 `taskboard`，`work.closed` 给 `taskboard` 与 `hibernaut`，`work.ready` 给 `hibernaut`，`execution.reported` 给 `taskboard` 与出站网关。
2. `render --no-token --switchboard-toml ...`，确认出现 `[ok] switchboard routing: work.* and execution.reported go through Taskboard`，把新的 `SWITCHBOARD_CONFIG_SHA` 填进 Environment（或 `.env`）后重新部署，只有 Switchboard 重建。
3. 给一个测试 Issue 加上 `[github] require_label` 指定的标签，确认：Taskboard 建单并变为 `ready`，Hibernaut 收到 `work.ready` 并执行，任务最终为 `done`（`tb_list_tasks`）；`delivery_log` 中 `taskboard` 与 `hibernaut` 的投递均为 `acked`：

   ```bash
   docker compose exec switchboard python3 -c "import sqlite3; print(sqlite3.connect('/data/switchboard.db').execute('SELECT consumer_id, status, count(*) FROM delivery_log GROUP BY 1, 2').fetchall())"
   ```

**回滚**（出问题时立即执行）：把下面的表加回 `switchboard.toml`，`render` 出新的 `SWITCHBOARD_CONFIG_SHA` 后重新部署 Switchboard：

```toml
[routing.event_consumers]
"work.opened" = ["hibernaut"]
"work.updated" = ["hibernaut"]
"work.closed" = ["hibernaut"]
"execution.reported" = ["switchboard.outbound"]
```

新事件立即直连 Hibernaut。已经投给 `taskboard` 的事件仍按队列送达（Taskboard 不用停）；回滚前已派发、回滚后才执行完的任务，结果不再回到 Taskboard，会停在 `in_progress`，需要时用 `tb_transition_task` 手动结案。

## 5. 旧域名平滑下线

1. 迁移期间保留旧域名绑定（Dokploy 里 `sb.example.com` → `switchboard:8790`，Path `/`；或 Caddy 里旧站点块）。设置 `base_path` 后裸路径仍然可用，新旧两个地址并行有效。
2. 逐个把入口切到新地址，每切一个就验收一个：
   - GitHub App（或仓库）Webhook URL 改为 `https://hub.example.com/switchboard/webhooks/github`，在 Recent Deliveries 确认 2xx；
   - 飞书事件订阅与卡片回调地址改为 `https://hub.example.com/switchboard/webhooks/feishu`，通过飞书的 URL 校验，发一条测试消息确认入站；
   - 本机 `hibernaut.toml` 的 `relay_url` / `mcp_url` 改为第 3 节的新地址并重启，确认启动日志无报错、`delivery_log` 中 `hibernaut` 为 `acked`。
3. 全部切完后观察一段时间（建议至少一天），在 Traefik / Caddy 的访问日志里确认旧域名已没有请求。
4. 先在 Dokploy 删除旧域名绑定（或删掉 Caddy 旧站点块并重启 caddy），再删除旧域名的 DNS 记录。

下线前任何一步出问题，把对应入口改回旧域名即可，旧绑定一直有效。

## 数据与停用

- 升级前备份数据卷（卷名以 `docker volume ls` 为准）：

  ```bash
  docker run --rm -v <taskboard 数据卷>:/data -v "$PWD":/backup python:3.12-slim \
    tar czf /backup/taskboard-data-$(date +%Y%m%d%H%M).tgz -C /data .
  ```

- 回滚版本：把 `TASKBOARD_REF` 改回上一个可用的 commit 再部署；新版本改过表结构时，连同数据卷一起恢复到升级前的备份。
- `docker compose down` 保留数据卷；`docker compose down -v` 会删除两个服务的数据库与 Caddy 证书，谨慎使用。
