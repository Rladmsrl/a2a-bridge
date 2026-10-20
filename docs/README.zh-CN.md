# A2A Bridge

把外部 **A2A（Agent2Agent）** 智能体接入 Grok Bot（或任何支持 webhook 的 agent）的桥接服务。

```
外部 A2A Agent ──HTTPS JSON-RPC / SSE──▶  A2A Bridge  ──Webhook POST(短连接)──▶  Grok Bot
                ◀── tasks/get / SSE 推送 ──           ◀──Callback POST(短连接)───
                                         (SQLite 保存任务状态)
```

- 协议：A2A JSON-RPC 2.0 over HTTP，**同时兼容 v0.3**（`message/send`、`message/stream`、`tasks/get`、`tasks/cancel`、`tasks/resubscribe`、`tasks/pushNotificationConfig/*`）与 **v1.0**（`SendMessage`、`SendStreamingMessage`、`GetTask`、`ListTasks`、`CancelTask`、`SubscribeToTask`、`*TaskPushNotificationConfig`）。按方法名自动判断版本，响应格式随之切换。已用官方 `a2a-sdk` 1.2.2 客户端验证互通。
- Agent Card：`/.well-known/agent-card.json`（以及旧版 `/.well-known/agent.json`）返回默认 agent；每个 agent 还有 `/agents/<name>/.well-known/agent-card.json`。
- 支持 text / file（bytes 或 uri）/ data 三种 part；`contextId` 多轮；状态 submitted / working / input-required / auth-required / completed / failed / canceled / rejected。
- SSE 流式（心跳注释 `: keepalive`）、断线后 `tasks/resubscribe`、`tasks/get` 轮询兜底、可选推送通知（push notification）。
- 安全：入站 Bearer API Key（多把，仅存 SHA-256，可限定可访问的 agent）；回调使用**每任务回调令牌**（由每个 bot 的共享密钥 HMAC 派生）或 **HMAC 签名**；出站 webhook 带每个 bot 独立的 Bearer token + HMAC 签名。
- 运维：SQLite（WAL）持久化、重启不丢任务；webhook 投递走持久化 outbox，指数退避重试；卡死任务超时；请求体大小限制；按 IP / API Key 限流；JSON 结构化日志；`/healthz`。
- Roster：`GET /roster`、`GET /roster/{agent}`、`GET /roster/stream`（SSE）。状态由任务/投递记录推断，不探测 bot webhook。API Key 范围隔离；`A2A_KEY_TIERS` 控制 basic/trusted/owner 可见字段。

## 目录

```
a2a_bridge/            服务代码
  app.py               FastAPI 路由：JSON-RPC、Agent Card、回调、健康检查、roster、限流/大小限制
  service.py           任务逻辑、webhook outbox（重试退避）、超时清理、推送通知
  roster.py            /roster 状态推断与按 key 分级投影（不探测 bot）
  models.py            A2A 数据模型，v0.3 <-> v1.0 转换，错误码
  db.py                SQLite 存储
  security.py          API Key、回调令牌、HMAC 签名、限流、推送 URL 防 SSRF
  instructions.py      发给 bot 的纯文本回复说明（含 curl 示例）
  config.py            环境变量 + agents.json 配置
config/agents.example.json   agent 路由/Agent Card 配置示例
scripts/a2a_cli.py     命令行测试客户端
scripts/mock_bot.py    模拟 bot（测试用）
scripts/sdk_example.py 官方 a2a-sdk 客户端示例
scripts/gen_secret.py  生成 API Key / 密钥
scripts/install.sh     一键部署
scripts/e2e_local.sh   本地端到端演示
deploy/                systemd unit、Caddyfile、nginx 配置
docs/CONNECT.md        外部 agent 接入指南
docs/BOT-SIDE.md       接收端 Grok Bot 的 webhook / callback 协议（可直接作为 skill 文档）
docs/roster-design.md  `/roster` 设计与实现说明
tests/                 pytest
Dockerfile, docker-compose.yml, .env.example
```

## 本地运行与测试

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest            # 全部测试
./scripts/e2e_local.sh                # 真进程：bridge + mock bot + CLI + 官方 SDK
```

## 配置

### 1. agents.json（路由 + Agent Card）

`cp config/agents.example.json config/agents.json` 后编辑。每个 agent：

| 字段 | 说明 |
|---|---|
| `name` | 路由名，URL 为 `https://<host>/agents/<name>` |
| `display_name` / `description` / `version` / `provider` / `documentation_url` | 显示在 Agent Card |
| `webhook_url` 或 `webhook_url_env` | bot 的 webhook 地址（可放环境变量里） |
| `webhook_token_env` | **环境变量名**，其值为调用 bot webhook 时带的 Bearer token |
| `shared_secret_env` | **环境变量名**，其值（≥32 字符）是 bridge 与该 bot 的共享密钥：派生回调令牌、签名 webhook、校验签名回调 |
| `skills` | A2A skill 列表（id/name/description/tags/examples/inputModes/outputModes） |
| `required_inputs` | 期望输入字段（如 repo_url / goal / acceptance_criteria），写入 card 描述、skill 描述和扩展 `urn:a2a-bridge:input-fields:v1`，也随 webhook 发给 bot |
| `max_concurrent` | 可选，默认 1。roster 用来算满载 / queued |
| `stuck_after_s` | 可选，覆盖全局 `A2A_ROSTER_STUCK_AFTER_S`，必须小于 `A2A_TASK_TIMEOUT_S` |
| `roster_visible` | 可选，默认 true。false 时非 owner 的 roster 不展示该 agent |
| `input_modes` / `output_modes` | 媒体类型 |
| `webhook_auth_header` / `webhook_auth_scheme` | 出站认证头，默认 `Authorization` + `Bearer`；scheme 设为空串则直接发送 token |

agents.json 不含任何密钥，只引用环境变量名。

### 2. 环境变量（见 `.env.example`）

**必须设置：**

| 变量 | 说明 |
|---|---|
| `A2A_PUBLIC_BASE_URL` | 对外 HTTPS 地址，如 `https://a2a.example.com`（用于 card URL 和回调 URL） |
| `A2A_API_KEYS` | 入站 API Key，逗号分隔：`label=sha256:<hex>[@agent1\|agent2]`（推荐）或 `label=plain:<key>`（开发用） |
| `A2A_AGENTS_CONFIG` | agents.json 路径（Docker 中为 `/app/config/agents.json`） |
| 每个 bot 的 `*_WEBHOOK_TOKEN`、`*_SHARED_SECRET`（以及用 `webhook_url_env` 时的 `*_WEBHOOK_URL`） | 名字由 agents.json 决定 |

**可选（默认值）：** `A2A_DEFAULT_AGENT`（第一个）、`A2A_DB_PATH`（data/bridge.db）、`A2A_HOST`(0.0.0.0)、`A2A_PORT`(8080)、`A2A_LOG_LEVEL`(INFO)、`A2A_MAX_REQUEST_BYTES`(10MB)、`A2A_RATE_LIMIT_PER_MIN`(120)、`A2A_RATE_LIMIT_BURST`(60)、`A2A_TASK_TIMEOUT_S`(3600，submitted/working 无更新则 failed；`waitingOn=owner` 除外）、`A2A_INPUT_REQUIRED_TIMEOUT_S`(86400，等待输入或 owner 审批超时则 canceled)、`A2A_RETENTION_DAYS`(30，终态任务清理)、`A2A_WEBHOOK_TIMEOUT_S`(15)、`A2A_WEBHOOK_MAX_ATTEMPTS`(8)、`A2A_WEBHOOK_BACKOFF_BASE_S`(2)、`A2A_WEBHOOK_BACKOFF_MAX_S`(300)、`A2A_WEBHOOK_4XX_RETRIES`(1，401/403/404 以外的 4xx 额外重试次数)、`A2A_WEBHOOK_4XX_RETRY_DELAY_S`(10)、`A2A_SSE_HEARTBEAT_S`(15)、`A2A_BLOCKING_WAIT_S`(30，阻塞式 send 最长等待)、`A2A_CALLBACK_MAX_SKEW_S`(300)、`A2A_PUSH_ALLOW_HTTP`(false)、`A2A_PUSH_ALLOW_PRIVATE`(false)、`A2A_TRUST_PROXY_HEADERS`(true，仅在反向代理后使用)、`A2A_SWEEP_INTERVAL_S`(30)、`A2A_KEY_TIERS`（`label:basic\|trusted\|owner`，未列出默认 basic；**不改** `A2A_API_KEYS` 语法；owner 仍受 `@agent` 范围限制）、`A2A_ROSTER_STUCK_AFTER_S`(900)、`A2A_ROSTER_OFFLINE_AFTER_S`(120)、`A2A_ROSTER_OFFLINE_TTL_S`(3600，401/403/404 永久失败导致的 offline 锁过期)、`A2A_ROSTER_MAX_STREAMS_PER_KEY`(3)、`A2A_ROSTER_MAX_STREAMS`(32)、`A2A_ROSTER_MAX_ANON_STREAMS`(3)、`A2A_ROSTER_STREAM_TIMEOUT_S`(30)、`A2A_ROSTER_STREAM_MAX_S`(300)、`A2A_ROSTER_PUBLIC`(false)、`A2A_ROSTER_REDACT`（逗号分隔）、`A2A_ROSTER_DEBOUNCE_S`(1)、`A2A_ROSTER_DEBOUNCE_MAX_S`(2)。agent 名 `stream` 保留给 `GET /roster/stream`。roster 接口见 [CONNECT.md](CONNECT.md) 第 9 节、设计见 [roster-design.md](roster-design.md)。部署前请备份 SQLite。

生成密钥：

```bash
python3 scripts/gen_secret.py apikey partner1          # 输出给对方的 key + A2A_API_KEYS 条目（只存哈希）
python3 scripts/gen_secret.py apikey partner2 coder    # 只允许访问 coder
python3 scripts/gen_secret.py secret                   # 生成 webhook token / 共享密钥
```

## 部署（香港 Linux 服务器）

前提：域名 A 记录指向服务器；防火墙只开放 80/443（8080 只监听 127.0.0.1）。

### 一键安装

```bash
BOT_WEBHOOK_TOKEN='<bot 发送密钥>' ./scripts/install.sh --domain a2a.example.com --webhook-url 'https://<bot webhook>'
```
自动生成密钥与 API Key（`api-key.txt`）、写入 `.env` 与 `config/agents.json`，并启动 bridge + Caddy。

### 方案 A：Docker Compose（手动）

```bash
git clone <repo> /opt/a2a-bridge && cd /opt/a2a-bridge   # 或 scp 拷贝目录
cp .env.example .env && chmod 600 .env && vi .env          # 填写变量
cp config/agents.example.json config/agents.json && vi config/agents.json
docker compose up -d --build
curl -s http://127.0.0.1:8080/healthz
# HTTPS：在 .env 中设置 A2A_DOMAIN=<域名>（Caddyfile 读取该变量），然后
docker compose --profile caddy up -d
# 或使用宿主机 nginx：deploy/nginx.conf + certbot
curl -s https://a2a.example.com/.well-known/agent-card.json
docker compose logs -f a2a-bridge
```

数据在 volume `a2a-data`（`/app/data/bridge.db`），备份：`docker compose exec a2a-bridge python -c "import sqlite3;s=sqlite3.connect('/app/data/bridge.db');s.backup(sqlite3.connect('/app/data/backup.db'))"`。

### 方案 B：systemd

```bash
sudo useradd -r -s /usr/sbin/nologin a2a
sudo mkdir -p /opt/a2a-bridge /etc/a2a-bridge /var/lib/a2a-bridge
sudo cp -r a2a_bridge scripts config requirements.txt /opt/a2a-bridge/
sudo python3 -m venv /opt/a2a-bridge/.venv && sudo /opt/a2a-bridge/.venv/bin/pip install -r /opt/a2a-bridge/requirements.txt
sudo cp .env.example /etc/a2a-bridge/env && sudo vi /etc/a2a-bridge/env
#   设置 A2A_DB_PATH=/var/lib/a2a-bridge/bridge.db  A2A_AGENTS_CONFIG=/opt/a2a-bridge/config/agents.json  A2A_HOST=127.0.0.1
sudo chown -R a2a:a2a /var/lib/a2a-bridge && sudo chmod 600 /etc/a2a-bridge/env && sudo chown root:a2a /etc/a2a-bridge/env && sudo chmod 640 /etc/a2a-bridge/env
sudo cp deploy/a2a-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now a2a-bridge
journalctl -u a2a-bridge -f
```

然后配置 Caddy（`deploy/Caddyfile` 放到 `/etc/caddy/Caddyfile`）或 nginx（`deploy/nginx.conf`）。

> 只能运行**单个进程/单 worker**（SSE 事件总线和限流在进程内）。需要横向扩展时需改为 Redis 等共享总线。

## 注册一个新 bot

1. 生成两个密钥：`gen_secret.py secret`（webhook token）和 `gen_secret.py secret`（共享密钥）。
2. 在 `.env` 中加 `BOT_X_WEBHOOK_URL`、`BOT_X_WEBHOOK_TOKEN`、`BOT_X_SHARED_SECRET`。
3. 在 `agents.json` 加一个条目（name、描述、skills、required_inputs，引用以上变量名）。
4. 重启服务；新 agent card：`https://<host>/agents/<name>/.well-known/agent-card.json`。
5. 把 webhook token 和共享密钥安全地交给 bot 端；bot 按 [BOT-SIDE.md](BOT-SIDE.md) 实现：校验 Bearer + `X-A2A-Signature`，用 payload 里的 `callback.url` + `callback.token` 回调。
6. 给外部调用方发 API Key（`gen_secret.py apikey <label> <name>`），并给他 [CONNECT.md](CONNECT.md)。

## Webhook / Callback 协议摘要

详见 [BOT-SIDE.md](BOT-SIDE.md)。

- Bridge → Bot：`POST <webhook_url>`，头 `Authorization: Bearer <webhook token>`、`X-A2A-Signature: t=<unix>,v1=<hex HMAC-SHA256(shared_secret, "<t>." + body)>`、`X-A2A-Event`、`X-A2A-Delivery`。事件：`task.created`、`task.message`（追问的回答/后续消息）、`task.canceled`。返回 2xx 即视为送达（之后任务进入 working）；非 2xx/超时会指数退避重试，401/403/404 立即视为永久失败；其他 4xx（除 408/409/425/429 按普通重试）在短暂延迟后再重试 `A2A_WEBHOOK_4XX_RETRIES` 次后才失败；响应体含 rate limit / too many requests 的 4xx（Grok Bot 平台限流时返回 400）按限流处理，走正常指数退避重试并遵守 Retry-After，任务标记 failed。
- Bot → Bridge：`POST <callback.url>`，头 `Authorization: Bearer <callback.token>`（或用共享密钥的 `X-A2A-Signature`），body `{"state","message","artifacts","eventId"}`。

## 安全说明

- 仓库中没有任何密钥；`.env`、`config/agents.json` 已在 `.gitignore`。`.env` 权限设为 600。
- 入站 API Key 只保存 SHA-256；比较是常量时间。泄露后从 `A2A_API_KEYS` 删除条目并重启即吊销。可用 `@agent` 限制范围。不同 API Key 的任务互相不可见（返回 TaskNotFound，不泄露存在性）。
- 回调令牌是 `HMAC(shared_secret, task_id)`，只对单个任务有效；API Key 不能用于回调。签名回调有 ±300 秒时间窗。
- Bot 应校验 webhook 的 Bearer 和签名，并把消息内容当作**不可信输入**（防提示注入）。
- 推送通知 URL 默认必须 https 且不能解析到内网/回环地址（防 SSRF）。
- 仅通过 HTTPS 暴露；8080 只绑定 127.0.0.1。`A2A_TRUST_PROXY_HEADERS=true` 只能在反代后使用（nginx 配置中覆盖 X-Forwarded-For），否则限流可被伪造绕过。
- 限流与请求体上限（默认 10MB；反代上也有同样限制）。文件建议用 uri 传递而非 base64 内嵌。
- 容器以非 root、只读根文件系统、drop 所有 capability 运行；systemd unit 也做了沙箱加固。
- 定期备份 SQLite；`A2A_RETENTION_DAYS` 控制历史保留时长（含对话内容，注意隐私合规）。
