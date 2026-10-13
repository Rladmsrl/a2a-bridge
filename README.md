# a2a-bridge

**Expose a personal webhook-driven agent (e.g. a Grok Bot routine) as a standard [A2A (Agent2Agent)](https://a2a-protocol.org) server — securely.**

```
External A2A agent ──HTTPS JSON-RPC / SSE──▶  a2a-bridge  ──webhook POST──▶  Grok Bot routine (or any webhook agent)
                   ◀── stream / tasks/get ──  (SQLite)    ◀──callback POST──  (progress, questions, artifacts)
```

External agents speak normal A2A to the bridge. The bridge turns every task into a short-lived webhook call to your bot (with the full conversation, a per-task callback URL/token and plain-text reply instructions). The bot replies asynchronously by POSTing to the callback; the bridge keeps task state and streams updates back to the caller.

The receiving side can be **any agent or service that can be triggered by an HTTP webhook and make an HTTP request** — Grok Bot routines, n8n/Zapier flows, your own server, etc.

## Features
- A2A **v0.3** (`message/send`, `message/stream`, `tasks/get`, `tasks/cancel`, `tasks/resubscribe`, push config) **and v1.0** (`SendMessage`, `SendStreamingMessage`, `GetTask`, `ListTasks`, `CancelTask`, `SubscribeToTask`, …), auto-detected by method name. Interop-tested with the official `a2a-sdk` 1.2.
- Agent Card at `/.well-known/agent-card.json` (+ legacy `agent.json`), one card per configured agent under `/agents/<name>/`.
- Text / file / data parts, `contextId` multi-turn, `input-required` follow-ups, artifacts (incl. append/chunks).
- SSE streaming with heartbeats, resubscribe after disconnect, polling fallback, optional push notifications (SSRF-guarded).
- Auth: inbound bearer API keys (stored as SHA-256, optionally scoped per agent); bot callbacks authenticated with per-task HMAC-derived tokens or HMAC signatures; outbound webhooks carry a bearer token + HMAC signature.
- Reliability: SQLite persistence, durable outbox with exponential-backoff retries (also handles platforms that report rate limits as HTTP 400), stuck-task timeouts, size limits, rate limiting, JSON logs, `/healthz`.

## 3-step setup
1. **Create the receiving bot**: e.g. a Grok Bot routine triggered by webhook. Note its webhook URL and sender key. Give the bot the instructions in [docs/BOT-SIDE.md](docs/BOT-SIDE.md) (how to read the payload and reply via callback).
2. **Deploy** on a Linux server with Docker, a domain pointing at it, and port 443 open:
   ```bash
   git clone https://github.com/<you>/a2a-bridge && cd a2a-bridge
   BOT_WEBHOOK_TOKEN='<bot sender key>' ./scripts/install.sh --domain a2a.example.com --webhook-url 'https://<bot webhook url>'
   ```
   This generates secrets and an API key (`api-key.txt`), writes `.env` + `config/agents.json`, and starts the bridge and Caddy (automatic HTTPS). Check `curl https://a2a.example.com/healthz`.
3. **Connect clients**: give an external agent the API key and [docs/SKILL.md](docs/SKILL.md) (a generic connect skill; key from env `A2A_KEY`). Full protocol guide: [docs/CONNECT.md](docs/CONNECT.md).

Manual setup, multiple bots, systemd, nginx and all env vars: see [docs/README.zh-CN.md](docs/README.zh-CN.md) and [.env.example](.env.example).

## Development
```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest          # unit + integration tests
./scripts/e2e_local.sh              # real processes: bridge + mock bot + CLI + official SDK
python scripts/a2a_cli.py --help    # manual test client
```

## 中文简介
a2a-bridge 让任何外部 A2A 智能体安全地与你的个人 Bot（例如 Grok Bot 的 webhook 例程，或任何支持 webhook 的 agent）对话：外部 agent → bridge（标准 A2A JSON-RPC/SSE）→ Bot webhook → Bot 通过回调上报进度、追问（input-required）和结果 → bridge 推回给调用方。支持 A2A v0.3 与 v1.0、SSE 流式与断线重订阅、多轮对话、API Key + HMAC 认证、SQLite 持久化与带退避的重试。

三步上手：① 创建 webhook 触发的 Bot，并把 [docs/BOT-SIDE.md](docs/BOT-SIDE.md) 交给它；② 在服务器上运行 `scripts/install.sh --domain <域名> --webhook-url <bot webhook>`（token 用环境变量 `BOT_WEBHOOK_TOKEN` 传入）；③ 把 `api-key.txt` 中的 key 和 [docs/SKILL.md](docs/SKILL.md) 交给外部 agent。详细中文文档见 [docs/README.zh-CN.md](docs/README.zh-CN.md)、[docs/CONNECT.md](docs/CONNECT.md)。

## License
MIT — see [LICENSE](LICENSE). Security notes: [SECURITY.md](SECURITY.md).
