# A2A Bridge `/roster` 设计文档（草案）

> 状态：**已实现阶段 1 + 阶段 2**（只读快照、状态推断、隐私分级、SSE、Agent Card 指针）。阶段 3（webhook 订阅、按负载拒绝、防循环、告警推送）仍为未来工作。
> 代码基线：与本文件一同合入的实现。文中历史 `file:line` 引用是调研时的提示，可能已偏移；以函数名与当前代码为准。
> 日期：2026-10-20

## 1. 背景与目标

一个常见需求是：维护一个持久的、可以分享给其他 AI 的 agent 列表/状态。bridge 已经有静态的发现入口（`GET /agents`、各 agent 的 Agent Card），但外部 A2A agent 在派发任务前无法知道“谁现在有空、在忙什么、排了几个、是不是卡住了”。

`/roster` 提供一个**实时、按 API Key 限定范围、状态由 bridge 推断（非 bot 自报）**的 bot 列表：

- 外部 agent：派单前查询或订阅，选空闲的 bot、避免往卡住/离线的 bot 塞任务。
- 运营者（owner）：一屏看清所有 bot 的负载、卡住、待审批情况。
- bridge 自身：为“按负载限流 / 防循环 / 卡住告警”提供统一数据源。

非目标：不改变现有 A2A 任务协议；不主动探测 bot（Grok Bot 这类 webhook 驱动的 agent，其 webhook 往往就是自动化运行入口，探测一次就会触发一次运行，见 §4.4）。

## 2. 现有代码中可复用的东西（调研结论）

| 需要的数据 | 现有来源（file:line） | 说明 |
|---|---|---|
| Key → bot 权限 | `config.py:60-63` `ApiKey(label, sha256, agents)`；`config.py:117-142` `parse_api_keys`（`label=sha256:<hex>@a\|b`）；校验 `app.py:235-242` | `agents=None` 表示全部。**没有信任等级/owner 概念**，需新增（§6） |
| 任务归属 | `db.py:14-25` `tasks.owner` = key label（`app.py:272` 传 `key.label`）；`service.py:56-63` 跨 key 不可见 | owner 视图可按 label 聚合 |
| 任务状态与时间 | `db.py:14-25` `tasks(state, status_json, created_at, updated_at)`；索引 `ix_tasks_state(state, updated_at)` `db.py:27` | `updated_at` 在**任何**回调（状态/消息/产物）时更新：`db.py:111-120`，`service.py:291,302` → 天然的“进度时间戳” |
| 状态枚举 | `models.py:17-19` `TERMINAL / INTERRUPTED / BOT_SETTABLE` | submitted / working / input-required / auth-required / 终态 |
| 唯一状态变更入口 | `service.py:65-77` `Bridge._set_status` → `_emit`（`service.py:79-81`） | 所有状态变化（客户端、回调、超时、投递失败）都经过这里，是挂 roster 钩子的最佳位置 |
| bot 如何被唤醒 | `service.py:224-251` `_enqueue_bot` 写 outbox；`service.py:336-354` outbox 循环；`service.py:356-412` `_deliver` | 2xx 即送达并转 working（`service.py:414-431`）；401/403/404 永久失败；限流识别 `service.py:25-27,378-379`；最终失败把任务置 failed（`service.py:394-403`） |
| 投递健康（离线判断） | `db.py:39-52` `outbox(kind, task_id, attempts, status, last_error, next_at)`；`db.py:216-218` `outbox_stats` | outbox **没有 agent 列、没有送达时间戳** → 需要补 presence 表（§5） |
| 超时/卡死 | `sweep()`：submitted/working 超过 `task_timeout_s`(3600) → failed（**`waiting_on=owner` 的 working 除外**）；input-required/auth-required 以及 `waiting_on=owner` 的 working 超过 `input_required_timeout_s`(86400) → canceled；`_sweeper_loop` 每 `sweep_interval_s`(30s) 跑一次 | “stuck” 定义为早于这个硬超时的软阈值 |
| 健康信息 | `app.py:205-214` `/healthz`（db、outbox 统计、SSE 流数） | 只有全局，没有按 bot |
| 流式机制 | `events.py` `EventBus`（按任意字符串 key 订阅，队列 1000，慢消费者丢弃）；SSE 生成器 `app.py:374-417`（`retry: 3000`、`: keepalive` 每 `sse_heartbeat_s`） | `/roster/stream` 直接复用，订阅一个特殊 key `"__roster__"`，EventBus 无需改 |
| Webhook 推送机制 | `service.py:205-221` `_enqueue_push` → outbox kind=`push`；SSRF 校验 `security.py` `check_push_url` | roster webhook 订阅复用 outbox，新增 kind=`roster` |
| Agent Card / skills | `config.py:67-83` `AgentConfig.skills / required_inputs / raw`；`app.py:134-182` `card()`，已有扩展 `urn:a2a-bridge:input-fields:v1` `app.py:163-166`；`GetExtendedAgentCard` 目前返回 Unsupported `app.py:338-339` | roster 的 capabilities 直接取 `skills[].id/name/tags` |
| 限流 | `security.py` `RateLimiter`；JSON-RPC 用 `ip:` / `key:` 桶，roster 用独立的 `ip-roster:` / `roster-key:` 桶 | 轮询 `/roster` 不消耗 JSON-RPC 配额 |
| Bot 侧约定（skill 文档） | `docs/BOT-SIDE.md`：“只发 message 不发 state = 进度更新”、“超过 1 小时没有回调会被置 failed——长任务请定期发 working 进度”、回调可带 `metadata` | 这条约定就是事实上的**任务内心跳**；roster 只需在 skill 文档里补一个可选 metadata 字段（§4.3） |

额外观察：
- 回调 `metadata` 会随 status-update 事件发出（`service.py:301-302` → `_set_status(..., md)`），但**不持久化**（`status_json` 里没有）。roster 需要的 `waitingOn` / `publicActivity` 要落库。
- 常见的部署形态之一是只配置一个“入口/路由型” agent（例如 `agent-router`），由它再把任务转发给背后的多个 bot；bridge 看不到这层转发。此时 roster 只会显示入口 agent 一行，“入口 agent 背后的 bot 列表”见开放问题 Q1。
- 进程内总线、单 worker（README:129）→ roster 的内存缓存同样只适用单进程，可接受。

## 3. API 规格

所有路径与 JSON-RPC 同一 host；认证与 JSON-RPC 相同：有 Bearer 用 Bearer，否则用 `X-API-Key`（反向代理加上的 `Basic` 不会挡住有效的 `X-API-Key`）。`A2A_ROSTER_PUBLIC=true` 时只有**完全不带凭据**才算匿名；畸形 Authorization 且没有有效 key 一律 401。响应 `Cache-Control: no-store`，带 `ETag`（视图哈希，不含 `generatedAt` / `computedAt` / `stale` / `statusDurationS`），支持 `If-None-Match` → 304。GET 只读缓存。`computedAt` 是**全量**重算开始的时间，只在全量重算成功时推进；只重算部分 agent 不会刷新它。缓存比 `3 * A2A_SWEEP_INTERVAL_S` 更旧（或尚未计算）时返回 **503** `{"error":"degraded","stale":true}`。全量重算持续失败时，即使有 agent 在活动，过期缓存仍然 503。

### 3.1 `GET /roster`

查询参数（均可选）：`agent=<name>`（可重复）、`availability=available,limited,unavailable`（任何 tier）、`status=idle,busy,...`（仅 trusted/owner；basic/anonymous → 403）、`include=tasks`（仅 owner 有效）。

返回该 key 可调用的 bot：**始终与 `key.agents` 求交**（owner tier 也一样；`agents is None` 才是全部）。非 owner 另外隐藏 `roster_visible=false`。不在范围内的 bot **不出现**（不报错，不泄露存在性；虽然 `/agents` 本身是公开的，但保持与 TaskNotFound 同样的原则）。

trusted 视图示例：
```json
{
  "generatedAt": "2026-10-20T08:54:03.120Z",
  "computedAt": "2026-10-20T08:54:02.000Z",
  "stale": false,
  "viewTier": "trusted",
  "ttlSeconds": 5,
  "stream": "https://<bridge-host>/roster/stream",
  "agents": [
    {
      "id": "code-reviewer",
      "name": "Code Reviewer",
      "url": "https://<bridge-host>/agents/code-reviewer",
      "agentCard": "https://<bridge-host>/agents/code-reviewer/.well-known/agent-card.json",
      "skills": [{"id": "review-change", "name": "Review change", "tags": ["code", "git"]}],
      "status": "busy",
      "statusSince": "2026-10-20T08:31:40.002Z",
      "statusDurationS": 1343,
      "availability": "limited",
      "capacity": {"maxConcurrent": 2, "active": 1, "queued": 0, "throttled": false},
      "activity": "Running tests for a requested change",
      "waitingOn": null,
      "lastSeen": "2026-10-20T08:52:10.551Z",
      "confidence": "high"
    },
    {
      "id": "agent-a",
      "name": "Agent A",
      "status": "input-required",
      "statusSince": "2026-10-20T07:10:00.000Z",
      "statusDurationS": 6243,
      "availability": "available",
      "capacity": {"maxConcurrent": 1, "active": 0, "queued": 0, "throttled": false},
      "activity": "Waiting for owner approval",
      "waitingOn": "owner",
      "lastSeen": "2026-10-20T07:10:00.000Z",
      "confidence": "medium"
    }
  ]
}
```

basic（低信任）视图：每个 agent 只有 `id, name, url, agentCard, skills, availability, statusDurationS 取整到分钟`，没有 `activity / capacity.queued / lastSeen 精确值`（§6）。

owner 视图在 trusted 基础上加：
```json
"detail": {
  "counts": {"submitted": 0, "working": 1, "input-required": 1, "auth-required": 0},
  "failedLastHour": 0,
  "delivery": {"lastOkAt": "...", "lastErrorAt": null, "lastError": null, "consecutiveFailures": 0, "outboxPending": 0},
  "rawActivity": "正在为 xxx 项目跑 pytest …",
  "tasks": [{"taskId": "a1b2…", "owner": "partner-1", "state": "working", "updatedAt": "...", "stuck": false}]
}
```
（`tasks` 仅在 `include=tasks` 时返回。）

### 3.2 `GET /roster/{agent}`
单个 bot，结构同上。key 无权或 agent 不存在 → 404（统一，不区分）。

### 3.3 `GET /roster/stream`（SSE）
- 首帧 `event: snapshot`，data 为 §3.1 完整对象；之后只在**该订阅者可见视图发生变化**时发 `event: update`，data 为单个 agent 对象 + `"seq"`（**按本连接**递增，不暴露其他 agent 的变更频率）。
- 每 `A2A_SSE_HEARTBEAT_S`(15s) `: keepalive`；`retry: 3000`；`id:` 为该连接的 seq，断线重连带 `Last-Event-ID` 时直接重发 snapshot（不做事件回放，roster 是状态而非日志）。
- 上限：每 key `A2A_ROSTER_MAX_STREAMS_PER_KEY`(3)；全局 `A2A_ROSTER_MAX_STREAMS`(32)；匿名另计 `A2A_ROSTER_MAX_ANON_STREAMS`(3)。写出阻塞超过 `A2A_ROSTER_STREAM_TIMEOUT_S`(30s) 或连接超过 `A2A_ROSTER_STREAM_MAX_S`(300s) 则断开。
- 路由：`/roster/stream` 是 SSE 入口，agent 名 `stream` **保留**（配置时报错）。
```
event: update
id: 1842
data: {"seq":1842,"agent":{"id":"code-reviewer","status":"stuck","statusSince":"…","availability":"unavailable", …}}
```

实现说明（与代码一致的细节）：
- `GET /roster/{agent}` 返回与列表相同的信封（`generatedAt` / `viewTier` / `ttlSeconds` / `stream` / `agents`），`agents` 仅含这一条。
- `ETag` 是视图哈希，**不含** `generatedAt` 和 `statusDurationS`（后者每秒都会变），因此 `If-None-Match` 可以 304。
- 非法 `waitingOn` 在回调里被忽略，不让整个 callback 失败。
- 不主动探测 bot webhook。
- 推断在后台循环（debounce + 最长等待 `A2A_ROSTER_DEBOUNCE_MAX_S`）和 sweeper `tick()` 里重算；`GET`/`SSE` 读缓存。`roster_signals` 不读 `status_json`（避免文件/base64）；outbox 有 `ix_outbox_task(task_id)` 与 `ix_outbox_kind_status(kind,status)`。

### 3.4 Webhook 订阅（阶段 3，未实现）
- `POST /roster/subscriptions` body `{"url":"https://…","token":"…","agents":["code-reviewer"],"statuses":["idle","stuck","offline"]}` → `201 {"id":"rsub_…", ...}`
- `GET /roster/subscriptions`、`DELETE /roster/subscriptions/{id}`；只能看到/删除本 key 的订阅。
- 推送体 = SSE `update` 的 data，头 `X-A2A-Notification-Token: <token>`，与任务推送一致（`service.py:215-220`）；URL 走 `check_push_url`（https、禁内网）。投递走 outbox kind=`roster`，同一 agent 30s 内合并为最新状态（先 `outbox_cancel_for_task` 式地作废旧的 pending）。
- 仅 trusted/owner 可创建（basic key 只能 SSE/轮询，防止被当作免费探针放大器）。

### 3.5 错误码

| HTTP | body | 场景 |
|---|---|---|
| 401 | `{"error":"unauthorized"}` + `WWW-Authenticate: Bearer` | 无/错误 key（且未开启匿名访问） |
| 404 | `{"error":"agent not found"}` | `/roster/{agent}` 不存在或不在 key 范围 |
| 400 | `{"error":"invalid parameter","param":"status"}` | 非法查询参数、订阅 URL 不合规 |
| 403 | `{"error":"forbidden"}` | basic/anonymous 用 `status=` 过滤、非 owner 用 `include=tasks`、basic key 创建 webhook 订阅 |
| 429 | `{"error":"rate limit exceeded"}` + `Retry-After` | 复用 IP/key 限流；或 SSE 流数超限 |
| 503 | `{"error":"degraded"}` | DB 不可用（同 `/healthz`） |

REST 风格而非 JSON-RPC：roster 不是 A2A 标准方法，放在 `rpc()` 里会被 `METHOD NOT FOUND` 拦（`app.py:263-264`）。可选再加 JSON-RPC 别名 `roster/get`，但收益不大，暂不做。

## 4. 状态推断规则

### 4.1 输入信号（全部来自 bridge 已有/将记录的数据）
对每个 agent，在 `tasks` 上做一次聚合（命中 `ix_tasks_state`）：
```sql
SELECT agent, state, COUNT(*) n, MIN(updated_at) oldest, MAX(updated_at) newest
FROM tasks WHERE state IN ('submitted','working','input-required','auth-required')
GROUP BY agent, state;
```
加上 presence 表（§5）里的投递健康与 lastSeen、tasks 新列 `waiting_on`、最近 1 小时 failed 数。

### 4.2 每个 bot 的状态（按优先级取第一条命中）

| 状态 | 规则 | 依据代码 |
|---|---|---|
| `offline` | 最近一次 **HTTP 401/403/404** 记在 `delivery_dead_at`（不是 `last_delivery_err_at`），之后没有更新的成功投递/回调，且仍在 `A2A_ROSTER_OFFLINE_TTL_S`(3600s) 内。TTL 过后清掉 `delivery_dead`，后续的超时/500 **不会**把锁刷回来。清理是条件更新：只有 `delivery_dead_at` 仍等于这次快照才清，重算期间主线程新写入的 401 不会被清掉。另外：连续 ≥3 次失败，且失败串已持续 `A2A_ROSTER_OFFLINE_AFTER_S`(120s)、最后一次失败仍在 TTL 内——**不要求**还有 submitted 任务（webhook 一直不可达、任务已 failed 时也是 offline）。失败计数同样只在计数和 `last_delivery_err_at` 都没变时才清零。限流（HTTP 429，或响应体像 rate limit 的 4xx）**只**写 `throttled_until`，不增加 `consecutive_failures`，也不刷新 `last_delivery_err_at`，所以三次 429 不会变成 offline。 | `note_delivery_err`、`_expire_auth_lock` |
| `stuck` | 存在 working 任务，`waiting_on` 不是 owner，且 `now - updated_at > stuck_after_s`（默认 `A2A_ROSTER_STUCK_AFTER_S`=900s，agents.json 可按 bot 覆盖 `stuck_after_s`，必须 < `task_timeout_s`）；或 submitted 任务已成功投递但超过 300s 没变 working | `updated_at` 刷新点：`db.py:111-120`、`service.py:291,302`；硬超时 `service.py:452-456` |
| `busy` | working 数 ≥ 1（`active`）；若 `active ≥ maxConcurrent` 或最近投递命中限流（`_RATE_LIMIT_RE`）则 `availability=unavailable`、`throttled=true` | `service.py:414-431`（2xx 后转 working）、`service.py:25-27,378-379` |
| `input-required` | 没有 working，但有 input-required/auth-required 任务，或 working 任务带 `waiting_on="owner"`；`waitingOn` ∈ `requester`/`owner`/`auth` | `models.py:18` `INTERRUPTED`；`service.py:298-302` |
| `idle` | 以上都不满足 | — |

补充：
- **queued** = submitted 数 + 超出 `maxConcurrent` 的 working 数。submitted 本身就表示“已入 outbox、bot 尚未 2xx”（`service.py:113,125`）。
- **statusSince**：presence 表记录聚合状态上一次变化的时间；进程重启后用 DB 推算（busy → 当前 working 任务中最早的 `status_json.timestamp`；idle → 最近一个任务进入终态的 `updated_at`）。
- **lastSeen** = max(最近一次投递 2xx 时间, 最近一次通过认证的回调时间)。这是 bot“活着”的唯一硬证据。
- **confidence**：`high` = lastSeen 在 10 分钟内；`medium` = 24 小时内；`low` = 更早或从未有过。idle 不等于在线——没有任务时 bridge 看不到 bot（见 §4.4），用 confidence 诚实表达。
- **availability**（给 basic 视图的唯一状态字段）：`available`（idle / 仅等待请求方输入且有余量）、`limited`（busy 但有余量）、`unavailable`（满载、throttled、stuck、offline）。

### 4.3 “等待 owner 审批”怎么区分
现状：A2A 的 `input-required` 意思是“请**请求方**补充输入”，bridge 无法区分“等外部请求方回答”和“等运营者（owner）审批”。而且如果是等 owner，对外部客户端返回 input-required 是错的（它无法提供这个输入）。

方案：bot 在等 owner 审批时**保持 `working`**，回调带 `metadata: {"roster": {"waitingOn": "owner"}}`；bridge 在 `apply_callback` 把它写入 tasks 新列 `waiting_on`，下次状态/消息进展（以及终态）时清空 `waiting_on` 与 `public_activity`（除非本次再次带上）。roster 把它显示成 `input-required / waitingOn=owner`，并从 stuck 判定中排除。硬超时走 `input_required_timeout_s`（canceled），**不会**在 `task_timeout_s`(1h) 被标 failed。这算 bot 提供的提示而不是状态自报——状态本身仍由任务数据推出。见 `docs/BOT-SIDE.md`。

### 4.4 为什么不做主动探测 / 空闲心跳
对 Grok Bot 这类 webhook 驱动的 agent，webhook 本身就是自动化运行入口（参见 `service.py:25-27` 对平台限流响应的处理），任何探测都会触发一次运行并消耗限额。因此空闲 bot 的在线性只能从“最近一次投递/回调”推断。可选的第三阶段：允许 bot 用共享密钥签名 `POST /presence/{agent}` 做低频心跳——但这属于自报，默认关闭。

### 4.5 实时性
- 事件驱动：`_set_status`、新建任务、`_deliver` 成功/失败、`apply_callback` 时调用 `roster.touch(agent)`。后台循环在安静 `A2A_ROSTER_DEBOUNCE_S`(1s) **或** 脏数据已持续 `A2A_ROSTER_DEBOUNCE_MAX_S`(2s) 时重算。SQL 在线程里跑，EventBus 发布留在事件循环。`recompute_async` 用一把 `asyncio.Lock`，debounce 和 sweeper 不会同时算。运行期间新标脏的 agent 留到下一轮。结果只在开始时间晚于当前 `computedAt` 时才写入缓存。
- 时间驱动：stuck/offline 随时间变化。`_sweeper_loop` 每 `sweep_interval_s`(30s) 调全量 `recompute_async()`。sweep 和这次重算各有自己的 try，一个失败不跳过另一个。HTTP GET 不重算。
- 热路径：`roster_signals` 走单独的只读连接，不占用事件循环那条连接的锁，也不扫全表终态（`last_terminal_at` 在任务进入终态时写入 presence）。已投递的 submitted 用 `EXISTS`，并 `INDEXED BY ix_outbox_task_kind_status`。pending 计数钉在 `ix_outbox_kind_status`。没跑过 `ANALYZE` 和跑过之后都是这个计划。

## 5. 数据模型变更

```sql
-- 新表：每个 agent 一行，记录推断结果与“活着”的证据
CREATE TABLE IF NOT EXISTS agent_presence (
  agent TEXT PRIMARY KEY,
  status TEXT NOT NULL,              -- idle|busy|input-required|stuck|offline
  since REAL NOT NULL,               -- 当前状态开始时间
  last_seen_at REAL,                 -- max(投递 2xx, 回调)
  last_delivery_ok_at REAL,
  last_delivery_err_at REAL,
  last_delivery_err TEXT,            -- 截断 200 字符，仅 owner 可见
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  throttled_until REAL,              -- 命中限流时 = now + retry 延迟
  delivery_dead INTEGER NOT NULL DEFAULT 0,  -- 401/403/404 永久失败锁
  delivery_dead_at REAL,             -- 该锁的时间，TTL 只看它
  failures_since REAL,               -- 当前连续失败从何时开始
  last_terminal_at REAL,             -- 最近一次任务进入终态（避免全表 MAX）
  updated_at REAL NOT NULL
);
-- tasks 新列（沿用 Store.__init__ 的 ALTER TABLE 迁移写法，启动时自动执行）
ALTER TABLE tasks ADD COLUMN waiting_on TEXT;         -- requester|owner|auth|NULL
ALTER TABLE tasks ADD COLUMN public_activity TEXT;    -- bot 提供的脱敏摘要，≤120 字符
ALTER TABLE tasks ADD COLUMN status_text TEXT;        -- 从 status_json 抽出的纯文本，推断用，避免读大 JSON
CREATE INDEX IF NOT EXISTS ix_tasks_agent_state ON tasks(agent, state, updated_at);
CREATE INDEX IF NOT EXISTS ix_outbox_task ON outbox(task_id);
CREATE INDEX IF NOT EXISTS ix_outbox_kind_status ON outbox(kind, status);
CREATE INDEX IF NOT EXISTS ix_outbox_task_kind_status ON outbox(task_id, kind, status);
-- 阶段 3（未实现）才会需要 roster_subscriptions
```
配置新增：
- agents.json 每个 agent 可选：`max_concurrent`（默认 1）、`stuck_after_s`、`roster_visible`（默认 true，可把内部 bot 从 roster 隐藏）。
- 环境变量：`A2A_KEY_TIERS`（如 `ops:owner,partner-1:trusted`，未列出默认 `basic`）、`A2A_ROSTER_STUCK_AFTER_S`(900)、`A2A_ROSTER_OFFLINE_AFTER_S`(120)、`A2A_ROSTER_OFFLINE_TTL_S`(3600)、`A2A_ROSTER_MAX_STREAMS_PER_KEY`(3)、`A2A_ROSTER_MAX_STREAMS`(32)、`A2A_ROSTER_MAX_ANON_STREAMS`(3)、`A2A_ROSTER_STREAM_TIMEOUT_S`(30)、`A2A_ROSTER_STREAM_MAX_S`(300)、`A2A_ROSTER_PUBLIC`(false，是否允许匿名看 availability)、`A2A_ROSTER_REDACT`（逗号分隔的敏感词/正则）、`A2A_ROSTER_DEBOUNCE_S`(1)、`A2A_ROSTER_DEBOUNCE_MAX_S`(2)。`A2A_ALERT_WEBHOOK_URL` 属于阶段 3，尚未使用。
- `ApiKey` 增加 `tier: str = "basic"`，由 `A2A_KEY_TIERS` 填充；**不改** `A2A_API_KEYS` 语法，向后兼容。owner 仍与 `key.agents` 求交。
- 迁移：`Store._migrate` 使用 `BEGIN IMMEDIATE`，`ALTER TABLE` 对已存在列容忍 duplicate column；启动时补 `status_text`、presence 新列与 outbox 索引（含 `ix_outbox_task_kind_status`）。`status_text` 回填：终态行、空值、非法 JSON、以及 `message.parts` 不是数组的行写成空串作为已处理标记，不再解析；其余非终态用 `json_extract` / `json_each` 只取出文本字段。空串或已有文本的行下次启动不再扫。`busy_timeout` 在 `journal_mode=WAL` 之前设置；第一次切到 WAL 时若多个进程同时启动碰到 `database is locked`，外层会重试。agent 名 `stream` 保留。API key label `*` 保留（匿名流）。配置解析错误不回显密钥片段。`roster_visible=false` 的 Agent Card **不**带 roster 扩展。

## 6. 隐私分级

### 6.1 分级与字段

| 字段 | anonymous（需 `A2A_ROSTER_PUBLIC=true`） | basic（默认 key） | trusted | owner |
|---|---|---|---|---|
| id/name/url/card/skills | ✓（仅公开 agent） | ✓（key 范围内） | ✓ | ✓ key 范围内（含 `roster_visible=false`） |
| availability | ✓ | ✓ | ✓ | ✓ |
| status / waitingOn | — | — | ✓ | ✓ |
| statusDurationS | — | 取整到分钟 | ✓ | ✓ |
| capacity（active/queued/throttled） | — | 仅 `queued>0` 布尔 | ✓ | ✓ |
| activity | — | — | 仅 `public_activity` 或通用模板 | 原始 status message |
| lastSeen | — | — | 取整到分钟 | ✓ |
| detail（计数、投递错误、任务列表、调用方 label） | — | — | — | ✓ |

说明：哪怕只给 availability，也会泄露“别人正在用这个 bot”。这是功能本身的代价，对 basic 用三档粗粒度 + 分钟取整来降低时序侧信道。

### 6.2 activity 文本如何避免泄露项目名
1. **默认不用原文**。原始 `status.message`（bot 写的进度，例如“已克隆 xxx 仓库…”）只给 owner。
2. trusted 看到的 activity 来源优先级：
   a. bot 在回调里提供的 `metadata.roster.publicActivity`（BOT-SIDE.md 约定：不含项目/仓库/人名/URL，≤120 字符），存入 `tasks.public_activity`；
   b. 否则用模板：`"Working on a <skill name> task"` / `"Waiting for requester input"` / `"Waiting for owner approval"`（skill 名来自 agents.json，可公开）。
3. 即使是 (a) 也过一遍清洗：入口先把字符串截到 512 字符（主机名/邮箱正则在超长输入上会回溯），再去掉 URL、邮箱、`org/repo`（`text/plain` 这类 MIME 保留；`unit/integration` 这类通用片段只有左右两边都是通用词才保留，所以 `src/secret-billing` 会去掉）、长 hex/uuid、私网/回环/链路本地 IPv4、全部 IPv6（含公网地址，以及 `::1`、`fe80::1`）、内网主机名（`.local`/`.internal`/`.corp` 等）和普通域名（如 `billing.example.com`）。**不**把 `ASP.NET`、`socket.io` 当主机名。GitHub/AWS token、JWT、电话（含 `+86` 和以 138 开头的 11 位手机号；不误伤 `2026-10-20` 或 `120 200 300`）同样去掉。命中 `A2A_ROSTER_REDACT` 的词替换成 `[redacted]`，输出截断 120 字符。
4. 请求方是自己的 key 时（`tasks.owner == key.label`），该 key 可以看到**自己任务**的原始状态消息——这和它通过 `tasks/get` 已经能拿到的信息一致，不额外泄露。

## 7. Agent Card 扩展与 A2A 兼容性

- 在 `card()` 的 `capabilities.extensions`（`app.py:163-166`）追加：
```json
{"uri": "urn:a2a-bridge:roster:v1", "required": false,
 "description": "Live availability of this agent; requires the same API key",
 "params": {"rosterUrl": "https://<bridge-host>/roster/<agent>", "streamUrl": "https://<bridge-host>/roster/stream",
            "statusValues": ["idle","busy","input-required","stuck","offline"]}}
```
- **不要把实时状态写进公开 Agent Card**：card 无需认证且带 `Cache-Control: public, max-age=300`（`app.py:187`），实时数据会被缓存并泄露给未授权方。card 里只放指针。
- 可选：实现 `agent/getAuthenticatedExtendedCard`（v0.3）/ `GetExtendedAgentCard`（v1.0），目前返回 Unsupported（`app.py:338-339`）。在扩展卡的同名 extension `params` 里附带按该 key 分级后的实时快照，这样只懂 A2A 标准的客户端也能拿到。同时需要把公开 card 的 `supportsAuthenticatedExtendedCard`（v0.3）/ `capabilities.extendedAgentCard`（v1.0）设为 true。
- 兼容性：A2A v0.3 和 v1.0 都没有标准的“在线状态/负载”概念；`AgentExtension{uri, description, required, params}` 在两个版本里都存在，未知扩展客户端会忽略，`required:false` 不影响现有 SDK（已验证的 a2a-sdk 1.2.2 不受影响，需回归测试确认）。roster 状态名用 kebab-case，与 bridge 内部 v0.3 状态名风格一致；不映射成 `TASK_STATE_*`，避免被误解成任务状态。

## 8. 后续用途

1. **按负载准入/限流**：`send_message`（`service.py:90`）入口检查该 agent 的 `queued ≥ max_queue`（新配置，默认关闭）→ 返回 JSON-RPC 错误（建议新增 `-32010 AgentBusy`，`data: {"retryAfterS": N, "roster": "<url>"}`），或按 key 的公平份额拒绝。throttled 期间新任务直接排队不立即投递，减少撞平台限流。
2. **防循环**：(a) 识别回环——调用方 key 被标记为某个本地 bot（`A2A_KEY_TIERS` 中加 `bot:<agent>` 属性），它不能给自己派任务；(b) 跳数——约定 `params.metadata.a2aHops`，bridge 在 `_enqueue_bot` 时 +1 传给 bot，超过 `A2A_MAX_HOPS`(3) 拒绝；(c) 同一 `contextId` 10 分钟内创建任务数 > N 拒绝。roster 给出“这个 bot 正在为谁工作”的数据，帮助 bot 自己判断。
3. **卡住告警**：状态转为 `stuck`/`offline` 时写 WARNING 日志，并（若配置 `A2A_ALERT_WEBHOOK_URL`）通过 outbox kind=`alert` 推给 owner；同一 agent 同一状态 1 小时内只告警一次；恢复时发 resolved。

## 9. 实施计划

| 阶段 | 内容 | 涉及文件 | 估算 |
|---|---|---|---|
| P1 只读快照 | **已实现** `ApiKey.tier`、`A2A_KEY_TIERS`、agents.json 新字段；`agent_presence` 表与 `tasks` 新列迁移；`roster.py`；`GET /roster`、`/roster/{agent}`；在 `_deliver`/`apply_callback`/`_set_status` 里更新 presence；`apply_callback` 持久化 `metadata.roster.*` | `config.py`、`db.py`、`roster.py`、`service.py`、`app.py` | — |
| P1 测试 | **已实现** 推断规则、key 范围、分级字段裁剪、清洗、旧库迁移 | `tests/test_roster.py` | — |
| P2 实时 | **已实现** `/roster/stream`（EventBus key `__roster__`，按订阅者视图去重）、`roster.tick()` 挂到 sweeper、Agent Card 扩展 | `app.py`、`service.py`、`roster.py` | — |
| P2 文档 | **已实现** CONNECT / BOT-SIDE / README / 本文件 | docs | — |
| P3 可选 | webhook 订阅、扩展卡、准入限流、防循环、告警 | `db.py`、`service.py`、`app.py` | 未做 |

合计：P1+P2 约 3–4 人日；全部约 5–6 人日。上线方式与现有一致（替换代码后 `docker compose up -d --build`，迁移在 `Store.__init__` 自动执行），部署前由运营者确认；迁移前先按 README 备份 SQLite。

## 10. 开放问题

1. **入口/路由型 agent 背后的 bot 算不算 roster 成员？** 本版只展示 bridge 配置的 agent（选项 c）。入口 agent 背后的转发对 bridge 不可见。
2. **owner 身份**：要求 `A2A_KEY_TIERS` 里显式 `label:owner` 的专用 key（可把现有 label 提升为 owner）。
3. **默认 tier**：未在 `A2A_KEY_TIERS` 中列出的已有 key 默认为 **basic**。
4. **max_concurrent 的默认值**：webhook 驱动的 bot 通常并行能力和平台限流阈值都未知，代码只能事后从 429/“rate limit” 400 推断。默认 1 是否合适？是否应根据限流反馈自适应？
5. **stuck 阈值**：900s 是否合适？编码类长任务可能 15 分钟没有回调——是否在 BOT-SIDE.md 里要求每 5 分钟发一次 `working` 进度作为心跳？
6. **匿名访问**：要不要开放 `A2A_ROSTER_PUBLIC`（让未认证的 agent 也能看 availability，便于“分享给其他 AI”）？风险是被用来观察使用情况。
7. **是否需要历史**：例如“过去 24 小时利用率/成功率”，需要另存时间序列；本设计只做当前状态。
8. **waitingOn=owner 的审批入口**：roster 只显示在等审批；审批本身走哪个渠道（bot 自己的聊天）不在 bridge 范围内，是否需要 bridge 提供审批 API？
