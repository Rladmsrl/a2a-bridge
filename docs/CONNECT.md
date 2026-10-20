# 外部 Agent 接入指南（A2A）

本服务是标准 **A2A（Agent2Agent）JSON-RPC 2.0 over HTTPS** 服务端，同时支持 **A2A v0.3** 与 **v1.0** 方法名。任何符合规范的 A2A 客户端都可以直接接入。下文以 `https://<bridge-host>` 作为占位地址，`<agent>` 为 agent 名（如 `coder`）。

## 1. 发现 Agent Card

```bash
curl -s https://<bridge-host>/.well-known/agent-card.json          # 默认 agent
curl -s https://<bridge-host>/agents/<agent>/.well-known/agent-card.json
curl -s https://<bridge-host>/agents                                # 所有 agent 列表
```
（旧路径 `/.well-known/agent.json` 也可用。）Card 中：
- `url` / `supportedInterfaces[].url`：JSON-RPC 端点，即 `https://<bridge-host>/agents/<agent>`
- `capabilities.streaming=true`、`pushNotifications=true`
- `description`、`skills[].description` 以及扩展 `urn:a2a-bridge:input-fields:v1` 描述了**需要提供的输入字段**（例如 `repo_url`、`goal`、`acceptance_criteria`）。请在第一条消息里提供（文本或 data part 均可），否则 agent 会以 `input-required` 追问。

## 2. 认证

所有 JSON-RPC 请求需带运营方发放的 API Key：

```
Authorization: Bearer <API_KEY>
```
缺失/错误返回 HTTP 401（`WWW-Authenticate: Bearer`）；Key 无权访问该 agent 返回 403。超限返回 429（带 `Retry-After`）。请求体上限默认 10MB。

## 3. 发送消息 `message/send`（v0.3）

```bash
curl -s https://<bridge-host>/agents/<agent> \
  -H "Authorization: Bearer $A2A_KEY" -H 'Content-Type: application/json' -d '{
  "jsonrpc":"2.0","id":"1","method":"message/send",
  "params":{
    "message":{"kind":"message","messageId":"msg-001","role":"user",
      "parts":[
        {"kind":"text","text":"Please add a /healthz endpoint"},
        {"kind":"data","data":{"repo_url":"https://github.com/org/repo","goal":"add /healthz","acceptance_criteria":"GET /healthz returns 200"}},
        {"kind":"file","file":{"name":"spec.md","mimeType":"text/markdown","uri":"https://example.com/spec.md"}}
      ]},
    "configuration":{"blocking":false}
  }}'
```
返回 `result` 为 Task：`{"kind":"task","id":"<taskId>","contextId":"<contextId>","status":{"state":"submitted"},...}`。
- `configuration.blocking=true`：等待到 input-required / 终态再返回（最多约 30 秒，超时返回当前状态）。
- 文件也可用 `"bytes":"<base64>"`，大文件建议用 `uri`。

v1.0 等价写法（方法 `SendMessage`，默认阻塞，可设 `returnImmediately`；返回 `{"task":{...}}`，状态为 `TASK_STATE_*`）：
```bash
curl -s https://<bridge-host>/agents/<agent> -H "Authorization: Bearer $A2A_KEY" -H 'A2A-Version: 1.0' \
  -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":"1","method":"SendMessage","params":{
  "message":{"messageId":"msg-001","role":"ROLE_USER","parts":[{"text":"hello"},{"data":{"repo_url":"https://github.com/org/repo"}}]},
  "configuration":{"returnImmediately":true}}}'
```

## 4. 流式 `message/stream`（SSE）

```bash
curl -N https://<bridge-host>/agents/<agent> -H "Authorization: Bearer $A2A_KEY" \
  -H 'Content-Type: application/json' -H 'Accept: text/event-stream' -d '{
  "jsonrpc":"2.0","id":"2","method":"message/stream",
  "params":{"message":{"kind":"message","messageId":"msg-002","role":"user","parts":[{"kind":"text","text":"..."}]}}}'
```
每个 `data:` 行是一个 JSON-RPC 响应，`result` 依次为：
1. `Task`（`kind:"task"`）—— 首个事件，拿到 `id` 与 `contextId`
2. `status-update`（`status.state` + 可选 `status.message`）/ `artifact-update`（`artifact`、`append`、`lastChunk`）
3. 终态时 `status-update` 带 `"final": true`，然后服务器关闭流。

流在 **`input-required`** 时也会结束（本轮结束，等待你回答）。服务器每 15 秒发送 `: keepalive` 注释作为心跳，请忽略。v1.0 名称为 `SendStreamingMessage`，事件为 `{"task":…}` / `{"statusUpdate":…}` / `{"artifactUpdate":…}`。

## 5. 多轮对话与 `input-required`

- 状态为 `input-required` 时，读取 `status.message` 中的问题，回答时**带上同一个 `taskId`（和 `contextId`）**：
```json
{"jsonrpc":"2.0","id":"3","method":"message/stream","params":{"message":{
  "kind":"message","messageId":"msg-003","role":"user","taskId":"<taskId>","contextId":"<contextId>",
  "parts":[{"kind":"text","text":"branch: main"}]}}}
```
- 在同一会话中开始**新任务**：只带 `contextId`、不带 `taskId`。agent 会看到该 context 的完整历史。
- 向已终态（completed/failed/canceled/rejected）的任务发消息会返回 `-32004`；请新建任务（同 contextId）。
- `messageId` 用于去重：重发相同 messageId 不会重复投递。

## 6. 断线恢复与轮询

- 重新订阅：`{"method":"tasks/resubscribe","params":{"id":"<taskId>"}}`（v1：`SubscribeToTask`），首个事件为当前 Task 快照，然后继续推送。
- 轮询兜底：`{"method":"tasks/get","params":{"id":"<taskId>","historyLength":10}}`（v1：`GetTask`）。建议间隔 2–5 秒并逐步退避。
- 取消：`tasks/cancel` / `CancelTask`。
- 列表：`tasks/list` / `ListTasks`（`contextId`、`status`、`pageSize`、`pageToken`、`includeArtifacts`）。
- 推送通知（可选）：`tasks/pushNotificationConfig/set`，参数 `{"taskId":"…","pushNotificationConfig":{"url":"https://your-host/hook","token":"…","authentication":{"schemes":["Bearer"],"credentials":"…"}}}`；也可在 send 时放入 `configuration.pushNotificationConfig`。URL 必须是公网 https。v0.3 推送体为完整 Task；v1.0（`CreateTaskPushNotificationConfig`）推送体为 StreamResponse。

## 7. Python 示例（官方 a2a-sdk ≥ 1.0）

```python
# pip install a2a-sdk httpx
import asyncio, uuid, httpx
from a2a.client import ClientConfig, create_client
from a2a.types import a2a_pb2 as pb

async def main():
    async with httpx.AsyncClient(headers={"Authorization": "Bearer <API_KEY>"}, timeout=120) as hc:
        client = await create_client("https://<bridge-host>/agents/<agent>",
                                     ClientConfig(httpx_client=hc, streaming=True))
        task_id = ctx = None
        for text in ["repo_url: https://github.com/org/repo\ngoal: add /healthz\nacceptance_criteria: returns 200",
                     "branch: main"]:
            msg = pb.Message(message_id=str(uuid.uuid4()), role=pb.ROLE_USER, parts=[pb.Part(text=text)])
            if task_id:
                msg.task_id, msg.context_id = task_id, ctx
            state = None
            async for ev in client.send_message(pb.SendMessageRequest(message=msg)):
                k = ev.WhichOneof("payload")
                if k == "task":
                    task_id, ctx, state = ev.task.id, ev.task.context_id, ev.task.status.state
                elif k == "status_update":
                    state = ev.status_update.status.state
                    print(pb.TaskState.Name(state), [p.text for p in ev.status_update.status.message.parts])
                elif k == "artifact_update":
                    print("artifact", ev.artifact_update.artifact.name)
            if state != pb.TASK_STATE_INPUT_REQUIRED:
                break
        task = await client.get_task(pb.GetTaskRequest(id=task_id))
        print(pb.TaskState.Name(task.status.state), task.artifacts)

asyncio.run(main())
```
（同样内容见 `scripts/sdk_example.py`，已在本地与官方 SDK 1.2.2 验证。）

无 SDK 的 Python（httpx，v0.3）：
```python
import httpx, json, uuid
URL, H = "https://<bridge-host>/agents/<agent>", {"Authorization": "Bearer <API_KEY>"}
body = {"jsonrpc": "2.0", "id": 1, "method": "message/stream", "params": {"message": {
    "kind": "message", "messageId": str(uuid.uuid4()), "role": "user",
    "parts": [{"kind": "text", "text": "hello"}]}}}
with httpx.stream("POST", URL, json=body, headers=H, timeout=None) as r:
    for line in r.iter_lines():
        if line.startswith("data:"):
            ev = json.loads(line[5:])["result"]
            print(ev.get("kind"), ev.get("status", {}).get("state"))
```

## 8. JavaScript 示例（Node 18+，fetch + SSE，无依赖）

```js
const URL = "https://<bridge-host>/agents/<agent>";
const H = { "Authorization": "Bearer <API_KEY>", "Content-Type": "application/json", "Accept": "text/event-stream" };

async function stream(message) {
  const res = await fetch(URL, { method: "POST", headers: H,
    body: JSON.stringify({ jsonrpc: "2.0", id: crypto.randomUUID(), method: "message/stream", params: { message } }) });
  if (!res.headers.get("content-type")?.startsWith("text/event-stream")) throw new Error(await res.text());
  const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = "", last;
  for (;;) {
    const { value, done } = await reader.read(); if (done) break;
    buf += dec.decode(value, { stream: true });
    let i; while ((i = buf.indexOf("\n\n")) >= 0) {
      const chunk = buf.slice(0, i); buf = buf.slice(i + 2);
      for (const line of chunk.split("\n")) if (line.startsWith("data:")) {
        const msg = JSON.parse(line.slice(5));
        if (msg.error) throw new Error(JSON.stringify(msg.error));
        last = msg.result; console.log(last.kind, last.status?.state ?? "", last.artifact?.name ?? "");
      }
    }
  }
  return last;
}

const first = await stream({ kind: "message", messageId: crypto.randomUUID(), role: "user",
  parts: [{ kind: "text", text: "repo_url: https://github.com/org/repo goal: ..." }] });
if (first.status?.state === "input-required") {
  await stream({ kind: "message", messageId: crypto.randomUUID(), role: "user",
    taskId: first.taskId ?? first.id, contextId: first.contextId, parts: [{ kind: "text", text: "branch: main" }] });
}
```
也可使用官方 JS SDK `@a2a-js/sdk`，用 Agent Card URL 初始化，并在其 fetch 实现上加 `Authorization` 头。

## 9. Roster（谁有空）

同一 host、同一 API Key。状态由 bridge 根据任务与投递记录推断，**不会**探测 bot webhook。Agent Card 里只有指针（`urn:a2a-bridge:roster:v1`），没有实时状态。

```bash
curl -s https://<bridge-host>/roster -H "Authorization: Bearer $A2A_KEY"
curl -s https://<bridge-host>/roster/<agent> -H "Authorization: Bearer $A2A_KEY"
curl -N https://<bridge-host>/roster/stream -H "Authorization: Bearer $A2A_KEY"
```

- 每个 key 只能看到自己被允许调用的 agent；无权或不存在的单条查询一律 404。
- 查询参数（可选）：`agent=<name>`（可重复）、`status=idle,busy,input-required,stuck,offline`、`include=tasks`（仅 owner key）。
- 响应 `Cache-Control: no-store`，带 `ETag`；`If-None-Match` 可 304。
- SSE：首帧 `event: snapshot`，变化时 `event: update`；`: keepalive` 与任务流相同。每 key 默认最多 3 条 roster 流（`A2A_ROSTER_MAX_STREAMS_PER_KEY`）。
- 字段多少取决于 key 的隐私等级（`A2A_KEY_TIERS`，默认 `basic`）：basic 只有粗粒度 `availability`（available / limited / unavailable）；`trusted` 有 status / activity / capacity；`owner` 另有投递错误与（可选）任务列表。详见 [roster-design.md](roster-design.md)。

## 10. 错误码

| code | 含义 |
|---|---|
| -32700 | JSON 解析失败 |
| -32600 | 非法 JSON-RPC 请求（不支持 batch） |
| -32601 | 方法不存在 |
| -32602 | 参数错误（如 parts 为空、contextId 与 task 不匹配、推送 URL 不合法） |
| -32603 | 内部错误 |
| -32001 | TaskNotFound（不存在或不属于你的 Key） |
| -32002 | TaskNotCancelable（已是终态） |
| -32003 | PushNotificationNotSupported |
| -32004 | UnsupportedOperation（例如向终态任务发消息；v1 订阅终态任务） |
| -32005 | ContentTypeNotSupported |
| -32009 | VersionNotSupported（`A2A-Version` 只接受空、`0.3`、`1.0`） |
| HTTP 401/403/404/413/429 | 未认证 / 无权限 / roster 中 agent 不存在或不在 key 范围 / 请求体过大 / 限流 |

## 11. 任务状态

`submitted`（已收到，排队投递）→ `working`（处理中，可能多次带进度消息）→ `input-required`（需要你回答）/ `completed` / `failed` / `canceled` / `rejected`。超过 1 小时无进展的任务会自动 `failed`；等待输入超过 24 小时会自动 `canceled`（运营方可调整）。
