# Skill: Talk to an A2A agent through a2a-bridge

Use this skill to delegate a task to a remote agent exposed by an a2a-bridge server.

## Inputs
- `A2A_URL`: the agent endpoint, e.g. `https://a2a.example.com/agents/gateway` (Agent Card: `$A2A_URL/.well-known/agent-card.json`; the default agent also at `https://a2a.example.com/.well-known/agent-card.json`).
- `A2A_KEY`: API key, **read from the environment**. Never print, log or paste it into messages.

## Steps
1. (Optional) Read the Agent Card to learn skills and expected input fields:
   `curl -s "$A2A_URL/.well-known/agent-card.json"`
2. Send the task (non-blocking) and remember `result.id` (taskId) and `result.contextId`:
   ```bash
   curl -s "$A2A_URL" -H "Authorization: Bearer $A2A_KEY" -H 'Content-Type: application/json' -d '{
     "jsonrpc":"2.0","id":"1","method":"message/send","params":{"message":{
       "kind":"message","messageId":"'"$(uuidgen)"'","role":"user",
       "parts":[{"kind":"text","text":"<clear task: goal, context, how success is judged>"}]}}}'
   ```
   Or stream live updates with `"method":"message/stream"` and `curl -N` (SSE; ignore `: keepalive` lines).
3. Poll every 5–15 s (back off for long tasks):
   ```bash
   curl -s "$A2A_URL" -H "Authorization: Bearer $A2A_KEY" -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"2","method":"tasks/get","params":{"id":"<taskId>","historyLength":5}}'
   ```
4. Act on `result.status.state`:
   - `submitted` / `working`: keep waiting (`status.message` may contain progress).
   - `input-required`: read the question in `status.message.parts[].text`, then answer with the **same** `taskId` and `contextId`:
     `{"jsonrpc":"2.0","id":"3","method":"message/send","params":{"message":{"kind":"message","messageId":"<new uuid>","role":"user","taskId":"<taskId>","contextId":"<contextId>","parts":[{"kind":"text","text":"<answer>"}]}}}`
   - `completed`: results are in `result.artifacts[].parts[]` (`text`, `data`, or `file`).
   - `failed` / `rejected` / `canceled`: report `status.message` to the user.
5. To start a related new task keep the `contextId` and omit `taskId`. To stop: `tasks/cancel` with `{"id":"<taskId>"}`.

## Errors
HTTP 401/403 = bad key / no access; 429 = slow down; JSON-RPC `-32001` task not found, `-32004` task already finished (start a new one), `-32602` invalid params.
