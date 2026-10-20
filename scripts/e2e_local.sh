#!/usr/bin/env bash
# End-to-end local run: real bridge process + mock bot process + CLI client.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
WORK=$(mktemp -d)
export MOCK_BOT_TOKEN=$($PY scripts/gen_secret.py secret) MOCK_BOT_SECRET=$($PY scripts/gen_secret.py secret)
KEY="e2e-$($PY scripts/gen_secret.py secret)"
cat > "$WORK/agents.json" <<JSON
{"agents":[{"name":"coder","display_name":"E2E Coder","description":"e2e","webhook_url":"http://127.0.0.1:19100/hook",
 "webhook_token_env":"MOCK_BOT_TOKEN","shared_secret_env":"MOCK_BOT_SECRET",
 "required_inputs":[{"name":"repo_url","required":true,"description":"git url"}]}]}
JSON
export A2A_ENV_FILE=/nonexistent A2A_PUBLIC_BASE_URL=http://127.0.0.1:18080 A2A_PORT=18080 A2A_HOST=127.0.0.1 \
  A2A_API_KEYS="e2e=sha256:$($PY scripts/gen_secret.py hash "$KEY")" A2A_AGENTS_CONFIG="$WORK/agents.json" \
  A2A_DB_PATH="$WORK/bridge.db" A2A_SSE_HEARTBEAT_S=1
$PY scripts/mock_bot.py --port 19100 > "$WORK/bot.log" 2>&1 & BOT=$!
$PY -m a2a_bridge > "$WORK/bridge.log" 2>&1 & BR=$!
trap 'kill $BOT $BR 2>/dev/null; echo "logs: $WORK"' EXIT
for i in $(seq 50); do curl -sf http://127.0.0.1:18080/healthz >/dev/null && break; sleep 0.2; done
export A2A_URL=http://127.0.0.1:18080/agents/coder A2A_KEY="$KEY"
echo "== healthz";  curl -s http://127.0.0.1:18080/healthz; echo
echo "== card";     $PY scripts/a2a_cli.py card | head -5
echo "== roster unauthorized (expect 401)"
code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18080/roster)
test "$code" = "401"
echo "== roster"
curl -sf -H "Authorization: Bearer $A2A_KEY" http://127.0.0.1:18080/roster | $PY -c '
import json,sys
d=json.load(sys.stdin)
assert d["viewTier"]=="basic" and d.get("computedAt") and d.get("stale") is False
a=d["agents"][0]
assert a["id"]=="coder" and "availability" in a and "status" not in a
print("roster OK", a["availability"])
'
echo "== roster status filter forbidden for basic (expect 403)"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $A2A_KEY" "http://127.0.0.1:18080/roster?status=idle")
test "$code" = "403"
echo "== card roster extension"
curl -sf http://127.0.0.1:18080/agents/coder/.well-known/agent-card.json | $PY -c '
import json,sys
exts=json.load(sys.stdin)["capabilities"]["extensions"]
assert any(e.get("uri")=="urn:a2a-bridge:roster:v1" for e in exts)
print("card roster OK")
'
echo "== unauthorized (expect 401)"; curl -s -o /dev/null -w '%{http_code}\n' -X POST "$A2A_URL" -d '{}'
echo "== stream turn 1 (expect input-required)"
OUT=$($PY scripts/a2a_cli.py stream "repo_url: https://github.com/x/y goal: demo")
echo "$OUT"
TID=$(echo "$OUT" | head -1 | $PY -c 'import sys,json; print(json.load(sys.stdin)["result"]["id"])')
CTX=$(echo "$OUT" | head -1 | $PY -c 'import sys,json; print(json.load(sys.stdin)["result"]["contextId"])')
echo "$OUT" | tail -1 | grep -q '"input-required"'
echo "== stream turn 2 follow-up (expect completed)"
OUT2=$($PY scripts/a2a_cli.py stream "branch: main" --task "$TID" --context "$CTX")
echo "$OUT2"
echo "$OUT2" | tail -1 | grep -q '"completed"'
echo "== tasks/get"
$PY scripts/a2a_cli.py get "$TID" | grep -q '"state": "completed"' && echo "completed OK"
echo "== v1 SendMessage (blocking)"
$PY scripts/a2a_cli.py send --v1 --wait "branch: dev repo_url: x" | grep -q TASK_STATE_COMPLETED && echo "v1 OK"
if $PY -c "import a2a.client" 2>/dev/null; then echo "== official a2a-sdk client"; A2A_BASE=$A2A_URL $PY scripts/sdk_example.py; fi
echo "E2E PASSED"
