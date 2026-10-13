#!/usr/bin/env bash
# One-click deploy: generates secrets + an API key, writes .env and config/agents.json,
# then starts the bridge and Caddy (HTTPS) with docker compose.
#
#   ./scripts/install.sh --domain a2a.example.com --webhook-url https://bot.example.com/hook \
#       --webhook-token '<sender key of your bot webhook>' [--name gateway] [--display-name "My A2A Gateway"] [--no-start]
#
# The webhook token can also be passed via env BOT_WEBHOOK_TOKEN (avoids shell history).
set -euo pipefail
cd "$(dirname "$0")/.."

DOMAIN="" WEBHOOK_URL="" TOKEN="${BOT_WEBHOOK_TOKEN:-}" NAME="gateway" DISPLAY="My A2A Gateway" START=1
while [ $# -gt 0 ]; do
  case "$1" in
    --domain) DOMAIN="$2"; shift 2;;
    --webhook-url) WEBHOOK_URL="$2"; shift 2;;
    --webhook-token) TOKEN="$2"; shift 2;;
    --name) NAME="$2"; shift 2;;
    --display-name) DISPLAY="$2"; shift 2;;
    --no-start) START=0; shift;;
    -h|--help) sed -n 2,10p "$0"; exit 0;;
    *) echo "unknown option $1"; exit 1;;
  esac
done
[ -n "$DOMAIN" ] && [ -n "$WEBHOOK_URL" ] && [ -n "$TOKEN" ] || { echo "need --domain, --webhook-url, --webhook-token"; exit 1; }
[[ "$NAME" =~ ^[A-Za-z0-9_-]+$ ]] || { echo "invalid --name"; exit 1; }
if [ -e .env ]; then echo ".env already exists - refusing to overwrite (delete it to reinstall)"; exit 1; fi
command -v python3 >/dev/null || { echo "python3 required"; exit 1; }

UP=$(echo "$NAME" | tr 'a-z-' 'A-Z_')
read -r API_KEY API_HASH SECRET < <(python3 - <<'PY'
import secrets, hashlib
k = "a2a_" + secrets.token_urlsafe(32)
print(k, hashlib.sha256(k.encode()).hexdigest(), secrets.token_urlsafe(48))
PY
)
umask 077
cat > .env <<ENV
A2A_DOMAIN=$DOMAIN
A2A_PUBLIC_BASE_URL=https://$DOMAIN
A2A_API_KEYS=client1=sha256:$API_HASH
A2A_AGENTS_CONFIG=/app/config/agents.json
A2A_DEFAULT_AGENT=$NAME
A2A_DB_PATH=/app/data/bridge.db
A2A_TRUST_PROXY_HEADERS=true
BOT_${UP}_WEBHOOK_URL=$WEBHOOK_URL
BOT_${UP}_WEBHOOK_TOKEN=$TOKEN
BOT_${UP}_SHARED_SECRET=$SECRET
ENV
printf '%s\n' "$API_KEY" > api-key.txt
umask 022
if [ ! -e config/agents.json ]; then
  NAME="$NAME" DISPLAY="$DISPLAY" UP="$UP" python3 - <<'PY'
import json, os
n, up = os.environ["NAME"], os.environ["UP"]
json.dump({"agents": [{
  "name": n, "display_name": os.environ["DISPLAY"],
  "description": "A general-purpose A2A entrypoint. Send any task as plain text; the agent may ask follow-up questions and returns results as artifacts.",
  "webhook_url_env": f"BOT_{up}_WEBHOOK_URL", "webhook_token_env": f"BOT_{up}_WEBHOOK_TOKEN",
  "shared_secret_env": f"BOT_{up}_SHARED_SECRET",
  "skills": [{"id": "general-task", "name": "General task", "tags": ["general"],
              "description": "Any task described in natural language."}]}]},
  open("config/agents.json", "w"), indent=2)
PY
fi
echo "Wrote .env (mode 600), config/agents.json, api-key.txt (mode 600: give this key to A2A clients)."
echo "Bot shared secret (for verifying X-A2A-Signature) is BOT_${UP}_SHARED_SECRET in .env."
if [ "$START" = 1 ]; then
  docker compose up -d --build
  docker compose --profile caddy up -d
  echo "Waiting for health..."; for i in $(seq 30); do curl -sf http://127.0.0.1:8080/healthz >/dev/null && break; sleep 1; done
  curl -s http://127.0.0.1:8080/healthz; echo
  echo "Agent card: https://$DOMAIN/.well-known/agent-card.json  (open firewall port 443)"
fi
