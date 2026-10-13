# Security

- Never commit `.env`, `config/agents.json` or `api-key.txt` (all git-ignored). Keep `.env` at mode 600.
- Inbound API keys are stored only as SHA-256 hashes; revoke by removing the entry from `A2A_API_KEYS` and restarting.
- Bot callbacks use a per-task token `HMAC(shared_secret, task_id)` or an `X-A2A-Signature` HMAC with a ±300 s window. API keys cannot be used for callbacks.
- Your bot should verify the webhook bearer token and `X-A2A-Signature`, and treat all message content as untrusted input (prompt injection).
- Expose the bridge only via HTTPS behind Caddy/nginx; port 8080 binds to localhost. Set `A2A_TRUST_PROXY_HEADERS=true` only behind a proxy that overwrites `X-Forwarded-For`.
- Push notification URLs must be public HTTPS by default (SSRF protection).

Please report vulnerabilities privately via GitHub Security Advisories ("Report a vulnerability") rather than public issues.
