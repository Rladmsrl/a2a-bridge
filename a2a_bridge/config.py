"""Configuration: everything comes from environment variables (optionally a .env file)
plus a JSON agents file that maps agent names to bot webhooks. No secrets in the repo."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    pass


def _load_dotenv(path: str) -> None:
    p = Path(path)
    if not p.is_file():
        return
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
            v = v[1:-1]
        os.environ.setdefault(k, v)


def _int(env: dict, name: str, default: int) -> int:
    v = env.get(name)
    try:
        return int(v) if v not in (None, "") else default
    except ValueError as e:
        raise ConfigError(f"{name} must be an integer") from e


def _float(env: dict, name: str, default: float) -> float:
    v = env.get(name)
    try:
        return float(v) if v not in (None, "") else default
    except ValueError as e:
        raise ConfigError(f"{name} must be a number") from e


def _bool(env: dict, name: str, default: bool) -> bool:
    v = env.get(name)
    if v in (None, ""):
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


KEY_TIERS = ("basic", "trusted", "owner")


@dataclass
class ApiKey:
    label: str
    sha256: str
    agents: set[str] | None  # None = all agents
    tier: str = "basic"


@dataclass
class AgentConfig:
    name: str
    display_name: str
    description: str
    version: str
    webhook_url: str
    webhook_token: str
    shared_secret: str
    input_modes: list[str]
    output_modes: list[str]
    skills: list[dict[str, Any]]
    required_inputs: list[dict[str, Any]]
    provider: dict[str, Any] | None
    documentation_url: str | None
    webhook_auth_header: str = "Authorization"
    webhook_auth_scheme: str = "Bearer"
    max_concurrent: int = 1
    stuck_after_s: int | None = None
    roster_visible: bool = True
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class Settings:
    public_base_url: str
    db_path: str
    api_keys: list[ApiKey]
    agents: dict[str, AgentConfig]
    default_agent: str
    max_request_bytes: int = 10 * 1024 * 1024
    rate_limit_per_min: int = 120
    rate_limit_burst: int = 60
    task_timeout_s: int = 3600
    input_required_timeout_s: int = 86400
    retention_days: int = 30
    webhook_timeout_s: float = 15.0
    webhook_max_attempts: int = 8
    webhook_backoff_base_s: float = 2.0
    webhook_backoff_max_s: float = 300.0
    webhook_4xx_retries: int = 1
    webhook_4xx_retry_delay_s: float = 10.0
    sse_heartbeat_s: float = 15.0
    blocking_wait_s: float = 30.0
    callback_max_skew_s: int = 300
    push_allow_private: bool = False
    push_allow_http: bool = False
    trust_proxy_headers: bool = True
    sweep_interval_s: float = 30.0
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 8080
    roster_stuck_after_s: int = 900
    roster_offline_after_s: int = 120
    roster_max_streams_per_key: int = 3
    roster_max_streams: int = 32
    roster_max_anon_streams: int = 3
    roster_stream_timeout_s: float = 30.0
    roster_stream_max_s: float = 300.0
    roster_public: bool = False
    roster_redact: list[str] = field(default_factory=list)
    roster_debounce_s: float = 1.0
    roster_debounce_max_s: float = 2.0
    roster_offline_ttl_s: int = 3600


def parse_api_keys(spec: str) -> list[ApiKey]:
    """A2A_API_KEYS: comma separated entries.
    label=sha256:<64 hex>[@agent1|agent2]   (recommended: only the hash is stored)
    label=plain:<secret>[@agent1|agent2]    (hashed at startup; convenient for dev)
    """
    keys: list[ApiKey] = []
    for entry in [e.strip() for e in spec.split(",") if e.strip()]:
        if "=" not in entry:
            raise ConfigError("A2A_API_KEYS entry must be label=sha256:<hex> or label=plain:<key>")
        label, rest = entry.split("=", 1)
        label = label.strip()
        if label == "*":
            raise ConfigError("API key label '*' is reserved")
        agents = None
        if "@" in rest:
            rest, a = rest.rsplit("@", 1)
            agents = {x.strip() for x in a.split("|") if x.strip()}
        if rest.startswith("sha256:"):
            h = rest[7:].lower()
            if len(h) != 64 or any(c not in "0123456789abcdef" for c in h):
                raise ConfigError(f"API key '{label}': invalid sha256 hex")
        elif rest.startswith("plain:"):
            if len(rest) - 6 < 16:
                raise ConfigError(f"API key '{label}': plain keys must be >= 16 chars")
            h = sha256_hex(rest[6:])
        else:
            raise ConfigError(f"API key '{label}': value must start with sha256: or plain:")
        keys.append(ApiKey(label=label, sha256=h, agents=agents))
    return keys


def parse_key_tiers(spec: str) -> dict[str, str]:
    """A2A_KEY_TIERS: comma separated label:tier entries (basic|trusted|owner).
    Labels not listed default to basic. Does not change A2A_API_KEYS syntax.
    """
    out: dict[str, str] = {}
    for entry in [e.strip() for e in spec.split(",") if e.strip()]:
        if ":" not in entry:
            raise ConfigError("A2A_KEY_TIERS entry must be label:tier")
        label, tier = entry.split(":", 1)
        label, tier = label.strip(), tier.strip().lower()
        if not label or tier not in KEY_TIERS:
            raise ConfigError(
                f"A2A_KEY_TIERS: unknown tier {tier!r} for {label!r} (use basic|trusted|owner)")
        out[label] = tier
    return out


def load_agents(path: str, env: dict) -> dict[str, AgentConfig]:
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"agents config not found: {path} (set A2A_AGENTS_CONFIG)")
    data = json.loads(p.read_text(encoding="utf-8"))
    items = data.get("agents", data) if isinstance(data, dict) else data
    if not isinstance(items, list) or not items:
        raise ConfigError("agents config must contain a non-empty 'agents' list")
    agents: dict[str, AgentConfig] = {}
    for a in items:
        name = a.get("name", "")
        if not name or not all(c.isalnum() or c in "-_" for c in name):
            raise ConfigError(f"invalid agent name: {name!r} (use [A-Za-z0-9_-])")
        if name == "stream":
            raise ConfigError("agent name 'stream' is reserved (GET /roster/stream)")
        tok_env = a.get("webhook_token_env")
        sec_env = a.get("shared_secret_env")
        if not tok_env or not sec_env:
            raise ConfigError(f"agent {name}: webhook_token_env and shared_secret_env are required")
        token = env.get(tok_env, "")
        secret = env.get(sec_env, "")
        if not token:
            raise ConfigError(f"agent {name}: env {tok_env} (outbound webhook bearer) is not set")
        if len(secret) < 32:
            raise ConfigError(f"agent {name}: env {sec_env} (shared secret) must be set, >= 32 chars")
        url = env.get(a["webhook_url_env"], "") if a.get("webhook_url_env") else a.get("webhook_url", "")
        if not url.startswith(("http://", "https://")):
            raise ConfigError(f"agent {name}: webhook_url missing/invalid")
        max_c = a.get("max_concurrent", 1)
        try:
            max_c = int(max_c)
        except (TypeError, ValueError) as e:
            raise ConfigError(f"agent {name}: max_concurrent must be an integer") from e
        if max_c < 1:
            raise ConfigError(f"agent {name}: max_concurrent must be >= 1")
        stuck = a.get("stuck_after_s")
        if stuck is not None and stuck != "":
            try:
                stuck = int(stuck)
            except (TypeError, ValueError) as e:
                raise ConfigError(f"agent {name}: stuck_after_s must be an integer") from e
            if stuck < 1:
                raise ConfigError(f"agent {name}: stuck_after_s must be >= 1")
        else:
            stuck = None
        vis = a.get("roster_visible", True)
        if isinstance(vis, str):
            vis = vis.strip().lower() in ("1", "true", "yes", "on")
        else:
            vis = bool(vis)
        agents[name] = AgentConfig(
            name=name,
            display_name=a.get("display_name", name),
            description=a.get("description", ""),
            version=a.get("version", "1.0.0"),
            webhook_url=url,
            webhook_token=token,
            shared_secret=secret,
            input_modes=a.get("input_modes", ["text/plain", "application/json", "*/*"]),
            output_modes=a.get("output_modes", ["text/plain", "application/json", "text/markdown"]),
            skills=a.get("skills", []),
            required_inputs=a.get("required_inputs", []),
            provider=a.get("provider"),
            documentation_url=a.get("documentation_url"),
            webhook_auth_header=a.get("webhook_auth_header", "Authorization"),
            webhook_auth_scheme=a.get("webhook_auth_scheme", "Bearer"),
            max_concurrent=max_c,
            stuck_after_s=stuck,
            roster_visible=vis,
            raw=a,
        )
    return agents


def load_settings(env: dict | None = None) -> Settings:
    if env is None:
        _load_dotenv(os.environ.get("A2A_ENV_FILE", ".env"))
        env = dict(os.environ)
    base = env.get("A2A_PUBLIC_BASE_URL", "").rstrip("/")
    if not base.startswith(("http://", "https://")):
        raise ConfigError("A2A_PUBLIC_BASE_URL must be set, e.g. https://a2a.example.com")
    keys = parse_api_keys(env.get("A2A_API_KEYS", ""))
    if not keys:
        raise ConfigError("A2A_API_KEYS must define at least one inbound API key")
    tiers = parse_key_tiers(env.get("A2A_KEY_TIERS", ""))
    for k in keys:
        k.tier = tiers.get(k.label, "basic")
    agents = load_agents(env.get("A2A_AGENTS_CONFIG", "config/agents.json"), env)
    default = env.get("A2A_DEFAULT_AGENT") or next(iter(agents))
    if default not in agents:
        raise ConfigError(f"A2A_DEFAULT_AGENT={default} not in agents config")
    task_timeout_s = _int(env, "A2A_TASK_TIMEOUT_S", 3600)
    for ag in agents.values():
        if ag.stuck_after_s is not None and ag.stuck_after_s >= task_timeout_s:
            raise ConfigError(
                f"agent {ag.name}: stuck_after_s ({ag.stuck_after_s}) must be < A2A_TASK_TIMEOUT_S ({task_timeout_s})")
    redact = [x.strip() for x in (env.get("A2A_ROSTER_REDACT") or "").split(",") if x.strip()]
    return Settings(
        public_base_url=base,
        db_path=env.get("A2A_DB_PATH", "data/bridge.db"),
        api_keys=keys,
        agents=agents,
        default_agent=default,
        max_request_bytes=_int(env, "A2A_MAX_REQUEST_BYTES", 10 * 1024 * 1024),
        rate_limit_per_min=_int(env, "A2A_RATE_LIMIT_PER_MIN", 120),
        rate_limit_burst=_int(env, "A2A_RATE_LIMIT_BURST", 60),
        task_timeout_s=task_timeout_s,
        input_required_timeout_s=_int(env, "A2A_INPUT_REQUIRED_TIMEOUT_S", 86400),
        retention_days=_int(env, "A2A_RETENTION_DAYS", 30),
        webhook_timeout_s=_float(env, "A2A_WEBHOOK_TIMEOUT_S", 15.0),
        webhook_max_attempts=_int(env, "A2A_WEBHOOK_MAX_ATTEMPTS", 8),
        webhook_backoff_base_s=_float(env, "A2A_WEBHOOK_BACKOFF_BASE_S", 2.0),
        webhook_backoff_max_s=_float(env, "A2A_WEBHOOK_BACKOFF_MAX_S", 300.0),
        webhook_4xx_retries=_int(env, "A2A_WEBHOOK_4XX_RETRIES", 1),
        webhook_4xx_retry_delay_s=_float(env, "A2A_WEBHOOK_4XX_RETRY_DELAY_S", 10.0),
        sse_heartbeat_s=_float(env, "A2A_SSE_HEARTBEAT_S", 15.0),
        blocking_wait_s=_float(env, "A2A_BLOCKING_WAIT_S", 30.0),
        callback_max_skew_s=_int(env, "A2A_CALLBACK_MAX_SKEW_S", 300),
        push_allow_private=_bool(env, "A2A_PUSH_ALLOW_PRIVATE", False),
        push_allow_http=_bool(env, "A2A_PUSH_ALLOW_HTTP", False),
        trust_proxy_headers=_bool(env, "A2A_TRUST_PROXY_HEADERS", True),
        sweep_interval_s=_float(env, "A2A_SWEEP_INTERVAL_S", 30.0),
        log_level=env.get("A2A_LOG_LEVEL", "INFO").upper(),
        host=env.get("A2A_HOST", "0.0.0.0"),
        port=_int(env, "A2A_PORT", 8080),
        roster_stuck_after_s=_int(env, "A2A_ROSTER_STUCK_AFTER_S", 900),
        roster_offline_after_s=_int(env, "A2A_ROSTER_OFFLINE_AFTER_S", 120),
        roster_max_streams_per_key=_int(env, "A2A_ROSTER_MAX_STREAMS_PER_KEY", 3),
        roster_max_streams=_int(env, "A2A_ROSTER_MAX_STREAMS", 32),
        roster_max_anon_streams=_int(env, "A2A_ROSTER_MAX_ANON_STREAMS", 3),
        roster_stream_timeout_s=_float(env, "A2A_ROSTER_STREAM_TIMEOUT_S", 30.0),
        roster_stream_max_s=_float(env, "A2A_ROSTER_STREAM_MAX_S", 300.0),
        roster_public=_bool(env, "A2A_ROSTER_PUBLIC", False),
        roster_redact=redact,
        roster_debounce_s=_float(env, "A2A_ROSTER_DEBOUNCE_S", 1.0),
        roster_debounce_max_s=_float(env, "A2A_ROSTER_DEBOUNCE_MAX_S", 2.0),
        roster_offline_ttl_s=_int(env, "A2A_ROSTER_OFFLINE_TTL_S", 3600),
    )
