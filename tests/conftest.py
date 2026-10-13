import json
import socket
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from a2a_bridge.app import create_app  # noqa: E402
from a2a_bridge.config import load_settings, sha256_hex  # noqa: E402
from mock_bot import make_app as make_bot  # noqa: E402

API_KEY = "test-key-0123456789abcdef"  # gitleaks:allow (test fixture)
LIMITED_KEY = "limited-key-0123456789abc"  # gitleaks:allow (test fixture)
BOT_TOKEN = "bot-token-xyz"
BOT_SECRET = "s" * 40


def free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


class Server:
    def __init__(self, app, port):
        self.port = port
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_config=None,
                                                    lifespan="on"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        for _ in range(200):
            if self.server.started:
                return self
            time.sleep(0.02)
        raise RuntimeError("server did not start")

    def __exit__(self, *a):
        self.server.should_exit = True
        self.thread.join(5)


def make_env(tmp_path, bot_port, bridge_port, **extra):
    agents = {"agents": [{
        "name": "coder", "display_name": "Coder", "description": "codes",
        "webhook_url": f"http://127.0.0.1:{bot_port}/hook",
        "webhook_token_env": "BOT_TOKEN", "shared_secret_env": "BOT_SECRET",
        "required_inputs": [{"name": "repo_url", "required": True, "description": "git url"}],
        "skills": [{"id": "code", "name": "Code", "description": "code it", "tags": ["code"]}]},
        {"name": "other", "description": "other bot", "webhook_url": f"http://127.0.0.1:{bot_port}/hook",
         "webhook_token_env": "BOT_TOKEN", "shared_secret_env": "BOT_SECRET"}]}
    cfg = tmp_path / "agents.json"
    cfg.write_text(json.dumps(agents))
    env = {
        "A2A_PUBLIC_BASE_URL": f"http://127.0.0.1:{bridge_port}",
        "A2A_API_KEYS": f"main=sha256:{sha256_hex(API_KEY)},lim=plain:{LIMITED_KEY}@other",
        "A2A_AGENTS_CONFIG": str(cfg), "A2A_DB_PATH": str(tmp_path / "bridge.db"),
        "BOT_TOKEN": BOT_TOKEN, "BOT_SECRET": BOT_SECRET,
        "A2A_SSE_HEARTBEAT_S": "0.5", "A2A_WEBHOOK_BACKOFF_BASE_S": "0.1", "A2A_WEBHOOK_MAX_ATTEMPTS": "3",
        "A2A_BLOCKING_WAIT_S": "10", "A2A_RATE_LIMIT_PER_MIN": "100000", "A2A_RATE_LIMIT_BURST": "100000",
        "A2A_LOG_LEVEL": "WARNING", "A2A_SWEEP_INTERVAL_S": "3600",
    }
    env.update(extra)
    return env


@pytest.fixture
def stack(tmp_path):
    bot_port, bridge_port = free_port(), free_port()
    bot = make_bot(BOT_TOKEN, BOT_SECRET)
    env = make_env(tmp_path, bot_port, bridge_port)
    app = create_app(load_settings(env))
    with Server(bot, bot_port), Server(app, bridge_port):
        yield {"base": f"http://127.0.0.1:{bridge_port}", "bot": bot, "app": app, "env": env,
               "tmp": tmp_path, "bridge_port": bridge_port, "bot_port": bot_port}


@pytest.fixture
def client(stack):
    with httpx.Client(base_url=stack["base"], headers={"Authorization": f"Bearer {API_KEY}"}, timeout=20) as c:
        yield c


def rpc(c, method, params, path="/agents/coder", **kw):
    r = c.post(path, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, **kw)
    return r


def user_msg(text, task=None, ctx=None):
    m = {"kind": "message", "messageId": f"m-{time.time_ns()}", "role": "user",
         "parts": [{"kind": "text", "text": text}]}
    if task:
        m["taskId"] = task
    if ctx:
        m["contextId"] = ctx
    return m


def wait_state(c, task_id, states, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        t = rpc(c, "tasks/get", {"id": task_id}).json()["result"]
        if t["status"]["state"] in states:
            return t
        time.sleep(0.05)
    raise AssertionError(f"task never reached {states}: {t['status']}")


def read_sse(c, method, params, path="/agents/coder", headers=None, max_events=50):
    events = []
    with c.stream("POST", path, json={"jsonrpc": "2.0", "id": 7, "method": method, "params": params},
                  headers=headers or {}) as r:
        assert r.status_code == 200
        ctype = r.headers["content-type"]
        if ctype.startswith("application/json"):
            return [json.loads(r.read())]
        assert ctype.startswith("text/event-stream")
        for line in r.iter_lines():
            if line.startswith(":"):
                events.append({"heartbeat": True})
            if line.startswith("data:"):
                events.append(json.loads(line[5:]))
            if len(events) >= max_events:
                break
    return events
