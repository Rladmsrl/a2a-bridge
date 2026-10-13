import httpx

from conftest import API_KEY, BOT_SECRET, LIMITED_KEY, Server, free_port, make_env, rpc, user_msg
from a2a_bridge.app import create_app
from a2a_bridge.config import ConfigError, load_settings
from a2a_bridge.security import callback_token
import pytest


def test_inbound_auth_failures(stack):
    base = stack["base"]
    with httpx.Client(base_url=base) as c:
        r = rpc(c, "message/send", {"message": user_msg("x")})
        assert r.status_code == 401 and "Bearer" in r.headers["www-authenticate"]
        r = rpc(c, "message/send", {"message": user_msg("x")}, headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401
        # key limited to agent "other"
        r = rpc(c, "message/send", {"message": user_msg("x")}, headers={"Authorization": f"Bearer {LIMITED_KEY}"})
        assert r.status_code == 403
        r = rpc(c, "message/send", {"message": user_msg("x")}, path="/agents/other",
                headers={"Authorization": f"Bearer {LIMITED_KEY}"})
        assert r.status_code == 200 and "result" in r.json()
        # tasks of one key are invisible to another key
        tid = r.json()["result"]["id"]
        r = rpc(c, "tasks/get", {"id": tid}, path="/agents/other", headers={"Authorization": f"Bearer {API_KEY}"})
        assert r.json()["error"]["code"] == -32001


def test_callback_auth_failures(stack, client):
    tid = rpc(client, "message/send", {"message": user_msg("x")}).json()["result"]["id"]
    url = f"{stack['base']}/callback/{tid}"
    assert httpx.post(url, json={"state": "completed"}).status_code == 401
    assert httpx.post(url, json={"state": "completed"}, headers={"Authorization": "Bearer cbt_bad"}).status_code == 401
    other = rpc(client, "message/send", {"message": user_msg("y")}).json()["result"]["id"]
    # token of another task does not work
    assert httpx.post(url, json={"state": "completed"},
                      headers={"Authorization": f"Bearer {callback_token(BOT_SECRET, other)}"}).status_code == 401
    assert httpx.post(url, json={"state": "completed"}, headers={"X-A2A-Signature": "t=1,v1=00"}).status_code == 401
    assert httpx.post(f"{stack['base']}/callback/nope", json={}).status_code == 401
    # API key is not accepted as a callback credential
    assert httpx.post(url, json={"state": "completed"},
                      headers={"Authorization": f"Bearer {API_KEY}"}).status_code == 401


def test_jsonrpc_errors(stack, client):
    assert client.post("/agents/coder", content=b"{bad").json()["error"]["code"] == -32700
    assert client.post("/agents/coder", json=[1]).json()["error"]["code"] == -32600
    assert rpc(client, "nope/method", {}).json()["error"]["code"] == -32601
    assert rpc(client, "message/send", {"message": {"role": "user", "parts": []}}).json()["error"]["code"] == -32602
    assert rpc(client, "tasks/get", {"id": "missing"}).json()["error"]["code"] == -32001
    assert rpc(client, "message/send", {"message": user_msg("x", task="missing")}).json()["error"]["code"] == -32001


def test_body_limit_and_rate_limit(tmp_path):
    port = free_port()
    env = make_env(tmp_path, free_port(), port, A2A_MAX_REQUEST_BYTES="1000",
                   A2A_RATE_LIMIT_PER_MIN="60", A2A_RATE_LIMIT_BURST="3")
    with Server(create_app(load_settings(env)), port):
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", headers={"Authorization": f"Bearer {API_KEY}"}) as c:
            r = rpc(c, "message/send", {"message": user_msg("x" * 5000)})
            assert r.status_code == 413
            codes = [rpc(c, "tasks/get", {"id": "a"}).status_code for _ in range(5)]
            assert 429 in codes


def test_config_validation(tmp_path):
    env = make_env(tmp_path, 1, 2)
    with pytest.raises(ConfigError):
        load_settings({**env, "A2A_API_KEYS": ""})
    with pytest.raises(ConfigError):
        load_settings({**env, "BOT_SECRET": "short"})
    with pytest.raises(ConfigError):
        load_settings({**env, "A2A_API_KEYS": "x=plain:short"})
