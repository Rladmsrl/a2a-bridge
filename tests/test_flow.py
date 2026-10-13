import json
import threading
import time

import httpx

from conftest import API_KEY, BOT_SECRET, BOT_TOKEN, Server, free_port, make_env, read_sse, rpc, user_msg, \
    wait_state
from a2a_bridge.app import create_app
from a2a_bridge.config import load_settings
from a2a_bridge.security import callback_token, sign


def test_agent_cards(stack):
    with httpx.Client(base_url=stack["base"]) as c:
        for p in ("/.well-known/agent-card.json", "/.well-known/agent.json",
                  "/agents/coder/.well-known/agent-card.json"):
            card = c.get(p).json()
            assert card["name"] == "Coder"
            assert card["url"].endswith("/agents/coder")
            assert card["capabilities"]["streaming"] is True
            assert "repo_url" in card["description"]
            assert card["securitySchemes"]["bearer"]["scheme"] == "bearer"
        assert c.get("/agents/other/.well-known/agent-card.json").json()["url"].endswith("/agents/other")
        assert c.get("/agents/nope/.well-known/agent-card.json").status_code == 404
        h = c.get("/healthz").json()
        assert h["status"] == "ok"


def test_full_flow_send_input_required_followup_completed(stack, client):
    bot = stack["bot"]
    # 1) external send (non-blocking)
    r = rpc(client, "message/send", {"message": user_msg("repo_url: https://x/y goal: add tests")})
    t = r.json()["result"]
    assert t["kind"] == "task" and t["status"]["state"] == "submitted"
    tid, ctx = t["id"], t["contextId"]

    # 2) webhook received by mock bot with full contract
    t = wait_state(client, tid, {"input-required"})
    p = bot.state.received[0]
    assert p["event"] == "task.created" and p["taskId"] == tid and p["contextId"] == ctx
    assert p["callback"]["url"].endswith(f"/callback/{tid}") and p["callback"]["token"].startswith("cbt_")
    assert "curl" in p["instructions"] and "input-required" in p["instructions"]
    assert p["history"][0]["parts"][0]["text"].startswith("repo_url")
    assert p["requiredInputs"][0]["name"] == "repo_url"
    assert t["status"]["message"]["parts"][0]["text"] == "Which branch should I use?"

    # 3) stream follow-up message on same task -> completed, see artifacts + final
    evs = read_sse(client, "message/stream", {"message": user_msg("branch: main", task=tid, ctx=ctx)})
    data = [e["result"] for e in evs if "result" in e]
    assert data[0]["kind"] == "task" and data[0]["id"] == tid
    kinds = [d["kind"] for d in data]
    assert "artifact-update" in kinds
    assert data[-1]["kind"] == "status-update" and data[-1]["final"] is True
    assert data[-1]["status"]["state"] == "completed"

    # 4) bot got the follow-up with full context history
    p2 = bot.state.received[1]
    assert p2["event"] == "task.message"
    roles = [m["role"] for m in p2["history"]]
    assert roles.count("user") == 2 and "agent" in roles

    # 5) tasks/get sees final state, artifacts, history
    t = rpc(client, "tasks/get", {"id": tid}).json()["result"]
    assert t["status"]["state"] == "completed"
    names = [a["name"] for a in t["artifacts"]]
    assert "result" in names and "summary" in names
    assert any(p["kind"] == "data" for a in t["artifacts"] for p in a["parts"])
    assert len(rpc(client, "tasks/get", {"id": tid, "historyLength": 1}).json()["result"]["history"]) == 1

    # 6) terminal task rejects further messages (-32004) and cancel (-32002)
    e = rpc(client, "message/send", {"message": user_msg("more", task=tid)}).json()["error"]
    assert e["code"] == -32004
    assert rpc(client, "tasks/cancel", {"id": tid}).json()["error"]["code"] == -32002

    # 7) a new task in the same context includes previous history for the bot
    t2 = rpc(client, "message/send", {"message": user_msg("another branch: dev", ctx=ctx),
                                      "configuration": {"blocking": True}}).json()["result"]
    assert t2["contextId"] == ctx and t2["id"] != tid and t2["status"]["state"] == "completed"
    assert len(bot.state.received[-1]["history"]) >= 4


def test_stream_new_task_heartbeat_and_resubscribe(stack, client):
    evs = read_sse(client, "message/stream", {"message": user_msg("hello")})
    data = [e["result"] for e in evs if "result" in e]
    assert data[0]["kind"] == "task"
    assert data[-1]["status"]["state"] == "input-required"
    tid = data[0]["id"]
    # resubscribe while waiting for input: initial task + heartbeat comments
    got = []

    def sub():
        with httpx.Client(base_url=stack["base"], headers={"Authorization": f"Bearer {API_KEY}"}, timeout=20) as c:
            got.extend(read_sse(c, "tasks/resubscribe", {"id": tid}))

    th = threading.Thread(target=sub); th.start()
    time.sleep(1.2)
    # the bot (simulated manually) completes the task via callback
    tok = callback_token(BOT_SECRET, tid)
    r = httpx.post(f"{stack['base']}/callback/{tid}", headers={"Authorization": f"Bearer {tok}"},
                   json={"state": "completed", "artifacts": [{"name": "x", "text": "y"}]})
    assert r.status_code == 200
    th.join(10)
    assert got[0]["result"]["kind"] == "task"
    assert any(e.get("heartbeat") for e in got)
    res = [e["result"] for e in got if "result" in e]
    assert res[-1]["status"]["state"] == "completed" and res[-1]["final"]
    # resubscribe to terminal task (0.3) returns task and closes
    evs = read_sse(client, "tasks/resubscribe", {"id": tid})
    assert evs[0]["result"]["status"]["state"] == "completed"


def test_v1_methods(stack, client):
    h = {"A2A-Version": "1.0"}
    m = {"messageId": "v1-1", "role": "ROLE_USER", "parts": [{"text": "branch: main please"},
                                                            {"data": {"repo_url": "https://x"}},
                                                            {"raw": "aGVsbG8=", "filename": "a.txt",
                                                             "mediaType": "text/plain"}]}
    r = rpc(client, "SendMessage", {"message": m}, headers=h).json()["result"]
    t = r["task"]
    assert t["status"]["state"] == "TASK_STATE_COMPLETED"  # blocking by default in 1.0
    assert t["history"][0]["role"] == "ROLE_USER"
    assert t["history"][0]["parts"][2]["filename"] == "a.txt"
    g = rpc(client, "GetTask", {"id": t["id"]}, headers=h).json()["result"]
    assert g["artifacts"][-1]["parts"][1] == {"data": {"ok": True}}
    lst = rpc(client, "ListTasks", {"pageSize": 10}, headers=h).json()["result"]
    assert lst["totalSize"] >= 1 and "artifacts" not in lst["tasks"][0] and lst["nextPageToken"] == ""
    evs = read_sse(client, "SendStreamingMessage", {"message": {**m, "messageId": "v1-2"}}, headers=h)
    res = [e["result"] for e in evs if "result" in e]
    assert "task" in res[0] and any("artifactUpdate" in x for x in res)
    assert res[-1]["statusUpdate"]["status"]["state"] == "TASK_STATE_COMPLETED"
    e = rpc(client, "SubscribeToTask", {"id": t["id"]}, headers=h).json()["error"]
    assert e["code"] == -32004
    assert rpc(client, "GetTask", {"id": "nope"}, headers=h).json()["error"]["code"] == -32001
    assert rpc(client, "SendMessage", {"message": m}, headers={"A2A-Version": "9.9"}).json()["error"]["code"] == -32009


def test_cancel_and_push_config(stack, client):
    t = rpc(client, "message/send", {"message": user_msg("hi")}).json()["result"]
    wait_state(client, t["id"], {"input-required"})
    s = rpc(client, "tasks/pushNotificationConfig/set",
            {"taskId": t["id"], "pushNotificationConfig": {"url": "http://127.0.0.1:1/x"}}).json()
    assert s["error"]["code"] == -32602  # http/private push URLs rejected by default
    c = rpc(client, "tasks/cancel", {"id": t["id"]}).json()["result"]
    assert c["status"]["state"] == "canceled"
    time.sleep(0.3)
    assert stack["bot"].state.received[-1]["event"] == "task.canceled"
    # bot callbacks after cancel -> 409
    tok = callback_token(BOT_SECRET, t["id"])
    r = httpx.post(f"{stack['base']}/callback/{t['id']}", headers={"Authorization": f"Bearer {tok}"},
                   json={"state": "completed"})
    assert r.status_code == 409


def test_callback_signature_idempotency_and_validation(stack, client):
    t = rpc(client, "message/send", {"message": user_msg("hi")}).json()["result"]
    tid = t["id"]
    wait_state(client, tid, {"input-required"})
    body = json.dumps({"state": "working", "message": "signed progress", "eventId": "e1"}).encode()
    url = f"{stack['base']}/callback/{tid}"
    r = httpx.post(url, content=body, headers={"X-A2A-Signature": sign(BOT_SECRET, body),
                                              "Content-Type": "application/json"})
    assert r.status_code == 200 and r.json()["task"]["status"]["state"] == "working"
    r = httpx.post(url, content=body, headers={"X-A2A-Signature": sign(BOT_SECRET, body)})
    assert r.json()["duplicate"] is True
    tok = callback_token(BOT_SECRET, tid)
    assert httpx.post(url, headers={"Authorization": f"Bearer {tok}"}, json={"state": "submitted"}).status_code == 400
    assert httpx.post(url, headers={"Authorization": f"Bearer {tok}"}, json={}).status_code == 400
    assert httpx.get(url, headers={"Authorization": f"Bearer {tok}"}).json()["task"]["id"] == tid
    # artifact append
    httpx.post(url, headers={"Authorization": f"Bearer {tok}"},
               json={"artifacts": [{"artifactId": "a1", "text": "x"}]})
    httpx.post(url, headers={"Authorization": f"Bearer {tok}"},
               json={"artifacts": [{"artifactId": "a1", "text": "y", "append": True}], "state": "completed"})
    t = rpc(client, "tasks/get", {"id": tid}).json()["result"]
    a1 = next(a for a in t["artifacts"] if a["artifactId"] == "a1")
    assert [p["text"] for p in a1["parts"]] == ["x", "y"]


def test_persistence_across_restart(tmp_path):
    bot_port, bridge_port = free_port(), free_port()
    env = make_env(tmp_path, bot_port, bridge_port)
    from mock_bot import make_app
    with Server(make_app(BOT_TOKEN, BOT_SECRET), bot_port):
        with Server(create_app(load_settings(env)), bridge_port):
            with httpx.Client(base_url=f"http://127.0.0.1:{bridge_port}",
                              headers={"Authorization": f"Bearer {API_KEY}"}) as c:
                tid = rpc(c, "message/send", {"message": user_msg("x")}).json()["result"]["id"]
                wait_state(c, tid, {"input-required"})
        bridge_port2 = free_port()
        env2 = dict(env, A2A_PUBLIC_BASE_URL=f"http://127.0.0.1:{bridge_port2}")
        with Server(create_app(load_settings(env2)), bridge_port2):
            with httpx.Client(base_url=f"http://127.0.0.1:{bridge_port2}",
                              headers={"Authorization": f"Bearer {API_KEY}"}) as c:
                t = rpc(c, "tasks/get", {"id": tid}).json()["result"]
                assert t["status"]["state"] == "input-required"
                t = rpc(c, "message/send", {"message": user_msg("branch: main", task=tid),
                                            "configuration": {"blocking": True}}).json()["result"]
                assert t["status"]["state"] == "completed"


def test_webhook_retry_then_fail(tmp_path):
    bridge_port = free_port()
    env = make_env(tmp_path, free_port(), bridge_port)  # nobody listens on bot port
    with Server(create_app(load_settings(env)), bridge_port):
        with httpx.Client(base_url=f"http://127.0.0.1:{bridge_port}",
                          headers={"Authorization": f"Bearer {API_KEY}"}) as c:
            tid = rpc(c, "message/send", {"message": user_msg("x")}).json()["result"]["id"]
            t = wait_state(c, tid, {"failed"}, timeout=15)
            assert "could not deliver" in t["status"]["message"]["parts"][0]["text"]


def test_stuck_task_timeout(stack, client):
    t = rpc(client, "message/send", {"message": user_msg("branch: main")}).json()["result"]
    wait_state(client, t["id"], {"completed"})
    t = rpc(client, "message/send", {"message": user_msg("hi")}).json()["result"]
    wait_state(client, t["id"], {"input-required"})
    bridge = stack["app"].state.bridge
    bridge.s.input_required_timeout_s = -1
    assert bridge.sweep() >= 1
    assert rpc(client, "tasks/get", {"id": t["id"]}).json()["result"]["status"]["state"] == "canceled"


def _flaky_bot(statuses, body=None):
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    app = FastAPI()
    app.state.calls = 0

    @app.post("/hook")
    async def hook():
        i = app.state.calls
        app.state.calls += 1
        code = statuses[i] if i < len(statuses) else 200
        return JSONResponse(body if (body and code != 200) else {"x": 1}, status_code=code)
    return app


def _run_with_bot(tmp_path, statuses, body=None, **extra):
    bot_port, bridge_port = free_port(), free_port()
    bot = _flaky_bot(statuses, body)
    env = make_env(tmp_path, bot_port, bridge_port, A2A_WEBHOOK_4XX_RETRY_DELAY_S="0.2", **extra)
    with Server(bot, bot_port), Server(create_app(load_settings(env)), bridge_port):
        with httpx.Client(base_url=f"http://127.0.0.1:{bridge_port}",
                          headers={"Authorization": f"Bearer {API_KEY}"}) as c:
            tid = rpc(c, "message/send", {"message": user_msg("x")}).json()["result"]["id"]
            t = wait_state(c, tid, {"working", "failed"}, timeout=10)
            time.sleep(0.5)
            return rpc(c, "tasks/get", {"id": tid}).json()["result"], bot.state.calls


def test_transient_400_is_retried_once(tmp_path):
    t, calls = _run_with_bot(tmp_path, [400])
    assert t["status"]["state"] == "working" and calls == 2


def test_repeated_400_fails_after_one_retry(tmp_path):
    t, calls = _run_with_bot(tmp_path, [400, 400, 400])
    assert t["status"]["state"] == "failed" and calls == 2


def test_401_fails_immediately(tmp_path):
    t, calls = _run_with_bot(tmp_path, [401])
    assert t["status"]["state"] == "failed" and calls == 1


def test_4xx_retries_configurable_zero(tmp_path):
    t, calls = _run_with_bot(tmp_path, [400], A2A_WEBHOOK_4XX_RETRIES="0")
    assert t["status"]["state"] == "failed" and calls == 1


def test_rate_limit_400_body_uses_normal_backoff(tmp_path):
    # real-world: upstream answers HTTP 400 {"error":"Automation run rate limit exceeded: ..."}
    body = {"success": False, "error": "Automation run rate limit exceeded: global limit 12000 per minute"}
    t, calls = _run_with_bot(tmp_path, [400, 400, 400], body=body, A2A_WEBHOOK_MAX_ATTEMPTS="6")
    assert t["status"]["state"] == "working" and calls == 4
