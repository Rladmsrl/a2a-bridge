"""Roster: status inference, key scoping, privacy tiers, SSE, and DB migration."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time

import httpx
import pytest

from a2a_bridge.app import create_app
from a2a_bridge.config import ConfigError, load_settings, parse_api_keys
from a2a_bridge.db import Store
from a2a_bridge.events import EventBus
from a2a_bridge.roster import Roster, redact_text, ts_to_iso
from a2a_bridge.security import callback_token
from a2a_bridge.service import Bridge
from conftest import API_KEY, BOT_SECRET, LIMITED_KEY, Server, free_port, make_env, rpc, user_msg, wait_state

OWNER_KEY = "owner-key-0123456789abcd"  # gitleaks:allow (test fixture)
OWNER_SCOPED_KEY = "owner-scoped-01234567ab"  # gitleaks:allow (test fixture)
TRUSTED_KEY = "trusted-key-0123456789ab"  # gitleaks:allow (test fixture)


def _roster_env(tmp_path, bot_port, bridge_port, **extra):
    keys = (f"main=plain:{API_KEY},lim=plain:{LIMITED_KEY}@other,"
            f"ops=plain:{OWNER_KEY},ops2=plain:{OWNER_SCOPED_KEY}@other,"
            f"trust=plain:{TRUSTED_KEY}")
    kwargs = dict(A2A_API_KEYS=keys,
                  A2A_KEY_TIERS="ops:owner,ops2:owner,trust:trusted",
                  A2A_ROSTER_DEBOUNCE_S="0.05",
                  A2A_ROSTER_REDACT="secretproject")
    kwargs.update(extra)
    env = make_env(tmp_path, bot_port, bridge_port, **kwargs)
    return env


def make_roster(tmp_path, **extra):
    env = _roster_env(tmp_path, 9, 8, **extra)
    s = load_settings(env)
    store = Store(s.db_path)
    return s, store, Roster(s, store, EventBus())


def _insert_task(store: Store, *, tid: str, agent="coder", owner="main", state="working",
                 updated_ago=0.0, waiting_on=None, public_activity=None, message=None,
                 created_ago=None):
    now = time.time()
    ts = now - updated_ago
    created = now - (created_ago if created_ago is not None else updated_ago)
    status = {"state": state, "timestamp": ts_to_iso(ts)}
    if message:
        status["message"] = {"kind": "message", "role": "agent", "messageId": "m-" + tid,
                             "parts": [{"kind": "text", "text": message}]}
    store.create_task(tid, "ctx-" + tid, agent, owner, status, None)
    store._x(
        "UPDATE tasks SET state=?, status_json=?, status_text=?, updated_at=?, created_at=?, "
        "waiting_on=?, public_activity=? WHERE id=?",
        (state, json.dumps(status), message, ts, created, waiting_on, public_activity, tid))


def _status(roster: Roster, name="coder", now=None):
    recs = roster.recompute(now=now)
    return recs[name]


# --------------------------------------------------------------------------- inference
def test_idle_by_default(tmp_path):
    _, _, roster = make_roster(tmp_path)
    rec = _status(roster)
    assert rec["status"] == "idle"
    assert rec["availability"] == "available"
    assert rec["confidence"] == "low"
    assert rec["capacity"]["active"] == 0 and rec["capacity"]["queued"] == 0


def test_busy_when_working(tmp_path):
    _, store, roster = make_roster(tmp_path)
    _insert_task(store, tid="w1", state="working", message="Running tests")
    rec = _status(roster)
    assert rec["status"] == "busy"
    assert rec["availability"] == "unavailable"  # maxConcurrent default 1
    assert rec["capacity"]["active"] == 1
    assert rec["rawActivity"] == "Running tests"
    assert rec["activity"] == "Working on a Code task"


def test_input_required(tmp_path):
    _, store, roster = make_roster(tmp_path)
    _insert_task(store, tid="i1", state="input-required", message="Which branch?")
    rec = _status(roster)
    assert rec["status"] == "input-required"
    assert rec["waitingOn"] == "requester"
    assert rec["availability"] == "available"
    assert rec["activity"] == "Waiting for requester input"


def test_stuck_working_past_threshold(tmp_path):
    _, store, roster = make_roster(tmp_path)
    now = time.time()
    _insert_task(store, tid="s1", state="working", updated_ago=1000, message="still going")
    rec = _status(roster, now=now)
    assert rec["status"] == "stuck"
    assert rec["availability"] == "unavailable"
    rec_fresh = _status(roster, now=now - 200)  # age ~800s < 900
    assert rec_fresh["status"] == "busy"


def test_waiting_on_owner_is_not_stuck(tmp_path):
    _, store, roster = make_roster(tmp_path)
    _insert_task(store, tid="s1", state="working", updated_ago=1000, waiting_on="owner",
                 message="Need approval", public_activity="Waiting on review")
    rec = _status(roster)
    assert rec["status"] == "input-required"
    assert rec["waitingOn"] == "owner"
    assert rec["activity"] == "Waiting on review"


def test_offline_after_permanent_delivery_failure(tmp_path):
    _, store, roster = make_roster(tmp_path)
    now = time.time()
    store.presence_update("coder", delivery_dead=1, last_delivery_err_at=now,
                          last_delivery_err="HTTP 401: denied")
    rec = _status(roster, now=now)
    assert rec["status"] == "offline"
    assert rec["availability"] == "unavailable"


def test_offline_beats_busy(tmp_path):
    _, store, roster = make_roster(tmp_path)
    now = time.time()
    _insert_task(store, tid="w1", state="working")
    store.presence_update("coder", delivery_dead=1, last_delivery_err_at=now,
                          last_delivery_err="HTTP 404")
    rec = _status(roster, now=now)
    assert rec["status"] == "offline"


def test_offline_consecutive_failures_with_old_submitted(tmp_path):
    _, store, roster = make_roster(tmp_path)
    _insert_task(store, tid="q1", state="submitted", updated_ago=200)
    store.presence_update("coder", consecutive_failures=3)
    rec = _status(roster)
    assert rec["status"] == "offline"


def test_submitted_delivered_becomes_stuck(tmp_path):
    _, store, roster = make_roster(tmp_path)
    _insert_task(store, tid="q1", state="submitted", updated_ago=400)
    store.outbox_add("bot", "q1", "d1", "http://127.0.0.1/hook", {}, "{}")
    store._x("UPDATE outbox SET status='done' WHERE task_id=?", ("q1",))
    rec = _status(roster)
    assert rec["status"] == "stuck"


def test_busy_with_capacity_is_limited(tmp_path):
    env = _roster_env(tmp_path, 9, 8)
    data = json.loads(open(env["A2A_AGENTS_CONFIG"]).read())
    for a in data["agents"]:
        if a["name"] == "coder":
            a["max_concurrent"] = 2
    open(env["A2A_AGENTS_CONFIG"], "w").write(json.dumps(data))
    s = load_settings(env)
    store = Store(s.db_path)
    roster = Roster(s, store, EventBus())
    _insert_task(store, tid="w1", state="working")
    rec = _status(roster)
    assert rec["status"] == "busy"
    assert rec["availability"] == "limited"
    assert rec["capacity"]["maxConcurrent"] == 2
    store.close()


def test_confidence_from_last_seen(tmp_path):
    _, store, roster = make_roster(tmp_path)
    now = time.time()
    store.presence_update("coder", last_seen_at=now - 30, last_delivery_ok_at=now - 30)
    rec = _status(roster, now=now)
    assert rec["confidence"] == "high"
    store.presence_update("coder", last_seen_at=now - 3600, last_delivery_ok_at=now - 3600)
    rec = _status(roster, now=now)
    assert rec["confidence"] == "medium"
    store.presence_update("coder", last_seen_at=now - 90000)
    rec = _status(roster, now=now)
    assert rec["confidence"] == "low"


def test_redact_urls_emails_repos_and_custom(tmp_path):
    s, _, roster = make_roster(tmp_path)
    text = "Cloning https://github.com/acme/widgets for user@acme.test in acme/widgets secretproject"
    out = redact_text(text, roster._redact)
    assert "https://" not in out
    assert "user@acme.test" not in out
    assert "acme/widgets" not in out
    assert "secretproject" not in out
    assert "[redacted]" in out
    assert "text/plain" == redact_text("text/plain", [])  # mime type kept
    assert s.roster_redact == ["secretproject"]
    broad = ("host db.internal.corp ip 10.0.0.5 and 192.168.1.8 token "
             "ghp_abcdefghijklmnopqrstuvwxyz012345 aws AKIAIOSFODNN7EXAMPLE "
             "jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk "
             "phone +1-415-555-0123")
    red = redact_text(broad, [])
    assert "10.0.0.5" not in red
    assert "192.168.1.8" not in red
    assert "db.internal.corp" not in red
    assert "ghp_" not in red
    assert "AKIAIOSFODNN7EXAMPLE" not in red
    assert "eyJhbGciOiJIUzI1NiJ9" not in red
    assert "415-555-0123" not in red
    assert "text/plain" == redact_text("text/plain", [])


# --------------------------------------------------------------------------- HTTP: auth, scope, tiers
@pytest.fixture
def roster_stack(tmp_path):
    bot_port, bridge_port = free_port(), free_port()
    from mock_bot import make_app as make_bot
    from conftest import BOT_TOKEN
    bot = make_bot(BOT_TOKEN, BOT_SECRET)
    env = _roster_env(tmp_path, bot_port, bridge_port)
    app = create_app(load_settings(env))
    with Server(bot, bot_port), Server(app, bridge_port):
        yield {"base": f"http://127.0.0.1:{bridge_port}", "app": app, "env": env, "tmp": tmp_path}


def _c(stack, key=API_KEY):
    return httpx.Client(base_url=stack["base"], headers={"Authorization": f"Bearer {key}"}, timeout=20)


def flush_roster(stack):
    stack["app"].state.bridge.roster.recompute()


def test_roster_requires_api_key(roster_stack):
    r = httpx.get(roster_stack["base"] + "/roster")
    assert r.status_code == 401 and "Bearer" in r.headers.get("www-authenticate", "")


def test_roster_key_scoping_and_404(roster_stack):
    with _c(roster_stack, LIMITED_KEY) as c:
        r = c.get("/roster")
        assert r.status_code == 200
        names = [a["id"] for a in r.json()["agents"]]
        assert names == ["other"]
        assert c.get("/roster/coder").status_code == 404
        assert c.get("/roster/nope").status_code == 404
        assert c.get("/roster/other").status_code == 200
        assert c.get("/roster/other").json()["agents"][0]["id"] == "other"


def test_roster_basic_vs_trusted_vs_owner_and_redaction(roster_stack):
    with _c(roster_stack, API_KEY) as c:
        body = c.get("/roster").json()
        assert body["viewTier"] == "basic"
        coder = next(a for a in body["agents"] if a["id"] == "coder")
        assert "status" not in coder and "activity" not in coder and "lastSeen" not in coder
        assert "availability" in coder and "statusDurationS" in coder
        assert coder["statusDurationS"] % 60 == 0
        assert set(coder["capacity"]) == {"queued"}
        assert c.get("/roster?include=tasks").status_code == 403

    with _c(roster_stack, API_KEY) as c:
        tid = rpc(c, "message/send", {"message": user_msg("hi")}).json()["result"]["id"]
        wait_state(c, tid, {"input-required"})
        tok = callback_token(BOT_SECRET, tid)
        httpx.post(f"{roster_stack['base']}/callback/{tid}",
                   headers={"Authorization": f"Bearer {tok}"},
                   json={"state": "working",
                         "message": "Cloning https://github.com/acme/widgets for secretproject",
                         "metadata": {"roster": {"publicActivity":
                                                 "Running tests at https://github.com/acme/widgets secretproject"}},
                         "eventId": "p-roster"})
    flush_roster(roster_stack)

    with _c(roster_stack, TRUSTED_KEY) as c:
        r = c.get("/roster/coder")
        assert r.status_code == 200
        a = r.json()["agents"][0]
        assert r.json()["viewTier"] == "trusted"
        assert a["status"] in ("busy", "input-required", "stuck")
        assert "activity" in a and a["activity"]
        assert "https://" not in a["activity"]
        assert "secretproject" not in a["activity"]
        assert "acme/widgets" not in a["activity"]
        assert "detail" not in a
        assert c.get("/roster?include=tasks").status_code == 403

    with _c(roster_stack, OWNER_KEY) as c:
        r = c.get("/roster/coder?include=tasks")
        assert r.status_code == 200
        a = r.json()["agents"][0]
        assert r.json()["viewTier"] == "owner"
        assert "detail" in a
        assert "counts" in a["detail"] and "delivery" in a["detail"]
        assert "tasks" in a["detail"]
        assert a["detail"]["rawActivity"] is None or isinstance(a["detail"]["rawActivity"], str)


def test_waiting_on_persisted_from_callback(roster_stack):
    with _c(roster_stack, TRUSTED_KEY) as c:
        tid = rpc(c, "message/send", {"message": user_msg("hi")}).json()["result"]["id"]
        wait_state(c, tid, {"input-required"})
        tok = callback_token(BOT_SECRET, tid)
        httpx.post(f"{roster_stack['base']}/callback/{tid}",
                   headers={"Authorization": f"Bearer {tok}"},
                   json={"state": "working", "message": "Need a review",
                         "metadata": {"roster": {"waitingOn": "owner",
                                                 "publicActivity": "Waiting for review"}},
                         "eventId": "wait-owner"})
        flush_roster(roster_stack)
        a = c.get("/roster/coder").json()["agents"][0]
        assert a["waitingOn"] == "owner"
        assert a["status"] == "input-required"
        row = roster_stack["app"].state.bridge.store.get_task(tid)
        assert row["waiting_on"] == "owner"
        assert row["public_activity"] == "Waiting for review"
        # later progress without waitingOn/publicActivity clears both
        httpx.post(f"{roster_stack['base']}/callback/{tid}",
                   headers={"Authorization": f"Bearer {tok}"},
                   json={"state": "working", "message": "Continuing", "eventId": "cont"})
        row = roster_stack["app"].state.bridge.store.get_task(tid)
        assert row["waiting_on"] is None
        assert row["public_activity"] is None
        flush_roster(roster_stack)
        a = c.get("/roster/coder").json()["agents"][0]
        assert a["status"] == "busy"
        assert a.get("waitingOn") is None
        assert a.get("activity") != "Waiting for review"


def test_roster_etag_and_invalid_status(roster_stack):
    with _c(roster_stack) as c:
        r = c.get("/roster")
        assert r.headers.get("cache-control") == "no-store"
        etag = r.headers["etag"]
        r2 = c.get("/roster", headers={"If-None-Match": etag})
        assert r2.status_code == 304
        assert c.get("/roster", params={"status": "idle"}).status_code == 403
        assert c.get("/roster", params={"availability": "nope"}).status_code == 400
    with _c(roster_stack, TRUSTED_KEY) as c:
        bad = c.get("/roster", params={"status": "nope"})
        assert bad.status_code == 400 and bad.json()["param"] == "status"


def test_agent_card_points_at_roster_not_live_status(roster_stack):
    r = httpx.get(roster_stack["base"] + "/agents/coder/.well-known/agent-card.json")
    card = r.json()
    uris = [e["uri"] for e in card["capabilities"]["extensions"]]
    assert "urn:a2a-bridge:roster:v1" in uris
    ext = next(e for e in card["capabilities"]["extensions"] if e["uri"].endswith("roster:v1"))
    assert ext["required"] is False
    assert ext["params"]["rosterUrl"].endswith("/roster/coder")
    assert ext["params"]["streamUrl"].endswith("/roster/stream")
    blob = json.dumps(card)
    assert "\"status\":" not in blob or "statusValues" in blob
    assert "idle" in ext["params"]["statusValues"]


def test_roster_sse_snapshot_and_update(roster_stack):
    got = []

    def read():
        with httpx.Client(base_url=roster_stack["base"],
                          headers={"Authorization": f"Bearer {TRUSTED_KEY}"}, timeout=15) as c:
            with c.stream("GET", "/roster/stream") as r:
                assert r.status_code == 200
                assert r.headers["content-type"].startswith("text/event-stream")
                event = None
                for line in r.iter_lines():
                    if line.startswith("event:"):
                        event = line.split(":", 1)[1].strip()
                    elif line.startswith("data:") and event:
                        got.append({"event": event, "data": json.loads(line[5:].strip())})
                        event = None
                    if any(e["event"] == "update" for e in got) and any(e["event"] == "snapshot" for e in got):
                        break
                    if len(got) >= 20:
                        break

    th = threading.Thread(target=read)
    th.start()
    time.sleep(0.4)
    with _c(roster_stack, TRUSTED_KEY) as c:
        rpc(c, "message/send", {"message": user_msg("hello from sse")})
    th.join(12)
    kinds = [e["event"] for e in got]
    assert "snapshot" in kinds
    snap = next(e["data"] for e in got if e["event"] == "snapshot")
    assert "agents" in snap and snap["viewTier"] == "trusted"
    assert "update" in kinds
    upd = next(e["data"] for e in got if e["event"] == "update")
    assert "seq" in upd and "agent" in upd
    assert upd["agent"]["id"] in ("coder", "other")


def test_roster_stream_cap_unit(tmp_path):
    s, store, roster = make_roster(tmp_path, A2A_ROSTER_MAX_STREAMS_PER_KEY="2")
    assert roster.acquire_stream("main")
    assert roster.acquire_stream("main")
    assert roster.acquire_stream("main") is False
    roster.release_stream("main")
    assert roster.acquire_stream("main")
    roster.release_stream("main")
    roster.release_stream("main")
    assert s.roster_max_streams_per_key == 2
    store.close()


def test_key_tiers_config(tmp_path):
    env = make_env(tmp_path, 1, 2, A2A_KEY_TIERS="main:trusted,lim:owner")
    s = load_settings(env)
    by = {k.label: k.tier for k in s.api_keys}
    assert by["main"] == "trusted" and by["lim"] == "owner"
    with pytest.raises(ConfigError):
        load_settings({**env, "A2A_KEY_TIERS": "main:superuser"})
    env2 = make_env(tmp_path, 1, 2)
    s2 = load_settings(env2)
    assert all(k.tier == "basic" for k in s2.api_keys)


def test_migration_on_existing_db(tmp_path):
    path = tmp_path / "legacy.db"
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE tasks (
          id TEXT PRIMARY KEY,
          context_id TEXT NOT NULL,
          agent TEXT NOT NULL,
          owner TEXT NOT NULL,
          state TEXT NOT NULL,
          status_json TEXT NOT NULL,
          artifacts_json TEXT NOT NULL DEFAULT '[]',
          metadata_json TEXT,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL
        );
        CREATE TABLE outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          task_id TEXT,
          delivery_id TEXT NOT NULL,
          url TEXT NOT NULL,
          headers_json TEXT NOT NULL,
          body TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          next_at REAL NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          last_error TEXT,
          created_at REAL NOT NULL
        );
    """)
    now = time.time()
    db.execute("INSERT INTO tasks(id,context_id,agent,owner,state,status_json,created_at,updated_at) "
               "VALUES(?,?,?,?,?,?,?,?)",
               ("old-1", "c", "coder", "main", "working", '{"state":"working"}', now, now))
    db.commit()
    db.close()
    store = Store(str(path))
    tcols = {r[1] for r in store._x("PRAGMA table_info(tasks)").fetchall()}
    assert "waiting_on" in tcols and "public_activity" in tcols and "status_text" in tcols
    idx = {r[1] for r in store._x("PRAGMA index_list(outbox)").fetchall()}
    assert "ix_outbox_task" in idx and "ix_outbox_kind_status" in idx
    tables = {r[0] for r in store._x("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "agent_presence" in tables
    pcols = {r[1] for r in store._x("PRAGMA table_info(agent_presence)").fetchall()}
    assert "delivery_dead" in pcols and "last_seen_at" in pcols
    row = store.get_task("old-1")
    assert row["state"] == "working" and row["waiting_on"] is None
    store.update_task("old-1", waiting_on="owner", public_activity="review")
    row = store.get_task("old-1")
    assert row["waiting_on"] == "owner" and row["public_activity"] == "review"
    store.close()


# --------------------------------------------------------------------------- review regressions
def test_basic_status_filter_forbidden_availability_ok(roster_stack):
    flush_roster(roster_stack)
    with _c(roster_stack) as c:
        r = c.get("/roster", params={"status": "idle"})
        assert r.status_code == 403
        r = c.get("/roster", params={"availability": "available"})
        assert r.status_code == 200
        assert {a["availability"] for a in r.json()["agents"]} <= {"available"}
        r = c.get("/roster/coder", params={"status": "busy"})
        assert r.status_code == 403
    with _c(roster_stack, TRUSTED_KEY) as c:
        r = c.get("/roster", params={"status": "idle"})
        assert r.status_code == 200
        assert all(a["status"] == "idle" for a in r.json()["agents"])


def test_owner_tier_intersects_key_agents(roster_stack):
    flush_roster(roster_stack)
    with _c(roster_stack, OWNER_SCOPED_KEY) as c:
        r = c.get("/roster")
        assert r.status_code == 200
        names = [a["id"] for a in r.json()["agents"]]
        assert names == ["other"]
        assert r.json()["viewTier"] == "owner"
        assert c.get("/roster/coder").status_code == 404
        r = c.get("/roster/other", params={"include": "tasks"})
        assert r.status_code == 200
        a = r.json()["agents"][0]
        assert "detail" in a
        assert all(t.get("taskId") for t in a["detail"].get("tasks", []))
    with _c(roster_stack, OWNER_KEY) as c:
        names = [a["id"] for a in c.get("/roster").json()["agents"]]
        assert "coder" in names and "other" in names


def test_owner_wait_uses_input_required_timeout(tmp_path):
    env = _roster_env(tmp_path, 9, 8)
    s = load_settings(env)
    store = Store(s.db_path)
    bridge = Bridge(s, store, EventBus(), None)
    _insert_task(store, tid="ow1", state="working", updated_ago=3700, waiting_on="owner")
    _insert_task(store, tid="w1", state="working", updated_ago=3700, agent="other")
    n = bridge.sweep()
    assert store.get_task("ow1")["state"] == "working"
    assert store.get_task("ow1")["waiting_on"] == "owner"
    assert store.get_task("w1")["state"] == "failed"
    assert n >= 1
    bridge.s.input_required_timeout_s = 3600
    assert bridge.sweep() >= 1
    assert store.get_task("ow1")["state"] == "canceled"
    assert store.get_task("ow1")["waiting_on"] is None
    store.close()


def test_snapshot_serves_cache_without_recompute_or_presence_write(tmp_path):
    s, store, roster = make_roster(tmp_path)
    key = next(k for k in s.api_keys if k.label == "main")
    body, _ = roster.snapshot(key)
    assert body["agents"] == []
    roster.recompute()
    _insert_task(store, tid="w1", state="working", message="Running tests")
    body, _ = roster.snapshot(key)
    coder = next(a for a in body["agents"] if a["id"] == "coder")
    assert coder["availability"] == "available"  # still cached idle
    rec_calls: list = []
    orig_re = roster.recompute

    def wrapped_re(*a, **k):
        rec_calls.append(1)
        return orig_re(*a, **k)

    roster.recompute = wrapped_re  # type: ignore[method-assign]
    pres_calls: list = []
    orig_p = store.presence_update

    def wrapped_p(*a, **k):
        pres_calls.append(1)
        return orig_p(*a, **k)

    store.presence_update = wrapped_p  # type: ignore[method-assign]
    roster.snapshot(key)
    assert rec_calls == []
    assert pres_calls == []
    roster.recompute = orig_re  # type: ignore[method-assign]
    rec = _status(roster)
    assert rec["status"] == "busy"
    assert rec["rawActivity"] == "Running tests"
    store.close()


def test_roster_signals_does_not_load_status_json(tmp_path):
    _, store, roster = make_roster(tmp_path)
    _insert_task(store, tid="w1", state="working", message="hi")
    blob = "A" * 50_000
    store._x("UPDATE tasks SET status_json=? WHERE id=?",
             (json.dumps({"state": "working", "file": blob}), "w1"))
    sig = store.roster_signals(time.time() - 3600)
    row = sig["active"][0]
    assert "status_json" not in row
    assert "status_text" in row
    rec = _status(roster)
    assert rec["status"] == "busy"
    store.close()


def test_http_500_does_not_lock_offline(tmp_path):
    _, store, roster = make_roster(tmp_path)
    now = time.time()
    roster.note_delivery_err("coder", "HTTP 500: boom", dead=True, rate_limited=False,
                             http_status=500, now=now)
    rec = _status(roster, now=now)
    assert rec["status"] != "offline"
    store.close()


def test_offline_ttl_expires_auth_lock(tmp_path):
    s, store, roster = make_roster(tmp_path, A2A_ROSTER_OFFLINE_TTL_S="60")
    now = time.time()
    store.presence_update("coder", delivery_dead=1, last_delivery_err_at=now - 120,
                          last_delivery_err="HTTP 401: denied")
    rec = _status(roster, now=now)
    assert rec["status"] == "idle"
    store.presence_update("coder", delivery_dead=1, last_delivery_err_at=now - 10,
                          last_delivery_err="HTTP 403")
    rec = _status(roster, now=now)
    assert rec["status"] == "offline"
    store.close()


def test_debounce_max_wait_flushes(tmp_path):
    s, store, roster = make_roster(tmp_path, A2A_ROSTER_DEBOUNCE_S="5",
                                   A2A_ROSTER_DEBOUNCE_MAX_S="0.15")
    roster.recompute()
    _insert_task(store, tid="w1", state="working")
    roster.touch("coder")

    async def run():
        t = asyncio.create_task(roster.loop())
        await asyncio.sleep(0.45)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    rec = roster.last("coder")
    assert rec is not None and rec["status"] == "busy"
    store.close()


def test_concurrent_migration_tolerates_duplicate_column(tmp_path):
    path = tmp_path / "race.db"
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE tasks (
          id TEXT PRIMARY KEY, context_id TEXT NOT NULL, agent TEXT NOT NULL,
          owner TEXT NOT NULL, state TEXT NOT NULL, status_json TEXT NOT NULL,
          artifacts_json TEXT NOT NULL DEFAULT '[]', metadata_json TEXT,
          created_at REAL NOT NULL, updated_at REAL NOT NULL
        );
        CREATE TABLE outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, task_id TEXT,
          delivery_id TEXT NOT NULL, url TEXT NOT NULL, headers_json TEXT NOT NULL,
          body TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending', last_error TEXT, created_at REAL NOT NULL
        );
    """)
    db.commit()
    db.close()
    errors: list[Exception] = []

    def worker():
        try:
            Store(str(path)).close()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    store = Store(str(path))
    store._ensure_column("tasks", "waiting_on", "TEXT")
    tcols = {r[1] for r in store._x("PRAGMA table_info(tasks)").fetchall()}
    assert "waiting_on" in tcols and "status_text" in tcols
    store.close()


def test_roster_rate_limit_separate_from_rpc(tmp_path):
    bot_port, bridge_port = free_port(), free_port()
    from mock_bot import make_app as make_bot
    from conftest import BOT_TOKEN
    bot = make_bot(BOT_TOKEN, BOT_SECRET)
    env = _roster_env(tmp_path, bot_port, bridge_port,
                      A2A_RATE_LIMIT_PER_MIN="60", A2A_RATE_LIMIT_BURST="3")
    app = create_app(load_settings(env))
    with Server(bot, bot_port), Server(app, bridge_port):
        base = f"http://127.0.0.1:{bridge_port}"
        with httpx.Client(base_url=base, headers={"Authorization": f"Bearer {API_KEY}"}, timeout=10) as c:
            codes = [rpc(c, "tasks/get", {"id": "missing"}).status_code for _ in range(6)]
            assert 429 in codes
            r = c.get("/roster")
            assert r.status_code == 200


def test_roster_stream_global_and_anon_caps(tmp_path):
    s, store, roster = make_roster(tmp_path, A2A_ROSTER_MAX_STREAMS="2",
                                   A2A_ROSTER_MAX_ANON_STREAMS="1",
                                   A2A_ROSTER_MAX_STREAMS_PER_KEY="3")
    assert roster.acquire_stream("a")
    assert roster.acquire_stream("b")
    assert roster.acquire_stream("c") is False
    roster.release_stream("a")
    roster.release_stream("b")
    assert roster.acquire_stream("*")
    assert roster.acquire_stream("*") is False
    roster.release_stream("*")
    store.close()


def test_public_malformed_authorization_is_401(tmp_path):
    bot_port, bridge_port = free_port(), free_port()
    from mock_bot import make_app as make_bot
    from conftest import BOT_TOKEN
    bot = make_bot(BOT_TOKEN, BOT_SECRET)
    env = _roster_env(tmp_path, bot_port, bridge_port, A2A_ROSTER_PUBLIC="true")
    app = create_app(load_settings(env))
    with Server(bot, bot_port), Server(app, bridge_port):
        base = f"http://127.0.0.1:{bridge_port}"
        r = httpx.get(base + "/roster", headers={"Authorization": "Basic abc"})
        assert r.status_code == 401
        r = httpx.get(base + "/roster", headers={"Authorization": "Bearer"})
        assert r.status_code == 401
        r = httpx.get(base + "/roster")
        assert r.status_code == 200
        assert r.json()["viewTier"] == "anonymous"
        r = httpx.get(base + "/roster", params={"status": "idle"})
        assert r.status_code == 403
        r = httpx.get(base + "/roster", params={"availability": "available"})
        assert r.status_code == 200


def test_etag_excludes_status_duration(tmp_path):
    s, store, roster = make_roster(tmp_path)
    key = next(k for k in s.api_keys if k.label == "trust")
    now = time.time()
    roster.recompute(now=now - 10)
    body1, etag1 = roster.snapshot(key, now=now - 10)
    body2, etag2 = roster.snapshot(key, now=now)
    assert etag1 == etag2
    d1 = next(a for a in body1["agents"] if a["id"] == "coder")["statusDurationS"]
    d2 = next(a for a in body2["agents"] if a["id"] == "coder")["statusDurationS"]
    assert d2 >= d1
    store.close()


def test_parse_api_keys_does_not_echo_secret(tmp_path):
    with pytest.raises(ConfigError) as ei:
        parse_api_keys("lab=oops:super-secret-value-abcdefgh")
    assert "super-secret-value" not in str(ei.value)
    with pytest.raises(ConfigError) as ei:
        parse_api_keys("not-an-entry")
    assert "not-an-entry" not in str(ei.value) or "label=" in str(ei.value)


def test_agent_name_stream_is_reserved(tmp_path):
    env = make_env(tmp_path, 1, 2)
    data = json.loads(open(env["A2A_AGENTS_CONFIG"]).read())
    data["agents"][0]["name"] = "stream"
    open(env["A2A_AGENTS_CONFIG"], "w").write(json.dumps(data))
    with pytest.raises(ConfigError, match="reserved"):
        load_settings(env)
