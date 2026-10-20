"""Roster: status inference, key scoping, privacy tiers, SSE, and DB migration."""
from __future__ import annotations

import json
import sqlite3
import threading
import time

import httpx
import pytest

from a2a_bridge.app import create_app
from a2a_bridge.config import ConfigError, load_settings
from a2a_bridge.db import Store
from a2a_bridge.events import EventBus
from a2a_bridge.roster import Roster, redact_text, ts_to_iso
from a2a_bridge.security import callback_token
from conftest import API_KEY, BOT_SECRET, LIMITED_KEY, Server, free_port, make_env, rpc, user_msg, wait_state

OWNER_KEY = "owner-key-0123456789abcd"  # gitleaks:allow (test fixture)
TRUSTED_KEY = "trusted-key-0123456789ab"  # gitleaks:allow (test fixture)


def _roster_env(tmp_path, bot_port, bridge_port, **extra):
    keys = (f"main=plain:{API_KEY},lim=plain:{LIMITED_KEY}@other,"
            f"ops=plain:{OWNER_KEY},trust=plain:{TRUSTED_KEY}")
    env = make_env(tmp_path, bot_port, bridge_port,
                   A2A_API_KEYS=keys,
                   A2A_KEY_TIERS="ops:owner,trust:trusted",
                   A2A_ROSTER_DEBOUNCE_S="0.05",
                   A2A_ROSTER_REDACT="secretproject",
                   **extra)
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
        "UPDATE tasks SET state=?, status_json=?, updated_at=?, created_at=?, waiting_on=?, public_activity=? WHERE id=?",
        (state, json.dumps(status), ts, created, waiting_on, public_activity, tid))


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
        a = c.get("/roster/coder").json()["agents"][0]
        assert a["waitingOn"] == "owner"
        assert a["status"] == "input-required"
        row = roster_stack["app"].state.bridge.store.get_task(tid)
        assert row["waiting_on"] == "owner"
        assert row["public_activity"] == "Waiting for review"
        # later progress without waitingOn clears it
        httpx.post(f"{roster_stack['base']}/callback/{tid}",
                   headers={"Authorization": f"Bearer {tok}"},
                   json={"state": "working", "message": "Continuing", "eventId": "cont"})
        row = roster_stack["app"].state.bridge.store.get_task(tid)
        assert row["waiting_on"] is None


def test_roster_etag_and_invalid_status(roster_stack):
    with _c(roster_stack) as c:
        r = c.get("/roster")
        assert r.headers.get("cache-control") == "no-store"
        etag = r.headers["etag"]
        r2 = c.get("/roster", headers={"If-None-Match": etag})
        assert r2.status_code == 304
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
    assert "waiting_on" in tcols and "public_activity" in tcols
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
