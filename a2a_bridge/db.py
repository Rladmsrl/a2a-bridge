"""SQLite persistence. Synchronous sqlite3 (WAL) behind a lock: every call is a few
hundred microseconds, which is fine for a bridge and keeps the code simple.
Run a single uvicorn worker per database file."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

UNSET = object()

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  context_id TEXT NOT NULL,
  agent TEXT NOT NULL,
  owner TEXT NOT NULL,
  state TEXT NOT NULL,
  status_json TEXT NOT NULL,
  artifacts_json TEXT NOT NULL DEFAULT '[]',
  metadata_json TEXT,
  waiting_on TEXT,
  public_activity TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tasks_ctx ON tasks(context_id);
CREATE INDEX IF NOT EXISTS ix_tasks_state ON tasks(state, updated_at);
CREATE INDEX IF NOT EXISTS ix_tasks_owner ON tasks(owner, agent, updated_at);
CREATE INDEX IF NOT EXISTS ix_tasks_agent_state ON tasks(agent, state, updated_at);
CREATE TABLE IF NOT EXISTS messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL,
  context_id TEXT NOT NULL,
  task_id TEXT,
  json TEXT NOT NULL,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_msg_ctx ON messages(context_id, seq);
CREATE INDEX IF NOT EXISTS ix_msg_task ON messages(task_id, seq);
CREATE TABLE IF NOT EXISTS outbox (
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
CREATE INDEX IF NOT EXISTS ix_outbox_due ON outbox(status, next_at);
CREATE TABLE IF NOT EXISTS push_configs (
  task_id TEXT NOT NULL,
  id TEXT NOT NULL,
  json TEXT NOT NULL,
  PRIMARY KEY (task_id, id)
);
CREATE TABLE IF NOT EXISTS callback_events (
  event_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY (task_id, event_id)
);
CREATE TABLE IF NOT EXISTS agent_presence (
  agent TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  since REAL NOT NULL,
  last_seen_at REAL,
  last_delivery_ok_at REAL,
  last_delivery_err_at REAL,
  last_delivery_err TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  throttled_until REAL,
  delivery_dead INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL
);
"""

_PRESENCE_COLS = {
    "status", "since", "last_seen_at", "last_delivery_ok_at", "last_delivery_err_at",
    "last_delivery_err", "consecutive_failures", "throttled_until", "delivery_dead", "updated_at",
}


class Store:
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.execute("PRAGMA busy_timeout=5000")
            self._db.executescript(SCHEMA)
            cols = {r[1] for r in self._db.execute("PRAGMA table_info(outbox)")}
            if "client_err_retries" not in cols:  # migration
                self._db.execute("ALTER TABLE outbox ADD COLUMN client_err_retries INTEGER NOT NULL DEFAULT 0")
            tcols = {r[1] for r in self._db.execute("PRAGMA table_info(tasks)")}
            if "waiting_on" not in tcols:
                self._db.execute("ALTER TABLE tasks ADD COLUMN waiting_on TEXT")
            if "public_activity" not in tcols:
                self._db.execute("ALTER TABLE tasks ADD COLUMN public_activity TEXT")
            pcols = {r[1] for r in self._db.execute("PRAGMA table_info(agent_presence)")}
            if pcols and "delivery_dead" not in pcols:
                self._db.execute(
                    "ALTER TABLE agent_presence ADD COLUMN delivery_dead INTEGER NOT NULL DEFAULT 0")
            # crash recovery: deliveries that were in flight are retried
            self._db.execute("UPDATE outbox SET status='pending' WHERE status='inflight'")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _x(self, sql: str, args: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._db.execute(sql, args)

    def ping(self) -> bool:
        return self._x("SELECT 1").fetchone()[0] == 1

    # ------------------------------------------------------------ tasks
    def create_task(self, task_id: str, context_id: str, agent: str, owner: str, status: dict,
                    metadata: dict | None) -> None:
        t = time.time()
        self._x("INSERT INTO tasks(id,context_id,agent,owner,state,status_json,artifacts_json,metadata_json,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (task_id, context_id, agent, owner, status["state"], json.dumps(status), "[]",
                 json.dumps(metadata) if metadata else None, t, t))

    def get_task(self, task_id: str) -> dict | None:
        r = self._x("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return dict(r) if r else None

    def update_task(self, task_id: str, status: dict | None = None, artifacts: list | None = None,
                    waiting_on: Any = UNSET, public_activity: Any = UNSET) -> None:
        sets, args = ["updated_at=?"], [time.time()]
        if status is not None:
            sets += ["state=?", "status_json=?"]
            args += [status["state"], json.dumps(status)]
        if artifacts is not None:
            sets.append("artifacts_json=?")
            args.append(json.dumps(artifacts))
        if waiting_on is not UNSET:
            sets.append("waiting_on=?")
            args.append(waiting_on)
        if public_activity is not UNSET:
            sets.append("public_activity=?")
            args.append(public_activity)
        args.append(task_id)
        self._x(f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", tuple(args))

    def context_owner(self, context_id: str) -> tuple[str, str] | None:
        r = self._x("SELECT agent, owner FROM tasks WHERE context_id=? LIMIT 1", (context_id,)).fetchone()
        return (r["agent"], r["owner"]) if r else None

    def list_tasks(self, owner: str, agent: str, context_id: str | None, state: str | None,
                   after_ts: float | None, limit: int, cursor: tuple[float, str] | None) -> tuple[list[dict], int]:
        where, args = ["owner=?", "agent=?"], [owner, agent]
        if context_id:
            where.append("context_id=?"); args.append(context_id)
        if state:
            where.append("state=?"); args.append(state)
        if after_ts is not None:
            where.append("updated_at>=?"); args.append(after_ts)
        w = " AND ".join(where)
        total = self._x(f"SELECT COUNT(*) FROM tasks WHERE {w}", tuple(args)).fetchone()[0]
        if cursor:
            w += " AND (updated_at < ? OR (updated_at = ? AND id < ?))"
            args += [cursor[0], cursor[0], cursor[1]]
        rows = self._x(f"SELECT * FROM tasks WHERE {w} ORDER BY updated_at DESC, id DESC LIMIT ?",
                       tuple(args + [limit])).fetchall()
        return [dict(r) for r in rows], total

    def stale_tasks(self, states: tuple[str, ...], older_than: float) -> list[dict]:
        q = ",".join("?" * len(states))
        rows = self._x(f"SELECT * FROM tasks WHERE state IN ({q}) AND updated_at < ?",
                       (*states, older_than)).fetchall()
        return [dict(r) for r in rows]

    def purge_terminal(self, older_than: float) -> int:
        with self._lock:
            ids = [r[0] for r in self._db.execute(
                "SELECT id FROM tasks WHERE state IN ('completed','failed','canceled','rejected') AND updated_at < ?",
                (older_than,)).fetchall()]
            for i in ids:
                self._db.execute("DELETE FROM messages WHERE task_id=?", (i,))
                self._db.execute("DELETE FROM push_configs WHERE task_id=?", (i,))
                self._db.execute("DELETE FROM callback_events WHERE task_id=?", (i,))
                self._db.execute("DELETE FROM tasks WHERE id=?", (i,))
            self._db.execute("DELETE FROM outbox WHERE status IN ('done','dead') AND created_at < ?", (older_than,))
            return len(ids)

    # ------------------------------------------------------------ messages
    def add_message(self, msg: dict) -> None:
        self._x("INSERT INTO messages(message_id,context_id,task_id,json,created_at) VALUES(?,?,?,?,?)",
                (msg["messageId"], msg["contextId"], msg.get("taskId"), json.dumps(msg), time.time()))

    def task_history(self, task_id: str) -> list[dict]:
        return [json.loads(r[0]) for r in
                self._x("SELECT json FROM messages WHERE task_id=? ORDER BY seq", (task_id,)).fetchall()]

    def context_history(self, context_id: str) -> list[dict]:
        return [json.loads(r[0]) for r in
                self._x("SELECT json FROM messages WHERE context_id=? ORDER BY seq", (context_id,)).fetchall()]

    def message_exists(self, task_id: str, message_id: str) -> bool:
        return self._x("SELECT 1 FROM messages WHERE task_id=? AND message_id=?",
                       (task_id, message_id)).fetchone() is not None

    # ------------------------------------------------------------ outbox
    def outbox_add(self, kind: str, task_id: str | None, delivery_id: str, url: str, headers: dict,
                   body: str, delay: float = 0.0) -> int:
        t = time.time()
        cur = self._x("INSERT INTO outbox(kind,task_id,delivery_id,url,headers_json,body,next_at,created_at)"
                      " VALUES(?,?,?,?,?,?,?,?)", (kind, task_id, delivery_id, url, json.dumps(headers), body,
                                                   t + delay, t))
        return cur.lastrowid

    def outbox_claim_due(self, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM outbox WHERE status='pending' AND next_at<=? ORDER BY id LIMIT ?",
                                    (time.time(), limit)).fetchall()
            for r in rows:
                self._db.execute("UPDATE outbox SET status='inflight' WHERE id=?", (r["id"],))
            return [dict(r) for r in rows]

    def outbox_next_due(self) -> float | None:
        r = self._x("SELECT MIN(next_at) FROM outbox WHERE status='pending'").fetchone()
        return r[0]

    def outbox_done(self, oid: int, attempts: int) -> None:
        self._x("UPDATE outbox SET status='done', attempts=?, last_error=NULL WHERE id=?", (attempts, oid))

    def outbox_retry(self, oid: int, attempts: int, next_at: float, err: str, client_err: bool = False) -> None:
        self._x("UPDATE outbox SET status='pending', attempts=?, next_at=?, last_error=?, "
                "client_err_retries=client_err_retries+? WHERE id=?",
                (attempts, next_at, err[:500], 1 if client_err else 0, oid))

    def outbox_dead(self, oid: int, attempts: int, err: str) -> None:
        self._x("UPDATE outbox SET status='dead', attempts=?, last_error=? WHERE id=?", (attempts, err[:500], oid))

    def outbox_cancel_for_task(self, task_id: str, kind: str) -> None:
        self._x("UPDATE outbox SET status='dead', last_error='superseded' WHERE task_id=? AND kind=? "
                "AND status='pending'", (task_id, kind))

    def outbox_stats(self) -> dict:
        rows = self._x("SELECT status, COUNT(*) FROM outbox GROUP BY status").fetchall()
        return {r[0]: r[1] for r in rows}

    # ------------------------------------------------------------ push configs
    def push_put(self, task_id: str, cfg: dict) -> None:
        self._x("INSERT OR REPLACE INTO push_configs(task_id,id,json) VALUES(?,?,?)",
                (task_id, cfg["id"], json.dumps(cfg)))

    def push_get(self, task_id: str, cid: str) -> dict | None:
        r = self._x("SELECT json FROM push_configs WHERE task_id=? AND id=?", (task_id, cid)).fetchone()
        return json.loads(r[0]) if r else None

    def push_list(self, task_id: str) -> list[dict]:
        return [json.loads(r[0]) for r in
                self._x("SELECT json FROM push_configs WHERE task_id=? ORDER BY rowid", (task_id,)).fetchall()]

    def push_delete(self, task_id: str, cid: str) -> None:
        self._x("DELETE FROM push_configs WHERE task_id=? AND id=?", (task_id, cid))

    # ------------------------------------------------------------ idempotency of callbacks
    def callback_seen(self, task_id: str, event_id: str) -> bool:
        """Returns True if this event was already processed; records it otherwise."""
        with self._lock:
            try:
                self._db.execute("INSERT INTO callback_events(event_id,task_id,created_at) VALUES(?,?,?)",
                                 (event_id, task_id, time.time()))
                return False
            except sqlite3.IntegrityError:
                return True

    def callback_forget(self, task_id: str, event_id: str) -> None:
        self._x("DELETE FROM callback_events WHERE task_id=? AND event_id=?", (task_id, event_id))

    # ------------------------------------------------------------ roster / presence
    def presence_ensure(self, agent: str, now: float | None = None) -> None:
        t = time.time() if now is None else now
        self._x("INSERT OR IGNORE INTO agent_presence(agent,status,since,consecutive_failures,"
                "delivery_dead,updated_at) VALUES(?,?,?,?,?,?)", (agent, "idle", t, 0, 0, t))

    def presence_update(self, agent: str, increment_failures: bool = False, **fields: Any) -> None:
        now = fields.pop("updated_at", time.time())
        self.presence_ensure(agent, now)
        sets, args = [], []
        for k, v in fields.items():
            if k not in _PRESENCE_COLS:
                raise ValueError(f"unknown presence column: {k}")
            sets.append(f"{k}=?")
            args.append(v)
        if increment_failures:
            sets.append("consecutive_failures=consecutive_failures+1")
        if not sets:
            return
        sets.append("updated_at=?")
        args.append(now)
        args.append(agent)
        self._x(f"UPDATE agent_presence SET {', '.join(sets)} WHERE agent=?", tuple(args))

    def roster_signals(self, hour_ago: float) -> dict[str, Any]:
        """One lock: active tasks plus the aggregates roster inference needs."""
        with self._lock:
            active = [dict(r) for r in self._db.execute(
                "SELECT id,context_id,agent,owner,state,status_json,waiting_on,public_activity,"
                "created_at,updated_at FROM tasks "
                "WHERE state IN ('submitted','working','input-required','auth-required')")]
            failed = {r[0]: r[1] for r in self._db.execute(
                "SELECT agent, COUNT(*) FROM tasks WHERE state='failed' AND updated_at>=? GROUP BY agent",
                (hour_ago,))}
            last_term = {r[0]: r[1] for r in self._db.execute(
                "SELECT agent, MAX(updated_at) FROM tasks WHERE state IN "
                "('completed','failed','canceled','rejected') GROUP BY agent")}
            pending = {r[0]: r[1] for r in self._db.execute(
                "SELECT t.agent, COUNT(*) FROM outbox o JOIN tasks t ON t.id=o.task_id "
                "WHERE o.kind='bot' AND o.status IN ('pending','inflight') GROUP BY t.agent")}
            delivered_sub = {r[0] for r in self._db.execute(
                "SELECT t.id FROM tasks t JOIN outbox o ON o.task_id=t.id "
                "AND o.kind='bot' AND o.status='done' WHERE t.state='submitted'")}
            presence = {r["agent"]: dict(r) for r in self._db.execute("SELECT * FROM agent_presence")}
        return {"active": active, "failed_hour": failed, "last_terminal": last_term,
                "outbox_pending": pending, "delivered_submitted": delivered_sub, "presence": presence}
