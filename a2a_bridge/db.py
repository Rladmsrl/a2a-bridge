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


def _text_from_status(status: dict) -> str | None:
    msg = status.get("message")
    if not isinstance(msg, dict):
        return None
    texts = [p.get("text") for p in (msg.get("parts") or [])
             if isinstance(p, dict) and p.get("kind") == "text" and isinstance(p.get("text"), str) and p["text"]]
    if not texts:
        return None
    t = "\n".join(texts)
    return t[:500]

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
  status_text TEXT,
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
CREATE INDEX IF NOT EXISTS ix_outbox_task ON outbox(task_id);
CREATE INDEX IF NOT EXISTS ix_outbox_kind_status ON outbox(kind, status);
-- Covering lookup for "does this task have a done bot delivery?" Without it the
-- planner uses ix_outbox_kind_status and scans every done row per submitted task.
CREATE INDEX IF NOT EXISTS ix_outbox_task_kind_status ON outbox(task_id, kind, status);
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
  delivery_dead_at REAL,
  failures_since REAL,
  last_terminal_at REAL,
  updated_at REAL NOT NULL
);
"""

_PRESENCE_COLS = {
    "status", "since", "last_seen_at", "last_delivery_ok_at", "last_delivery_err_at",
    "last_delivery_err", "consecutive_failures", "throttled_until", "delivery_dead",
    "delivery_dead_at", "failures_since", "last_terminal_at", "updated_at",
}


# Pinned plans. A database that never ran ANALYZE (the production default) otherwise
# picks ix_outbox_kind_status for the delivered lookup, and after ANALYZE the pending
# count can fall back to a full outbox scan.
SQL_DELIVERED_SUBMITTED = (
    "SELECT t.id FROM tasks t WHERE t.state='submitted' AND EXISTS ("
    "SELECT 1 FROM outbox o INDEXED BY ix_outbox_task_kind_status "
    "WHERE o.task_id=t.id AND o.kind='bot' AND o.status='done')"
)
SQL_OUTBOX_PENDING = (
    "SELECT t.agent, COUNT(*) FROM outbox o INDEXED BY ix_outbox_kind_status "
    "JOIN tasks t ON t.id=o.task_id "
    "WHERE o.kind='bot' AND o.status IN ('pending','inflight') GROUP BY t.agent"
)


class Store:
    def __init__(self, path: str):
        self._read_db: sqlite3.Connection | None = None
        self._lock = threading.RLock()
        self._read_lock = threading.Lock()
        self._memory = path == ":memory:"
        if self._memory:
            # A second connection only shares an in-memory database via a shared-cache URI.
            self._path = f"file:bridge-{id(self)}?mode=memory&cache=shared"
            self._db = sqlite3.connect(self._path, uri=True, check_same_thread=False, isolation_level=None)
        else:
            self._path = path
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._init_writer()
        self._read_db = self._open_reader()

    def _connect(self, *, query_only: bool) -> sqlite3.Connection:
        if self._memory:
            conn = sqlite3.connect(self._path, uri=True, check_same_thread=False, isolation_level=None)
        else:
            conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        if query_only:
            conn.execute("PRAGMA query_only=ON")
        return conn

    def _rollback_quiet(self) -> None:
        try:
            self._db.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def _init_writer(self) -> None:
        """Open WAL and migrate. Retry if several processes switch the file to WAL at once."""
        delay = 0.02
        for attempt in range(8):
            try:
                with self._lock:
                    self._db.execute("PRAGMA busy_timeout=5000")
                    self._db.execute("PRAGMA journal_mode=WAL").fetchone()
                    self._db.execute("PRAGMA synchronous=NORMAL")
                    self._db.executescript(SCHEMA)
                    self._migrate()
                return
            except sqlite3.OperationalError as e:
                self._rollback_quiet()
                if "locked" not in str(e).lower() or attempt == 7:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.5)

    def _open_reader(self) -> sqlite3.Connection:
        """Read-only connection for background roster queries.

        The event-loop connection and its lock stay free while a long read runs.
        """
        delay = 0.02
        for attempt in range(8):
            try:
                return self._connect(query_only=True)
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or attempt == 7:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
        raise sqlite3.OperationalError("database is locked")

    def _migrate(self) -> None:
        """Idempotent column/index adds. BEGIN IMMEDIATE serializes concurrent startups."""
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._ensure_column("outbox", "client_err_retries", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("tasks", "waiting_on", "TEXT")
            self._ensure_column("tasks", "public_activity", "TEXT")
            self._ensure_column("tasks", "status_text", "TEXT")
            self._ensure_column("agent_presence", "delivery_dead", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("agent_presence", "delivery_dead_at", "REAL")
            self._ensure_column("agent_presence", "failures_since", "REAL")
            self._ensure_column("agent_presence", "last_terminal_at", "REAL")
            self._db.execute("CREATE INDEX IF NOT EXISTS ix_outbox_task ON outbox(task_id)")
            self._db.execute("CREATE INDEX IF NOT EXISTS ix_outbox_kind_status ON outbox(kind, status)")
            self._db.execute(
                "CREATE INDEX IF NOT EXISTS ix_outbox_task_kind_status ON outbox(task_id, kind, status)")
            self._db.execute("UPDATE outbox SET status='pending' WHERE status='inflight'")
            self._backfill_status_text()
            self._backfill_presence()
            self._db.execute("COMMIT")
        except Exception:
            try:
                self._db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def _ensure_column(self, table: str, name: str, decl: str) -> None:
        if table not in {"tasks", "outbox", "agent_presence"} or not name.replace("_", "").isalnum():
            raise ValueError("invalid migration target")
        cols = {r[1] for r in self._db.execute(f"PRAGMA table_info({table})")}
        if name in cols:
            return
        try:
            self._db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise

    def _backfill_status_text(self) -> None:
        """Fill status_text once. Terminal rows are marked and not parsed.

        An empty string means "already considered", so the next boot does not walk
        those rows again. Non-terminal text is extracted inside SQLite; the full
        status_json (which may carry file bytes) is never loaded into Python.
        """
        self._db.execute(
            "UPDATE tasks SET status_text='' WHERE status_text IS NULL "
            "AND state IN ('completed','failed','canceled','rejected')")
        # Anything that is not a JSON array of parts cannot be extracted. Mark it so
        # startup does not parse it again, and so json_each is never handed a string.
        self._db.execute(
            "UPDATE tasks SET status_text='' WHERE status_text IS NULL AND NOT ("
            "json_valid(status_json)=1 AND json_type(status_json, '$.message.parts')='array')")
        self._db.execute(
            "UPDATE tasks SET status_text=COALESCE(("
            "SELECT substr(group_concat(json_extract(j.value, '$.text'), char(10)), 1, 500) "
            "FROM json_each(json_extract(tasks.status_json, '$.message.parts')) AS j "
            "WHERE json_extract(j.value, '$.kind')='text' "
            "AND json_type(j.value, '$.text')='text' "
            "AND json_extract(j.value, '$.text')!=''"
            "), '') WHERE status_text IS NULL")

    def _backfill_presence(self) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO agent_presence(agent,status,since,consecutive_failures,delivery_dead,"
            "updated_at,last_terminal_at) "
            "SELECT agent,'idle',MAX(updated_at),0,0,MAX(updated_at),MAX(updated_at) FROM tasks "
            "WHERE state IN ('completed','failed','canceled','rejected') GROUP BY agent")
        self._db.execute(
            "UPDATE agent_presence SET last_terminal_at=("
            "SELECT MAX(updated_at) FROM tasks WHERE tasks.agent=agent_presence.agent "
            "AND tasks.state IN ('completed','failed','canceled','rejected')) "
            "WHERE last_terminal_at IS NULL AND EXISTS ("
            "SELECT 1 FROM tasks WHERE tasks.agent=agent_presence.agent "
            "AND tasks.state IN ('completed','failed','canceled','rejected'))")
        self._db.execute(
            "UPDATE agent_presence SET delivery_dead_at=last_delivery_err_at "
            "WHERE delivery_dead=1 AND delivery_dead_at IS NULL AND last_delivery_err_at IS NOT NULL")
        self._db.execute(
            "UPDATE agent_presence SET failures_since=last_delivery_err_at "
            "WHERE consecutive_failures>=3 AND failures_since IS NULL AND last_delivery_err_at IS NOT NULL")

    def close(self) -> None:
        with self._read_lock:
            if self._read_db is not None:
                self._read_db.close()
                self._read_db = None
        with self._lock:
            self._db.close()

    def _x(self, sql: str, args: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._db.execute(sql, args)

    def _read(self, sql: str, args: tuple = ()) -> list:
        """Query on the read-only connection. Does not take the writer lock."""
        with self._read_lock:
            db = self._read_db
            if db is None:
                raise sqlite3.ProgrammingError("store is closed")
            return db.execute(sql, args).fetchall()

    def ping(self) -> bool:
        return self._x("SELECT 1").fetchone()[0] == 1

    # ------------------------------------------------------------ tasks
    def create_task(self, task_id: str, context_id: str, agent: str, owner: str, status: dict,
                    metadata: dict | None) -> None:
        t = time.time()
        self._x("INSERT INTO tasks(id,context_id,agent,owner,state,status_json,artifacts_json,metadata_json,"
                "status_text,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, context_id, agent, owner, status["state"], json.dumps(status), "[]",
                 json.dumps(metadata) if metadata else None, _text_from_status(status), t, t))

    def get_task(self, task_id: str) -> dict | None:
        r = self._x("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return dict(r) if r else None

    def update_task(self, task_id: str, status: dict | None = None, artifacts: list | None = None,
                    waiting_on: Any = UNSET, public_activity: Any = UNSET) -> None:
        sets, args = ["updated_at=?"], [time.time()]
        if status is not None:
            sets += ["state=?", "status_json=?", "status_text=?"]
            args += [status["state"], json.dumps(status), _text_from_status(status)]
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
    def presence_get(self, agent: str) -> dict | None:
        r = self._x("SELECT * FROM agent_presence WHERE agent=?", (agent,)).fetchone()
        return dict(r) if r else None

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
            sets.append("failures_since=COALESCE(failures_since, ?)")
            args.append(now)
        if not sets:
            return
        sets.append("updated_at=?")
        args.append(now)
        args.append(agent)
        self._x(f"UPDATE agent_presence SET {', '.join(sets)} WHERE agent=?", tuple(args))

    def presence_clear_auth_lock(self, agent: str, delivery_dead_at: float | None) -> bool:
        """Clear the auth lock only if it is still the snapshot the caller decided on."""
        cur = self._x(
            "UPDATE agent_presence SET delivery_dead=0, delivery_dead_at=NULL, updated_at=? "
            "WHERE agent=? AND delivery_dead=1 AND delivery_dead_at IS ?",
            (time.time(), agent, delivery_dead_at))
        return cur.rowcount > 0

    def presence_clear_failure_streak(self, agent: str, consecutive_failures: int,
                                      last_delivery_err_at: float | None) -> bool:
        """Reset the failure counter only if both the count and the last error are unchanged."""
        cur = self._x(
            "UPDATE agent_presence SET consecutive_failures=0, failures_since=NULL, updated_at=? "
            "WHERE agent=? AND consecutive_failures=? AND last_delivery_err_at IS ?",
            (time.time(), agent, int(consecutive_failures), last_delivery_err_at))
        return cur.rowcount > 0

    def roster_signals(self, hour_ago: float) -> dict[str, Any]:
        """Cheap aggregates for roster inference. Does not load status_json (may contain files).

        Uses the read-only connection so a slow aggregate cannot block event-loop reads.
        """
        active = [dict(r) for r in self._read(
            "SELECT id,agent,owner,state,waiting_on,public_activity,status_text,created_at,updated_at "
            "FROM tasks WHERE state IN ('submitted','working','input-required','auth-required')")]
        failed = {r[0]: r[1] for r in self._read(
            "SELECT agent, COUNT(*) FROM tasks WHERE state='failed' AND updated_at>=? GROUP BY agent",
            (hour_ago,))}
        presence = {r["agent"]: dict(r) for r in self._read("SELECT * FROM agent_presence")}
        last_term = {a: p["last_terminal_at"] for a, p in presence.items()
                     if p.get("last_terminal_at") is not None}
        pending = {r[0]: r[1] for r in self._read(SQL_OUTBOX_PENDING)}
        delivered_sub = {r[0] for r in self._read(SQL_DELIVERED_SUBMITTED)}
        return {"active": active, "failed_hour": failed, "last_terminal": last_term,
                "outbox_pending": pending, "delivered_submitted": delivered_sub, "presence": presence}
