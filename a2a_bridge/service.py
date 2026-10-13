"""Core task logic: create/continue tasks, apply bot callbacks, cancel, push configs,
and background workers (webhook outbox with retry/backoff, stuck-task sweeper)."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import random
import re
import time
from typing import Any

import httpx

from . import models as M
from .config import AgentConfig, Settings
from .db import Store
from .events import EventBus
from .instructions import bot_instructions
from .security import callback_token, check_push_url, sign

log = logging.getLogger("a2a_bridge.service")

# Some upstreams (e.g. some automation-platform webhooks) report rate limiting as HTTP 400
# with a body like "Automation run rate limit exceeded". Treat such responses as retryable.
_RATE_LIMIT_RE = re.compile(r"rate.?limit|too many requests|throttl|try again later", re.I)


class Bridge:
    def __init__(self, settings: Settings, store: Store, bus: EventBus,
                 http_client: httpx.AsyncClient | None = None):
        self.s = settings
        self.store = store
        self.bus = bus
        self.http = http_client
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------ helpers
    def agent(self, name: str) -> AgentConfig:
        return self.s.agents[name]

    def callback_url(self, task_id: str) -> str:
        return f"{self.s.public_base_url}/callback/{task_id}"

    def build_task(self, row: dict, include_history: bool = True) -> dict:
        t = {"kind": "task", "id": row["id"], "contextId": row["context_id"],
             "status": json.loads(row["status_json"]), "artifacts": json.loads(row["artifacts_json"])}
        if include_history:
            t["history"] = self.store.task_history(row["id"])
        if row.get("metadata_json"):
            t["metadata"] = json.loads(row["metadata_json"])
        return t

    def load_task(self, task_id: Any, owner: str, agent: str) -> dict:
        if not isinstance(task_id, str) or not task_id:
            raise M.InvalidParams("task id is required")
        row = self.store.get_task(task_id)
        # do not reveal tasks of other API keys / agents
        if not row or row["owner"] != owner or row["agent"] != agent:
            raise M.TaskNotFound()
        return row

    def _set_status(self, row: dict, state: str, message: dict | None = None,
                    metadata: dict | None = None) -> dict:
        status: dict[str, Any] = {"state": state, "timestamp": M.now_iso()}
        if message:
            status["message"] = message
        self.store.update_task(row["id"], status=status)
        ev = {"kind": "status-update", "taskId": row["id"], "contextId": row["context_id"],
              "status": status, "final": state in M.TERMINAL}
        if metadata:
            ev["metadata"] = metadata
        self._emit(row, ev)
        log.info("task state", extra={"task_id": row["id"], "state": state, "agent": row["agent"]})
        return status

    def _emit(self, row: dict, ev: dict) -> None:
        self.bus.publish(row["id"], ev)
        self._enqueue_push(row["id"], ev)

    def _agent_message(self, row: dict, parts: list[dict]) -> dict:
        msg = {"kind": "message", "messageId": M.new_id(), "role": "agent", "parts": parts,
               "taskId": row["id"], "contextId": row["context_id"]}
        self.store.add_message(msg)
        return msg

    # ------------------------------------------------------------------ inbound A2A
    def send_message(self, agent: str, owner: str, params: dict) -> dict:
        """Create a new task or continue an existing one. Returns the internal task dict."""
        if not isinstance(params, dict):
            raise M.InvalidParams("params must be an object")
        msg = M.normalize_message(params.get("message"))
        if msg["role"] != "user":
            raise M.InvalidParams("message.role must be user")
        cfg = params.get("configuration") or {}
        push_cfg = cfg.get("pushNotificationConfig") or cfg.get("taskPushNotificationConfig")
        if push_cfg:
            # validate before creating anything
            push_cfg = self._normalize_push(push_cfg, "1.0" if "taskPushNotificationConfig" in cfg else "0.3")

        if msg.get("taskId"):
            row = self.load_task(msg["taskId"], owner, agent)
            if msg.get("contextId") and msg["contextId"] != row["context_id"]:
                raise M.InvalidParams("contextId does not match the task's contextId")
            if row["state"] in M.TERMINAL:
                raise M.UnsupportedOperation(f"Task {row['id']} is in terminal state {row['state']}")
            msg["contextId"] = row["context_id"]
            if self.store.message_exists(row["id"], msg["messageId"]):  # idempotent resend
                return self.build_task(self.store.get_task(row["id"]))
            self.store.add_message(msg)
            self._set_status(row, "submitted")
            event = "task.message"
        else:
            ctx = msg.get("contextId")
            if ctx:
                own = self.store.context_owner(ctx)
                if own and own != (agent, owner):
                    raise M.InvalidParams("contextId is not accessible")
            else:
                ctx = M.new_id()
            task_id = M.new_id()
            msg["contextId"], msg["taskId"] = ctx, task_id
            status = {"state": "submitted", "timestamp": M.now_iso()}
            md = params.get("metadata") if isinstance(params.get("metadata"), dict) else None
            self.store.create_task(task_id, ctx, agent, owner, status, md)
            self.store.add_message(msg)
            row = self.store.get_task(task_id)
            event = "task.created"
            log.info("task created", extra={"task_id": task_id, "context_id": ctx, "agent": agent, "owner": owner})

        if push_cfg:
            push_cfg["taskId"] = row["id"]
            self.store.push_put(row["id"], push_cfg)
        row = self.store.get_task(row["id"])
        self._enqueue_bot(row, event, msg)
        return self.build_task(row)

    def cancel(self, agent: str, owner: str, task_id: Any) -> dict:
        row = self.load_task(task_id, owner, agent)
        if row["state"] in M.TERMINAL:
            raise M.TaskNotCancelable(f"Task is already {row['state']}")
        self.store.outbox_cancel_for_task(row["id"], "bot")
        self._set_status(row, "canceled", self._agent_message(row, [{"kind": "text", "text": "Canceled by client"}]))
        row = self.store.get_task(row["id"])
        self._enqueue_bot(row, "task.canceled", None)
        return self.build_task(row)

    # ------------------------------------------------------------------ push configs
    def _normalize_push(self, p: Any, version: str) -> dict:
        if not isinstance(p, dict):
            raise M.InvalidParams("push notification config must be an object")
        inner = p.get("pushNotificationConfig", p)
        url = inner.get("url")
        if not isinstance(url, str):
            raise M.InvalidParams("push notification url is required")
        try:
            check_push_url(url, self.s.push_allow_http, self.s.push_allow_private)
        except ValueError as e:
            raise M.InvalidParams(str(e))
        auth = inner.get("authentication") or {}
        scheme = auth.get("scheme") or ((auth.get("schemes") or [None])[0])
        return {"id": inner.get("id") or M.new_id(), "url": url, "token": inner.get("token"),
                "scheme": scheme, "credentials": auth.get("credentials"), "version": version}

    def push_out(self, cfg: dict, version: str) -> dict:
        if version == "1.0":
            out = {"id": cfg["id"], "taskId": cfg["taskId"], "url": cfg["url"]}
            if cfg.get("token"):
                out["token"] = cfg["token"]
            if cfg.get("scheme"):
                out["authentication"] = {"scheme": cfg["scheme"]}
            return out
        pc = {"id": cfg["id"], "url": cfg["url"]}
        if cfg.get("token"):
            pc["token"] = cfg["token"]
        if cfg.get("scheme"):
            pc["authentication"] = {"schemes": [cfg["scheme"]]}
        return {"taskId": cfg["taskId"], "pushNotificationConfig": pc}

    def push_set(self, agent: str, owner: str, task_id: Any, raw: dict, version: str) -> dict:
        row = self.load_task(task_id, owner, agent)
        cfg = self._normalize_push(raw, version)
        cfg["taskId"] = row["id"]
        self.store.push_put(row["id"], cfg)
        return cfg

    def push_get(self, agent: str, owner: str, task_id: Any, cid: str | None) -> dict:
        row = self.load_task(task_id, owner, agent)
        cfgs = self.store.push_list(row["id"])
        for c in cfgs:
            if cid in (None, c["id"]):
                return c
        raise M.TaskNotFound("Push notification config not found")

    def push_list(self, agent: str, owner: str, task_id: Any) -> list[dict]:
        row = self.load_task(task_id, owner, agent)
        return self.store.push_list(row["id"])

    def push_delete(self, agent: str, owner: str, task_id: Any, cid: Any) -> None:
        row = self.load_task(task_id, owner, agent)
        self.store.push_delete(row["id"], str(cid))

    def _enqueue_push(self, task_id: str, ev: dict) -> None:
        cfgs = self.store.push_list(task_id)
        if not cfgs:
            return
        row = self.store.get_task(task_id)
        for c in cfgs:
            if c.get("version") == "1.0":
                body = M.event_v1(ev)
            else:  # A2A 0.3: POST the full Task object
                body = self.build_task(row, include_history=False)
            headers = {"Content-Type": "application/json"}
            if c.get("credentials") and c.get("scheme"):
                headers["Authorization"] = f"{c['scheme']} {c['credentials']}"
            if c.get("token"):
                headers["X-A2A-Notification-Token"] = c["token"]
            self.store.outbox_add("push", task_id, M.new_id(), c["url"], headers, json.dumps(body))
        self._wake.set()

    # ------------------------------------------------------------------ outbound to bot
    def _enqueue_bot(self, row: dict, event: str, msg: dict | None) -> None:
        a = self.agent(row["agent"])
        token = callback_token(a.shared_secret, row["id"])
        cb = self.callback_url(row["id"])
        delivery_id = M.new_id()
        task = self.build_task(row)
        payload = {
            "event": event,
            "deliveryId": delivery_id,
            "agent": a.name,
            "taskId": row["id"],
            "contextId": row["context_id"],
            "state": row["state"],
            "message": msg,
            "messageText": M.text_of(msg["parts"]) if msg else None,
            "history": self.store.context_history(row["context_id"]),
            "task": task,
            "requiredInputs": a.required_inputs,
            "callback": {"url": cb, "token": token, "method": "POST",
                         "headers": {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}},
            "instructions": bot_instructions(cb, token, row["id"]),
            "sentAt": M.now_iso(),
        }
        auth_val = f"{a.webhook_auth_scheme} {a.webhook_token}" if a.webhook_auth_scheme else a.webhook_token
        headers = {"Content-Type": "application/json", a.webhook_auth_header: auth_val,
                   "X-A2A-Event": event, "X-A2A-Delivery": delivery_id, "X-A2A-Task-Id": row["id"]}
        self.store.outbox_add("bot", row["id"], delivery_id, a.webhook_url, headers, json.dumps(payload))
        self._wake.set()

    # ------------------------------------------------------------------ bot callback
    def apply_callback(self, row: dict, body: Any) -> tuple[int, dict]:
        if not isinstance(body, dict):
            raise M.InvalidParams("callback body must be a JSON object")
        event_id = body.get("eventId")
        if event_id is not None:
            event_id = str(event_id)[:200]
            if self.store.callback_seen(row["id"], event_id):
                return 200, {"ok": True, "duplicate": True, "task": self.build_task(self.store.get_task(row["id"]))}
        try:
            if row["state"] in M.TERMINAL:
                raise _Conflict(f"task is already {row['state']}")
            state = body.get("state")
            if state is not None:
                state = M.state_from_any(str(state))
                if state not in M.BOT_SETTABLE:
                    raise M.InvalidParams(f"state must be one of {sorted(M.BOT_SETTABLE)}")
            msg_parts = M.parts_from_loose(body["message"]) if body.get("message") is not None else None
            arts = body.get("artifacts") or []
            if not isinstance(arts, list):
                raise M.InvalidParams("artifacts must be a list")
            norm_arts = [self._norm_artifact(a) for a in arts]
            if state is None and msg_parts is None and not norm_arts:
                raise M.InvalidParams("send at least one of state, message, artifacts")
        except Exception:
            if event_id is not None:
                self.store.callback_forget(row["id"], event_id)
            raise

        current = json.loads(row["artifacts_json"])
        for art, append, last in norm_arts:
            existing = next((x for x in current if x["artifactId"] == art["artifactId"]), None)
            if existing and append:
                existing["parts"].extend(art["parts"])
            elif existing:
                current[current.index(existing)] = art
            else:
                current.append(art)
            self.store.update_task(row["id"], artifacts=current)
            ev = {"kind": "artifact-update", "taskId": row["id"], "contextId": row["context_id"],
                  "artifact": art, "append": append}
            if last is not None:
                ev["lastChunk"] = bool(last)
            self._emit(row, ev)

        if state is not None or msg_parts is not None:
            new_state = state or (row["state"] if row["state"] != "submitted" else "working")
            m = self._agent_message(row, msg_parts) if msg_parts else None
            md = body.get("metadata") if isinstance(body.get("metadata"), dict) else None
            self._set_status(row, new_state, m, md)
        return 200, {"ok": True, "task": self.build_task(self.store.get_task(row["id"]))}

    def _norm_artifact(self, a: Any) -> tuple[dict, bool, bool | None]:
        if not isinstance(a, dict):
            raise M.InvalidParams("artifact must be an object")
        if "parts" in a:
            parts = M.normalize_parts(a["parts"])
        elif "text" in a:
            parts = [{"kind": "text", "text": str(a["text"])}]
        elif "data" in a:
            parts = [{"kind": "data", "data": a["data"]}]
        else:
            raise M.InvalidParams("artifact needs parts, text or data")
        art = {"artifactId": str(a.get("artifactId") or M.new_id()), "parts": parts}
        for k in ("name", "description", "metadata"):
            if a.get(k) is not None:
                art[k] = a[k]
        return art, bool(a.get("append", False)), a.get("lastChunk")

    # ------------------------------------------------------------------ background workers
    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._outbox_loop(), name="outbox"),
                       asyncio.create_task(self._sweeper_loop(), name="sweeper")]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    async def _outbox_loop(self) -> None:
        while True:
            try:
                rows = self.store.outbox_claim_due()
                if rows:
                    await asyncio.gather(*(self._deliver(r) for r in rows))
                    continue
                nxt = self.store.outbox_next_due()
                timeout = 5.0 if nxt is None else max(0.05, min(5.0, nxt - time.time()))
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("outbox loop error")
                await asyncio.sleep(1)

    async def _deliver(self, r: dict) -> None:
        headers = json.loads(r["headers_json"])
        body = r["body"].encode()
        if r["kind"] == "bot":
            row = self.store.get_task(r["task_id"]) if r["task_id"] else None
            if row:
                headers["X-A2A-Signature"] = sign(self.agent(row["agent"]).shared_secret, body)
        attempts = r["attempts"] + 1
        err = ""
        permanent = False
        resp = None
        try:
            resp = await self.http.post(r["url"], content=body, headers=headers, timeout=self.s.webhook_timeout_s)
            if 200 <= resp.status_code < 300:
                self.store.outbox_done(r["id"], attempts)
                log.info("webhook delivered", extra={"kind": r["kind"], "task_id": r["task_id"],
                                                     "attempt": attempts, "status": resp.status_code})
                if r["kind"] == "bot":
                    self._after_bot_delivery(r["task_id"], resp)
                return
            snippet = resp.text[:300].replace("\n", " ")
            err = f"HTTP {resp.status_code}: {snippet}"
            rate_limited = resp.status_code == 429 or (
                400 <= resp.status_code < 500 and _RATE_LIMIT_RE.search(resp.text[:2000] or ""))
            permanent = 400 <= resp.status_code < 500 and resp.status_code not in (408, 409, 425, 429) \
                and not rate_limited
        except Exception as e:  # network errors, timeouts
            err = f"{type(e).__name__}: {e}"
        if permanent and resp is not None and resp.status_code not in (401, 403, 404) \
                and r.get("client_err_retries", 0) < self.s.webhook_4xx_retries \
                and attempts < self.s.webhook_max_attempts:
            # possibly transient client error (seen in practice): retry after a short delay
            self.store.outbox_retry(r["id"], attempts, time.time() + self.s.webhook_4xx_retry_delay_s, err,
                                    client_err=True)
            log.warning("webhook got 4xx, retrying once more",
                        extra={"kind": r["kind"], "task_id": r["task_id"], "attempt": attempts,
                               "retry_in_s": self.s.webhook_4xx_retry_delay_s, "error": err})
            return
        if permanent or attempts >= self.s.webhook_max_attempts:
            self.store.outbox_dead(r["id"], attempts, err)
            log.error("webhook delivery failed permanently",
                      extra={"kind": r["kind"], "task_id": r["task_id"], "attempt": attempts, "error": err})
            if r["kind"] == "bot" and r["task_id"]:
                row = self.store.get_task(r["task_id"])
                if row and row["state"] not in M.TERMINAL and json.loads(r["body"]).get("event") != "task.canceled":
                    m = self._agent_message(row, [{"kind": "text",
                                                   "text": f"Bridge could not deliver task to bot: {err}"}])
                    self._set_status(row, "failed", m)
            return
        delay = min(self.s.webhook_backoff_max_s, self.s.webhook_backoff_base_s * (2 ** (attempts - 1)))
        delay *= random.uniform(0.8, 1.2)
        if resp is not None and resp.headers.get("retry-after", "").isdigit():
            delay = min(self.s.webhook_backoff_max_s, max(delay, float(resp.headers["retry-after"])))
        self.store.outbox_retry(r["id"], attempts, time.time() + delay, err)
        log.warning("webhook delivery failed, will retry",
                    extra={"kind": r["kind"], "task_id": r["task_id"], "attempt": attempts,
                           "retry_in_s": round(delay, 2), "error": err})

    def _after_bot_delivery(self, task_id: str, resp: httpx.Response) -> None:
        row = self.store.get_task(task_id)
        if not row or row["state"] in M.TERMINAL:
            return
        # The bot MAY answer the webhook synchronously with a callback-shaped JSON body.
        try:
            data = resp.json()
        except Exception:
            data = None
        if isinstance(data, dict) and any(k in data for k in ("state", "artifacts", "message")) \
                and data.get("state") != "submitted":
            try:
                self.apply_callback(row, data)
                return
            except Exception as e:
                log.warning("ignoring invalid inline webhook response", extra={"task_id": task_id, "error": str(e)})
        if row["state"] == "submitted":
            self._set_status(row, "working")

    async def _sweeper_loop(self) -> None:
        last_purge = 0.0
        while True:
            try:
                await asyncio.sleep(self.s.sweep_interval_s)
                self.sweep()
                if time.time() - last_purge > 3600 and self.s.retention_days > 0:
                    n = self.store.purge_terminal(time.time() - self.s.retention_days * 86400)
                    last_purge = time.time()
                    if n:
                        log.info("purged old tasks", extra={"count": n})
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("sweeper error")

    def sweep(self) -> int:
        now = time.time()
        n = 0
        for row in self.store.stale_tasks(("submitted", "working"), now - self.s.task_timeout_s):
            self._set_status(row, "failed", self._agent_message(
                row, [{"kind": "text", "text": f"Task timed out after {self.s.task_timeout_s}s without bot update"}]))
            self._enqueue_bot(self.store.get_task(row["id"]), "task.canceled", None)
            n += 1
        for row in self.store.stale_tasks(("input-required", "auth-required"), now - self.s.input_required_timeout_s):
            self._set_status(row, "canceled", self._agent_message(
                row, [{"kind": "text", "text": "No input received in time; task canceled"}]))
            self._enqueue_bot(self.store.get_task(row["id"]), "task.canceled", None)
            n += 1
        return n


class _Conflict(Exception):
    pass


Conflict = _Conflict


def b64ok(s: str) -> bool:
    try:
        base64.b64decode(s, validate=True)
        return True
    except Exception:
        return False
