"""Live agent roster: infer status from existing task/delivery data (never probe bots),
project a privacy-tiered view per API key, and publish changes for SSE subscribers.

GET /roster and SSE snapshot/update read a cache that is recomputed in the background
(debounce loop + sweeper tick). Reads do not write presence or recompute.
"""
from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterable

from .config import ApiKey, Settings
from .db import Store
from .events import EventBus

log = logging.getLogger("a2a_bridge.roster")

ROSTER_BUS_KEY = "__roster__"
VALID_STATUSES = ("offline", "stuck", "busy", "input-required", "idle")
VALID_AVAILABILITY = ("available", "limited", "unavailable")
TTL_SECONDS = 5
SUBMITTED_STUCK_S = 300
CONFIDENCE_HIGH_S = 600
CONFIDENCE_MEDIUM_S = 86400
ANON_STREAM_LABEL = "*"
AUTH_DEAD_STATUSES = {401, 403, 404}

_URL_RE = re.compile(r"https?://[^\s]+|www\.[^\s]+", re.I)
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.\w+\b")
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
_HEX_RE = re.compile(r"\b[0-9a-fA-F]{32,}\b")
_REPO_RE = re.compile(r"\b[A-Za-z0-9][A-Za-z0-9._-]+/[A-Za-z0-9._-]+\b")
_MIME_LEFT = {"text", "application", "image", "audio", "video", "multipart", "font", "model"}
# Generic slash pairs that are not org/repo (unit/integration, text/plain, …).
_REPO_SKIP = {
    "unit", "integration", "test", "tests", "src", "lib", "app", "and", "or",
    "read", "write", "input", "output", "text", "plain", "socket", "asp",
    "main", "master", "dev", "bin", "docs", "doc",
}
_IPV4_RE = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b")
# Compressed forms (::1, fe80::1) as well as fully written addresses.
_IPV6_RE = re.compile(r"(?<![\w:])(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}(?![\w:])")
_INTERNAL_HOST_RE = re.compile(
    r"\b(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+(?:local|internal|lan|corp|home|intranet)\b",
    re.I)
# Ordinary DNS names. Two well-known non-host tokens are put back by _redact_host.
_PUBLIC_HOST_RE = re.compile(
    r"\b(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+(?:com|net|org|edu|gov|io|ai|app|dev|cloud|co)\b",
    re.I)
_HOST_SKIP = {"asp.net", "socket.io"}
_GITHUB_TOKEN_RE = re.compile(
    r"\b(?:gh[pours]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b")
_AWS_KEY_RE = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")
# NANP, +86, and 11-digit mobiles (138…). Dates (2026-10-20) and counts (120 200 300) stay.
_PHONE_RE = re.compile(
    r"(?<!\w)(?:"
    r"\+86[\s.-]?1[3-9](?:[\s.-]?\d){9}"
    r"|(?:\+\d{1,3}[\s.-])?(?:\(\d{3}\)[\s.-]?|\d{3}[\s.-])\d{3}[\s.-]\d{4}"
    r"|1[3-9](?:[\s.-]?\d){9}"
    r")(?!\w)")
# Host and email patterns backtrack on long input. Cap before they run; output is 120.
_REDACT_INPUT_MAX = 512


def ts_to_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def iso_to_ts(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def floor_minute_iso(iso: str) -> str:
    ts = iso_to_ts(iso)
    if ts is None:
        return iso
    return ts_to_iso((int(ts) // 60) * 60) or iso


def etag_of(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()
    return f'"{hashlib.sha256(raw).hexdigest()[:16]}"'


def etag_of_roster(body: dict) -> str:
    """ETag ignores request time, cache time, and fields that tick without a real change."""
    skip = {"generatedAt", "computedAt", "stale", "agents"}
    agents = [{k: v for k, v in a.items() if k != "statusDurationS"} for a in body.get("agents", [])]
    payload = {k: v for k, v in body.items() if k not in skip}
    payload["agents"] = agents
    return etag_of(payload)


def compile_redact(patterns: list[str]) -> list[re.Pattern]:
    out: list[re.Pattern] = []
    for p in patterns:
        try:
            out.append(re.compile(p, re.I))
        except re.error:
            out.append(re.compile(re.escape(p), re.I))
    return out


def _redact_ip(m: re.Match) -> str:
    try:
        ip = ipaddress.ip_address(m.group(0))
    except ValueError:
        return m.group(0)
    # Every IPv6 address is sensitive enough for a roster line (public ones included).
    if isinstance(ip, ipaddress.IPv6Address):
        return "[redacted]"
    if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
        return "[redacted]"
    return m.group(0)


def _redact_host(m: re.Match) -> str:
    if m.group(0).lower() in _HOST_SKIP:
        return m.group(0)
    return "[redacted]"


def redact_text(text: str, extra: list[re.Pattern]) -> str:
    if not text:
        return text
    if len(text) > _REDACT_INPUT_MAX:
        text = text[:_REDACT_INPUT_MAX]
    s = _URL_RE.sub("[redacted]", text)
    s = _EMAIL_RE.sub("[redacted]", s)
    s = _JWT_RE.sub("[redacted]", s)
    s = _GITHUB_TOKEN_RE.sub("[redacted]", s)
    s = _AWS_KEY_RE.sub("[redacted]", s)
    s = _UUID_RE.sub("[redacted]", s)
    s = _HEX_RE.sub("[redacted]", s)
    s = _IPV4_RE.sub(_redact_ip, s)
    s = _IPV6_RE.sub(_redact_ip, s)
    s = _INTERNAL_HOST_RE.sub("[redacted]", s)
    s = _PUBLIC_HOST_RE.sub(_redact_host, s)
    s = _PHONE_RE.sub("[redacted]", s)

    def _repo(m: re.Match) -> str:
        left, right = m.group(0).split("/", 1)
        ll, rl = left.lower(), right.lower()
        if ll in _MIME_LEFT:
            return m.group(0)
        # Both sides have to be generic. src/secret-billing is still a path.
        if ll in _REPO_SKIP and rl in _REPO_SKIP:
            return m.group(0)
        return "[redacted]"

    s = _REPO_RE.sub(_repo, s)
    for p in extra:
        s = p.sub("[redacted]", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120].rstrip() if len(s) > 120 else s


def _row_text(row: dict | None) -> str | None:
    if not row:
        return None
    t = row.get("status_text")
    return t or None


def _row_ts(row: dict) -> float:
    return float(row["updated_at"])


def _skills(agent) -> list[dict]:
    out = []
    for sk in agent.skills or []:
        out.append({"id": sk.get("id"), "name": sk.get("name"), "tags": list(sk.get("tags") or [])})
    return out


def _skill_name(agent) -> str:
    if agent.skills:
        return agent.skills[0].get("name") or agent.display_name
    return agent.display_name


def _fp(rec: dict) -> str:
    """Change fingerprint: ignore sub-minute duration (clients have statusSince)."""
    payload = {k: rec[k] for k in rec if k not in ("statusDurationS",)}
    payload["durMin"] = rec.get("statusDurationS", 0) // 60
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def view_fp(projected: dict) -> str:
    payload = {k: v for k, v in projected.items() if k != "statusDurationS"}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


class Roster:
    def __init__(self, settings: Settings, store: Store, bus: EventBus):
        self.s = settings
        self.store = store
        self.bus = bus
        self.seq = 0
        self._last: dict[str, dict] = {}
        self._dirty: set[str] = set()
        self._last_touch = 0.0
        self._dirty_since = 0.0
        self._streams: dict[str, int] = {}
        self._stream_lock = threading.Lock()
        self._computed_at: float | None = None
        self._recompute_lock = asyncio.Lock()
        self._redact = compile_redact(settings.roster_redact)

    # ------------------------------------------------------------------ liveness evidence (never probes)
    def note_seen(self, agent: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        self.store.presence_update(agent, last_seen_at=now)
        self.touch(agent)

    def note_delivery_ok(self, agent: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        self.store.presence_update(agent, last_seen_at=now, last_delivery_ok_at=now,
                                   consecutive_failures=0, failures_since=None,
                                   delivery_dead=0, delivery_dead_at=None, throttled_until=None)
        self.touch(agent)

    def note_delivery_err(self, agent: str, err: str, *, dead: bool, rate_limited: bool,
                          retry_in_s: float = 0.0, now: float | None = None,
                          http_status: int | None = None) -> None:
        now = time.time() if now is None else now
        # A 429 (or a body that says rate limit) only pauses delivery. It is not a failure streak.
        if rate_limited:
            self.store.presence_update(agent, throttled_until=now + max(float(retry_in_s), 1.0),
                                       updated_at=now)
            self.touch(agent)
            return
        prev = self.store.presence_get(agent)
        prev_err = prev.get("last_delivery_err_at") if prev else None
        if prev_err is not None and (now - float(prev_err)) > max(1, self.s.roster_offline_ttl_s):
            self.store.presence_update(agent, consecutive_failures=0, failures_since=None, updated_at=now)
        fields: dict[str, Any] = {"last_delivery_err_at": now, "last_delivery_err": (err or "")[:200],
                                  "updated_at": now}
        # Auth failures record their own timestamp. Other errors must not refresh it.
        if dead and http_status in AUTH_DEAD_STATUSES:
            fields["delivery_dead"] = 1
            fields["delivery_dead_at"] = now
        self.store.presence_update(agent, increment_failures=True, **fields)
        self.touch(agent)

    def touch(self, agent: str) -> None:
        if agent:
            if not self._dirty:
                self._dirty_since = time.monotonic()
            self._dirty.add(agent)
            self._last_touch = time.monotonic()

    def tick(self) -> None:
        self.recompute()

    def cache_limit_s(self) -> float:
        return max(float(self.s.sweep_interval_s), 1.0) * 3

    def is_stale(self, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        if self._computed_at is None:
            return True
        return (now - self._computed_at) > self.cache_limit_s()

    def last(self, name: str) -> dict | None:
        return self._last.get(name)

    def with_duration(self, rec: dict, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        out = dict(rec)
        out["statusDurationS"] = max(0, int(now - rec["since"]))
        return out

    # ------------------------------------------------------------------ streaming caps
    def acquire_stream(self, label: str) -> bool:
        per_key = max(1, self.s.roster_max_streams_per_key)
        global_cap = max(1, self.s.roster_max_streams)
        anon_cap = max(1, self.s.roster_max_anon_streams)
        with self._stream_lock:
            total = sum(self._streams.values())
            if total >= global_cap:
                return False
            if label == ANON_STREAM_LABEL and self._streams.get(ANON_STREAM_LABEL, 0) >= anon_cap:
                return False
            n = self._streams.get(label, 0)
            if n >= per_key:
                return False
            self._streams[label] = n + 1
            return True

    def release_stream(self, label: str) -> None:
        with self._stream_lock:
            n = self._streams.get(label, 0) - 1
            if n <= 0:
                self._streams.pop(label, None)
            else:
                self._streams[label] = n

    async def loop(self) -> None:
        while True:
            try:
                delay = 0.05 if self.s.roster_debounce_s <= 0.05 else min(0.2, self.s.roster_debounce_s)
                await asyncio.sleep(delay)
                if not self._dirty:
                    continue
                nowm = time.monotonic()
                quiet = nowm - self._last_touch
                age = nowm - self._dirty_since
                if quiet < self.s.roster_debounce_s and age < self.s.roster_debounce_max_s:
                    continue
                await self.recompute_async(set(self._dirty))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("roster loop error")

    # ------------------------------------------------------------------ scope
    def visible_names(self, key: ApiKey) -> list[str]:
        """Always intersect with key.agents (owner included). Non-owner also hides roster_visible=false."""
        names = list(self.s.agents)
        if key.agents is not None:
            names = [n for n in names if n in key.agents]
        if key.tier != "owner":
            names = [n for n in names if self.s.agents[n].roster_visible]
        return names

    # ------------------------------------------------------------------ infer + persist
    def recompute(self, names: Iterable[str] | None = None, now: float | None = None) -> dict[str, dict]:
        """Synchronous recompute. Publishes on this thread; use from tests and startup."""
        now = time.time() if now is None else now
        recs, changed = self._compute(names, dict(self._last), now)
        self._apply(recs, changed, now, publish=True, full=names is None)
        return recs

    async def recompute_async(self, names: Iterable[str] | None = None) -> dict[str, dict]:
        """One recompute at a time. DB work is off the event loop; publish stays on it.

        Agents marked dirty while this run holds the lock are left for the next run.
        A result that started before the cache's current computation is dropped.
        """
        async with self._recompute_lock:
            if names is None:
                targets: set[str] | None = None
                pending = set(self._dirty)
                self._dirty.clear()
            else:
                targets = set(names) | set(self._dirty)
                pending = set(targets)
                self._dirty.difference_update(targets)
            started = time.time()
            prev = dict(self._last)
            try:
                recs, changed = await asyncio.to_thread(
                    self._compute, None if targets is None else list(targets), prev, started)
            except Exception:
                self._dirty.update(pending)
                raise
            if not self._apply(recs, changed, started, publish=True, full=targets is None):
                self._dirty.update(pending)
            return recs

    def _apply(self, recs: dict[str, dict], changed: list[str], started: float, *,
               publish: bool, full: bool) -> bool:
        """Install `recs` when this run started after the cache was last fully computed.

        `_computed_at` moves only on a full recompute, so a partial run cannot hide a
        failing full recompute from the stale-cache check.
        """
        if self._computed_at is not None and started < self._computed_at:
            return False
        self._last.update(recs)
        if full:
            self._computed_at = started
        if not publish:
            return True
        for name in changed:
            self.seq += 1
            self.bus.publish(ROSTER_BUS_KEY, {"agent": name})
        return True

    def _compute(self, names: Iterable[str] | None, prev: dict[str, dict],
                 now: float) -> tuple[dict[str, dict], list[str]]:
        sig = self.store.roster_signals(now - 3600)
        targets = list(self.s.agents) if names is None else [n for n in names if n in self.s.agents]
        out: dict[str, dict] = {}
        changed: list[str] = []
        for name in targets:
            presence = self._expire_auth_lock(name, sig["presence"].get(name) or {}, now)
            presence = self._expire_failure_streak(name, presence, now)
            sig["presence"][name] = presence
            old = prev.get(name)
            rec = self._infer(name, now, sig, old)
            if old is None or old.get("status") != rec["status"] or old.get("since") != rec["since"]:
                self.store.presence_update(name, status=rec["status"], since=rec["since"])
            if rec["status"] in ("stuck", "offline") and (old or {}).get("status") != rec["status"]:
                log.warning("roster %s", rec["status"], extra={"agent": name})
            out[name] = rec
            if old is None or _fp(old) != _fp(rec):
                changed.append(name)
        return out, changed

    def _expire_auth_lock(self, name: str, presence: dict, now: float) -> dict:
        """Drop delivery_dead once its own timestamp (not the latest error) is past TTL."""
        if not presence.get("delivery_dead"):
            return presence
        dead_at = presence.get("delivery_dead_at")
        ttl = max(1, self.s.roster_offline_ttl_s)
        if dead_at is not None and (now - float(dead_at)) <= ttl:
            return presence
        if not self.store.presence_clear_auth_lock(name, dead_at):
            return self.store.presence_get(name) or presence
        presence = dict(presence)
        presence["delivery_dead"] = 0
        presence["delivery_dead_at"] = None
        return presence

    def _expire_failure_streak(self, name: str, presence: dict, now: float) -> dict:
        """A quiet stretch longer than the offline TTL ends the failure streak."""
        last_err = presence.get("last_delivery_err_at")
        consec = presence.get("consecutive_failures")
        if not consec or last_err is None:
            return presence
        if (now - float(last_err)) <= max(1, self.s.roster_offline_ttl_s):
            return presence
        if not self.store.presence_clear_failure_streak(name, int(consec), last_err):
            return self.store.presence_get(name) or presence
        presence = dict(presence)
        presence["consecutive_failures"] = 0
        presence["failures_since"] = None
        return presence

    def snapshot(self, key: ApiKey, *, agent_filter: list[str] | None = None,
                 status_filter: set[str] | None = None, availability_filter: set[str] | None = None,
                 include_tasks: bool = False, now: float | None = None) -> tuple[dict, str]:
        """Serve from cache only. Duration is derived from `since` at request time."""
        now = time.time() if now is None else now
        names = self.visible_names(key)
        if agent_filter:
            want = set(agent_filter)
            names = [n for n in names if n in want]
        agents = []
        for n in names:
            rec = self._last.get(n)
            if rec is None:
                continue
            live = self.with_duration(rec, now)
            if status_filter and live["status"] not in status_filter:
                continue
            if availability_filter and live["availability"] not in availability_filter:
                continue
            agents.append(self.project(live, key, include_tasks=include_tasks))
        stale = self.is_stale(now)
        body = {
            "generatedAt": ts_to_iso(now),
            "computedAt": ts_to_iso(self._computed_at),
            "stale": stale,
            "viewTier": key.tier,
            "ttlSeconds": TTL_SECONDS,
            "stream": f"{self.s.public_base_url}/roster/stream",
            "agents": agents,
        }
        return body, etag_of_roster(body)

    def _infer(self, name: str, now: float, sig: dict, prev: dict | None = None) -> dict:
        agent = self.s.agents[name]
        tasks = [t for t in sig["active"] if t["agent"] == name]
        presence = sig["presence"].get(name) or {}
        stuck_after = agent.stuck_after_s or self.s.roster_stuck_after_s
        max_c = agent.max_concurrent
        by: dict[str, list] = {"submitted": [], "working": [], "input-required": [], "auth-required": []}
        for t in tasks:
            by.setdefault(t["state"], []).append(t)
        working = by["working"]
        submitted = by["submitted"]
        interrupted = by["input-required"] + by["auth-required"]
        working_owner = [t for t in working if t.get("waiting_on") == "owner"]
        working_real = [t for t in working if t.get("waiting_on") != "owner"]
        oldest_sub = min((t["updated_at"] for t in submitted), default=None)
        last_seen = presence.get("last_seen_at")
        last_err = presence.get("last_delivery_err_at")
        throttled_until = presence.get("throttled_until")
        throttled = bool(throttled_until and throttled_until > now)
        consec = int(presence.get("consecutive_failures") or 0)
        dead = bool(presence.get("delivery_dead"))
        dead_at = presence.get("delivery_dead_at")
        ttl = max(1, self.s.roster_offline_ttl_s)
        failures_since = presence.get("failures_since")
        auth_lock = bool(dead and dead_at is not None and (now - float(dead_at)) <= ttl
                         and (last_seen is None or float(last_seen) <= float(dead_at)))
        err_fresh = last_err is None or (now - float(last_err)) <= ttl
        streak_aged = failures_since is not None and (now - float(failures_since)) >= self.s.roster_offline_after_s
        sub_aged = oldest_sub is not None and (now - oldest_sub) >= self.s.roster_offline_after_s
        seen_after_err = last_seen is not None and last_err is not None and float(last_seen) > float(last_err)
        fail_lock = consec >= 3 and err_fresh and (streak_aged or sub_aged) and not seen_after_err

        status = "idle"
        if auth_lock or fail_lock:
            status = "offline"
        else:
            stale = [t for t in working_real if now - t["updated_at"] > stuck_after]
            delivered_stuck = [t for t in submitted
                               if t["id"] in sig["delivered_submitted"]
                               and now - t["updated_at"] > SUBMITTED_STUCK_S]
            if stale or delivered_stuck:
                status = "stuck"
            elif working_real:
                status = "busy"
            elif interrupted or working_owner:
                status = "input-required"

        waiting_on = None
        if status == "input-required":
            wos = [t.get("waiting_on") for t in tasks if t.get("waiting_on") in ("requester", "owner", "auth")]
            if "owner" in wos or working_owner:
                waiting_on = "owner"
            elif "auth" in wos or by["auth-required"]:
                waiting_on = "auth"
            else:
                waiting_on = "requester"

        active_n = len(working)
        queued = len(submitted) + max(0, active_n - max_c)
        capacity = {"maxConcurrent": max_c, "active": active_n, "queued": queued, "throttled": throttled}
        if status in ("offline", "stuck") or throttled or active_n >= max_c:
            availability = "unavailable"
        elif status == "busy" or queued > 0:
            availability = "limited"
        else:
            availability = "available"

        if last_seen is None:
            confidence = "low"
        elif now - last_seen <= CONFIDENCE_HIGH_S:
            confidence = "high"
        elif now - last_seen <= CONFIDENCE_MEDIUM_S:
            confidence = "medium"
        else:
            confidence = "low"

        primary = self._primary(status, tasks, stuck_after, now, sig["delivered_submitted"])
        public = (primary.get("public_activity") if primary else None) or None
        raw = _row_text(primary)
        skill = _skill_name(agent)
        activity = self._activity(status, waiting_on, public)
        if not activity and status in ("busy", "stuck"):
            activity = f"Working on a {skill} task"
        elif not activity and status == "input-required":
            activity = {"owner": "Waiting for owner approval", "auth": "Waiting for authentication"
                        }.get(waiting_on or "", "Waiting for requester input")

        own_activity: dict[str, str] = {}
        for t in sorted(tasks, key=lambda x: x["updated_at"]):
            txt = _row_text(t)
            if txt:
                own_activity[t["owner"]] = txt

        since = self._since(status, prev, presence, now, tasks, sig["last_terminal"].get(name))
        counts = {k: len(by[k]) for k in ("submitted", "working", "input-required", "auth-required")}
        task_rows = []
        for t in tasks:
            is_stuck = (t["state"] == "working" and t.get("waiting_on") != "owner"
                        and now - t["updated_at"] > stuck_after)
            task_rows.append({"taskId": t["id"], "owner": t["owner"], "state": t["state"],
                              "updatedAt": ts_to_iso(t["updated_at"]), "stuck": is_stuck})
        delivery = {
            "lastOkAt": ts_to_iso(presence.get("last_delivery_ok_at")),
            "lastErrorAt": ts_to_iso(presence.get("last_delivery_err_at")),
            "lastError": presence.get("last_delivery_err"),
            "consecutiveFailures": consec,
            "outboxPending": int(sig["outbox_pending"].get(name, 0)),
        }
        return {
            "id": name,
            "name": agent.display_name,
            "url": f"{self.s.public_base_url}/agents/{name}",
            "agentCard": f"{self.s.public_base_url}/agents/{name}/.well-known/agent-card.json",
            "skills": _skills(agent),
            "status": status,
            "since": since,
            "statusDurationS": max(0, int(now - since)),
            "availability": availability,
            "capacity": capacity,
            "activity": activity,
            "rawActivity": raw,
            "waitingOn": waiting_on,
            "lastSeen": last_seen,
            "confidence": confidence,
            "own_activity": own_activity,
            "detail": {
                "counts": counts,
                "failedLastHour": int(sig["failed_hour"].get(name, 0)),
                "delivery": delivery,
                "rawActivity": raw,
                "tasks": task_rows,
            },
        }

    def _primary(self, status: str, tasks: list[dict], stuck_after: int, now: float,
                 delivered_sub: set) -> dict | None:
        if status == "stuck":
            stale = [t for t in tasks if t["state"] == "working" and t.get("waiting_on") != "owner"
                     and now - t["updated_at"] > stuck_after]
            delivered = [t for t in tasks if t["state"] == "submitted" and t["id"] in delivered_sub
                         and now - t["updated_at"] > SUBMITTED_STUCK_S]
            pool = stale or delivered
            return min(pool, key=lambda t: t["updated_at"], default=None)
        if status == "busy":
            real = [t for t in tasks if t["state"] == "working" and t.get("waiting_on") != "owner"]
            return max(real, key=lambda t: t["updated_at"], default=None)
        if status == "input-required":
            cand = [t for t in tasks if t["state"] in ("input-required", "auth-required")
                    or (t["state"] == "working" and t.get("waiting_on") == "owner")]
            return max(cand, key=lambda t: t["updated_at"], default=None)
        return None

    def _since(self, status: str, prev: dict | None, presence: dict, now: float,
               tasks: list[dict], last_terminal: float | None) -> float:
        if prev and prev.get("status") == status:
            return float(prev["since"])
        if prev and prev.get("status") != status:
            return now
        if presence.get("status") == status and presence.get("since") is not None:
            return float(presence["since"])
        if status in ("busy", "stuck"):
            working = [t for t in tasks if t["state"] == "working"]
            if working:
                return min(_row_ts(t) for t in working)
        if status == "input-required":
            cand = [t for t in tasks if t["state"] in ("input-required", "auth-required")
                    or t.get("waiting_on") == "owner"]
            if cand:
                return min(_row_ts(t) for t in cand)
        if status == "offline":
            for key in ("delivery_dead_at", "failures_since", "last_delivery_err_at"):
                if presence.get(key) is not None:
                    return float(presence[key])
            return now
        if status == "idle":
            return float(last_terminal) if last_terminal else now
        return now

    def _activity(self, status: str, waiting_on: str | None, public: str | None) -> str | None:
        if public:
            return redact_text(public, self._redact)
        if status not in ("busy", "stuck", "input-required"):
            return None
        if waiting_on == "owner":
            return "Waiting for owner approval"
        if waiting_on == "auth":
            return "Waiting for authentication"
        if status == "input-required":
            return "Waiting for requester input"
        return None  # caller fills skill template

    def project(self, rec: dict, key: ApiKey, include_tasks: bool = False) -> dict:
        tier = key.tier
        out: dict[str, Any] = {
            "id": rec["id"], "name": rec["name"], "url": rec["url"],
            "agentCard": rec["agentCard"], "skills": rec["skills"],
            "availability": rec["availability"],
        }
        if tier == "anonymous":
            return out
        dur = int(rec["statusDurationS"])
        if tier == "basic":
            out["statusDurationS"] = (dur // 60) * 60
            out["capacity"] = {"queued": rec["capacity"]["queued"] > 0}
            return out
        out["status"] = rec["status"]
        out["statusSince"] = ts_to_iso(rec["since"])
        out["statusDurationS"] = dur
        out["capacity"] = dict(rec["capacity"])
        out["waitingOn"] = rec["waitingOn"]
        own = (rec.get("own_activity") or {}).get(key.label)
        out["activity"] = own if own else rec["activity"]
        last = ts_to_iso(rec["lastSeen"])
        if last and tier == "trusted":
            last = floor_minute_iso(last)
        out["lastSeen"] = last
        out["confidence"] = rec["confidence"]
        if tier == "owner":
            detail = {
                "counts": rec["detail"]["counts"],
                "failedLastHour": rec["detail"]["failedLastHour"],
                "delivery": rec["detail"]["delivery"],
                "rawActivity": rec["detail"]["rawActivity"],
            }
            if include_tasks:
                detail["tasks"] = rec["detail"]["tasks"]
            out["detail"] = detail
        return out
