"""A2A data model helpers.

Internally the bridge stores everything in the A2A v0.3 JSON shape
(`kind` discriminators, lower-case kebab states, roles "user"/"agent").
Clients speaking A2A v1.0 (method names like `SendMessage`) get responses
converted to the v1.0 ProtoJSON shape (TASK_STATE_*, ROLE_*, oneof parts,
StreamResponse wrappers)."""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any

STATES = {"submitted", "working", "input-required", "completed", "failed", "canceled",
          "rejected", "auth-required", "unknown"}
TERMINAL = {"completed", "failed", "canceled", "rejected"}
INTERRUPTED = {"input-required", "auth-required"}
BOT_SETTABLE = {"working", "input-required", "auth-required", "completed", "failed", "rejected", "canceled"}


class A2AError(Exception):
    code = -32603
    default_msg = "Internal error"
    http_status = 200

    def __init__(self, message: str | None = None, data: Any = None):
        super().__init__(message or self.default_msg)
        self.message = message or self.default_msg
        self.data = data

    def to_json(self) -> dict:
        e = {"code": self.code, "message": self.message}
        if self.data is not None:
            e["data"] = self.data
        return e


class ParseError(A2AError):
    code, default_msg = -32700, "Invalid JSON payload"


class InvalidRequest(A2AError):
    code, default_msg = -32600, "Request payload validation error"


class MethodNotFound(A2AError):
    code, default_msg = -32601, "Method not found"


class InvalidParams(A2AError):
    code, default_msg = -32602, "Invalid parameters"


class TaskNotFound(A2AError):
    code, default_msg = -32001, "Task not found"


class TaskNotCancelable(A2AError):
    code, default_msg = -32002, "Task cannot be canceled"


class PushNotSupported(A2AError):
    code, default_msg = -32003, "Push Notification is not supported"


class UnsupportedOperation(A2AError):
    code, default_msg = -32004, "This operation is not supported"


class ContentTypeNotSupported(A2AError):
    code, default_msg = -32005, "Incompatible content types"


class VersionNotSupported(A2AError):
    code, default_msg = -32009, "Protocol version not supported"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def now_ts() -> float:
    return time.time()


def new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------- normalization (inbound)

def _role(r: Any) -> str:
    if r in ("user", "ROLE_USER"):
        return "user"
    if r in ("agent", "ROLE_AGENT"):
        return "agent"
    raise InvalidParams("message.role must be 'user' or 'agent' (or ROLE_USER/ROLE_AGENT)")


def normalize_part(p: Any) -> dict:
    if not isinstance(p, dict):
        raise InvalidParams("each part must be an object")
    out: dict[str, Any]
    kind = p.get("kind") or p.get("type")
    if kind == "text" or (kind is None and "text" in p):
        if not isinstance(p.get("text"), str):
            raise InvalidParams("text part requires string 'text'")
        out = {"kind": "text", "text": p["text"]}
    elif kind == "file":
        f = p.get("file")
        if not isinstance(f, dict) or not (isinstance(f.get("bytes"), str) or isinstance(f.get("uri"), str)):
            raise InvalidParams("file part requires file.bytes (base64) or file.uri")
        file = {k: f[k] for k in ("name", "mimeType", "bytes", "uri") if f.get(k) is not None}
        out = {"kind": "file", "file": file}
    elif kind == "data" or (kind is None and "data" in p):
        if "data" not in p:
            raise InvalidParams("data part requires 'data'")
        out = {"kind": "data", "data": p["data"]}
    elif kind is None and ("raw" in p or "url" in p):  # A2A v1.0 file part
        file: dict[str, Any] = {}
        if "raw" in p:
            if not isinstance(p["raw"], str):
                raise InvalidParams("raw must be base64 string")
            file["bytes"] = p["raw"]
        else:
            if not isinstance(p["url"], str):
                raise InvalidParams("url must be a string")
            file["uri"] = p["url"]
        if p.get("filename"):
            file["name"] = p["filename"]
        if p.get("mediaType"):
            file["mimeType"] = p["mediaType"]
        out = {"kind": "file", "file": file}
    else:
        raise InvalidParams("unsupported part: expected text, file (bytes/uri, raw/url) or data")
    if p.get("metadata") is not None:
        out["metadata"] = p["metadata"]
    if kind is None and p.get("mediaType") and out["kind"] != "file":
        out.setdefault("metadata", {})["mediaType"] = p["mediaType"]
    return out


def normalize_parts(parts: Any) -> list[dict]:
    if not isinstance(parts, list) or not parts:
        raise InvalidParams("parts must be a non-empty array")
    return [normalize_part(p) for p in parts]


def normalize_message(m: Any) -> dict:
    if not isinstance(m, dict):
        raise InvalidParams("params.message is required")
    msg: dict[str, Any] = {
        "kind": "message",
        "messageId": str(m.get("messageId") or new_id()),
        "role": _role(m.get("role")),
        "parts": normalize_parts(m.get("parts")),
    }
    for k in ("contextId", "taskId"):
        if m.get(k) is not None:
            if not isinstance(m[k], str) or not m[k] or len(m[k]) > 200:
                raise InvalidParams(f"message.{k} must be a non-empty string")
            msg[k] = m[k]
    for k in ("referenceTaskIds", "extensions"):
        if isinstance(m.get(k), list):
            msg[k] = [str(x) for x in m[k]]
    if isinstance(m.get("metadata"), dict):
        msg["metadata"] = m["metadata"]
    return msg


def parts_from_loose(v: Any) -> list[dict]:
    """Bot-friendly: accept a string, a list of parts, or {parts:[...]}."""
    if isinstance(v, str):
        return [{"kind": "text", "text": v}]
    if isinstance(v, dict) and "parts" in v:
        return normalize_parts(v["parts"])
    if isinstance(v, dict):
        return [normalize_part(v)]
    return normalize_parts(v)


def text_of(parts: list[dict]) -> str:
    return "\n".join(p["text"] for p in parts if p.get("kind") == "text")


# ---------------------------------------------------------------- v1.0 conversion (outbound)

def state_v1(s: str) -> str:
    return "TASK_STATE_" + s.upper().replace("-", "_") if s != "unknown" else "TASK_STATE_UNSPECIFIED"


def state_from_any(s: str) -> str:
    if s.startswith("TASK_STATE_"):
        s = s[len("TASK_STATE_"):].lower().replace("_", "-")
    return s


def part_v1(p: dict) -> dict:
    k = p.get("kind")
    out: dict[str, Any]
    if k == "text":
        out = {"text": p["text"]}
    elif k == "data":
        out = {"data": p["data"]}
    else:
        f = p.get("file", {})
        out = {"raw": f["bytes"]} if "bytes" in f else {"url": f.get("uri")}
        if f.get("name"):
            out["filename"] = f["name"]
        if f.get("mimeType"):
            out["mediaType"] = f["mimeType"]
    if p.get("metadata") is not None:
        out["metadata"] = p["metadata"]
    return out


def message_v1(m: dict) -> dict:
    out = {k: v for k, v in m.items() if k not in ("kind", "role", "parts")}
    out["role"] = "ROLE_USER" if m.get("role") == "user" else "ROLE_AGENT"
    out["parts"] = [part_v1(p) for p in m.get("parts", [])]
    return out


def status_v1(s: dict) -> dict:
    out = {"state": state_v1(s["state"])}
    if s.get("message"):
        out["message"] = message_v1(s["message"])
    if s.get("timestamp"):
        out["timestamp"] = s["timestamp"]
    return out


def artifact_v1(a: dict) -> dict:
    out = {k: v for k, v in a.items() if k != "parts"}
    out["parts"] = [part_v1(p) for p in a.get("parts", [])]
    return out


def task_v1(t: dict) -> dict:
    out = {"id": t["id"], "contextId": t["contextId"], "status": status_v1(t["status"])}
    if "artifacts" in t:
        out["artifacts"] = [artifact_v1(a) for a in t["artifacts"]]
    if "history" in t:
        out["history"] = [message_v1(m) for m in t["history"]]
    if t.get("metadata"):
        out["metadata"] = t["metadata"]
    return out


def event_v1(e: dict) -> dict:
    """Internal (v0.3) event -> v1.0 StreamResponse."""
    k = e.get("kind")
    if k == "task":
        return {"task": task_v1(e)}
    if k == "message":
        return {"message": message_v1(e)}
    if k == "status-update":
        su = {"taskId": e["taskId"], "contextId": e["contextId"], "status": status_v1(e["status"])}
        if e.get("metadata"):
            su["metadata"] = e["metadata"]
        return {"statusUpdate": su}
    if k == "artifact-update":
        au = {"taskId": e["taskId"], "contextId": e["contextId"], "artifact": artifact_v1(e["artifact"])}
        for f in ("append", "lastChunk", "metadata"):
            if e.get(f) is not None:
                au[f] = e[f]
        return {"artifactUpdate": au}
    return e


def trim_history(task: dict, history_length: Any) -> dict:
    if history_length is None:
        return task
    try:
        n = int(history_length)
    except (TypeError, ValueError):
        raise InvalidParams("historyLength must be an integer")
    if n < 0:
        raise InvalidParams("historyLength must be >= 0")
    t = dict(task)
    if n == 0:
        t.pop("history", None)
    else:
        t["history"] = task.get("history", [])[-n:]
    return t
