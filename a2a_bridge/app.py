"""FastAPI application: A2A JSON-RPC endpoints, agent cards, bot callback, health."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import __version__
from . import models as M
from .config import Settings, load_settings
from .db import Store
from .events import EventBus
from .logging_setup import setup_logging
from .security import RateLimiter, bearer, callback_token, match_api_key, verify_signature, SIG_HEADER
from .service import Bridge, Conflict

log = logging.getLogger("a2a_bridge.http")

V03 = {"message/send", "message/stream", "tasks/get", "tasks/cancel", "tasks/resubscribe", "tasks/list",
       "tasks/pushNotificationConfig/set", "tasks/pushNotificationConfig/get",
       "tasks/pushNotificationConfig/list", "tasks/pushNotificationConfig/delete",
       "agent/getAuthenticatedExtendedCard"}
V1 = {"SendMessage", "SendStreamingMessage", "GetTask", "ListTasks", "CancelTask", "SubscribeToTask",
      "CreateTaskPushNotificationConfig", "GetTaskPushNotificationConfig", "ListTaskPushNotificationConfigs",
      "DeleteTaskPushNotificationConfig", "GetExtendedAgentCard"}
STREAMING = {"message/stream", "tasks/resubscribe", "SendStreamingMessage", "SubscribeToTask"}
SUPPORTED_VERSIONS = {"", "0.3", "1.0"}


# ---------------------------------------------------------------------------- body size limit (ASGI)
class BodyLimitMiddleware:
    def __init__(self, app, max_bytes: int):
        self.app, self.max = app, max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        for k, v in scope.get("headers", []):
            if k == b"content-length":
                try:
                    if int(v) > self.max:
                        return await _send_413(send)
                except ValueError:
                    pass
        seen = 0

        async def limited():
            nonlocal seen
            msg = await receive()
            if msg["type"] == "http.request":
                seen += len(msg.get("body", b""))
                if seen > self.max:
                    raise _TooLarge()
            return msg

        try:
            await self.app(scope, limited, send)
        except _TooLarge:
            await _send_413(send)


class _TooLarge(Exception):
    pass


async def _send_413(send):
    body = b'{"error":"request body too large"}'
    await send({"type": "http.response.start", "status": 413,
                "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
    await send({"type": "http.response.body", "body": body})


# ---------------------------------------------------------------------------- app factory
def create_app(settings: Settings | None = None, http_client: httpx.AsyncClient | None = None) -> FastAPI:
    s = settings or load_settings()
    setup_logging(s.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = Store(s.db_path)
        client = http_client or httpx.AsyncClient(follow_redirects=False,
                                                  headers={"User-Agent": f"a2a-bridge/{__version__}"})
        bridge = Bridge(s, store, EventBus(), client)
        app.state.bridge = bridge
        bridge.start()
        log.info("a2a bridge started", extra={"agents": list(s.agents), "base_url": s.public_base_url})
        try:
            yield
        finally:
            await bridge.stop()
            if http_client is None:
                await client.aclose()
            store.close()

    app = FastAPI(title="A2A Bridge", version=__version__, lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)
    limiter = RateLimiter(s.rate_limit_per_min, s.rate_limit_burst)

    def client_ip(req: Request) -> str:
        if s.trust_proxy_headers:
            xff = req.headers.get("x-forwarded-for")
            if xff:
                return xff.split(",")[0].strip()
            if req.headers.get("x-real-ip"):
                return req.headers["x-real-ip"]
        return req.client.host if req.client else "?"

    @app.middleware("http")
    async def access_log(req: Request, call_next):
        rid = req.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        t0 = time.perf_counter()
        ip = client_ip(req)
        if req.url.path not in ("/healthz",):
            ok, retry = limiter.allow(f"ip:{ip}")
            if not ok:
                return JSONResponse({"error": "rate limit exceeded"}, status_code=429,
                                    headers={"Retry-After": str(int(retry) + 1)})
        resp = await call_next(req)
        resp.headers["X-Request-Id"] = rid
        log.info("http", extra={"rid": rid, "method": req.method, "path": req.url.path, "status": resp.status_code,
                                "ip": ip, "ms": round((time.perf_counter() - t0) * 1000, 1)})
        return resp

    # ------------------------------------------------------------------ agent cards
    def card(name: str) -> dict:
        a = s.agents[name]
        url = f"{s.public_base_url}/agents/{name}"
        skills = []
        req_txt = ""
        if a.required_inputs:
            req_txt = " Required/optional input fields (send as text, or as a data part with these keys): " + "; ".join(
                f"{f['name']}{' (required)' if f.get('required') else ' (optional)'}: {f.get('description', '')}"
                for f in a.required_inputs) + "."
        for sk in a.skills or [{"id": "default", "name": a.display_name, "description": a.description,
                                "tags": ["general"]}]:
            d = dict(sk)
            d.setdefault("tags", [])
            d["description"] = d.get("description", "") + req_txt
            skills.append(d)
        c: dict[str, Any] = {
            "protocolVersion": "0.3.0",
            "name": a.display_name,
            "description": a.description + req_txt,
            "url": url,
            "preferredTransport": "JSONRPC",
            "additionalInterfaces": [{"url": url, "transport": "JSONRPC"}],
            "supportedInterfaces": [
                {"url": url, "protocolBinding": "JSONRPC", "protocolVersion": "1.0"},
                {"url": url, "protocolBinding": "JSONRPC", "protocolVersion": "0.3"},
            ],
            "version": a.version,
            "capabilities": {
                "streaming": True, "pushNotifications": True, "stateTransitionHistory": False,
                "extensions": ([{"uri": "urn:a2a-bridge:input-fields:v1",
                                 "description": "Input fields this agent expects in the first message",
                                 "required": False, "params": {"fields": a.required_inputs}}]
                               if a.required_inputs else []),
            },
            "securitySchemes": {"bearer": {
                "type": "http", "scheme": "bearer", "description": "API key issued by the bridge operator",
                "httpAuthSecurityScheme": {"scheme": "Bearer",
                                           "description": "API key issued by the bridge operator"}}},
            "security": [{"bearer": []}],
            "securityRequirements": [{"schemes": {"bearer": {"list": []}}}],
            "defaultInputModes": a.input_modes,
            "defaultOutputModes": a.output_modes,
            "skills": skills,
        }
        if a.provider:
            c["provider"] = a.provider
        if a.documentation_url:
            c["documentationUrl"] = a.documentation_url
        return c

    def card_resp(name: str):
        if name not in s.agents:
            return JSONResponse({"error": "agent not found"}, status_code=404)
        return JSONResponse(card(name), headers={"Cache-Control": "public, max-age=300"})

    @app.get("/.well-known/agent-card.json")
    @app.get("/.well-known/agent.json")
    async def root_card():
        return card_resp(s.default_agent)

    @app.get("/agents/{name}/.well-known/agent-card.json")
    @app.get("/agents/{name}/.well-known/agent.json")
    async def agent_card(name: str):
        return card_resp(name)

    @app.get("/agents")
    async def list_agents():
        return {"agents": [{"name": n, "url": f"{s.public_base_url}/agents/{n}",
                            "agentCard": f"{s.public_base_url}/agents/{n}/.well-known/agent-card.json"}
                           for n in s.agents]}

    @app.get("/healthz")
    async def healthz(req: Request):
        b: Bridge = req.app.state.bridge
        try:
            ok = b.store.ping()
        except Exception:
            ok = False
        return JSONResponse({"status": "ok" if ok else "degraded", "db": ok, "version": __version__,
                             "outbox": b.store.outbox_stats(), "streams": b.bus.count()},
                            status_code=200 if ok else 503)

    # ------------------------------------------------------------------ JSON-RPC
    @app.post("/")
    async def rpc_root(req: Request):
        return await rpc(req, s.default_agent)

    @app.post("/agents/{name}")
    @app.post("/agents/{name}/")
    async def rpc_agent(req: Request, name: str):
        if name not in s.agents:
            return JSONResponse({"error": "agent not found"}, status_code=404)
        return await rpc(req, name)

    def rpc_err(rid: Any, e: M.A2AError, status: int = 200) -> JSONResponse:
        return JSONResponse({"jsonrpc": "2.0", "id": rid, "error": e.to_json()}, status_code=status)

    def rpc_ok(rid: Any, result: Any) -> JSONResponse:
        return JSONResponse({"jsonrpc": "2.0", "id": rid, "result": result})

    async def rpc(req: Request, agent: str):
        key = match_api_key(s.api_keys, bearer(req.headers.get("authorization")) or req.headers.get("x-api-key"))
        if not key or (key.agents is not None and agent not in key.agents):
            log.warning("auth failed", extra={"ip": client_ip(req), "agent": agent})
            return JSONResponse({"jsonrpc": "2.0", "id": None,
                                 "error": {"code": -32000,
                                           "message": "Unauthorized" if key is None else "Forbidden"}},
                                status_code=401 if key is None else 403,
                                headers={"WWW-Authenticate": 'Bearer realm="a2a-bridge"'})
        ok, retry = limiter.allow(f"key:{key.label}")
        if not ok:
            return JSONResponse({"error": "rate limit exceeded"}, status_code=429,
                                headers={"Retry-After": str(int(retry) + 1)})
        raw = await req.body()
        try:
            body = json.loads(raw)
        except Exception:
            return rpc_err(None, M.ParseError())
        if not isinstance(body, dict):
            return rpc_err(None, M.InvalidRequest("batch requests are not supported"))
        rid = body.get("id")
        method = body.get("method")
        if body.get("jsonrpc") != "2.0" or not isinstance(method, str) \
                or not (rid is None or isinstance(rid, (str, int))):
            return rpc_err(rid, M.InvalidRequest())
        ver_hdr = (req.headers.get("a2a-version") or req.query_params.get("A2A-Version") or "").strip()
        if ver_hdr not in SUPPORTED_VERSIONS:
            return rpc_err(rid, M.VersionNotSupported(f"A2A-Version {ver_hdr} not supported",
                                                      {"supportedVersions": ["0.3", "1.0"]}))
        if method not in V03 and method not in V1:
            return rpc_err(rid, M.MethodNotFound(f"Method not found: {method}"))
        v1 = method in V1
        params = body.get("params") or {}
        if not isinstance(params, dict):
            return rpc_err(rid, M.InvalidParams("params must be an object"))
        b: Bridge = req.app.state.bridge
        try:
            if method in STREAMING:
                return await stream(req, b, agent, key.label, rid, method, params, v1)
            result = await dispatch(b, agent, key.label, method, params, v1)
            return rpc_ok(rid, result)
        except M.A2AError as e:
            return rpc_err(rid, e)
        except Exception:
            log.exception("rpc internal error", extra={"method": method})
            return rpc_err(rid, M.A2AError())

    def out_task(t: dict, v1: bool, hl: Any = None) -> dict:
        t = M.trim_history(t, hl)
        return M.task_v1(t) if v1 else t

    async def wait_for(b: Bridge, task: dict, q: asyncio.Queue) -> None:
        deadline = time.monotonic() + s.blocking_wait_s
        state = task["status"]["state"]
        while state not in M.TERMINAL and state not in M.INTERRUPTED:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                ev = await asyncio.wait_for(q.get(), remaining)
            except asyncio.TimeoutError:
                return
            if ev.get("kind") == "status-update":
                state = ev["status"]["state"]

    async def dispatch(b: Bridge, agent: str, owner: str, method: str, p: dict, v1: bool) -> Any:
        if method in ("message/send", "SendMessage"):
            cfg = p.get("configuration") or {}
            blocking = (not cfg.get("returnImmediately", False)) if v1 else bool(cfg.get("blocking", False))
            task = b.send_message(agent, owner, p)
            if blocking:
                q = b.bus.subscribe(task["id"])
                try:
                    await wait_for(b, task, q)
                finally:
                    b.bus.unsubscribe(task["id"], q)
                task = b.build_task(b.store.get_task(task["id"]))
            t = out_task(task, v1, cfg.get("historyLength"))
            return {"task": t} if v1 else t
        if method in ("tasks/get", "GetTask"):
            row = b.load_task(p.get("id"), owner, agent)
            return out_task(b.build_task(row), v1, p.get("historyLength"))
        if method in ("tasks/cancel", "CancelTask"):
            return out_task(b.cancel(agent, owner, p.get("id")), v1)
        if method in ("tasks/list", "ListTasks"):
            return list_tasks(b, agent, owner, p, v1)
        if method in ("tasks/pushNotificationConfig/set", "CreateTaskPushNotificationConfig"):
            cfg = b.push_set(agent, owner, p.get("taskId"), p, "1.0" if v1 else "0.3")
            return b.push_out(cfg, "1.0" if v1 else "0.3")
        if method in ("tasks/pushNotificationConfig/get", "GetTaskPushNotificationConfig"):
            if v1:
                return b.push_out(b.push_get(agent, owner, p.get("taskId"), p.get("id")), "1.0")
            return b.push_out(b.push_get(agent, owner, p.get("id"), p.get("pushNotificationConfigId")), "0.3")
        if method in ("tasks/pushNotificationConfig/list", "ListTaskPushNotificationConfigs"):
            if v1:
                return {"configs": [b.push_out(c, "1.0") for c in b.push_list(agent, owner, p.get("taskId"))],
                        "nextPageToken": ""}
            return [b.push_out(c, "0.3") for c in b.push_list(agent, owner, p.get("id"))]
        if method in ("tasks/pushNotificationConfig/delete", "DeleteTaskPushNotificationConfig"):
            if v1:
                b.push_delete(agent, owner, p.get("taskId"), p.get("id"))
            else:
                b.push_delete(agent, owner, p.get("id"), p.get("pushNotificationConfigId"))
            return {} if v1 else None
        if method in ("agent/getAuthenticatedExtendedCard", "GetExtendedAgentCard"):
            raise M.UnsupportedOperation("Extended agent card is not supported")
        raise M.MethodNotFound()

    def list_tasks(b: Bridge, agent: str, owner: str, p: dict, v1: bool) -> dict:
        size = p.get("pageSize", 50)
        if not isinstance(size, int) or not 1 <= size <= 100:
            raise M.InvalidParams("pageSize must be between 1 and 100")
        cursor = None
        if p.get("pageToken"):
            try:
                ts, tid = json.loads(base64.urlsafe_b64decode(p["pageToken"].encode()))
                cursor = (float(ts), str(tid))
            except Exception:
                raise M.InvalidParams("invalid pageToken")
        state = M.state_from_any(p["status"]) if p.get("status") else None
        after = None
        if p.get("statusTimestampAfter"):
            from datetime import datetime
            try:
                after = datetime.fromisoformat(p["statusTimestampAfter"].replace("Z", "+00:00")).timestamp()
            except ValueError:
                raise M.InvalidParams("invalid statusTimestampAfter")
        rows, total = b.store.list_tasks(owner, agent, p.get("contextId"), state, after, size, cursor)
        tasks = []
        for r in rows:
            t = out_task(b.build_task(r), v1, p.get("historyLength"))
            if not p.get("includeArtifacts"):
                t.pop("artifacts", None)
            tasks.append(t)
        nxt = ""
        if len(rows) == size:
            last = rows[-1]
            nxt = base64.urlsafe_b64encode(json.dumps([last["updated_at"], last["id"]]).encode()).decode()
        return {"tasks": tasks, "nextPageToken": nxt, "pageSize": size, "totalSize": total}

    async def stream(req: Request, b: Bridge, agent: str, owner: str, rid: Any, method: str, p: dict, v1: bool):
        if method in ("message/stream", "SendStreamingMessage"):
            task = b.send_message(agent, owner, p)  # sync: no events can be missed before subscribe
            q = b.bus.subscribe(task["id"])
        else:
            row = b.load_task(p.get("id"), owner, agent)
            if row["state"] in M.TERMINAL and v1:
                raise M.UnsupportedOperation(f"Task is in terminal state {row['state']}")
            q = b.bus.subscribe(row["id"])
            task = b.build_task(row)
        tid = task["id"]

        def frame(ev: dict) -> bytes:
            result = M.event_v1(ev) if v1 else ev
            return f"id: {uuid.uuid4().hex[:12]}\ndata: {json.dumps({'jsonrpc': '2.0', 'id': rid, 'result': result})}\n\n".encode()

        async def gen():
            try:
                yield b"retry: 3000\n\n"
                yield frame(task)
                if task["status"]["state"] in M.TERMINAL:
                    return
                while True:
                    try:
                        ev = await asyncio.wait_for(q.get(), s.sse_heartbeat_s)
                    except asyncio.TimeoutError:
                        if getattr(q, "dropped", False):
                            return
                        yield b": keepalive\n\n"
                        continue
                    yield frame(ev)
                    if ev.get("kind") == "status-update" and ev["status"]["state"] in M.TERMINAL:
                        return
                    if ev.get("kind") == "status-update" and ev["status"]["state"] in M.INTERRUPTED \
                            and method in ("message/stream", "SendStreamingMessage"):
                        # stream for this turn ends when input is required; client sends a follow-up
                        # (another message/stream with taskId) or calls tasks/resubscribe.
                        return
            finally:
                b.bus.unsubscribe(tid, q)

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                                          "Connection": "keep-alive"})

    # ------------------------------------------------------------------ bot callback
    def cb_auth(req: Request, b: Bridge, task_id: str, raw: bytes) -> dict | None:
        row = b.store.get_task(task_id)
        if not row:
            return None
        secret = s.agents[row["agent"]].shared_secret if row["agent"] in s.agents else None
        if not secret:
            return None
        tok = bearer(req.headers.get("authorization"))
        import hmac as _h
        if tok and _h.compare_digest(tok, callback_token(secret, task_id)):
            return row
        if verify_signature(secret, raw, req.headers.get(SIG_HEADER), s.callback_max_skew_s):
            return row
        return None

    @app.post("/callback/{task_id}")
    async def callback(req: Request, task_id: str):
        b: Bridge = req.app.state.bridge
        raw = await req.body()
        row = cb_auth(req, b, task_id, raw)
        if not row:
            log.warning("callback auth failed", extra={"task_id": task_id, "ip": client_ip(req)})
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
        try:
            body = json.loads(raw)
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid JSON"}, status_code=400)
        try:
            code, out = b.apply_callback(row, body)
            return JSONResponse(out, status_code=code)
        except Conflict as e:
            return JSONResponse({"ok": False, "error": str(e),
                                 "task": b.build_task(b.store.get_task(task_id))}, status_code=409)
        except M.A2AError as e:
            return JSONResponse({"ok": False, "error": e.message}, status_code=400)

    @app.get("/callback/{task_id}")
    async def callback_get(req: Request, task_id: str):
        b: Bridge = req.app.state.bridge
        row = cb_auth(req, b, task_id, b"")
        if not row:
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
        return {"ok": True, "task": b.build_task(row)}

    app.add_middleware(BodyLimitMiddleware, max_bytes=s.max_request_bytes)
    return app
