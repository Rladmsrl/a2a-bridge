#!/usr/bin/env python3
"""Mock Grok Bot for local testing.

Receives bridge webhooks, verifies bearer + HMAC signature, then (in the background,
short connections) calls back: working -> if the conversation has no 'branch' it asks
input-required -> after the follow-up it posts an artifact and completed.

  MOCK_BOT_TOKEN=... MOCK_BOT_SECRET=... python scripts/mock_bot.py --port 9100
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import os
import time

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def verify_sig(secret: str, body: bytes, header: str | None, skew: int = 300) -> bool:
    try:
        kv = dict(x.split("=", 1) for x in (header or "").split(","))
        ts = int(kv["t"])
        exp = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
        return abs(time.time() - ts) <= skew and hmac.compare_digest(exp, kv["v1"])
    except Exception:
        return False


def make_app(token: str, secret: str, delay: float = 0.05) -> FastAPI:
    app = FastAPI()
    app.state.received = []

    async def work(p: dict) -> None:
        cb = p["callback"]
        h = cb["headers"]
        async with httpx.AsyncClient(timeout=10) as c:
            await asyncio.sleep(delay)
            if p["event"] == "task.canceled":
                return
            await c.post(cb["url"], headers=h, json={"state": "working", "message": "mock bot: on it",
                                                    "eventId": f"{p['deliveryId']}-w"})
            all_text = " ".join((m.get("parts") or [{}])[0].get("text", "") for m in p["history"]
                                if m["role"] == "user")
            await asyncio.sleep(delay)
            if "branch" not in all_text.lower():
                await c.post(cb["url"], headers=h, json={"state": "input-required",
                                                        "message": "Which branch should I use?",
                                                        "eventId": f"{p['deliveryId']}-q"})
                return
            await c.post(cb["url"], headers=h, json={"artifacts": [{"name": "result", "text": "partial-1 "}],
                                                    "eventId": f"{p['deliveryId']}-a1"})
            await c.post(cb["url"], headers=h, json={
                "state": "completed", "message": "Done.",
                "artifacts": [{"name": "summary", "parts": [{"kind": "text", "text": f"handled: {all_text}"},
                                                             {"kind": "data", "data": {"ok": True}}]}],
                "eventId": f"{p['deliveryId']}-c"})

    @app.post("/hook")
    async def hook(req: Request):
        raw = await req.body()
        if req.headers.get("authorization") != f"Bearer {token}":
            return JSONResponse({"error": "bad token"}, status_code=401)
        if not verify_sig(secret, raw, req.headers.get("x-a2a-signature")):
            return JSONResponse({"error": "bad signature"}, status_code=401)
        p = await req.json()
        app.state.received.append(p)
        asyncio.get_running_loop().create_task(work(p))
        return {"accepted": True}

    return app


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9100)
    a = ap.parse_args()
    uvicorn.run(make_app(os.environ["MOCK_BOT_TOKEN"], os.environ["MOCK_BOT_SECRET"], 0.3),
                host="127.0.0.1", port=a.port)
