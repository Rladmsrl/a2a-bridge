#!/usr/bin/env python3
"""Tiny A2A client for manual testing (JSON-RPC, A2A 0.3 method names by default).

  export A2A_URL=https://a2a.example.com/agents/coder  A2A_KEY=a2a_xxx
  a2a_cli.py card
  a2a_cli.py send "repo_url: ... goal: ..."            # message/send (non-blocking)
  a2a_cli.py send --wait "..."                         # blocking until input-required/terminal
  a2a_cli.py stream "..." [--task ID] [--context ID]   # message/stream (SSE)
  a2a_cli.py get TASK_ID | cancel TASK_ID | resubscribe TASK_ID | list
  a2a_cli.py chat                                      # interactive multi-turn using streaming
  add --v1 to use A2A 1.0 method names (SendMessage, ...)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid

import httpx

M03 = {"send": "message/send", "stream": "message/stream", "get": "tasks/get", "cancel": "tasks/cancel",
       "resubscribe": "tasks/resubscribe", "list": "tasks/list"}
M10 = {"send": "SendMessage", "stream": "SendStreamingMessage", "get": "GetTask", "cancel": "CancelTask",
       "resubscribe": "SubscribeToTask", "list": "ListTasks"}


def msg(text: str, task: str | None, ctx: str | None, v1: bool) -> dict:
    m = {"messageId": str(uuid.uuid4()), "role": "ROLE_USER" if v1 else "user",
         "parts": [{"text": text}] if v1 else [{"kind": "text", "text": text}]}
    if not v1:
        m["kind"] = "message"
    if task:
        m["taskId"] = task
    if ctx:
        m["contextId"] = ctx
    return m


def rpc(c: httpx.Client, url: str, method: str, params: dict) -> dict:
    r = c.post(url, json={"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params})
    if r.status_code != 200:
        sys.exit(f"HTTP {r.status_code}: {r.text}")
    return r.json()


def sse(c: httpx.Client, url: str, method: str, params: dict, quiet=False):
    body = {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params}
    last = None
    with c.stream("POST", url, json=body, headers={"Accept": "text/event-stream"}, timeout=None) as r:
        if r.headers.get("content-type", "").startswith("application/json"):
            print(r.read().decode()); return None
        for line in r.iter_lines():
            if line.startswith("data:"):
                ev = json.loads(line[5:])
                last = ev.get("result", ev)
                if not quiet:
                    print(json.dumps(ev, ensure_ascii=False))
                else:
                    show(last)
    return last


def show(ev: dict) -> None:
    e = ev.get("task") or ev.get("statusUpdate") or ev.get("artifactUpdate") or ev
    if "artifact" in e:
        for p in e["artifact"]["parts"]:
            print(f"  [artifact {e['artifact'].get('name', '')}] {p.get('text') or json.dumps(p.get('data'))}")
        return
    st = e.get("status", {})
    txt = " ".join(p.get("text", "") for p in (st.get("message") or {}).get("parts", []))
    print(f"  [{st.get('state')}] {txt}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["card", "send", "stream", "get", "cancel", "resubscribe", "list", "chat"])
    ap.add_argument("arg", nargs="?")
    ap.add_argument("--url", default=os.environ.get("A2A_URL", "http://127.0.0.1:8080/agents/coder"))
    ap.add_argument("--key", default=os.environ.get("A2A_KEY"))
    ap.add_argument("--task"); ap.add_argument("--context")
    ap.add_argument("--wait", action="store_true"); ap.add_argument("--v1", action="store_true")
    a = ap.parse_args()
    names = M10 if a.v1 else M03
    h = {"Authorization": f"Bearer {a.key}"} if a.key else {}
    if a.v1:
        h["A2A-Version"] = "1.0"
    with httpx.Client(headers=h, timeout=60) as c:
        if a.cmd == "card":
            print(json.dumps(c.get(a.url.rstrip("/") + "/.well-known/agent-card.json").json(), indent=2,
                             ensure_ascii=False))
        elif a.cmd == "send":
            cfg = {"returnImmediately": not a.wait} if a.v1 else {"blocking": a.wait}
            print(json.dumps(rpc(c, a.url, names["send"], {"message": msg(a.arg, a.task, a.context, a.v1),
                                                           "configuration": cfg}), indent=2, ensure_ascii=False))
        elif a.cmd == "stream":
            sse(c, a.url, names["stream"], {"message": msg(a.arg, a.task, a.context, a.v1)})
        elif a.cmd in ("get", "cancel"):
            print(json.dumps(rpc(c, a.url, names[a.cmd], {"id": a.arg}), indent=2, ensure_ascii=False))
        elif a.cmd == "resubscribe":
            sse(c, a.url, names["resubscribe"], {"id": a.arg})
        elif a.cmd == "list":
            print(json.dumps(rpc(c, a.url, names["list"], {}), indent=2, ensure_ascii=False))
        elif a.cmd == "chat":
            task = ctx = None
            while True:
                try:
                    text = input("you> ").strip()
                except EOFError:
                    break
                if not text:
                    continue
                last = sse(c, a.url, names["stream"], {"message": msg(text, task, ctx, a.v1)}, quiet=True)
                if not last:
                    continue
                e = last.get("task") or last.get("statusUpdate") or last
                ctx = e.get("contextId", ctx)
                tid = e.get("taskId") or e.get("id")
                state = (e.get("status") or {}).get("state", "")
                task = tid if state in ("input-required", "TASK_STATE_INPUT_REQUIRED") else None


if __name__ == "__main__":
    main()
