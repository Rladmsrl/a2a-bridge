#!/usr/bin/env python3
"""Interop example using the official a2a-sdk (>=1.0) Python client.
  pip install a2a-sdk httpx
  A2A_BASE=https://<bridge-host>/agents/coder A2A_KEY=... python scripts/sdk_example.py "your request"
"""
import asyncio
import os
import sys
import uuid

import httpx
from a2a.client import ClientConfig, create_client
from a2a.types import a2a_pb2 as pb


async def main(text: str) -> None:
    base = os.environ.get("A2A_BASE", "http://127.0.0.1:8080/agents/coder")
    async with httpx.AsyncClient(headers={"Authorization": f"Bearer {os.environ['A2A_KEY']}"}, timeout=120) as hc:
        client = await create_client(base, ClientConfig(httpx_client=hc, streaming=True))
        task_id = context_id = None
        for turn in [text, "branch: main"]:
            msg = pb.Message(message_id=str(uuid.uuid4()), role=pb.ROLE_USER, parts=[pb.Part(text=turn)])
            if task_id:
                msg.task_id, msg.context_id = task_id, context_id
            state = None
            async for ev in client.send_message(pb.SendMessageRequest(message=msg)):
                kind = ev.WhichOneof("payload")
                if kind == "task":
                    task_id, context_id, state = ev.task.id, ev.task.context_id, ev.task.status.state
                elif kind == "status_update":
                    state = ev.status_update.status.state
                    print("status:", pb.TaskState.Name(state),
                          " ".join(p.text for p in ev.status_update.status.message.parts))
                elif kind == "artifact_update":
                    print("artifact:", ev.artifact_update.artifact.name,
                          [p.text for p in ev.artifact_update.artifact.parts if p.text])
            if state != pb.TASK_STATE_INPUT_REQUIRED:
                break
        task = await client.get_task(pb.GetTaskRequest(id=task_id))
        print("final:", pb.TaskState.Name(task.status.state), "artifacts:", len(task.artifacts))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "repo_url: https://github.com/x/y goal: demo"))
