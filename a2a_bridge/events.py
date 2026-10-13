"""In-process pub/sub used to fan task events out to SSE streams and blocking waiters."""
from __future__ import annotations

import asyncio
from collections import defaultdict


class EventBus:
    def __init__(self) -> None:
        self._subs: dict[str, set[asyncio.Queue]] = defaultdict(set)

    def subscribe(self, task_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subs[task_id].add(q)
        return q

    def unsubscribe(self, task_id: str, q: asyncio.Queue) -> None:
        s = self._subs.get(task_id)
        if s:
            s.discard(q)
            if not s:
                self._subs.pop(task_id, None)

    def publish(self, task_id: str, event: dict) -> None:
        for q in list(self._subs.get(task_id, ())):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:  # slow consumer: drop it, it can resubscribe
                q.dropped = True  # type: ignore[attr-defined]
                self.unsubscribe(task_id, q)

    def count(self) -> int:
        return sum(len(s) for s in self._subs.values())
