"""Async broadcast: per-subscriber Queue with pre-serialized change events."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from mkio._json import dumps


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    table: str
    op: str  # "insert" | "update" | "delete"
    row: dict[str, Any]
    version: str
    raw_bytes: bytes  # Pre-serialized JSON envelope


class ChangeBus:
    def __init__(self) -> None:
        # table_name -> set of asyncio.Queue
        self._subscribers: dict[str, set[asyncio.Queue[ChangeEvent]]] = defaultdict(set)

    def subscribe(self, tables: list[str], maxsize: int = 4096) -> asyncio.Queue[ChangeEvent]:
        """Create a bounded queue subscribed to changes on the given tables."""
        q: asyncio.Queue[ChangeEvent] = asyncio.Queue(maxsize=maxsize)
        for table in tables:
            self._subscribers[table].add(q)
        return q

    def unsubscribe(self, tables: list[str], q: asyncio.Queue[ChangeEvent]) -> None:
        """Remove a queue from the given tables."""
        for table in tables:
            self._subscribers[table].discard(q)

    def publish(self, events: list[ChangeEvent]) -> None:
        """Fan out events to subscribers. Drops on full queue (backpressure)."""
        for event in events:
            for q in self._subscribers.get(event.table, ()):
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass  # Backpressure: slow consumer misses this event

    @staticmethod
    def make_event(table: str, op: str, row: dict[str, Any], version: str) -> ChangeEvent:
        """Build a ChangeEvent with pre-serialized bytes."""
        envelope = {
            "type": "update",
            "table": table,
            "op": op,
            "row": row,
            "version": version,
        }
        raw = dumps(envelope)
        return ChangeEvent(table=table, op=op, row=row, version=version, raw_bytes=raw)
