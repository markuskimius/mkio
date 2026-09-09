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
    ref: str
    raw_bytes: bytes  # Pre-serialized JSON envelope
    cause: str | None = None
    # cause: "undo" | "redo" when a version cursor move produced this change,
    # None for an ordinary write.  ``op`` stays the shape of the change itself,
    # so an undo of an insert arrives as a delete with cause "undo".
    old: dict[str, Any] | None = None
    # old: the row as it stood before the change, None when it did not exist.
    # Only version cursor moves capture it; ordinary writes leave it None.

    @property
    def new(self) -> dict[str, Any] | None:
        """The row as it stands after the change, None when it was removed."""
        return None if self.op == "delete" else self.row


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

    def has_subscribers(self, table: str) -> bool:
        """True if anything is listening for changes on ``table``."""
        return bool(self._subscribers.get(table))

    def publish(self, events: list[ChangeEvent]) -> None:
        """Fan out events to subscribers. Drops on full queue (backpressure)."""
        for event in events:
            for q in self._subscribers.get(event.table, ()):
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass  # Backpressure: slow consumer misses this event

    @staticmethod
    def make_event(
        table: str,
        op: str,
        row: dict[str, Any],
        ref: str,
        *,
        cause: str | None = None,
        old: dict[str, Any] | None = None,
    ) -> ChangeEvent:
        """Build a ChangeEvent with pre-serialized bytes.

        ``cause`` and ``old`` describe a version cursor move (see
        :class:`ChangeEvent`); they are omitted from the envelope when unset,
        so an ordinary write serializes exactly as before.
        """
        envelope: dict[str, Any] = {
            "type": "update",
            "table": table,
            "op": op,
            "row": row,
            "ref": ref,
        }
        if cause is not None:
            envelope["cause"] = cause
        if old is not None:
            envelope["old"] = old
        raw = dumps(envelope)
        return ChangeEvent(
            table=table, op=op, row=row, ref=ref, raw_bytes=raw,
            cause=cause, old=old,
        )
