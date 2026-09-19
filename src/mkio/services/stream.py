"""Stream service: append-only ring buffer with ref-based cursor reconnect."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio.expr import compile_filter
from mkio._ref import compare_refs
from mkio.change_bus import ChangeEvent
from mkio.services.base import Service
from mkio.ws_protocol import make_nack, make_snapshot, make_update

logger = logging.getLogger("mkio.stream")


@dataclass(eq=False)
class StreamSubscriber:
    ws: WebSocketResponse
    filter_fn: Callable[[dict[str, Any]], bool] | None = None
    formatter: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    subid: str | None = None
    fields: list[str] | None = None


class StreamService(Service):
    """Reliable append-only datastream with cursor-based reconnection.

    Config:
        primary_table: str
        watch_tables: list[str]
        buffer_size: int (default 10000)
        sql: str (optional, defaults to SELECT * FROM primary_table)
        filterable: list[str] (optional)
        publish: dict (optional)
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._table = self.config["primary_table"]
        self._buffer_size = self.config.get("buffer_size", 10000)
        self._sql = self.config.get("sql", f"SELECT * FROM {self._table}")
        self._filterable = set(self.config.get("filterable", []))
        self._formatter = self.config.get("_compiled_formatter")

        # Ring buffer: (ref, row_dict)
        self._buffer: deque[tuple[str, dict[str, Any]]] = deque(maxlen=self._buffer_size)
        # Refs of buffered rows deleted since they were buffered (an archive
        # run).  Filtered out on subscribe; the buffer is rebuilt without them
        # once the set grows, so a burst of deletes costs one pass.
        self._deleted: set[str] = set()
        # Refs a resync loaded from the database, whose change events may
        # still be queued behind it; emptied once the queue is.
        self._resynced: set[str] = set()
        self._subscribers: list[StreamSubscriber] = []
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None

    def _compact(self) -> None:
        kept = [(v, r) for v, r in self._buffer if v not in self._deleted]
        self._buffer = deque(kept, maxlen=self._buffer_size)
        self._deleted.clear()

    def _buffered(self) -> list[tuple[str, dict[str, Any]]]:
        """The buffer minus rows deleted since they were buffered."""
        if not self._deleted:
            return list(self._buffer)
        return [(v, r) for v, r in self._buffer if v not in self._deleted]

    async def start(self) -> None:
        await self._load_buffer()

        watch = self.config.get("watch_tables", [self._table])
        self._bus_queue = self.bus.subscribe(watch)
        self._listener_task = asyncio.create_task(self._listen_changes())

    async def _load_buffer(self) -> None:
        # Fill the buffer with recent rows, using stored _mkio_ref for
        # consistent refs across restarts
        if "JOIN" not in self._sql.upper():
            sql = f"{self._sql} ORDER BY rowid DESC LIMIT ?"
        else:
            sql = f"SELECT * FROM ({self._sql}) LIMIT ?"
        rows = await self.db.read(sql, (self._buffer_size,))
        if "JOIN" not in self._sql.upper():
            rows.reverse()
        from mkio._ref import next_ref
        self._buffer.clear()
        self._deleted.clear()
        for row in rows:
            ref = row.get("_mkio_ref", "")
            ver = ref if ref else next_ref()
            self._buffer.append((ver, row))

    async def stop(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._bus_queue:
            watch = self.config.get("watch_tables", [self._table])
            self.bus.unsubscribe(watch, self._bus_queue)

    async def on_subscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> int:
        client_ref = msg.get("ref")
        filter_expr = msg.get("filter")
        subid = msg.get("subid")
        fields = msg.get("fields")
        before = msg.get("before") is True
        maxcount = msg.get("maxcount", 0)
        if not (isinstance(maxcount, int) and not isinstance(maxcount, bool) and maxcount > 0):
            maxcount = 0

        filter_fn = None
        if filter_expr and self._filterable:
            filter_fn = compile_filter(filter_expr)

        sub = StreamSubscriber(ws=ws, filter_fn=filter_fn, formatter=self._formatter, subid=subid, fields=fields)

        rows_to_send: list[tuple[str, dict[str, Any]]] = []

        buffered = self._buffered()
        if buffered:
            if before and client_ref:
                for ver, row in buffered:
                    if compare_refs(ver, client_ref) < 0:
                        out_row = sub.formatter(row) if sub.formatter else row
                        if sub.filter_fn and not sub.filter_fn(out_row):
                            continue
                        rows_to_send.append((ver, self._project(out_row, fields)))
            elif client_ref:
                buffer_start_ver = buffered[0][0]
                if compare_refs(client_ref, buffer_start_ver) >= 0:
                    for ver, row in buffered:
                        if compare_refs(ver, client_ref) > 0:
                            out_row = sub.formatter(row) if sub.formatter else row
                            if sub.filter_fn and not sub.filter_fn(out_row):
                                continue
                            rows_to_send.append((ver, self._project(out_row, fields)))
                else:
                    for ver, row in buffered:
                        out_row = sub.formatter(row) if sub.formatter else row
                        if sub.filter_fn and not sub.filter_fn(out_row):
                            continue
                        rows_to_send.append((ver, self._project(out_row, fields)))
            else:
                for ver, row in buffered:
                    out_row = sub.formatter(row) if sub.formatter else row
                    if sub.filter_fn and not sub.filter_fn(out_row):
                        continue
                    rows_to_send.append((ver, self._project(out_row, fields)))

        if maxcount:
            if before:
                hasmore = len(rows_to_send) > maxcount
                page = rows_to_send[-maxcount:]
                page_ref = page[0][0] if page else ""
            else:
                hasmore = len(rows_to_send) > maxcount
                page = rows_to_send[:maxcount]
                page_ref = page[-1][0] if page else ""
            resp = make_snapshot(page_ref, self.name, [r for _, r in page], subid=subid, hasmore=hasmore)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
            return 0

        if before:
            page_ref = rows_to_send[0][0] if rows_to_send else ""
            resp = make_snapshot(page_ref, self.name, [r for _, r in rows_to_send], subid=subid, hasmore=False)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
            return 0

        latest_ref = buffered[-1][0] if buffered else ""
        resp = make_snapshot(latest_ref, self.name, [r for _, r in rows_to_send], subid=subid, hasmore=False)
        await ws.send_bytes(resp)
        # Live before anything else is awaited: a row appended in between
        # would be in neither the snapshot nor this subscriber's feed.
        self._subscribers.append(sub)
        await self.notify_monitors("out", resp)
        return 1

    @staticmethod
    def _project(row: dict[str, Any], fields: list[str] | None) -> dict[str, Any]:
        if not fields:
            return row
        return {k: v for k, v in row.items() if k in fields}

    async def on_unsubscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> int:
        before = len(self._subscribers)
        subid = msg.get("subid")
        if subid is not None:
            self._subscribers = [s for s in self._subscribers if not (s.ws is ws and s.subid == subid)]
        else:
            self._subscribers = [s for s in self._subscribers if s.ws is not ws]
        return before - len(self._subscribers)

    async def _listen_changes(self) -> None:
        """Consume insert events, append to buffer, fan out."""
        assert self._bus_queue is not None
        while True:
            event: ChangeEvent = await self._bus_queue.get()
            # One bad event must not end the feed for every subscriber, now
            # and to come: whatever it raises is logged and the loop goes on.
            try:
                if self.bus.take_overflow(self._bus_queue):
                    await self._resync()
                else:
                    await self._on_event(event)
            except Exception:
                logger.exception("stream service %r: change not delivered", self.name)
            if self._resynced and self._bus_queue.empty():
                self._resynced.clear()

    async def _resync(self) -> None:
        """The change queue overflowed, so rows were missed: reload the
        buffer and reset each subscriber, which subscribes again from the
        last ref it holds and is sent what it lacks."""
        while not self._bus_queue.empty():
            self._bus_queue.get_nowait()
        await self._load_buffer()
        self._resynced = {ver for ver, _ in self._buffer}
        subscribers, self._subscribers = self._subscribers, []
        for sub in subscribers:
            resp = make_nack(self.name, "subscription reset: change queue overflow",
                             subid=sub.subid, code="reset")
            try:
                await sub.ws.send_bytes(resp)
            except (ConnectionError, RuntimeError):
                continue

    async def _on_event(self, event: ChangeEvent) -> None:
        if event.op == "delete":
            ref = (event.row or {}).get("_mkio_ref")
            if ref:
                self._deleted.add(ref)
                if len(self._deleted) * 10 > self._buffer_size:
                    self._compact()
            return
        # Only inserts append to an append-only table
        if event.op != "insert" or event.ref in self._resynced:
            return

        row = event.row
        # Re-query if using JOINs
        if "JOIN" in self._sql.upper():
            rows = await self.db.read(self._sql + " ORDER BY rowid DESC LIMIT 1")
            if rows:
                row = rows[0]

        self._buffer.append((event.ref, row))

        # Fan out
        dead: list[StreamSubscriber] = []
        notified_monitor = False
        for sub in list(self._subscribers):
            out_row = sub.formatter(row) if sub.formatter else row
            if sub.filter_fn and not sub.filter_fn(out_row):
                continue
            try:
                msg_bytes = make_update(self.name, ref=event.ref, op=event.op,
                                        row=self._project(out_row, sub.fields),
                                        subid=sub.subid, cause=event.cause)
                await sub.ws.send_bytes(msg_bytes)
                if not notified_monitor:
                    await self.notify_monitors("out", msg_bytes)
                    notified_monitor = True
            except (ConnectionError, RuntimeError):
                dead.append(sub)
        if dead:
            # By identity, and against the list as it stands now: a
            # disconnect may have replaced it while a send was awaited.
            self._subscribers = [s for s in self._subscribers if not any(s is d for d in dead)]
