"""Stream service: append-only ring buffer with ref-based cursor reconnect."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio._expr import compile_filter
from mkio._ref import compare_refs
from mkio.change_bus import ChangeEvent
from mkio.services.base import Service
from mkio.ws_protocol import make_error, make_snapshot, make_update


@dataclass
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
        self._subscribers: list[StreamSubscriber] = []
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        # Pre-fill buffer with recent rows, using stored _mkio_ref for
        # consistent refs across restarts
        if "JOIN" not in self._sql.upper():
            sql = f"{self._sql} ORDER BY rowid DESC LIMIT ?"
        else:
            sql = f"SELECT * FROM ({self._sql}) LIMIT ?"
        rows = await self.db.read(sql, (self._buffer_size,))
        if "JOIN" not in self._sql.upper():
            rows.reverse()
        from mkio._ref import next_ref
        for row in rows:
            ref = row.get("_mkio_ref", "")
            ver = ref if ref else next_ref()
            self._buffer.append((ver, row))

        watch = self.config.get("watch_tables", [self._table])
        self._bus_queue = self.bus.subscribe(watch)
        self._listener_task = asyncio.create_task(self._listen_changes())

    async def stop(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._bus_queue:
            watch = self.config.get("watch_tables", [self._table])
            self.bus.unsubscribe(watch, self._bus_queue)

    async def on_subscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> int:
        client_ref = msg.get("ref")
        filter_expr = msg.get("filter")
        subid = msg.get("subid")

        if not client_ref:
            resp = make_error(None, "Stream subscribe requires a ref")
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
            return 0

        fields = msg.get("fields")

        filter_fn = None
        if filter_expr and self._filterable:
            filter_fn = compile_filter(filter_expr)

        sub = StreamSubscriber(ws=ws, filter_fn=filter_fn, formatter=self._formatter, subid=subid, fields=fields)

        rows_to_send: list[dict[str, Any]] = []

        if self._buffer:
            buffer_start_ver = self._buffer[0][0]
            if compare_refs(client_ref, buffer_start_ver) >= 0:
                for ver, row in self._buffer:
                    if compare_refs(ver, client_ref) > 0:
                        out_row = sub.formatter(row) if sub.formatter else row
                        if sub.filter_fn and not sub.filter_fn(out_row):
                            continue
                        rows_to_send.append(self._project(out_row, fields))
            else:
                for ver, row in self._buffer:
                    out_row = sub.formatter(row) if sub.formatter else row
                    if sub.filter_fn and not sub.filter_fn(out_row):
                        continue
                    rows_to_send.append(self._project(out_row, fields))

        latest_ref = self._buffer[-1][0] if self._buffer else ""
        resp = make_snapshot(latest_ref, self.name, rows_to_send, subid=sub.subid)
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)
        self._subscribers.append(sub)
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

            # Only process inserts for append-only tables
            if event.op != "insert":
                continue

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
            for sub in self._subscribers:
                out_row = sub.formatter(row) if sub.formatter else row
                if sub.filter_fn and not sub.filter_fn(out_row):
                    continue
                try:
                    msg_bytes = make_update(self.name, ref=event.ref, op=event.op, row=self._project(out_row, sub.fields), subid=sub.subid)
                    await sub.ws.send_bytes(msg_bytes)
                    if not notified_monitor:
                        await self.notify_monitors("out", msg_bytes)
                        notified_monitor = True
                except (ConnectionError, RuntimeError):
                    dead.append(sub)
            for sub in dead:
                self._subscribers.remove(sub)
