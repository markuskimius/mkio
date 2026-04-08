"""Query service: snapshot from SQLite + change feed with version-based reconnection."""

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
from mkio.ws_protocol import make_snapshot, make_delta, make_update


@dataclass
class QuerySubscriber:
    ws: WebSocketResponse
    filter_fn: Callable[[dict[str, Any]], bool] | None = None
    formatter: Callable[[dict[str, Any]], dict[str, Any]] | None = None


class QueryService(Service):
    """Snapshot + change feed with version-based delta reconnection.

    Config:
        primary_table: str
        watch_tables: list[str]
        sql: str (optional, defaults to SELECT * FROM primary_table)
        filterable: list[str] (optional)
        publish: dict (optional)
        change_log_size: int (default 10000)
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._table = self.config["primary_table"]
        self._sql = self.config.get("sql", f"SELECT * FROM {self._table}")
        self._filterable = set(self.config.get("filterable", []))
        self._formatter = self.config.get("_compiled_formatter")
        self._change_log_size = self.config.get("change_log_size", 10000)

        self._change_log: deque[tuple[str, str, dict[str, Any]]] = deque(
            maxlen=self._change_log_size
        )  # (version, op, row)
        self._subscribers: list[QuerySubscriber] = []
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        # Seed change log from DB for cross-restart delta reconnection
        recent = await self.db.read(
            f"SELECT * FROM (SELECT * FROM {self._table} WHERE _mkio_ref != '' "
            f"ORDER BY _mkio_ref DESC LIMIT ?) ORDER BY _mkio_ref ASC",
            (self._change_log_size,),
        )
        for row in recent:
            ref = row.get("_mkio_ref", "")
            if ref:
                self._change_log.append((ref, "upsert", row))

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

    async def on_subscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        client_ref = msg.get("ref")
        filter_expr = msg.get("filter")

        filter_fn = None
        if filter_expr and self._filterable:
            filter_fn = compile_filter(filter_expr)

        sub = QuerySubscriber(ws=ws, filter_fn=filter_fn, formatter=self._formatter)

        # Check for delta reconnection
        if client_ref and self._change_log:
            oldest_ref = self._change_log[0][0]
            if compare_refs(client_ref, oldest_ref) >= 0:
                changes = []
                for ver, op, row in self._change_log:
                    if compare_refs(ver, client_ref) > 0:
                        out_row = sub.formatter(row) if sub.formatter else row
                        if sub.filter_fn and not sub.filter_fn(out_row):
                            continue
                        changes.append({"op": op, "row": out_row})
                latest_ref = self._change_log[-1][0] if self._change_log else client_ref
                resp = make_delta(latest_ref, self.name, changes)
                await ws.send_bytes(resp)
                await self.notify_monitors("out", resp)
                self._subscribers.append(sub)
                return

        # Full snapshot from SQLite
        rows = await self.db.read(self._sql)
        out_rows = []
        for row in rows:
            out_row = sub.formatter(row) if sub.formatter else row
            if sub.filter_fn and not sub.filter_fn(out_row):
                continue
            out_rows.append(out_row)

        latest_ref = self._change_log[-1][0] if self._change_log else ""
        resp = make_snapshot(latest_ref, self.name, out_rows)
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)
        self._subscribers.append(sub)

    async def on_unsubscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        self._subscribers = [s for s in self._subscribers if s.ws is not ws]

    async def _listen_changes(self) -> None:
        """Consume change events, append to log, fan out."""
        assert self._bus_queue is not None
        while True:
            event: ChangeEvent = await self._bus_queue.get()

            row = event.row
            # For secondary table changes with JOINs, re-query
            if event.table != self._table and "JOIN" in self._sql.upper():
                # Can't easily identify which rows are affected — skip for now
                # Clients will get primary table changes; secondary table changes
                # would require a full re-query which is expensive
                continue

            self._change_log.append((event.ref, event.op, row))

            # Fan out
            dead: list[QuerySubscriber] = []
            notified_monitor = False
            for sub in self._subscribers:
                out_row = sub.formatter(row) if sub.formatter else row
                if sub.filter_fn and not sub.filter_fn(out_row):
                    continue
                try:
                    msg_bytes = make_update(self.name, ref=event.ref, op=event.op, row=out_row)
                    await sub.ws.send_bytes(msg_bytes)
                    if not notified_monitor:
                        await self.notify_monitors("out", msg_bytes)
                        notified_monitor = True
                except (ConnectionError, RuntimeError):
                    dead.append(sub)
            for sub in dead:
                self._subscribers.remove(sub)
