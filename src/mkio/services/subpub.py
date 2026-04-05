"""SubPub service: in-memory cache + live push with multi-table support."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio._expr import compile_filter
from mkio._json import dumps
from mkio._version import compare_versions
from mkio.change_bus import ChangeEvent
from mkio.services.base import Service
from mkio.ws_protocol import make_snapshot, make_delta, make_update


@dataclass
class Subscriber:
    ws: WebSocketResponse
    filter_fn: Callable[[dict[str, Any]], bool] | None = None
    formatter: Callable[[dict[str, Any]], dict[str, Any]] | None = None


class SubPubService(Service):
    """Subscribe-Publish: snapshot from cache + live updates.

    Config:
        primary_table: str
        watch_tables: list[str]
        key: str (primary key field for cache indexing)
        sql: str (optional, defaults to SELECT * FROM primary_table)
        filterable: list[str] (optional)
        publish: dict (optional, expression formatter)
        change_log_size: int (default 10000)
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._table = self.config["primary_table"]
        self._key_field = self.config["key"]
        self._sql = self.config.get("sql", f"SELECT * FROM {self._table}")
        self._filterable = set(self.config.get("filterable", []))
        self._formatter = self.config.get("_compiled_formatter")
        self._change_log_size = self.config.get("change_log_size", 10000)

        self._cache: dict[Any, dict[str, Any]] = {}
        self._change_log: deque[tuple[str, str, dict[str, Any]]] = deque(
            maxlen=self._change_log_size
        )  # (version, op, row)
        self._subscribers: list[Subscriber] = []
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        rows = await self.db.read(self._sql)
        for row in rows:
            self._cache[row[self._key_field]] = row

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
        ref = msg.get("ref")
        client_version = msg.get("version")
        filter_expr = msg.get("filter")

        # Build subscriber
        filter_fn = None
        if filter_expr and self._filterable:
            filter_fn = compile_filter(filter_expr)

        sub = Subscriber(ws=ws, filter_fn=filter_fn, formatter=self._formatter)

        # Check for delta reconnection
        if client_version and self._change_log:
            oldest_version = self._change_log[0][0]
            if compare_versions(client_version, oldest_version) >= 0:
                # Client version is in the log — send delta
                changes = []
                for ver, op, row in self._change_log:
                    if compare_versions(ver, client_version) > 0:
                        out_row = sub.formatter(row) if sub.formatter else row
                        if sub.filter_fn and not sub.filter_fn(out_row):
                            continue
                        changes.append({"op": op, "row": out_row, "version": ver})
                latest_ver = self._change_log[-1][0] if self._change_log else client_version
                await ws.send_bytes(make_delta(ref, self.name, latest_ver, changes))
                self._subscribers.append(sub)
                return

        # Full snapshot
        rows = []
        for row in self._cache.values():
            out_row = sub.formatter(row) if sub.formatter else row
            if sub.filter_fn and not sub.filter_fn(out_row):
                continue
            rows.append(out_row)

        latest_ver = self._change_log[-1][0] if self._change_log else ""
        await ws.send_bytes(make_snapshot(ref, self.name, latest_ver, rows))
        self._subscribers.append(sub)

    async def on_unsubscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        self._subscribers = [s for s in self._subscribers if s.ws is not ws]

    async def _listen_changes(self) -> None:
        """Consume change events, update cache, fan out to subscribers."""
        assert self._bus_queue is not None
        while True:
            event: ChangeEvent = await self._bus_queue.get()

            # Update cache
            if event.table == self._table:
                # Primary table — direct cache update for simple queries
                if "JOIN" not in self._sql.upper():
                    key_val = event.row.get(self._key_field)
                    if event.op in ("insert", "update", "upsert"):
                        self._cache[key_val] = event.row
                    elif event.op == "delete":
                        self._cache.pop(key_val, None)
                else:
                    await self._requery_row(event)
            else:
                # Secondary table change — re-query affected rows
                await self._requery_all()

            # Append to change log
            self._change_log.append((event.version, event.op, event.row))

            # Fan out to subscribers
            await self._fan_out(event)

    async def _requery_row(self, event: ChangeEvent) -> None:
        """Re-query a single row when using JOINs."""
        key_val = event.row.get(self._key_field)
        if event.op == "delete":
            self._cache.pop(key_val, None)
            return
        # Re-run the JOIN query for this specific key
        # Append a WHERE clause for the primary key
        sql = f"SELECT * FROM ({self._sql}) AS _sub WHERE {self._key_field} = ?"
        rows = await self.db.read(sql, (key_val,))
        if rows:
            self._cache[key_val] = rows[0]
        else:
            self._cache.pop(key_val, None)

    async def _requery_all(self) -> None:
        """Re-query entire dataset (for secondary table changes)."""
        rows = await self.db.read(self._sql)
        self._cache.clear()
        for row in rows:
            self._cache[row[self._key_field]] = row

    async def _fan_out(self, event: ChangeEvent) -> None:
        """Send update to all subscribers, respecting filters and formatters."""
        dead: list[Subscriber] = []
        for sub in self._subscribers:
            out_row = sub.formatter(event.row) if sub.formatter else event.row
            if sub.filter_fn and not sub.filter_fn(out_row):
                continue
            try:
                msg_bytes = make_update(self.name, event.version, event.op, out_row)
                await sub.ws.send_bytes(msg_bytes)
            except (ConnectionError, RuntimeError):
                dead.append(sub)
        for sub in dead:
            self._subscribers.remove(sub)
