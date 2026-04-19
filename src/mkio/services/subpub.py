"""SubPub service: in-memory cache + live push with multi-table support."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio._expr import compile_filter
from mkio.change_bus import ChangeEvent
from mkio.services.base import Service
from mkio.ws_protocol import make_snapshot, make_update


@dataclass
class Subscriber:
    ws: WebSocketResponse
    filter_fn: Callable[[dict[str, Any]], bool] | None = None
    formatter: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    subid: str | None = None
    fields: list[str] | None = None


class SubPubService(Service):
    """Subscribe-Publish: snapshot from cache + live updates.

    Config:
        primary_table: str
        watch_tables: list[str]
        key: str (primary key field for cache indexing)
        sql: str (optional, defaults to SELECT * FROM primary_table)
        where: str (optional, expression filter applied to rows before caching)
        filterable: list[str] (optional)
        publish: dict (optional, expression formatter)
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._table = self.config["primary_table"]
        self._key_field = self.config["key"]
        self._sql = self.config.get("sql", f"SELECT * FROM {self._table}")
        self._filterable = set(self.config.get("filterable", []))
        self._formatter = self.config.get("_compiled_formatter")
        self._where = self.config.get("_compiled_where")

        self._cache: dict[Any, dict[str, Any]] = {}
        self._subscribers: list[Subscriber] = []
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        rows = await self.db.read(self._sql)
        for row in rows:
            if self._where and not self._where(row):
                continue
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
        filter_expr = msg.get("filter")
        subid = msg.get("subid")
        fields = msg.get("fields")

        filter_fn = None
        if filter_expr and self._filterable:
            filter_fn = compile_filter(filter_expr)

        sub = Subscriber(ws=ws, filter_fn=filter_fn, formatter=self._formatter, subid=subid, fields=fields)

        rows = []
        for row in self._cache.values():
            out_row = sub.formatter(row) if sub.formatter else row
            if sub.filter_fn and not sub.filter_fn(out_row):
                continue
            rows.append(self._project(self._clean_row(out_row), fields))

        resp = make_snapshot(None, self.name, rows, subid=sub.subid)
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)
        self._subscribers.append(sub)

    @staticmethod
    def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(row)
        cleaned.pop("_mkio_ref", None)
        return cleaned

    @staticmethod
    def _project(row: dict[str, Any], fields: list[str] | None) -> dict[str, Any]:
        if not fields:
            return row
        return {k: v for k, v in row.items() if k in fields}

    async def on_unsubscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        self._subscribers = [s for s in self._subscribers if s.ws is not ws]

    async def _listen_changes(self) -> None:
        """Consume change events, update cache, fan out to subscribers."""
        assert self._bus_queue is not None
        while True:
            event: ChangeEvent = await self._bus_queue.get()

            # Apply where filter
            passes_where = not self._where or self._where(event.row)

            # Update cache and resolve effective op
            effective_op = event.op
            if event.table == self._table:
                # Primary table — direct cache update for simple queries
                if "JOIN" not in self._sql.upper():
                    key_val = event.row.get(self._key_field)
                    if event.op in ("insert", "update", "upsert"):
                        if passes_where:
                            existed = key_val in self._cache
                            self._cache[key_val] = event.row
                            effective_op = "update" if existed else "insert"
                        else:
                            # Row no longer matches — remove from cache if present
                            if key_val in self._cache:
                                self._cache.pop(key_val)
                                effective_op = "delete"
                            else:
                                continue
                    elif event.op == "delete":
                        self._cache.pop(key_val, None)
                else:
                    await self._requery_row(event)
            else:
                # Secondary table change — re-query affected rows
                await self._requery_all()

            # Fan out to subscribers
            await self._fan_out_op(event, effective_op)

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
        if rows and (not self._where or self._where(rows[0])):
            self._cache[key_val] = rows[0]
        else:
            self._cache.pop(key_val, None)

    async def _requery_all(self) -> None:
        """Re-query entire dataset (for secondary table changes)."""
        rows = await self.db.read(self._sql)
        self._cache.clear()
        for row in rows:
            if self._where and not self._where(row):
                continue
            self._cache[row[self._key_field]] = row

    async def _fan_out_op(self, event: ChangeEvent, op: str) -> None:
        """Send update to all subscribers, respecting filters and formatters."""
        dead: list[Subscriber] = []
        notified_monitor = False
        for sub in self._subscribers:
            out_row = sub.formatter(event.row) if sub.formatter else event.row
            if sub.filter_fn and not sub.filter_fn(out_row):
                continue
            try:
                msg_bytes = make_update(self.name, ref=None, op=op, row=self._project(self._clean_row(out_row), sub.fields), subid=sub.subid)
                await sub.ws.send_bytes(msg_bytes)
                if not notified_monitor:
                    await self.notify_monitors("out", msg_bytes)
                    notified_monitor = True
            except (ConnectionError, RuntimeError):
                dead.append(sub)
        for sub in dead:
            self._subscribers.remove(sub)
