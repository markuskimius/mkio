"""Query service: snapshot from SQLite + change feed."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio._expr import compile_filter
from mkio.change_bus import ChangeEvent
from mkio.services.base import Service
from mkio.ws_protocol import make_snapshot, make_update


@dataclass
class QuerySubscriber:
    ws: WebSocketResponse
    filter_fn: Callable[[dict[str, Any]], bool] | None = None
    formatter: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    subid: str | None = None
    fields: list[str] | None = None
    sent_rows: set[str] | None = None


class QueryService(Service):
    """Snapshot from SQLite + change feed.

    Config:
        primary_table: str
        watch_tables: list[str]
        sql: str (optional, defaults to SELECT * FROM primary_table)
        filterable: list[str] (optional)
        publish: dict (optional)
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._table = self.config["primary_table"]
        self._sql = self.config.get("sql", f"SELECT * FROM {self._table}")
        self._filterable = set(self.config.get("filterable", []))
        self._formatter = self.config.get("_compiled_formatter")

        self._subscribers: list[QuerySubscriber] = []
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._pk_cols: list[str] = []

    async def start(self) -> None:
        tables = self.config.get("watch_tables", [self._table])
        ordered = [self._table] + [t for t in tables if t != self._table]
        seen: set[str] = set()
        self._pk_cols = []
        for table in ordered:
            info = await self.db.read(f"PRAGMA table_info({table})")
            for r in sorted(info, key=lambda r: r["pk"]):
                if r["pk"] > 0 and r["name"] not in seen:
                    self._pk_cols.append(r["name"])
                    seen.add(r["name"])

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
        want_snapshot = msg.get("snapshot", True)
        want_updates = msg.get("updates", True)
        fields = msg.get("fields")

        filter_fn = None
        if filter_expr and self._filterable:
            filter_fn = compile_filter(filter_expr)

        sent_rows = set() if filter_fn else None
        sub = QuerySubscriber(ws=ws, filter_fn=filter_fn, formatter=self._formatter, subid=subid, fields=fields, sent_rows=sent_rows)

        if want_snapshot:
            rows = await self.db.read(self._sql)
            out_rows = []
            for row in rows:
                out_row = sub.formatter(row) if sub.formatter else row
                if sub.filter_fn and not sub.filter_fn(out_row):
                    continue
                if sent_rows is not None:
                    rid = self._row_id(row)
                    if rid is not None:
                        sent_rows.add(rid)
                out_rows.append(self._project(self._tag_row(row, out_row), fields))

            resp = make_snapshot(None, self.name, out_rows, subid=sub.subid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)

        if want_updates:
            self._subscribers.append(sub)

    def _row_id(self, row: dict[str, Any]) -> str | None:
        if not self._pk_cols:
            return None
        try:
            vals = [row[col] for col in self._pk_cols]
        except (KeyError, TypeError):
            return None
        if len(vals) == 1:
            return str(vals[0])
        return json.dumps(vals, separators=(",", ":"))

    def _tag_row(self, raw_row: dict[str, Any], out_row: dict[str, Any]) -> dict[str, Any]:
        tagged = dict(out_row)
        tagged["_mkio_ref"] = raw_row.get("_mkio_ref", "")
        rid = self._row_id(raw_row)
        if rid is not None:
            tagged["_mkio_row"] = rid
        return tagged

    @staticmethod
    def _project(row: dict[str, Any], fields: list[str] | None) -> dict[str, Any]:
        if not fields:
            return row
        return {k: v for k, v in row.items() if k in fields or k.startswith("_mkio_")}

    async def on_unsubscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> int:
        before = len(self._subscribers)
        subid = msg.get("subid")
        if subid is not None:
            self._subscribers = [s for s in self._subscribers if not (s.ws is ws and s.subid == subid)]
        else:
            self._subscribers = [s for s in self._subscribers if s.ws is not ws]
        return before - len(self._subscribers)

    async def _listen_changes(self) -> None:
        """Consume change events, append to log, fan out."""
        assert self._bus_queue is not None
        while True:
            event: ChangeEvent = await self._bus_queue.get()

            row = dict(event.row)
            row["_mkio_ref"] = event.ref
            # For secondary table changes with JOINs, re-query
            if event.table != self._table and "JOIN" in self._sql.upper():
                # Can't easily identify which rows are affected — skip for now
                # Clients will get primary table changes; secondary table changes
                # would require a full re-query which is expensive
                continue

            # Fan out
            dead: list[QuerySubscriber] = []
            notified_monitor = False
            rid = self._row_id(row)
            for sub in self._subscribers:
                out_row = sub.formatter(row) if sub.formatter else row
                if sub.filter_fn and not sub.filter_fn(out_row):
                    if sub.sent_rows is not None and rid is not None and rid in sub.sent_rows:
                        sub.sent_rows.discard(rid)
                        try:
                            tagged = self._project(self._tag_row(row, out_row), sub.fields)
                            msg_bytes = make_update(self.name, ref=None, op="delete", row=tagged, subid=sub.subid)
                            await sub.ws.send_bytes(msg_bytes)
                            if not notified_monitor:
                                await self.notify_monitors("out", msg_bytes)
                                notified_monitor = True
                        except (ConnectionError, RuntimeError):
                            dead.append(sub)
                    continue
                if sub.sent_rows is not None and rid is not None:
                    sub.sent_rows.add(rid)
                try:
                    tagged = self._project(self._tag_row(row, out_row), sub.fields)
                    msg_bytes = make_update(self.name, ref=None, op=event.op, row=tagged, subid=sub.subid)
                    await sub.ws.send_bytes(msg_bytes)
                    if not notified_monitor:
                        await self.notify_monitors("out", msg_bytes)
                        notified_monitor = True
                except (ConnectionError, RuntimeError):
                    dead.append(sub)
            for sub in dead:
                self._subscribers.remove(sub)
