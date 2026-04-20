"""SubPub service: in-memory cache + topic-based single-row push."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio._expr import compile_formatter
from mkio.change_bus import ChangeEvent
from mkio.services.base import Service
from mkio.ws_protocol import make_error, make_snapshot, make_update


@dataclass
class Subscriber:
    ws: WebSocketResponse
    topic: Any
    formatter: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    subid: str | None = None
    fields: list[str] | None = None


class SubPubService(Service):
    """Subscribe-Publish: topic-based single-row snapshot + live updates.

    Config:
        primary_table: str
        watch_tables: list[str]
        topic: str (column name used for topic matching)
        sql: str (optional, defaults to SELECT * FROM primary_table)
        where: str (optional, expression filter applied to rows before caching)
        publish: dict (optional, expression formatter)
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._table = self.config["primary_table"]
        self._topic_field = self.config["topic"]
        self._sql = self.config.get("sql", f"SELECT * FROM {self._table}")
        self._formatter = self.config.get("_compiled_formatter")
        self._where = self.config.get("_compiled_where")
        self._defaults_fn = self.config.get("_compiled_defaults")
        if not self._defaults_fn and self.config.get("defaults"):
            self._defaults_fn = compile_formatter(self.config["defaults"])

        self._cache: dict[Any, dict[str, Any]] = {}
        self._not_found_template: dict[str, Any] = {}
        self._needs_requery = False
        self._subscribers: list[Subscriber] = []
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        rows = await self.db.read(self._sql)
        for row in rows:
            if self._where and not self._where(row):
                continue
            self._cache[str(row[self._topic_field])] = row

        # Build the not-found template: all output fields set to null,
        # then overlaid with configured defaults.
        publish = self.config.get("publish")
        if publish:
            columns = list(publish.keys())
        elif rows:
            columns = [c for c in rows[0].keys() if c != "_mkio_ref"]
        else:
            columns = [c for c in await self.db.read_columns(self._sql) if c != "_mkio_ref"]
        self._not_found_template = {c: None for c in columns}
        if self._defaults_fn:
            self._not_found_template.update(self._defaults_fn(self._not_found_template))
        self._not_found_template.setdefault("_mkio_ref", None)

        self._needs_requery = "JOIN" in self._sql.upper() or self._sql != f"SELECT * FROM {self._table}"

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
        topic = msg.get("topic")
        if topic is None:
            err = make_error(None, "SubPub subscribe requires 'topic'")
            await ws.send_bytes(err)
            await self.notify_monitors("out", err)
            return 0
        if isinstance(topic, list):
            if len(topic) == 0:
                err = make_error(None, "SubPub subscribe 'topic' array must not be empty")
                await ws.send_bytes(err)
                await self.notify_monitors("out", err)
                return 0
            topics = [str(t) for t in topic]
        else:
            topics = [str(topic)]

        subid = msg.get("subid")
        fields = msg.get("fields")

        rows: list[dict[str, Any]] = []
        subs: list[Subscriber] = []
        for t in topics:
            sub = Subscriber(ws=ws, topic=t, formatter=self._formatter, subid=subid, fields=fields)
            cached_row = self._cache.get(t)
            rows.append(self._build_row(sub, cached_row or {}, exists=cached_row is not None))
            subs.append(sub)

        resp = make_snapshot(None, self.name, rows, subid=subid)
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)
        self._subscribers.extend(subs)
        return len(subs)

    def _build_row(self, sub: Subscriber, row: dict[str, Any], *, exists: bool) -> dict[str, Any]:
        if exists:
            out = sub.formatter(row) if sub.formatter else dict(row)
            out = self._project(out, sub.fields)
            out["_mkio_ref"] = row.get("_mkio_ref", "")
            out["_mkio_topic"] = sub.topic
            out["_mkio_exists"] = True
        else:
            out = dict(self._not_found_template)
            out = self._project(out, sub.fields)
            out["_mkio_topic"] = sub.topic
            out["_mkio_exists"] = False
        return out

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
        """Consume change events, update cache, notify topic subscribers."""
        assert self._bus_queue is not None
        while True:
            event: ChangeEvent = await self._bus_queue.get()

            if event.table == self._table:
                if not self._needs_requery:
                    topic_val = str(event.row.get(self._topic_field))
                    notify = self._update_cache(event, topic_val)
                    if notify is not None:
                        await self._notify_topic(topic_val, exists=notify)
                else:
                    old_cache = dict(self._cache)
                    await self._requery_all()
                    topics = {sub.topic for sub in self._subscribers}
                    for topic in topics:
                        if old_cache.get(topic) != self._cache.get(topic):
                            await self._notify_topic(topic, exists=topic in self._cache)
            else:
                old_cache = dict(self._cache)
                await self._requery_all()
                topics = {sub.topic for sub in self._subscribers}
                for topic in topics:
                    if old_cache.get(topic) != self._cache.get(topic):
                        await self._notify_topic(topic, exists=topic in self._cache)

    def _update_cache(self, event: ChangeEvent, topic_val: Any) -> bool | None:
        """Update cache for a primary-table change without JOINs.

        Returns True (row exists), False (row removed), or None (no change).
        """
        passes_where = not self._where or self._where(event.row)
        if event.op in ("insert", "update", "upsert"):
            if passes_where:
                row = dict(event.row)
                row["_mkio_ref"] = event.ref
                self._cache[topic_val] = row
                return True
            if topic_val in self._cache:
                self._cache.pop(topic_val)
                return False
            return None
        if event.op == "delete":
            if topic_val in self._cache:
                self._cache.pop(topic_val)
                return False
            return None
        return None

    async def _notify_topic(self, topic_val: Any, *, exists: bool) -> None:
        """Send update to all subscribers watching this topic."""
        dead: list[Subscriber] = []
        notified_monitor = False
        for sub in self._subscribers:
            if sub.topic != topic_val:
                continue
            if exists:
                out_row = self._build_row(sub, self._cache[topic_val], exists=True)
            else:
                out_row = self._build_row(sub, {}, exists=False)
            try:
                msg_bytes = make_update(self.name, ref=None, op="update", row=out_row, subid=sub.subid)
                await sub.ws.send_bytes(msg_bytes)
                if not notified_monitor:
                    await self.notify_monitors("out", msg_bytes)
                    notified_monitor = True
            except (ConnectionError, RuntimeError):
                dead.append(sub)
        for sub in dead:
            self._subscribers.remove(sub)

    async def _requery_all(self) -> None:
        """Re-query entire dataset (for secondary table changes)."""
        rows = await self.db.read(self._sql)
        self._cache.clear()
        for row in rows:
            if self._where and not self._where(row):
                continue
            self._cache[str(row[self._topic_field])] = row
