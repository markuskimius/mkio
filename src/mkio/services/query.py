"""Query service: snapshot from SQLite + change feed."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio.expr import compile_filter
from mkio._ref import next_ref
from mkio.change_bus import ChangeEvent
from mkio.services.base import Service
from mkio.ws_protocol import make_nack, make_snapshot, make_update

_DEFAULT_MAX_BUFFER = 1000
_GETMORE_TIMEOUT = 60.0

logger = logging.getLogger("mkio.query")


# A change held for a subscriber whose snapshot is still going out.
Buffered = tuple[str, "str | None", dict[str, Any], "str | None"]  # op, row id, row, cause


@dataclass(eq=False)
class QuerySubscriber:
    ws: WebSocketResponse
    filter_fn: Callable[[dict[str, Any]], bool] | None = None
    formatter: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    subid: str | None = None
    fields: list[str] | None = None
    sent_rows: set[str] | None = None
    maxcount: int = 0
    pending_rows: list[dict[str, Any]] = field(default_factory=list)
    buffered_updates: list[Buffered] = field(default_factory=list)
    # Row ids the snapshot carried: a buffered insert of one is an update.
    snapshot_ids: set[str] = field(default_factory=set)
    max_buffer: int = 0
    overflowed: bool = False
    want_updates: bool = True
    last_activity: float = field(default_factory=time.monotonic)


class QueryService(Service):
    """Snapshot from SQLite + change feed.

    Config:
        primary_table: str
        watch_tables: list[str]
        sql: str (optional, defaults to SELECT * FROM primary_table)
        key: list[str] (optional, the columns ``_mkio_row`` is built from)
        watch_columns: dict[str, list[str]] (optional, per secondary table)
        filterable: list[str] (optional)
        publish: dict (optional)
        max_buffer: int (optional, default 1000)

    With the default ``sql`` a change event *is* the row: the writer's
    RETURNING row is fanned out as it came. A custom ``sql`` may reshape the
    row — computed columns, a WHERE, a JOIN — so the service then **re-reads
    the changed row through its own SQL** before fanning it out, and a row
    the SQL no longer returns goes out as a delete. When ``watch_tables``
    names tables beyond the primary one (the tables the SQL joins), the
    service keeps the query's result set in memory and, after a change to
    one of them, re-runs the SQL and sends only the rows that differ. A
    subscriber therefore sees a joined column change live, as an update to
    the primary row it belongs to.

    ``_mkio_row`` is built from the primary table's key followed by each
    watched table's, which tells apart the rows of a one-to-many join. A
    one-to-one join wants ``key`` set to the primary key alone, so the
    identity a client tracks the record by does not change with the join.

    A secondary table may change far more often than the columns the SQL
    reads from it: ``watch_columns`` names those columns, and the service
    remembers their last values per row of that table, so an event that
    leaves them as they were costs a comparison and no re-query. The first
    sighting of a row, and a delete, always re-query.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._table = self.config["primary_table"]
        self._sql = self.config.get("sql", f"SELECT * FROM {self._table}")
        self._filterable = set(self.config.get("filterable", []))
        self._formatter = self.config.get("_compiled_formatter")
        self._max_buffer = self.config.get("max_buffer", _DEFAULT_MAX_BUFFER)

        self._subscribers: list[QuerySubscriber] = []
        # Subscribers whose snapshot is being read or paged out. Changes are
        # buffered for them from before the read, so none falls between the
        # snapshot and the live feed. A list, found by (ws, subid): every
        # page of a UI names its tables alike, so a subid alone is not one
        # subscription.
        self._pending: list[QuerySubscriber] = []
        self._pending_counter: int = 0
        self._bus_queue: asyncio.Queue[ChangeEvent] | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._timeout_task: asyncio.Task[None] | None = None
        self._pk_cols: list[str] = []
        # Secondary tables' key columns, and the last values seen of each
        # one's watched columns, per row: table -> row key -> values.
        self._table_pk: dict[str, list[str]] = {}
        self._watch_columns: dict[str, list[str]] = dict(self.config.get("watch_columns", {}))
        self._seen: dict[str, dict[str, tuple[Any, ...]]] = {t: {} for t in self._watch_columns}
        # Re-query mode (custom sql): the primary table's key columns, and
        # the SQL that reads one primary row's query rows by them.
        self._primary_pk: list[str] = []
        self._row_sql: str | None = None
        # The result set, kept only when a secondary table is watched:
        # primary row id -> query row id -> row as the SQL returned it.
        self._cache: dict[str, dict[str, dict[str, Any]]] | None = None

    async def start(self) -> None:
        tables = self.config.get("watch_tables", [self._table])
        ordered = [self._table] + [t for t in tables if t != self._table]
        seen: set[str] = set()
        self._pk_cols = []
        for table in ordered:
            info = await self.db.read(f"PRAGMA table_info({table})")
            pk = [r["name"] for r in sorted(info, key=lambda r: r["pk"]) if r["pk"] > 0]
            self._table_pk[table] = pk
            if table == self._table:
                self._primary_pk = pk
            for name in pk:
                if name not in seen:
                    self._pk_cols.append(name)
                    seen.add(name)
        if self.config.get("key"):
            self._pk_cols = list(self.config["key"])

        if "sql" in self.config:
            await self._setup_requery(secondary=[t for t in ordered if t != self._table])

        watch = self.config.get("watch_tables", [self._table])
        self._bus_queue = self.bus.subscribe(watch)
        self._listener_task = asyncio.create_task(self._listen_changes())
        self._timeout_task = asyncio.create_task(self._check_timeouts())

    async def _setup_requery(self, secondary: list[str]) -> None:
        """Arm re-query mode for a custom ``sql``, if its rows can be found
        by the primary table's key. A SQL that leaves a key column out (or
        aliases it away) cannot be re-read one row at a time; it is served
        the way the default SQL is, with a warning, so nothing breaks."""
        columns = set(await self.db.read_columns(self._sql))
        missing = [c for c in self._primary_pk if c not in columns]
        if not self._primary_pk or missing:
            logger.warning(
                "query service %r: sql does not return %s of %s; live updates "
                "carry the bare row, not the query's",
                self.name, missing or "the primary key", self._table,
            )
            return
        inner = self._sql.strip().rstrip(";")
        where = " AND ".join(f"{c} = ?" for c in self._primary_pk)
        self._row_sql = f"SELECT * FROM ({inner}) WHERE {where}"
        if secondary:
            self._cache = self._index(await self.db.read(self._sql))

    async def stop(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._timeout_task:
            self._timeout_task.cancel()
            try:
                await self._timeout_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._bus_queue:
            watch = self.config.get("watch_tables", [self._table])
            self.bus.unsubscribe(watch, self._bus_queue)

    def _generate_subid(self) -> str:
        self._pending_counter += 1
        return f"_mkio_q_{self._pending_counter}"

    async def on_subscribe(self, ws: WebSocketResponse, msg: dict[str, Any]) -> int:
        filter_expr = msg.get("filter")
        subid = msg.get("subid")
        want_snapshot = msg.get("snapshot", True)
        want_updates = msg.get("updates", True)
        maxcount = msg.get("maxcount", 0)
        fields = msg.get("fields")

        if isinstance(maxcount, int) and maxcount > 0:
            paginating = True
        else:
            paginating = False
            maxcount = 0

        if paginating and not subid:
            subid = self._generate_subid()

        filter_fn = None
        if filter_expr and self._filterable:
            filter_fn = compile_filter(filter_expr)

        sent_rows = set() if filter_fn else None
        sub = QuerySubscriber(
            ws=ws,
            filter_fn=filter_fn,
            formatter=self._formatter,
            subid=subid,
            fields=fields,
            sent_rows=sent_rows,
            maxcount=maxcount,
            want_updates=want_updates,
            max_buffer=self._max_buffer,
        )

        if not want_snapshot:
            if want_updates:
                self._subscribers.append(sub)
                return 1
            return 0

        # Pending from before the read: a change committed while the
        # snapshot is read or sent is buffered, not lost.
        self._pending.append(sub)
        try:
            rows = await self.db.read(self._sql)
        except BaseException:
            self._drop_pending(sub)
            raise
        if self._cache is not None:
            self._cache = self._index(rows)  # a free resync
        out_rows = []
        for row in rows:
            out_row = sub.formatter(row) if sub.formatter else row
            if sub.filter_fn and not sub.filter_fn(out_row):
                continue
            rid = self._row_id(row)
            if rid is not None:
                sub.snapshot_ids.add(rid)
                if sent_rows is not None:
                    sent_rows.add(rid)
            out_rows.append(self._project(self._tag_row(row, out_row), fields))

        page = out_rows[:maxcount] if paginating else out_rows
        remaining = out_rows[maxcount:] if paginating else []
        hasmore = len(remaining) > 0
        if hasmore:
            sub.max_buffer = max(self._max_buffer, len(out_rows) + maxcount)
            sub.pending_rows = remaining
            sub.last_activity = time.monotonic()

        resp = make_snapshot(None, self.name, page, subid=sub.subid, hasmore=hasmore)
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)

        if hasmore:
            return 1
        await self._finalize_pending(sub)
        return 1 if want_updates else 0

    def _find_pending(self, ws: WebSocketResponse, subid: str) -> QuerySubscriber | None:
        for sub in self._pending:
            if sub.ws is ws and sub.subid == subid:
                return sub
        return None

    def _drop_pending(self, sub: QuerySubscriber) -> None:
        self._pending = [s for s in self._pending if s is not sub]

    async def _nack_reset(self, sub: QuerySubscriber, message: str) -> None:
        """Tell a subscriber the server lost its place; it subscribes again."""
        resp = make_nack(self.name, message, subid=sub.subid, code="reset")
        try:
            await sub.ws.send_bytes(resp)
        except (ConnectionError, RuntimeError):
            return
        await self.notify_monitors("out", resp)

    async def on_getmore(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        subid = msg.get("subid")
        sub = self._find_pending(ws, subid) if subid else None

        if sub is None:
            # No subid is a client bug; an unknown one is a page sequence
            # that timed out here, which subscribing again puts right.
            resp = make_nack(
                self.name, "unknown subid" if subid else "missing subid",
                subid=subid, code="reset" if subid else None,
            )
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
            return

        if sub.overflowed:
            self._drop_pending(sub)
            await self._nack_reset(sub, "subscription reset: update buffer overflow")
            return

        sub.last_activity = time.monotonic()

        page = sub.pending_rows[:sub.maxcount]
        sub.pending_rows = sub.pending_rows[sub.maxcount:]
        hasmore = len(sub.pending_rows) > 0

        resp = make_snapshot(None, self.name, page, subid=subid, hasmore=hasmore)
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)

        if not hasmore:
            await self._finalize_pending(sub)

    async def _finalize_pending(self, sub: QuerySubscriber) -> None:
        """The snapshot is out: deliver what was buffered and go live."""
        if not any(s is sub for s in self._pending):
            return  # unsubscribed or timed out meanwhile
        self._drop_pending(sub)
        if not sub.want_updates:
            return
        if sub.overflowed:
            await self._nack_reset(sub, "subscription reset: update buffer overflow")
            return

        # The snapshot may already reflect a buffered change — the read can
        # land after the commit — and sending it again is harmless, except
        # that a row the client holds must not arrive as an insert.
        known = sub.snapshot_ids
        for op, rid, row, cause in sub.buffered_updates:
            if rid is not None:
                if op == "insert" and rid in known:
                    op = "update"
                elif op == "delete":
                    known.discard(rid)
                else:
                    known.add(rid)
            await sub.ws.send_bytes(make_update(self.name, ref=None, op=op, row=row,
                                                subid=sub.subid, cause=cause))
        sub.buffered_updates = []
        sub.snapshot_ids = set()
        self._subscribers.append(sub)

    async def on_message(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type", "")
        if msg_type == "getmore":
            await self.on_getmore(ws, msg)

    def _row_id(self, row: dict[str, Any]) -> str | None:
        return self._key(row, self._pk_cols)

    def _primary_id(self, row: dict[str, Any]) -> str | None:
        """The row's identity by the primary table's key alone — what a
        delete event carries, and what a re-read looks a row up by."""
        return self._key(row, self._primary_pk)

    @staticmethod
    def _key(row: dict[str, Any], cols: list[str]) -> str | None:
        if not cols:
            return None
        try:
            vals = [row[col] for col in cols]
        except (KeyError, TypeError):
            return None
        if len(vals) == 1:
            return str(vals[0])
        return json.dumps(vals, separators=(",", ":"))

    def _cache_key(self, row: dict[str, Any]) -> str:
        """A query row's key within its primary row: the full row id, or
        the row itself when the SQL returns no usable key."""
        return self._row_id(row) or json.dumps(row, sort_keys=True, default=str)

    def _index(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
        index: dict[str, dict[str, dict[str, Any]]] = {}
        for row in rows:
            pid = self._primary_id(row)
            if pid is None:
                continue
            index.setdefault(pid, {})[self._cache_key(row)] = row
        return index

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
        before = len(self._subscribers) + len(self._pending)
        subid = msg.get("subid")
        if subid is not None:
            def keep(s: QuerySubscriber) -> bool:
                return not (s.ws is ws and s.subid == subid)
        else:
            def keep(s: QuerySubscriber) -> bool:
                return s.ws is not ws
        self._subscribers = [s for s in self._subscribers if keep(s)]
        self._pending = [s for s in self._pending if keep(s)]
        return before - len(self._subscribers) - len(self._pending)

    async def _listen_changes(self) -> None:
        """Consume change events, fan out to live subscribers and buffer for paginating ones.

        Events are taken in bursts — everything already queued goes out in
        one pass — so a batch that touched a joined table many times costs
        one re-query, not one per row.
        """
        assert self._bus_queue is not None
        while True:
            burst = [await self._bus_queue.get()]
            while True:
                try:
                    burst.append(self._bus_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            # One bad event must not end the feed for every subscriber, now
            # and to come: whatever it raises is logged and the loop goes on.
            if self.bus.take_overflow(self._bus_queue):
                try:
                    await self._resync(burst[-1])
                except Exception:
                    logger.exception("query service %r: resync failed", self.name)
                continue
            secondary: ChangeEvent | None = None
            for event in burst:
                try:
                    if event.table == self._table or self._cache is None:
                        await self._on_event(event)
                    elif not self._unchanged(event):
                        secondary = event  # the last one stands for the burst
                except Exception:
                    logger.exception("query service %r: change to %r not delivered", self.name, event.table)
            if secondary is not None:
                try:
                    await self._on_secondary(secondary)
                except Exception:
                    logger.exception("query service %r: re-query failed", self.name)

    async def _resync(self, last: ChangeEvent) -> None:
        """The change queue overflowed, so events were missed. With the
        result set cached, a re-run of the SQL publishes whatever differs;
        without one there is nothing to diff against, and each subscriber is
        reset to subscribe again for a fresh snapshot."""
        for seen in self._seen.values():
            seen.clear()
        if self._cache is not None:
            await self._requery_diff(last.ref or next_ref(), None)
            return
        subscribers, self._subscribers = self._subscribers, []
        for sub in self._pending:
            sub.overflowed = True
            sub.pending_rows = []
            sub.buffered_updates = []
        for sub in subscribers:
            await self._nack_reset(sub, "subscription reset: change queue overflow")

    def _unchanged(self, event: ChangeEvent) -> bool:
        """Whether a secondary-table event left its watched columns as the
        service last saw them, so the SQL's result cannot have moved."""
        columns = self._watch_columns.get(event.table)
        if not columns:
            return False
        seen = self._seen[event.table]
        key = self._key(event.row, self._table_pk.get(event.table, []))
        if key is None:
            return False
        if event.op == "delete":
            seen.pop(key, None)
            return False
        values = tuple(event.row.get(c) for c in columns)
        if seen.get(key) == values:
            return True
        seen[key] = values
        return False

    async def _on_event(self, event: ChangeEvent) -> None:
        """A change to the primary table (or, without re-query, any watched
        table): fan the row out — as it came, or re-read through the SQL."""
        row = dict(event.row)
        row["_mkio_ref"] = event.ref
        if self._row_sql is None or event.table != self._table:
            await self._fan_out(event.op, row, event.cause)
            return

        pid = self._primary_id(event.row)
        if event.op == "delete":
            cached = self._cache.pop(pid, None) if self._cache is not None and pid else None
            if cached:
                # The bare delete names only the key; the cached rows carry
                # the joined columns and the full _mkio_row a client keys by.
                for old in cached.values():
                    await self._fan_out("delete", self._stamp(old, event.ref), event.cause)
            else:
                await self._fan_out("delete", row, event.cause)
            return

        if pid is None:
            await self._fan_out(event.op, row, event.cause)  # no key to re-read by
            return
        fresh = await self.db.read(self._row_sql, tuple(event.row[c] for c in self._primary_pk))
        if self._cache is None:
            if not fresh:
                # The SQL no longer returns it (a WHERE it fails now): to a
                # subscriber that is a delete.
                await self._fan_out("delete", row, event.cause)
            for new in fresh:
                await self._fan_out(event.op, self._stamp(new, event.ref), event.cause)
            return

        old = self._cache.pop(pid, {})
        new = {self._cache_key(r): r for r in fresh}
        if new:
            self._cache[pid] = new
        await self._diff_out(old, new, event.ref, event.cause)

    async def _on_secondary(self, event: ChangeEvent) -> None:
        """A change to a joined table: re-run the SQL and send what differs."""
        await self._requery_diff(event.ref, event.cause)

    async def _requery_diff(self, ref: str, cause: str | None) -> None:
        assert self._cache is not None
        before = self._cache
        self._cache = self._index(await self.db.read(self._sql))
        for pid in before.keys() | self._cache.keys():
            await self._diff_out(before.get(pid, {}), self._cache.get(pid, {}), ref, cause)

    async def _diff_out(
        self, old: dict[str, dict[str, Any]], new: dict[str, dict[str, Any]],
        ref: str, cause: str | None,
    ) -> None:
        """Fan out one primary row's query rows as inserts, updates and
        deletes against what the cache held. The op is what the subscriber
        needs, not what the writer did: a row the cache did not hold is an
        insert whatever wrote it, and an unchanged row is nothing at all."""
        for key, row in new.items():
            if key not in old:
                await self._fan_out("insert", self._stamp(row, ref), cause)
            elif old[key] != row:
                await self._fan_out("update", self._stamp(row, ref), cause)
        for key, row in old.items():
            if key not in new:
                await self._fan_out("delete", self._stamp(row, ref), cause)

    @staticmethod
    def _stamp(row: dict[str, Any], ref: str) -> dict[str, Any]:
        out = dict(row)
        out["_mkio_ref"] = ref
        return out

    async def _fan_out(self, op: str, row: dict[str, Any], cause: str | None) -> None:
        """One change to every subscriber: filtered, formatted, projected."""
        dead: list[QuerySubscriber] = []
        notified_monitor = False
        rid = self._row_id(row)
        is_delete = op == "delete"

        # Buffer for paginating subscribers
        for sub in list(self._pending):
            if sub.overflowed or not sub.want_updates:
                continue
            out_row = sub.formatter(row) if sub.formatter and not is_delete else row
            if is_delete:
                if sub.sent_rows is not None and rid is not None and rid not in sub.sent_rows:
                    continue
                if sub.sent_rows is not None and rid is not None:
                    sub.sent_rows.discard(rid)
            elif sub.filter_fn and not sub.filter_fn(out_row):
                if sub.sent_rows is not None and rid is not None and rid in sub.sent_rows:
                    sub.sent_rows.discard(rid)
                    tagged = self._project(self._tag_row(row, out_row), sub.fields)
                    sub.buffered_updates.append(("delete", rid, tagged, cause))
                continue
            else:
                if sub.sent_rows is not None and rid is not None:
                    sub.sent_rows.add(rid)
            tagged = self._project(self._tag_row(row, out_row), sub.fields)
            sub.buffered_updates.append((op, rid, tagged, cause))
            total = len(sub.pending_rows) + len(sub.buffered_updates)
            if total > sub.max_buffer:
                sub.overflowed = True
                sub.pending_rows = []
                sub.buffered_updates = []

        # Fan out to live subscribers
        for sub in list(self._subscribers):
            out_row = sub.formatter(row) if sub.formatter and not is_delete else row
            if is_delete:
                if sub.sent_rows is not None and rid is not None and rid not in sub.sent_rows:
                    continue
                if sub.sent_rows is not None and rid is not None:
                    sub.sent_rows.discard(rid)
            elif sub.filter_fn and not sub.filter_fn(out_row):
                if sub.sent_rows is not None and rid is not None and rid in sub.sent_rows:
                    sub.sent_rows.discard(rid)
                    try:
                        tagged = self._project(self._tag_row(row, out_row), sub.fields)
                        msg_bytes = make_update(self.name, ref=None, op="delete", row=tagged,
                                              subid=sub.subid, cause=cause)
                        await sub.ws.send_bytes(msg_bytes)
                        if not notified_monitor:
                            await self.notify_monitors("out", msg_bytes)
                            notified_monitor = True
                    except (ConnectionError, RuntimeError):
                        dead.append(sub)
                continue
            else:
                if sub.sent_rows is not None and rid is not None:
                    sub.sent_rows.add(rid)
            try:
                tagged = self._project(self._tag_row(row, out_row), sub.fields)
                msg_bytes = make_update(self.name, ref=None, op=op, row=tagged,
                                    subid=sub.subid, cause=cause)
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

    async def _check_timeouts(self) -> None:
        """Periodically remove paginating subscribers that have gone idle."""
        while True:
            await asyncio.sleep(10)
            self._expire_pending()

    def _expire_pending(self) -> None:
        now = time.monotonic()
        self._pending = [
            sub for sub in self._pending
            if now - sub.last_activity <= _GETMORE_TIMEOUT
        ]
