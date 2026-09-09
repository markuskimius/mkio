"""Write batcher: collects writes, executes in single SQLite transaction."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from mkio._ref import next_ref
from mkio.change_bus import ChangeBus, ChangeEvent
from mkio.database import Database
from mkio.history import HistorySpec, VersionPlan


@dataclass(frozen=True, slots=True)
class CompiledOp:
    table: str
    op_type: str  # "insert" | "update" | "delete" | "upsert" | "undo" | "redo"
    sql: str
    param_names: tuple[str, ...]
    bind: dict[str, tuple[int, str]] = field(default_factory=dict)
    # bind: param_name -> (op_index, field_name) for cross-op references
    defaults: dict[str, Any] = field(default_factory=dict)
    # defaults: param_name -> static value (client doesn't need to provide)
    plan: VersionPlan | None = None
    # plan: two-step cursor move for undo/redo, in place of a single `sql`


@dataclass(slots=True)
class WriteRequest:
    ops: tuple[CompiledOp, ...]
    params_list: tuple[tuple[Any, ...], ...]
    data: dict[str, Any]
    future: asyncio.Future[dict[str, Any]]
    ref: str | None = None
    user: str | None = None      # authenticated user, recorded in history
    service: str | None = None   # originating service, recorded in history


class WriteBatcher:
    def __init__(
        self,
        db: Database,
        change_bus: ChangeBus,
        batch_max_size: int = 500,
        batch_max_wait_ms: float = 2.0,
        versioned: dict[str, HistorySpec] | None = None,
        versioned_configs: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._db = db
        self._bus = change_bus
        self._batch_max_size = batch_max_size
        self._batch_max_wait_ms = batch_max_wait_ms
        # base table -> history capture spec (empty when nothing is versioned)
        self._versioned: dict[str, HistorySpec] = versioned or {}
        self._versioned_configs: dict[str, dict[str, Any]] = versioned_configs or {}
        self._queue: asyncio.Queue[WriteRequest] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    @property
    def versioned_tables(self) -> frozenset[str]:
        """Base tables whose changes are captured to a history table."""
        return frozenset(self._versioned)

    @property
    def versioned_configs(self) -> dict[str, dict[str, Any]]:
        """Table configs of the versioned tables, for compiling version ops."""
        return self._versioned_configs

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self, drain: bool = True) -> None:
        """Stop the writer. If drain=True, commit all queued writes first."""
        self._stopping = True
        # Always send a sentinel to wake the run loop (it may be blocked on queue.get)
        sentinel = WriteRequest(
            ops=(), params_list=(), data={},
            future=asyncio.get_running_loop().create_future(),
        )
        self._queue.put_nowait(sentinel)
        if self._task:
            if drain:
                try:
                    await asyncio.wait_for(self._task, timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
            else:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

    async def submit(
        self,
        ops: tuple[CompiledOp, ...],
        params_list: tuple[tuple[Any, ...], ...],
        data: dict[str, Any],
        ref: str | None = None,
        user: str | None = None,
        service: str | None = None,
    ) -> dict[str, Any]:
        """Submit a write request. Returns when the write commits.

        ``user`` and ``service`` are recorded on history rows for versioned
        tables and are otherwise unused.
        """
        if self._stopping:
            raise RuntimeError("Writer is stopping, no new submissions accepted")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        req = WriteRequest(
            ops=ops, params_list=params_list, data=data, future=future,
            ref=ref, user=user, service=service,
        )
        self._queue.put_nowait(req)
        return await future

    async def _run(self) -> None:
        """Main loop: collect batch, execute, commit, publish, resolve."""
        while True:
            batch = await self._collect_batch()
            if not batch:
                if self._stopping:
                    return
                continue
            await self._execute_batch(batch)
            if self._stopping and self._queue.empty():
                return

    async def _collect_batch(self) -> list[WriteRequest]:
        """Collect writes until batch is full or timeout expires."""
        try:
            if self._stopping and self._queue.empty():
                return []
            first = await asyncio.wait_for(
                self._queue.get(), timeout=0.1 if self._stopping else None
            )
        except asyncio.TimeoutError:
            return []

        # Skip sentinels
        if not first.ops and not first.data:
            if not first.future.done():
                first.future.set_result({"ok": True})
            if self._stopping and self._queue.empty():
                return []
            # Try to get real items
            try:
                first = await asyncio.wait_for(self._queue.get(), timeout=0.01)
            except asyncio.TimeoutError:
                return []

        batch = [first]
        deadline = time.monotonic() + (self._batch_max_wait_ms / 1000.0)

        while len(batch) < self._batch_max_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                req = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                # Skip sentinels
                if not req.ops and not req.data:
                    if not req.future.done():
                        req.future.set_result({"ok": True})
                    continue
                batch.append(req)
            except asyncio.TimeoutError:
                break

        return batch

    async def _execute_version_op(
        self,
        conn: Any,
        op: CompiledOp,
        req: WriteRequest,
        ref: str,
    ) -> tuple[dict[str, Any], str, dict[str, Any] | None]:
        """Move a row's version cursor for an undo or redo.

        Runs the plan's primary statement — step back or forward onto an
        adjacent recorded version — and falls back to the edge case: undo at
        version 1 removes the row, redo from an absent row rebuilds version 1.
        History is left untouched either way, so the step stays reversible.

        Returns the row the cursor landed on, the op that describes the move,
        and the row as it stood beforehand (None if there was none).  The
        before-and-after pair travels on the change event so an application can
        work out the dependent action a given undo or redo implies.
        """
        plan = op.plan
        assert plan is not None
        cursor = await conn.execute(
            plan.current_sql, tuple(req.data[n] for n in plan.current_params)
        )
        before = await cursor.fetchone()
        await cursor.close()
        old = dict(before) if before is not None else None
        for sql, names, emit_op in (
            (plan.primary_sql, plan.primary_params, plan.primary_op),
            (plan.fallback_sql, plan.fallback_params, plan.fallback_op),
        ):
            params = tuple(
                ref if n == "_mkio_ref" else req.data[n] for n in names
            )
            cursor = await conn.execute(sql, params)
            row = await cursor.fetchone()
            await cursor.close()
            if row is not None:
                return dict(row), emit_op, old
        raise ValueError(
            f"{plan.empty_message}: no recorded version for the given key "
            f"in {op.table!r}"
        )

    async def _capture_history(
        self,
        conn: Any,
        hist: HistorySpec,
        op: CompiledOp,
        rows: list[dict[str, Any]],
        req: WriteRequest,
        ref: str,
    ) -> list[ChangeEvent]:
        """Record changed rows in a history table, inside the caller's SAVEPOINT.

        Writing at version V first discards the recorded versions at V and
        above: an edit made after an undo abandons the redo branch it left
        behind.  A delete drops the row's history outright.

        Returns change events for the history table, but only when something is
        subscribed to it — the common case pays nothing for the feed.
        """
        publish = self._bus.has_subscribers(hist.table)
        captured: list[ChangeEvent] = []
        for row_data in rows:
            if op.op_type == "delete":
                await (await conn.execute(
                    hist.truncate_all_sql, hist.key_params(row_data)
                )).close()
                continue
            await (await conn.execute(
                hist.truncate_sql, hist.truncate_params(row_data)
            )).close()
            await (await conn.execute(
                hist.insert_sql,
                hist.insert_params(row_data, op.op_type, ref, req.user, req.service),
            )).close()
            if publish:
                hist_row = {
                    "_mkio_version": row_data.get("_mkio_version"),
                    "_mkio_op": op.op_type,
                    "_mkio_ref": ref,
                    "_mkio_user": req.user,
                    "_mkio_service": req.service,
                    **{c: row_data.get(c) for c in hist.columns},
                }
                captured.append(
                    ChangeBus.make_event(hist.table, "insert", hist_row, ref)
                )
        return captured

    async def _execute_batch(self, batch: list[WriteRequest]) -> None:
        """Execute all writes in a single SQLite transaction with SAVEPOINTs."""
        conn = self._db.write_conn
        events: list[ChangeEvent] = []
        successful: list[tuple[WriteRequest, str]] = []  # (req, ref)
        failed: list[tuple[WriteRequest, Exception]] = []

        try:
            for i, req in enumerate(batch):
                savepoint = f"req_{i}"
                try:
                    ref = req.ref if req.ref else next_ref()
                    await (await conn.execute(f"SAVEPOINT {savepoint}")).close()
                    returned_rows: list[tuple[CompiledOp, dict[str, Any]]] = []
                    history_events: list[ChangeEvent] = []
                    # One event per statement, held back until the SAVEPOINT is
                    # released.  Undo/redo decide their op at execution time, so
                    # it is not always op.op_type.
                    emitted: list[ChangeEvent] = []
                    for op_idx, (op, params) in enumerate(zip(req.ops, req.params_list)):
                        # Resolve cross-op bindings and _mkio_ref
                        resolved = list(params)
                        if op.bind:
                            for param_name, (src_idx, src_field) in op.bind.items():
                                param_pos = op.param_names.index(param_name)
                                resolved[param_pos] = returned_rows[src_idx][1][src_field]
                        if "_mkio_ref" in op.param_names:
                            resolved[op.param_names.index("_mkio_ref")] = ref
                        params = tuple(resolved)
                        hist = self._versioned.get(op.table)
                        if op.plan is not None:
                            row, emit_op, old_row = await self._execute_version_op(
                                conn, op, req, ref
                            )
                            returned_rows.append((op, row))
                            emitted.append(ChangeBus.make_event(
                                op.table, emit_op, row, ref,
                                cause=op.op_type, old=old_row,
                            ))
                            continue
                        cursor = await conn.execute(op.sql, params)
                        if hist is not None:
                            # Versioned ops RETURN every affected row (deletes
                            # included) so each one is recorded.
                            rows = [dict(r) for r in await cursor.fetchall()]
                        elif op.op_type != "delete":
                            row = await cursor.fetchone()
                            rows = [dict(row)] if row else []
                        else:
                            rows = []
                        await cursor.close()
                        returned_rows.append((op, rows[0] if rows else req.data))
                        emitted.append(ChangeBus.make_event(
                            op.table, op.op_type, rows[0] if rows else req.data, ref
                        ))
                        if hist is not None:
                            history_events.extend(
                                await self._capture_history(conn, hist, op, rows, req, ref)
                            )
                    await (await conn.execute(f"RELEASE {savepoint}")).close()

                    successful.append((req, ref))

                    events.extend(emitted)
                    events.extend(history_events)
                except Exception as exc:
                    try:
                        await (await conn.execute(f"ROLLBACK TO {savepoint}")).close()
                        await (await conn.execute(f"RELEASE {savepoint}")).close()
                    except Exception:
                        pass
                    failed.append((req, exc))

            await conn.commit()
        except Exception as exc:
            # Entire commit failed
            for req in batch:
                if not req.future.done():
                    req.future.set_exception(exc)
            return

        # Publish changes after successful commit
        if events:
            self._bus.publish(events)

        # Resolve futures
        for req, ref in successful:
            if not req.future.done():
                req.future.set_result({"ok": True, "ref": ref})

        for req, exc in failed:
            if not req.future.done():
                req.future.set_exception(exc)
