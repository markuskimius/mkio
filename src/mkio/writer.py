"""Write batcher: collects writes, executes in single SQLite transaction."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from mkio._version import next_version
from mkio.change_bus import ChangeBus, ChangeEvent
from mkio.database import Database


@dataclass(frozen=True, slots=True)
class CompiledOp:
    table: str
    op_type: str  # "insert" | "update" | "delete" | "upsert"
    sql: str
    param_names: tuple[str, ...]
    bind: dict[str, tuple[int, str]] = field(default_factory=dict)
    # bind: param_name -> (op_index, field_name) for cross-op references
    defaults: dict[str, Any] = field(default_factory=dict)
    # defaults: param_name -> static value (client doesn't need to provide)


@dataclass(slots=True)
class WriteRequest:
    ops: tuple[CompiledOp, ...]
    params_list: tuple[tuple[Any, ...], ...]
    data: dict[str, Any]
    future: asyncio.Future[dict[str, Any]]


class WriteBatcher:
    def __init__(
        self,
        db: Database,
        change_bus: ChangeBus,
        batch_max_size: int = 500,
        batch_max_wait_ms: float = 2.0,
    ) -> None:
        self._db = db
        self._bus = change_bus
        self._batch_max_size = batch_max_size
        self._batch_max_wait_ms = batch_max_wait_ms
        self._queue: asyncio.Queue[WriteRequest] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

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
    ) -> dict[str, Any]:
        """Submit a write request. Returns when the write commits."""
        if self._stopping:
            raise RuntimeError("Writer is stopping, no new submissions accepted")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        req = WriteRequest(ops=ops, params_list=params_list, data=data, future=future)
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

    async def _execute_batch(self, batch: list[WriteRequest]) -> None:
        """Execute all writes in a single SQLite transaction with SAVEPOINTs."""
        conn = self._db.write_conn
        events: list[ChangeEvent] = []
        successful: list[tuple[WriteRequest, str]] = []  # (req, version)
        failed: list[tuple[WriteRequest, Exception]] = []

        try:
            for i, req in enumerate(batch):
                savepoint = f"req_{i}"
                try:
                    version = next_version()
                    await (await conn.execute(f"SAVEPOINT {savepoint}")).close()
                    returned_rows: list[tuple[CompiledOp, dict[str, Any]]] = []
                    for op_idx, (op, params) in enumerate(zip(req.ops, req.params_list)):
                        # Resolve cross-op bindings and _mkio_ref
                        resolved = list(params)
                        if op.bind:
                            for param_name, (src_idx, src_field) in op.bind.items():
                                param_pos = op.param_names.index(param_name)
                                resolved[param_pos] = returned_rows[src_idx][1][src_field]
                        if "_mkio_ref" in op.param_names:
                            resolved[op.param_names.index("_mkio_ref")] = version
                        params = tuple(resolved)
                        cursor = await conn.execute(op.sql, params)
                        if op.op_type != "delete":
                            row = await cursor.fetchone()
                        else:
                            row = None
                        await cursor.close()
                        returned_rows.append((op, dict(row) if row else req.data))
                    await (await conn.execute(f"RELEASE {savepoint}")).close()

                    successful.append((req, version))

                    for op, row_data in returned_rows:
                        events.append(
                            ChangeBus.make_event(op.table, op.op_type, row_data, version)
                        )
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
        for req, version in successful:
            if not req.future.done():
                req.future.set_result({"ok": True, "version": version})

        for req, exc in failed:
            if not req.future.done():
                req.future.set_exception(exc)
