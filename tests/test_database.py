"""Tests for the database layer's connection setup."""

from __future__ import annotations

import asyncio
import os
import sqlite3

import pytest
import pytest_asyncio

from mkio.database import Database
from mkio.writer import CompiledOp


TEST_TABLES = {
    "orders": {
        "columns": {
            "id": "TEXT PRIMARY KEY",
            "symbol": "TEXT",
            "qty": "INTEGER",
        },
    },
}

INSERT_ORDER = CompiledOp(
    table="orders",
    op_type="insert",
    sql="INSERT INTO orders (id, symbol, qty) VALUES (?, ?, ?) RETURNING *",
    param_names=("id", "symbol", "qty"),
)


@pytest_asyncio.fixture
async def file_db(tmp_path):
    path = str(tmp_path / "t.db")
    database = Database(path, TEST_TABLES, config={
        "db_path": path, "tables": TEST_TABLES, "auto_migrate": "safe",
    })
    await database.start()
    yield database
    await database.stop()


async def _pragma(conn, name):
    async with conn.execute(f"PRAGMA {name}") as cursor:
        row = await cursor.fetchone()
    return row[0]


class TestReadsDuringWrites:
    """An in-memory database needs both connections on one shared cache, which
    cannot use WAL and therefore locks per table: a read of a table the writer
    holds open fails with SQLITE_LOCKED, and no busy timeout retries it."""

    async def test_memory_read_during_open_write_transaction(self, db):
        await db.write_conn.execute("BEGIN")
        await db.write_conn.execute(
            "INSERT INTO orders (id, symbol, qty) VALUES ('held', 'AAPL', 1)"
        )
        try:
            rows = await db.read("SELECT * FROM orders")
        finally:
            await db.write_conn.rollback()
        assert [r["id"] for r in rows] == ["held"]

    async def test_file_read_during_open_write_transaction(self, file_db):
        """WAL gives a file database real snapshot isolation, so the same read
        succeeds there and sees the pre-transaction state."""
        await file_db.write_conn.execute("BEGIN")
        await file_db.write_conn.execute(
            "INSERT INTO orders (id, symbol, qty) VALUES ('held', 'AAPL', 1)"
        )
        try:
            rows = await file_db.read("SELECT * FROM orders")
        finally:
            await file_db.write_conn.rollback()
        assert rows == []

    async def test_concurrent_reads_while_batches_commit(self, writer, db):
        """The same contention through the real write path: reads of a table a
        batch is writing keep answering rather than raising."""
        async def spam_reads():
            for _ in range(200):
                await db.read("SELECT * FROM orders")
                await asyncio.sleep(0)
            return 200

        reads = asyncio.create_task(spam_reads())
        await asyncio.gather(*(
            writer.submit(
                ops=(INSERT_ORDER,),
                params_list=((f"o{i}", "AAPL", i),),
                data={"id": f"o{i}"},
            )
            for i in range(50)
        ))
        assert await reads == 200
        assert len(await db.read("SELECT * FROM orders")) == 50


class TestReadUncommittedScope:
    """read_uncommitted is the remedy for the shared cache's table locks, so it
    is set on the in-memory read connection and nowhere else — the writer must
    never see uncommitted data, and a file database has WAL instead."""

    async def test_set_on_memory_read_connection(self, db):
        assert await _pragma(db.read_conn, "read_uncommitted") == 1

    async def test_not_set_on_memory_write_connection(self, db):
        assert await _pragma(db.write_conn, "read_uncommitted") == 0

    @pytest.mark.parametrize("which", ["read_conn", "write_conn"])
    async def test_not_set_on_a_file_database(self, file_db, which):
        conn = getattr(file_db, which)
        assert await _pragma(conn, "read_uncommitted") == 0
        assert (await _pragma(conn, "journal_mode")).lower() == "wal"


class TestStopWithStrandedReader:
    """The final TRUNCATE checkpoint in stop() waits for every reader to leave
    its snapshot, up to the 5 s busy timeout. A cursor can hold one after its
    owner is gone: a task cancelled while its execute() was queued never gets
    the cursor, but the aiosqlite thread still makes it and keeps it alive in
    its locals until the next request on that connection. Closing the reader
    before the checkpoint finalizes it, so stop() stays instant and the
    checkpoint completes."""

    async def _open(self, tmp_path):
        path = str(tmp_path / "t.db")
        database = Database(path, TEST_TABLES, config={
            "db_path": path, "tables": TEST_TABLES, "auto_migrate": "safe",
        })
        await database.start()
        for i in range(50):
            await (await database.write_conn.execute(
                "INSERT INTO orders (id, symbol, qty) VALUES (?, 'AAPL', ?)", (f"o{i}", i),
            )).close()
        await database.write_conn.commit()
        return database, path

    async def _assert_fast_and_intact(self, database, path):
        started = asyncio.get_running_loop().time()
        await database.stop()
        assert asyncio.get_running_loop().time() - started < 1.0
        assert not os.path.exists(path + "-wal")
        conn = sqlite3.connect(path)
        try:
            assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 50
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            conn.close()

    async def test_reader_cancelled_while_its_execute_is_queued(self, tmp_path):
        database, path = await self._open(tmp_path)

        async def reader():
            async with database.read_conn.execute("SELECT * FROM orders") as cursor:
                await cursor.fetchall()

        task = asyncio.ensure_future(reader())
        await asyncio.sleep(0)  # the execute is queued to the read thread
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.05)  # the thread has produced the cursor nobody reads
        await self._assert_fast_and_intact(database, path)

    async def test_stepped_cursor_dropped_without_close(self, tmp_path):
        database, path = await self._open(tmp_path)
        cursor = await database.read_conn.execute("SELECT * FROM orders")
        await cursor.fetchone()
        del cursor  # not closed: the thread's last result still references it
        await self._assert_fast_and_intact(database, path)
