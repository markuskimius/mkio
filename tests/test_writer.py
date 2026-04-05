"""Tests for write batcher."""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from mkio.change_bus import ChangeBus
from mkio.database import Database
from mkio.writer import CompiledOp, WriteBatcher


TEST_TABLES = {
    "orders": {
        "columns": {
            "id": "TEXT PRIMARY KEY",
            "symbol": "TEXT",
            "qty": "INTEGER",
        },
    },
    "audit_log": {
        "columns": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "event": "TEXT",
            "order_id": "TEXT",
        },
    },
}

INSERT_ORDER = CompiledOp(
    table="orders",
    op_type="insert",
    sql="INSERT INTO orders (id, symbol, qty) VALUES (?, ?, ?) RETURNING *",
    param_names=("id", "symbol", "qty"),
)

INSERT_AUDIT = CompiledOp(
    table="audit_log",
    op_type="insert",
    sql="INSERT INTO audit_log (event, order_id) VALUES (?, ?) RETURNING *",
    param_names=("event", "order_id"),
)


@pytest_asyncio.fixture
async def db():
    database = Database(":memory:", TEST_TABLES)
    await database.start()
    yield database
    await database.stop()


@pytest.fixture
def bus():
    return ChangeBus()


@pytest_asyncio.fixture
async def writer(db, bus):
    w = WriteBatcher(db=db, change_bus=bus, batch_max_size=100, batch_max_wait_ms=1.0)
    await w.start()
    yield w
    await w.stop(drain=True)


async def test_single_write(writer, db):
    result = await writer.submit(
        ops=(INSERT_ORDER,),
        params_list=(("1", "AAPL", 100),),
        data={"id": "1", "symbol": "AAPL", "qty": 100},
    )
    assert result["ok"] is True
    assert "version" in result

    rows = await db.read("SELECT * FROM orders WHERE id = '1'")
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


async def test_concurrent_writes(writer, db):
    """100 concurrent writes should all resolve with unique versions."""
    futures = []
    for i in range(100):
        f = writer.submit(
            ops=(INSERT_ORDER,),
            params_list=((str(i), f"SYM{i}", i * 10),),
            data={"id": str(i), "symbol": f"SYM{i}", "qty": i * 10},
        )
        futures.append(f)

    results = await asyncio.gather(*futures)
    assert len(results) == 100
    versions = {r["version"] for r in results}
    assert len(versions) == 100  # All unique

    rows = await db.read("SELECT COUNT(*) as cnt FROM orders")
    assert rows[0]["cnt"] == 100


async def test_multi_op_transaction(writer, db):
    """Multi-op: insert into orders AND audit_log atomically."""
    result = await writer.submit(
        ops=(INSERT_ORDER, INSERT_AUDIT),
        params_list=(
            ("1", "AAPL", 100),
            ("order_placed", "1"),
        ),
        data={"id": "1", "symbol": "AAPL", "qty": 100, "event": "order_placed", "order_id": "1"},
    )
    assert result["ok"] is True

    orders = await db.read("SELECT * FROM orders")
    assert len(orders) == 1

    audits = await db.read("SELECT * FROM audit_log")
    assert len(audits) == 1
    assert audits[0]["event"] == "order_placed"


async def test_failed_op_savepoint_rollback(writer, db):
    """If second op fails, the request rolls back but doesn't affect other requests."""
    # First: a good write
    r1 = writer.submit(
        ops=(INSERT_ORDER,),
        params_list=(("good", "MSFT", 50),),
        data={"id": "good", "symbol": "MSFT", "qty": 50},
    )
    # Second: a bad write (duplicate PK will fail after first good one commits)
    # Actually, let's use a SQL error — bad table
    bad_op = CompiledOp(
        table="nonexistent",
        op_type="insert",
        sql="INSERT INTO nonexistent (x) VALUES (?)",
        param_names=("x",),
    )
    r2 = writer.submit(
        ops=(bad_op,),
        params_list=(("oops",),),
        data={"x": "oops"},
    )

    # Good write should succeed
    result1 = await r1
    assert result1["ok"] is True

    # Bad write should fail
    with pytest.raises(Exception):
        await r2

    # Good data should be in DB
    rows = await db.read("SELECT * FROM orders WHERE id = 'good'")
    assert len(rows) == 1


async def test_change_events_published(writer, bus):
    """Change events should be published after commit."""
    q = bus.subscribe(["orders"])

    await writer.submit(
        ops=(INSERT_ORDER,),
        params_list=(("1", "AAPL", 100),),
        data={"id": "1", "symbol": "AAPL", "qty": 100},
    )

    # Give the bus a moment
    event = await asyncio.wait_for(q.get(), timeout=1.0)
    assert event.table == "orders"
    assert event.op == "insert"
    assert event.version  # Non-empty version string


async def test_multi_op_publishes_per_table(writer, bus):
    """Multi-op transaction should publish one event per op."""
    q_orders = bus.subscribe(["orders"])
    q_audit = bus.subscribe(["audit_log"])

    await writer.submit(
        ops=(INSERT_ORDER, INSERT_AUDIT),
        params_list=(
            ("1", "AAPL", 100),
            ("placed", "1"),
        ),
        data={"id": "1", "symbol": "AAPL", "qty": 100, "event": "placed", "order_id": "1"},
    )

    e1 = await asyncio.wait_for(q_orders.get(), timeout=1.0)
    assert e1.table == "orders"

    e2 = await asyncio.wait_for(q_audit.get(), timeout=1.0)
    assert e2.table == "audit_log"


async def test_graceful_drain(db, bus):
    """Submit writes, then stop(drain=True). All should commit."""
    w = WriteBatcher(db=db, change_bus=bus, batch_max_size=100, batch_max_wait_ms=50.0)
    await w.start()

    # Use create_task so submits are enqueued before stop is called
    tasks = []
    for i in range(50):
        t = asyncio.create_task(w.submit(
            ops=(INSERT_ORDER,),
            params_list=((str(i), f"SYM{i}", i),),
            data={"id": str(i), "symbol": f"SYM{i}", "qty": i},
        ))
        tasks.append(t)

    # Give tasks a moment to enqueue
    await asyncio.sleep(0.01)

    await w.stop(drain=True)

    results = await asyncio.gather(*tasks)
    assert all(r["ok"] for r in results)

    rows = await db.read("SELECT COUNT(*) as cnt FROM orders")
    assert rows[0]["cnt"] == 50


async def test_reject_after_stop(db, bus):
    """After stop(), submit() should raise."""
    w = WriteBatcher(db=db, change_bus=bus, batch_max_size=100, batch_max_wait_ms=1.0)
    await w.start()
    await w.stop(drain=False)

    with pytest.raises(RuntimeError, match="stopping"):
        await w.submit(
            ops=(INSERT_ORDER,),
            params_list=(("1", "AAPL", 100),),
            data={"id": "1"},
        )


async def test_cross_op_bind(writer, db):
    """Bound params should be resolved from a prior op's returned row."""
    # orders uses TEXT PK, audit_log has AUTOINCREMENT id
    # Insert order, then insert audit with order_id bound to orders.id
    INSERT_ORDER_AUTO = CompiledOp(
        table="orders",
        op_type="insert",
        sql="INSERT INTO orders (id, symbol, qty) VALUES (?, ?, ?) RETURNING *",
        param_names=("id", "symbol", "qty"),
    )
    INSERT_AUDIT_BOUND = CompiledOp(
        table="audit_log",
        op_type="insert",
        sql="INSERT INTO audit_log (event, order_id) VALUES (?, ?) RETURNING *",
        param_names=("event", "order_id"),
        bind={"order_id": (0, "id")},  # order_id comes from op 0's "id" field
    )

    result = await writer.submit(
        ops=(INSERT_ORDER_AUTO, INSERT_AUDIT_BOUND),
        params_list=(
            ("ORD1", "AAPL", 100),
            ("order_placed", None),  # order_id is None placeholder, resolved by bind
        ),
        data={"id": "ORD1", "symbol": "AAPL", "qty": 100, "event": "order_placed"},
    )
    assert result["ok"] is True

    audits = await db.read("SELECT * FROM audit_log")
    assert len(audits) == 1
    assert audits[0]["order_id"] == "ORD1"
    assert audits[0]["event"] == "order_placed"


async def test_cross_op_bind_autoincrement(writer, db):
    """Bound params should capture autoincrement IDs from prior ops."""
    # Use a table with autoincrement to verify the generated ID flows through
    INSERT_AUDIT_FIRST = CompiledOp(
        table="audit_log",
        op_type="insert",
        sql="INSERT INTO audit_log (event) VALUES (?) RETURNING *",
        param_names=("event",),
    )
    INSERT_AUDIT_REF = CompiledOp(
        table="audit_log",
        op_type="insert",
        sql="INSERT INTO audit_log (event, order_id) VALUES (?, ?) RETURNING *",
        param_names=("event", "order_id"),
        bind={"order_id": (0, "id")},  # bind to autoincrement id of first insert
    )

    result = await writer.submit(
        ops=(INSERT_AUDIT_FIRST, INSERT_AUDIT_REF),
        params_list=(
            ("first_event",),
            ("second_event", None),
        ),
        data={"event": "first_event"},
    )
    assert result["ok"] is True

    audits = await db.read("SELECT * FROM audit_log ORDER BY id")
    assert len(audits) == 2
    # Second audit's order_id should be the first audit's autoincrement id
    # order_id is TEXT column so SQLite stores it as string
    assert str(audits[1]["order_id"]) == str(audits[0]["id"])


async def test_change_event_includes_db_defaults(writer, bus):
    """ChangeEvent row should include DB-generated defaults and autoincrement IDs."""
    q = bus.subscribe(["audit_log"])

    await writer.submit(
        ops=(INSERT_AUDIT,),
        params_list=(("test_event", "order_1"),),
        data={"event": "test_event", "order_id": "order_1"},
    )

    event = await asyncio.wait_for(q.get(), timeout=1.0)
    assert event.row["event"] == "test_event"
    assert event.row["order_id"] == "order_1"
    # Autoincrement ID should be present from RETURNING *
    assert "id" in event.row
    assert isinstance(event.row["id"], int)
    assert event.row["id"] > 0
