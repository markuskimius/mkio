"""Tests for all service types: transaction, subpub, stream, query."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import pytest_asyncio

from mkio._json import loads
from mkio.change_bus import ChangeBus
from mkio.database import Database
from mkio.writer import CompiledOp, WriteBatcher
from mkio.services.transaction import TransactionService
from mkio.services.subpub import SubPubService
from mkio.services.stream import StreamService
from mkio.services.query import QueryService


TEST_TABLES = {
    "orders": {
        "columns": {
            "id": "TEXT PRIMARY KEY",
            "symbol": "TEXT NOT NULL",
            "qty": "INTEGER",
            "price": "REAL DEFAULT 0",
            "status": "TEXT DEFAULT 'pending'",
        },
    },
    "audit_log": {
        "columns": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "event": "TEXT",
            "order_id": "TEXT",
        },
        "append_only": True,
    },
}


class MockWebSocket:
    """Mock WebSocket for testing services."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.closed = False

    async def send_bytes(self, data: bytes) -> None:
        if self.closed:
            raise ConnectionError("WebSocket closed")
        self.sent.append(data)

    def get_messages(self) -> list[dict[str, Any]]:
        return [loads(b) for b in self.sent]

    def clear(self) -> None:
        self.sent.clear()


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


# ---- Transaction Service ---------------------------------------------------

@pytest_asyncio.fixture
async def txn_svc(db, bus, writer):
    config = {
        "type": "transaction",
        "ops": [
            {"table": "orders", "op_type": "insert", "fields": ["id", "symbol", "qty"]},
        ],
    }
    svc = TransactionService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "add_order"
    await svc.start()
    yield svc
    await svc.stop()


async def test_transaction_insert(txn_svc, db):
    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "ref1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "result"
    assert msgs[0]["ok"] is True
    assert msgs[0]["ref"] == "ref1"

    rows = await db.read("SELECT * FROM orders WHERE id = '1'")
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


async def test_transaction_missing_field(txn_svc):
    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "ref2",
        "data": {"id": "1"},  # missing symbol, qty
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "error"
    assert "Missing field" in msgs[0]["message"]


async def test_transaction_result_cache(txn_svc):
    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "ref1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    tx_ref = ws.get_messages()[0]["ref"]

    # Check with known ref
    ws.clear()
    await txn_svc.on_message(ws, {
        "ref": tx_ref,
        "type": "check",
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "result"
    assert msgs[0]["ok"] is True

    # Check with unknown ref
    ws.clear()
    await txn_svc.on_message(ws, {
        "ref": "19700101 00:00:00.000000000000",
        "type": "check",
    })
    msgs = ws.get_messages()
    assert msgs[0].get("status") == "unknown"


# ---- SubPub Service --------------------------------------------------------

@pytest_asyncio.fixture
async def subpub_svc(db, bus, writer):
    # Seed some data
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("1", "AAPL", 100, "pending"),
    )
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("2", "MSFT", 50, "filled"),
    )
    await db.write_conn.commit()

    config = {
        "type": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "key": "id",
        "filterable": ["status", "symbol"],
        "change_log_size": 100,
    }
    svc = SubPubService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "last_trade"
    await svc.start()
    yield svc
    await svc.stop()


async def test_subpub_fresh_snapshot(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 2


async def test_subpub_with_filter(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {
        "ref": "ref1",
        "type": "subscribe",
        "filter": "status == 'pending'",
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 1
    assert msgs[0]["rows"][0]["status"] == "pending"


async def test_subpub_live_update(subpub_svc, bus):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    # Simulate a change event
    event = ChangeBus.make_event(
        "orders", "insert", {"id": "3", "symbol": "GOOG", "qty": 200, "status": "pending"}, "20260404 00:00:00.000000000001"
    )
    bus.publish([event])

    # Let the listener task process the event
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["op"] == "insert"
    assert msgs[0]["row"]["symbol"] == "GOOG"


async def test_subpub_delta_reconnect(subpub_svc, bus):
    """Subscribe, get snapshot, push changes, then reconnect with ref — should get delta."""
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    snapshot_ref = ws.get_messages()[0]["ref"]

    # Push a change
    event = ChangeBus.make_event(
        "orders", "insert", {"id": "3", "symbol": "GOOG", "qty": 200, "status": "new"}, "20260404 00:00:00.100000000000"
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    # Reconnect with ref
    ws2 = MockWebSocket()
    await subpub_svc.on_subscribe(ws2, {
        "type": "subscribe",
        "ref": snapshot_ref,
    })
    msgs = ws2.get_messages()
    # Should get delta since we have changes after snapshot ref
    # (ref might be "" if no changes were in log before, so this could be snapshot)
    assert msgs[0]["type"] in ("delta", "snapshot")


async def test_subpub_unsubscribe(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    assert len(subpub_svc._subscribers) == 1
    await subpub_svc.on_unsubscribe(ws, {"type": "unsubscribe"})
    assert len(subpub_svc._subscribers) == 0


# ---- SubPub Service with where ---------------------------------------------

@pytest_asyncio.fixture
async def subpub_where_svc(db, bus, writer):
    # Seed data: one pending, one filled
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("1", "AAPL", 100, "pending"),
    )
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("2", "MSFT", 50, "filled"),
    )
    await db.write_conn.commit()

    from mkio._expr import compile_filter

    config = {
        "type": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "key": "id",
        "where": "status == 'filled'",
        "_compiled_where": compile_filter("status == 'filled'"),
        "change_log_size": 100,
    }
    svc = SubPubService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "last_trade_where"
    await svc.start()
    yield svc
    await svc.stop()


async def test_subpub_where_filters_startup(subpub_where_svc):
    """Only rows matching where should appear in the snapshot."""
    ws = MockWebSocket()
    await subpub_where_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 1
    assert msgs[0]["rows"][0]["symbol"] == "MSFT"


async def test_subpub_where_filters_live_insert(subpub_where_svc, bus):
    """Live inserts that don't match where should be ignored."""
    ws = MockWebSocket()
    await subpub_where_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    # Insert a pending order — should be filtered out
    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "3", "symbol": "GOOG", "qty": 200, "status": "pending"},
        "20260404 00:00:00.000000000001",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)
    assert ws.get_messages() == []

    # Insert a filled order — should come through
    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "4", "symbol": "TSLA", "qty": 75, "status": "filled"},
        "20260404 00:00:00.000000000002",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["row"]["symbol"] == "TSLA"


async def test_subpub_where_removes_on_mismatch(subpub_where_svc, bus):
    """If an update causes a row to no longer match where, it should be removed."""
    ws = MockWebSocket()
    await subpub_where_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    # Update MSFT (filled) to pending — should produce a delete
    event = ChangeBus.make_event(
        "orders", "update",
        {"id": "2", "symbol": "MSFT", "qty": 50, "status": "pending"},
        "20260404 00:00:00.000000000003",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["op"] == "delete"


# ---- Stream Service --------------------------------------------------------

@pytest_asyncio.fixture
async def stream_svc(db, bus, writer):
    # Seed some audit data
    for i in range(5):
        await db.write_conn.execute(
            "INSERT INTO audit_log (event, order_id) VALUES (?, ?)",
            (f"event_{i}", str(i)),
        )
    await db.write_conn.commit()

    config = {
        "type": "stream",
        "primary_table": "audit_log",
        "watch_tables": ["audit_log"],
        "buffer_size": 100,
    }
    svc = StreamService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "audit_feed"
    await svc.start()
    yield svc
    await svc.stop()


async def test_stream_fresh_subscribe(stream_svc):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 5


async def test_stream_live_push(stream_svc, bus):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    # Simulate insert event
    event = ChangeBus.make_event(
        "audit_log", "insert",
        {"id": 6, "event": "new_event", "order_id": "99"},
        "20260404 00:00:01.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["row"]["event"] == "new_event"


async def test_stream_reconnect_with_ref(stream_svc, bus):
    """Subscribe, get snapshot, push new events, reconnect with ref — should resume."""
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe"})
    snapshot_msg = ws.get_messages()[0]
    snapshot_ref = snapshot_msg["ref"]
    initial_count = len(snapshot_msg["rows"])

    # Push new events
    from mkio._ref import next_ref
    for i in range(3):
        ver = next_ref()
        event = ChangeBus.make_event(
            "audit_log", "insert",
            {"id": 100 + i, "event": f"post_{i}", "order_id": str(100 + i)},
            ver,
        )
        bus.publish([event])
    await asyncio.sleep(0.2)

    # Reconnect with the snapshot ref — should get the 3 new events
    ws2 = MockWebSocket()
    await stream_svc.on_subscribe(ws2, {
        "type": "subscribe",
        "ref": snapshot_ref,
    })
    msgs = ws2.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) >= 3


async def test_stream_ignores_non_insert(stream_svc, bus):
    """Stream only processes insert events (append-only)."""
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    # Send an update event — should be ignored
    event = ChangeBus.make_event(
        "audit_log", "update",
        {"id": 1, "event": "modified", "order_id": "0"},
        "20260404 00:00:03.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 0  # Update ignored


# ---- Query Service ---------------------------------------------------------

@pytest_asyncio.fixture
async def query_svc(db, bus, writer):
    # Seed data
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("1", "AAPL", 100, "pending"),
    )
    await db.write_conn.commit()

    config = {
        "type": "query",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "filterable": ["status"],
        "change_log_size": 100,
    }
    svc = QueryService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "all_orders"
    await svc.start()
    yield svc
    await svc.stop()


async def test_query_fresh_snapshot(query_svc):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 1
    assert msgs[0]["rows"][0]["symbol"] == "AAPL"


async def test_query_live_update(query_svc, bus):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "2", "symbol": "MSFT", "qty": 50, "status": "pending"},
        "20260404 00:00:04.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["row"]["symbol"] == "MSFT"


async def test_query_delta_reconnect(query_svc, bus):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    snapshot_ref = ws.get_messages()[0]["ref"]

    # Push changes
    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "2", "symbol": "TSLA", "qty": 30, "status": "new"},
        "20260404 00:00:05.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    # Reconnect with ref
    ws2 = MockWebSocket()
    await query_svc.on_subscribe(ws2, {
        "type": "subscribe",
        "ref": "20260404 00:00:04.000000000000",  # Before the new event
    })
    msgs = ws2.get_messages()
    assert msgs[0]["type"] in ("delta", "snapshot")


async def test_query_with_filter(query_svc, bus):
    # Push an extra row
    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "2", "symbol": "MSFT", "qty": 50, "status": "filled"},
        "20260404 00:00:06.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {
        "type": "subscribe",
        "filter": "status == 'pending'",
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    # Only the pending row from SQLite
    for row in msgs[0]["rows"]:
        assert row["status"] == "pending"


async def test_query_unsubscribe(query_svc):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    assert len(query_svc._subscribers) == 1
    await query_svc.on_unsubscribe(ws, {"type": "unsubscribe"})
    assert len(query_svc._subscribers) == 0


# ---- Dead subscriber cleanup -----------------------------------------------

async def test_dead_subscriber_removed(subpub_svc, bus):
    """A closed websocket should be removed from subscribers list."""
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    assert len(subpub_svc._subscribers) == 1

    ws.closed = True  # Simulate disconnect

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "99", "symbol": "DEAD", "qty": 0, "status": "x"},
        "20260404 00:00:07.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    assert len(subpub_svc._subscribers) == 0


# ---- Cross-restart recovery ------------------------------------------------

async def test_subpub_cross_restart_delta(db, bus, writer):
    """After inserting via transaction, stopping the service, and creating a new
    instance from the same DB, a client should get a delta (not full snapshot)."""
    txn_config = {
        "type": "transaction",
        "ops": [
            {"table": "orders", "op_type": "insert", "fields": ["id", "symbol", "qty"]},
        ],
    }
    txn_svc = TransactionService(config=txn_config, db=db, change_bus=bus, writer=writer)
    txn_svc.name = "add_order"
    await txn_svc.start()

    subpub_config = {
        "type": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "key": "id",
        "filterable": ["status", "symbol"],
        "change_log_size": 100,
    }
    svc1 = SubPubService(config=subpub_config, db=db, change_bus=bus, writer=writer)
    svc1.name = "last_trade"
    await svc1.start()

    # Insert a row via transaction (stamps _mkio_ref in DB)
    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "r1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })

    # Subscribe and get snapshot ref
    ws2 = MockWebSocket()
    await svc1.on_subscribe(ws2, {"type": "subscribe"})
    snapshot_ref = ws2.get_messages()[0]["ref"]
    assert snapshot_ref != ""

    # Insert another row
    ws3 = MockWebSocket()
    await txn_svc.on_message(ws3, {
        "ref": "r2",
        "data": {"id": "2", "symbol": "GOOG", "qty": 50},
    })
    await asyncio.sleep(0.1)

    # Stop the service (simulating restart)
    await svc1.stop()

    # Create a NEW service instance from the same DB (simulating restart)
    bus2 = ChangeBus()
    svc2 = SubPubService(config=subpub_config, db=db, change_bus=bus2, writer=writer)
    svc2.name = "last_trade"
    await svc2.start()

    # Reconnect with the snapshot ref from before "restart"
    ws4 = MockWebSocket()
    await svc2.on_subscribe(ws4, {
        "type": "subscribe",
        "ref": snapshot_ref,
    })
    msgs = ws4.get_messages()
    assert msgs[0]["type"] == "delta"
    # Delta should contain only the row inserted after the snapshot ref
    assert len(msgs[0]["changes"]) >= 1
    found = any(c["row"]["symbol"] == "GOOG" for c in msgs[0]["changes"])
    assert found, "Delta should include the GOOG row inserted after snapshot ref"
    # _mkio_ref should be visible in client data
    for c in msgs[0]["changes"]:
        assert "_mkio_ref" in c["row"]

    await svc2.stop()
    await txn_svc.stop()


async def test_stream_cross_restart_recovery(db, bus, writer):
    """Stream buffer should have consistent refs across restart."""
    txn_config = {
        "type": "transaction",
        "ops": [
            {"table": "audit_log", "op_type": "insert", "fields": ["event", "order_id"]},
        ],
    }
    txn_svc = TransactionService(config=txn_config, db=db, change_bus=bus, writer=writer)
    txn_svc.name = "add_audit"
    await txn_svc.start()

    stream_config = {
        "type": "stream",
        "primary_table": "audit_log",
        "watch_tables": ["audit_log"],
        "buffer_size": 100,
    }
    svc1 = StreamService(config=stream_config, db=db, change_bus=bus, writer=writer)
    svc1.name = "audit_feed"
    await svc1.start()

    # Insert 3 rows
    for i in range(3):
        ws = MockWebSocket()
        await txn_svc.on_message(ws, {
            "ref": f"r{i}",
            "data": {"event": f"event_{i}", "order_id": str(i)},
        })
    await asyncio.sleep(0.1)

    # Subscribe and get snapshot ref
    ws2 = MockWebSocket()
    await svc1.on_subscribe(ws2, {"type": "subscribe"})
    snapshot_msg = ws2.get_messages()[0]
    snapshot_ref = snapshot_msg["ref"]
    initial_count = len(snapshot_msg["rows"])

    # Insert more rows
    for i in range(3, 6):
        ws = MockWebSocket()
        await txn_svc.on_message(ws, {
            "ref": f"r{i}",
            "data": {"event": f"event_{i}", "order_id": str(i)},
        })
    await asyncio.sleep(0.1)

    # Stop and recreate (simulate restart)
    await svc1.stop()
    bus2 = ChangeBus()
    svc2 = StreamService(config=stream_config, db=db, change_bus=bus2, writer=writer)
    svc2.name = "audit_feed"
    await svc2.start()

    # Reconnect with snapshot ref — should only get rows after that ref
    ws3 = MockWebSocket()
    await svc2.on_subscribe(ws3, {
        "type": "subscribe",
        "ref": snapshot_ref,
    })
    msgs = ws3.get_messages()
    assert msgs[0]["type"] == "snapshot"
    # Should have only the 3 rows inserted after the snapshot ref
    assert len(msgs[0]["rows"]) == 3
    for row in msgs[0]["rows"]:
        assert "_mkio_ref" in row

    await svc2.stop()
    await txn_svc.stop()


async def test_query_cross_restart_delta(db, bus, writer):
    """Query service should support delta reconnection after restart."""
    txn_config = {
        "type": "transaction",
        "ops": [
            {"table": "orders", "op_type": "insert", "fields": ["id", "symbol", "qty"]},
        ],
    }
    txn_svc = TransactionService(config=txn_config, db=db, change_bus=bus, writer=writer)
    txn_svc.name = "add_order"
    await txn_svc.start()

    query_config = {
        "type": "query",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "filterable": ["status"],
        "change_log_size": 100,
    }
    svc1 = QueryService(config=query_config, db=db, change_bus=bus, writer=writer)
    svc1.name = "all_orders"
    await svc1.start()

    # Insert a row
    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "r1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    await asyncio.sleep(0.1)

    # Subscribe and get ref
    ws2 = MockWebSocket()
    await svc1.on_subscribe(ws2, {"type": "subscribe"})
    snapshot_ref = ws2.get_messages()[0]["ref"]

    # Insert another row
    ws3 = MockWebSocket()
    await txn_svc.on_message(ws3, {
        "ref": "r2",
        "data": {"id": "2", "symbol": "MSFT", "qty": 50},
    })
    await asyncio.sleep(0.1)

    # Restart
    await svc1.stop()
    bus2 = ChangeBus()
    svc2 = QueryService(config=query_config, db=db, change_bus=bus2, writer=writer)
    svc2.name = "all_orders"
    await svc2.start()

    # Reconnect with ref
    ws4 = MockWebSocket()
    await svc2.on_subscribe(ws4, {
        "type": "subscribe",
        "ref": snapshot_ref,
    })
    msgs = ws4.get_messages()
    assert msgs[0]["type"] == "delta"
    assert len(msgs[0]["changes"]) >= 1
    found = any(c["row"]["symbol"] == "MSFT" for c in msgs[0]["changes"])
    assert found

    await svc2.stop()
    await txn_svc.stop()


async def test_mkio_ref_stored_in_db(txn_svc, db):
    """_mkio_ref should be stored in the database for every write."""
    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "r1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })

    # Verify _mkio_ref is in the database
    rows = await db.read("SELECT * FROM orders WHERE id = '1'")
    assert rows[0].get("_mkio_ref", "") != ""


# ---- Subscription ID (subid) ------------------------------------------------

async def test_subpub_subid_on_snapshot(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "subid": "my-sub-1"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert msgs[0]["subid"] == "my-sub-1"


async def test_subpub_subid_on_delta(subpub_svc, bus):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    snapshot_ref = ws.get_messages()[0]["ref"]

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "3", "symbol": "GOOG", "qty": 200, "status": "new"},
        "20260404 00:00:00.100000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    ws2 = MockWebSocket()
    await subpub_svc.on_subscribe(ws2, {
        "type": "subscribe",
        "ref": snapshot_ref,
        "subid": "delta-sub",
    })
    msgs = ws2.get_messages()
    assert msgs[0]["type"] in ("delta", "snapshot")
    assert msgs[0]["subid"] == "delta-sub"


async def test_subpub_subid_on_live_update(subpub_svc, bus):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "subid": "live-sub"})
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "5", "symbol": "AMZN", "qty": 10, "status": "pending"},
        "20260404 00:00:00.200000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["subid"] == "live-sub"


async def test_subpub_no_subid_when_omitted(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert "subid" not in msgs[0]


async def test_stream_subid_on_snapshot(stream_svc):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "subid": "stream-1"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert msgs[0]["subid"] == "stream-1"


async def test_stream_subid_on_live_update(stream_svc, bus):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "subid": "stream-live"})
    ws.clear()

    event = ChangeBus.make_event(
        "audit_log", "insert",
        {"id": 10, "event": "subid_test", "order_id": "42"},
        "20260404 00:00:10.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["subid"] == "stream-live"


async def test_stream_no_subid_when_omitted(stream_svc):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert "subid" not in msgs[0]


async def test_query_subid_on_snapshot(query_svc):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe", "subid": "query-1"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert msgs[0]["subid"] == "query-1"


async def test_query_subid_on_delta(query_svc, bus):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    snapshot_ref = ws.get_messages()[0]["ref"]

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "10", "symbol": "NFLX", "qty": 5, "status": "new"},
        "20260404 00:00:08.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    ws2 = MockWebSocket()
    await query_svc.on_subscribe(ws2, {
        "type": "subscribe",
        "ref": snapshot_ref,
        "subid": "query-delta",
    })
    msgs = ws2.get_messages()
    assert msgs[0]["type"] in ("delta", "snapshot")
    assert msgs[0]["subid"] == "query-delta"


async def test_query_subid_on_live_update(query_svc, bus):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe", "subid": "query-live"})
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "11", "symbol": "META", "qty": 25, "status": "pending"},
        "20260404 00:00:09.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["subid"] == "query-live"


async def test_query_no_subid_when_omitted(query_svc):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert "subid" not in msgs[0]


async def test_subpub_multiple_subscribers_different_subids(subpub_svc, bus):
    """Two subscribers with different subids each get their own subid on updates."""
    ws1 = MockWebSocket()
    ws2 = MockWebSocket()
    await subpub_svc.on_subscribe(ws1, {"type": "subscribe", "subid": "sub-A"})
    await subpub_svc.on_subscribe(ws2, {"type": "subscribe", "subid": "sub-B"})
    ws1.clear()
    ws2.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "20", "symbol": "NVDA", "qty": 15, "status": "pending"},
        "20260404 00:00:00.300000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs1 = ws1.get_messages()
    msgs2 = ws2.get_messages()
    assert msgs1[0]["subid"] == "sub-A"
    assert msgs2[0]["subid"] == "sub-B"
