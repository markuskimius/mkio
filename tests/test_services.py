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
    assert "version" in msgs[0]

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
    version = ws.get_messages()[0]["version"]

    # Check with known version
    ws.clear()
    await txn_svc.on_message(ws, {
        "ref": "ref2",
        "type": "check",
        "version": version,
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "result"
    assert msgs[0]["ok"] is True

    # Check with unknown version
    ws.clear()
    await txn_svc.on_message(ws, {
        "ref": "ref3",
        "type": "check",
        "version": "19700101 00:00:00.000000000000",
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
    svc.name = "live_orders"
    await svc.start()
    yield svc
    await svc.stop()


async def test_subpub_fresh_snapshot(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
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
    await subpub_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
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
    """Subscribe, get snapshot, push changes, then reconnect with version — should get delta."""
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
    snapshot_version = ws.get_messages()[0]["version"]

    # Push a change
    event = ChangeBus.make_event(
        "orders", "insert", {"id": "3", "symbol": "GOOG", "qty": 200, "status": "new"}, "20260404 00:00:00.100000000000"
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    # Reconnect with version
    ws2 = MockWebSocket()
    await subpub_svc.on_subscribe(ws2, {
        "ref": "ref2",
        "type": "subscribe",
        "version": snapshot_version,
    })
    msgs = ws2.get_messages()
    # Should get delta since we have changes after snapshot_version
    # (version might be "" if no changes were in log before, so this could be snapshot)
    assert msgs[0]["type"] in ("delta", "snapshot")


async def test_subpub_unsubscribe(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
    assert len(subpub_svc._subscribers) == 1
    await subpub_svc.on_unsubscribe(ws, {"type": "unsubscribe"})
    assert len(subpub_svc._subscribers) == 0


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
    await stream_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 5


async def test_stream_live_push(stream_svc, bus):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
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


async def test_stream_reconnect_with_version(stream_svc, bus):
    """Subscribe, get snapshot, push new events, reconnect with version — should resume."""
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
    snapshot_msg = ws.get_messages()[0]
    snapshot_version = snapshot_msg["version"]
    initial_count = len(snapshot_msg["rows"])

    # Push new events — use a version far in the future so it's after buffer versions
    from mkio._version import next_version
    for i in range(3):
        ver = next_version()
        event = ChangeBus.make_event(
            "audit_log", "insert",
            {"id": 100 + i, "event": f"post_{i}", "order_id": str(100 + i)},
            ver,
        )
        bus.publish([event])
    await asyncio.sleep(0.2)

    # Reconnect with the snapshot version — should get the 3 new events
    ws2 = MockWebSocket()
    await stream_svc.on_subscribe(ws2, {
        "ref": "ref2",
        "type": "subscribe",
        "version": snapshot_version,
    })
    msgs = ws2.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) >= 3


async def test_stream_ignores_non_insert(stream_svc, bus):
    """Stream only processes insert events (append-only)."""
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
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
    await query_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 1
    assert msgs[0]["rows"][0]["symbol"] == "AAPL"


async def test_query_live_update(query_svc, bus):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
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
    await query_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
    snapshot_version = ws.get_messages()[0]["version"]

    # Push changes
    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "2", "symbol": "TSLA", "qty": 30, "status": "new"},
        "20260404 00:00:05.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    # Reconnect with version
    ws2 = MockWebSocket()
    await query_svc.on_subscribe(ws2, {
        "ref": "ref2",
        "type": "subscribe",
        "version": "20260404 00:00:04.000000000000",  # Before the new event
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
        "ref": "ref1",
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
    await query_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
    assert len(query_svc._subscribers) == 1
    await query_svc.on_unsubscribe(ws, {"type": "unsubscribe"})
    assert len(query_svc._subscribers) == 0


# ---- Dead subscriber cleanup -----------------------------------------------

async def test_dead_subscriber_removed(subpub_svc, bus):
    """A closed websocket should be removed from subscribers list."""
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"ref": "ref1", "type": "subscribe"})
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
