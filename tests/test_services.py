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
        "protocol": "transaction",
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
    assert "Missing required field" in msgs[0]["message"]


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
        "protocol": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "topic": "id",
        "change_log_size": 100,
    }
    svc = SubPubService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "last_trade"
    await svc.start()
    yield svc
    await svc.stop()


async def test_subpub_topic_exists(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert "ref" not in msgs[0]
    assert len(msgs[0]["rows"]) == 1
    row = msgs[0]["rows"][0]
    assert row["id"] == "1"
    assert row["symbol"] == "AAPL"
    assert row["_mkio_exists"] is True
    assert "_mkio_ref" in row
    assert row["_mkio_topic"] == "1"


async def test_subpub_topic_not_found(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "999"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 1
    row = msgs[0]["rows"][0]
    assert row["_mkio_topic"] == "999"
    assert row["_mkio_exists"] is False
    assert row["_mkio_ref"] is None
    # All fields present with null defaults
    assert row["symbol"] is None
    assert row["qty"] is None
    assert row["status"] is None


async def test_subpub_requires_topic(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "error"
    assert "topic" in msgs[0]["message"].lower()
    assert len(subpub_svc._subscribers) == 0


async def test_subpub_live_update(subpub_svc, bus):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "3"})
    ws.clear()

    # Insert a row matching the topic
    event = ChangeBus.make_event(
        "orders", "insert", {"id": "3", "symbol": "GOOG", "qty": 200, "status": "pending"}, "20260404 00:00:00.000000000001"
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["op"] == "update"
    assert msgs[0]["row"]["symbol"] == "GOOG"
    assert msgs[0]["row"]["_mkio_exists"] is True
    assert msgs[0]["row"]["_mkio_ref"] == "20260404 00:00:00.000000000001"
    assert msgs[0]["row"]["_mkio_topic"] == "3"


async def test_subpub_live_update_only_matching_topic(subpub_svc, bus):
    """Subscribers only get updates for their own topic."""
    ws1 = MockWebSocket()
    ws2 = MockWebSocket()
    await subpub_svc.on_subscribe(ws1, {"type": "subscribe", "topic": "1"})
    await subpub_svc.on_subscribe(ws2, {"type": "subscribe", "topic": "2"})
    ws1.clear()
    ws2.clear()

    # Update only topic "1"
    event = ChangeBus.make_event(
        "orders", "update", {"id": "1", "symbol": "AAPL", "qty": 150, "status": "pending"}, "20260404 00:00:00.000000000001"
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    assert len(ws1.get_messages()) == 1
    assert len(ws2.get_messages()) == 0


async def test_subpub_topic_appears_as_update(subpub_svc, bus):
    """When a topic goes from not-found to found, it comes as an update (not insert)."""
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "new"})
    msgs = ws.get_messages()
    assert msgs[0]["rows"][0]["_mkio_exists"] is False
    ws.clear()

    # Now insert the row
    event = ChangeBus.make_event(
        "orders", "insert", {"id": "new", "symbol": "TSLA", "qty": 10, "status": "new"}, "20260404 00:00:00.000000000002"
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["op"] == "update"
    assert msgs[0]["row"]["_mkio_exists"] is True
    assert msgs[0]["row"]["symbol"] == "TSLA"


async def test_subpub_topic_deleted(subpub_svc, bus):
    """When a topic's row is deleted, subscriber gets _mkio_exists: False with all fields."""
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1"})
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "delete", {"id": "1"}, "20260404 00:00:00.000000000003"
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["op"] == "update"
    assert msgs[0]["row"]["_mkio_exists"] is False
    assert msgs[0]["row"]["_mkio_topic"] == "1"
    assert msgs[0]["row"]["_mkio_ref"] is None
    assert msgs[0]["row"]["symbol"] is None


async def test_subpub_unsubscribe(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1"})
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
        "protocol": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "topic": "id",
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
    """Only rows matching where should appear as existing topics."""
    ws = MockWebSocket()
    # Topic "2" (MSFT, filled) should exist
    await subpub_where_svc.on_subscribe(ws, {"type": "subscribe", "topic": "2"})
    msgs = ws.get_messages()
    assert msgs[0]["rows"][0]["_mkio_exists"] is True
    assert msgs[0]["rows"][0]["symbol"] == "MSFT"

    ws2 = MockWebSocket()
    # Topic "1" (AAPL, pending) should not exist (filtered by where)
    await subpub_where_svc.on_subscribe(ws2, {"type": "subscribe", "topic": "1"})
    msgs2 = ws2.get_messages()
    assert msgs2[0]["rows"][0]["_mkio_exists"] is False


async def test_subpub_where_filters_live_insert(subpub_where_svc, bus):
    """Live inserts that don't match where should not notify topic subscribers."""
    ws = MockWebSocket()
    await subpub_where_svc.on_subscribe(ws, {"type": "subscribe", "topic": "3"})
    ws.clear()

    # Insert a pending order for topic "3" — doesn't match where
    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "3", "symbol": "GOOG", "qty": 200, "status": "pending"},
        "20260404 00:00:00.000000000001",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)
    assert ws.get_messages() == []

    # Now insert a filled order for topic "4" — no subscriber for this topic
    ws2 = MockWebSocket()
    await subpub_where_svc.on_subscribe(ws2, {"type": "subscribe", "topic": "4"})
    ws2.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "4", "symbol": "TSLA", "qty": 75, "status": "filled"},
        "20260404 00:00:00.000000000002",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)
    msgs = ws2.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["row"]["_mkio_exists"] is True
    assert msgs[0]["row"]["symbol"] == "TSLA"


async def test_subpub_where_removes_on_mismatch(subpub_where_svc, bus):
    """If an update causes a row to no longer match where, subscriber gets _mkio_exists: False."""
    ws = MockWebSocket()
    await subpub_where_svc.on_subscribe(ws, {"type": "subscribe", "topic": "2"})
    ws.clear()

    # Update MSFT (filled) to pending — should notify exists=False
    event = ChangeBus.make_event(
        "orders", "update",
        {"id": "2", "symbol": "MSFT", "qty": 50, "status": "pending"},
        "20260404 00:00:00.000000000003",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["op"] == "update"
    assert msgs[0]["row"]["_mkio_exists"] is False


# ---- SubPub with configured defaults ----------------------------------------

@pytest_asyncio.fixture
async def subpub_defaults_svc(db, bus, writer):
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("1", "AAPL", 100, "pending"),
    )
    await db.write_conn.commit()

    config = {
        "protocol": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "topic": "id",
        "defaults": {"qty": "0", "status": "'unknown'"},
        "change_log_size": 100,
    }
    svc = SubPubService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "with_defaults"
    await svc.start()
    yield svc
    await svc.stop()


async def test_subpub_defaults_on_not_found(subpub_defaults_svc):
    """Not-found rows use configured defaults instead of null."""
    ws = MockWebSocket()
    await subpub_defaults_svc.on_subscribe(ws, {"type": "subscribe", "topic": "999"})
    msgs = ws.get_messages()
    row = msgs[0]["rows"][0]
    assert row["_mkio_exists"] is False
    assert row["_mkio_topic"] == "999"
    assert row["_mkio_ref"] is None
    assert row["qty"] == 0
    assert row["status"] == "unknown"
    assert row["symbol"] is None  # no default configured → null


async def test_subpub_defaults_not_used_when_found(subpub_defaults_svc):
    """When the topic exists, real values are used, not defaults."""
    ws = MockWebSocket()
    await subpub_defaults_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1"})
    msgs = ws.get_messages()
    row = msgs[0]["rows"][0]
    assert row["_mkio_exists"] is True
    assert row["qty"] == 100
    assert row["status"] == "pending"


async def test_subpub_defaults_on_delete(subpub_defaults_svc, bus):
    """Defaults used when a topic transitions from found to not-found."""
    ws = MockWebSocket()
    await subpub_defaults_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1"})
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "delete", {"id": "1"}, "20260404 00:00:00.000000000001"
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    row = msgs[0]["row"]
    assert row["_mkio_exists"] is False
    assert row["qty"] == 0
    assert row["status"] == "unknown"
    assert row["symbol"] is None


async def test_subpub_mkio_ref_default_on_not_found(db, bus, writer):
    """_mkio_ref defaults to null on not-found, or uses configured default."""
    config = {
        "protocol": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "topic": "id",
        "defaults": {"_mkio_ref": "'no-ref'"},
    }
    svc = SubPubService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "ref_default"
    await svc.start()

    ws = MockWebSocket()
    await svc.on_subscribe(ws, {"type": "subscribe", "topic": "999"})
    msgs = ws.get_messages()
    row = msgs[0]["rows"][0]
    assert row["_mkio_exists"] is False
    assert row["_mkio_ref"] == "no-ref"

    await svc.stop()


# ---- SubPub with computed key (sql) ----------------------------------------

@pytest_asyncio.fixture
async def subpub_computed_key_svc(db, bus, writer):
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("1", "AAPL", 100, "pending"),
    )
    await db.write_conn.execute(
        "INSERT INTO orders (id, symbol, qty, status) VALUES (?, ?, ?, ?)",
        ("2", "AAPL", 50, "filled"),
    )
    await db.write_conn.commit()

    config = {
        "protocol": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "topic": "topic_key",
        "sql": "SELECT *, id || ':' || symbol AS topic_key FROM orders",
        "change_log_size": 100,
    }
    svc = SubPubService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "computed_key"
    await svc.start()
    yield svc
    await svc.stop()


async def test_subpub_computed_key_snapshot(subpub_computed_key_svc):
    ws = MockWebSocket()
    await subpub_computed_key_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1:AAPL"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    row = msgs[0]["rows"][0]
    assert row["_mkio_exists"] is True
    assert row["_mkio_topic"] == "1:AAPL"
    assert row["id"] == "1"
    assert row["symbol"] == "AAPL"
    assert row["qty"] == 100


async def test_subpub_computed_key_not_found(subpub_computed_key_svc):
    ws = MockWebSocket()
    await subpub_computed_key_svc.on_subscribe(ws, {"type": "subscribe", "topic": "99:NOPE"})
    msgs = ws.get_messages()
    row = msgs[0]["rows"][0]
    assert row["_mkio_exists"] is False
    assert row["_mkio_topic"] == "99:NOPE"


async def test_subpub_computed_key_live_update(subpub_computed_key_svc, db, bus):
    ws = MockWebSocket()
    await subpub_computed_key_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1:AAPL"})
    ws.clear()

    await db.write_conn.execute(
        "UPDATE orders SET qty = 200, status = 'filled' WHERE id = '1'"
    )
    await db.write_conn.commit()

    event = ChangeBus.make_event(
        "orders", "update",
        {"id": "1", "symbol": "AAPL", "qty": 200, "status": "filled"},
        "20260404 00:00:00.000000000001",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert msgs[0]["row"]["qty"] == 200
    assert msgs[0]["row"]["_mkio_exists"] is True


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
        "protocol": "stream",
        "primary_table": "audit_log",
        "watch_tables": ["audit_log"],
        "buffer_size": 100,
    }
    svc = StreamService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "audit_feed"
    await svc.start()
    yield svc
    await svc.stop()


async def test_stream_requires_ref(stream_svc):
    """Subscribe without ref should be rejected."""
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "error"


async def test_stream_fresh_subscribe(stream_svc):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000"})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 5


async def test_stream_live_push(stream_svc, bus):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000"})
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
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000"})
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
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000"})
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
        "protocol": "query",
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
    assert "ref" not in msgs[0]
    assert len(msgs[0]["rows"]) == 1
    assert msgs[0]["rows"][0]["symbol"] == "AAPL"
    assert "_mkio_ref" in msgs[0]["rows"][0]


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
    assert "ref" not in msgs[0]
    assert msgs[0]["row"]["symbol"] == "MSFT"
    assert "_mkio_ref" in msgs[0]["row"]


async def test_query_snapshot_only(query_svc, bus):
    """snapshot=False skips the snapshot, updates=False skips adding to subscribers."""
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe", "updates": False})
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert len(query_svc._subscribers) == 0

    ws2 = MockWebSocket()
    await query_svc.on_subscribe(ws2, {"type": "subscribe", "snapshot": False})
    msgs2 = ws2.get_messages()
    assert len(msgs2) == 0
    assert len(query_svc._subscribers) == 1


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
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "99"})
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

async def test_subpub_cross_restart_snapshot(db, bus, writer):
    """After restart, reconnecting always gets a full snapshot."""
    txn_config = {
        "protocol": "transaction",
        "ops": [
            {"table": "orders", "op_type": "insert", "fields": ["id", "symbol", "qty"]},
        ],
    }
    txn_svc = TransactionService(config=txn_config, db=db, change_bus=bus, writer=writer)
    txn_svc.name = "add_order"
    await txn_svc.start()

    subpub_config = {
        "protocol": "subpub",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "topic": "id",
    }
    svc1 = SubPubService(config=subpub_config, db=db, change_bus=bus, writer=writer)
    svc1.name = "last_trade"
    await svc1.start()

    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "r1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    ws3 = MockWebSocket()
    await txn_svc.on_message(ws3, {
        "ref": "r2",
        "data": {"id": "2", "symbol": "GOOG", "qty": 50},
    })
    await asyncio.sleep(0.1)

    await svc1.stop()

    bus2 = ChangeBus()
    svc2 = SubPubService(config=subpub_config, db=db, change_bus=bus2, writer=writer)
    svc2.name = "last_trade"
    await svc2.start()

    ws4 = MockWebSocket()
    await svc2.on_subscribe(ws4, {"type": "subscribe", "topic": "1"})
    msgs = ws4.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 1
    assert msgs[0]["rows"][0]["symbol"] == "AAPL"
    assert msgs[0]["rows"][0]["_mkio_exists"] is True
    assert "_mkio_ref" in msgs[0]["rows"][0]

    await svc2.stop()
    await txn_svc.stop()


async def test_stream_cross_restart_recovery(db, bus, writer):
    """Stream buffer should have consistent refs across restart."""
    txn_config = {
        "protocol": "transaction",
        "ops": [
            {"table": "audit_log", "op_type": "insert", "fields": ["event", "order_id"]},
        ],
    }
    txn_svc = TransactionService(config=txn_config, db=db, change_bus=bus, writer=writer)
    txn_svc.name = "add_audit"
    await txn_svc.start()

    stream_config = {
        "protocol": "stream",
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
    await svc1.on_subscribe(ws2, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000"})
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


async def test_query_cross_restart_snapshot(db, bus, writer):
    """Query service always sends full snapshot after restart."""
    txn_config = {
        "protocol": "transaction",
        "ops": [
            {"table": "orders", "op_type": "insert", "fields": ["id", "symbol", "qty"]},
        ],
    }
    txn_svc = TransactionService(config=txn_config, db=db, change_bus=bus, writer=writer)
    txn_svc.name = "add_order"
    await txn_svc.start()

    query_config = {
        "protocol": "query",
        "primary_table": "orders",
        "watch_tables": ["orders"],
        "filterable": ["status"],
    }
    svc1 = QueryService(config=query_config, db=db, change_bus=bus, writer=writer)
    svc1.name = "all_orders"
    await svc1.start()

    ws = MockWebSocket()
    await txn_svc.on_message(ws, {
        "ref": "r1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    ws3 = MockWebSocket()
    await txn_svc.on_message(ws3, {
        "ref": "r2",
        "data": {"id": "2", "symbol": "MSFT", "qty": 50},
    })
    await asyncio.sleep(0.1)

    await svc1.stop()
    bus2 = ChangeBus()
    svc2 = QueryService(config=query_config, db=db, change_bus=bus2, writer=writer)
    svc2.name = "all_orders"
    await svc2.start()

    ws4 = MockWebSocket()
    await svc2.on_subscribe(ws4, {"type": "subscribe"})
    msgs = ws4.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert len(msgs[0]["rows"]) == 2
    symbols = {r["symbol"] for r in msgs[0]["rows"]}
    assert symbols == {"AAPL", "MSFT"}

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
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1", "subid": "my-sub-1"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert msgs[0]["subid"] == "my-sub-1"


async def test_subpub_subid_on_snapshot_and_updates(subpub_svc, bus):
    """subid echoed on snapshot."""
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {
        "type": "subscribe",
        "topic": "1",
        "subid": "both-sub",
    })
    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "snapshot"
    assert msgs[0]["subid"] == "both-sub"
    assert len(subpub_svc._subscribers) == 1
    assert subpub_svc._subscribers[0].subid == "both-sub"


async def test_subpub_subid_on_live_update(subpub_svc, bus):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "5", "subid": "live-sub"})
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
    await subpub_svc.on_subscribe(ws, {"type": "subscribe", "topic": "1"})
    msgs = ws.get_messages()
    assert "subid" not in msgs[0]


async def test_stream_subid_on_snapshot(stream_svc):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000", "subid": "stream-1"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert msgs[0]["subid"] == "stream-1"


async def test_stream_subid_on_live_update(stream_svc, bus):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000", "subid": "stream-live"})
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
    await stream_svc.on_subscribe(ws, {"type": "subscribe", "ref": "00000000 00:00:00.000000000000"})
    msgs = ws.get_messages()
    assert "subid" not in msgs[0]


async def test_query_subid_on_snapshot(query_svc):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe", "subid": "query-1"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    assert msgs[0]["subid"] == "query-1"


async def test_query_subid_on_updates_only(query_svc, bus):
    """subid echoed when subscribing with snapshot=False (updates only)."""
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {
        "type": "subscribe",
        "snapshot": False,
        "subid": "query-updates",
    })
    msgs = ws.get_messages()
    assert len(msgs) == 0
    assert len(query_svc._subscribers) == 1
    assert query_svc._subscribers[0].subid == "query-updates"


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
    """Two subscribers watching the same topic with different subids each get their own subid."""
    ws1 = MockWebSocket()
    ws2 = MockWebSocket()
    await subpub_svc.on_subscribe(ws1, {"type": "subscribe", "topic": "20", "subid": "sub-A"})
    await subpub_svc.on_subscribe(ws2, {"type": "subscribe", "topic": "20", "subid": "sub-B"})
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


# ---- Query _mkio_row --------------------------------------------------------

async def test_query_mkio_row_on_snapshot(query_svc):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    for row in msgs[0]["rows"]:
        assert "_mkio_row" in row
        assert row["_mkio_row"] == row["id"]


async def test_query_mkio_row_on_live_update(query_svc, bus):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "77", "symbol": "AMD", "qty": 10, "status": "new"},
        "20260404 00:01:00.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["row"]["_mkio_row"] == "77"


async def test_query_mkio_row_on_update(query_svc, bus):
    """Live updates include _mkio_row and _mkio_ref."""
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {"type": "subscribe"})
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "78", "symbol": "INTC", "qty": 5, "status": "new"},
        "20260404 00:01:01.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "update"
    assert "_mkio_row" in msgs[0]["row"]
    assert "_mkio_ref" in msgs[0]["row"]
    assert msgs[0]["row"]["_mkio_row"] == msgs[0]["row"]["id"]


async def test_query_mkio_row_composite_pk(db, bus, writer):
    """Composite PK produces a JSON array _mkio_row."""
    await db.write_conn.execute(
        "CREATE TABLE IF NOT EXISTS positions ("
        "exchange TEXT NOT NULL, symbol TEXT NOT NULL, qty INTEGER, "
        "PRIMARY KEY (exchange, symbol))"
    )
    await db.write_conn.execute(
        "ALTER TABLE positions ADD COLUMN _mkio_ref TEXT DEFAULT ''"
    )
    await db.write_conn.execute(
        "INSERT INTO positions (exchange, symbol, qty) VALUES (?, ?, ?)",
        ("NYSE", "AAPL", 100),
    )
    await db.write_conn.commit()

    config = {
        "protocol": "query",
        "primary_table": "positions",
        "watch_tables": ["positions"],
        "change_log_size": 100,
    }
    svc = QueryService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "positions"
    await svc.start()

    ws = MockWebSocket()
    await svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    row = msgs[0]["rows"][0]
    assert row["_mkio_row"] == '["NYSE","AAPL"]'

    await svc.stop()


async def test_query_mkio_row_join_includes_secondary_pk(db, bus, writer):
    """JOIN query includes PK columns from both tables in _mkio_row."""
    await db.write_conn.execute(
        "CREATE TABLE IF NOT EXISTS products ("
        "product_id TEXT PRIMARY KEY, name TEXT, _mkio_ref TEXT DEFAULT '')"
    )
    await db.write_conn.execute(
        "CREATE TABLE IF NOT EXISTS reviews ("
        "review_id INTEGER PRIMARY KEY, product_id TEXT, rating INTEGER, "
        "_mkio_ref TEXT DEFAULT '')"
    )
    await db.write_conn.execute(
        "INSERT INTO products (product_id, name) VALUES ('P1', 'Widget')"
    )
    await db.write_conn.execute(
        "INSERT INTO reviews (review_id, product_id, rating) VALUES (10, 'P1', 5)"
    )
    await db.write_conn.execute(
        "INSERT INTO reviews (review_id, product_id, rating) VALUES (11, 'P1', 3)"
    )
    await db.write_conn.commit()

    config = {
        "protocol": "query",
        "primary_table": "products",
        "watch_tables": ["products", "reviews"],
        "sql": (
            "SELECT products.product_id, products.name, "
            "reviews.review_id, reviews.rating "
            "FROM products JOIN reviews ON products.product_id = reviews.product_id"
        ),
        "change_log_size": 100,
    }
    svc = QueryService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = "product_reviews"
    await svc.start()

    ws = MockWebSocket()
    await svc.on_subscribe(ws, {"type": "subscribe"})
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    rows = msgs[0]["rows"]
    assert len(rows) == 2

    row_ids = {r["_mkio_row"] for r in rows}
    assert len(row_ids) == 2
    assert '["P1",10]' in row_ids
    assert '["P1",11]' in row_ids

    await svc.stop()


# ---- Field projection -------------------------------------------------------

async def test_subpub_fields_snapshot(subpub_svc):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {
        "type": "subscribe",
        "topic": "1",
        "fields": ["symbol", "qty"],
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    row = msgs[0]["rows"][0]
    assert set(row.keys()) == {"symbol", "qty", "_mkio_exists", "_mkio_ref", "_mkio_topic"}
    assert row["_mkio_exists"] is True


async def test_subpub_fields_update(subpub_svc, bus):
    ws = MockWebSocket()
    await subpub_svc.on_subscribe(ws, {
        "type": "subscribe",
        "topic": "99",
        "fields": ["symbol", "status"],
    })
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "99", "symbol": "NVDA", "qty": 10, "status": "new", "price": 800},
        "20260404 00:00:00.500000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert set(msgs[0]["row"].keys()) == {"symbol", "status", "_mkio_exists", "_mkio_ref", "_mkio_topic"}
    assert msgs[0]["row"]["_mkio_exists"] is True


async def test_query_fields_snapshot(query_svc):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {
        "type": "subscribe",
        "fields": ["symbol"],
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    for row in msgs[0]["rows"]:
        assert "symbol" in row
        assert "_mkio_row" in row
        assert "_mkio_ref" in row
        assert "qty" not in row


async def test_query_fields_update(query_svc, bus):
    ws = MockWebSocket()
    await query_svc.on_subscribe(ws, {
        "type": "subscribe",
        "fields": ["symbol"],
    })
    ws.clear()

    event = ChangeBus.make_event(
        "orders", "insert",
        {"id": "88", "symbol": "AMD", "qty": 5, "status": "new"},
        "20260404 00:02:00.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    row = msgs[0]["row"]
    assert "symbol" in row
    assert "_mkio_row" in row
    assert "_mkio_ref" in row
    assert "qty" not in row


async def test_stream_fields_snapshot(stream_svc):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {
        "type": "subscribe",
        "ref": "00000000 00:00:00.000000000000",
        "fields": ["event"],
    })
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "snapshot"
    for row in msgs[0]["rows"]:
        assert set(row.keys()) == {"event"}


async def test_stream_fields_update(stream_svc, bus):
    ws = MockWebSocket()
    await stream_svc.on_subscribe(ws, {
        "type": "subscribe",
        "ref": "00000000 00:00:00.000000000000",
        "fields": ["event"],
    })
    ws.clear()

    event = ChangeBus.make_event(
        "audit_log", "insert",
        {"id": 99, "event": "field_test", "order_id": "42"},
        "20260404 00:03:00.000000000000",
    )
    bus.publish([event])
    await asyncio.sleep(0.1)

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert set(msgs[0]["row"].keys()) == {"event"}
