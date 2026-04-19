"""Tests for CLI send and subscribe commands."""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web

from mkio._json import dumps, loads
from mkio.config import load_config
from mkio.server import _on_startup, _on_shutdown, _ws_handler, _api_services


TEST_CONFIG = {
    "host": "127.0.0.1",
    "port": 0,
    "db_path": ":memory:",
    "batch_max_size": 100,
    "batch_max_wait_ms": 1.0,
    "tables": {
        "orders": {
            "columns": {
                "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
                "side": "TEXT NOT NULL",
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
                "order_id": "INTEGER",
                "status": "TEXT",
            },
        },
    },
    "services": {
        "orders": {
            "protocol": "transaction",
            "ops": {
                "place": [
                    {"table": "orders", "op_type": "insert", "fields": ["side", "symbol", "qty", "price"]},
                    {"table": "audit_log", "op_type": "insert", "fields": ["event"], "bind": {"order_id": "$0.id", "status": "$0.status"}},
                ],
                "update_status": [
                    {"table": "orders", "op_type": "update", "key": ["id"], "fields": ["status"]},
                    {"table": "audit_log", "op_type": "insert", "fields": ["event"], "bind": {"order_id": "$0.id", "status": "$0.status"}},
                ],
            },
        },
        "last_trade": {
            "protocol": "subpub",
            "primary_table": "orders",
            "topic": "id",
            "change_log_size": 100,
        },
        "audit_feed": {
            "protocol": "stream",
            "primary_table": "audit_log",
            "buffer_size": 100,
        },
        "all_orders": {
            "protocol": "query",
            "primary_table": "orders",
            "filterable": ["status", "symbol"],
            "change_log_size": 100,
        },
    },
}


def _build_app() -> web.Application:
    cfg = load_config(TEST_CONFIG)
    app = web.Application()
    app["config"] = cfg
    from collections import defaultdict
    app["monitors"] = defaultdict(set)
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)
    app.router.add_get("/api/services", _api_services)
    app.router.add_get("/ws", _ws_handler)
    app.router.add_get("/ws/{service_name}", _ws_handler)
    return app


@pytest_asyncio.fixture
async def app():
    return _build_app()


@pytest_asyncio.fixture
async def client(aiohttp_client, app):
    return await aiohttp_client(app)


def _ws_url(client) -> str:
    """Get the WebSocket URL for a test client."""
    return f"ws://localhost:{client.port}/ws"


# ---- send tests -------------------------------------------------------------

async def test_send_inline_json(client):
    """Send a single inline JSON message."""
    from mkio.__main__ import _send_messages

    ws_url = _ws_url(client)
    messages = [{"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0, "event": "order_placed"}]
    await _send_messages(ws_url, "orders", "place", messages)

    # Verify the order was created
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({"service": "all_orders", "type": "subscribe", "protocol": "query"}))
    resp = await ws.receive()
    data = loads(resp.data)
    assert data["type"] == "snapshot"
    assert len(data["rows"]) == 1
    assert data["rows"][0]["symbol"] == "AAPL"
    assert data["rows"][0]["side"] == "Buy"
    await ws.close()


async def test_send_with_named_op(client):
    """Send with --op flag selects the correct named op."""
    from mkio.__main__ import _send_messages

    ws_url = _ws_url(client)
    # Place an order
    messages = [{"side": "Sell", "symbol": "MSFT", "qty": 50, "price": 300.0, "event": "order_placed"}]
    await _send_messages(ws_url, "orders", "place", messages)

    # Update its status
    messages = [{"id": 1, "status": "accepted", "event": "order_accepted"}]
    await _send_messages(ws_url, "orders", "update_status", messages)

    # Verify updated
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({"service": "all_orders", "type": "subscribe", "protocol": "query"}))
    resp = await ws.receive()
    data = loads(resp.data)
    assert data["rows"][0]["status"] == "accepted"
    await ws.close()


async def test_send_json_file(client, tmp_path):
    """Send from a .json file (array of objects)."""
    from mkio.__main__ import _load_messages, _send_messages

    json_file = tmp_path / "orders.json"
    json_file.write_text(json.dumps([
        {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0, "event": "order_placed"},
        {"side": "Sell", "symbol": "GOOG", "qty": 50, "price": 2800.0, "event": "order_placed"},
        {"side": "Buy", "symbol": "MSFT", "qty": 200, "price": 300.0, "event": "order_placed"},
    ]))

    messages = _load_messages(str(json_file))
    assert len(messages) == 3

    ws_url = _ws_url(client)
    await _send_messages(ws_url, "orders", "place", messages)

    # Verify all 3 orders created
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({"service": "all_orders", "type": "subscribe", "protocol": "query"}))
    resp = await ws.receive()
    data = loads(resp.data)
    assert len(data["rows"]) == 3
    symbols = {r["symbol"] for r in data["rows"]}
    assert symbols == {"AAPL", "GOOG", "MSFT"}
    await ws.close()


async def test_send_csv_file(client, tmp_path):
    """Send from a .csv file."""
    from mkio.__main__ import _load_messages, _send_messages

    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("side,symbol,qty,price,event\nBuy,AAPL,100,150.0,order_placed\nSell,GOOG,50,2800.0,order_placed\n")

    messages = _load_messages(str(csv_file))
    assert len(messages) == 2
    assert messages[0]["qty"] == 100  # auto-converted to int
    assert messages[0]["price"] == 150.0  # auto-converted to float

    ws_url = _ws_url(client)
    await _send_messages(ws_url, "orders", "place", messages)

    # Verify
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({"service": "all_orders", "type": "subscribe", "protocol": "query"}))
    resp = await ws.receive()
    data = loads(resp.data)
    assert len(data["rows"]) == 2
    await ws.close()


async def test_send_error(client, capsys):
    """Send with missing required field prints error."""
    from mkio.__main__ import _send_messages

    ws_url = _ws_url(client)
    # Missing 'side' which is NOT NULL
    messages = [{"symbol": "AAPL", "qty": 100, "price": 150.0, "event": "order_placed"}]
    await _send_messages(ws_url, "orders", "place", messages)

    captured = capsys.readouterr()
    assert "error" in captured.out.lower()


async def test_send_csv_structured(client, tmp_path):
    """CSV with data.* columns and op column extracts envelope fields."""
    from mkio.__main__ import _load_messages, _send_messages

    # First place an order so we have id=1
    ws_url = _ws_url(client)
    await _send_messages(ws_url, "orders", "place", [
        {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0, "event": "order_placed"},
    ])

    # Now update via structured CSV (like test_txn.csv)
    csv_file = tmp_path / "update.csv"
    csv_file.write_text(
        'service,data.id,data.status,data.event,op\n'
        '"orders",1,"accepted","order_accepted","update_status"\n'
    )

    messages = _load_messages(str(csv_file))
    assert len(messages) == 1
    assert messages[0]["op"] == "update_status"
    assert messages[0]["data"]["id"] == 1
    assert "service" not in messages[0]

    await _send_messages(ws_url, "orders", None, messages)

    # Verify status updated
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({"service": "all_orders", "type": "subscribe", "protocol": "query"}))
    resp = await ws.receive()
    data = loads(resp.data)
    assert data["rows"][0]["status"] == "accepted"
    await ws.close()


async def test_send_csv_per_row_op(client, tmp_path, capsys):
    """CSV rows with different op values each use their own op."""
    from mkio.__main__ import _load_messages, _send_messages

    ws_url = _ws_url(client)

    csv_file = tmp_path / "mixed.csv"
    csv_file.write_text(
        'op,data.side,data.symbol,data.qty,data.price,data.event,data.id,data.status\n'
        'place,Buy,AAPL,100,150.0,order_placed,,\n'
        'place,Sell,GOOG,50,2800.0,order_placed,,\n'
    )

    messages = _load_messages(str(csv_file))
    assert len(messages) == 2
    assert messages[0]["op"] == "place"
    assert messages[1]["op"] == "place"

    await _send_messages(ws_url, "orders", None, messages)

    captured = capsys.readouterr()
    assert "[1/2] ok" in captured.out
    assert "[2/2] ok" in captured.out

    # Verify both orders created
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({"service": "all_orders", "type": "subscribe", "protocol": "query"}))
    resp = await ws.receive()
    data = loads(resp.data)
    assert len(data["rows"]) == 2
    await ws.close()


async def test_send_csv_with_msgid(client, tmp_path):
    """CSV with msgid column passes it through as envelope field."""
    from mkio.__main__ import _load_messages

    csv_file = tmp_path / "with_msgid.csv"
    csv_file.write_text(
        'op,msgid,data.side,data.symbol,data.qty,data.price,data.event\n'
        'place,msg-1,Buy,AAPL,100,150.0,order_placed\n'
        'place,msg-2,Sell,GOOG,50,2800.0,order_placed\n'
    )

    messages = _load_messages(str(csv_file))
    assert len(messages) == 2
    assert messages[0]["msgid"] == "msg-1"
    assert messages[1]["msgid"] == "msg-2"
    assert "msgid" not in messages[0]["data"]
    assert "msgid" not in messages[1]["data"]


async def test_send_json_with_msgid(client, tmp_path):
    """JSON with msgid field passes it through as envelope field."""
    from mkio.__main__ import _load_messages

    json_file = tmp_path / "with_msgid.json"
    json_file.write_text(json.dumps({
        "op": "place",
        "msgid": "msg-42",
        "data": {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0, "event": "order_placed"},
    }))

    messages = _load_messages(str(json_file))
    assert len(messages) == 1
    assert messages[0]["msgid"] == "msg-42"


# ---- subscribe tests --------------------------------------------------------

async def _insert_order(client, side: str, symbol: str, qty: int, price: float) -> None:
    """Helper to insert an order via WebSocket."""
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({
        "service": "orders",
        "ref": "ins1",
        "op": "place",
        "data": {"side": side, "symbol": symbol, "qty": qty, "price": price, "event": "order_placed"},
    }))
    resp = await ws.receive()
    result = loads(resp.data)
    assert result.get("ok") is True
    await ws.close()


async def test_subscribe_subpub(client):
    """Subscribe to subpub service with topic, receive snapshot and live update."""
    from mkio.client import MkioClient

    # Insert initial data
    await _insert_order(client, "Buy", "AAPL", 100, 150.0)

    ws_url = _ws_url(client)
    received: list[dict] = []

    # Subscribe to topic "AAPL" (we use auto-inc id, so find the id first)
    # The key is "id", so subscribe to the topic we just inserted
    async with MkioClient(ws_url, reconnect=False) as mk:
        async for msg in mk.subscribe("last_trade", "subpub", topic="1"):
            received.append(msg)
            if msg.get("type") == "snapshot":
                assert msg["rows"][0]["_mkio_exists"] is True
                break

    assert received[0]["type"] == "snapshot"
    assert len(received[0]["rows"]) == 1
    assert received[0]["rows"][0]["_mkio_exists"] is True


async def test_subscribe_stream(client):
    """Subscribe to stream service, receive snapshot and live update."""
    from mkio.client import MkioClient
    from mkio._ref import next_ref

    # Insert initial data (creates audit_log entry via bind)
    await _insert_order(client, "Buy", "AAPL", 100, 150.0)

    ws_url = _ws_url(client)
    received: list[dict] = []

    async with MkioClient(ws_url, reconnect=False) as mk:
        async for msg in mk.subscribe("audit_feed", "stream", ref="00000000 00:00:00.000000000000"):
            received.append(msg)
            if msg.get("type") == "snapshot":
                # Insert another to trigger update
                await _insert_order(client, "Sell", "GOOG", 50, 2800.0)
            elif msg.get("type") == "update":
                break

    assert received[0]["type"] == "snapshot"
    assert received[1]["type"] == "update"


async def test_subscribe_query(client):
    """Subscribe to query service, receive snapshot and live update."""
    from mkio.client import MkioClient

    await _insert_order(client, "Buy", "AAPL", 100, 150.0)

    ws_url = _ws_url(client)
    received: list[dict] = []

    async with MkioClient(ws_url, reconnect=False) as mk:
        async for msg in mk.subscribe("all_orders", "query"):
            received.append(msg)
            if msg.get("type") == "snapshot":
                await _insert_order(client, "Sell", "GOOG", 50, 2800.0)
            elif msg.get("type") == "update":
                break

    assert received[0]["type"] == "snapshot"
    assert len(received[0]["rows"]) == 1
    assert received[1]["type"] == "update"
    assert received[1]["row"]["symbol"] == "GOOG"


async def test_subscribe_subpub_topic_not_found(client):
    """Subscribe to subpub with topic that doesn't exist, get _mkio_exists=False."""
    from mkio.client import MkioClient

    ws_url = _ws_url(client)
    received: list[dict] = []

    async with MkioClient(ws_url, reconnect=False) as mk:
        async for msg in mk.subscribe("last_trade", "subpub", topic="nonexistent"):
            received.append(msg)
            if msg.get("type") == "snapshot":
                break

    snapshot = received[0]
    assert snapshot["type"] == "snapshot"
    assert len(snapshot["rows"]) == 1
    assert snapshot["rows"][0]["_mkio_exists"] is False
    assert snapshot["rows"][0]["id"] == "nonexistent"


async def test_subscribe_with_filter_query(client):
    """Subscribe to query with --filter, only matching rows appear."""
    from mkio.client import MkioClient

    await _insert_order(client, "Buy", "AAPL", 100, 150.0)
    await _insert_order(client, "Sell", "GOOG", 50, 2800.0)

    ws_url = _ws_url(client)
    received: list[dict] = []

    async with MkioClient(ws_url, reconnect=False) as mk:
        async for msg in mk.subscribe("all_orders", "query", filter="symbol == 'GOOG'"):
            received.append(msg)
            if msg.get("type") == "snapshot":
                # Insert matching and non-matching
                await _insert_order(client, "Sell", "GOOG", 25, 2750.0)
                await _insert_order(client, "Buy", "MSFT", 75, 400.0)
            elif msg.get("type") == "update":
                break

    snapshot = received[0]
    assert snapshot["type"] == "snapshot"
    assert len(snapshot["rows"]) == 1
    assert snapshot["rows"][0]["symbol"] == "GOOG"

    update = received[1]
    assert update["type"] == "update"
    assert update["row"]["symbol"] == "GOOG"


async def test_subscribe_with_filter_stream(client):
    """Subscribe to stream with --filter."""
    from mkio.client import MkioClient

    # Insert orders with different events
    await _insert_order(client, "Buy", "AAPL", 100, 150.0)

    ws_url = _ws_url(client)
    received: list[dict] = []

    async with MkioClient(ws_url, reconnect=False) as mk:
        async for msg in mk.subscribe("audit_feed", "stream", ref="00000000 00:00:00.000000000000", filter="event == 'order_placed'"):
            received.append(msg)
            if msg.get("type") == "snapshot":
                # Insert another order (generates order_placed event)
                await _insert_order(client, "Sell", "GOOG", 50, 2800.0)
            elif msg.get("type") == "update":
                break

    snapshot = received[0]
    assert snapshot["type"] == "snapshot"
    # All existing audit entries should be order_placed
    assert all(r["event"] == "order_placed" for r in snapshot["rows"])

    update = received[1]
    assert update["type"] == "update"
    assert update["row"]["event"] == "order_placed"
