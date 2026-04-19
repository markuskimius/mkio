"""Tests for service listing API and monitor protocol."""

from __future__ import annotations

import asyncio
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
                "id": "TEXT PRIMARY KEY",
                "symbol": "TEXT NOT NULL",
                "qty": "INTEGER",
                "status": "TEXT DEFAULT 'pending'",
            },
        },
    },
    "services": {
        "add_order": {
            "protocol": "transaction",
            "ops": [
                {"table": "orders", "op_type": "insert", "fields": ["id", "symbol", "qty"]},
            ],
        },
        "last_trade": {
            "protocol": "subpub",
            "primary_table": "orders",
            "watch_tables": ["orders"],
            "topic": "id",
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


# ---- Helper ----------------------------------------------------------------

async def ws_send_recv(ws, msg: dict) -> dict:
    await ws.send_bytes(dumps(msg))
    resp = await ws.receive()
    return loads(resp.data)


# ---- GET /api/services -----------------------------------------------------

async def test_api_services_list(client):
    resp = await client.get("/api/services")
    assert resp.status == 200
    data = await resp.json()
    assert isinstance(data, list)
    assert len(data) == 2

    names = {s["name"] for s in data}
    assert "add_order" in names
    assert "last_trade" in names

    # Check metadata
    txn = next(s for s in data if s["name"] == "add_order")
    assert txn["protocol"] == "transaction"
    assert "tables" in txn

    subpub = next(s for s in data if s["name"] == "last_trade")
    assert subpub["protocol"] == "subpub"
    assert subpub["primary_table"] == "orders"


async def test_api_services_empty():
    """Server with no services returns empty list."""
    cfg = load_config({"db_path": ":memory:"})
    app = web.Application()
    app["config"] = cfg
    from collections import defaultdict
    app["monitors"] = defaultdict(set)
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)
    app.router.add_get("/api/services", _api_services)

    from aiohttp.test_utils import TestClient, TestServer
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/api/services")
        data = await resp.json()
        assert data == []


# ---- Monitor protocol -------------------------------------------------------

async def test_monitor_ack(client):
    """Sending a monitor request should return monitor_ack."""
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "type": "monitor",
        "service": "add_order",
    })
    assert result["type"] == "monitor_ack"
    assert result["service"] == "add_order"
    await ws.close()


async def test_monitor_unknown_service(client):
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "type": "monitor",
        "service": "nonexistent",
    })
    assert result["type"] == "error"
    assert "Unknown service" in result["message"]
    await ws.close()


async def test_monitor_sees_transaction_inbound_and_outbound(client):
    """Monitor should see both the inbound transaction and outbound result."""
    # Set up monitor
    mon_ws = await client.ws_connect("/ws")
    ack = await ws_send_recv(mon_ws, {
        "type": "monitor",
        "service": "add_order",
    })
    assert ack["type"] == "monitor_ack"

    # Send a transaction from another connection
    tx_ws = await client.ws_connect("/ws")
    await tx_ws.send_bytes(dumps({
        "service": "add_order",
        "ref": "tx1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    }))
    # Consume the transaction result on the tx connection
    tx_resp = await tx_ws.receive()

    # Monitor should receive inbound and outbound
    messages = []
    for _ in range(2):
        msg = await asyncio.wait_for(mon_ws.receive(), timeout=2.0)
        messages.append(loads(msg.data))

    directions = {m["direction"] for m in messages}
    assert "in" in directions
    assert "out" in directions

    # Inbound should be the transaction message
    inbound = next(m for m in messages if m["direction"] == "in")
    assert inbound["service"] == "add_order"
    assert inbound["message"]["data"]["symbol"] == "AAPL"

    # Outbound should be the result
    outbound = next(m for m in messages if m["direction"] == "out")
    assert outbound["service"] == "add_order"
    assert outbound["message"]["type"] == "result"
    assert outbound["message"]["ok"] is True

    await mon_ws.close()
    await tx_ws.close()


async def test_monitor_sees_subscribe_and_updates(client):
    """Monitor should see subscribe inbound, snapshot outbound, and live update outbound."""
    # Set up monitor for last_trade
    mon_ws = await client.ws_connect("/ws")
    ack = await ws_send_recv(mon_ws, {
        "type": "monitor",
        "service": "last_trade",
    })
    assert ack["type"] == "monitor_ack"

    # Subscribe from another connection with a topic
    sub_ws = await client.ws_connect("/ws")
    await sub_ws.send_bytes(dumps({
        "service": "last_trade",
        "type": "subscribe",
        "topic": "1",
    }))
    # Consume snapshot on subscriber
    await sub_ws.receive()

    # Monitor should see: inbound subscribe + outbound snapshot
    messages = []
    for _ in range(2):
        msg = await asyncio.wait_for(mon_ws.receive(), timeout=2.0)
        messages.append(loads(msg.data))

    inbound = next(m for m in messages if m["direction"] == "in")
    assert inbound["message"]["type"] == "subscribe"

    outbound = next(m for m in messages if m["direction"] == "out")
    assert outbound["message"]["type"] == "snapshot"

    # Now insert data — monitor should see live update pushed to subscriber
    tx_ws = await client.ws_connect("/ws")
    await tx_ws.send_bytes(dumps({
        "service": "add_order",
        "ref": "tx1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    }))
    await tx_ws.receive()  # Consume tx result

    # Wait for the live update on subscriber
    await asyncio.wait_for(sub_ws.receive(), timeout=2.0)

    # Monitor should see the outbound update for last_trade
    msg = await asyncio.wait_for(mon_ws.receive(), timeout=2.0)
    update_msg = loads(msg.data)
    assert update_msg["direction"] == "out"
    assert update_msg["service"] == "last_trade"
    assert update_msg["message"]["type"] == "update"
    assert update_msg["message"]["op"] == "update"

    await mon_ws.close()
    await sub_ws.close()
    await tx_ws.close()


async def test_monitor_cleanup_on_disconnect(client):
    """Monitor should be removed from monitors set on disconnect."""
    mon_ws = await client.ws_connect("/ws")
    await ws_send_recv(mon_ws, {
        "type": "monitor",
        "service": "add_order",
    })
    await mon_ws.close()

    # Send a transaction — should not fail even though monitor disconnected
    tx_ws = await client.ws_connect("/ws")
    result = await ws_send_recv(tx_ws, {
        "service": "add_order",
        "ref": "tx1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    assert result["ok"] is True
    await tx_ws.close()


async def test_multiple_monitors(client):
    """Multiple monitors on same service both receive messages."""
    mon1 = await client.ws_connect("/ws")
    mon2 = await client.ws_connect("/ws")

    await ws_send_recv(mon1, {"type": "monitor", "service": "add_order"})
    await ws_send_recv(mon2, {"type": "monitor", "service": "add_order"})

    # Send transaction
    tx_ws = await client.ws_connect("/ws")
    await tx_ws.send_bytes(dumps({
        "service": "add_order",
        "ref": "tx1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    }))
    await tx_ws.receive()

    # Both monitors should receive messages
    for mon in (mon1, mon2):
        messages = []
        for _ in range(2):
            msg = await asyncio.wait_for(mon.receive(), timeout=2.0)
            messages.append(loads(msg.data))
        assert len(messages) == 2

    await mon1.close()
    await mon2.close()
    await tx_ws.close()
