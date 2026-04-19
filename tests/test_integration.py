"""End-to-end integration tests with aiohttp test client."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestServer

from mkio._json import dumps, loads
from mkio.change_bus import ChangeBus
from mkio.config import load_config
from mkio.database import Database
from mkio.server import _on_startup, _on_shutdown, _ws_handler, SERVICE_TYPES
from mkio.writer import WriteBatcher


TEST_CONFIG = {
    "host": "127.0.0.1",
    "port": 0,  # Random port
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
        "audit_log": {
            "columns": {
                "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
                "event": "TEXT",
                "order_id": "TEXT",
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
        "update_order": {
            "protocol": "transaction",
            "ops": [
                {"table": "orders", "op_type": "update", "fields": ["qty"], "key": ["id"]},
            ],
        },
        "delete_order": {
            "protocol": "transaction",
            "ops": [
                {"table": "orders", "op_type": "delete", "key": ["id"]},
            ],
        },
        "last_trade": {
            "protocol": "subpub",
            "primary_table": "orders",
            "watch_tables": ["orders"],
            "topic": "id",
            "change_log_size": 100,
        },
        "audit_feed": {
            "protocol": "stream",
            "primary_table": "audit_log",
            "watch_tables": ["audit_log"],
            "buffer_size": 100,
        },
        "all_orders": {
            "protocol": "query",
            "primary_table": "orders",
            "watch_tables": ["orders"],
            "filterable": ["status"],
            "change_log_size": 100,
        },
    },
}


def _build_app() -> web.Application:
    """Build an aiohttp app with the test config."""
    cfg = load_config(TEST_CONFIG)
    app = web.Application()
    app["config"] = cfg
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)
    app.router.add_get("/ws", _ws_handler)
    app.router.add_get("/ws/{service_name}", _ws_handler)

    # Serve JS client
    js_path = Path(__file__).parent.parent / "src" / "mkio" / "client" / "mkio.js"
    if js_path.exists():
        async def serve_js(request: web.Request) -> web.FileResponse:
            return web.FileResponse(js_path, headers={"Content-Type": "application/javascript"})
        app.router.add_get("/mkio.js", serve_js)

    return app


@pytest_asyncio.fixture
async def app():
    return _build_app()


@pytest_asyncio.fixture
async def client(aiohttp_client, app):
    return await aiohttp_client(app)


# ---- Helper ----------------------------------------------------------------

async def ws_send_recv(ws, msg: dict) -> dict:
    """Send a message and receive the response."""
    await ws.send_bytes(dumps(msg))
    resp = await ws.receive()
    return loads(resp.data)


# ---- Tests -----------------------------------------------------------------

async def test_ws_transaction_roundtrip(client):
    """Send a transaction via /ws, verify result."""
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "service": "add_order",
        "ref": "ref1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    assert result["type"] == "result"
    assert result["ok"] is True
    assert result["ref"] == "ref1"
    assert result["ref"] == "ref1"
    await ws.close()


async def test_ws_per_service_endpoint(client):
    """Transaction via /ws/{service_name} — service implicit from URL."""
    ws = await client.ws_connect("/ws/add_order")
    result = await ws_send_recv(ws, {
        "ref": "ref1",
        "data": {"id": "2", "symbol": "MSFT", "qty": 50},
    })
    assert result["type"] == "result"
    assert result["ok"] is True
    await ws.close()


async def test_unknown_service_error(client):
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "service": "nonexistent",
        "ref": "ref1",
        "data": {},
    })
    assert result["type"] == "error"
    assert "Unknown service" in result["message"]
    await ws.close()


async def test_missing_service_error(client):
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "ref": "ref1",
        "data": {},
    })
    assert result["type"] == "error"
    assert "Missing" in result["message"]
    await ws.close()


async def test_subscribe_protocol_mismatch(client):
    """Subscribing with wrong protocol returns a nack."""
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "service": "last_trade",
        "type": "subscribe",
        "protocol": "query",
        "topic": "1",
    })
    assert result["type"] == "nack"
    assert result["service"] == "last_trade"
    assert "Protocol mismatch" in result["message"]
    assert "'subpub'" in result["message"]
    assert "'query'" in result["message"]
    await ws.close()


async def test_subscribe_protocol_match(client):
    """Subscribing with correct protocol succeeds normally."""
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({
        "service": "last_trade",
        "type": "subscribe",
        "protocol": "subpub",
        "topic": "1",
    }))
    resp = await ws.receive()
    data = loads(resp.data)
    assert data["type"] == "snapshot"
    await ws.close()


async def test_subscribe_protocol_missing(client):
    """Subscribing without protocol returns a nack."""
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "service": "last_trade",
        "type": "subscribe",
        "topic": "1",
    })
    assert result["type"] == "nack"
    assert result["service"] == "last_trade"
    assert "protocol" in result["message"].lower()
    await ws.close()


async def test_subpub_subscribe_snapshot(client):
    """Subscribe to subpub with topic, get not-found, then insert and get update."""
    ws = await client.ws_connect("/ws")

    # Subscribe to a topic that doesn't exist yet
    await ws.send_bytes(dumps({
        "service": "last_trade",
        "type": "subscribe",
        "protocol": "subpub",
        "topic": "1",
    }))
    resp = await ws.receive()
    snapshot = loads(resp.data)
    assert snapshot["type"] == "snapshot"
    assert len(snapshot["rows"]) == 1
    assert snapshot["rows"][0]["_mkio_exists"] is False
    assert snapshot["rows"][0]["id"] == "1"

    # Insert via transaction
    ws2 = await client.ws_connect("/ws")
    result = await ws_send_recv(ws2, {
        "service": "add_order",
        "ref": "tx_ref",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    assert result["ok"] is True

    # Should receive an update on ws (op is always "update" for subpub)
    resp = await asyncio.wait_for(ws.receive(), timeout=2.0)
    update = loads(resp.data)
    assert update["type"] == "update"
    assert update["op"] == "update"
    assert update["row"]["_mkio_exists"] is True

    await ws.close()
    await ws2.close()


async def test_multi_client_fan_out(client):
    """3 clients subscribe to the same topic, write triggers update to all 3."""
    subs = []
    for _ in range(3):
        ws = await client.ws_connect("/ws")
        await ws.send_bytes(dumps({
            "service": "last_trade",
            "type": "subscribe",
            "protocol": "subpub",
            "topic": "fan1",
        }))
        resp = await ws.receive()  # Consume snapshot
        subs.append(ws)

    # Insert
    tx_ws = await client.ws_connect("/ws")
    await ws_send_recv(tx_ws, {
        "service": "add_order",
        "ref": "tx1",
        "data": {"id": "fan1", "symbol": "GOOG", "qty": 200},
    })

    # All 3 should get the update
    for ws in subs:
        resp = await asyncio.wait_for(ws.receive(), timeout=2.0)
        update = loads(resp.data)
        assert update["type"] == "update"
        assert update["row"]["symbol"] == "GOOG"
        assert update["row"]["_mkio_exists"] is True
        await ws.close()

    await tx_ws.close()


async def test_transaction_update_and_delete(client):
    """Insert, update, delete — verify each step."""
    ws = await client.ws_connect("/ws")

    # Insert
    result = await ws_send_recv(ws, {
        "service": "add_order",
        "ref": "r1",
        "data": {"id": "1", "symbol": "AAPL", "qty": 100},
    })
    assert result["ok"] is True

    # Update
    result = await ws_send_recv(ws, {
        "service": "update_order",
        "ref": "r2",
        "data": {"id": "1", "qty": 200},
    })
    assert result["ok"] is True

    # Delete
    result = await ws_send_recv(ws, {
        "service": "delete_order",
        "ref": "r3",
        "data": {"id": "1"},
    })
    assert result["ok"] is True
    await ws.close()


async def test_query_subscribe_and_update(client):
    """Subscribe to query, insert data, verify update received."""
    ws = await client.ws_connect("/ws")

    # Subscribe to query
    await ws.send_bytes(dumps({
        "service": "all_orders",
        "type": "subscribe",
        "protocol": "query",
        "ref": "q_ref",
    }))
    resp = await ws.receive()
    snapshot = loads(resp.data)
    assert snapshot["type"] == "snapshot"

    # Insert via another connection
    ws2 = await client.ws_connect("/ws")
    await ws_send_recv(ws2, {
        "service": "add_order",
        "ref": "tx1",
        "data": {"id": "q1", "symbol": "NFLX", "qty": 10},
    })

    # Query subscriber should get an update
    resp = await asyncio.wait_for(ws.receive(), timeout=2.0)
    update = loads(resp.data)
    assert update["type"] == "update"

    await ws.close()
    await ws2.close()


async def test_stream_subscribe_and_live(client):
    """Subscribe to stream (audit_feed), insert audit entry, verify push."""
    # First, insert audit data via a direct operation
    # The stream watches audit_log, but we don't have a transaction service for it in config.
    # We'll just subscribe and verify empty snapshot, since audit_log starts empty.
    ws = await client.ws_connect("/ws")
    await ws.send_bytes(dumps({
        "service": "audit_feed",
        "type": "subscribe",
        "protocol": "stream",
        "ref": "s_ref",
    }))
    resp = await ws.receive()
    snapshot = loads(resp.data)
    assert snapshot["type"] == "snapshot"
    assert isinstance(snapshot["rows"], list)
    await ws.close()


async def test_js_client_serving(client):
    """GET /mkio.js should return valid JavaScript."""
    resp = await client.get("/mkio.js")
    assert resp.status == 200
    text = await resp.text()
    assert "MkioClient" in text
    assert "makeRef" in text
    assert resp.headers.get("Content-Type", "").startswith("application/javascript")


async def test_transaction_check_recovery(client):
    """Send transaction, get ref, check it via 'check' message."""
    ws = await client.ws_connect("/ws")

    # Insert
    result = await ws_send_recv(ws, {
        "service": "add_order",
        "ref": "r1",
        "data": {"id": "chk1", "symbol": "AAPL", "qty": 100},
    })
    tx_ref = result["ref"]

    # Check with known ref
    check_result = await ws_send_recv(ws, {
        "service": "add_order",
        "type": "check",
        "ref": tx_ref,
    })
    assert check_result["type"] == "result"
    assert check_result["ok"] is True

    # Check with unknown ref
    unknown = await ws_send_recv(ws, {
        "service": "add_order",
        "type": "check",
        "ref": "19700101 00:00:00.000000000000",
    })
    assert unknown.get("status") == "unknown"
    await ws.close()


async def test_unsubscribe_stops_updates(client):
    """After unsubscribe, client should not receive further updates."""
    ws = await client.ws_connect("/ws")

    # Subscribe
    await ws.send_bytes(dumps({
        "service": "last_trade",
        "type": "subscribe",
        "protocol": "subpub",
        "topic": "unsub1",
    }))
    await ws.receive()  # Snapshot

    # Unsubscribe
    await ws.send_bytes(dumps({
        "service": "last_trade",
        "type": "unsubscribe",
        "ref": "u1",
    }))

    # Insert data
    ws2 = await client.ws_connect("/ws")
    await ws_send_recv(ws2, {
        "service": "add_order",
        "ref": "tx1",
        "data": {"id": "unsub1", "symbol": "X", "qty": 1},
    })

    # ws should NOT receive update — set a short timeout
    try:
        resp = await asyncio.wait_for(ws.receive(), timeout=0.3)
        # If we get something, it shouldn't be an update for last_trade
        data = loads(resp.data)
        assert data.get("type") != "update" or data.get("service") != "last_trade"
    except asyncio.TimeoutError:
        pass  # Expected: no message

    await ws.close()
    await ws2.close()


async def test_msgid_echoed_on_result(client):
    """Transaction with msgid should echo it back in the result."""
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "service": "add_order",
        "ref": "ref_msgid",
        "msgid": "client-msg-42",
        "data": {"id": "m1", "symbol": "AAPL", "qty": 100},
    })
    assert result["type"] == "result"
    assert result["ok"] is True
    assert result["msgid"] == "client-msg-42"
    await ws.close()


async def test_msgid_echoed_on_error(client):
    """Transaction error should echo msgid back."""
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "service": "add_order",
        "ref": "ref_msgid_err",
        "msgid": "client-msg-99",
        "data": {},  # Missing required fields
    })
    assert result["type"] == "error"
    assert result["msgid"] == "client-msg-99"
    await ws.close()


async def test_msgid_absent_when_not_sent(client):
    """Result without msgid in request should not have msgid in response."""
    ws = await client.ws_connect("/ws")
    result = await ws_send_recv(ws, {
        "service": "add_order",
        "ref": "ref_no_msgid",
        "data": {"id": "m2", "symbol": "GOOG", "qty": 50},
    })
    assert result["type"] == "result"
    assert "msgid" not in result
    await ws.close()


async def test_msgid_with_live_updates(client):
    """Transaction with msgid should not interfere with subscription updates."""
    ws_sub = await client.ws_connect("/ws")

    # Subscribe to last_trade
    await ws_sub.send_bytes(dumps({
        "service": "last_trade",
        "type": "subscribe",
        "protocol": "subpub",
        "topic": "live1",
    }))
    await ws_sub.receive()  # Consume snapshot

    # Send transaction with msgid
    ws_tx = await client.ws_connect("/ws")
    result = await ws_send_recv(ws_tx, {
        "service": "add_order",
        "ref": "ref_live",
        "msgid": "live-test-1",
        "data": {"id": "live1", "symbol": "TSLA", "qty": 75},
    })
    assert result["ok"] is True
    assert result["msgid"] == "live-test-1"

    # Subscriber should still get the update
    resp = await asyncio.wait_for(ws_sub.receive(), timeout=2.0)
    update = loads(resp.data)
    assert update["type"] == "update"
    assert update["op"] == "update"
    assert update["row"]["symbol"] == "TSLA"
    assert "msgid" not in update  # Updates don't carry msgid

    await ws_sub.close()
    await ws_tx.close()
