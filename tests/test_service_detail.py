"""Tests for service detail API endpoint."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web

from mkio.config import load_config
from mkio.server import _on_startup, _on_shutdown, _api_services, _api_service_detail


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


def _build_app(config=None) -> web.Application:
    cfg = load_config(config or TEST_CONFIG)
    app = web.Application()
    app["config"] = cfg
    app["monitors"] = defaultdict(set)
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)
    app.router.add_get("/api/services", _api_services)
    app.router.add_get("/api/services/{service_name}", _api_service_detail)
    return app


@pytest_asyncio.fixture
async def client(aiohttp_client):
    return await aiohttp_client(_build_app())


# ---- Transaction detail -----------------------------------------------------

async def test_detail_transaction(client):
    resp = await client.get("/api/services/orders")
    assert resp.status == 200
    data = await resp.json()

    assert data["name"] == "orders"
    assert data["protocol"] == "transaction"
    assert "place" in data["ops"]
    assert "update_status" in data["ops"]

    # Check place op
    place = data["ops"]["place"]
    assert len(place["steps"]) == 2

    # First step: insert into orders
    step0 = place["steps"][0]
    assert step0["table"] == "orders"
    assert step0["op_type"] == "insert"

    # Fields the client provides
    fields = step0["fields"]
    assert "side" in fields
    assert fields["side"]["required"] is True
    assert fields["side"]["type"] == "TEXT"
    assert "symbol" in fields
    assert fields["symbol"]["required"] is True
    assert "qty" in fields
    assert fields["qty"]["required"] is False
    assert "price" in fields
    assert fields["price"]["required"] is False
    assert fields["price"]["default"] == "0"

    # Auto-generated columns
    auto = step0["auto"]
    assert "id" in auto
    assert auto["id"]["source"] == "autoincrement"
    assert "status" in auto
    assert auto["status"]["source"] == "default"
    assert auto["status"]["default"] == "'pending'"

    # Second step: audit_log with binds
    step1 = place["steps"][1]
    assert step1["table"] == "audit_log"
    assert step1["bind"]["order_id"] == "$0.id"
    assert step1["bind"]["status"] == "$0.status"

    # Transaction recovery info
    assert "recovery" in data
    assert "check_message" in data["recovery"]
    assert data["recovery"]["check_message"]["type"] == "check"
    assert data["recovery"]["check_message"]["service"] == "orders"


async def test_detail_transaction_update_op(client):
    resp = await client.get("/api/services/orders")
    data = await resp.json()

    update = data["ops"]["update_status"]
    step0 = update["steps"][0]
    assert step0["op_type"] == "update"

    # id is a key field
    assert step0["fields"]["id"]["key"] is True
    assert step0["fields"]["id"]["required"] is True

    # status is a client-provided field
    assert "status" in step0["fields"]


# ---- Listener detail --------------------------------------------------------

async def test_detail_subpub(client):
    resp = await client.get("/api/services/last_trade")
    assert resp.status == 200
    data = await resp.json()

    assert data["name"] == "last_trade"
    assert data["protocol"] == "subpub"
    assert data["primary_table"] == "orders"
    assert data["topic"] == "id"

    # Schema from orders table
    schema = data["schema"]
    assert "id" in schema
    assert schema["id"]["pk"] is True
    assert schema["id"]["type"] == "INTEGER"
    assert "side" in schema
    assert "symbol" in schema
    assert "qty" in schema
    assert "price" in schema
    assert "status" in schema

    # Subscribe protocol — subpub uses topic, not filter
    sub = data["subscribe"]
    assert sub["message"]["service"] == "last_trade"
    assert sub["message"]["type"] == "subscribe"
    assert sub["message"]["topic"] == "<topic_value>"
    assert sub["topic"] == "id"
    assert "recovery" not in sub
    assert sub["response_types"] == ["snapshot", "update"]
    assert "filter_fields" not in sub

    # Examples
    assert "subscribe" in data["example"]
    assert "subpub" in data["example"]["subscribe"]
    assert "subscribe_filter" not in data["example"]
    assert "subscribe_recover" not in data["example"]


async def test_detail_stream(client):
    resp = await client.get("/api/services/audit_feed")
    assert resp.status == 200
    data = await resp.json()

    assert data["name"] == "audit_feed"
    assert data["protocol"] == "stream"
    assert data["primary_table"] == "audit_log"

    # Stream subscribe protocol
    sub = data["subscribe"]
    assert sub["response_types"] == ["snapshot", "update"]
    assert sub["buffer_size"] == 100
    assert "recovery" in sub
    assert "ref" in sub["recovery"].lower()

    schema = data["schema"]
    assert "id" in schema
    assert "event" in schema
    assert "order_id" in schema


async def test_detail_query(client):
    resp = await client.get("/api/services/all_orders")
    assert resp.status == 200
    data = await resp.json()

    assert data["name"] == "all_orders"
    assert data["protocol"] == "query"
    assert data["filterable"] == ["status", "symbol"]
    assert "schema" in data

    # Query subscribe protocol
    sub = data["subscribe"]
    assert sub["response_types"] == ["snapshot", "update"]
    assert "change_log_size" not in sub
    assert "recovery" not in sub


# ---- Error cases ------------------------------------------------------------

async def test_detail_unknown_service(client):
    resp = await client.get("/api/services/nonexistent")
    assert resp.status == 404
    data = await resp.json()
    assert "error" in data


# ---- Descriptions -----------------------------------------------------------

async def test_detail_with_description(aiohttp_client):
    config = {
        **TEST_CONFIG,
        "services": {
            "orders": {
                **TEST_CONFIG["services"]["orders"],
                "description": "Place and manage orders",
                "descriptions": {
                    "place": "Submit a new order",
                    "update_status": "Change order status",
                },
            },
        },
    }
    client = await aiohttp_client(_build_app(config))
    resp = await client.get("/api/services/orders")
    data = await resp.json()

    assert data["description"] == "Place and manage orders"
    assert data["ops"]["place"]["description"] == "Submit a new order"
    assert data["ops"]["update_status"]["description"] == "Change order status"


# ---- Example generation -----------------------------------------------------

async def test_detail_example_generation(client):
    resp = await client.get("/api/services/orders")
    data = await resp.json()

    # Transaction ops should have examples
    place = data["ops"]["place"]
    assert "example" in place
    assert "mkio send" in place["example"]
    assert "--op place" in place["example"]

    # Listener should have subscribe example with topic as positional arg
    resp = await client.get("/api/services/last_trade")
    data = await resp.json()
    assert "mkio subpub" in data["example"]["subscribe"]
