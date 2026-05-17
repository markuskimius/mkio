"""Tests for the built-in _mkio info service."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web

from mkio._json import dumps, loads
from mkio._ref import next_ref
from mkio.change_bus import ChangeBus
from mkio.config import load_config
from mkio.database import Database
from mkio.server import _on_startup, _on_shutdown, _ws_handler, _api_services, _api_service_detail
from mkio.services.info import InfoService, _config_hash
from mkio.writer import WriteBatcher


# ---- MockWebSocket ---------------------------------------------------------

class MockWebSocket:
    def __init__(self) -> None:
        self.sent: list[bytes] = []

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data)

    def get_messages(self) -> list[dict[str, Any]]:
        return [loads(b) for b in self.sent]


# ---- Fixtures --------------------------------------------------------------

TEST_CONFIG = {
    "name": "test-app",
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
            },
        },
    },
    "services": {
        "add_order": {
            "protocol": "transaction",
            "ops": [
                {"table": "orders", "op_type": "insert", "fields": ["id", "symbol"]},
            ],
        },
        "live_orders": {
            "protocol": "subpub",
            "primary_table": "orders",
            "topic": "id",
            "change_log_size": 100,
        },
    },
}


def _make_info_svc(db, bus, writer, config=None, services=None):
    svc = InfoService(config={"protocol": "reqrep"}, db=db, change_bus=bus, writer=writer)
    svc.name = "_mkio"
    svc._server_config = config or TEST_CONFIG
    svc._server_services = services or {}
    svc._started_ref = next_ref()
    svc._started_monotonic = time.monotonic()
    return svc


@pytest_asyncio.fixture
async def db():
    database = Database(":memory:", TEST_CONFIG.get("tables", {}))
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


# ---- Unit tests ------------------------------------------------------------

async def test_reply_shape(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})

    msgs = ws.get_messages()
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg["type"] == "reply"
    assert msg["service"] == "_mkio"
    assert msg["reqid"] == "r1"

    row = msg["row"]
    assert set(row.keys()) == {"name", "version", "mkio", "protocol", "services", "tables", "config_hash", "uptime", "started"}
    assert row["protocol"] == "1.0"


async def test_reqid_echoed(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "my-id-123"})
    assert ws.get_messages()[0]["reqid"] == "my-id-123"


async def test_no_reqid(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request"})
    msg = ws.get_messages()[0]
    assert msg["type"] == "reply"
    assert "reqid" not in msg


async def test_name_from_config(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    assert ws.get_messages()[0]["row"]["name"] == "test-app"


async def test_name_default_when_absent(db, bus, writer):
    config = {k: v for k, v in TEST_CONFIG.items() if k != "name"}
    svc = _make_info_svc(db, bus, writer, config=config)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    assert ws.get_messages()[0]["row"]["name"] == ""


async def test_mkio_version_is_string(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    row = ws.get_messages()[0]["row"]
    assert isinstance(row["mkio"], str)
    assert len(row["mkio"]) > 0


async def test_app_version_from_config(db, bus, writer):
    config = {**TEST_CONFIG, "version": "2.1.0"}
    svc = _make_info_svc(db, bus, writer, config=config)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    assert ws.get_messages()[0]["row"]["version"] == "2.1.0"


async def test_app_version_default_when_absent(db, bus, writer):
    svc = _make_info_svc(db, bus, writer, config={k: v for k, v in TEST_CONFIG.items() if k != "version"})
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    assert ws.get_messages()[0]["row"]["version"] == ""


async def test_services_excludes_internal(db, bus, writer):
    from mkio.services.base import Service
    fake_user_svc = Service(config={"protocol": "subpub"}, db=db, change_bus=bus, writer=writer)
    fake_user_svc.name = "live"
    services = {"live": fake_user_svc, "_mkio": _make_info_svc(db, bus, writer)}
    svc = _make_info_svc(db, bus, writer, services=services)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    svc_map = ws.get_messages()[0]["row"]["services"]
    assert "live" in svc_map
    assert "_mkio" not in svc_map


async def test_tables_list(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    assert ws.get_messages()[0]["row"]["tables"] == ["orders"]


async def test_uptime_non_negative(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    assert ws.get_messages()[0]["row"]["uptime"] >= 0


async def test_started_ref_format(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    started = ws.get_messages()[0]["row"]["started"]
    assert len(started) == 30
    assert started[8] == " "


async def test_subscribe_rejected(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    count = await svc.on_subscribe(ws, {
        "type": "subscribe",
        "service": "_mkio",
        "protocol": "reqrep",
    })
    assert count == 0
    msg = ws.get_messages()[0]
    assert msg["type"] == "nack"
    assert "_mkio" in msg["service"]


async def test_config_hash_stable(db, bus, writer):
    svc = _make_info_svc(db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r1"})
    hash1 = ws.get_messages()[0]["row"]["config_hash"]
    ws.sent.clear()

    await svc.on_message(ws, {"type": "request", "reqid": "r2"})
    hash2 = ws.get_messages()[0]["row"]["config_hash"]
    assert hash1 == hash2


async def test_config_hash_differs():
    h1 = _config_hash({"name": "a", "tables": {}})
    h2 = _config_hash({"name": "b", "tables": {}})
    assert h1 != h2
    assert len(h1) == 8
    assert len(h2) == 8


# ---- Integration tests -----------------------------------------------------

def _build_app(config=None) -> web.Application:
    cfg = load_config(config or TEST_CONFIG)
    app = web.Application()
    app["config"] = cfg
    app["monitors"] = defaultdict(set)
    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)
    app.router.add_get("/api/services", _api_services)
    app.router.add_get("/api/services/{service_name}", _api_service_detail)
    app.router.add_get("/ws", _ws_handler)
    return app


@pytest_asyncio.fixture
async def client(aiohttp_client):
    return await aiohttp_client(_build_app())


async def ws_send_recv(ws, msg: dict) -> dict:
    await ws.send_bytes(dumps(msg))
    resp = await ws.receive()
    return loads(resp.data)


async def test_mkio_auto_registered(client):
    ws = await client.ws_connect("/ws")
    reply = await ws_send_recv(ws, {
        "type": "request",
        "service": "_mkio",
        "reqid": "test1",
    })
    assert reply["type"] == "reply"
    assert reply["service"] == "_mkio"
    assert reply["reqid"] == "test1"
    row = reply["row"]
    assert row["name"] == "test-app"
    assert "add_order" in row["services"]
    assert "live_orders" in row["services"]
    assert "_mkio" not in row["services"]
    assert "orders" in row["tables"]
    await ws.close()


async def test_mkio_not_in_api_services(client):
    resp = await client.get("/api/services")
    data = await resp.json()
    names = {s["name"] for s in data}
    assert "_mkio" not in names


async def test_mkio_not_in_api_service_detail(client):
    resp = await client.get("/api/services/_mkio")
    assert resp.status == 404


async def test_mkio_not_in_error_hints(client):
    ws = await client.ws_connect("/ws")
    reply = await ws_send_recv(ws, {
        "type": "request",
        "service": "nonexistent",
        "reqid": "t1",
    })
    assert reply["type"] == "nack"
    assert "_mkio" not in reply["message"]
    await ws.close()


async def test_request_to_non_reqrep_returns_error(client):
    ws = await client.ws_connect("/ws")
    reply = await ws_send_recv(ws, {
        "type": "request",
        "service": "add_order",
        "reqid": "r1",
        "data": {},
    })
    assert reply["type"] == "error"
    assert "transaction" in reply["message"]
    assert "not 'reqrep'" in reply["message"]
    assert reply["reqid"] == "r1"
    await ws.close()


async def test_subscribe_to_reqrep_returns_protocol_mismatch(client):
    ws = await client.ws_connect("/ws")
    reply = await ws_send_recv(ws, {
        "type": "subscribe",
        "service": "_mkio",
        "protocol": "subpub",
        "topic": "x",
    })
    assert reply["type"] == "nack"
    assert "reqrep" in reply["message"]
    assert "not 'subpub'" in reply["message"]
    await ws.close()


async def test_config_name_accepted():
    cfg = load_config({"name": "my-server", "db_path": ":memory:"})
    assert cfg["name"] == "my-server"


async def test_config_version_accepted():
    cfg = load_config({"version": "1.0.0", "db_path": ":memory:"})
    assert cfg["version"] == "1.0.0"


async def test_config_no_name_no_warning(caplog):
    import logging
    with caplog.at_level(logging.WARNING, logger="mkio.config"):
        load_config({"db_path": ":memory:"})
    assert "name" not in caplog.text.lower() or "unknown" not in caplog.text.lower()
