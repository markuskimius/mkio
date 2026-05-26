"""Tests for MkioApp lifecycle and custom routes."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from aiohttp import ClientSession

from mkio import MkioApp, create_app
from mkio._json import dumps, loads


MINIMAL_CONFIG = {
    "host": "127.0.0.1",
    "port": 0,
    "db_path": ":memory:",
    "tables": {
        "items": {
            "columns": {"id": "TEXT PRIMARY KEY", "val": "TEXT"},
        },
    },
    "services": {
        "add": {
            "protocol": "transaction",
            "ops": [{"table": "items", "op_type": "insert", "fields": ["id", "val"]}],
        },
        "live": {
            "protocol": "subpub",
            "primary_table": "items",
            "watch_tables": ["items"],
            "topic": "id",
        },
    },
}


def _get_port(app: MkioApp) -> int:
    """Extract the bound port from a running MkioApp."""
    assert app._site is not None
    for sock in app._site._server.sockets:
        return sock.getsockname()[1]
    raise RuntimeError("No sockets found")


# ---- Lifecycle ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_stop_lifecycle():
    app = create_app(MINIMAL_CONFIG)
    assert not app._running

    await app.start()
    assert app._running
    port = _get_port(app)
    assert port > 0

    await app.stop()
    assert not app._running


@pytest.mark.asyncio
async def test_double_start_raises():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            await app.start()
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_stop_idempotent():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    await app.stop()
    await app.stop()


@pytest.mark.asyncio
async def test_stop_before_start_is_noop():
    app = create_app(MINIMAL_CONFIG)
    await app.stop()


@pytest.mark.asyncio
async def test_wait_resolves_on_stop():
    app = create_app(MINIMAL_CONFIG)
    await app.start()

    async def stop_soon():
        await asyncio.sleep(0.05)
        await app.stop()

    asyncio.create_task(stop_soon())
    await asyncio.wait_for(app.wait(), timeout=2.0)


@pytest.mark.asyncio
async def test_wait_before_start_raises():
    app = create_app(MINIMAL_CONFIG)
    with pytest.raises(RuntimeError, match="not been started"):
        await app.wait()


# ---- Config ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_config_property():
    app = create_app(MINIMAL_CONFIG)
    assert app.config["host"] == "127.0.0.1"
    assert "tables" in app.config


@pytest.mark.asyncio
async def test_config_reflects_defaults():
    cfg = {"db_path": ":memory:", "port": 0, "host": "127.0.0.1"}
    app = create_app(cfg)
    assert app.config.get("batch_max_size") == 500
    assert app.config.get("batch_max_wait_ms") == 2.0


@pytest.mark.asyncio
async def test_create_app_from_toml_file(tmp_path: Path):
    toml = tmp_path / "test.toml"
    toml.write_text(
        'port = 0\nhost = "127.0.0.1"\ndb_path = ":memory:"\n'
    )
    app = create_app(toml)
    assert app.config["port"] == 0
    await app.start()
    try:
        assert app._running
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_create_app_bad_config_raises():
    with pytest.raises((ValueError, SystemExit)):
        create_app({"services": {"bad": {"protocol": "nope"}}})


# ---- Custom routes -----------------------------------------------------------


@pytest.mark.asyncio
async def test_custom_get_route():
    from aiohttp import web

    async def health(request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    app = create_app(MINIMAL_CONFIG, routes=[("GET", "/health", health)])
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data == {"ok": True}
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_custom_post_route():
    from aiohttp import web

    async def echo(request: web.Request) -> web.Response:
        body = await request.json()
        return web.json_response(body)

    app = create_app(MINIMAL_CONFIG, routes=[("POST", "/echo", echo)])
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.post(
                f"http://127.0.0.1:{port}/echo", json={"x": 1}
            ) as resp:
                assert resp.status == 200
                assert (await resp.json()) == {"x": 1}
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_custom_put_delete_patch_routes():
    from aiohttp import web

    async def method_echo(request: web.Request) -> web.Response:
        return web.Response(text=request.method)

    app = create_app(MINIMAL_CONFIG, routes=[
        ("PUT", "/m", method_echo),
        ("DELETE", "/m", method_echo),
        ("PATCH", "/m", method_echo),
    ])
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            for method in ("PUT", "DELETE", "PATCH"):
                async with session.request(method, f"http://127.0.0.1:{port}/m") as resp:
                    assert resp.status == 200
                    assert await resp.text() == method
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_multiple_custom_routes():
    from aiohttp import web

    async def a(request: web.Request) -> web.Response:
        return web.Response(text="a")

    async def b(request: web.Request) -> web.Response:
        return web.Response(text="b")

    app = create_app(MINIMAL_CONFIG, routes=[
        ("GET", "/a", a),
        ("GET", "/b", b),
    ])
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/a") as resp:
                assert await resp.text() == "a"
            async with session.get(f"http://127.0.0.1:{port}/b") as resp:
                assert await resp.text() == "b"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_routes_before_start():
    from aiohttp import web

    async def ping(request: web.Request) -> web.Response:
        return web.Response(text="pong")

    app = create_app(MINIMAL_CONFIG)
    app.add_routes([("GET", "/ping", ping)])
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/ping") as resp:
                assert resp.status == 200
                assert await resp.text() == "pong"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_routes_empty_list():
    app = create_app(MINIMAL_CONFIG)
    app.add_routes([])
    await app.start()
    await app.stop()


@pytest.mark.asyncio
async def test_add_routes_after_start_raises():
    from aiohttp import web

    async def noop(request: web.Request) -> web.Response:
        return web.Response()

    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        with pytest.raises(RuntimeError, match="Cannot add routes"):
            app.add_routes([("GET", "/noop", noop)])
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_unsupported_http_method_raises():
    from aiohttp import web

    async def noop(request: web.Request) -> web.Response:
        return web.Response()

    app = create_app(MINIMAL_CONFIG, routes=[("TRACE", "/t", noop)])
    with pytest.raises(ValueError, match="Unsupported HTTP method"):
        await app.start()


# ---- Built-in routes ---------------------------------------------------------


@pytest.mark.asyncio
async def test_api_services_list():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/api/services") as resp:
                assert resp.status == 200
                data = await resp.json()
                names = {s["name"] for s in data}
                assert "add" in names
                assert "live" in names
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_api_service_detail():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/api/services/add") as resp:
                assert resp.status == 200
                detail = await resp.json()
                assert detail["name"] == "add"
                assert detail["protocol"] == "transaction"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_api_service_detail_unknown_returns_404():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/api/services/nope") as resp:
                assert resp.status == 404
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_mkio_js_served():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/mkio.js") as resp:
                assert resp.status == 200
                assert "javascript" in resp.content_type
                body = await resp.text()
                assert "MkioClient" in body
    finally:
        await app.stop()


# ---- WebSocket ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_websocket_subscribe():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "type": "subscribe",
                    "service": "live",
                    "protocol": "subpub",
                    "topic": "x",
                }))
                resp = await ws.receive()
                msg = loads(resp.data)
                assert msg["type"] == "snapshot"
                assert msg["service"] == "live"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_websocket_per_service_endpoint():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws/live") as ws:
                await ws.send_bytes(dumps({
                    "type": "subscribe",
                    "protocol": "subpub",
                    "topic": "y",
                }))
                resp = await ws.receive()
                msg = loads(resp.data)
                assert msg["type"] == "snapshot"
                assert msg["service"] == "live"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_websocket_transaction():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "service": "add",
                    "data": {"id": "t1", "val": "hello"},
                }))
                resp = await ws.receive()
                msg = loads(resp.data)
                assert msg["type"] == "result"
                assert msg["ok"] is True
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_websocket_unknown_service_nacks():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "type": "subscribe",
                    "service": "nonexistent",
                    "protocol": "subpub",
                    "topic": "x",
                }))
                resp = await ws.receive()
                msg = loads(resp.data)
                assert msg["type"] == "nack"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_websocket_write_then_subscribe():
    """Write a row via transaction, then subscribe and see it in the snapshot."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "service": "add",
                    "data": {"id": "k1", "val": "v1"},
                }))
                result = loads((await ws.receive()).data)
                assert result["ok"] is True

                await ws.send_bytes(dumps({
                    "type": "subscribe",
                    "service": "live",
                    "protocol": "subpub",
                    "topic": "k1",
                }))
                snap = loads((await ws.receive()).data)
                assert snap["type"] == "snapshot"
                assert snap["rows"][0]["_mkio_exists"] is True
                assert snap["rows"][0]["val"] == "v1"
    finally:
        await app.stop()
