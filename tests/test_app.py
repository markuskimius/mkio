"""Tests for MkioApp lifecycle, custom routes, services, hooks, and data facade."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from aiohttp import ClientSession

from mkio import ChangeEvent, MkioApp, Service, create_app
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


NAMED_OPS_CONFIG = {
    "host": "127.0.0.1",
    "port": 0,
    "db_path": ":memory:",
    "tables": {
        "items": {
            "columns": {"id": "TEXT PRIMARY KEY", "val": "TEXT"},
        },
    },
    "services": {
        "ops": {
            "protocol": "transaction",
            "ops": {
                "create": [{"table": "items", "op_type": "insert", "fields": ["id", "val"]}],
                "remove": [{"table": "items", "op_type": "delete", "key": ["id"]}],
            },
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


@pytest.mark.asyncio
async def test_mkio_expr_js_served():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/mkio-expr.js") as resp:
                assert resp.status == 200
                assert "javascript" in resp.content_type
                body = await resp.text()
                assert "export const LANGUAGE_VERSION" in body
                assert "globalThis.mkioExpr" in body
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


# ---- Custom services ---------------------------------------------------------


class CounterService(Service):
    started = False
    stopped = False

    async def start(self):
        CounterService.started = True

    async def stop(self):
        CounterService.stopped = True

    async def on_message(self, ws, msg):
        from mkio.ws_protocol import make_result
        resp = make_result(msg.get("ref", ""), self.name, {"count": 42})
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)


class SubscribableService(Service):
    """Custom service that supports subscribe/unsubscribe."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._subscribers: set = set()

    async def on_subscribe(self, ws, msg):
        self._subscribers.add(id(ws))
        from mkio._json import dumps as _dumps
        await ws.send_bytes(_dumps({
            "type": "snapshot",
            "service": self.name,
            "rows": [],
        }))
        return 1

    async def on_unsubscribe(self, ws, msg):
        self._subscribers.discard(id(ws))
        return 1


class BusWatcherService(Service):
    """Custom service that subscribes to ChangeBus internally."""
    received: list = []

    async def start(self):
        BusWatcherService.received = []
        self._q = self.bus.subscribe(["items"])
        self._task = asyncio.create_task(self._watch())

    async def _watch(self):
        try:
            while True:
                event = await self._q.get()
                BusWatcherService.received.append(event.row)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.bus.unsubscribe(["items"], self._q)


@pytest.mark.asyncio
async def test_add_service_lifecycle():
    CounterService.started = False
    CounterService.stopped = False
    app = create_app(MINIMAL_CONFIG)
    app.add_service("counter", CounterService, {"protocol": "reqrep"})
    await app.start()
    try:
        assert CounterService.started
    finally:
        await app.stop()
    assert CounterService.stopped


@pytest.mark.asyncio
async def test_add_service_reachable_via_ws():
    app = create_app(MINIMAL_CONFIG)
    app.add_service("counter", CounterService, {"protocol": "reqrep"})
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "type": "request",
                    "service": "counter",
                    "reqid": "r1",
                    "data": {},
                }))
                resp = loads((await ws.receive()).data)
                assert resp["type"] == "result"
                assert resp["ok"] is True
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_appears_in_api():
    app = create_app(MINIMAL_CONFIG)
    app.add_service("counter", CounterService, {"protocol": "reqrep"})
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/api/services") as resp:
                data = await resp.json()
                names = {s["name"] for s in data}
                assert "counter" in names
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_after_start_raises():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        with pytest.raises(RuntimeError, match="Cannot add services"):
            app.add_service("late", CounterService)
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_default_empty_config():
    app = create_app(MINIMAL_CONFIG)
    app.add_service("counter", CounterService)
    await app.start()
    try:
        svc = app.services["counter"]
        assert svc.config == {}
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_multiple_services():
    """Register multiple programmatic services at once."""
    CounterService.started = False
    app = create_app(MINIMAL_CONFIG)
    app.add_service("counter", CounterService, {"protocol": "reqrep"})
    app.add_service("watcher", BusWatcherService)
    await app.start()
    try:
        assert "counter" in app.services
        assert "watcher" in app.services
        assert CounterService.started
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_receives_db_bus_writer():
    """Programmatic service gets working db, bus, writer references."""
    app = create_app(MINIMAL_CONFIG)
    app.add_service("watcher", BusWatcherService)
    await app.start()
    try:
        svc = app.services["watcher"]
        assert svc.db is app.db
        assert svc.bus is app.change_bus
        assert svc.writer is app.writer
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_uses_change_bus():
    """Programmatic service can subscribe to ChangeBus and receive events."""
    BusWatcherService.received = []
    app = create_app(MINIMAL_CONFIG)
    app.add_service("watcher", BusWatcherService)
    await app.start()
    try:
        await app.execute("add", {"id": "bw1", "val": "hello"})
        await asyncio.sleep(0.05)
        assert len(BusWatcherService.received) == 1
        assert BusWatcherService.received[0]["id"] == "bw1"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_with_subscribe_protocol():
    """Programmatic service with on_subscribe is reachable via WS subscribe."""
    app = create_app(MINIMAL_CONFIG)
    app.add_service("custom_sub", SubscribableService, {"protocol": "subpub"})
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "type": "subscribe",
                    "service": "custom_sub",
                    "protocol": "subpub",
                    "topic": "t1",
                }))
                resp = loads((await ws.receive()).data)
                assert resp["type"] == "snapshot"
                assert resp["service"] == "custom_sub"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_coexists_with_config_services():
    """Programmatic and config-driven services both work on the same server."""
    app = create_app(MINIMAL_CONFIG)
    app.add_service("counter", CounterService, {"protocol": "reqrep"})
    await app.start()
    try:
        # Config-driven transaction service still works
        result = await app.execute("add", {"id": "coex1", "val": "v"})
        assert result["ok"] is True

        # Programmatic service also works
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "type": "request",
                    "service": "counter",
                    "reqid": "r1",
                    "data": {},
                }))
                resp = loads((await ws.receive()).data)
                assert resp["type"] == "result"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_add_service_name_set_on_instance():
    """The service instance has its name attribute set."""
    app = create_app(MINIMAL_CONFIG)
    app.add_service("my_counter", CounterService)
    await app.start()
    try:
        svc = app.services["my_counter"]
        assert svc.name == "my_counter"
    finally:
        await app.stop()


# ---- Lifecycle hooks ---------------------------------------------------------


@pytest.mark.asyncio
async def test_on_startup_hook():
    started = []

    async def hook():
        started.append(True)

    app = create_app(MINIMAL_CONFIG)
    app.on_startup(hook)
    await app.start()
    try:
        assert started == [True]
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_on_shutdown_hook():
    stopped = []

    async def hook():
        stopped.append(True)

    app = create_app(MINIMAL_CONFIG)
    app.on_shutdown(hook)
    await app.start()
    assert stopped == []
    await app.stop()
    assert stopped == [True]


@pytest.mark.asyncio
async def test_on_connect_disconnect_hooks():
    connected = []
    disconnected = []

    async def on_connect(ws):
        connected.append(True)

    async def on_disconnect(ws):
        disconnected.append(True)

    app = create_app(MINIMAL_CONFIG)
    app.on_connect(on_connect)
    app.on_disconnect(on_disconnect)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                assert connected == [True]
                await ws.send_bytes(dumps({
                    "type": "subscribe",
                    "service": "live",
                    "protocol": "subpub",
                    "topic": "x",
                }))
                await ws.receive()
            # ws closed by context manager exit
            await asyncio.sleep(0.05)
        assert disconnected == [True]
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_multiple_startup_hooks_run_in_order():
    order = []

    async def first():
        order.append("first")

    async def second():
        order.append("second")

    app = create_app(MINIMAL_CONFIG)
    app.on_startup(first)
    app.on_startup(second)
    await app.start()
    try:
        assert order == ["first", "second"]
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_multiple_shutdown_hooks_run_in_order():
    order = []

    async def first():
        order.append("first")

    async def second():
        order.append("second")

    app = create_app(MINIMAL_CONFIG)
    app.on_shutdown(first)
    app.on_shutdown(second)
    await app.start()
    await app.stop()
    assert order == ["first", "second"]


@pytest.mark.asyncio
async def test_startup_hook_can_access_services():
    """Startup hook runs after services are initialized, so app.services is populated."""
    found_services = []

    async def hook():
        found_services.extend(app.services.keys())

    app = create_app(MINIMAL_CONFIG)
    app.on_startup(hook)
    await app.start()
    try:
        assert "add" in found_services
        assert "live" in found_services
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_shutdown_hook_runs_before_services_stop():
    """Shutdown hook can still access services (runs before service.stop())."""
    services_alive = []

    async def hook():
        services_alive.extend(list(app.services.keys()))

    app = create_app(MINIMAL_CONFIG)
    app.on_shutdown(hook)
    await app.start()
    await app.stop()
    assert "add" in services_alive
    assert "live" in services_alive


@pytest.mark.asyncio
async def test_connect_disconnect_same_ws_object():
    """Connect and disconnect hooks receive the same WS object."""
    connect_ids = []
    disconnect_ids = []

    async def on_connect(ws):
        connect_ids.append(id(ws))

    async def on_disconnect(ws):
        disconnect_ids.append(id(ws))

    app = create_app(MINIMAL_CONFIG)
    app.on_connect(on_connect)
    app.on_disconnect(on_disconnect)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                pass
            await asyncio.sleep(0.05)
        assert len(connect_ids) == 1
        assert len(disconnect_ids) == 1
        assert connect_ids[0] == disconnect_ids[0]
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_multiple_connections_trigger_hooks():
    """Each WS connection triggers its own connect/disconnect hook call."""
    connect_count = []
    disconnect_count = []

    async def on_connect(ws):
        connect_count.append(True)

    async def on_disconnect(ws):
        disconnect_count.append(True)

    app = create_app(MINIMAL_CONFIG)
    app.on_connect(on_connect)
    app.on_disconnect(on_disconnect)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws"):
                async with session.ws_connect(f"http://127.0.0.1:{port}/ws"):
                    assert len(connect_count) == 2
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.02)
        assert len(disconnect_count) == 2
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_multiple_connect_hooks():
    """Multiple on_connect hooks all fire for each connection."""
    first_called = []
    second_called = []

    async def first(ws):
        first_called.append(True)

    async def second(ws):
        second_called.append(True)

    app = create_app(MINIMAL_CONFIG)
    app.on_connect(first)
    app.on_connect(second)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws"):
                pass
        assert first_called == [True]
        assert second_called == [True]
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_disconnect_hook_exception_does_not_break_cleanup():
    """An exception in one disconnect hook doesn't prevent others from running."""
    second_called = []

    async def bad_hook(ws):
        raise RuntimeError("hook error")

    async def good_hook(ws):
        second_called.append(True)

    app = create_app(MINIMAL_CONFIG)
    app.on_disconnect(bad_hook)
    app.on_disconnect(good_hook)
    await app.start()
    try:
        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws"):
                pass
            await asyncio.sleep(0.05)
        assert second_called == [True]
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_no_hooks_is_fine():
    """Server works normally with zero hooks registered."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        result = await app.execute("add", {"id": "nh1", "val": "v"})
        assert result["ok"] is True
    finally:
        await app.stop()


# ---- Raw internals (properties) ----------------------------------------------


@pytest.mark.asyncio
async def test_properties_none_before_start():
    app = create_app(MINIMAL_CONFIG)
    assert app.db is None
    assert app.writer is None
    assert app.change_bus is None
    assert app.services == {}


@pytest.mark.asyncio
async def test_properties_available_after_start():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        assert app.db is not None
        assert app.writer is not None
        assert app.change_bus is not None
        assert "add" in app.services
        assert "live" in app.services
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_properties_none_after_stop():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    await app.stop()
    assert app.db is None
    assert app.writer is None
    assert app.change_bus is None


@pytest.mark.asyncio
async def test_services_includes_programmatic_services():
    """The services property includes both config-driven and programmatic services."""
    app = create_app(MINIMAL_CONFIG)
    app.add_service("counter", CounterService, {"protocol": "reqrep"})
    await app.start()
    try:
        assert "add" in app.services
        assert "live" in app.services
        assert "counter" in app.services
        assert "_mkio" in app.services
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_db_property_is_usable():
    """The raw db property can execute queries directly."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        await app.execute("add", {"id": "dp1", "val": "v"})
        rows = await app.db.read("SELECT * FROM items WHERE id = 'dp1'")
        assert len(rows) == 1
        assert rows[0]["val"] == "v"
    finally:
        await app.stop()


# ---- Data facade: execute ----------------------------------------------------


@pytest.mark.asyncio
async def test_execute_inserts_row():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        result = await app.execute("add", {"id": "e1", "val": "hello"})
        assert result["ok"] is True
        assert "ref" in result

        rows = await app.query("SELECT * FROM items WHERE id = 'e1'")
        assert len(rows) == 1
        assert rows[0]["val"] == "hello"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_fans_out_to_subscribers():
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
                    "topic": "e2",
                }))
                snap = loads((await ws.receive()).data)
                assert snap["type"] == "snapshot"
                assert snap["rows"][0]["_mkio_exists"] is False

                await app.execute("add", {"id": "e2", "val": "world"})

                update = loads((await asyncio.wait_for(ws.receive(), timeout=2.0)).data)
                assert update["type"] == "update"
                assert update["row"]["val"] == "world"
                assert update["row"]["_mkio_exists"] is True
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_named_op():
    app = create_app(NAMED_OPS_CONFIG)
    await app.start()
    try:
        result = await app.execute("ops", {"id": "n1", "val": "v"}, op="create")
        assert result["ok"] is True

        rows = await app.query("SELECT * FROM items WHERE id = 'n1'")
        assert len(rows) == 1

        result = await app.execute("ops", {"id": "n1"}, op="remove")
        assert result["ok"] is True

        rows = await app.query("SELECT * FROM items WHERE id = 'n1'")
        assert len(rows) == 0
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_unknown_service_raises():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        with pytest.raises(KeyError, match="Unknown service"):
            await app.execute("nonexistent", {"id": "x"})
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_non_transaction_raises():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        with pytest.raises(ValueError, match="not 'transaction'"):
            await app.execute("live", {"id": "x"})
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_before_start_raises():
    app = create_app(MINIMAL_CONFIG)
    with pytest.raises(RuntimeError, match="not running"):
        await app.execute("add", {"id": "x", "val": "v"})


@pytest.mark.asyncio
async def test_execute_missing_required_field_raises():
    """Missing a required field in execute data raises KeyError."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        with pytest.raises(KeyError):
            await app.execute("add", {"id": "mf1"})  # missing "val" — nullable, but let's test with only key
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_unknown_op_raises():
    """Specifying a nonexistent named op raises ValueError."""
    app = create_app(NAMED_OPS_CONFIG)
    await app.start()
    try:
        with pytest.raises(ValueError, match="Unknown op"):
            await app.execute("ops", {"id": "x"}, op="nonexistent")
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_multiple_sequential():
    """Multiple sequential executes all commit and are queryable."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        for i in range(10):
            result = await app.execute("add", {"id": f"seq{i}", "val": f"v{i}"})
            assert result["ok"] is True

        rows = await app.query("SELECT COUNT(*) as cnt FROM items")
        assert rows[0]["cnt"] == 10
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_concurrent():
    """Multiple concurrent executes all succeed."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        tasks = [
            app.execute("add", {"id": f"con{i}", "val": f"v{i}"})
            for i in range(20)
        ]
        results = await asyncio.gather(*tasks)
        assert all(r["ok"] for r in results)
        assert len({r["ref"] for r in results}) == 20  # all unique refs

        rows = await app.query("SELECT COUNT(*) as cnt FROM items")
        assert rows[0]["cnt"] == 20
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_ref_is_sortable():
    """Refs from sequential executes are lexicographically ordered."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        r1 = await app.execute("add", {"id": "rf1", "val": "a"})
        r2 = await app.execute("add", {"id": "rf2", "val": "b"})
        assert r1["ref"] < r2["ref"]
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_execute_fans_out_to_app_subscribe():
    """execute() triggers callbacks registered via app.subscribe()."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        events: list[ChangeEvent] = []

        async def on_change(event):
            events.append(event)

        unsub = await app.subscribe(["items"], on_change)
        await app.execute("add", {"id": "fas1", "val": "v"})
        await asyncio.sleep(0.05)

        assert len(events) == 1
        assert events[0].op == "insert"
        assert events[0].row["id"] == "fas1"
        unsub()
    finally:
        await app.stop()


# ---- Data facade: query ------------------------------------------------------


@pytest.mark.asyncio
async def test_query_returns_rows():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        await app.execute("add", {"id": "q1", "val": "a"})
        await app.execute("add", {"id": "q2", "val": "b"})

        rows = await app.query("SELECT id, val FROM items ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["id"] == "q1"
        assert rows[1]["id"] == "q2"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_query_with_params():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        await app.execute("add", {"id": "p1", "val": "yes"})
        await app.execute("add", {"id": "p2", "val": "no"})

        rows = await app.query("SELECT * FROM items WHERE val = ?", ("yes",))
        assert len(rows) == 1
        assert rows[0]["id"] == "p1"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_query_empty_result():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        rows = await app.query("SELECT * FROM items")
        assert rows == []
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_query_before_start_raises():
    app = create_app(MINIMAL_CONFIG)
    with pytest.raises(RuntimeError, match="not running"):
        await app.query("SELECT 1")


@pytest.mark.asyncio
async def test_query_returns_dicts():
    """Each row is a proper dict with column-name keys."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        await app.execute("add", {"id": "d1", "val": "v1"})
        rows = await app.query("SELECT id, val FROM items")
        assert isinstance(rows[0], dict)
        assert set(rows[0].keys()) == {"id", "val"}
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_query_with_named_params():
    """Query with dict params (named placeholders)."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        await app.execute("add", {"id": "np1", "val": "alpha"})
        await app.execute("add", {"id": "np2", "val": "beta"})

        rows = await app.query(
            "SELECT * FROM items WHERE val = :v", {"v": "beta"}
        )
        assert len(rows) == 1
        assert rows[0]["id"] == "np2"
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_query_bad_sql_raises():
    """Invalid SQL propagates the database error."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        with pytest.raises(Exception):
            await app.query("SELECT * FROM nonexistent_table")
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_query_aggregate():
    """Aggregate queries work and return the computed value."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        await app.execute("add", {"id": "ag1", "val": "x"})
        await app.execute("add", {"id": "ag2", "val": "x"})
        await app.execute("add", {"id": "ag3", "val": "y"})

        rows = await app.query(
            "SELECT val, COUNT(*) as cnt FROM items GROUP BY val ORDER BY val"
        )
        assert len(rows) == 2
        assert rows[0] == {"val": "x", "cnt": 2}
        assert rows[1] == {"val": "y", "cnt": 1}
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_query_sees_execute_writes():
    """query() on the read connection sees rows written by execute()."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        rows_before = await app.query("SELECT * FROM items")
        assert rows_before == []

        await app.execute("add", {"id": "qe1", "val": "v"})

        rows_after = await app.query("SELECT * FROM items")
        assert len(rows_after) == 1
    finally:
        await app.stop()


# ---- Data facade: subscribe --------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_receives_changes():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        events: list[ChangeEvent] = []

        async def on_change(event: ChangeEvent):
            events.append(event)

        unsub = await app.subscribe(["items"], on_change)

        await app.execute("add", {"id": "s1", "val": "v1"})
        await asyncio.sleep(0.05)

        assert len(events) == 1
        assert events[0].table == "items"
        assert events[0].row["id"] == "s1"

        unsub()
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_subscribe_unsub_stops_events():
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        events: list[ChangeEvent] = []

        async def on_change(event: ChangeEvent):
            events.append(event)

        unsub = await app.subscribe(["items"], on_change)

        await app.execute("add", {"id": "u1", "val": "v1"})
        await asyncio.sleep(0.05)
        assert len(events) == 1

        unsub()

        await app.execute("add", {"id": "u2", "val": "v2"})
        await asyncio.sleep(0.05)
        assert len(events) == 1
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_subscribe_before_start_raises():
    app = create_app(MINIMAL_CONFIG)

    async def noop(event):
        pass

    with pytest.raises(RuntimeError, match="not running"):
        await app.subscribe(["items"], noop)


@pytest.mark.asyncio
async def test_subscribe_event_fields():
    """ChangeEvent has all expected fields populated correctly."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        events: list[ChangeEvent] = []

        async def on_change(event):
            events.append(event)

        unsub = await app.subscribe(["items"], on_change)
        result = await app.execute("add", {"id": "ef1", "val": "v1"})
        await asyncio.sleep(0.05)

        assert len(events) == 1
        e = events[0]
        assert e.table == "items"
        assert e.op == "insert"
        assert e.row["id"] == "ef1"
        assert e.row["val"] == "v1"
        assert e.ref == result["ref"]
        assert isinstance(e.raw_bytes, bytes)
        assert len(e.raw_bytes) > 0
        # raw_bytes should be valid JSON
        parsed = loads(e.raw_bytes)
        assert parsed["type"] == "update"
        assert parsed["table"] == "items"

        unsub()
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_subscribe_multiple_events():
    """Multiple writes produce multiple events in order."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        events: list[ChangeEvent] = []

        async def on_change(event):
            events.append(event)

        unsub = await app.subscribe(["items"], on_change)

        for i in range(5):
            await app.execute("add", {"id": f"me{i}", "val": f"v{i}"})
        await asyncio.sleep(0.1)

        assert len(events) == 5
        ids = [e.row["id"] for e in events]
        assert ids == [f"me{i}" for i in range(5)]

        unsub()
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_subscribe_multiple_subscribers():
    """Multiple subscribers on the same table each receive all events."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        events_a: list[ChangeEvent] = []
        events_b: list[ChangeEvent] = []

        async def on_a(event):
            events_a.append(event)

        async def on_b(event):
            events_b.append(event)

        unsub_a = await app.subscribe(["items"], on_a)
        unsub_b = await app.subscribe(["items"], on_b)

        await app.execute("add", {"id": "ms1", "val": "v"})
        await asyncio.sleep(0.05)

        assert len(events_a) == 1
        assert len(events_b) == 1
        assert events_a[0].row["id"] == "ms1"
        assert events_b[0].row["id"] == "ms1"

        unsub_a()
        unsub_b()
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_subscribe_unsub_one_of_many():
    """Unsubscribing one subscriber doesn't affect others."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        events_a: list[ChangeEvent] = []
        events_b: list[ChangeEvent] = []

        async def on_a(event):
            events_a.append(event)

        async def on_b(event):
            events_b.append(event)

        unsub_a = await app.subscribe(["items"], on_a)
        unsub_b = await app.subscribe(["items"], on_b)

        unsub_a()

        await app.execute("add", {"id": "uo1", "val": "v"})
        await asyncio.sleep(0.05)

        assert len(events_a) == 0
        assert len(events_b) == 1

        unsub_b()
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_subscribe_cleaned_up_on_stop():
    """Active subscriptions are cleaned up when the server stops."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()

    async def noop(event):
        pass

    await app.subscribe(["items"], noop)
    assert len(app._sub_tasks) == 1

    await app.stop()
    assert len(app._sub_tasks) == 0


@pytest.mark.asyncio
async def test_subscribe_with_ws_subscriber():
    """App.subscribe and WS subscriber both receive events from the same execute."""
    app = create_app(MINIMAL_CONFIG)
    await app.start()
    try:
        app_events: list[ChangeEvent] = []

        async def on_change(event):
            app_events.append(event)

        unsub = await app.subscribe(["items"], on_change)

        port = _get_port(app)
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                await ws.send_bytes(dumps({
                    "type": "subscribe",
                    "service": "live",
                    "protocol": "subpub",
                    "topic": "sw1",
                }))
                snap = loads((await ws.receive()).data)
                assert snap["type"] == "snapshot"

                await app.execute("add", {"id": "sw1", "val": "v"})

                update = loads((await asyncio.wait_for(ws.receive(), timeout=2.0)).data)
                assert update["type"] == "update"
                assert update["row"]["val"] == "v"

        await asyncio.sleep(0.05)
        assert len(app_events) == 1
        assert app_events[0].row["id"] == "sw1"

        unsub()
    finally:
        await app.stop()


# ---- ChangeEvent export ------------------------------------------------------


def test_change_event_importable():
    from mkio import ChangeEvent
    assert ChangeEvent is not None
    assert hasattr(ChangeEvent, "table")
    assert hasattr(ChangeEvent, "op")
    assert hasattr(ChangeEvent, "row")
    assert hasattr(ChangeEvent, "ref")


def test_change_event_is_frozen():
    """ChangeEvent is a frozen dataclass."""
    e = ChangeEvent(table="t", op="insert", row={"a": 1}, ref="r", raw_bytes=b"")
    with pytest.raises(AttributeError):
        e.table = "other"


def test_change_event_fields():
    """ChangeEvent stores all fields correctly."""
    row = {"id": "1", "val": "hello"}
    e = ChangeEvent(table="items", op="update", row=row, ref="ref123", raw_bytes=b'{"x":1}')
    assert e.table == "items"
    assert e.op == "update"
    assert e.row == row
    assert e.ref == "ref123"
    assert e.raw_bytes == b'{"x":1}'


def test_change_event_cause_and_old_default_to_none():
    """The version-move fields are optional, so 5-arg construction still works."""
    e = ChangeEvent(table="t", op="insert", row={"a": 1}, ref="r", raw_bytes=b"")
    assert e.cause is None
    assert e.old is None


def test_change_event_new_is_the_row_unless_deleted():
    """`new` is defined for every event, not only for cursor moves."""
    row = {"id": "1"}
    for op in ("insert", "update"):
        e = ChangeEvent(table="t", op=op, row=row, ref="r", raw_bytes=b"")
        assert e.new is row
    gone = ChangeEvent(table="t", op="delete", row=row, ref="r", raw_bytes=b"")
    assert gone.new is None
    assert gone.row is row      # a delete still names what went


def test_make_event_omits_cause_and_old_when_unset():
    """An ordinary write serializes exactly as it did before the fields existed."""
    from mkio._json import loads
    from mkio.change_bus import ChangeBus

    e = ChangeBus.make_event("items", "update", {"id": "1"}, "ref1")
    assert loads(e.raw_bytes) == {
        "type": "update", "table": "items", "op": "update",
        "row": {"id": "1"}, "ref": "ref1",
    }


def test_make_event_carries_cause_and_old_into_the_envelope():
    from mkio._json import loads
    from mkio.change_bus import ChangeBus

    e = ChangeBus.make_event(
        "items", "update", {"id": "1", "v": 2}, "ref2",
        cause="undo", old={"id": "1", "v": 3},
    )
    assert (e.cause, e.old) == ("undo", {"id": "1", "v": 3})
    assert e.new == {"id": "1", "v": 2}
    envelope = loads(e.raw_bytes)
    assert envelope["cause"] == "undo"
    assert envelope["old"] == {"id": "1", "v": 3}


def test_make_event_omits_old_alone_when_there_was_no_row():
    """A redo that rebuilds a row has a cause but no prior shape."""
    from mkio._json import loads
    from mkio.change_bus import ChangeBus

    e = ChangeBus.make_event("items", "insert", {"id": "1"}, "r", cause="redo")
    envelope = loads(e.raw_bytes)
    assert envelope["cause"] == "redo"
    assert "old" not in envelope
    assert e.old is None


# ---- Integration: combined features ------------------------------------------


@pytest.mark.asyncio
async def test_hooks_and_services_and_facade_together():
    """All features work together: hooks, custom service, execute, query, subscribe."""
    lifecycle = []
    events: list[ChangeEvent] = []

    class TrackingService(Service):
        async def start(self):
            lifecycle.append("service_started")

        async def stop(self):
            lifecycle.append("service_stopped")

    async def on_startup():
        lifecycle.append("startup_hook")

    async def on_shutdown():
        lifecycle.append("shutdown_hook")

    async def on_change(event):
        events.append(event)

    app = create_app(MINIMAL_CONFIG)
    app.add_service("tracker", TrackingService)
    app.on_startup(on_startup)
    app.on_shutdown(on_shutdown)
    await app.start()
    try:
        assert "service_started" in lifecycle
        assert "startup_hook" in lifecycle

        unsub = await app.subscribe(["items"], on_change)

        await app.execute("add", {"id": "int1", "val": "v1"})
        await app.execute("add", {"id": "int2", "val": "v2"})
        await asyncio.sleep(0.05)

        rows = await app.query("SELECT * FROM items ORDER BY id")
        assert len(rows) == 2

        assert len(events) == 2

        unsub()
    finally:
        await app.stop()

    assert "shutdown_hook" in lifecycle
    assert "service_stopped" in lifecycle


@pytest.mark.asyncio
async def test_execute_from_startup_hook():
    """Can call execute() from a startup hook (server is running at that point)."""
    app = create_app(MINIMAL_CONFIG)

    async def seed():
        await app.execute("add", {"id": "seed1", "val": "initial"})

    app.on_startup(seed)
    await app.start()
    try:
        rows = await app.query("SELECT * FROM items WHERE id = 'seed1'")
        assert len(rows) == 1
        assert rows[0]["val"] == "initial"
    finally:
        await app.stop()
