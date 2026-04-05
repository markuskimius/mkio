"""Server: aiohttp app wiring, WS dispatch, static serving, routing."""

from __future__ import annotations

import asyncio
import importlib
from collections import defaultdict
from pathlib import Path
from typing import Any

from aiohttp import web

from mkio._json import dumps, loads
from mkio.change_bus import ChangeBus
from mkio.config import load_config
from mkio.database import Database
from mkio.services.base import Service
from mkio.services.query import QueryService
from mkio.services.stream import StreamService
from mkio.services.subpub import SubPubService
from mkio.services.transaction import TransactionService
from mkio.writer import WriteBatcher
from mkio.ws_protocol import make_error, parse_message

SERVICE_TYPES: dict[str, type[Service]] = {
    "transaction": TransactionService,
    "subpub": SubPubService,
    "stream": StreamService,
    "query": QueryService,
}


def serve(config: str | Path | dict[str, Any]) -> None:
    """Entry point. Blocks until shutdown.

    Args:
        config: Path to TOML file, or config dict.
    """
    cfg = load_config(config)

    # Try uvloop
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

    app = web.Application()
    app["config"] = cfg

    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)

    # API routes
    app.router.add_get("/api/services", _api_services)

    # WebSocket routes
    app.router.add_get("/ws", _ws_handler)
    app.router.add_get("/ws/{service_name}", _ws_handler)

    # Serve JS client library
    js_path = Path(__file__).parent / "client" / "mkio.js"
    if js_path.exists():
        async def serve_js(request: web.Request) -> web.FileResponse:
            return web.FileResponse(
                js_path, headers={"Content-Type": "application/javascript"}
            )
        app.router.add_get("/mkio.js", serve_js)

    # Static file routes
    for route, directory in cfg.get("static", {}).items():
        path = Path(directory).resolve()
        if route == "/":
            app.router.add_get("/", _make_index_handler(path))
            app.router.add_static("/static", path)
        else:
            app.router.add_static(route, path)

    web.run_app(app, host=cfg.get("host", "0.0.0.0"), port=cfg.get("port", 8080))


def _make_index_handler(static_path: Path):
    async def handler(request: web.Request) -> web.FileResponse:
        return web.FileResponse(static_path / "index.html")
    return handler


async def _api_services(request: web.Request) -> web.Response:
    """Return JSON list of available services."""
    services: dict[str, Service] = request.app.get("services", {})
    result = []
    for name, svc in services.items():
        info: dict[str, Any] = {
            "name": name,
            "type": svc.config.get("type", "unknown"),
        }
        # Include useful metadata per service type
        if "primary_table" in svc.config:
            info["primary_table"] = svc.config["primary_table"]
        if "watch_tables" in svc.config:
            info["watch_tables"] = svc.config["watch_tables"]
        if "ops" in svc.config:
            ops = svc.config["ops"]
            if isinstance(ops, dict):
                # Named ops: collect tables from all op sets
                tables: set[str] = set()
                for op_list in ops.values():
                    tables.update(op["table"] for op in op_list)
                info["tables"] = list(tables)
                info["ops"] = list(ops.keys())
            else:
                info["tables"] = list({op["table"] for op in ops})
        result.append(info)
    return web.json_response(result)


async def _on_startup(app: web.Application) -> None:
    cfg = app["config"]

    # Monitors: service_name -> set of WebSocketResponse
    app.setdefault("monitors", defaultdict(set))

    # Database
    db = Database(
        path=cfg.get("db_path", "mkio.db"),
        tables=cfg.get("tables", {}),
        config=cfg,
    )
    await db.start()
    app["db"] = db

    # Change bus
    bus = ChangeBus()
    app["bus"] = bus

    # Writer
    writer = WriteBatcher(
        db=db,
        change_bus=bus,
        batch_max_size=cfg.get("batch_max_size", 500),
        batch_max_wait_ms=cfg.get("batch_max_wait_ms", 2.0),
    )
    await writer.start()
    app["writer"] = writer

    # Services
    services: dict[str, Service] = {}
    for svc_name, svc_config in cfg.get("services", {}).items():
        svc_type = svc_config.get("type", "")

        if svc_type in SERVICE_TYPES:
            cls = SERVICE_TYPES[svc_type]
        elif isinstance(svc_type, type) and issubclass(svc_type, Service):
            cls = svc_type
        elif isinstance(svc_type, str) and "." in svc_type:
            module_path, cls_name = svc_type.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            cls = getattr(mod, cls_name)
        else:
            raise ValueError(f"Unknown service type: {svc_type!r} for service '{svc_name}'")

        svc = cls(config=svc_config, db=db, change_bus=bus, writer=writer)
        svc.name = svc_name
        svc._monitor_notifier = lambda sn, d, data, _app=app: _notify_monitors(_app, sn, d, data)
        await svc.start()
        services[svc_name] = svc

    app["services"] = services


async def _on_shutdown(app: web.Application) -> None:
    # 1. Stop services
    for svc in app.get("services", {}).values():
        await svc.stop()

    # 2. Drain writer (commit all queued writes)
    writer: WriteBatcher | None = app.get("writer")
    if writer:
        await writer.stop(drain=True)

    # 3. Checkpoint WAL and close database
    db: Database | None = app.get("db")
    if db:
        await db.stop()


async def _notify_monitors(
    app: web.Application,
    service_name: str,
    direction: str,
    data: dict[str, Any] | bytes,
) -> None:
    """Send a monitor envelope to all monitors watching a service."""
    monitors: set[web.WebSocketResponse] = app["monitors"].get(service_name, set())
    if not monitors:
        return
    # Build the monitor envelope
    payload = data if isinstance(data, dict) else loads(data)
    envelope = dumps({"direction": direction, "service": service_name, "message": payload})
    dead = []
    for mon_ws in monitors:
        try:
            await mon_ws.send_bytes(envelope)
        except (ConnectionError, RuntimeError):
            dead.append(mon_ws)
    for d in dead:
        monitors.discard(d)


async def _ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    services: dict[str, Service] = request.app["services"]
    monitors: dict[str, set[web.WebSocketResponse]] = request.app["monitors"]

    # Per-service endpoint pre-fills the service name
    url_service_name = request.match_info.get("service_name")

    # Track active subscriptions and monitor registrations for cleanup
    subscribed: dict[str, Service] = {}
    monitoring: set[str] = set()

    try:
        async for ws_msg in ws:
            if ws_msg.type in (web.WSMsgType.TEXT, web.WSMsgType.BINARY):
                raw = ws_msg.data
            else:
                continue

            try:
                msg = parse_message(raw)
            except (ValueError, Exception) as e:
                await ws.send_bytes(make_error(None, str(e)))
                continue

            service_name = url_service_name or msg.get("service")
            ref = msg.get("ref")
            msg_type = msg.get("type", "")

            # Handle monitor requests — no service required for "list"
            if msg_type == "monitor":
                target = service_name
                if not target:
                    await ws.send_bytes(make_error(ref, "Missing 'service' field"))
                    continue
                if target not in services:
                    await ws.send_bytes(make_error(ref, f"Unknown service: {target}"))
                    continue
                monitors[target].add(ws)
                monitoring.add(target)
                ack = {"type": "monitor_ack", "service": target}
                if ref:
                    ack["ref"] = ref
                await ws.send_bytes(dumps(ack))
                continue

            if not service_name:
                await ws.send_bytes(make_error(ref, "Missing 'service' field"))
                continue

            svc = services.get(service_name)
            if svc is None:
                await ws.send_bytes(make_error(ref, f"Unknown service: {service_name}"))
                continue

            # Notify monitors of inbound message
            await _notify_monitors(request.app, service_name, "in", msg)

            if msg_type == "subscribe":
                await svc.on_subscribe(ws, msg)
                subscribed[service_name] = svc
            elif msg_type == "unsubscribe":
                if service_name in subscribed:
                    await svc.on_unsubscribe(ws, msg)
                    del subscribed[service_name]
            else:
                await svc.on_message(ws, msg)

    except (asyncio.CancelledError, ConnectionError):
        pass
    finally:
        # Unsubscribe from all services on disconnect
        for svc_name, svc in subscribed.items():
            try:
                await svc.on_unsubscribe(ws, {"type": "unsubscribe", "service": svc_name})
            except Exception:
                pass
        # Remove from monitor sets
        for svc_name in monitoring:
            monitors[svc_name].discard(ws)

    return ws
