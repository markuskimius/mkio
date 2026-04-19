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
from mkio.migration import _parse_config_columns
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
    app.router.add_get("/api/services/{service_name}", _api_service_detail)

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

    web.run_app(
        app,
        host=cfg.get("host", "0.0.0.0"),
        port=cfg.get("port", 8080),
        shutdown_timeout=cfg.get("shutdown_timeout", 0),
    )


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
            "protocol": svc.config.get("protocol", "unknown"),
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


async def _api_service_detail(request: web.Request) -> web.Response:
    """Return detailed usage info for a single service."""
    service_name = request.match_info["service_name"]
    services: dict[str, Service] = request.app.get("services", {})
    svc = services.get(service_name)
    if svc is None:
        return web.json_response(
            {"error": f"Unknown service: {service_name}"}, status=404
        )

    tables = request.app["config"].get("tables", {})
    return web.json_response(
        _build_service_detail(service_name, svc.config, tables)
    )


def _build_service_detail(
    name: str, config: dict[str, Any], tables: dict[str, dict]
) -> dict[str, Any]:
    """Build detailed service info from config and table schemas."""
    svc_type = config.get("protocol", "unknown")
    detail: dict[str, Any] = {"name": name, "protocol": svc_type}

    desc = config.get("description")
    if desc:
        detail["description"] = desc

    if svc_type == "transaction":
        detail["ops"] = _build_transaction_ops(name, config, tables)
        detail["recovery"] = {
            "description": (
                "Each result includes a ref string. To check if a transaction "
                "committed after a disconnect, send a check message with that ref."
            ),
            "check_message": {"service": name, "type": "check", "ref": "<ref>"},
        }
    else:
        detail.update(_build_listener_detail(name, config, tables))

    return detail


def _build_transaction_ops(
    name: str, config: dict[str, Any], tables: dict[str, dict]
) -> dict[str, Any]:
    """Build op detail for transaction services."""
    raw_ops = config.get("ops", [])
    descriptions = config.get("descriptions", {})
    result: dict[str, Any] = {}

    if isinstance(raw_ops, dict):
        op_sets = raw_ops
    else:
        op_sets = {"default": raw_ops}

    for op_name, op_list in op_sets.items():
        op_info: dict[str, Any] = {}
        op_desc = descriptions.get(op_name)
        if op_desc:
            op_info["description"] = op_desc

        steps = []
        for step_idx, spec in enumerate(op_list):
            step = _build_op_step(spec, tables)
            steps.append(step)
        op_info["steps"] = steps

        # Build example from the first step's required + optional fields
        example = _build_send_example(name, op_name, op_list, tables)
        if example:
            op_info["example"] = example

        result[op_name] = op_info

    return result


def _build_op_step(spec: dict[str, Any], tables: dict[str, dict]) -> dict[str, Any]:
    """Build detail for a single op step (insert/update/delete/upsert)."""
    table_name = spec["table"]
    op_type = spec["op_type"]
    fields = spec.get("fields", [])
    key = spec.get("key", [])
    raw_bind = spec.get("bind", {})
    op_defaults = spec.get("defaults", {})

    step: dict[str, Any] = {"table": table_name, "op_type": op_type}

    # Parse table schema
    table_config = tables.get(table_name, {})
    col_defs = table_config.get("columns", {})
    parsed_cols = _parse_config_columns(col_defs) if col_defs else {}

    # Client-provided fields (from fields + key, excluding those fully defaulted)
    client_fields = list(fields) + [k for k in key if k not in fields]
    fields_info: dict[str, Any] = {}
    for f in client_fields:
        col = parsed_cols.get(f, {})
        info: dict[str, Any] = {"type": col.get("type", "TEXT")}
        if f in key:
            info["key"] = True
            info["required"] = True
        elif f in op_defaults:
            # Op provides a default — client can override but doesn't have to
            info["required"] = False
            info["default"] = op_defaults[f]
        elif col.get("notnull") and col.get("dflt_value") is None and not col.get("pk"):
            info["required"] = True
        else:
            info["required"] = False
            if col.get("dflt_value") is not None:
                info["default"] = col["dflt_value"]
        fields_info[f] = info
    if fields_info:
        step["fields"] = fields_info

    # Auto-generated columns (not in fields, key, bind, or op defaults)
    all_client = set(fields) | set(key) | set(raw_bind.keys()) | set(op_defaults.keys())
    auto_info: dict[str, Any] = {}
    for col_name, col in parsed_cols.items():
        if col_name not in all_client:
            info = {"type": col.get("type", "TEXT")}
            if col.get("pk"):
                if "AUTOINCREMENT" in col_defs.get(col_name, "").upper():
                    info["source"] = "autoincrement"
                else:
                    info["source"] = "primary_key"
            elif col.get("dflt_value") is not None:
                info["source"] = "default"
                info["default"] = col["dflt_value"]
            auto_info[col_name] = info
    # Op-level defaults for columns not in fields/key go into auto
    for col_name, val in op_defaults.items():
        if col_name not in fields and col_name not in key:
            col = parsed_cols.get(col_name, {})
            auto_info[col_name] = {
                "type": col.get("type", "TEXT"),
                "source": "op_default",
                "default": val,
            }
    if auto_info:
        step["auto"] = auto_info

    # Bind references
    if raw_bind:
        step["bind"] = dict(raw_bind)

    return step


def _build_listener_detail(
    name: str, config: dict[str, Any], tables: dict[str, dict]
) -> dict[str, Any]:
    """Build detail for listener services (subpub/query/stream)."""
    detail: dict[str, Any] = {}

    primary_table = config.get("primary_table")
    if primary_table:
        detail["primary_table"] = primary_table

    topic_field = config.get("topic")
    if topic_field:
        detail["topic"] = topic_field

    svc_type = config.get("protocol")
    filterable = config.get("filterable", [])
    if filterable and svc_type != "subpub":
        detail["filterable"] = filterable

    # Table schema
    if primary_table and primary_table in tables:
        col_defs = tables[primary_table].get("columns", {})
        parsed = _parse_config_columns(col_defs)
        schema: dict[str, Any] = {}
        for col_name, col in parsed.items():
            info: dict[str, Any] = {"type": col.get("type", "TEXT")}
            if col.get("pk"):
                info["pk"] = True
            schema[col_name] = info
        detail["schema"] = schema

    # Subscribe protocol info
    subscribe: dict[str, Any] = {
        "message": {
            "service": name,
            "type": "subscribe",
        },
    }
    if svc_type == "subpub":
        subscribe["message"]["topic"] = "<topic_value>"
        subscribe["response_types"] = ["snapshot", "update"]
        subscribe["topic"] = config.get("topic", "")
    elif svc_type == "stream":
        subscribe["recovery"] = (
            "Send ref from last received message to resume from that point in the buffer. "
            "If ref is too old (beyond buffer), the full buffer is sent as a snapshot."
        )
        subscribe["response_types"] = ["snapshot", "update"]
        subscribe["buffer_size"] = config.get("buffer_size", 10000)
    elif svc_type == "query":
        subscribe["response_types"] = ["snapshot", "update"]

    filterable = config.get("filterable", [])
    if filterable and svc_type != "subpub":
        subscribe["message"]["filter"] = "<expr>"
        subscribe["filter_fields"] = filterable

    detail["subscribe"] = subscribe

    # Examples
    cli_cmd = svc_type if svc_type in ("subpub", "stream", "query") else "subpub"
    example: dict[str, str] = {}
    if svc_type == "subpub":
        topic_field = config.get("topic", "id")
        example["subscribe"] = f"mkio {cli_cmd} <url> {name} <{topic_field}>"
    else:
        example["subscribe"] = f"mkio {cli_cmd} <url> {name}"
    if filterable and svc_type != "subpub":
        f = filterable[0]
        example["subscribe_filter"] = (
            f"mkio {cli_cmd} <url> {name} --filter \"{f} == '...'\""
        )
    if svc_type == "stream":
        example["subscribe_recover"] = (
            f"mkio {cli_cmd} <url> {name} --ref \"<ref from last message>\""
        )
    detail["example"] = example

    return detail


def _build_send_example(
    svc_name: str, op_name: str, op_list: list[dict], tables: dict[str, dict]
) -> str | None:
    """Build an example mkio send command from the first step's fields."""
    if not op_list:
        return None

    spec = op_list[0]
    fields = spec.get("fields", [])
    key = spec.get("key", [])
    op_defaults = spec.get("defaults", {})
    table_name = spec["table"]
    col_defs = tables.get(table_name, {}).get("columns", {})
    parsed = _parse_config_columns(col_defs) if col_defs else {}

    example_data: dict[str, Any] = {}
    for f in key:
        example_data[f] = "..."
    for f in fields:
        if f in op_defaults:
            continue  # Skip fields with op-level defaults
        col = parsed.get(f, {})
        col_type = col.get("type", "TEXT")
        if col_type in ("INTEGER", "INT"):
            example_data[f] = 0
        elif col_type == "REAL":
            example_data[f] = 0.0
        else:
            example_data[f] = "..."

    if not example_data:
        return None

    import json
    data_str = json.dumps(example_data)
    op_flag = f" --op {op_name}" if op_name != "default" else ""
    return f"mkio send <url> {svc_name}{op_flag} '{data_str}'"


async def _on_startup(app: web.Application) -> None:
    cfg = app["config"]

    # Monitors: service_name -> set of WebSocketResponse
    app.setdefault("monitors", defaultdict(set))

    # Track all active WebSocket connections for clean shutdown
    app.setdefault("websockets", set())

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
        svc_type = svc_config.get("protocol", "")

        if svc_type in SERVICE_TYPES:
            cls = SERVICE_TYPES[svc_type]
        elif isinstance(svc_type, type) and issubclass(svc_type, Service):
            cls = svc_type
        elif isinstance(svc_type, str) and "." in svc_type:
            module_path, cls_name = svc_type.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            cls = getattr(mod, cls_name)
        else:
            raise ValueError(f"Unknown protocol: {svc_type!r} for service '{svc_name}'")

        svc = cls(config=svc_config, db=db, change_bus=bus, writer=writer)
        svc.name = svc_name
        svc._monitor_notifier = lambda sn, d, data, _app=app: _notify_monitors(_app, sn, d, data)
        await svc.start()
        services[svc_name] = svc

    app["services"] = services


async def _on_shutdown(app: web.Application) -> None:
    # 0. Close all WebSocket connections so handlers can exit
    for ws in set(app.get("websockets", set())):
        await ws.close()

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
    request.app["websockets"].add(ws)

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
            msgid = msg.get("msgid")
            msg_type = msg.get("type", "")

            # Handle monitor requests — no service required for "list"
            if msg_type == "monitor":
                target = service_name
                if not target:
                    await ws.send_bytes(make_error(ref, "Missing 'service' field", msgid=msgid))
                    continue
                if target not in services:
                    await ws.send_bytes(make_error(ref, f"Unknown service: {target}", msgid=msgid))
                    continue
                monitors[target].add(ws)
                monitoring.add(target)
                ack = {"type": "monitor_ack", "service": target}
                if ref:
                    ack["ref"] = ref
                await ws.send_bytes(dumps(ack))
                continue

            if not service_name:
                await ws.send_bytes(make_error(ref, "Missing 'service' field", msgid=msgid))
                continue

            svc = services.get(service_name)
            if svc is None:
                await ws.send_bytes(make_error(ref, f"Unknown service: {service_name}", msgid=msgid))
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
        request.app["websockets"].discard(ws)

    return ws
