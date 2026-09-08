"""Server: aiohttp app wiring, WS dispatch, static serving, routing."""

from __future__ import annotations

import asyncio
import importlib
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web

from mkio._json import dumps, loads
from mkio._ref import next_ref
from mkio.change_bus import ChangeBus
from mkio.database import Database
from mkio.history import effective_tables, history_specs, versioned_tables
from mkio.services.base import Service
from mkio.services.info import InfoService
from mkio.services.query import QueryService
from mkio.services.reqrep import ReqRepService
from mkio.services.stream import StreamService
from mkio.services.subpub import SubPubService
from mkio.services.transaction import TransactionService
from mkio.writer import WriteBatcher
from mkio.migration import _parse_config_columns
from mkio.ws_protocol import make_error, make_nack, parse_message

log = logging.getLogger("mkio.server")

SERVICE_TYPES: dict[str, type[Service]] = {
    "transaction": TransactionService,
    "subpub": SubPubService,
    "stream": StreamService,
    "query": QueryService,
    "reqrep": ReqRepService,
}


def serve(config: str | Path | dict[str, Any]) -> None:
    """Entry point. Blocks until shutdown.

    Args:
        config: Path to TOML file, or config dict.
    """
    from mkio.app import create_app
    app = create_app(config)
    app.run()


def _make_index_handler(static_path: Path):
    async def handler(request: web.Request) -> web.FileResponse:
        return web.FileResponse(static_path / "index.html")
    return handler


def _make_config_handler(config_path: Path):
    import tomllib as _tomllib

    async def handler(request: web.Request) -> web.Response:
        rel = request.match_info["path"]
        if "\x00" in rel:
            raise web.HTTPForbidden()
        resolved = (config_path / rel).resolve()
        if not str(resolved).startswith(str(config_path)):
            raise web.HTTPForbidden()

        if resolved.suffix == ".json":
            toml_path = resolved.with_suffix(".toml")
            if toml_path.is_file():
                with open(toml_path, "rb") as f:
                    data = _tomllib.load(f)
                return web.Response(
                    body=dumps(data),
                    content_type="application/json",
                )
            if resolved.is_file():
                return web.FileResponse(resolved)
            raise web.HTTPNotFound()

        if resolved.is_file():
            return web.FileResponse(resolved)
        raise web.HTTPNotFound()

    return handler


async def _api_services(request: web.Request) -> web.Response:
    """Return JSON list of available services."""
    services: dict[str, Service] = request.app.get("services", {})
    result = []
    for name, svc in services.items():
        if name.startswith("_"):
            continue
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
    if svc is None or service_name.startswith("_"):
        return web.json_response(
            {"error": f"Unknown service: {service_name}"}, status=404
        )

    # History tables included, so a service configured on one still reports
    # its columns and examples.
    tables = effective_tables(request.app["config"])
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
    elif svc_type == "reqrep":
        detail.update(_build_reqrep_detail(name, config))
    else:
        detail.update(_build_listener_detail(name, config, tables))

    return detail


def _build_reqrep_detail(
    name: str, config: dict[str, Any]
) -> dict[str, Any]:
    """Build detail for reqrep services."""
    import json
    import re

    from mkio.expr import field_refs, function_refs, numeric_fields, parse

    detail: dict[str, Any] = {}
    sql = config.get("sql")
    reply = config.get("reply")
    params_cfg = config.get("params")

    if sql:
        detail["sql"] = sql
        sql_params = re.findall(r":(\w+)", sql)
        if sql_params:
            detail["parameters"] = sql_params
    if params_cfg:
        detail["params"] = params_cfg
    if reply:
        detail["reply"] = reply

    # Reply shape
    if sql and isinstance(reply, dict):
        detail["reply_shape"] = "rows (transformed)"
    elif sql and isinstance(reply, str):
        detail["reply_shape"] = "value (scalar from SQL)"
    elif sql:
        detail["reply_shape"] = "rows"
    elif isinstance(reply, dict):
        detail["reply_shape"] = "row (single record)"
    elif isinstance(reply, str):
        detail["reply_shape"] = "value (scalar)"

    # Discover input fields from expressions
    input_fields: set[str] = set()
    num_fields: set[str] = set()
    functions: set[str] = set()
    if params_cfg:
        for expr_str in params_cfg.values():
            ast = parse(str(expr_str))
            input_fields |= field_refs(ast)
            num_fields |= numeric_fields(ast)
            functions |= function_refs(ast)
    elif not sql and reply:
        exprs = [reply] if isinstance(reply, str) else list(reply.values())
        for expr_str in exprs:
            ast = parse(str(expr_str))
            input_fields |= field_refs(ast)
            num_fields |= numeric_fields(ast)
            functions |= function_refs(ast)

    if input_fields:
        detail["input_fields"] = sorted(input_fields)
    if functions:
        detail["functions"] = sorted(functions)

    # Build example data from SQL params or input fields
    example_data: dict[str, Any] = {}
    for p in detail.get("parameters", []):
        example_data[p] = "..."
    if not example_data:
        for f in detail.get("input_fields", []):
            example_data[f] = 0 if f in num_fields else "..."

    detail["request"] = {
        "message": {
            "service": name,
            "type": "request",
            "reqid": "<reqid>",
            "data": example_data or {"...": "..."},
        },
        "reply_type": "reply",
    }

    data_str = json.dumps(example_data) if example_data else "{}"
    detail["example"] = {
        "request": f"mkio reqrep <url> {name} '{data_str}'",
    }

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
            "protocol": svc_type,
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
        subscribe["message"]["before"] = "(optional, bool) return rows before ref instead of after"
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
        example["subscribe_before"] = (
            f"mkio {cli_cmd} <url> {name} --before --ref \"<ref>\" --maxcount 20"
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


async def _preflight_services(cfg: dict[str, Any]) -> None:
    """Validate service startup before entering the main event loop.

    Opens a temporary DB connection and starts each service to surface
    errors (e.g. bad SQL) that would otherwise be swallowed by asyncio.
    """
    db = Database(
        path=cfg.get("db_path", "mkio.db"),
        tables=cfg.get("tables", {}),
        config=cfg,
        skip_migration=True,
    )
    await db.start()
    bus = ChangeBus()
    writer = WriteBatcher(
        db=db, change_bus=bus, batch_max_size=1, batch_max_wait_ms=1000,
        versioned=history_specs(cfg),
        versioned_configs=versioned_tables(cfg),
    )
    await writer.start()

    services: list[Service] = []
    try:
        for svc_name, svc_config in cfg.get("services", {}).items():
            svc_type = svc_config.get("protocol", "")
            if svc_type not in SERVICE_TYPES:
                continue
            cls = SERVICE_TYPES[svc_type]
            svc = cls(config=svc_config, db=db, change_bus=bus, writer=writer)
            svc.name = svc_name
            svc._monitor_notifier = lambda *a, **k: None
            try:
                await svc.start()
            except Exception as exc:
                print(f"Error starting service '{svc_name}': {exc}", file=sys.stderr)
                raise SystemExit(1) from exc
            services.append(svc)
    finally:
        for svc in services:
            await svc.stop()
        await writer.stop(drain=False)
        await db.stop()


async def _on_startup(app: web.Application) -> None:
    cfg = app["config"]
    started_ref = next_ref()
    started_monotonic = time.monotonic()

    # Monitors: service_name -> set of WebSocketResponse
    app.setdefault("monitors", defaultdict(set))

    # Track all active WebSocket connections for clean shutdown
    app.setdefault("websockets", set())

    # Database (migration already ran in serve() before the event loop)
    db = Database(
        path=cfg.get("db_path", "mkio.db"),
        tables=cfg.get("tables", {}),
        config=cfg,
        skip_migration=True,
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
        versioned=history_specs(cfg),
        versioned_configs=versioned_tables(cfg),
    )
    await writer.start()
    app["writer"] = writer

    # Services
    services: dict[str, Service] = {}
    app["services"] = services
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
            from mkio.config import _VALID_PROTOCOLS
            available = ", ".join(sorted(_VALID_PROTOCOLS))
            raise ValueError(
                f"Unknown protocol: {svc_type!r} for service '{svc_name}'. "
                f"Valid protocols: {available}"
            )

        svc = cls(config=svc_config, db=db, change_bus=bus, writer=writer)
        svc.name = svc_name
        svc._monitor_notifier = lambda sn, d, data, _app=app: _notify_monitors(_app, sn, d, data)
        await svc.start()
        services[svc_name] = svc

    # Programmatically registered services
    mkio_app = app.get("mkio_app")
    if mkio_app is not None:
        for svc_name, cls, svc_config in mkio_app._pending_services:
            svc = cls(config=svc_config, db=db, change_bus=bus, writer=writer)
            svc.name = svc_name
            svc._monitor_notifier = lambda sn, d, data, _app=app: _notify_monitors(_app, sn, d, data)
            await svc.start()
            services[svc_name] = svc

    # Built-in _mkio service
    if "_mkio" in services:
        logging.getLogger("mkio").warning(
            "User service '_mkio' overridden by built-in _mkio service"
        )
    info_svc = InfoService(config={"protocol": "reqrep"}, db=db, change_bus=bus, writer=writer)
    info_svc.name = "_mkio"
    info_svc._server_config = cfg
    info_svc._server_services = services
    info_svc._started_ref = started_ref
    info_svc._started_monotonic = started_monotonic
    info_svc._monitor_notifier = lambda sn, d, data, _app=app: _notify_monitors(_app, sn, d, data)
    services["_mkio"] = info_svc

    # Auth: load rights cache if auth is enabled
    if cfg.get("auth"):
        from mkio.auth import load_rights_cache
        rights_cache = await load_rights_cache(db)
        app["rights_cache"] = rights_cache

        async def _rights_listener():
            try:
                while True:
                    await rights_q.get()
                    new_cache = await load_rights_cache(db)
                    rights_cache._rights = new_cache._rights
            except asyncio.CancelledError:
                pass

        rights_q = bus.subscribe(["_mkio_rights"])
        app["_rights_listener"] = asyncio.create_task(_rights_listener())

    # User startup hooks
    if mkio_app is not None:
        for hook in mkio_app._startup_hooks:
            await hook()


async def _on_shutdown(app: web.Application) -> None:
    # 0. User shutdown hooks (before services stop)
    mkio_app = app.get("mkio_app")
    if mkio_app is not None:
        for hook in mkio_app._shutdown_hooks:
            await hook()

    # 0b. Stop rights cache listener
    rights_task = app.get("_rights_listener")
    if rights_task is not None:
        rights_task.cancel()
        try:
            await rights_task
        except asyncio.CancelledError:
            pass

    # 1. Close all WebSocket connections so handlers can exit
    wss = set(app.get("websockets", set()))
    if wss:
        await asyncio.wait(
            [asyncio.create_task(ws.close(code=aiohttp.WSCloseCode.GOING_AWAY, message=b"server shutdown")) for ws in wss],
            timeout=2.0,
        )

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
    all_monitors = app["monitors"]
    targets: set[web.WebSocketResponse] = set()
    targets.update(all_monitors.get(service_name, set()))
    targets.update(all_monitors.get("*", set()))
    if not targets:
        return
    # Build the monitor envelope
    payload = data if isinstance(data, dict) else loads(data)
    envelope = dumps({"direction": direction, "service": service_name, "message": payload})
    dead = []
    for mon_ws in targets:
        try:
            await mon_ws.send_bytes(envelope)
        except (ConnectionError, RuntimeError):
            dead.append(mon_ws)
    for d in dead:
        all_monitors.get(service_name, set()).discard(d)
        all_monitors.get("*", set()).discard(d)


async def _handle_auth(
    app: web.Application, ws: web.WebSocketResponse, msg: dict[str, Any]
) -> dict[str, Any] | None:
    """Handle an auth message. Returns auth info dict on success, None on failure."""
    data = msg.get("data", {})
    mkio_app = app.get("mkio_app")
    await _notify_monitors(app, "_auth", "in", msg)
    try:
        if mkio_app is not None and mkio_app._auth_handler is not None:
            auth_info = await mkio_app._auth_handler(data)
        elif app["config"].get("auth_builtin"):
            from mkio.auth import authenticate_builtin
            auth_info = await authenticate_builtin(app["db"], data)
        else:
            resp = {"type": "auth", "ok": False, "message": "no auth provider configured"}
            await ws.send_bytes(dumps(resp))
            await _notify_monitors(app, "_auth", "out", resp)
            return None
        resp = {"type": "auth", "ok": True, "user": auth_info.get("user", ""), "role": auth_info.get("role", "")}
        await ws.send_bytes(dumps(resp))
        await _notify_monitors(app, "_auth", "out", resp)
        return auth_info
    except Exception as exc:
        resp = {"type": "auth", "ok": False, "message": str(exc) if str(exc) else "authentication failed"}
        await asyncio.sleep(1)
        await ws.send_bytes(dumps(resp))
        await _notify_monitors(app, "_auth", "out", resp)
        return None


async def _check_service_access(
    app: web.Application,
    ws: web.WebSocketResponse,
    service_name: str,
    msg: dict[str, Any],
    msg_type: str,
) -> bytes | None:
    """Check access control. Returns nack bytes if denied, None if allowed."""
    from mkio.auth import check_access, build_when_params, execute_when_check

    rights_cache = app.get("rights_cache")
    if rights_cache is None:
        return None

    auth_info = getattr(ws, "_mkio_auth", None)
    services = app["services"]
    svc = services.get(service_name)
    if svc is None:
        return None

    # The built-in identity service gates itself: any authenticated user gets
    # the full reply; before authentication it answers with identity fields
    # only (see InfoService), so `mkio check` and client verification work
    # against auth-enabled servers.
    if service_name == "_mkio":
        return None

    svc_config = svc.config
    ref = msg.get("ref")
    txnid = msg.get("txnid")
    subid = msg.get("subid")
    reqid = msg.get("reqid")

    # Determine the access config: op-level overrides service-level for transactions
    access_config = svc_config.get("access")
    op_name = msg.get("op")
    if svc_config.get("protocol") == "transaction" and op_name:
        op_access = svc_config.get("_op_access", {})
        if op_name in op_access:
            access_config = op_access[op_name]

    def _deny(message: str) -> bytes:
        if msg_type == "subscribe":
            return make_nack(service_name, message, ref=ref, txnid=txnid, subid=subid)
        # Requests correlate on reqid — an error without it would leave the
        # caller's future pending forever.
        return make_error(ref, message, txnid=txnid, reqid=reqid, service=service_name)

    if access_config is None:
        if auth_info is None:
            return _deny("authentication required")
        return _deny("permission denied")

    allowed, when_sql = check_access(access_config, auth_info, rights_cache)
    if not allowed:
        message = "authentication required" if auth_info is None else "permission denied"
        return _deny(message)

    if when_sql is not None:
        params = build_when_params(auth_info, msg)
        db = app["db"]
        try:
            passed = await execute_when_check(db, when_sql, params)
        except Exception as exc:
            log.warning(f"Auth when check failed for {service_name}: {exc}")
            return _deny("permission denied")
        if not passed:
            return _deny("permission denied")

    return None


async def _ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    request.app["websockets"].add(ws)

    # Connect hooks
    mkio_app = request.app.get("mkio_app")
    if mkio_app is not None:
        for hook in mkio_app._connect_hooks:
            await hook(ws)

    services: dict[str, Service] = request.app["services"]
    monitors: dict[str, set[web.WebSocketResponse]] = request.app["monitors"]

    # Per-service endpoint pre-fills the service name
    url_service_name = request.match_info.get("service_name")

    # Track active subscriptions (refcounted) and monitor registrations for cleanup
    subscribed: dict[str, tuple[Service, int]] = {}
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
            txnid = msg.get("txnid")
            msg_type = msg.get("type", "")

            # Handle auth messages
            if msg_type == "auth":
                auth_info = await _handle_auth(request.app, ws, msg)
                if auth_info is not None:
                    ws._mkio_auth = auth_info
                continue

            # Handle monitor requests — omit service to monitor all
            if msg_type == "monitor":
                target = service_name or "*"
                if target != "*" and target not in services:
                    available = ", ".join(sorted(k for k in services if not k.startswith("_")))
                    await ws.send_bytes(make_error(
                        ref,
                        f"Unknown service: {target!r}. Available services: {available}",
                        txnid=txnid,
                    ))
                    continue
                if request.app["config"].get("auth"):
                    from mkio.auth import check_access, build_when_params, execute_when_check
                    monitor_access = request.app["config"].get("monitor_access")
                    if monitor_access is None:
                        await ws.send_bytes(make_error(ref, "monitoring disabled", txnid=txnid))
                        continue
                    auth_info = getattr(ws, "_mkio_auth", None)
                    rights_cache = request.app.get("rights_cache")
                    allowed, when_sql = check_access(monitor_access, auth_info, rights_cache)
                    if not allowed:
                        message = "authentication required" if auth_info is None else "permission denied"
                        await ws.send_bytes(make_error(ref, message, txnid=txnid))
                        continue
                    if when_sql is not None:
                        params = build_when_params(auth_info, {})
                        db = request.app["db"]
                        try:
                            passed = await execute_when_check(db, when_sql, params)
                        except Exception:
                            await ws.send_bytes(make_error(ref, "permission denied", txnid=txnid))
                            continue
                        if not passed:
                            await ws.send_bytes(make_error(ref, "permission denied", txnid=txnid))
                            continue

                monitors[target].add(ws)
                monitoring.add(target)
                ack: dict[str, Any] = {"type": "monitor_ack"}
                if target != "*":
                    ack["service"] = target
                if ref:
                    ack["ref"] = ref
                await ws.send_bytes(dumps(ack))
                continue

            if not service_name:
                await ws.send_bytes(make_error(ref, "Missing 'service' field", txnid=txnid))
                continue

            svc = services.get(service_name)
            if svc is None:
                available = ", ".join(sorted(k for k in services if not k.startswith("_")))
                subid = msg.get("subid")
                await ws.send_bytes(make_nack(
                    service_name,
                    f"Unknown service: {service_name!r}. Available services: {available}",
                    ref=ref,
                    txnid=txnid,
                    subid=subid,
                ))
                continue

            # Notify monitors of inbound message
            await _notify_monitors(request.app, service_name, "in", msg)

            # Access control check
            if request.app["config"].get("auth"):
                denied = await _check_service_access(
                    request.app, ws, service_name, msg, msg_type,
                )
                if denied:
                    await ws.send_bytes(denied)
                    await _notify_monitors(request.app, service_name, "out", denied)
                    continue

            if msg_type == "subscribe":
                req_protocol = msg.get("protocol")
                subid = msg.get("subid")
                if req_protocol is None:
                    resp = make_nack(
                        service_name,
                        "Missing 'protocol' field in subscribe message",
                        ref=ref,
                        txnid=txnid,
                        subid=subid,
                    )
                    await ws.send_bytes(resp)
                    await _notify_monitors(request.app, service_name, "out", resp)
                    continue
                actual = svc.config.get("protocol", "")
                if req_protocol != actual:
                    resp = make_nack(
                        service_name,
                        f"Protocol mismatch: service '{service_name}' is {actual!r}, not {req_protocol!r}",
                        ref=ref,
                        txnid=txnid,
                        subid=subid,
                    )
                    await ws.send_bytes(resp)
                    await _notify_monitors(request.app, service_name, "out", resp)
                    continue
                count = await svc.on_subscribe(ws, msg)
                if count is None:
                    count = 1
                if count > 0:
                    entry = subscribed.get(service_name)
                    if entry:
                        subscribed[service_name] = (svc, entry[1] + count)
                    else:
                        subscribed[service_name] = (svc, count)
            elif msg_type == "unsubscribe":
                if service_name in subscribed:
                    removed = await svc.on_unsubscribe(ws, msg)
                    entry = subscribed[service_name]
                    remaining = entry[1] - (removed if removed is not None else entry[1])
                    if remaining <= 0:
                        del subscribed[service_name]
                    else:
                        subscribed[service_name] = (entry[0], remaining)
            elif msg_type == "request":
                actual = svc.config.get("protocol", "")
                if actual != "reqrep":
                    resp = make_error(
                        ref,
                        f"Service '{service_name}' uses protocol {actual!r}, not 'reqrep'",
                        reqid=msg.get("reqid"),
                    )
                    await ws.send_bytes(resp)
                    await _notify_monitors(request.app, service_name, "out", resp)
                    continue
                await svc.on_message(ws, msg)
            else:
                await svc.on_message(ws, msg)

    except (asyncio.CancelledError, ConnectionError):
        pass
    finally:
        # Unsubscribe from all services on disconnect
        for svc_name, (svc, _count) in subscribed.items():
            try:
                await svc.on_unsubscribe(ws, {"type": "unsubscribe", "service": svc_name})
            except Exception:
                pass
        # Remove from monitor sets
        for svc_name in monitoring:
            monitors[svc_name].discard(ws)
        request.app["websockets"].discard(ws)
        # Disconnect hooks
        if mkio_app is not None:
            for hook in mkio_app._disconnect_hooks:
                try:
                    await hook(ws)
                except Exception:
                    pass

    return ws
