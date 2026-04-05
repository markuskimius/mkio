"""CLI entry point: mkio serve | services | monitor | send | subscribe"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

from mkio._json import loads


def main() -> None:
    if len(sys.argv) < 2:
        _usage()

    cmd = sys.argv[1]
    if cmd == "serve":
        _cmd_serve()
    elif cmd == "services":
        _cmd_services()
    elif cmd == "monitor":
        _cmd_monitor()
    elif cmd == "send":
        _cmd_send()
    elif cmd == "subscribe":
        _cmd_subscribe()
    else:
        _usage()


def _usage() -> None:
    print("Usage:")
    print("  mkio serve [mkio.toml]           Start a server (default: mkio.toml)")
    print("  mkio services <url> [service]    List services, or show detail for one")
    print("  mkio monitor <url> <service>     Monitor a service's messages")
    print("  mkio send <url> <service> [--op <name>] <data>")
    print("                                   Send transaction(s) from JSON/CSV/inline")
    print("  mkio subscribe <url> <service> [--filter <expr>] [--ref <ver>]")
    print("                                   Subscribe and stream live messages")
    sys.exit(1)


def _cmd_serve() -> None:
    config_path = sys.argv[2] if len(sys.argv) >= 3 else "mkio.toml"
    from pathlib import Path
    if not Path(config_path).exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    from mkio.server import serve
    serve(config_path)


def _cmd_services() -> None:
    if len(sys.argv) < 3:
        print("Usage: mkio services <url> [service]")
        print("  e.g. mkio services http://localhost:8080")
        print("  e.g. mkio services http://localhost:8080 orders")
        sys.exit(1)
    url = sys.argv[2].rstrip("/")
    service_name = sys.argv[3] if len(sys.argv) >= 4 else None
    if service_name:
        asyncio.run(_fetch_service_detail(url, service_name))
    else:
        asyncio.run(_fetch_services(url))


async def _fetch_services(url: str) -> None:
    import aiohttp
    api_url = f"{url}/api/services"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as resp:
                if resp.status != 200:
                    print(f"Error: HTTP {resp.status}")
                    sys.exit(1)
                services = await resp.json()
    except aiohttp.ClientError as e:
        print(f"Error connecting to {api_url}: {e}")
        sys.exit(1)

    if not services:
        print("No services available.")
        return

    # Print table
    name_w = max(len(s["name"]) for s in services)
    type_w = max(len(s["type"]) for s in services)
    name_w = max(name_w, 7)  # "SERVICE"
    type_w = max(type_w, 4)  # "TYPE"

    print(f"{'SERVICE':<{name_w}}  {'TYPE':<{type_w}}  DETAILS")
    print(f"{'-' * name_w}  {'-' * type_w}  {'-' * 30}")
    for svc in services:
        details = []
        if "primary_table" in svc:
            details.append(f"table={svc['primary_table']}")
        if "tables" in svc:
            details.append(f"tables={','.join(svc['tables'])}")
        if "watch_tables" in svc:
            details.append(f"watch={','.join(svc['watch_tables'])}")
        print(f"{svc['name']:<{name_w}}  {svc['type']:<{type_w}}  {', '.join(details)}")


async def _fetch_service_detail(url: str, service_name: str) -> None:
    import aiohttp
    api_url = f"{url}/api/services/{service_name}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as resp:
                if resp.status == 404:
                    print(f"Unknown service: {service_name}")
                    sys.exit(1)
                if resp.status != 200:
                    print(f"Error: HTTP {resp.status}")
                    sys.exit(1)
                detail = await resp.json()
    except aiohttp.ClientError as e:
        print(f"Error connecting to {api_url}: {e}")
        sys.exit(1)

    _print_service_detail(detail)


def _print_service_detail(detail: dict[str, Any]) -> None:
    """Pretty-print service detail."""
    name = detail["name"]
    svc_type = detail["type"]
    desc = detail.get("description", "")

    print(f"Service: {name} ({svc_type})")
    if desc:
        print(f"  {desc}")
    print()

    if svc_type == "transaction":
        _print_transaction_detail(detail)
    else:
        _print_listener_detail(detail)


def _print_transaction_detail(detail: dict[str, Any]) -> None:
    ops = detail.get("ops", {})
    for op_name, op_info in ops.items():
        op_desc = op_info.get("description", "")
        label = f"  Op: {op_name}"
        if op_desc:
            label += f" — {op_desc}"
        print(label)

        for i, step in enumerate(op_info.get("steps", [])):
            if len(op_info.get("steps", [])) > 1:
                table_label = f"    [{step['table']}] {step['op_type']}"
            else:
                table_label = None

            fields = step.get("fields", {})
            auto = step.get("auto", {})
            bind = step.get("bind", {})

            # Only show the first (primary) step's fields in detail
            # For secondary steps (audit, etc.), show a summary
            if i > 0 and bind:
                bound_cols = ", ".join(f"{k} from {v}" for k, v in bind.items())
                user_fields = [f for f in fields if f not in bind]
                parts = []
                if user_fields:
                    parts.append(", ".join(user_fields))
                parts.append(bound_cols)
                print(f"    Also writes to: {step['table']} ({'; '.join(parts)})")
                continue

            if table_label:
                print(table_label)

            if fields:
                # Separate key fields, required fields, optional fields
                key_fields = {f: info for f, info in fields.items() if info.get("key")}
                req_fields = {f: info for f, info in fields.items()
                              if info.get("required") and not info.get("key")}
                opt_fields = {f: info for f, info in fields.items()
                              if not info.get("required") and not info.get("key")}

                if key_fields or req_fields or opt_fields:
                    print("    Fields (you provide):")
                    col_w = max(len(f) for f in fields)
                    for f, info in fields.items():
                        typ = info.get("type", "")
                        notes = []
                        if info.get("key"):
                            notes.append("required, key")
                        elif info.get("required"):
                            notes.append("required")
                        if info.get("default"):
                            notes.append(f"default: {info['default']}")
                        note_str = f"  ({', '.join(notes)})" if notes else ""
                        print(f"      {f:<{col_w}}  {typ:<10}{note_str}")

            if auto:
                print("    Auto-generated:")
                col_w = max(len(f) for f in auto)
                for f, info in auto.items():
                    typ = info.get("type", "")
                    source = info.get("source", "")
                    dflt = info.get("default", "")
                    if source == "default" and dflt:
                        note = f"default: {dflt}"
                    else:
                        note = source
                    print(f"      {f:<{col_w}}  {typ:<10}  {note}")

        # Example
        example = op_info.get("example")
        if example:
            print(f"    Example:")
            print(f"      {example}")
        print()

    # Recovery info
    recovery = detail.get("recovery")
    if recovery:
        print("  Recovery:")
        print(f"    {recovery['description']}")
        check = recovery.get("check_message", {})
        if check:
            print(f"    Check message: {json.dumps(check)}")
        print()


def _print_listener_detail(detail: dict[str, Any]) -> None:
    primary = detail.get("primary_table")
    if primary:
        print(f"  Table: {primary}")

    key = detail.get("key")
    if key:
        print(f"  Key: {key}")

    filterable = detail.get("filterable", [])
    if filterable:
        print(f"  Filter by: {', '.join(filterable)}")

    schema = detail.get("schema", {})
    if schema:
        print()
        print("  Schema:")
        col_w = max(len(f) for f in schema)
        for f, info in schema.items():
            typ = info.get("type", "")
            note = "  (primary key)" if info.get("pk") else ""
            print(f"    {f:<{col_w}}  {typ}{note}")

    # Subscribe protocol / recovery
    subscribe = detail.get("subscribe", {})
    if subscribe:
        print()
        print("  Subscribe protocol:")
        msg = subscribe.get("message", {})
        print(f"    Message: {json.dumps(msg)}")
        response_types = subscribe.get("response_types", [])
        if response_types:
            print(f"    Response types: {', '.join(response_types)}")
        recovery = subscribe.get("recovery")
        if recovery:
            print(f"    Recovery: {recovery}")
        log_size = subscribe.get("change_log_size") or subscribe.get("buffer_size")
        if log_size:
            label = "buffer_size" if "buffer_size" in subscribe else "change_log_size"
            print(f"    {label}: {log_size:,}")

    example = detail.get("example", {})
    if example:
        print()
        print("  Example:")
        for cmd in example.values():
            print(f"    {cmd}")
    print()


def _cmd_monitor() -> None:
    if len(sys.argv) < 4:
        print("Usage: mkio monitor <url> <service>")
        print("  e.g. mkio monitor ws://localhost:8080 live_orders")
        sys.exit(1)
    _check_unknown_flags(sys.argv[2:], set(), "mkio monitor <url> <service>")
    url = sys.argv[2].rstrip("/")
    service = sys.argv[3]
    ws_url = _normalize_ws_url(url)

    try:
        asyncio.run(_monitor_service(ws_url, service))
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


async def _monitor_service(ws_url: str, service: str) -> None:
    import aiohttp

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url) as ws:
                # Send monitor request
                from mkio._json import dumps
                await ws.send_bytes(dumps({
                    "type": "monitor",
                    "service": service,
                }))

                # Wait for ack
                ack_msg = await ws.receive()
                ack = loads(ack_msg.data)
                if ack.get("type") == "error":
                    print(f"Error: {ack.get('message', 'Unknown error')}")
                    sys.exit(1)

                print(f"Monitoring service: {service}")
                print(f"Connected to: {ws_url}")
                print("---")

                # Stream messages
                async for msg in ws:
                    if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        data = loads(msg.data)
                        _print_monitor_message(data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"WebSocket error: {ws.exception()}")
                        break

    except aiohttp.ClientError as e:
        print(f"Error connecting to {ws_url}: {e}")
        sys.exit(1)


def _print_monitor_message(data: dict[str, Any]) -> None:
    """Pretty-print a monitor envelope."""
    direction = data.get("direction", "?")
    service = data.get("service", "?")
    message = data.get("message", {})
    msg_type = message.get("type", "")

    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    arrow = ">>" if direction == "in" else "<<"

    # Color codes (if terminal supports it)
    if sys.stdout.isatty():
        if direction == "in":
            color, reset = "\033[36m", "\033[0m"  # cyan
        else:
            color, reset = "\033[33m", "\033[0m"  # yellow
    else:
        color = reset = ""

    header = f"{color}[{now}] {arrow} {direction.upper():3s} {msg_type or '(message)'}{reset}"
    print(header)

    # Compact JSON for the message body
    body = json.dumps(message, indent=2, default=str)
    print(body)
    print()


# ---- send command -----------------------------------------------------------

def _cmd_send() -> None:
    args = sys.argv[2:]
    if len(args) < 3:
        print("Usage: mkio send <url> <service> [--op <name>] <data>")
        print("  <data> can be inline JSON, a .json file, or a .csv file")
        sys.exit(1)

    url = args[0].rstrip("/")
    service = args[1]
    rest = args[2:]

    _check_unknown_flags(rest, {"--op"}, "mkio send <url> <service> [--op <name>] <data>")

    op_name = None
    if "--op" in rest:
        idx = rest.index("--op")
        if idx + 1 >= len(rest):
            print("Error: --op requires a value")
            sys.exit(1)
        op_name = rest[idx + 1]
        rest = rest[:idx] + rest[idx + 2:]

    if not rest:
        print("Error: no data argument provided")
        sys.exit(1)

    data_arg = rest[0]
    messages = _load_messages(data_arg)

    ws_url = _normalize_ws_url(url)

    try:
        asyncio.run(_send_messages(ws_url, service, op_name, messages))
    except KeyboardInterrupt:
        pass


_ENVELOPE_KEYS = {"op", "ref", "service"}


def _load_messages(data_arg: str) -> list[dict[str, Any]]:
    """Load messages from inline JSON, .json file, or .csv file.

    Returns a list of dicts. Each dict is either:
    - Flat data (all keys are data fields), or
    - Structured with envelope fields: {"data": {...}, "op": "...", "ref": "..."}

    CSV files support ``data.`` prefixed columns (e.g. ``data.id``) to separate
    data fields from envelope fields (``op``, ``ref``).  Flat CSVs without
    ``data.`` prefixes remain backwards-compatible.
    """
    if data_arg.endswith(".json"):
        with open(data_arg) as f:
            parsed = json.load(f)
        items = parsed if isinstance(parsed, list) else [parsed]
        return [_structure_json_msg(m) for m in items]
    elif data_arg.endswith(".csv"):
        with open(data_arg) as f:
            reader = csv.DictReader(f)
            rows = []
            for raw_row in reader:
                rows.append(_structure_csv_row(raw_row))
            return rows
    else:
        parsed = json.loads(data_arg)
        items = parsed if isinstance(parsed, list) else [parsed]
        return [_structure_json_msg(m) for m in items]


def _structure_json_msg(obj: dict[str, Any]) -> dict[str, Any]:
    """If obj already has a 'data' sub-dict, treat as structured; otherwise flat."""
    if "data" in obj and isinstance(obj["data"], dict):
        return obj
    return obj


def _structure_csv_row(raw_row: dict[str, str]) -> dict[str, Any]:
    """Parse a CSV row, separating envelope fields from data fields.

    Columns prefixed with ``data.`` have the prefix stripped and go into
    the ``data`` sub-dict. Recognised envelope keys (``op``, ``ref``) become
    top-level. ``service`` is dropped (already on CLI). Other columns go
    into ``data`` for backwards compatibility with flat CSVs.
    """
    has_data_prefix = any(k.startswith("data.") for k in raw_row)
    msg: dict[str, Any] = {}
    data: dict[str, Any] = {}

    for k, v in raw_row.items():
        if k.startswith("data."):
            data[k[5:]] = _auto_convert(v)
        elif k in _ENVELOPE_KEYS:
            if k != "service":
                msg[k] = v
        elif has_data_prefix:
            pass  # ignore unknown non-data columns when data. prefix is used
        else:
            data[k] = _auto_convert(v)

    if msg:
        msg["data"] = data
        return msg
    return data


def _auto_convert(value: str) -> Any:
    """Convert string values to int/float if possible."""
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


async def _send_messages(
    ws_url: str,
    service: str,
    op_name: str | None,
    messages: list[dict[str, Any]],
) -> None:
    from mkio.client import MkioClient

    async with MkioClient(ws_url, reconnect=False) as client:
        total = len(messages)
        for i, msg in enumerate(messages, 1):
            # Extract envelope fields if present
            if "data" in msg and isinstance(msg["data"], dict):
                data = msg["data"]
                row_op = msg.get("op")
                row_ref = msg.get("ref")
            else:
                data = msg
                row_op = None
                row_ref = None

            kwargs: dict[str, Any] = {}
            effective_op = row_op or op_name
            if effective_op:
                kwargs["op"] = effective_op

            try:
                result = await client.send(service, data, ref=row_ref, **kwargs)
                version = result.get("version", "")
                if result.get("ok"):
                    print(f"[{i}/{total}] ok version={version}")
                else:
                    err_msg = result.get("message", "unknown error")
                    print(f"[{i}/{total}] error: {err_msg}")
            except Exception as e:
                print(f"[{i}/{total}] error: {e}")


# ---- subscribe command ------------------------------------------------------

def _cmd_subscribe() -> None:
    args = sys.argv[2:]
    if len(args) < 2:
        print("Usage: mkio subscribe <url> <service> [--filter <expr>] [--ref <ver>]")
        sys.exit(1)

    url = args[0].rstrip("/")
    service = args[1]
    rest = args[2:]

    _check_unknown_flags(rest, {"--filter", "--ref"}, "mkio subscribe <url> <service> [--filter <expr>] [--ref <ver>]")

    filter_expr = None
    if "--filter" in rest:
        idx = rest.index("--filter")
        if idx + 1 >= len(rest):
            print("Error: --filter requires a value")
            sys.exit(1)
        filter_expr = rest[idx + 1]
        rest = rest[:idx] + rest[idx + 2:]

    ref = None
    if "--ref" in rest:
        idx = rest.index("--ref")
        if idx + 1 >= len(rest):
            print("Error: --ref requires a value")
            sys.exit(1)
        ref = rest[idx + 1]

    ws_url = _normalize_ws_url(url)

    try:
        asyncio.run(_subscribe_service(ws_url, service, filter_expr, ref))
    except KeyboardInterrupt:
        print("\nSubscription stopped.")


async def _subscribe_service(
    ws_url: str,
    service: str,
    filter_expr: str | None,
    ref: str | None = None,
) -> None:
    from mkio.client import MkioClient

    async with MkioClient(ws_url, reconnect=True) as client:
        async for msg in client.subscribe(service, filter=filter_expr, ref=ref):
            _print_subscribe_message(msg)


def _print_subscribe_message(data: dict[str, Any]) -> None:
    """Pretty-print a subscription message."""
    msg_type = data.get("type", "")
    version = data.get("version", "")
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]

    is_tty = sys.stdout.isatty()
    ver_suffix = f" ref={version}" if version else ""

    if msg_type == "snapshot":
        rows = data.get("rows", [])
        if is_tty:
            print(f"\033[32m[{now}] SNAPSHOT ({len(rows)} rows){ver_suffix}\033[0m")
        else:
            print(f"[{now}] SNAPSHOT ({len(rows)} rows){ver_suffix}")
        for row in rows:
            print(f"  {json.dumps(row, default=str)}")
    elif msg_type == "delta":
        changes = data.get("changes", [])
        if is_tty:
            print(f"\033[35m[{now}] DELTA ({len(changes)} changes){ver_suffix}\033[0m")
        else:
            print(f"[{now}] DELTA ({len(changes)} changes){ver_suffix}")
        for c in changes:
            op = c.get("op", "?")
            row = c.get("row", {})
            print(f"  {op}: {json.dumps(row, default=str)}")
    elif msg_type == "update":
        op = data.get("op", "?")
        row = data.get("row", {})
        if is_tty:
            color = "\033[36m" if op == "insert" else "\033[33m" if op == "update" else "\033[31m"
            print(f"{color}[{now}] UPDATE {op}{ver_suffix}\033[0m")
        else:
            print(f"[{now}] UPDATE {op}{ver_suffix}")
        print(f"  {json.dumps(row, default=str)}")
    else:
        print(f"[{now}] {msg_type}: {json.dumps(data, default=str)}")


def _check_unknown_flags(args: list[str], known: set[str], usage: str) -> None:
    """Error and exit if args contain any unrecognised --flags."""
    for arg in args:
        if arg.startswith("--") and arg not in known:
            print(f"Unknown option: {arg}")
            print(f"Usage: {usage}")
            sys.exit(1)


# ---- helpers ----------------------------------------------------------------

def _normalize_ws_url(url: str) -> str:
    """Normalize a URL to ws:// and append /ws path."""
    if url.startswith("http://"):
        url = "ws://" + url[7:]
    elif url.startswith("https://"):
        url = "wss://" + url[8:]
    elif not url.startswith("ws://") and not url.startswith("wss://"):
        url = "ws://" + url
    return f"{url}/ws"


if __name__ == "__main__":
    main()
