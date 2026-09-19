"""CLI entry point: mkio serve | services | monitor | send | subpub | stream | query | reqrep"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import sys
import textwrap
from datetime import datetime, timezone
from typing import Any, NamedTuple

from mkio._json import loads
from mkio._ref import local_ts

_TRACEBACK = False

_PROTOCOL_CLI_HINT = {
    "subpub": "mkio subpub <url> <service> <topic>",
    "stream": "mkio stream <url> <service>",
    "query": "mkio query <url> <service>",
    "reqrep": "mkio reqrep <url> <service> [data]",
    "transaction": "mkio send <url> <service> <op> [data]",
}


_USERNAME = ("--username <user>", "Log in as <user> (password: MKIO_PASSWORD or prompt)")
_SUBID = ("--subid <id>", "Tag the subscription; echoed on every message for it")
_FIELDS = ("--fields <f1,f2,...>", "Receive only these columns")
_FILTER = ("--filter <expr>", "Receive only rows matching the expression")
_DRY_RUN = ("--dry-run", "Show what would happen and change nothing")


class _Mode(NamedTuple):
    """One invocation form of a command: positionals, then (option, help) pairs."""
    summary: str
    args: tuple[str, ...] = ()
    options: tuple[tuple[str, str], ...] = ()
    # How the options read in the synopsis, when "[--each] [--on] [--its-own]"
    # would mislead (alternatives, a required flag).
    synopsis: tuple[str, ...] = ()


class _Command(NamedTuple):
    group: str
    modes: tuple[_Mode, ...]
    notes: tuple[str, ...] = ()
    examples: tuple[str, ...] = ()


# The one description of the command line: the top-level usage, each command's
# --help, the "Usage:" line under an error and the accepted-flag sets all come
# from here.
_COMMANDS: dict[str, _Command] = {
    "serve": _Command(
        "Server",
        (_Mode("Start a server", ("[server.toml]",)),),
        notes=("The config path defaults to server.toml.",
               "Refuses to start when the database schema differs from the config, unless",
               "auto_migrate is set; run 'mkio dbupdate' to apply the changes."),
        examples=("mkio serve", "mkio serve prod.toml"),
    ),
    "init": _Command(
        "Server",
        (_Mode("Scaffold a project (server.toml, static/)", ("[directory]",),
               (("--no-static", "Write the config only, no static/index.html"),)),),
        notes=("The directory defaults to the current one. An existing server.toml is never",
               "overwritten."),
        examples=("mkio init", "mkio init ./my-project --no-static"),
    ),
    "dbupdate": _Command(
        "Server",
        (_Mode("Apply pending schema migrations", ("[server.toml]",), (
            ("--allow-risky", "Also apply potentially destructive changes"),
            ("--allow-destructive", "Apply every change, destructive ones included"),
            ("--drop-history", "Drop history tables whose base is no longer versioned"),
            ("--keep-redo", "Keep undone versions (redo state), discarded by default"),
        )),),
        notes=("Run with the server stopped. Without flags only safe changes are applied.",),
        examples=("mkio dbupdate", "mkio dbupdate server.toml --allow-risky"),
    ),
    "archive": _Command(
        "Server",
        (
            _Mode("Archive rows to CSV, then delete them",
                  ("<server.toml|url>",), (
                ("--tables a,b", "Archive these tables"),
                ("--group <g>", "Archive one archive group (default group: data)"),
                ("--all", "Archive every archivable table"),
                ("--cutoff <when>", "Rows older than Nd/Nh/Nm ago, a date, or a date-time"),
                ("--cutoff-literal <text>", "Cutoff text compared as given (custom column formats)"),
                ("--out <dir>", "Directory the archive is written under (default: .)"),
                _DRY_RUN,
                ("--yes", "Skip the confirmation (required off a terminal)"),
                _USERNAME,
            ), synopsis=("[--tables a,b | --group <g> | --all]", "[--cutoff <when>]",
                         "[--cutoff-literal <text>]", "[--out <dir>]", "[--dry-run]", "[--yes]",
                         "[--username <user>]")),
            _Mode("Archive old history versions only",
                  ("[server.toml]",), (
                ("--older-than <N>d|<ref>", "Versions older than N days, or than a ref (required)"),
                ("--table <name>", "Only this versioned table (default: all of them)"),
                ("--out <dir>", "Directory the CSV is written to (default: .)"),
                ("--delete", "Purge the archived versions once the CSV is written"),
                ("--prune-source", "Also delete live rows archived in full (needs --yes)"),
                _DRY_RUN,
                ("--yes", "Confirm --prune-source"),
            ), synopsis=("--older-than <N>d|<ref>", "[--table <name>]", "[--out <dir>]", "[--delete]",
                         "[--prune-source]", "[--dry-run]", "[--yes]")),
        ),
        notes=("Row mode takes the tables that opt in with archive = {...} in the config.",
               "A config path archives offline (server stopped); a url archives through the",
               "running server."),
        examples=("mkio archive server.toml --cutoff 30d --out ./archive --dry-run",
                  "mkio archive localhost:8080 --cutoff 1d --out /srv/archive --yes",
                  "mkio archive server.toml --older-than 90d --out ./archive --delete"),
    ),
    "restore": _Command(
        "Server",
        (_Mode("Put an archive's rows back (server stopped)", ("<server.toml>", "<archive-dir>"), (
            ("--tables a,b", "Restore only these tables"),
            _DRY_RUN,
        )),),
        examples=("mkio restore server.toml ./archive/20260911 --dry-run",),
    ),
    "adduser": _Command(
        "Server",
        (_Mode("Add a user to _mkio_users", ("<username>", "<role>", "[server.toml]")),),
        notes=("The password comes from MKIO_PASSWORD or a prompt.",),
        examples=("mkio adduser alice trader",),
    ),
    "hashpass": _Command(
        "Server",
        (_Mode("Generate a hashed password for seed files"),),
        notes=("The password comes from MKIO_PASSWORD or a prompt.",),
    ),
    "services": _Command(
        "Client",
        (_Mode("List services, or show detail for one", ("<url>", "[service]")),),
        examples=("mkio services 8080", "mkio services 8080 orders"),
    ),
    "send": _Command(
        "Client",
        (_Mode("Send transaction(s) to a transaction service", ("<url>", "<service>", "<data>"), (
            ("--op <name>", "The op to run (needed when the service defines several)"),
            _USERNAME,
        )),),
        notes=("<data> can be inline JSON, a .json file, or a .csv file.",),
        examples=("mkio send 8080 orders '{\"symbol\": \"AAPL\", \"qty\": 10}'",
                  "mkio send 8080 orders --op cancel orders.csv"),
    ),
    "subpub": _Command(
        "Client",
        (_Mode("Subscribe to topics of a subpub service", ("<url>", "<service>", "<topic>", "[<topic2> ...]"),
               (_SUBID, _FIELDS, _USERNAME)),),
        examples=("mkio subpub 8080 prices AAPL", "mkio subpub 8080 prices AAPL MSFT --fields bid,ask"),
    ),
    "stream": _Command(
        "Client",
        (_Mode("Subscribe to a stream service", ("<url>", "<service>"), (
            _SUBID, _FIELDS, _FILTER,
            ("--ref <ref>", "Start after this ref (default: now, new rows only)"),
            ("--maxcount <n>", "One page of at most n rows, no live updates"),
            ("--before", "Page backward: rows before --ref (or the newest rows)"),
            _USERNAME,
        )),),
        examples=("mkio stream 8080 trades", "mkio stream 8080 trades --before --maxcount 20"),
    ),
    "query": _Command(
        "Client",
        (_Mode("Subscribe to a query service", ("<url>", "<service>"), (
            _SUBID, _FIELDS, _FILTER,
            ("--snapshotOnly", "Snapshot only, no live updates"),
            ("--updateOnly", "Live updates only, no snapshot"),
            _USERNAME,
        )),),
        examples=("mkio query 8080 open_orders --filter \"symbol == 'AAPL'\"",),
    ),
    "reqrep": _Command(
        "Client",
        (_Mode("Send a request, print the reply", ("<url>", "<service>", "[data]"),
               (_USERNAME,)),),
        notes=("<data> can be inline JSON or key=value pairs.",),
        examples=("mkio reqrep 8080 lookup '{\"symbol\": \"AAPL\"}'",
                  "mkio reqrep 8080 calculate qty=10 price=99.95"),
    ),
    "schema": _Command(
        "Client",
        (_Mode("Show a table's schema (columns, types, keys)", ("<url>", "<table>"), (_USERNAME,)),),
        examples=("mkio schema 8080 orders",),
    ),
    "check": _Command(
        "Client",
        (_Mode("Check version compatibility with a server",
               ("<url>", "[version=... protocol=... mkio=... expr=...]"), (_USERNAME,)),),
        notes=("Exits 0 when compatible, 1 when not.",),
        examples=("mkio check 8080", "mkio check 8080 version=2.0.0 protocol=1.0"),
    ),
    "monitor": _Command(
        "Client",
        (_Mode("Monitor messages (all services or one)", ("<url>", "[service]"), (
            ("--filter <expr>", "Show only messages matching the expression"),
            _USERNAME,
        )),),
        examples=("mkio monitor 8080", "mkio monitor 8080 orders",
                  "mkio monitor 8080 --filter \"service == 'orders' && direction == 'out'\""),
    ),
}

_VALID_COMMANDS = tuple(_COMMANDS)
_HELP_FLAGS = ("-h", "--help")
_HELP_WIDTH = 80


def _enable_ansi_colors() -> None:
    """Turn on escape-sequence processing in the Windows console.

    Windows Terminal handles ANSI colors already; the legacy console host
    prints the raw sequences unless virtual-terminal mode is switched on.
    A no-op elsewhere, and when stdout is not a console.
    """
    if sys.platform != "win32" or not sys.stdout.isatty():
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            enable_vt = 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
            kernel32.SetConsoleMode(handle, mode.value | enable_vt)
    except Exception:
        pass


def main() -> None:
    global _TRACEBACK
    _enable_ansi_colors()
    if "--traceback" in sys.argv:
        sys.argv.remove("--traceback")
        _TRACEBACK = True
    if os.environ.get("MKIO_TRACEBACK") == "1":
        _TRACEBACK = True

    if len(sys.argv) < 2:
        _usage()

    cmd = sys.argv[1]
    if cmd in ("--version", "-V"):
        print(f"mkio {_mkio_version()}")
        sys.exit(0)
    if cmd in _HELP_FLAGS or (cmd == "help" and len(sys.argv) == 2):
        _print_help()
        sys.exit(0)
    if cmd == "help":
        cmd = sys.argv[2]
        if cmd in _COMMANDS:
            _print_command_help(cmd)
            sys.exit(0)
        _unknown_command(cmd)
    if cmd.startswith("-"):
        print(f"Error: expected a command, got {cmd!r}")
        print("Run 'mkio --help' for the list of commands.")
        sys.exit(1)
    if cmd in _COMMANDS and any(a in _HELP_FLAGS for a in sys.argv[2:]):
        _print_command_help(cmd)
        sys.exit(0)
    if cmd == "serve":
        _cmd_serve()
    elif cmd == "services":
        _cmd_services()
    elif cmd == "monitor":
        _cmd_monitor()
    elif cmd == "send":
        _cmd_send()
    elif cmd == "subpub":
        _cmd_subpub()
    elif cmd == "stream":
        _cmd_stream()
    elif cmd == "query":
        _cmd_query()
    elif cmd == "reqrep":
        _cmd_reqrep()
    elif cmd == "check":
        _cmd_check()
    elif cmd == "dbupdate":
        _cmd_dbupdate()
    elif cmd == "archive":
        _cmd_archive()
    elif cmd == "restore":
        _cmd_restore()
    elif cmd == "schema":
        _cmd_schema()
    elif cmd == "init":
        _cmd_init()
    elif cmd == "adduser":
        _cmd_adduser()
    elif cmd == "hashpass":
        _cmd_hashpass()
    else:
        _unknown_command(cmd)


def _unknown_command(cmd: str) -> None:
    import difflib
    close = difflib.get_close_matches(cmd, _VALID_COMMANDS, n=1, cutoff=0.5)
    hint = f" Did you mean {close[0]!r}?" if close else ""
    print(f"Unknown command: {cmd!r}.{hint}")
    print("Run 'mkio --help' for the list of commands.")
    sys.exit(1)


def _synopsis(cmd: str, mode: int = 0) -> str:
    """One-line ``mkio <cmd> ...`` synopsis of a command mode."""
    return " ".join(["mkio", cmd, *_synopsis_tokens(cmd, mode)])


def _synopsis_tokens(cmd: str, mode: int) -> tuple[str, ...]:
    m = _COMMANDS[cmd].modes[mode]
    return (*m.args, *(m.synopsis or (f"[{opt}]" for opt, _ in m.options)))


def _flags(cmd: str, mode: int = 0) -> set[str]:
    """The ``--flags`` a command mode accepts."""
    return {opt.split()[0] for opt, _ in _COMMANDS[cmd].modes[mode].options}


def _wrap_synopsis(cmd: str, mode: int, indent: str) -> list[str]:
    """The synopsis folded to the help width, never splitting a ``[--flag <value>]``."""
    head = f"{indent}mkio {cmd}"
    hang = " " * (len(head) + 1)
    lines = [head]
    for token in _synopsis_tokens(cmd, mode):
        if len(lines[-1]) + 1 + len(token) > _HELP_WIDTH and lines[-1].strip():
            lines.append(hang + token)
        else:
            lines[-1] += " " + token
    return lines


def _print_help() -> None:
    summary_col = 35
    print("Usage: mkio <command> [arguments]")
    group = None
    for cmd, spec in _COMMANDS.items():
        if spec.group != group:
            group = spec.group
            print()
            print(f"{group} commands:")
        for mode, m in enumerate(spec.modes):
            lines = _wrap_synopsis(cmd, mode, "  ")
            if len(lines) == 1 and len(lines[0]) < summary_col - 1:
                print(f"{lines[0]:<{summary_col}}{m.summary}")
                continue
            for line in lines:
                print(line)
            print(" " * summary_col + m.summary)
    print()
    print("Options:")
    print("  -h, --help             Show help; 'mkio <command> --help' for one command")
    print("  --traceback            Show full Python traceback on errors")
    print("  --version, -V          Print the mkio version and exit")
    print()
    print("<url> shorthand: 8080 is http://localhost:8080, myhost is http://myhost:80.")


def _print_command_help(cmd: str) -> None:
    spec = _COMMANDS[cmd]
    several = len(spec.modes) > 1
    for mode in range(len(spec.modes)):
        print("\n".join(_wrap_synopsis(cmd, mode, "Usage: " if mode == 0 else "       ")))
    for m in spec.modes:
        print()
        # With several modes each summary heads its own option list.
        print(f"{m.summary}:" if several else m.summary)
        if m.options:
            if not several:
                print()
                print("Options:")
            width = max(len(opt) for opt, _ in m.options)
            for opt, text in m.options:
                print(f"  {opt:<{width}}  {text}")
    if spec.notes:
        print()
        for note in spec.notes:
            print(note)
    if spec.examples:
        print()
        print("Examples:")
        for example in spec.examples:
            print(f"  {example}")


def _usage() -> None:
    _print_help()
    sys.exit(1)


def _print_usage(usage: str) -> None:
    """The ``Usage:`` line under an argument error, pointing at the full help."""
    cmd = usage.split()[1]
    modes = range(len(_COMMANDS[cmd].modes))
    mode = next(m for m in modes if _synopsis(cmd, m) == usage)
    print("\n".join(_wrap_synopsis(cmd, mode, "Usage: ")))
    print(f"Run 'mkio {cmd} --help' for details.")


def _missing_args(cmd: str) -> None:
    """Too few arguments to do anything: show the command's help, exit 1."""
    _print_command_help(cmd)
    sys.exit(1)


def _cmd_serve() -> None:
    usage = _synopsis("serve")
    args = sys.argv[2:]
    _check_unknown_flags(args, set(), usage)
    if len(args) > 1:
        print(f"Error: 'serve' takes at most 1 argument (config path), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)
    config_path = args[0] if args else "server.toml"
    from pathlib import Path
    if not Path(config_path).exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    from mkio.server import serve
    try:
        serve(config_path)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        if _TRACEBACK:
            raise
        _serve_error(config_path, exc)


def _cmd_services() -> None:
    usage = _synopsis("services")
    args = sys.argv[2:]
    _check_unknown_flags(args, set(), usage)
    if len(args) < 1:
        _missing_args("services")
    if len(args) > 2:
        print(f"Error: 'services' takes 1–2 arguments (url [service]), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)
    url = _normalize_url(args[0].rstrip("/"))
    service_name = args[1] if len(args) >= 2 else None
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
    proto_w = max(len(s["protocol"]) for s in services)
    name_w = max(name_w, 7)  # "SERVICE"
    proto_w = max(proto_w, 8)  # "PROTOCOL"

    print(f"{'SERVICE':<{name_w}}  {'PROTOCOL':<{proto_w}}  DETAILS")
    print(f"{'-' * name_w}  {'-' * proto_w}  {'-' * 30}")
    for svc in services:
        details = []
        if "primary_table" in svc:
            details.append(f"table={svc['primary_table']}")
        if "tables" in svc:
            details.append(f"tables={','.join(svc['tables'])}")
        if "watch_tables" in svc:
            details.append(f"watch={','.join(svc['watch_tables'])}")
        print(f"{svc['name']:<{name_w}}  {svc['protocol']:<{proto_w}}  {', '.join(details)}")


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
    protocol = detail["protocol"]
    desc = detail.get("description", "")

    print(f"Service: {name} ({protocol})")
    if desc:
        print(f"  {desc}")
    print()

    if protocol == "transaction":
        _print_transaction_detail(detail)
    else:
        _print_listener_detail(detail)


def _print_transaction_detail(detail: dict[str, Any]) -> None:
    ops = detail.get("ops", {})

    # Collect all op names and their primary fields for a summary table
    op_names = list(ops.keys())
    if not op_names:
        return

    # Find max widths for alignment
    name_w = max(len(n) for n in op_names)
    name_w = max(name_w, 2)

    print("  Operations:")
    print()
    for op_name in op_names:
        op_info = ops[op_name]
        op_desc = op_info.get("description", "")
        steps = op_info.get("steps", [])
        primary = steps[0] if steps else {}
        fields = primary.get("fields", {})

        # Build a concise field summary
        parts = []
        for f, info in fields.items():
            typ_short = info.get("type", "")[0:3].lower() if info.get("type") else ""
            if info.get("key"):
                parts.append(f"{f}* (key)")
            elif info.get("required"):
                parts.append(f"{f}*")
            elif info.get("default"):
                parts.append(f"{f}={info['default']}")
            else:
                parts.append(f)

        field_str = ", ".join(parts) if parts else "(no fields)"
        desc_str = f"  — {op_desc}" if op_desc else ""

        print(f"    {op_name:<{name_w}}  {field_str}{desc_str}")

    # Detailed field info for each op
    print()
    for op_name in op_names:
        op_info = ops[op_name]
        steps = op_info.get("steps", [])
        if not steps:
            continue

        primary = steps[0]
        fields = primary.get("fields", {})
        auto = primary.get("auto", {})

        if not fields and not auto:
            continue

        print(f"  {op_name}:")
        if fields:
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
                note_str = f"  {', '.join(notes)}" if notes else ""
                print(f"    {f:<{col_w}}  {typ:<10}{note_str}")

        if auto:
            auto_names = ", ".join(f"{f} ({info.get('source', '')})" for f, info in auto.items())
            print(f"    auto: {auto_names}")

        # Secondary steps (audit, etc.)
        for step in steps[1:]:
            bind = step.get("bind", {})
            if bind:
                bound_parts = ", ".join(f"{k}={v}" for k, v in bind.items())
                print(f"    + {step['table']}: {bound_parts}")

        # Example
        example = op_info.get("example")
        if example:
            print(f"    example: {example}")

        print()

    # Recovery info
    recovery = detail.get("recovery")
    if recovery:
        print("  Recovery:")
        print(f"    {recovery['description']}")
        check = recovery.get("check_message", {})
        if check:
            print(f"    Check: {json.dumps(check)}")
        print()


def _print_listener_detail(detail: dict[str, Any]) -> None:
    primary = detail.get("primary_table")
    if primary:
        print(f"  Table: {primary}")

    topic = detail.get("topic")
    if topic:
        print(f"  Topic: {topic}")

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
    usage = _synopsis("monitor")
    args = sys.argv[2:]
    if len(args) < 1:
        _missing_args("monitor")
    _check_unknown_flags(args, _flags("monitor"), usage)
    filter_expr = _extract_flag(args, "--filter")
    username, password = _extract_auth(args)
    _check_extra_positional(args[2:] if len(args) > 2 else [], usage)
    url = args[0].rstrip("/")
    service = args[1] if len(args) >= 2 else None
    ws_url = _normalize_ws_url(url)

    filter_fn = None
    if filter_expr:
        from mkio.expr import compile_filter
        try:
            filter_fn = compile_filter(filter_expr)
        except Exception as e:
            print(f"Error: invalid filter expression: {e}")
            sys.exit(1)

    _run_client_command(
        ws_url,
        _monitor_service(ws_url, service, filter_fn, username=username, password=password),
        "\nMonitor stopped.",
    )


async def _monitor_service(
    ws_url: str,
    service: str | None,
    filter_fn: Any = None,
    username: str | None = None,
    password: str | None = None,
) -> None:
    import aiohttp

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            from mkio._json import dumps

            if username:
                auth_msg = {"type": "auth", "data": {"username": username, "password": password}}
                await ws.send_bytes(dumps(auth_msg))
                auth_resp = await ws.receive()
                auth_data = loads(auth_resp.data)
                if not auth_data.get("ok"):
                    print(f"Authentication failed: {auth_data.get('message', 'unknown error')}")
                    sys.exit(1)

            monitor_msg: dict[str, Any] = {"type": "monitor"}
            if service:
                monitor_msg["service"] = service
            await ws.send_bytes(dumps(monitor_msg))

            ack_msg = await ws.receive()
            ack = loads(ack_msg.data)
            if ack.get("type") == "error":
                print(f"Error: {ack.get('message', 'Unknown error')}")
                sys.exit(1)

            label = f"service: {service}" if service else "all services"
            print(f"Monitoring {label}")
            print(f"Connected to: {ws_url}")
            if filter_fn:
                print(f"Filter active")
            print("---")

            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                    data = loads(msg.data)
                    if filter_fn:
                        try:
                            if not filter_fn(data):
                                continue
                        except Exception:
                            pass
                    _print_monitor_message(data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print(f"WebSocket error: {ws.exception()}")
                    break


def _print_monitor_message(data: dict[str, Any]) -> None:
    """Pretty-print a monitor envelope."""
    direction = data.get("direction", "?")
    service = data.get("service", "?")
    message = data.get("message", {})
    msg_type = message.get("type", "")

    now = local_ts()
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
    usage = _synopsis("send")
    args = sys.argv[2:]
    if len(args) < 3:
        _missing_args("send")

    url = args[0].rstrip("/")
    service = args[1]
    rest = args[2:]

    _check_unknown_flags(rest, _flags("send"), usage)
    username, password = _extract_auth(rest)

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
        _print_usage(usage)
        sys.exit(1)

    if len(rest) > 1:
        print(f"Error: expected 1 data argument, got {len(rest)}: {rest}")
        _print_usage(usage)
        sys.exit(1)

    data_arg = rest[0]
    messages = _load_messages(data_arg)

    ws_url = _normalize_ws_url(url)
    _run_client_command(ws_url, _send_messages(ws_url, service, op_name, messages, username=username, password=password))


_ENVELOPE_KEYS = {"op", "ref", "service", "txnid"}


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
        from pathlib import Path
        if not Path(data_arg).exists():
            print(f"Error: file not found: {data_arg}")
            sys.exit(1)
        try:
            with open(data_arg) as f:
                parsed = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error: invalid JSON in {data_arg}: {e}")
            sys.exit(1)
        items = parsed if isinstance(parsed, list) else [parsed]
        return [_structure_json_msg(m) for m in items]
    elif data_arg.endswith(".csv"):
        from pathlib import Path
        if not Path(data_arg).exists():
            print(f"Error: file not found: {data_arg}")
            sys.exit(1)
        with open(data_arg) as f:
            reader = csv.DictReader(f)
            rows = []
            for raw_row in reader:
                rows.append(_structure_csv_row(raw_row))
            if not rows:
                print(f"Warning: CSV file {data_arg} has no data rows")
            return rows
    else:
        try:
            parsed = json.loads(data_arg)
        except json.JSONDecodeError as e:
            print(f"Error: invalid inline JSON: {e}")
            print("  Data must be inline JSON, a .json file, or a .csv file")
            sys.exit(1)
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
    username: str | None = None,
    password: str | None = None,
) -> None:
    from mkio.client import MkioClient

    async with MkioClient(ws_url, reconnect=False) as client:
        if username:
            await _authenticate(client, username, password)
        total = len(messages)
        for i, msg in enumerate(messages, 1):
            # Extract envelope fields if present
            if "data" in msg and isinstance(msg["data"], dict):
                data = msg["data"]
                row_op = msg.get("op")
                row_ref = msg.get("ref")
                row_txnid = msg.get("txnid")
            else:
                data = msg
                row_op = None
                row_ref = None
                row_txnid = None

            kwargs: dict[str, Any] = {}
            effective_op = row_op or op_name
            if effective_op:
                kwargs["op"] = effective_op
            if row_txnid is not None:
                kwargs["txnid"] = row_txnid

            try:
                result = await client.send(service, data, ref=row_ref, **kwargs)
                ref = result.get("ref", "")
                if result.get("ok"):
                    print(f"[{i}/{total}] ok ref={ref}")
                else:
                    err_msg = result.get("message", "unknown error")
                    print(f"[{i}/{total}] error: {err_msg}")
            except Exception as e:
                print(f"[{i}/{total}] error: {e}")


# ---- subscribe commands (subpub, stream, query) -----------------------------

def _cmd_subpub() -> None:
    args = sys.argv[2:]
    usage = _synopsis("subpub")
    if len(args) < 3:
        _missing_args("subpub")

    url = args[0].rstrip("/")
    service = args[1]

    topics: list[str] = []
    i = 2
    while i < len(args) and not args[i].startswith("--"):
        topics.append(args[i])
        i += 1
    if not topics:
        _missing_args("subpub")

    rest = list(args[i:])
    _check_unknown_flags(rest, _flags("subpub"), usage)
    username, password = _extract_auth(rest)
    fields = _extract_fields(rest)
    subid = _extract_flag(rest, "--subid")
    _check_extra_positional(rest, usage)
    ws_url = _normalize_ws_url(url)

    topic_arg: str | list[str] = topics[0] if len(topics) == 1 else topics

    _run_client_command(
        ws_url,
        _subscribe_service(ws_url, service, "subpub", None, None, subid, topic=topic_arg, fields=fields, username=username, password=password),
        "\nSubscription stopped.",
    )


def _cmd_stream() -> None:
    args = sys.argv[2:]
    usage = _synopsis("stream")
    if len(args) < 2:
        _missing_args("stream")

    url = args[0].rstrip("/")
    service = args[1]
    rest = args[2:]
    _check_unknown_flags(rest, _flags("stream"), usage)
    username, password = _extract_auth(rest)
    filter_expr = _extract_flag(rest, "--filter")
    fields = _extract_fields(rest)
    ref = _extract_flag(rest, "--ref")
    maxcount_str = _extract_flag(rest, "--maxcount")
    maxcount = int(maxcount_str) if maxcount_str else None
    before = "--before" in rest
    if before:
        rest.remove("--before")
    if ref is None and not maxcount and not before:
        from mkio._ref import next_ref
        ref = next_ref()
    subid = _extract_flag(rest, "--subid")
    _check_extra_positional(rest, usage)
    ws_url = _normalize_ws_url(url)

    _run_client_command(
        ws_url,
        _subscribe_service(ws_url, service, "stream", filter_expr, ref, subid, fields=fields, maxcount=maxcount, before=before, username=username, password=password),
        "\nSubscription stopped.",
    )


def _cmd_query() -> None:
    args = sys.argv[2:]
    usage = _synopsis("query")
    if len(args) < 2:
        _missing_args("query")

    url = args[0].rstrip("/")
    service = args[1]
    rest = args[2:]
    _check_unknown_flags(rest, _flags("query"), usage)
    username, password = _extract_auth(rest)
    filter_expr = _extract_flag(rest, "--filter")
    fields = _extract_fields(rest)
    subid = _extract_flag(rest, "--subid")
    snapshot, updates = _parse_mode_flags(rest)
    _check_extra_positional(rest, usage)
    ws_url = _normalize_ws_url(url)

    _run_client_command(
        ws_url,
        _subscribe_service(ws_url, service, "query", filter_expr, None, subid, snapshot=snapshot, updates=updates, fields=fields, username=username, password=password),
        "\nSubscription stopped.",
    )


def _parse_mode_flags(args: list[str]) -> tuple[bool, bool]:
    snapshot_only = "--snapshotOnly" in args
    update_only = "--updateOnly" in args
    if snapshot_only and update_only:
        print("Error: --snapshotOnly and --updateOnly are mutually exclusive")
        sys.exit(1)
    if snapshot_only:
        args.remove("--snapshotOnly")
        return True, False
    if update_only:
        args.remove("--updateOnly")
        return False, True
    return True, True


def _extract_fields(args: list[str]) -> list[str] | None:
    raw = _extract_flag(args, "--fields")
    if raw is None:
        return None
    return [f.strip() for f in raw.split(",") if f.strip()]


def _extract_flag(args: list[str], flag: str) -> str | None:
    if flag not in args:
        return None
    idx = args.index(flag)
    if idx + 1 >= len(args):
        print(f"Error: {flag} requires a value")
        sys.exit(1)
    value = args[idx + 1]
    del args[idx:idx + 2]
    return value


def _extract_auth(args: list[str]) -> tuple[str | None, str | None]:
    if "--password" in args:
        print("Error: --password is not supported (visible in process list)")
        print("  Use the MKIO_PASSWORD environment variable instead")
        sys.exit(1)
    username = _extract_flag(args, "--username")
    if not username:
        return None, None
    password = os.environ.get("MKIO_PASSWORD")
    if not password:
        import getpass
        password = getpass.getpass(f"Password for {username}: ")
    return username, password


async def _authenticate(
    client: Any, username: str, password: str,
) -> None:
    try:
        await client.auth({"username": username, "password": password})
    except ValueError as exc:
        print(f"Authentication failed: {exc}")
        sys.exit(1)


async def _subscribe_service(
    ws_url: str,
    service: str,
    protocol: str,
    filter_expr: str | None,
    ref: str | None = None,
    subid: str | None = None,
    snapshot: bool = True,
    updates: bool = True,
    fields: list[str] | None = None,
    topic: str | list[str] | None = None,
    maxcount: int | None = None,
    before: bool = False,
    username: str | None = None,
    password: str | None = None,
) -> None:
    from mkio.client import MkioClient

    async with MkioClient(ws_url, reconnect=True) as client:
        if username:
            await _authenticate(client, username, password)
        async for msg in client.subscribe(service, protocol, topic=topic, filter=filter_expr, ref=ref, subid=subid, snapshot=snapshot, updates=updates, fields=fields, maxcount=maxcount, before=before):
            if msg.get("type") == "nack":
                message = msg.get("message", "subscription rejected")
                print(f"Error: {message}")
                if "Protocol mismatch" in message:
                    for proto, hint in _PROTOCOL_CLI_HINT.items():
                        if f"is {proto!r}" in message or f"is '{proto}'" in message:
                            print(f"Hint: try {hint}")
                            break
                sys.exit(1)
            _print_subscribe_message(msg)


def _print_subscribe_message(data: dict[str, Any]) -> None:
    """Pretty-print a subscription message."""
    msg_type = data.get("type", "")
    ref = data.get("ref", "")
    now = local_ts()

    is_tty = sys.stdout.isatty()
    ver_suffix = f" ref={ref}" if ref else ""

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
        # A version cursor move says so, so an undo is not read as a fresh edit.
        cause = f" ({data['cause']})" if data.get("cause") else ""
        if is_tty:
            color = "\033[36m" if op == "insert" else "\033[33m" if op == "update" else "\033[31m"
            print(f"{color}[{now}] UPDATE {op}{cause}{ver_suffix}\033[0m")
        else:
            print(f"[{now}] UPDATE {op}{cause}{ver_suffix}")
        print(f"  {json.dumps(row, default=str)}")
    else:
        print(f"[{now}] {msg_type}: {json.dumps(data, default=str)}")


def _check_unknown_flags(args: list[str], known: set[str], usage: str) -> None:
    """Error and exit if args contain any unrecognised --flags."""
    for arg in args:
        if arg.startswith("--") and arg not in known:
            if arg == "--password":
                print("Error: --password is not supported (visible in process list)")
                print("  Use the MKIO_PASSWORD environment variable instead")
                sys.exit(1)
            if known:
                import difflib
                close = difflib.get_close_matches(arg, known, n=1, cutoff=0.5)
                hint = f" Did you mean {close[0]!r}?" if close else ""
                valid = ", ".join(sorted(known))
                print(f"Unknown option: {arg}.{hint}")
                print(textwrap.fill(f"Valid options: {valid}", _HELP_WIDTH, subsequent_indent=" " * 15,
                                    break_on_hyphens=False))
            else:
                print(f"Unknown option: {arg} (this command takes no options)")
            _print_usage(usage)
            sys.exit(1)


def _check_extra_positional(args: list[str], usage: str) -> None:
    """Error and exit if there are leftover positional args after flag extraction."""
    extra = [a for a in args if not a.startswith("--")]
    if extra:
        print(f"Error: unexpected argument(s): {' '.join(extra)}")
        _print_usage(usage)
        sys.exit(1)


# ---- helpers ----------------------------------------------------------------

def _connection_error(ws_url: str, exc: Exception) -> None:
    """Print a helpful connection error and exit."""
    base_url = ws_url.rsplit("/ws", 1)[0]
    print(f"Error: could not connect to {base_url}")
    print(f"  {type(exc).__name__}: {exc}")
    print("  Is the mkio server running?")
    sys.exit(1)


def _run_client_command(
    ws_url: str,
    coro: Any,
    interrupt_msg: str | None = None,
) -> None:
    """Run an async client command with connection error handling."""
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        if interrupt_msg:
            print(interrupt_msg)
    except Exception as exc:
        import aiohttp
        if isinstance(exc, (aiohttp.ClientError, OSError)):
            if _TRACEBACK:
                raise
            _connection_error(ws_url, exc)
        raise


def _serve_error(config_path: str, exc: Exception) -> None:
    """Print a helpful server start error and exit."""
    import errno as _errno
    import tomllib
    if isinstance(exc, tomllib.TOMLDecodeError):
        print(f"Error: invalid TOML in {config_path}")
        print(f"  {exc}")
    elif isinstance(exc, ValueError):
        print(f"Error: invalid config: {exc}")
    elif isinstance(exc, OSError) and exc.errno == _errno.EADDRINUSE:
        print(f"Error: address already in use")
        print(f"  {exc}")
        print(f"  Stop the other process or change the port in {config_path}")
    elif isinstance(exc, OSError) and exc.errno == _errno.EACCES:
        print(f"Error: permission denied")
        print(f"  {exc}")
        print(f"  Try a port >= 1024 or run with elevated privileges.")
    elif isinstance(exc, OSError):
        print(f"Error: could not start server")
        print(f"  {exc}")
    else:
        raise exc
    sys.exit(1)


def _normalize_url(url: str) -> str:
    """Normalize a URL: default to http:// and port 80."""
    if url.isdigit():
        url = "localhost:" + url
    if not (url.startswith("http://") or url.startswith("https://")
            or url.startswith("ws://") or url.startswith("wss://")):
        url = "http://" + url
    # If no port specified, add :80 for http/ws schemes.
    # Split off scheme, check host:port portion.
    scheme_end = url.index("://") + 3
    rest = url[scheme_end:]
    # rest is host[:port][/path...]
    slash = rest.find("/")
    hostport = rest[:slash] if slash >= 0 else rest
    if ":" not in hostport:
        scheme = url[:scheme_end]
        if scheme in ("http://", "ws://"):
            hostport += ":80"
        elif scheme in ("https://", "wss://"):
            hostport += ":443"
        tail = rest[slash:] if slash >= 0 else ""
        url = scheme + hostport + tail
    return url


def _normalize_ws_url(url: str) -> str:
    """Normalize a URL to ws:// and append /ws path."""
    url = _normalize_url(url)
    if url.startswith("http://"):
        url = "ws://" + url[7:]
    elif url.startswith("https://"):
        url = "wss://" + url[8:]
    return f"{url}/ws"


def _cmd_reqrep() -> None:
    usage = _synopsis("reqrep")
    args = sys.argv[2:]
    if len(args) < 2:
        _missing_args("reqrep")

    url = args[0]
    service = args[1]
    rest = list(args[2:])
    username, password = _extract_auth(rest)

    data: dict[str, Any] = {}
    if rest:
        if len(rest) == 1 and rest[0].startswith("{"):
            try:
                data = json.loads(rest[0])
            except json.JSONDecodeError as e:
                print(f"Error: invalid JSON: {e}")
                sys.exit(1)
        else:
            for kv in rest:
                if "=" not in kv:
                    print(f"Error: expected key=value, got {kv!r}")
                    _print_usage(usage)
                    sys.exit(1)
                k, v = kv.split("=", 1)
                data[k] = _auto_convert(v)

    ws_url = _normalize_ws_url(url)
    _run_client_command(ws_url, _reqrep_request(ws_url, service, data, username=username, password=password))


async def _reqrep_request(
    ws_url: str, service: str, data: dict[str, Any],
    username: str | None = None, password: str | None = None,
) -> None:
    from mkio.client import MkioClient

    async with MkioClient(ws_url, reconnect=False) as client:
        if username:
            await _authenticate(client, username, password)
        result = await client.request(service, data)
        if result.get("type") == "error":
            message = result.get("message", "unknown error")
            print(f"Error: {message}")
            if "not 'reqrep'" in message:
                for proto, hint in _PROTOCOL_CLI_HINT.items():
                    if f"protocol {proto!r}" in message or f"protocol '{proto}'" in message:
                        print(f"Hint: try {hint}")
                        break
            sys.exit(1)
        if "value" in result:
            print(result["value"])
        elif "row" in result:
            print(json.dumps(result["row"], default=str))
        elif "rows" in result:
            rows = result["rows"]
            for row in rows:
                print(json.dumps(row, default=str))


def _cmd_dbupdate() -> None:
    usage = _synopsis("dbupdate")
    args = sys.argv[2:]
    allow_risky = "--allow-risky" in args
    allow_destructive = "--allow-destructive" in args
    drop_history = "--drop-history" in args
    keep_redo = "--keep-redo" in args
    _check_unknown_flags(args, _flags("dbupdate"), usage)
    args = [a for a in args if a not in _flags("dbupdate")]
    if len(args) > 1:
        print(f"Error: 'dbupdate' takes at most 1 argument (config path), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)

    config_path = args[0] if args else "server.toml"
    from pathlib import Path
    if not Path(config_path).exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    import sqlite3
    from mkio.config import load_config
    from mkio.history import effective_tables
    from mkio.migration import (
        check_schema, collect_redo_garbage, migrate_schema,
        orphan_history_tables, print_change_summary,
    )

    try:
        config = load_config(config_path)
    except Exception as exc:
        if _TRACEBACK:
            raise
        print(f"Error loading config: {exc}")
        sys.exit(1)

    db_path = config["db_path"]
    if not config.get("tables"):
        print("No tables defined in config.")
        sys.exit(0)
    tables = effective_tables(config)

    if db_path == ":memory:":
        print("Error: dbupdate does not apply to in-memory databases")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        orphans = orphan_history_tables(conn, tables)
        if orphans and drop_history:
            for name in orphans:
                conn.execute(f"DROP TABLE {name}")
                print(f"  Dropped orphaned history table: {name}")
            conn.commit()
            orphans = []

        if not keep_redo:
            _discard_redo(conn, tables)

        changes = check_schema(conn, tables)
        if not changes:
            print("Schema is up to date.")
            _print_orphan_note(orphans)
            sys.exit(0)

        print_change_summary(changes, db_path, conn)
        _print_orphan_note(orphans)

        if allow_destructive:
            level = "destructive"
        elif allow_risky:
            level = "risky"
        else:
            level = "safe"

        success = migrate_schema(
            conn=conn,
            config_tables=tables,
            db_path=db_path,
            level=level,
        )
        if not success:
            sys.exit(1)
        print("  Done.")
    finally:
        conn.close()


def _ref_slug(ref: str) -> str:
    """Filename-safe form of a ref string."""
    return ref.replace(" ", "_").replace(":", "").replace(".", "_")


def _archive_cutoff(older_than: str) -> str:
    """Resolve --older-than to a ref cutoff.

    ``90d`` (also ``h``/``m``) counts back from now; anything else is taken as
    a literal ref, so a bare ``20260101`` means "before that date".
    """
    import re
    from datetime import timedelta

    m = re.fullmatch(r"(\d+)([dhm])", older_than.strip())
    if not m:
        return older_than
    seconds = int(m.group(1)) * {"d": 86400, "h": 3600, "m": 60}[m.group(2)]
    dt = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    return dt.strftime("%Y%m%d %H:%M:%S") + ".000000000000"


def _archive_table(
    conn: Any,
    base: str,
    base_config: dict,
    cutoff: str,
    out_dir: str,
    *,
    delete: bool,
    prune_source: bool,
    dry_run: bool,
) -> None:
    """Archive one table's recorded versions older than the cutoff."""
    from pathlib import Path
    from mkio.history import (
        VERSION_COLUMN, history_table_name, primary_key_columns, source_columns,
    )

    hist = history_table_name(base)
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (hist,)
    ).fetchone():
        print(f"  {hist}: table does not exist yet — run 'mkio dbupdate' first")
        return

    pk = primary_key_columns(base_config)
    match = " AND ".join(f"b.{k} = h.{k}" for k in pk)
    # A row's current version is what the base table points at, so archiving it
    # would strand the live row. Only --prune-source, which removes the live row
    # too, may take the whole chain.
    guard = "" if prune_source else (
        f" AND (NOT EXISTS (SELECT 1 FROM {base} b WHERE {match})"
        f" OR h.{VERSION_COLUMN} < (SELECT b.{VERSION_COLUMN} FROM {base} b"
        f" WHERE {match}))"
    )
    order = ", ".join(f"h.{k}" for k in pk)
    cursor = conn.execute(
        f"SELECT h.* FROM {hist} h WHERE h._mkio_ref < ?{guard} "
        f"ORDER BY {order}, h.{VERSION_COLUMN}",
        (cutoff,),
    )
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    if not rows:
        print(f"  {hist}: no archivable versions older than the cutoff")
        return

    refs = sorted(r["_mkio_ref"] for r in rows)
    name = f"{hist}_{_ref_slug(refs[0])}_{_ref_slug(refs[-1])}.csv"
    dest = Path(out_dir) / name

    if dry_run:
        print(f"  {hist}: {len(rows):,} versions would be written to {dest}")
    else:
        # The CSV lands on disk before anything is removed, so a failed write
        # leaves the history intact.
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        with open(dest, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in rows:
                writer.writerow([row[c] for c in columns])
            f.flush()
            os.fsync(f.fileno())
        print(f"  {hist}: {len(rows):,} versions -> {dest}")
    print("    archiving a version removes that much undo depth")

    if not delete:
        return

    # Delete exactly the rows that were archived, addressed by key and version,
    # so a concurrent undo cannot widen the set.
    archived_keys = [tuple(r[c] for c in pk) + (r[VERSION_COLUMN],) for r in rows]
    key_filter = " AND ".join(f"{k} = ?" for k in list(pk) + [VERSION_COLUMN])
    if dry_run:
        print(f"    {len(rows):,} versions would be purged")
    else:
        conn.executemany(f"DELETE FROM {hist} WHERE {key_filter}", archived_keys)
        print(f"    purged {len(rows):,} versions")

    if not prune_source:
        return

    # A source row is removable only when its whole chain has just been archived
    # and the live row still matches the newest archived version exactly.
    src_cols = source_columns(base_config)
    compare = list(src_cols) + [VERSION_COLUMN]
    where = " AND ".join(f"{k} = ?" for k in pk)

    newest: dict[tuple, Any] = {}
    for row in rows:
        key = tuple(row[k] for k in pk)
        current = newest.get(key)
        if current is None or row[VERSION_COLUMN] > current[VERSION_COLUMN]:
            newest[key] = row

    archived_versions: dict[tuple, set] = {}
    for row in rows:
        archived_versions.setdefault(tuple(row[k] for k in pk), set()).add(
            row[VERSION_COLUMN]
        )

    pruned = 0
    for key, row in newest.items():
        # Versions the purge leaves behind, evaluated the same way in a dry run.
        remaining = [
            r[0] for r in conn.execute(
                f"SELECT {VERSION_COLUMN} FROM {hist} WHERE {where}", key
            )
        ]
        if any(v not in archived_versions[key] for v in remaining):
            continue  # part of the chain survives, so the row is still versioned
        live = conn.execute(f"SELECT * FROM {base} WHERE {where}", key).fetchone()
        if live is None:
            continue
        if any(live[c] != row[c] for c in compare):
            continue
        if not dry_run:
            conn.execute(f"DELETE FROM {base} WHERE {where}", key)
        pruned += 1

    verb = "would be pruned" if dry_run else "pruned"
    print(f"    {pruned:,} source rows {verb} from {base}")


def _cmd_archive() -> None:
    """Two modes: ``--older-than`` keeps the history-only archiver; anything
    else is a row archive of the tables that opted in."""
    if "--older-than" in sys.argv[2:]:
        _cmd_archive_history()
    else:
        _cmd_archive_rows()


def _looks_like_url(arg: str) -> bool:
    from pathlib import Path
    if Path(arg).exists():
        return False
    return (
        "://" in arg or arg.isdigit()
        or bool(re.fullmatch(r"[A-Za-z0-9.\-\[\]:]+:\d+(/.*)?", arg))
    )


def _confirm(prompt: str) -> bool:
    if not sys.stdin.isatty():
        print("Error: pass --yes to confirm (stdin is not a terminal)")
        sys.exit(1)
    try:
        answer = input(f"{prompt} [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in ("y", "yes")


def _print_archive_summary(summary: dict[str, Any], *, cutoff_desc: str, dry_run: bool) -> None:
    tables = summary.get("tables", {})
    verb = "would be archived" if dry_run else "to archive"
    print(f"Cutoff: {cutoff_desc}")
    for name, t in tables.items():
        parts = [f"{t['rows']:,} rows"]
        if t.get("history") is not None:
            parts.append(f"{t['history']:,} history rows")
        for comp, n in (t.get("companions") or {}).items():
            parts.append(f"{n:,} {comp} rows")
        if t.get("cutoff_column"):
            scope = f"{t['cutoff_column']} < {t['cutoff_value']}"
        else:
            scope = "whole table, cutoff ignored"
        print(f"  {name}: {', '.join(parts)} {verb} ({scope})")
    if summary.get("dir"):
        print(f"  Written to {summary['dir']}")


def _cmd_archive_rows() -> None:
    usage = _synopsis("archive")
    args = sys.argv[2:]
    dry_run = "--dry-run" in args
    assume_yes = "--yes" in args
    everything = "--all" in args
    args = [a for a in args if a not in ("--dry-run", "--yes", "--all")]
    tables_arg = _extract_flag(args, "--tables")
    group = _extract_flag(args, "--group")
    cutoff = _extract_flag(args, "--cutoff")
    cutoff_literal = _extract_flag(args, "--cutoff-literal")
    out_dir = _extract_flag(args, "--out") or "."
    username, password = _extract_auth(args)
    _check_unknown_flags(args, _flags("archive"), usage)
    if len(args) > 1:
        print(f"Error: 'archive' takes one argument (config path or server url), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)
    if everything and (tables_arg or group):
        print("Error: --all cannot be combined with --tables or --group")
        sys.exit(1)
    if cutoff and cutoff_literal:
        print("Error: give either --cutoff or --cutoff-literal, not both")
        sys.exit(1)
    if everything:
        group = "all"
    tables = [t.strip() for t in tables_arg.split(",") if t.strip()] if tables_arg else None
    target = args[0] if args else "server.toml"
    cutoff_desc = cutoff_literal or cutoff or "(none — whole tables only)"

    from mkio.archive import ArchiveError
    from pathlib import Path

    if _looks_like_url(target):
        ws_url = _normalize_ws_url(target)
        out_abs = str(Path(out_dir).resolve())
        _run_client_command(ws_url, _archive_via_server(
            ws_url, tables, group, cutoff, cutoff_literal, out_abs, dry_run, assume_yes,
            cutoff_desc, username=username, password=password,
        ))
        return

    if not Path(target).exists():
        print(f"Config file not found: {target}")
        sys.exit(1)
    from mkio.archive import archive_offline, parse_cutoff
    from mkio.config import load_config
    try:
        config = load_config(target)
        instant = parse_cutoff(cutoff) if cutoff else None
        kwargs = dict(
            tables=tables, group=group, cutoff=instant, cutoff_literal=cutoff_literal,
            cutoff_given=cutoff or cutoff_literal, out_dir=out_dir,
            mkio_version=_mkio_version(),
        )
        preview = archive_offline(config, dry_run=True, **kwargs)
        _print_archive_summary(preview, cutoff_desc=cutoff_desc, dry_run=dry_run)
        if dry_run:
            return
        if not any(t["rows"] for t in preview["tables"].values()):
            print("Nothing to archive.")
            return
        print("  The server must be stopped: offline deletes bypass its change bus.")
        if not assume_yes and not _confirm("Archive and delete these rows?"):
            print("Aborted.")
            sys.exit(1)
        result = archive_offline(config, dry_run=False, **kwargs)
        total = sum(t["rows"] for t in result["tables"].values())
    except ArchiveError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    except Exception as exc:
        if _TRACEBACK:
            raise
        print(f"Error: {exc}")
        sys.exit(1)
    print(f"  Archived {total:,} rows to {result['dir']}")
    print("  Done.")


def _mkio_version() -> str:
    import importlib.metadata
    try:
        return importlib.metadata.version("mkio")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


async def _archive_via_server(
    ws_url: str, tables: list[str] | None, group: str | None, cutoff: str | None,
    cutoff_literal: str | None, out_dir: str, dry_run: bool, assume_yes: bool,
    cutoff_desc: str, *, username: str | None, password: str | None,
) -> None:
    from mkio.client import MkioClient

    spec: dict[str, Any] = {
        "tables": tables, "group": group, "cutoff": cutoff,
        "cutoff_literal": cutoff_literal, "out": out_dir,
    }
    async with MkioClient(ws_url, reconnect=False) as client:
        if username:
            await _authenticate(client, username, password)
        preview = await client.request("_mkio", {"archive": {**spec, "dry_run": True}})
        if preview.get("type") == "error":
            print(f"Error: {preview.get('message', 'unknown error')}")
            sys.exit(1)
        _print_archive_summary(preview["row"], cutoff_desc=cutoff_desc, dry_run=dry_run)
        if dry_run:
            return
        if not any(t["rows"] for t in preview["row"]["tables"].values()):
            print("Nothing to archive.")
            return
        print(f"  Files are written by the server, under {out_dir} on its host.")
        if not assume_yes and not _confirm("Archive and delete these rows?"):
            print("Aborted.")
            sys.exit(1)
        result = await client.request("_mkio", {"archive": {**spec, "dry_run": False}})
        if result.get("type") == "error":
            print(f"Error: {result.get('message', 'unknown error')}")
            sys.exit(1)
        total = sum(t["rows"] for t in result["row"]["tables"].values())
        print(f"  Archived {total:,} rows to {result['row']['dir']}")
        print("  Done.")


def _cmd_restore() -> None:
    usage = _synopsis("restore")
    args = sys.argv[2:]
    dry_run = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]
    tables_arg = _extract_flag(args, "--tables")
    _check_unknown_flags(args, _flags("restore"), usage)
    if len(args) != 2:
        print(f"Error: 'restore' takes 2 arguments (config path, archive directory), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)
    config_path, archive_dir = args
    from pathlib import Path
    if not Path(config_path).exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    tables = [t.strip() for t in tables_arg.split(",") if t.strip()] if tables_arg else None

    from mkio.archive import ArchiveError, restore_offline
    from mkio.config import load_config
    try:
        config = load_config(config_path)
        result = restore_offline(config, archive_dir, tables=tables, dry_run=dry_run)
    except ArchiveError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    except Exception as exc:
        if _TRACEBACK:
            raise
        print(f"Error: {exc}")
        sys.exit(1)
    verb = "would be restored" if dry_run else "restored"
    print(f"Restoring from {archive_dir}" + (" (dry run)" if dry_run else ""))
    for name, t in result["tables"].items():
        parts = [f"{t['rows']:,} rows"]
        if t.get("history"):
            parts.append(f"{t['history']:,} history rows")
        for comp, n in (t.get("companions") or {}).items():
            parts.append(f"{n:,} {comp} rows")
        note = f", {t['replaced']:,} existing replaced" if t.get("replaced") else ""
        print(f"  {name}: {', '.join(parts)} {verb}{note}")
        if t.get("history_skipped"):
            print(f"    note: {t['history_skipped']}")
    print("  The server must be stopped while restoring." if not dry_run else "")
    print("  Done." if not dry_run else "  Nothing changed.")


def _cmd_archive_history() -> None:
    usage = _synopsis("archive", 1)
    args = sys.argv[2:]
    delete = "--delete" in args
    prune_source = "--prune-source" in args
    dry_run = "--dry-run" in args
    assume_yes = "--yes" in args
    args = [a for a in args
            if a not in ("--delete", "--prune-source", "--dry-run", "--yes")]
    table_arg = _extract_flag(args, "--table")
    older_than = _extract_flag(args, "--older-than")
    out_dir = _extract_flag(args, "--out") or "."
    _check_unknown_flags(args, _flags("archive", 1), usage)
    if len(args) > 1:
        print(f"Error: 'archive' takes at most 1 argument (config path), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)
    if not older_than:
        print("Error: --older-than is required (e.g. --older-than 90d)")
        _print_usage(usage)
        sys.exit(1)
    if prune_source and not delete:
        delete = True
    if prune_source and not dry_run and not assume_yes:
        print("Error: --prune-source deletes live rows — pass --yes to confirm")
        print("  (or --dry-run to see what it would remove)")
        sys.exit(1)

    config_path = args[0] if args else "server.toml"
    from pathlib import Path
    if not Path(config_path).exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    import sqlite3
    from mkio.config import load_config
    from mkio.history import base_table_name, is_history_table, versioned_tables

    try:
        config = load_config(config_path)
    except Exception as exc:
        if _TRACEBACK:
            raise
        print(f"Error loading config: {exc}")
        sys.exit(1)

    db_path = config["db_path"]
    if db_path == ":memory:":
        print("Error: archive does not apply to in-memory databases")
        sys.exit(1)
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    versioned = versioned_tables(config)
    if not versioned:
        print("No versioned tables in config — nothing to archive.")
        print("  Set versioned = true on a table to record its change history.")
        sys.exit(0)

    if table_arg:
        base = base_table_name(table_arg) if is_history_table(table_arg) else table_arg
        if base not in versioned:
            available = ", ".join(sorted(versioned))
            print(f"Error: table {table_arg!r} is not versioned. Versioned tables: {available}")
            sys.exit(1)
        targets = {base: versioned[base]}
    else:
        targets = versioned

    cutoff = _archive_cutoff(older_than)
    print(f"Archiving history rows before {cutoff}" + (" (dry run)" if dry_run else ""))
    if prune_source and not dry_run:
        print("  Note: pruning source rows bypasses the change bus — run this")
        print("  against a stopped server so live subscribers stay consistent.")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        for base, base_config in targets.items():
            _archive_table(
                conn, base, base_config, cutoff, out_dir,
                delete=delete, prune_source=prune_source, dry_run=dry_run,
            )
        # Nothing is committed until every table is written, so a failure
        # part-way through leaves the history exactly as it was.
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    except (sqlite3.Error, OSError) as exc:
        conn.rollback()
        if _TRACEBACK:
            raise
        print(f"Error: {exc}")
        sys.exit(1)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print("  Done.")


def _discard_redo(conn: Any, tables: dict) -> None:
    """Drop every versioned table's redo stack, reporting what went.

    Undo/redo state is deliberately transient: it survives until the next
    maintenance run. Pass --keep-redo to leave it alone.
    """
    from mkio.migration import collect_redo_garbage

    dropped = collect_redo_garbage(conn, tables)
    if not dropped:
        return
    print()
    for hist, (orphaned, dangling) in dropped.items():
        parts = []
        if orphaned:
            parts.append(f"{orphaned:,} rows of fully undone records")
        if dangling:
            parts.append(f"{dangling:,} redo rows above the current version")
        print(f"  Discarded redo history from {hist}: {', '.join(parts)}")
    print("  These are no longer redo-able. Use --keep-redo to retain them.")
    print()


def _print_orphan_note(orphans: list[str]) -> None:
    """Note history tables whose base table is no longer versioned."""
    if not orphans:
        return
    print(f"  Note: history retained for tables no longer versioned: {', '.join(orphans)}")
    print("  These are left in place. To remove them: mkio dbupdate --drop-history")
    print()


def _cmd_check() -> None:
    usage = _synopsis("check")
    args = sys.argv[2:]
    if len(args) < 1:
        _missing_args("check")

    url = args[0]
    rest = list(args[1:])
    username, password = _extract_auth(rest)
    data: dict[str, Any] = {}
    for kv in rest:
        if "=" not in kv:
            print(f"Error: expected key=value, got {kv!r}")
            _print_usage(usage)
            sys.exit(1)
        k, v = kv.split("=", 1)
        if k not in ("version", "protocol", "mkio", "expr"):
            print(f"Error: unknown key {k!r} (expected version, protocol, mkio, or expr)")
            sys.exit(1)
        data[k] = v

    ws_url = _normalize_ws_url(url)
    _run_client_command(ws_url, _check_request(ws_url, data, username=username, password=password))


async def _check_request(
    ws_url: str, data: dict[str, Any],
    username: str | None = None, password: str | None = None,
) -> None:
    from mkio.client import MkioClient

    async with MkioClient(ws_url, reconnect=False) as client:
        if username:
            await _authenticate(client, username, password)
        result = await client.request("_mkio", data)
        if result.get("type") == "error":
            print(f"Error: {result.get('message', 'unknown error')}")
            sys.exit(1)
        row = result.get("row", {})
        print(f"  name:        {row.get('name') or '(not set)'}")
        print(f"  version:     {row.get('version') or '(not set)'}")
        print(f"  mkio:        {row.get('mkio', '?')}")
        print(f"  protocol:    {row.get('protocol', '?')}")
        if "compatible" in row:
            status = "yes" if row["compatible"] else "NO"
            print(f"  compatible:  {status}")
            for k, v in row.get("compatibility", {}).items():
                label = "ok" if v else "MISMATCH"
                print(f"    {k}: {row.get(k, '?')} ({label})")
            sys.exit(0 if row["compatible"] else 1)


def _cmd_schema() -> None:
    usage = _synopsis("schema")
    args = sys.argv[2:]
    _check_unknown_flags(args, _flags("schema"), usage)
    username, password = _extract_auth(args)
    if len(args) < 2:
        _missing_args("schema")
    if len(args) > 2:
        print(f"Error: 'schema' takes exactly 2 arguments (url table), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)

    url = args[0]
    table = args[1]
    ws_url = _normalize_ws_url(url)
    _run_client_command(ws_url, _schema_request(ws_url, table, username=username, password=password))


async def _schema_request(
    ws_url: str, table: str,
    username: str | None = None, password: str | None = None,
) -> None:
    from mkio.client import MkioClient

    async with MkioClient(ws_url, reconnect=False) as client:
        if username:
            await _authenticate(client, username, password)
        result = await client.request("_mkio", {"table": table})
        if result.get("type") == "error":
            print(f"Error: {result.get('message', 'unknown error')}")
            sys.exit(1)
        row = result.get("row", {})
        columns = row.get("columns", [])
        if not columns:
            print(f"Table {table!r}: no columns")
            return
        name_w = max(len(c["name"]) for c in columns)
        type_w = max(len(c["type"]) for c in columns)
        name_w = max(name_w, 6)  # "COLUMN"
        type_w = max(type_w, 4)  # "TYPE"
        print(f"{'COLUMN':<{name_w}}  {'TYPE':<{type_w}}  FLAGS")
        print(f"{'-' * name_w}  {'-' * type_w}  {'-' * 20}")
        for col in columns:
            flags = []
            if col["pk"]:
                flags.append("pk")
            if col["notnull"]:
                flags.append("not null")
            if col["dflt_value"] is not None:
                flags.append(f"default={col['dflt_value']}")
            print(f"{col['name']:<{name_w}}  {col['type']:<{type_w}}  {', '.join(flags)}")
        if row.get("versioned"):
            print()
            print(f"  Versioned — changes are recorded in {row['history_table']}")
            print(f"  e.g. mkio schema <url> {row['history_table']}")
        elif row.get("history_of"):
            print()
            print(f"  History of {row['history_of']!r} — one row per recorded change")


def _cmd_init() -> None:
    usage = _synopsis("init")
    args = sys.argv[2:]
    _check_unknown_flags(args, _flags("init"), usage)
    no_static = "--no-static" in args
    args = [a for a in args if a != "--no-static"]
    if len(args) > 1:
        print(f"Error: 'init' takes at most 1 argument (directory), got {len(args)}")
        _print_usage(usage)
        sys.exit(1)

    from pathlib import Path
    from mkio.scaffold import init

    target = args[0] if args else "."
    try:
        created = init(target, no_static=no_static)
    except FileExistsError as e:
        print(f"Error: {e}")
        sys.exit(1)

    cwd = Path.cwd()
    for p in created:
        try:
            rel = p.relative_to(cwd)
        except ValueError:
            rel = p
        print(f"Created {rel}")

    target_path = Path(target)
    print()
    print("Start the server with:")
    if target_path.resolve() == cwd.resolve():
        print("  mkio serve")
    else:
        print(f"  cd {target_path} && mkio serve")


def _cmd_adduser() -> None:
    usage = _synopsis("adduser")
    args = sys.argv[2:]
    _check_unknown_flags(args, set(), usage)
    if len(args) < 2:
        _missing_args("adduser")
    if len(args) > 3:
        print(f"Error: 'adduser' takes 2-3 arguments, got {len(args)}")
        _print_usage(usage)
        sys.exit(1)

    username = args[0]
    role = args[1]
    config_path = args[2] if len(args) >= 3 else "server.toml"

    from pathlib import Path
    if not Path(config_path).exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    from mkio.config import load_config
    try:
        config = load_config(config_path)
    except Exception as exc:
        if _TRACEBACK:
            raise
        print(f"Error loading config: {exc}")
        sys.exit(1)

    if "_mkio_users" not in config.get("tables", {}):
        print("Error: _mkio_users table not defined in config")
        sys.exit(1)

    password = os.environ.get("MKIO_PASSWORD")
    if password is None:
        import getpass
        password = getpass.getpass("Password: ")
        if not password:
            print("Error: password cannot be empty")
            sys.exit(1)
        confirm = getpass.getpass("Confirm: ")
        if password != confirm:
            print("Error: passwords do not match")
            sys.exit(1)
    elif not password:
        print("Error: MKIO_PASSWORD is set but empty")
        sys.exit(1)

    from mkio.auth import hash_password
    hashed = hash_password(password)

    import sqlite3
    db_path = config.get("db_path", "mkio.db")
    if db_path == ":memory:":
        print("Error: adduser does not work with in-memory databases")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "INSERT OR REPLACE INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
            (username, hashed, role),
        )
        conn.commit()
        print(f"User '{username}' added with role '{role}'")
    except sqlite3.OperationalError as exc:
        if _TRACEBACK:
            raise
        print(f"Error: {exc}")
        print("  Make sure the database and _mkio_users table exist (run 'mkio serve' or 'mkio dbupdate' first)")
        sys.exit(1)
    finally:
        conn.close()


def _cmd_hashpass() -> None:
    usage = _synopsis("hashpass")
    args = sys.argv[2:]
    _check_unknown_flags(args, set(), usage)
    if args:
        print("Error: 'hashpass' takes no arguments")
        _print_usage(usage)
        sys.exit(1)

    password = os.environ.get("MKIO_PASSWORD")
    if password is None:
        import getpass
        password = getpass.getpass("Password: ")
        if not password:
            print("Error: password cannot be empty")
            sys.exit(1)
        confirm = getpass.getpass("Confirm: ")
        if password != confirm:
            print("Error: passwords do not match")
            sys.exit(1)
    elif not password:
        print("Error: MKIO_PASSWORD is set but empty")
        sys.exit(1)

    from mkio.auth import hash_password
    print(hash_password(password))


if __name__ == "__main__":
    main()
