"""CLI entry point: mkio serve | services | monitor"""

from __future__ import annotations

import asyncio
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
    else:
        _usage()


def _usage() -> None:
    print("Usage:")
    print("  mkio serve <config.toml>         Start a server")
    print("  mkio services <url>              List available services")
    print("  mkio monitor <url> <service>     Monitor a service's messages")
    sys.exit(1)


def _cmd_serve() -> None:
    if len(sys.argv) < 3:
        print("Usage: mkio serve <config.toml>")
        sys.exit(1)
    from mkio.server import serve
    serve(sys.argv[2])


def _cmd_services() -> None:
    if len(sys.argv) < 3:
        print("Usage: mkio services <url>")
        print("  e.g. mkio services http://localhost:8080")
        sys.exit(1)
    url = sys.argv[2].rstrip("/")
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


def _cmd_monitor() -> None:
    if len(sys.argv) < 4:
        print("Usage: mkio monitor <url> <service>")
        print("  e.g. mkio monitor ws://localhost:8080 live_orders")
        sys.exit(1)
    url = sys.argv[2].rstrip("/")
    service = sys.argv[3]

    # Normalize URL: ensure ws:// prefix
    if url.startswith("http://"):
        url = "ws://" + url[7:]
    elif url.startswith("https://"):
        url = "wss://" + url[8:]
    elif not url.startswith("ws://") and not url.startswith("wss://"):
        url = "ws://" + url

    ws_url = f"{url}/ws"

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


if __name__ == "__main__":
    main()
