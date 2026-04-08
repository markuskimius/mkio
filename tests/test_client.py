"""Tests for Python client library."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web

from mkio._json import dumps, loads
from mkio.client import MkioClient, make_ref


# ---- Ref generation --------------------------------------------------------

def test_make_ref_format():
    ref = make_ref()
    assert re.match(r"\d{8} \d{2}:\d{2}:\d{2}\.\d{12}", ref), f"Bad format: {ref}"


def test_make_ref_unique():
    refs = [make_ref() for _ in range(1000)]
    assert len(set(refs)) == 1000


def test_make_ref_monotonic():
    refs = [make_ref() for _ in range(100)]
    for i in range(1, len(refs)):
        assert refs[i] > refs[i - 1]


# ---- Test server for client tests ------------------------------------------

class FakeServer:
    """Minimal WS server that echoes transactions and supports subscribe."""

    def __init__(self) -> None:
        self.app = web.Application()
        self.app.router.add_get("/ws", self.ws_handler)
        self.received: list[dict[str, Any]] = []

    async def ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async for msg in ws:
            if msg.type in (web.WSMsgType.TEXT, web.WSMsgType.BINARY):
                data = loads(msg.data)
                self.received.append(data)
                msg_type = data.get("type", "")
                ref = data.get("ref")
                service = data.get("service", "")

                if msg_type == "subscribe":
                    # Send snapshot
                    resp = {
                        "type": "snapshot",
                        "service": service,
                        "ref": "20260404 00:00:01.000000000000",
                        "rows": [{"id": "1", "name": "test"}],
                    }
                    await ws.send_bytes(dumps(resp))
                elif msg_type == "check":
                    resp = {
                        "type": "result",
                        "service": service,
                        "ref": ref,
                        "ok": True,
                    }
                    await ws.send_bytes(dumps(resp))
                else:
                    # Transaction — server always assigns a ref
                    if not ref:
                        ref = "20260404 00:00:03.000000000000"
                    resp = {
                        "type": "result",
                        "service": service,
                        "ref": ref,
                        "ok": True,
                    }
                    await ws.send_bytes(dumps(resp))

        return ws


@pytest_asyncio.fixture
async def fake_server(aiohttp_server):
    server_obj = FakeServer()
    server = await aiohttp_server(server_obj.app)
    yield server, server_obj


# ---- Client basic operations -----------------------------------------------

async def test_client_send(fake_server):
    server, _ = fake_server
    url = f"ws://localhost:{server.port}/ws"

    async with MkioClient(url, reconnect=False) as client:
        result = await client.send("test_service", {"id": "1", "name": "hello"})
        assert result["ok"] is True
        assert result["type"] == "result"
        assert "ref" in result


async def test_client_send_with_custom_ref(fake_server):
    server, server_obj = fake_server
    url = f"ws://localhost:{server.port}/ws"

    async with MkioClient(url, reconnect=False) as client:
        my_ref = "20260404 12:00:00.000000000000"
        result = await client.send("test_service", {"id": "1"}, ref=my_ref)
        assert result["ref"] == my_ref


async def test_client_send_without_ref(fake_server):
    """When ref is omitted, client should not include it and still get the result."""
    server, server_obj = fake_server
    url = f"ws://localhost:{server.port}/ws"

    async with MkioClient(url, reconnect=False) as client:
        result = await client.send("test_service", {"id": "1", "name": "hello"})
        assert result["ok"] is True
        assert result["type"] == "result"
        # Server should have received the message without a ref field
        sent = server_obj.received[-1]
        assert "ref" not in sent


async def test_client_subscribe(fake_server):
    server, _ = fake_server
    url = f"ws://localhost:{server.port}/ws"

    async with MkioClient(url, reconnect=False) as client:
        msg_count = 0
        async for msg in client.subscribe("test_service"):
            assert msg["type"] == "snapshot"
            assert len(msg["rows"]) == 1
            msg_count += 1
            break  # Just get the first message
        assert msg_count == 1


async def test_client_check(fake_server):
    server, _ = fake_server
    url = f"ws://localhost:{server.port}/ws"

    async with MkioClient(url, reconnect=False) as client:
        result = await client.check("test_service", "20260404 00:00:01.000000000000")
        assert result["ok"] is True
        assert result["type"] == "result"


async def test_client_subscribe_tracks_ref(fake_server):
    """Client should store latest ref from received messages."""
    server, _ = fake_server
    url = f"ws://localhost:{server.port}/ws"

    async with MkioClient(url, reconnect=False) as client:
        async for msg in client.subscribe("test_service"):
            break
        # Ref should be tracked
        sub = client._subscriptions.get("test_service")
        assert sub is not None
        assert sub.ref == "20260404 00:00:01.000000000000"
