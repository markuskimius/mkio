"""Python client library for mkio microservices."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import aiohttp

from mkio._json import dumps, loads

# ---------------------------------------------------------------------------
# Ref generator (client-side, same format as server versions)
# ---------------------------------------------------------------------------

_last_time_ns: int = 0
_counter: int = 0


def make_ref() -> str:
    """Generate a ref string in YYYYMMDD HH:mm:ss.mmmuuunnnppp UTC format."""
    global _last_time_ns, _counter

    now_ns = time.time_ns()
    if now_ns <= _last_time_ns:
        _counter += 1
    else:
        _counter = 0
        _last_time_ns = now_ns

    dt = datetime.fromtimestamp(now_ns / 1_000_000_000, tz=timezone.utc)
    total_ns = now_ns % 1_000_000_000
    ms = total_ns // 1_000_000
    us = (total_ns // 1_000) % 1_000
    ns = total_ns % 1_000
    ps = _counter % 1000

    return dt.strftime("%Y%m%d %H:%M:%S") + f".{ms:03d}{us:03d}{ns:03d}{ps:03d}"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class MkioClient:
    """Async WebSocket client for mkio microservices.

    Supports auto-reconnect with version tracking for delta recovery.
    """

    def __init__(
        self,
        url: str,
        reconnect: bool = True,
        backoff_base: float = 0.1,
        backoff_max: float = 30.0,
    ) -> None:
        self.url = url
        self.reconnect = reconnect
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._subscriptions: dict[str, _Subscription] = {}
        self._receive_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.url)
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def close(self) -> None:
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()

    async def __aenter__(self) -> MkioClient:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def send(
        self,
        service: str,
        data: dict[str, Any],
        ref: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Send a transaction message and wait for the result.

        Extra keyword arguments (e.g. op="place") are included in the message.
        """
        if ref is None:
            ref = make_ref()
        msg: dict[str, Any] = {"service": service, "data": data, "ref": ref, **kwargs}

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[ref] = future

        assert self._ws is not None
        await self._ws.send_bytes(dumps(msg))
        return await future

    async def subscribe(
        self,
        service: str,
        filter: str | None = None,
        ref: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to a service. Yields messages (snapshot, delta, update).

        Pass ``ref`` from a previous message to resume from that point.
        """
        sub = _Subscription(
            service=service,
            filter=filter,
            ref=ref,
            queue=asyncio.Queue(),
        )
        self._subscriptions[service] = sub

        msg: dict[str, Any] = {"service": service, "type": "subscribe"}
        if filter:
            msg["filter"] = filter
        if ref or sub.ref:
            msg["ref"] = ref or sub.ref

        assert self._ws is not None
        await self._ws.send_bytes(dumps(msg))

        try:
            while True:
                item = await sub.queue.get()
                yield item
        except asyncio.CancelledError:
            pass

    async def check(self, service: str, version: str) -> dict[str, Any]:
        """Check if a transaction committed (for recovery after reconnect)."""
        ref = make_ref()
        msg = {"service": service, "type": "check", "version": version, "ref": ref}

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[ref] = future

        assert self._ws is not None
        await self._ws.send_bytes(dumps(msg))
        return await future

    async def _receive_loop(self) -> None:
        """Receive messages and dispatch to pending futures or subscription queues."""
        assert self._ws is not None
        backoff = self.backoff_base

        try:
            async for ws_msg in self._ws:
                if ws_msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                    data = loads(ws_msg.data)
                    self._dispatch(data)
                    backoff = self.backoff_base  # Reset on successful receive
                elif ws_msg.type == aiohttp.WSMsgType.ERROR:
                    break
        except (asyncio.CancelledError, ConnectionError):
            pass

        # Connection lost — attempt reconnect
        if self.reconnect:
            await self._reconnect(backoff)

    def _dispatch(self, data: dict[str, Any]) -> None:
        """Route a received message to the appropriate handler."""
        msg_type = data.get("type", "")
        ref = data.get("ref")
        service = data.get("service", "")

        # Route to pending future (transaction results, check results)
        if ref and ref in self._pending:
            future = self._pending.pop(ref)
            if not future.done():
                future.set_result(data)
            return

        # Route to subscription queue
        if service and service in self._subscriptions:
            sub = self._subscriptions[service]
            # Track ref for recovery on reconnect
            if ref:
                sub.ref = ref
            try:
                sub.queue.put_nowait(data)
            except asyncio.QueueFull:
                pass

    async def _reconnect(self, backoff: float) -> None:
        """Reconnect with exponential backoff and re-subscribe."""
        while True:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.backoff_max)
            try:
                if self._ws:
                    await self._ws.close()
                if self._session:
                    await self._session.close()

                self._session = aiohttp.ClientSession()
                self._ws = await self._session.ws_connect(self.url)

                # Re-subscribe with stored refs for recovery
                for service, sub in self._subscriptions.items():
                    msg: dict[str, Any] = {
                        "service": service,
                        "type": "subscribe",
                    }
                    if sub.filter:
                        msg["filter"] = sub.filter
                    if sub.ref:
                        msg["ref"] = sub.ref
                    await self._ws.send_bytes(dumps(msg))

                # Restart receive loop
                self._receive_task = asyncio.create_task(self._receive_loop())
                return

            except (aiohttp.ClientError, OSError):
                continue


class _Subscription:
    def __init__(
        self,
        service: str,
        filter: str | None,
        ref: str | None,
        queue: asyncio.Queue[dict[str, Any]],
    ) -> None:
        self.service = service
        self.filter = filter
        self.ref = ref
        self.queue = queue
