"""Python client library for mkio microservices."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from mkio._json import dumps, loads
from mkio._ref import next_ref as make_ref


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
        backoff_max: float = 1.0,
    ) -> None:
        self.url = url
        self.reconnect = reconnect
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._pending_no_ref: list[asyncio.Future[dict[str, Any]]] = []
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

        If *ref* is not provided, the server will assign one.
        Extra keyword arguments (e.g. op="place") are included in the message.
        """
        msg: dict[str, Any] = {"service": service, "data": data, **kwargs}

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()

        if ref is not None:
            msg["ref"] = ref
            self._pending[ref] = future
        else:
            self._pending_no_ref.append(future)

        assert self._ws is not None
        await self._ws.send_bytes(dumps(msg))
        return await future

    async def subscribe(
        self,
        service: str,
        topic: str | None = None,
        filter: str | None = None,
        ref: str | None = None,
        subid: str | None = None,
        snapshot: bool = True,
        updates: bool = True,
        fields: list[str] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to a service. Yields messages (snapshot, update).

        Pass ``topic`` for subpub services (required, the primary key value).
        Pass ``filter`` for query services (expression filter).
        Pass ``ref`` from a previous message to resume from that point (stream only).
        Pass ``subid`` to tag all messages from this subscription.
        Set ``snapshot=False`` to skip the initial snapshot.
        Set ``updates=False`` to receive only the snapshot then stop.
        Pass ``fields`` to receive only the specified columns in each row.
        """
        sub = _Subscription(
            service=service,
            topic=topic,
            filter=filter,
            ref=ref,
            queue=asyncio.Queue(),
            subid=subid,
            snapshot=snapshot,
            updates=updates,
            fields=fields,
        )
        self._subscriptions[service] = sub

        msg: dict[str, Any] = {"service": service, "type": "subscribe"}
        if topic:
            msg["topic"] = topic
        if filter:
            msg["filter"] = filter
        if ref or sub.ref:
            msg["ref"] = ref or sub.ref
        if sub.subid:
            msg["subid"] = sub.subid
        if not snapshot:
            msg["snapshot"] = False
        if not updates:
            msg["updates"] = False
        if fields:
            msg["fields"] = fields

        assert self._ws is not None
        await self._ws.send_bytes(dumps(msg))

        try:
            while True:
                item = await sub.queue.get()
                yield item
                if not updates and item.get("type") == "snapshot":
                    return
        except asyncio.CancelledError:
            pass

    async def check(self, service: str, ref: str) -> dict[str, Any]:
        """Check if a transaction committed (for recovery after reconnect)."""
        msg = {"service": service, "type": "check", "ref": ref}

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

        # Route to no-ref pending (sends where client omitted ref)
        if msg_type in ("result", "error") and self._pending_no_ref:
            future = self._pending_no_ref.pop(0)
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

                # Re-subscribe with stored state
                for service, sub in self._subscriptions.items():
                    msg: dict[str, Any] = {
                        "service": service,
                        "type": "subscribe",
                    }
                    if sub.topic:
                        msg["topic"] = sub.topic
                    if sub.filter:
                        msg["filter"] = sub.filter
                    if sub.ref:
                        msg["ref"] = sub.ref
                    if sub.subid:
                        msg["subid"] = sub.subid
                    if not sub.snapshot:
                        msg["snapshot"] = False
                    if not sub.updates:
                        msg["updates"] = False
                    if sub.fields:
                        msg["fields"] = sub.fields
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
        topic: str | None,
        filter: str | None,
        ref: str | None,
        queue: asyncio.Queue[dict[str, Any]],
        subid: str | None = None,
        snapshot: bool = True,
        updates: bool = True,
        fields: list[str] | None = None,
    ) -> None:
        self.service = service
        self.topic = topic
        self.filter = filter
        self.ref = ref
        self.queue = queue
        self.subid = subid
        self.snapshot = snapshot
        self.updates = updates
        self.fields = fields
