"""A WebSocket whose sends never wait on the peer."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any

import aiohttp
from aiohttp import web

logger = logging.getLogger("mkio.ws")

DEFAULT_SEND_BUFFER = 16 * 1024 * 1024
_CLOSE_TIMEOUT = 5.0


class OutboxWebSocket(web.WebSocketResponse):
    """``send_bytes``/``send_str`` queue the frame and return; one task per
    connection writes the queue to the socket.

    A service listener fans a change out to every subscriber in turn, so a
    send that waited on one peer's TCP window held up all the others — a
    laptop asleep with a page open froze the service for everyone once its
    socket buffers filled. Here a slow peer backs up only its own queue, and
    frames still leave in the order they were sent, across every service on
    the connection.

    The queue is bounded by ``send_buffer`` bytes of backlog. A peer further
    behind than that is closed (1013, try again later) instead of being fed
    a stream with holes in it: the client reconnects and takes a fresh
    snapshot. A frame over a sixteenth of the buffer — a snapshot — is sized
    by the data, not by the peer's pace, and does not count: counted, a
    large table's snapshot would trip the limit on the first update behind
    it, and the reconnect would fetch the same snapshot again, for ever.
    Once the connection is closed or dropped, sends raise
    ``ConnectionResetError``, which is what the services already treat as a
    dead subscriber.
    """

    def __init__(self, *, send_buffer: int = DEFAULT_SEND_BUFFER, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._outbox: deque[tuple[bytes | str, int]] = deque()  # frame, bytes counted
        self._outbox_bytes = 0
        self._outbox_limit = send_buffer
        self._outbox_bulk = max(send_buffer // 16, 1)
        self._outbox_started = False
        self._outbox_wake = asyncio.Event()
        self._outbox_task: asyncio.Task[None] | None = None
        self._outbox_dead = False

    async def prepare(self, request: web.BaseRequest) -> Any:
        writer = await super().prepare(request)
        # aiohttp prepares the response the handler returns once more, after
        # stop_outbox(): the sender starts once.
        if not self._outbox_started:
            self._outbox_started = True
            self._outbox_task = asyncio.create_task(self._pump())
        return writer

    async def send_bytes(self, data: bytes, compress: int | None = None) -> None:
        self._enqueue(data)

    async def send_str(self, data: str, compress: int | None = None) -> None:
        self._enqueue(data)

    def _enqueue(self, data: bytes | str) -> None:
        if self._outbox_dead or self.closed or self._outbox_task is None:
            raise ConnectionResetError("websocket is closed")
        if self._outbox_bytes > self._outbox_limit:
            self._overflow()
            raise ConnectionResetError("websocket send buffer overflow")
        counted = len(data) if len(data) <= self._outbox_bulk else 0
        self._outbox.append((data, counted))
        self._outbox_bytes += counted
        self._outbox_wake.set()

    def _overflow(self) -> None:
        logger.warning(
            "closing a websocket %d bytes behind (ws_send_buffer_mb); the client "
            "reconnects for a fresh snapshot", self._outbox_bytes,
        )
        self._drop()
        asyncio.create_task(self._close_slow())

    def _drop(self) -> None:
        self._outbox_dead = True
        self._outbox.clear()
        self._outbox_bytes = 0
        self._outbox_wake.set()

    async def _close_slow(self) -> None:
        # The close handshake writes too, and this peer is not reading: give
        # it a moment, then take the transport away.
        try:
            await asyncio.wait_for(
                self.close(code=aiohttp.WSCloseCode.TRY_AGAIN_LATER, message=b"send buffer overflow"),
                _CLOSE_TIMEOUT,
            )
        except (asyncio.TimeoutError, ConnectionError, RuntimeError):
            pass
        transport = getattr(self._req, "transport", None) if self._req is not None else None
        if transport is not None and not transport.is_closing():
            transport.abort()

    async def _pump(self) -> None:
        try:
            while not self._outbox_dead:
                if not self._outbox:
                    self._outbox_wake.clear()
                    await self._outbox_wake.wait()
                    continue
                data, counted = self._outbox.popleft()
                self._outbox_bytes -= counted
                if isinstance(data, str):
                    await super().send_str(data)
                else:
                    await super().send_bytes(data)
        except (ConnectionError, RuntimeError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("websocket sender failed")
        finally:
            self._drop()

    async def stop_outbox(self) -> None:
        """End the sender task; called once the connection's handler is done."""
        self._drop()
        task, self._outbox_task = self._outbox_task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
