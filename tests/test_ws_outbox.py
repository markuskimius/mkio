"""OutboxWebSocket: sends queue and return; one task per connection writes."""

from __future__ import annotations

import asyncio

import aiohttp
import pytest
from aiohttp import web

from mkio.ws_outbox import OutboxWebSocket


def _app(handler, **ws_kwargs) -> tuple[web.Application, list[OutboxWebSocket]]:
    made: list[OutboxWebSocket] = []

    async def route(request: web.Request) -> web.WebSocketResponse:
        ws = OutboxWebSocket(**ws_kwargs)
        made.append(ws)
        await ws.prepare(request)
        try:
            await handler(ws)
        finally:
            await ws.stop_outbox()
        return ws

    app = web.Application()
    app.router.add_get("/ws", route)
    return app, made


async def _until(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition never came true"
        await asyncio.sleep(0.01)


async def test_frames_leave_in_the_order_sent_whatever_their_kind(aiohttp_client):
    async def handler(ws):
        for i in range(50):
            if i % 3 == 0:
                await ws.send_str(f"s{i}")
            elif i % 3 == 1:
                await ws.send_bytes(f"b{i}".encode())
            else:
                await ws.send_json({"j": i})
        async for _ in ws:
            pass

    app, _ = _app(handler)
    client = await aiohttp_client(app)
    ws = await client.ws_connect("/ws")
    got = []
    for _ in range(50):
        msg = await ws.receive()
        got.append(msg.data.decode() if isinstance(msg.data, bytes) else msg.data)
    expected = [f"s{i}" if i % 3 == 0 else f"b{i}" if i % 3 == 1 else '{"j": %d}' % i for i in range(50)]
    assert got == expected
    await ws.close()


async def test_a_send_returns_without_waiting_on_a_peer_that_is_not_reading(aiohttp_client):
    sent = asyncio.Event()

    async def handler(ws):
        chunk = b"x" * 65536
        for _ in range(200):  # 13 MB: far more than the socket will take
            await ws.send_bytes(chunk)
        sent.set()
        async for _ in ws:
            pass

    app, made = _app(handler, send_buffer=64 * 1024 * 1024)
    client = await aiohttp_client(app)
    ws = await client.ws_connect("/ws", max_msg_size=0)
    ws._conn.transport.pause_reading()
    await asyncio.wait_for(sent.wait(), 2)
    assert made[0]._outbox_bytes > 1024 * 1024  # backed up here, not in the caller
    ws._conn.transport.abort()


async def test_a_backlog_past_the_buffer_closes_the_connection(aiohttp_client, caplog):
    raised: list[BaseException] = []

    async def handler(ws):
        chunk = b"x" * 8192  # under a sixteenth of the buffer, so it counts
        try:
            for _ in range(4000):
                await ws.send_bytes(chunk)
        except ConnectionResetError as e:
            raised.append(e)
        # Sends keep failing the same way: what the services read as a dead peer.
        with pytest.raises(ConnectionResetError):
            await ws.send_bytes(b"more")
        with pytest.raises(ConnectionResetError):
            await ws.send_str("more")

    app, made = _app(handler, send_buffer=256 * 1024)
    client = await aiohttp_client(app)
    ws = await client.ws_connect("/ws", max_msg_size=0)
    ws._conn.transport.pause_reading()
    await _until(lambda: raised)
    assert "overflow" in str(raised[0])
    assert made[0]._outbox_bytes == 0 and not made[0]._outbox  # the backlog is let go
    assert "bytes behind" in caplog.text
    ws._conn.transport.resume_reading()
    async for msg in ws:  # drains what was already on the wire, then the close
        pass
    assert ws.close_code in (aiohttp.WSCloseCode.TRY_AGAIN_LATER, aiohttp.WSCloseCode.ABNORMAL_CLOSURE)


async def test_a_snapshot_sized_frame_does_not_count_against_the_peer(aiohttp_client):
    """Counted, a snapshot larger than the buffer would trip the limit on the
    first update queued behind it; the client would reconnect, fetch the same
    snapshot, and be closed again, for ever."""
    big = b"y" * (512 * 1024)

    async def handler(ws):
        await ws.send_bytes(big)
        for i in range(20):  # queued right behind it, before anything is written
            await ws.send_bytes(b"update-%d" % i)
        async for _ in ws:
            pass

    app, made = _app(handler, send_buffer=64 * 1024)
    client = await aiohttp_client(app)
    ws = await client.ws_connect("/ws", max_msg_size=0)
    assert (await ws.receive()).data == big
    assert [(await ws.receive()).data for _ in range(20)] == [b"update-%d" % i for i in range(20)]
    assert made[0]._outbox_bytes == 0
    await ws.close()


async def test_sends_raise_once_the_peer_is_gone(aiohttp_client):
    outcome: list[str] = []
    gone = asyncio.Event()

    async def handler(ws):
        async for _ in ws:
            pass
        gone.set()
        try:
            await ws.send_bytes(b"late")
            outcome.append("sent")
        except ConnectionResetError:
            outcome.append("reset")

    app, _ = _app(handler)
    client = await aiohttp_client(app)
    ws = await client.ws_connect("/ws")
    await ws.close()
    await asyncio.wait_for(gone.wait(), 2)
    await _until(lambda: outcome)
    assert outcome == ["reset"]


async def test_send_before_prepare_and_after_stop_raise():
    ws = OutboxWebSocket()
    with pytest.raises(ConnectionResetError):
        await ws.send_bytes(b"early")
    await ws.stop_outbox()  # harmless when nothing ever started
    with pytest.raises(ConnectionResetError):
        await ws.send_str("late")


async def test_stop_outbox_ends_the_sender_task(aiohttp_client):
    async def handler(ws):
        await ws.send_bytes(b"hello")
        async for _ in ws:
            pass

    app, made = _app(handler)
    client = await aiohttp_client(app)
    ws = await client.ws_connect("/ws")
    assert (await ws.receive()).data == b"hello"
    task = made[0]._outbox_task
    assert task is not None and not task.done()
    await ws.close()
    await _until(lambda: task.done())
    await asyncio.sleep(0.05)  # aiohttp prepares the returned response again
    assert made[0]._outbox_task is None  # and that must not start a second sender
