"""Shared test fixtures."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import pytest_asyncio

from mkio.change_bus import ChangeBus
from mkio.database import Database
from mkio.writer import WriteBatcher


TEST_TABLES = {
    "orders": {
        "columns": {
            "id": "TEXT PRIMARY KEY",
            "symbol": "TEXT NOT NULL",
            "qty": "INTEGER",
            "price": "REAL DEFAULT 0",
            "status": "TEXT DEFAULT 'pending'",
        },
    },
    "audit_log": {
        "columns": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "event": "TEXT",
            "order_id": "TEXT",
        },
        "append_only": True,
    },
}


@pytest_asyncio.fixture
async def db():
    database = Database(":memory:", TEST_TABLES)
    await database.start()
    yield database
    await database.stop()


@pytest.fixture
def bus():
    return ChangeBus()


@pytest_asyncio.fixture
async def writer(db, bus):
    w = WriteBatcher(
        db=db,
        change_bus=bus,
        batch_max_size=100,
        batch_max_wait_ms=1.0,
    )
    await w.start()
    yield w
    await w.stop(drain=True)


class MockWebSocket:
    """Mock WebSocket for testing services."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.closed = False

    async def send_bytes(self, data: bytes) -> None:
        if self.closed:
            raise ConnectionError("WebSocket closed")
        self.sent.append(data)

    def get_messages(self) -> list[dict[str, Any]]:
        from mkio._json import loads
        return [loads(b) for b in self.sent]
