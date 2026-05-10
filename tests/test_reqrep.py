"""Tests for the reqrep (request-reply) protocol."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio

from mkio._json import loads
from mkio._expr import compile_expression, compile_formatter
from mkio.change_bus import ChangeBus
from mkio.config import load_config
from mkio.database import Database
from mkio.writer import WriteBatcher
from mkio.services.reqrep import ReqRepService


TEST_TABLES = {
    "prices": {
        "columns": {
            "symbol": "TEXT PRIMARY KEY",
            "price": "REAL",
            "volume": "INTEGER",
        },
    },
}


class MockWebSocket:
    def __init__(self) -> None:
        self.sent: list[bytes] = []

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data)

    def get_messages(self) -> list[dict[str, Any]]:
        return [loads(b) for b in self.sent]

    def clear(self) -> None:
        self.sent.clear()


@pytest_asyncio.fixture
async def db():
    database = Database(":memory:", TEST_TABLES)
    await database.start()
    # Seed some data
    conn = database.write_conn
    await conn.execute(
        "INSERT INTO prices (symbol, price, volume) VALUES (?, ?, ?)",
        ("AAPL", 150.25, 1200000),
    )
    await conn.execute(
        "INSERT INTO prices (symbol, price, volume) VALUES (?, ?, ?)",
        ("MSFT", 420.50, 800000),
    )
    await conn.commit()
    yield database
    await database.stop()


@pytest.fixture
def bus():
    return ChangeBus()


@pytest_asyncio.fixture
async def writer(db, bus):
    w = WriteBatcher(db=db, change_bus=bus, batch_max_size=100, batch_max_wait_ms=1.0)
    await w.start()
    yield w
    await w.stop(drain=True)


def _make_svc(config, db, bus, writer, name="test_svc"):
    svc = ReqRepService(config=config, db=db, change_bus=bus, writer=writer)
    svc.name = name
    return svc


# ---- Scalar from expression (no SQL) ----------------------------------------

async def test_scalar_expression(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "_compiled_reply_expr": compile_expression("qty * price"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r1",
        "data": {"qty": 10, "price": 99.95},
    })

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "reply"
    assert msgs[0]["reqid"] == "r1"
    assert msgs[0]["value"] == pytest.approx(999.5)
    assert "row" not in msgs[0]
    assert "rows" not in msgs[0]


async def test_scalar_expression_with_functions(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "_compiled_reply_expr": compile_expression("ROUND(qty * price * 0.08, 2)"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r2",
        "data": {"qty": 10, "price": 99.95},
    })

    msgs = ws.get_messages()
    assert msgs[0]["value"] == pytest.approx(79.96)


# ---- Row from expression (no SQL) -------------------------------------------

async def test_row_expression(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "_compiled_reply_formatter": compile_formatter({
            "subtotal": "qty * price",
            "tax": "ROUND(qty * price * 0.08, 2)",
            "label": "UPPER(name)",
        }),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r3",
        "data": {"qty": 5, "price": 100, "name": "acme"},
    })

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "reply"
    assert msgs[0]["reqid"] == "r3"
    row = msgs[0]["row"]
    assert row["subtotal"] == 500
    assert row["tax"] == 40.0
    assert row["label"] == "ACME"
    assert "value" not in msgs[0]
    assert "rows" not in msgs[0]


# ---- Rows from SQL (no reply config) ----------------------------------------

async def test_rows_sql(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT symbol, price FROM prices WHERE symbol = :symbol",
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r4",
        "data": {"symbol": "AAPL"},
    })

    msgs = ws.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "reply"
    assert msgs[0]["reqid"] == "r4"
    rows = msgs[0]["rows"]
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["price"] == pytest.approx(150.25)


async def test_rows_sql_multi(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT symbol, price FROM prices ORDER BY symbol",
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r5",
        "data": {},
    })

    msgs = ws.get_messages()
    rows = msgs[0]["rows"]
    assert len(rows) == 2
    assert rows[0]["symbol"] == "AAPL"
    assert rows[1]["symbol"] == "MSFT"


async def test_rows_sql_empty(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT * FROM prices WHERE symbol = :symbol",
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r6",
        "data": {"symbol": "ZZZZ"},
    })

    msgs = ws.get_messages()
    assert msgs[0]["rows"] == []


# ---- Rows from SQL + reply dict (per-row transform) -------------------------

async def test_rows_sql_with_formatter(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT * FROM prices ORDER BY symbol",
        "_compiled_reply_formatter": compile_formatter({
            "ticker": "UPPER(symbol)",
            "display_price": "ROUND(price, 2)",
            "size": "IF(volume > 1000000, 'large', 'small')",
        }),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r7",
        "data": {},
    })

    msgs = ws.get_messages()
    rows = msgs[0]["rows"]
    assert len(rows) == 2
    assert rows[0] == {"ticker": "AAPL", "display_price": 150.25, "size": "large"}
    assert rows[1] == {"ticker": "MSFT", "display_price": 420.5, "size": "small"}


# ---- Scalar from SQL + reply string -----------------------------------------

async def test_scalar_sql(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT SUM(volume) as total FROM prices",
        "_compiled_reply_expr": compile_expression("total"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r8",
        "data": {},
    })

    msgs = ws.get_messages()
    assert msgs[0]["value"] == 2000000


async def test_scalar_sql_empty(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT price FROM prices WHERE symbol = :symbol",
        "_compiled_reply_expr": compile_expression("price"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r9",
        "data": {"symbol": "ZZZZ"},
    })

    msgs = ws.get_messages()
    assert msgs[0]["value"] is None


# ---- Params manipulation ----------------------------------------------------

async def test_params_transform(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT * FROM prices WHERE symbol = :symbol",
        "_compiled_params": compile_formatter({"symbol": "UPPER(symbol)"}),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r10",
        "data": {"symbol": "aapl"},
    })

    msgs = ws.get_messages()
    rows = msgs[0]["rows"]
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


async def test_params_with_defaults(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT * FROM prices WHERE volume >= :min_vol ORDER BY symbol",
        "_compiled_params": compile_formatter({
            "min_vol": "COALESCE(min_volume, 0)",
        }),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    # With min_volume=null — COALESCE defaults to 0, returns all
    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r11",
        "data": {"min_volume": None},
    })
    msgs = ws.get_messages()
    assert len(msgs[0]["rows"]) == 2

    ws.clear()

    # With min_volume set — filters
    await svc.on_message(ws, {
        "type": "request",
        "reqid": "r12",
        "data": {"min_volume": 1000000},
    })
    msgs = ws.get_messages()
    assert len(msgs[0]["rows"]) == 1
    assert msgs[0]["rows"][0]["symbol"] == "AAPL"


# ---- reqid echoed on reply and error -----------------------------------------

async def test_reqid_echoed(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "_compiled_reply_expr": compile_expression("42"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "my-req-123",
        "data": {},
    })

    msgs = ws.get_messages()
    assert msgs[0]["reqid"] == "my-req-123"


async def test_reqid_echoed_on_error(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "sql": "SELECT * FROM nonexistent_table",
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "err-req",
        "data": {},
    })

    msgs = ws.get_messages()
    assert msgs[0]["type"] == "error"
    assert msgs[0]["reqid"] == "err-req"


async def test_no_reqid(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "_compiled_reply_expr": compile_expression("1 + 1"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "data": {}})

    msgs = ws.get_messages()
    assert msgs[0]["type"] == "reply"
    assert "reqid" not in msgs[0]


# ---- Subscribe rejected -----------------------------------------------------

async def test_subscribe_rejected(db, bus, writer):
    config = {"protocol": "reqrep", "sql": "SELECT 1"}
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    count = await svc.on_subscribe(ws, {
        "type": "subscribe",
        "protocol": "reqrep",
    })

    assert count == 0
    msgs = ws.get_messages()
    assert msgs[0]["type"] == "nack"
    assert "subscriptions" in msgs[0]["message"].lower() or "request" in msgs[0]["message"].lower()


# ---- Config validation ------------------------------------------------------

def test_config_reqrep_sql():
    config = {
        "tables": TEST_TABLES,
        "services": {
            "lookup": {
                "protocol": "reqrep",
                "sql": "SELECT * FROM prices WHERE symbol = :symbol",
            },
        },
    }
    cfg = load_config(config)
    assert cfg["services"]["lookup"]["protocol"] == "reqrep"


def test_config_reqrep_reply_string():
    config = {
        "tables": TEST_TABLES,
        "services": {
            "calc": {
                "protocol": "reqrep",
                "reply": "qty * price",
            },
        },
    }
    cfg = load_config(config)
    svc = cfg["services"]["calc"]
    assert "_compiled_reply_expr" in svc
    assert callable(svc["_compiled_reply_expr"])


def test_config_reqrep_reply_dict():
    config = {
        "tables": TEST_TABLES,
        "services": {
            "calc": {
                "protocol": "reqrep",
                "reply": {"total": "qty * price", "label": "UPPER(name)"},
            },
        },
    }
    cfg = load_config(config)
    svc = cfg["services"]["calc"]
    assert "_compiled_reply_formatter" in svc
    assert callable(svc["_compiled_reply_formatter"])


def test_config_reqrep_params():
    config = {
        "tables": TEST_TABLES,
        "services": {
            "lookup": {
                "protocol": "reqrep",
                "sql": "SELECT * FROM prices WHERE symbol = :symbol",
                "params": {"symbol": "UPPER(symbol)"},
            },
        },
    }
    cfg = load_config(config)
    svc = cfg["services"]["lookup"]
    assert "_compiled_params" in svc
    assert callable(svc["_compiled_params"])


def test_config_reqrep_missing_both():
    config = {
        "tables": TEST_TABLES,
        "services": {
            "bad": {
                "protocol": "reqrep",
            },
        },
    }
    with pytest.raises(ValueError, match="requires at least one"):
        load_config(config)


# ---- Monitor notifications ---------------------------------------------------

async def test_monitor_notifications(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "_compiled_reply_expr": compile_expression("42"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    monitor_calls: list[tuple[str, str, Any]] = []
    async def mock_notifier(name: str, direction: str, data: Any) -> None:
        monitor_calls.append((name, direction, data))

    svc._monitor_notifier = mock_notifier

    await svc.on_message(ws, {
        "type": "request",
        "reqid": "m1",
        "data": {},
    })

    assert len(monitor_calls) == 1
    assert monitor_calls[0][1] == "out"


# ---- Data defaults to empty dict --------------------------------------------

async def test_missing_data_field(db, bus, writer):
    config = {
        "protocol": "reqrep",
        "_compiled_reply_expr": compile_expression("42"),
    }
    svc = _make_svc(config, db, bus, writer)
    ws = MockWebSocket()

    await svc.on_message(ws, {"type": "request", "reqid": "r0"})

    msgs = ws.get_messages()
    assert msgs[0]["type"] == "reply"
    assert msgs[0]["value"] == 42
