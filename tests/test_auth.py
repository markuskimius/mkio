"""Tests for authentication and access control."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import pytest_asyncio

from mkio._json import dumps, loads
from mkio.app import MkioApp, create_app
from mkio.auth import hash_password, verify_password, RightsCache, check_access, build_when_params


# ---------------------------------------------------------------------------
# Unit tests: password hashing
# ---------------------------------------------------------------------------


def test_hash_and_verify():
    hashed = hash_password("secret")
    assert verify_password("secret", hashed)
    assert not verify_password("wrong", hashed)


def test_verify_bad_hash():
    assert not verify_password("secret", "garbage")


# ---------------------------------------------------------------------------
# Unit tests: RightsCache
# ---------------------------------------------------------------------------


def test_rights_cache():
    cache = RightsCache()
    cache.load([
        {"role": "trader", "right": "market"},
        {"role": "trader", "right": "trade"},
        {"role": "admin", "right": "admin"},
    ])
    assert cache.role_has_right("trader", "market")
    assert cache.role_has_right("trader", "trade")
    assert not cache.role_has_right("trader", "admin")
    assert cache.role_has_right("admin", "admin")
    assert not cache.role_has_right("viewer", "market")
    assert cache.get_rights("trader") == {"market", "trade"}
    assert cache.get_rights("nobody") == set()


# ---------------------------------------------------------------------------
# Unit tests: check_access
# ---------------------------------------------------------------------------


def test_check_access_open():
    cache = RightsCache()
    allowed, when = check_access("open", None, cache)
    assert allowed
    assert when is None


def test_check_access_auth_no_user():
    cache = RightsCache()
    allowed, _ = check_access("auth", None, cache)
    assert not allowed


def test_check_access_auth_with_user():
    cache = RightsCache()
    allowed, when = check_access("auth", {"user": "bob", "role": "trader"}, cache)
    assert allowed
    assert when is None


def test_check_access_right_string():
    cache = RightsCache()
    cache.load([{"role": "trader", "right": "market"}])
    allowed, _ = check_access("market", {"user": "bob", "role": "trader"}, cache)
    assert allowed

    allowed, _ = check_access("admin", {"user": "bob", "role": "trader"}, cache)
    assert not allowed


def test_check_access_dict_unconditional():
    cache = RightsCache()
    cache.load([{"role": "senior", "right": "trade_full"}])
    access = {"trade_limited": "some SQL", "trade_full": True}
    allowed, when = check_access(access, {"user": "alice", "role": "senior"}, cache)
    assert allowed
    assert when is None


def test_check_access_dict_with_when():
    cache = RightsCache()
    cache.load([{"role": "junior", "right": "trade_limited"}])
    access = {"trade_limited": "accounts WHERE username = :user", "trade_full": True}
    allowed, when = check_access(access, {"user": "bob", "role": "junior"}, cache)
    assert allowed
    assert when == "accounts WHERE username = :user"


def test_check_access_dict_no_match():
    cache = RightsCache()
    cache.load([{"role": "viewer", "right": "view"}])
    access = {"trade_limited": "some SQL", "trade_full": True}
    allowed, _ = check_access(access, {"user": "carol", "role": "viewer"}, cache)
    assert not allowed


# ---------------------------------------------------------------------------
# Unit tests: build_when_params
# ---------------------------------------------------------------------------


def test_build_when_params_basic():
    auth = {"user": "alice", "role": "trader", "desk": "FX"}
    params = build_when_params(auth)
    assert params["user"] == "alice"
    assert params["role"] == "trader"
    assert params["desk"] == "FX"


def test_build_when_params_with_msg():
    auth = {"user": "alice", "role": "trader"}
    msg = {"data": {"symbol": "AAPL", "qty": 100}, "topic": "FX"}
    params = build_when_params(auth, msg)
    assert params["user"] == "alice"
    assert params["symbol"] == "AAPL"
    assert params["qty"] == 100
    assert params["topic"] == "FX"


def test_build_when_params_no_password():
    auth = {"user": "alice", "role": "trader", "password": "secret"}
    params = build_when_params(auth)
    assert "password" not in params


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_config_auth_detection():
    from mkio.config import load_config
    config = load_config({
        "tables": {
            "_mkio_rights": {"columns": {"role": "TEXT", "right": "TEXT"}},
        },
        "services": {},
    })
    assert config["auth"] is True
    assert config["auth_builtin"] is False


def test_config_auth_builtin_detection():
    from mkio.config import load_config
    config = load_config({
        "tables": {
            "_mkio_users": {"columns": {"username": "TEXT PRIMARY KEY", "password": "TEXT", "role": "TEXT"}},
            "_mkio_rights": {"columns": {"role": "TEXT", "right": "TEXT"}},
        },
        "services": {},
    })
    assert config["auth"] is True
    assert config["auth_builtin"] is True


def test_config_no_auth():
    from mkio.config import load_config
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {},
    })
    assert config["auth"] is False


def test_config_access_string():
    from mkio.config import load_config
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {
            "prices": {"protocol": "subpub", "primary_table": "orders", "topic": "id", "access": "open"},
        },
    })
    assert config["services"]["prices"]["access"] == "open"


def test_config_access_dict():
    from mkio.config import load_config
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {
            "prices": {
                "protocol": "subpub",
                "primary_table": "orders",
                "topic": "id",
                "access": {"market": True, "market_desk": "users WHERE desk = :desk"},
            },
        },
    })
    access = config["services"]["prices"]["access"]
    assert access["market"] is True
    assert access["market_desk"] == "users WHERE desk = :desk"


def test_config_access_invalid():
    from mkio.config import load_config
    with pytest.raises(ValueError, match="access.*must be"):
        load_config({
            "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
            "services": {
                "s": {"protocol": "subpub", "primary_table": "t", "topic": "id", "access": 42},
            },
        })


def test_config_monitor_access():
    from mkio.config import load_config
    config = load_config({
        "tables": {
            "_mkio_rights": {"columns": {"role": "TEXT", "right": "TEXT"}},
        },
        "services": {},
        "monitor_access": "admin",
    })
    assert config["monitor_access"] == "admin"


def test_config_monitor_access_invalid():
    from mkio.config import load_config
    with pytest.raises(ValueError, match="access.*must be"):
        load_config({
            "tables": {},
            "services": {},
            "monitor_access": 42,
        })


def test_config_op_access():
    from mkio.config import load_config
    config = load_config({
        "tables": {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}},
        "services": {
            "orders": {
                "protocol": "transaction",
                "ops": {
                    "place": {
                        "table": "orders",
                        "op_type": "insert",
                        "fields": ["qty"],
                        "access": {"trade": True},
                    },
                },
            },
        },
    })
    assert "_op_access" in config["services"]["orders"]
    assert config["services"]["orders"]["_op_access"]["place"] == {"trade": True}


# ---------------------------------------------------------------------------
# Integration tests: full server with auth
# ---------------------------------------------------------------------------


def _get_port(app: MkioApp) -> int:
    assert app._site is not None
    for sock in app._site._server.sockets:
        return sock.getsockname()[1]
    raise RuntimeError("No sockets found")


AUTH_CONFIG = {
    "db_path": ":memory:",
    "port": 0,
    "tables": {
        "prices": {"columns": {"symbol": "TEXT PRIMARY KEY", "price": "REAL"}},
        "orders": {"columns": {"id": "TEXT PRIMARY KEY", "symbol": "TEXT", "qty": "INTEGER", "owner": "TEXT"}},
        "_mkio_users": {"columns": {"username": "TEXT PRIMARY KEY", "password": "TEXT NOT NULL", "role": "TEXT NOT NULL"}},
        "_mkio_rights": {"columns": {"role": "TEXT", "right": "TEXT"}},
    },
    "services": {
        "prices": {
            "protocol": "subpub",
            "primary_table": "prices",
            "topic": "symbol",
            "access": "open",
        },
        "orders_view": {
            "protocol": "query",
            "primary_table": "orders",
            "access": "market",
        },
        "orders_write": {
            "protocol": "transaction",
            "ops": {
                "place": {
                    "table": "orders",
                    "op_type": "insert",
                    "fields": ["symbol", "qty"],
                    "access": {"trade": True},
                },
            },
            "access": "auth",
        },
        "_mkio_rights_admin": {
            "protocol": "transaction",
            "ops": [{"table": "_mkio_rights", "op_type": "insert", "fields": ["role", "right"]}],
            "access": "open",
        },
    },
}


@pytest_asyncio.fixture
async def auth_app():
    app = create_app(AUTH_CONFIG)
    await app.start()

    # Seed auth data
    db = app.db
    pw = hash_password("secret")
    await db.write_conn.execute(
        "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
        ("alice", pw, "trader"),
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
        ("bob", pw, "viewer"),
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)",
        ("trader", "market"),
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)",
        ("trader", "trade"),
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)",
        ("viewer", "market"),
    )
    await db.write_conn.commit()

    # Refresh the rights cache
    from mkio.auth import load_rights_cache
    app._aiohttp_app["rights_cache"]._rights = (await load_rights_cache(db))._rights

    yield app
    await app.stop()


@pytest.fixture
def auth_url(auth_app):
    port = _get_port(auth_app)
    return f"ws://localhost:{port}/ws"


async def test_auth_success(auth_app, auth_url):
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        result = await client.auth({"username": "alice", "password": "secret"})
        assert result["ok"] is True
        assert result["user"] == "alice"
        assert result["role"] == "trader"


async def test_auth_bad_password(auth_app, auth_url):
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        with pytest.raises(ValueError, match="invalid credentials"):
            await client.auth({"username": "alice", "password": "wrong"})


async def test_auth_unknown_user(auth_app, auth_url):
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        with pytest.raises(ValueError, match="invalid credentials"):
            await client.auth({"username": "nobody", "password": "secret"})


async def test_open_access_no_auth(auth_app, auth_url):
    """Open services work without auth."""
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        msgs = []
        async for msg in client.subscribe("prices", "subpub", topic="AAPL", updates=False):
            msgs.append(msg)
        assert len(msgs) == 1
        assert msgs[0]["type"] == "snapshot"


async def test_locked_service_no_auth(auth_app, auth_url):
    """Locked services nack without auth."""
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        async for msg in client.subscribe("orders_view", "query", updates=False):
            assert msg["type"] == "nack"
            assert "authentication required" in msg["message"]
            break


async def test_locked_service_wrong_role(auth_app, auth_url):
    """Service with right that user's role doesn't have."""
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        await client.auth({"username": "bob", "password": "secret"})
        # bob is a viewer, has "market" right — trade requires "trade" right
        result = await client.send("orders_write", {"symbol": "AAPL", "qty": 10}, op="place")
        assert result["type"] == "error"
        assert "permission denied" in result["message"]


async def test_locked_service_correct_role(auth_app, auth_url):
    """Service access with correct right."""
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        await client.auth({"username": "alice", "password": "secret"})
        msgs = []
        async for msg in client.subscribe("orders_view", "query", updates=False):
            msgs.append(msg)
        assert len(msgs) == 1
        assert msgs[0]["type"] == "snapshot"


async def test_auth_only_service(auth_app, auth_url):
    """access = "auth" allows any logged-in user."""
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        # Without auth — denied
        result = await client.send("orders_write", {"symbol": "AAPL", "qty": 10}, op="place")
        assert result["type"] == "error"
        assert "authentication required" in result["message"]

    async with MkioClient(auth_url, reconnect=False) as client:
        # With auth (viewer role, no trade right) — service-level access = "auth" allows
        await client.auth({"username": "bob", "password": "secret"})
        # But op-level access requires "trade" right
        result = await client.send("orders_write", {"symbol": "AAPL", "qty": 10}, op="place")
        assert result["type"] == "error"
        assert "permission denied" in result["message"]


async def test_op_level_access_allowed(auth_app, auth_url):
    """Op-level access with the correct right succeeds."""
    from mkio.client import MkioClient
    async with MkioClient(auth_url, reconnect=False) as client:
        await client.auth({"username": "alice", "password": "secret"})
        result = await client.send("orders_write", {"symbol": "AAPL", "qty": 10}, op="place")
        assert result.get("ok") is True


async def test_when_sql_precheck():
    """Dict-form access with when SQL pre-check."""
    import copy
    config = copy.deepcopy(AUTH_CONFIG)
    config["tables"]["accounts"] = {"columns": {"username": "TEXT PRIMARY KEY", "balance": "REAL"}}
    config["services"]["orders_write"]["_op_access"] = {
        "place": {
            "trade_check": "accounts WHERE username = :user AND balance >= :qty",
            "trade_unlimited": True,
        },
    }
    app = create_app(config)
    await app.start()
    port = _get_port(app)
    url = f"ws://localhost:{port}/ws"

    db = app.db
    pw = hash_password("secret")
    await db.write_conn.execute(
        "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)", ("alice", pw, "trader")
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)", ("trader", "trade_check")
    )
    await db.write_conn.execute(
        "INSERT INTO accounts (username, balance) VALUES (?, ?)", ("alice", 1000.0)
    )
    await db.write_conn.commit()

    from mkio.auth import load_rights_cache
    app._aiohttp_app["rights_cache"]._rights = (await load_rights_cache(db))._rights

    try:

        from mkio.client import MkioClient
        async with MkioClient(url, reconnect=False) as client:
            await client.auth({"username": "alice", "password": "secret"})
            # qty=500 — within balance
            result = await client.send("orders_write", {"symbol": "AAPL", "qty": 500}, op="place")
            assert result.get("ok") is True

        async with MkioClient(url, reconnect=False) as client:
            await client.auth({"username": "alice", "password": "secret"})
            # qty=5000 — exceeds balance
            result = await client.send("orders_write", {"symbol": "AAPL", "qty": 5000}, op="place")
            assert result["type"] == "error"
            assert "permission denied" in result["message"]
    finally:
        await app.stop()


async def test_rights_cache_refresh(auth_app, auth_url):
    """Manually refreshing the rights cache picks up new rights."""
    from mkio.client import MkioClient
    from mkio.auth import load_rights_cache
    db = auth_app.db

    # bob (viewer) has no "trade" right initially
    async with MkioClient(auth_url, reconnect=False) as client:
        await client.auth({"username": "bob", "password": "secret"})
        result = await client.send("orders_write", {"symbol": "AAPL", "qty": 10}, op="place")
        assert result["type"] == "error"

    # Insert the right directly and refresh the cache
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)", ("viewer", "trade")
    )
    await db.write_conn.commit()
    auth_app._aiohttp_app["rights_cache"]._rights = (await load_rights_cache(db))._rights

    # Now bob should have access
    async with MkioClient(auth_url, reconnect=False) as client:
        await client.auth({"username": "bob", "password": "secret"})
        result = await client.send("orders_write", {"symbol": "AAPL", "qty": 10}, op="place")
        assert result.get("ok") is True


async def test_custom_on_auth():
    """app.on_auth() overrides table-backed auth."""
    app = create_app(AUTH_CONFIG)

    async def custom_auth(data):
        if data.get("token") == "valid-token":
            return {"user": "custom_user", "role": "trader"}
        raise ValueError("bad token")

    app.on_auth(custom_auth)
    await app.start()
    port = _get_port(app)
    try:
        from mkio.client import MkioClient
        async with MkioClient(f"ws://localhost:{port}/ws", reconnect=False) as client:
            result = await client.auth({"token": "valid-token"})
            assert result["ok"] is True
            assert result["user"] == "custom_user"

        async with MkioClient(f"ws://localhost:{port}/ws", reconnect=False) as client:
            with pytest.raises(ValueError, match="bad token"):
                await client.auth({"token": "invalid"})
    finally:
        await app.stop()


# ---------------------------------------------------------------------------
# Monitor access control
# ---------------------------------------------------------------------------


async def _send_monitor(ws_url: str, service: str | None = None) -> dict:
    """Send a monitor message and return the response."""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            msg: dict[str, Any] = {"type": "monitor"}
            if service:
                msg["service"] = service
            await ws.send_bytes(dumps(msg))
            resp = await ws.receive()
            return loads(resp.data)


async def _send_monitor_with_auth(
    ws_url: str, username: str, password: str, service: str | None = None,
) -> dict:
    """Authenticate then send a monitor message, return the monitor response."""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            auth_msg = {"type": "auth", "data": {"username": username, "password": password}}
            await ws.send_bytes(dumps(auth_msg))
            auth_resp = await ws.receive()
            auth_data = loads(auth_resp.data)
            assert auth_data["ok"] is True

            msg: dict[str, Any] = {"type": "monitor"}
            if service:
                msg["service"] = service
            await ws.send_bytes(dumps(msg))
            resp = await ws.receive()
            return loads(resp.data)


async def test_monitor_disabled_by_default(auth_app, auth_url):
    """Monitor disabled when auth enabled and no monitor_access configured."""
    resp = await _send_monitor(auth_url)
    assert resp["type"] == "error"
    assert "monitoring disabled" in resp["message"]


async def test_monitor_disabled_even_after_auth(auth_app, auth_url):
    """Monitor still disabled after auth when no monitor_access configured."""
    resp = await _send_monitor_with_auth(auth_url, "alice", "secret")
    assert resp["type"] == "error"
    assert "monitoring disabled" in resp["message"]


async def test_monitor_denied_wrong_right():
    """Monitor denied when user lacks required right."""
    config = {
        **AUTH_CONFIG,
        "monitor_access": "admin",
    }
    app = create_app(config)
    await app.start()
    port = _get_port(app)
    ws_url = f"ws://localhost:{port}/ws"

    db = app.db
    pw = hash_password("secret")
    await db.write_conn.execute(
        "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
        ("alice", pw, "trader"),
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)",
        ("trader", "market"),
    )
    await db.write_conn.commit()
    from mkio.auth import load_rights_cache
    app._aiohttp_app["rights_cache"]._rights = (await load_rights_cache(db))._rights

    try:
        resp = await _send_monitor_with_auth(ws_url, "alice", "secret")
        assert resp["type"] == "error"
        assert "permission denied" in resp["message"]
    finally:
        await app.stop()


async def test_monitor_allowed_correct_right():
    """Monitor allowed when user has the required right."""
    config = {
        **AUTH_CONFIG,
        "monitor_access": "market",
    }
    app = create_app(config)
    await app.start()
    port = _get_port(app)
    ws_url = f"ws://localhost:{port}/ws"

    db = app.db
    pw = hash_password("secret")
    await db.write_conn.execute(
        "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
        ("alice", pw, "trader"),
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)",
        ("trader", "market"),
    )
    await db.write_conn.commit()
    from mkio.auth import load_rights_cache
    app._aiohttp_app["rights_cache"]._rights = (await load_rights_cache(db))._rights

    try:
        resp = await _send_monitor_with_auth(ws_url, "alice", "secret")
        assert resp["type"] == "monitor_ack"
    finally:
        await app.stop()


async def test_monitor_open_access():
    """monitor_access='open' allows monitoring without auth even when auth is enabled."""
    config = {
        **AUTH_CONFIG,
        "monitor_access": "open",
    }
    app = create_app(config)
    await app.start()
    port = _get_port(app)
    ws_url = f"ws://localhost:{port}/ws"
    try:
        resp = await _send_monitor(ws_url)
        assert resp["type"] == "monitor_ack"
    finally:
        await app.stop()


async def test_monitor_no_auth_system():
    """Without auth (no _mkio_rights table), monitor works without restriction."""
    from mkio.app import create_app as ca
    config = {
        "db_path": ":memory:",
        "port": 0,
        "tables": {"t": {"columns": {"id": "TEXT PRIMARY KEY"}}},
        "services": {"s": {"protocol": "subpub", "primary_table": "t", "topic": "id"}},
    }
    app = ca(config)
    await app.start()
    port = _get_port(app)
    ws_url = f"ws://localhost:{port}/ws"
    try:
        resp = await _send_monitor(ws_url)
        assert resp["type"] == "monitor_ack"
    finally:
        await app.stop()


async def test_monitor_specific_service_disabled(auth_app, auth_url):
    """Monitor on a specific service is also disabled without monitor_access."""
    resp = await _send_monitor(auth_url, service="prices")
    assert resp["type"] == "error"
    assert "monitoring disabled" in resp["message"]


async def test_monitor_specific_service_allowed():
    """Monitor on a specific service is allowed after auth with monitor_access configured."""
    config = {
        **AUTH_CONFIG,
        "monitor_access": "market",
    }
    app = create_app(config)
    await app.start()
    port = _get_port(app)
    ws_url = f"ws://localhost:{port}/ws"

    db = app.db
    pw = hash_password("secret")
    await db.write_conn.execute(
        "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
        ("alice", pw, "trader"),
    )
    await db.write_conn.execute(
        "INSERT INTO _mkio_rights (role, right) VALUES (?, ?)",
        ("trader", "market"),
    )
    await db.write_conn.commit()
    from mkio.auth import load_rights_cache
    app._aiohttp_app["rights_cache"]._rights = (await load_rights_cache(db))._rights

    try:
        resp = await _send_monitor_with_auth(ws_url, "alice", "secret", service="prices")
        assert resp["type"] == "monitor_ack"
        assert resp["service"] == "prices"
    finally:
        await app.stop()


async def test_monitor_auth_any_user():
    """monitor_access='auth' allows any authenticated user."""
    config = {
        **AUTH_CONFIG,
        "monitor_access": "auth",
    }
    app = create_app(config)
    await app.start()
    port = _get_port(app)
    ws_url = f"ws://localhost:{port}/ws"

    db = app.db
    pw = hash_password("secret")
    await db.write_conn.execute(
        "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
        ("alice", pw, "trader"),
    )
    await db.write_conn.commit()

    try:
        resp = await _send_monitor_with_auth(ws_url, "alice", "secret")
        assert resp["type"] == "monitor_ack"
    finally:
        await app.stop()


async def test_monitor_unauthenticated_denied():
    """Unauthenticated user denied when monitor_access requires a right."""
    config = {
        **AUTH_CONFIG,
        "monitor_access": "admin",
    }
    app = create_app(config)
    await app.start()
    port = _get_port(app)
    ws_url = f"ws://localhost:{port}/ws"
    try:
        resp = await _send_monitor(ws_url)
        assert resp["type"] == "error"
        assert "authentication required" in resp["message"]
    finally:
        await app.stop()
