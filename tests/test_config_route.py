"""Tests for /config route (TOML-to-JSON serving)."""

from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp import web

from mkio._json import loads
from mkio.server import _make_config_handler


@pytest_asyncio.fixture
async def config_dir(tmp_path):
    # Root-level files
    (tmp_path / "app.toml").write_text('[database]\nhost = "localhost"\nport = 5432\n')
    (tmp_path / "plain.json").write_text('{"raw": true}')
    (tmp_path / "both.toml").write_text('[source]\nfrom = "toml"\n')
    (tmp_path / "both.json").write_text('{"source": {"from": "json"}}')
    (tmp_path / "style.css").write_text("body { color: red; }")
    (tmp_path / "data.yaml").write_text("key: value\n")
    (tmp_path / "empty.toml").write_text("")

    # Subdirectories
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "nested.toml").write_text('[server]\nname = "prod"\n')
    (sub / "file.txt").write_text("hello")

    deep = sub / "deep"
    deep.mkdir()
    (deep / "config.toml").write_text('[deep]\nlevel = 3\n')

    # Sibling directory (should not be accessible)
    sibling = tmp_path.parent / "sibling_secret"
    sibling.mkdir(exist_ok=True)
    (sibling / "secret.toml").write_text('[credentials]\ntoken = "leaked"\n')

    return tmp_path


@pytest_asyncio.fixture
async def alt_dir(tmp_path):
    alt = tmp_path / "alt"
    alt.mkdir()
    (alt / "settings.toml").write_text('[ui]\ntheme = "dark"\n')
    return alt


@pytest_asyncio.fixture
async def client(aiohttp_client, config_dir):
    app = web.Application()
    app.router.add_get("/config/{path:.*}", _make_config_handler(config_dir))
    return await aiohttp_client(app)


@pytest_asyncio.fixture
async def multi_client(aiohttp_client, config_dir, alt_dir):
    app = web.Application()
    app.router.add_get("/config/{path:.*}", _make_config_handler(config_dir))
    app.router.add_get("/settings/{path:.*}", _make_config_handler(alt_dir))
    return await aiohttp_client(app)


# ---- Basic TOML-to-JSON serving ---------------------------------------------


async def test_toml_served_as_json(client):
    resp = await client.get("/config/app.json")
    assert resp.status == 200
    assert resp.content_type == "application/json"
    data = await resp.json()
    assert data == {"database": {"host": "localhost", "port": 5432}}


async def test_toml_takes_priority_over_json(client):
    """When both .toml and .json exist, .toml wins."""
    resp = await client.get("/config/both.json")
    assert resp.status == 200
    data = await resp.json()
    assert data["source"]["from"] == "toml"


async def test_json_fallback_when_no_toml(client):
    resp = await client.get("/config/plain.json")
    assert resp.status == 200
    data = await resp.json()
    assert data == {"raw": True}


async def test_empty_toml_serves_empty_object(client):
    resp = await client.get("/config/empty.json")
    assert resp.status == 200
    data = await resp.json()
    assert data == {}


# ---- Non-JSON extensions served directly ------------------------------------


async def test_css_served_directly(client):
    resp = await client.get("/config/style.css")
    assert resp.status == 200
    text = await resp.text()
    assert "body" in text


async def test_yaml_served_directly(client):
    resp = await client.get("/config/data.yaml")
    assert resp.status == 200
    text = await resp.text()
    assert "key: value" in text


# ---- Subdirectory support ---------------------------------------------------


async def test_subdirectory_toml_to_json(client):
    resp = await client.get("/config/sub/nested.json")
    assert resp.status == 200
    data = await resp.json()
    assert data == {"server": {"name": "prod"}}


async def test_deeply_nested_toml_to_json(client):
    resp = await client.get("/config/sub/deep/config.json")
    assert resp.status == 200
    data = await resp.json()
    assert data == {"deep": {"level": 3}}


async def test_subdirectory_non_json(client):
    resp = await client.get("/config/sub/file.txt")
    assert resp.status == 200
    text = await resp.text()
    assert text == "hello"


# ---- 404 cases --------------------------------------------------------------


async def test_missing_json_no_toml_no_json(client):
    resp = await client.get("/config/nonexistent.json")
    assert resp.status == 404


async def test_missing_non_json(client):
    resp = await client.get("/config/missing.txt")
    assert resp.status == 404


async def test_missing_subdirectory(client):
    resp = await client.get("/config/nodir/file.json")
    assert resp.status == 404


# ---- Path traversal / security -----------------------------------------------


async def test_traversal_dotdot(client):
    resp = await client.get("/config/../../../etc/passwd")
    assert resp.status in (403, 404)


async def test_traversal_encoded_dotdot(client):
    """URL-encoded ../ sequences."""
    resp = await client.get("/config/%2e%2e/%2e%2e/etc/passwd")
    assert resp.status in (403, 404)


async def test_traversal_to_sibling_directory(client, config_dir):
    """Attempt to escape config_dir to a sibling directory."""
    resp = await client.get("/config/../sibling_secret/secret.json")
    assert resp.status in (403, 404)
    if resp.status == 200:
        data = await resp.json()
        assert "credentials" not in data


async def test_traversal_double_encoded(client):
    """Double-encoded traversal."""
    resp = await client.get("/config/%252e%252e/%252e%252e/etc/passwd")
    assert resp.status in (403, 404)


async def test_traversal_backslash(client):
    """Backslash traversal (Windows-style)."""
    resp = await client.get("/config/..\\..\\etc\\passwd")
    assert resp.status in (403, 404)


async def test_traversal_mixed_slashes(client):
    resp = await client.get("/config/sub/../../etc/passwd")
    assert resp.status in (403, 404)


async def test_traversal_null_byte(client):
    """Null byte injection attempt."""
    resp = await client.get("/config/app%00.json")
    assert resp.status in (403, 404)


async def test_traversal_absolute_path(client):
    """Absolute path in request."""
    resp = await client.get("/config//etc/passwd")
    assert resp.status in (403, 404)


async def test_traversal_dot_segment_at_end(client):
    """Trailing dot segment."""
    resp = await client.get("/config/sub/../sub/../../../etc/passwd")
    assert resp.status in (403, 404)


# ---- Multiple routes ---------------------------------------------------------


async def test_multiple_routes_independent(multi_client):
    """Each route maps to its own directory."""
    resp = await multi_client.get("/config/app.json")
    assert resp.status == 200
    data = await resp.json()
    assert data == {"database": {"host": "localhost", "port": 5432}}

    resp = await multi_client.get("/settings/settings.json")
    assert resp.status == 200
    data = await resp.json()
    assert data == {"ui": {"theme": "dark"}}


async def test_multiple_routes_no_cross_access(multi_client):
    """Files from one route's directory aren't visible on the other route."""
    resp = await multi_client.get("/settings/app.json")
    assert resp.status == 404

    resp = await multi_client.get("/config/settings.json")
    assert resp.status == 404
