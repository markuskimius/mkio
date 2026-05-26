"""Tests for scaffold: init() and get_default_config()."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from mkio.scaffold import get_default_config, init


# ---- init() ------------------------------------------------------------------


def test_init_creates_files(tmp_path: Path):
    created = init(tmp_path / "proj")
    assert len(created) == 2
    assert (tmp_path / "proj" / "server.toml").exists()
    assert (tmp_path / "proj" / "static" / "index.html").exists()


def test_init_no_static(tmp_path: Path):
    created = init(tmp_path / "proj", no_static=True)
    assert len(created) == 1
    assert (tmp_path / "proj" / "server.toml").exists()
    assert not (tmp_path / "proj" / "static").exists()


def test_init_existing_config_raises(tmp_path: Path):
    (tmp_path / "server.toml").write_text("x = 1")
    with pytest.raises(FileExistsError):
        init(tmp_path)


def test_init_creates_parent_dirs(tmp_path: Path):
    created = init(tmp_path / "a" / "b" / "c", no_static=True)
    assert (tmp_path / "a" / "b" / "c" / "server.toml").exists()
    assert len(created) == 1


def test_init_accepts_string_path(tmp_path: Path):
    created = init(str(tmp_path / "strdir"))
    assert len(created) == 2
    assert (tmp_path / "strdir" / "server.toml").exists()


def test_init_default_directory(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    created = init()
    assert (tmp_path / "server.toml").exists()
    assert len(created) >= 1


def test_init_returns_path_objects(tmp_path: Path):
    created = init(tmp_path / "proj")
    for p in created:
        assert isinstance(p, Path)
        assert p.exists()


def test_init_toml_is_valid(tmp_path: Path):
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    assert "tables" in cfg
    assert "services" in cfg
    assert cfg["port"] == 8080


def test_init_toml_includes_static_section(tmp_path: Path):
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    assert "static" in cfg
    assert "/" in cfg["static"]


def test_init_no_static_toml_excludes_static_section(tmp_path: Path):
    init(tmp_path / "proj", no_static=True)
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    assert "static" not in cfg


def test_init_html_has_mkio_client(tmp_path: Path):
    init(tmp_path / "proj")
    html = (tmp_path / "proj" / "static" / "index.html").read_text()
    assert "MkioClient" in html
    assert "/mkio.js" in html
    assert "<!DOCTYPE html>" in html


def test_init_toml_has_all_service_protocols(tmp_path: Path):
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    protocols = {s.get("protocol") for s in cfg["services"].values()}
    assert "transaction" in protocols
    assert "subpub" in protocols
    assert "stream" in protocols
    assert "query" in protocols
    assert "reqrep" in protocols


def test_init_scaffolded_config_creates_working_app(tmp_path: Path):
    """The scaffolded config should pass validation via create_app."""
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    cfg["db_path"] = ":memory:"
    cfg["port"] = 0
    cfg["host"] = "127.0.0.1"
    from mkio import create_app
    app = create_app(cfg)
    assert app.config["auto_migrate"] == "safe"


# ---- get_default_config() ----------------------------------------------------


def test_get_default_config_returns_valid_dict():
    cfg = get_default_config()
    assert isinstance(cfg, dict)
    assert "tables" in cfg
    assert "services" in cfg
    assert cfg["port"] == 8080
    assert cfg["host"] == "0.0.0.0"


def test_get_default_config_has_static():
    cfg = get_default_config()
    assert "static" in cfg
    assert "/" in cfg["static"]


def test_get_default_config_is_loadable():
    from mkio.config import load_config
    cfg = get_default_config()
    cfg["db_path"] = ":memory:"
    loaded = load_config(cfg)
    assert loaded["port"] == 8080


def test_get_default_config_returns_fresh_dict():
    cfg1 = get_default_config()
    cfg2 = get_default_config()
    assert cfg1 == cfg2
    cfg1["port"] = 9999
    assert cfg2["port"] == 8080


def test_get_default_config_matches_init_output(tmp_path: Path):
    """get_default_config() and init() should produce equivalent configs."""
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        from_init = tomllib.load(f)
    from_func = get_default_config()
    assert from_init == from_func


def test_get_default_config_has_all_top_level_keys():
    cfg = get_default_config()
    assert "port" in cfg
    assert "host" in cfg
    assert "db_path" in cfg
    assert "name" in cfg
    assert "version" in cfg
    assert "batch_max_size" in cfg
    assert "auto_migrate" in cfg
    assert "tables" in cfg
    assert "services" in cfg
    assert "static" in cfg
