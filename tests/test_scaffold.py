"""Tests for scaffold: init() and get_default_config()."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from mkio.scaffold import get_default_config, init


# ---- init() ------------------------------------------------------------------


def test_init_creates_files(tmp_path: Path):
    created = init(tmp_path / "proj")
    assert len(created) == 4
    assert (tmp_path / "proj" / "server.toml").exists()
    assert (tmp_path / "proj" / "data" / "users.csv").exists()
    assert (tmp_path / "proj" / "data" / "rights.csv").exists()
    assert (tmp_path / "proj" / "static" / "index.html").exists()


def test_init_no_static(tmp_path: Path):
    created = init(tmp_path / "proj", no_static=True)
    assert len(created) == 3
    assert (tmp_path / "proj" / "server.toml").exists()
    assert (tmp_path / "proj" / "data" / "users.csv").exists()
    assert (tmp_path / "proj" / "data" / "rights.csv").exists()
    assert not (tmp_path / "proj" / "static").exists()


def test_init_existing_config_raises(tmp_path: Path):
    (tmp_path / "server.toml").write_text("x = 1")
    with pytest.raises(FileExistsError):
        init(tmp_path)


def test_init_creates_parent_dirs(tmp_path: Path):
    created = init(tmp_path / "a" / "b" / "c", no_static=True)
    assert (tmp_path / "a" / "b" / "c" / "server.toml").exists()
    assert len(created) == 3


def test_init_accepts_string_path(tmp_path: Path):
    created = init(str(tmp_path / "strdir"))
    assert len(created) == 4
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
    proj = tmp_path / "proj"
    init(proj)
    with open(proj / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    cfg["db_path"] = ":memory:"
    cfg["port"] = 0
    cfg["host"] = "127.0.0.1"
    cfg["_config_dir"] = str(proj)
    from mkio import create_app
    app = create_app(cfg)
    assert app.config["auto_migrate"] == "safe"


def test_init_toml_has_auth_config(tmp_path: Path):
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    assert "_mkio_users" in cfg["tables"]
    assert "_mkio_rights" in cfg["tables"]
    assert cfg["tables"]["_mkio_users"]["seed"] == "data/users.csv"
    assert cfg["tables"]["_mkio_rights"]["seed"] == "data/rights.csv"
    assert cfg["monitor_access"] == "admin"


def test_init_services_have_access(tmp_path: Path):
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        cfg = tomllib.load(f)
    for name, svc in cfg["services"].items():
        assert "access" in svc, f"service '{name}' missing access"


def test_init_seed_file_content(tmp_path: Path):
    proj = tmp_path / "proj"
    init(proj)
    import csv
    with open(proj / "data" / "users.csv") as f:
        users = list(csv.DictReader(f))
    assert len(users) == 2
    usernames = {u["username"] for u in users}
    assert usernames == {"admin", "user"}
    assert all(u["password"].startswith("pbkdf2:") for u in users)

    with open(proj / "data" / "rights.csv") as f:
        rights = list(csv.DictReader(f))
    roles_rights = {(r["role"], r["right"]) for r in rights}
    assert ("admin", "admin") in roles_rights
    assert ("admin", "view") in roles_rights
    assert ("user", "edit") in roles_rights
    assert ("user", "view") in roles_rights


def test_init_index_has_login_form(tmp_path: Path):
    init(tmp_path / "proj")
    html = (tmp_path / "proj" / "static" / "index.html").read_text()
    assert "login-form" in html
    assert 'client.auth' in html
    assert "password" in html


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
    """get_default_config() and init() should produce equivalent configs (minus seed paths)."""
    init(tmp_path / "proj")
    with open(tmp_path / "proj" / "server.toml", "rb") as f:
        from_init = tomllib.load(f)
    for tbl in from_init.get("tables", {}).values():
        tbl.pop("seed", None)
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
