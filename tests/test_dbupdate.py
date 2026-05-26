"""Tests for mkio dbupdate CLI and Database startup migration behavior."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from mkio.config import load_config
from mkio.database import Database
from mkio.migration import check_schema


def _write_config(tmp: Path, auto_migrate=False) -> Path:
    """Write a test config TOML file."""
    am_line = f'auto_migrate = "{auto_migrate}"' if isinstance(auto_migrate, str) else (
        "auto_migrate = true" if auto_migrate else "auto_migrate = false"
    )
    config_path = tmp / "server.toml"
    config_path.write_text(f"""\
port = 9999
db_path = "{tmp / 'test.db'}"
{am_line}

[tables.items]
columns = {{ id = "TEXT PRIMARY KEY", name = "TEXT" }}
""")
    return config_path


def _create_db_with_extra_column(tmp: Path) -> None:
    """Create a DB with an extra column not in config (triggers destructive change)."""
    db_path = tmp / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, extra TEXT)")
    conn.execute("INSERT INTO items VALUES ('1', 'first', 'old_data')")
    conn.commit()
    conn.close()


def _create_matching_db(tmp: Path) -> None:
    """Create a DB matching the config schema."""
    db_path = tmp / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()


# --- Database._run_migration tests ---


class TestDatabaseMigration:
    def test_auto_migrate_safe_creates_table(self, tmp_path):
        config_path = _write_config(tmp_path, auto_migrate="safe")
        cfg = load_config(str(config_path))
        db = Database(path=cfg["db_path"], tables=cfg["tables"], config=cfg)
        db._run_migration()
        conn = sqlite3.connect(cfg["db_path"])
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "items" in tables
        conn.close()

    def test_auto_migrate_false_blocks_when_schema_differs(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path, auto_migrate=False)
        cfg = load_config(str(config_path))
        db = Database(path=cfg["db_path"], tables=cfg["tables"], config=cfg)
        with pytest.raises(SystemExit) as exc_info:
            db._run_migration()
        assert exc_info.value.code == 1

    def test_auto_migrate_false_passes_when_schema_matches(self, tmp_path):
        _create_matching_db(tmp_path)
        config_path = _write_config(tmp_path, auto_migrate=False)
        cfg = load_config(str(config_path))
        db = Database(path=cfg["db_path"], tables=cfg["tables"], config=cfg)
        db._run_migration()  # Should not raise

    def test_auto_migrate_safe_blocks_destructive(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path, auto_migrate="safe")
        cfg = load_config(str(config_path))
        db = Database(path=cfg["db_path"], tables=cfg["tables"], config=cfg)
        with pytest.raises(SystemExit) as exc_info:
            db._run_migration()
        assert exc_info.value.code == 1

    def test_auto_migrate_destructive_applies_all(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path, auto_migrate="destructive")
        cfg = load_config(str(config_path))
        db = Database(path=cfg["db_path"], tables=cfg["tables"], config=cfg)
        db._run_migration()
        conn = sqlite3.connect(cfg["db_path"])
        cols = [r[1] for r in conn.execute("PRAGMA table_info(items)").fetchall()]
        assert "extra" not in cols
        assert "id" in cols
        assert "name" in cols
        row = conn.execute("SELECT * FROM items").fetchone()
        assert row == ("1", "first")
        conn.close()

    def test_auto_migrate_true_normalizes_to_safe(self, tmp_path):
        config_path = _write_config(tmp_path, auto_migrate=True)
        cfg = load_config(str(config_path))
        assert cfg["auto_migrate"] == "safe"

    def test_skip_migration_flag(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path, auto_migrate=False)
        cfg = load_config(str(config_path))
        db = Database(path=cfg["db_path"], tables=cfg["tables"], config=cfg, skip_migration=True)
        # Should not raise even with schema mismatch
        # (skip_migration is used when preflight already ran)
        # _run_migration is not called by start() when skip_migration=True


# --- mkio dbupdate CLI tests ---


class TestDbUpdateCLI:
    def test_dbupdate_creates_tables(self, tmp_path):
        config_path = _write_config(tmp_path)
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "dbupdate", str(config_path)],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "Applied (safe)" in result.stdout
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "items" in tables
        conn.close()

    def test_dbupdate_no_changes(self, tmp_path):
        _create_matching_db(tmp_path)
        config_path = _write_config(tmp_path)
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "dbupdate", str(config_path)],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "up to date" in result.stdout

    def test_dbupdate_blocks_destructive_by_default(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path)
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "dbupdate", str(config_path)],
            capture_output=True, text=True,
        )
        assert result.returncode == 1
        assert "Blocked" in result.stdout
        # Column still exists
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        cols = [r[1] for r in conn.execute("PRAGMA table_info(items)").fetchall()]
        assert "extra" in cols
        conn.close()

    def test_dbupdate_allow_destructive(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path)
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "dbupdate", "--allow-destructive", str(config_path)],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "Applied (destructive)" in result.stdout
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        cols = [r[1] for r in conn.execute("PRAGMA table_info(items)").fetchall()]
        assert "extra" not in cols
        row = conn.execute("SELECT * FROM items").fetchone()
        assert row == ("1", "first")
        conn.close()

    def test_dbupdate_allow_risky_still_blocks_destructive(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path)
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "dbupdate", "--allow-risky", str(config_path)],
            capture_output=True, text=True,
        )
        assert result.returncode == 1
        assert "Blocked" in result.stdout

    def test_dbupdate_missing_config(self):
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "dbupdate", "/nonexistent/config.toml"],
            capture_output=True, text=True,
        )
        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_dbupdate_memory_db_error(self, tmp_path):
        config_path = tmp_path / "server.toml"
        config_path.write_text("""\
db_path = ":memory:"

[tables.items]
columns = { id = "TEXT PRIMARY KEY" }
""")
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "dbupdate", str(config_path)],
            capture_output=True, text=True,
        )
        assert result.returncode == 1
        assert "in-memory" in result.stdout


# --- Preflight service validation tests ---


class TestPreflight:
    def test_preflight_catches_bad_sql(self, tmp_path):
        config_path = tmp_path / "server.toml"
        config_path.write_text(f"""\
port = 9999
db_path = "{tmp_path / 'test.db'}"
auto_migrate = "safe"

[tables.items]
columns = {{ id = "TEXT PRIMARY KEY", name = "TEXT" }}

[services.feed]
protocol = "stream"
primary_table = "items"
sql = "SELECT id, bogus_column FROM items"
""")
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "serve", str(config_path)],
            capture_output=True, text=True,
            timeout=10,
        )
        assert result.returncode == 1
        assert "bogus_column" in result.stderr

    def test_preflight_passes_valid_config(self, tmp_path):
        config_path = tmp_path / "server.toml"
        config_path.write_text(f"""\
port = 9999
db_path = "{tmp_path / 'test.db'}"
auto_migrate = "safe"

[tables.items]
columns = {{ id = "TEXT PRIMARY KEY", name = "TEXT" }}

[services.feed]
protocol = "stream"
primary_table = "items"
""")
        # Server should start and run (timeout means success — it got past preflight)
        proc = subprocess.Popen(
            [sys.executable, "-m", "mkio", "serve", str(config_path)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        import time
        time.sleep(2)
        assert proc.poll() is None  # still running = preflight passed
        proc.terminate()
        proc.wait(timeout=3)

    def test_serve_refuses_schema_mismatch(self, tmp_path):
        _create_db_with_extra_column(tmp_path)
        config_path = _write_config(tmp_path, auto_migrate=False)
        result = subprocess.run(
            [sys.executable, "-m", "mkio", "serve", str(config_path)],
            capture_output=True, text=True,
            timeout=10,
        )
        assert result.returncode == 1
        assert "Schema out of date" in result.stdout
