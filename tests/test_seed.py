"""Tests for table seeding from CSV/JSON/JSONL files."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from mkio.config import load_config
from mkio.migration import (
    _load_seed_rows,
    seed_table,
    async_seed_table,
    diff_schema,
    apply_changes,
    get_existing_schema,
)


# ---------------------------------------------------------------------------
# Unit tests: _load_seed_rows
# ---------------------------------------------------------------------------


def test_load_csv(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("name,qty,price\nAlice,10,1.5\nBob,20,2.0\n")
    rows = _load_seed_rows(str(f))
    assert len(rows) == 2
    assert rows[0] == {"name": "Alice", "qty": 10, "price": 1.5}
    assert rows[1] == {"name": "Bob", "qty": 20, "price": 2.0}


def test_load_json(tmp_path):
    f = tmp_path / "data.json"
    f.write_text(json.dumps([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]))
    rows = _load_seed_rows(str(f))
    assert len(rows) == 2
    assert rows[0] == {"a": 1, "b": "x"}


def test_load_json_not_array(tmp_path):
    f = tmp_path / "data.json"
    f.write_text(json.dumps({"a": 1}))
    with pytest.raises(ValueError, match="expected a JSON array"):
        _load_seed_rows(str(f))


def test_load_jsonl(tmp_path):
    f = tmp_path / "data.jsonl"
    f.write_text('{"role": "admin", "right": "all"}\n\n{"role": "user", "right": "view"}\n')
    rows = _load_seed_rows(str(f))
    assert len(rows) == 2
    assert rows[0] == {"role": "admin", "right": "all"}
    assert rows[1] == {"role": "user", "right": "view"}


def test_load_jsonl_bad_line(tmp_path):
    f = tmp_path / "data.jsonl"
    f.write_text('{"ok": true}\nnot json\n')
    with pytest.raises(ValueError, match="line 2"):
        _load_seed_rows(str(f))


def test_load_unsupported_extension(tmp_path):
    f = tmp_path / "data.xml"
    f.write_text("<data/>")
    with pytest.raises(ValueError, match="unsupported extension"):
        _load_seed_rows(str(f))


def test_load_empty_csv(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("name,qty\n")
    rows = _load_seed_rows(str(f))
    assert rows == []


def test_load_empty_json(tmp_path):
    f = tmp_path / "data.json"
    f.write_text("[]")
    rows = _load_seed_rows(str(f))
    assert rows == []


# ---------------------------------------------------------------------------
# Unit tests: seed_table (sync)
# ---------------------------------------------------------------------------


def test_seed_table_csv(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("id,name\n1,Alice\n2,Bob\n")

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    count = seed_table(conn, "t", str(f), {"id", "name"})
    conn.commit()

    assert count == 2
    rows = conn.execute("SELECT * FROM t ORDER BY id").fetchall()
    assert rows == [(1, "Alice"), (2, "Bob")]
    conn.close()


def test_seed_table_json(tmp_path):
    f = tmp_path / "data.json"
    f.write_text(json.dumps([{"role": "admin", "right": "all"}]))

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE rights (role TEXT, right TEXT)")
    count = seed_table(conn, "rights", str(f), {"role", "right"})
    conn.commit()

    assert count == 1
    rows = conn.execute("SELECT * FROM rights").fetchall()
    assert rows == [("admin", "all")]
    conn.close()


def test_seed_table_jsonl(tmp_path):
    f = tmp_path / "data.jsonl"
    f.write_text('{"k": "a", "v": 1}\n{"k": "b", "v": 2}\n')

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)")
    count = seed_table(conn, "kv", str(f), {"k", "v"})
    conn.commit()

    assert count == 2
    conn.close()


def test_seed_table_unknown_column(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("id,name,bogus\n1,Alice,x\n")

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    with pytest.raises(ValueError, match="unknown column.*bogus"):
        seed_table(conn, "t", str(f), {"id", "name"})
    conn.close()


def test_seed_table_empty_file(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("id,name\n")

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    count = seed_table(conn, "t", str(f), {"id", "name"})
    assert count == 0
    conn.close()


def test_seed_table_duplicate_pk(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("id,name\n1,Alice\n1,Bob\n")

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    with pytest.raises(sqlite3.IntegrityError):
        seed_table(conn, "t", str(f), {"id", "name"})
    conn.close()


# ---------------------------------------------------------------------------
# Config validation tests
# ---------------------------------------------------------------------------


def test_config_seed_path_relative_to_config(tmp_path):
    data = tmp_path / "data.csv"
    data.write_text("id\n1\n")
    config_file = tmp_path / "server.toml"
    config_file.write_text(f"""
[tables.t]
columns = {{ id = "INTEGER PRIMARY KEY" }}
seed = "data.csv"
""")
    cfg = load_config(str(config_file))
    assert cfg["tables"]["t"]["_seed_path"] == str(data.resolve())


def test_config_seed_path_cwd_relative(tmp_path):
    data = tmp_path / "seed.csv"
    data.write_text("id\n1\n")
    old_cwd = os.getcwd()
    try:
        os.chdir(str(tmp_path))
        cfg = load_config({
            "tables": {"t": {"columns": {"id": "INTEGER PRIMARY KEY"}, "seed": "./seed.csv"}},
        })
        assert cfg["tables"]["t"]["_seed_path"] == str(data.resolve())
    finally:
        os.chdir(old_cwd)


def test_config_seed_path_absolute(tmp_path):
    data = tmp_path / "abs.json"
    data.write_text("[]")
    cfg = load_config({
        "tables": {"t": {"columns": {"id": "INTEGER PRIMARY KEY"}, "seed": str(data)}},
    })
    assert cfg["tables"]["t"]["_seed_path"] == str(data.resolve())


def test_config_seed_file_not_found():
    with pytest.raises(ValueError, match="seed file not found"):
        load_config({
            "tables": {"t": {"columns": {"id": "INTEGER PRIMARY KEY"}, "seed": "/nonexistent/file.csv"}},
        })


def test_config_seed_bad_extension(tmp_path):
    f = tmp_path / "data.xml"
    f.write_text("")
    with pytest.raises(ValueError, match="must be .csv, .json, or .jsonl"):
        load_config({
            "tables": {"t": {"columns": {"id": "INTEGER PRIMARY KEY"}, "seed": str(f)}},
        })


# ---------------------------------------------------------------------------
# Integration tests: seed on table creation (on-disk)
# ---------------------------------------------------------------------------


def test_seed_on_disk_creation(tmp_path):
    """Seed data is loaded when a table is first created on disk."""
    data = tmp_path / "seed.csv"
    data.write_text("id,name\n1,Alice\n2,Bob\n")
    db_path = str(tmp_path / "test.db")

    config_tables = {
        "t": {
            "columns": {"id": "INTEGER PRIMARY KEY", "name": "TEXT"},
            "_seed_path": str(data),
        },
    }

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    from mkio.migration import migrate_schema
    migrate_schema(conn, config_tables, db_path, level="safe")

    rows = conn.execute("SELECT id, name FROM t ORDER BY id").fetchall()
    assert rows == [(1, "Alice"), (2, "Bob")]
    conn.close()


def test_seed_not_reapplied_on_restart(tmp_path):
    """Seed data is NOT reloaded when the table already exists."""
    data = tmp_path / "seed.csv"
    data.write_text("id,name\n1,Alice\n")
    db_path = str(tmp_path / "test.db")

    config_tables = {
        "t": {
            "columns": {"id": "INTEGER PRIMARY KEY", "name": "TEXT"},
            "_seed_path": str(data),
        },
    }

    # First run: creates table + seeds
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    from mkio.migration import migrate_schema
    migrate_schema(conn, config_tables, db_path, level="safe")
    conn.close()

    # Delete the row
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM t WHERE id = 1")
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0
    conn.close()

    # Second run: table already exists, seed should NOT re-run
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    migrate_schema(conn, config_tables, db_path, level="safe")
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0
    conn.close()


# ---------------------------------------------------------------------------
# Integration tests: seed on table creation (in-memory)
# ---------------------------------------------------------------------------


async def test_seed_in_memory(tmp_path):
    """Seed data is loaded for in-memory databases."""
    data = tmp_path / "seed.json"
    data.write_text(json.dumps([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]))

    from mkio.database import Database
    db = Database(
        path=":memory:",
        tables={"t": {"columns": {"id": "INTEGER PRIMARY KEY", "name": "TEXT"}, "_seed_path": str(data)}},
    )
    await db.start()
    rows = await db.read("SELECT * FROM t ORDER BY id")
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Bob"
    await db.stop()


async def test_seed_in_memory_jsonl(tmp_path):
    """JSONL seed format works for in-memory databases."""
    data = tmp_path / "seed.jsonl"
    data.write_text('{"k": "x", "v": 10}\n{"k": "y", "v": 20}\n')

    from mkio.database import Database
    db = Database(
        path=":memory:",
        tables={"kv": {"columns": {"k": "TEXT PRIMARY KEY", "v": "INTEGER"}, "_seed_path": str(data)}},
    )
    await db.start()
    rows = await db.read("SELECT * FROM kv ORDER BY k")
    assert len(rows) == 2
    assert rows[0]["k"] == "x"
    assert rows[0]["v"] == 10
    assert rows[0]["_mkio_ref"] != ""
    await db.stop()


# ---------------------------------------------------------------------------
# Integration test: full server with seed
# ---------------------------------------------------------------------------


def test_seed_populates_mkio_ref(tmp_path):
    """Seeded rows get a unique _mkio_ref timestamp when the column exists."""
    data = tmp_path / "data.csv"
    data.write_text("id,name\n1,Alice\n2,Bob\n")

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, _mkio_ref TEXT DEFAULT '')")
    count = seed_table(conn, "t", str(data), {"id", "name"})
    conn.commit()

    assert count == 2
    rows = conn.execute("SELECT _mkio_ref FROM t ORDER BY id").fetchall()
    refs = [r[0] for r in rows]
    assert all(r != "" for r in refs)
    assert refs[0][:4].isdigit()
    # Each row gets a unique ref
    assert len(set(refs)) == len(refs)
    # Refs are in chronological order (lexicographically sortable)
    assert refs == sorted(refs)
    conn.close()


def test_seed_unique_refs_many_rows(tmp_path):
    """Every seeded row gets a distinct _mkio_ref, even with many rows."""
    lines = ["id,name"] + [f"{i},row{i}" for i in range(100)]
    data = tmp_path / "data.csv"
    data.write_text("\n".join(lines) + "\n")

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, _mkio_ref TEXT DEFAULT '')")
    count = seed_table(conn, "t", str(data), {"id", "name"})
    conn.commit()

    assert count == 100
    refs = [r[0] for r in conn.execute("SELECT _mkio_ref FROM t ORDER BY id").fetchall()]
    assert len(set(refs)) == 100
    assert refs == sorted(refs)
    conn.close()


def test_seed_works_without_mkio_ref(tmp_path):
    """Seed still works on tables without _mkio_ref (e.g. unit test tables)."""
    data = tmp_path / "data.csv"
    data.write_text("id,name\n1,Alice\n")

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    count = seed_table(conn, "t", str(data), {"id", "name"})
    conn.commit()

    assert count == 1
    rows = conn.execute("SELECT * FROM t").fetchall()
    assert rows == [(1, "Alice")]
    conn.close()


def test_seed_on_disk_populates_mkio_ref(tmp_path):
    """On-disk seeding adds _mkio_ref column and populates unique refs."""
    data = tmp_path / "seed.csv"
    data.write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")
    db_path = str(tmp_path / "test.db")

    config_tables = {
        "t": {
            "columns": {"id": "INTEGER PRIMARY KEY", "name": "TEXT"},
            "_seed_path": str(data),
        },
    }

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    from mkio.migration import migrate_schema
    migrate_schema(conn, config_tables, db_path, level="safe")

    refs = [r[0] for r in conn.execute(
        "SELECT _mkio_ref FROM t ORDER BY id"
    ).fetchall()]
    assert len(refs) == 3
    assert all(r != "" for r in refs)
    assert len(set(refs)) == 3
    assert refs == sorted(refs)
    conn.close()


async def test_seed_in_memory_unique_refs(tmp_path):
    """In-memory seeding populates unique _mkio_ref per row."""
    data = tmp_path / "seed.csv"
    data.write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")

    from mkio.database import Database
    db = Database(
        path=":memory:",
        tables={"t": {"columns": {"id": "INTEGER PRIMARY KEY", "name": "TEXT"}, "_seed_path": str(data)}},
    )
    await db.start()
    rows = await db.read("SELECT _mkio_ref FROM t ORDER BY id")
    refs = [r["_mkio_ref"] for r in rows]
    assert len(refs) == 3
    assert all(r != "" for r in refs)
    assert len(set(refs)) == 3
    assert refs == sorted(refs)
    await db.stop()


async def test_seed_full_server(tmp_path):
    """Seed works through the full create_app → start path."""
    data = tmp_path / "items.csv"
    data.write_text("id,label\n1,first\n2,second\n")

    from mkio.app import create_app
    app = create_app({
        "port": 0,
        "db_path": ":memory:",
        "tables": {
            "items": {
                "columns": {"id": "INTEGER PRIMARY KEY", "label": "TEXT"},
                "seed": str(data),
            },
        },
        "services": {},
    })
    await app.start()
    try:
        rows = await app.query("SELECT * FROM items ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["label"] == "first"
        # Verify unique refs through full server path
        refs = [r["_mkio_ref"] for r in rows]
        assert len(set(refs)) == 2
    finally:
        await app.stop()
