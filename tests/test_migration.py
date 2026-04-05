"""Tests for schema migration."""

import sqlite3
import pytest
from mkio.migration import (
    diff_schema,
    get_existing_schema,
    apply_changes,
    migrate_schema,
)


def _create_db(tables: dict) -> sqlite3.Connection:
    """Create an in-memory DB with the given schema."""
    conn = sqlite3.connect(":memory:")
    for name, spec in tables.items():
        cols = ", ".join(f"{col} {typ}" for col, typ in spec["columns"].items())
        conn.execute(f"CREATE TABLE {name} ({cols})")
    conn.commit()
    return conn


def test_new_table_safe():
    conn = _create_db({})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    assert len(changes) == 1
    assert changes[0].level == "safe"
    assert "Create new table" in changes[0].description
    conn.close()


def test_add_nullable_column_safe():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}})
    conn.execute("INSERT INTO orders VALUES ('a')")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    assert len(changes) == 1
    assert changes[0].level == "safe"
    apply_changes(conn, changes)
    row = conn.execute("SELECT * FROM orders").fetchone()
    assert row == ("a", None)
    conn.close()


def test_add_column_with_default_safe():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}})
    conn.execute("INSERT INTO orders VALUES ('a')")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "status": "TEXT DEFAULT 'pending'"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    safe = [c for c in changes if c.level == "safe"]
    assert len(safe) >= 1
    apply_changes(conn, changes)
    row = conn.execute("SELECT status FROM orders").fetchone()
    assert row[0] == "pending"
    conn.close()


def test_remove_column_destructive():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    conn.execute("INSERT INTO orders VALUES ('a', 'old_data')")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    destructive = [c for c in changes if c.level == "destructive"]
    assert len(destructive) == 1
    assert "Remove column" in destructive[0].description
    apply_changes(conn, changes)
    # Data preserved for remaining columns
    row = conn.execute("SELECT * FROM orders").fetchone()
    assert row == ("a",)
    conn.close()


def test_remove_table_destructive():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}})
    config = {}  # No tables in config
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    assert len(changes) == 1
    assert changes[0].level == "destructive"
    assert "Remove table" in changes[0].description
    conn.close()


def test_change_primary_key_potentially_destructive():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "symbol": "TEXT"}}})
    conn.execute("INSERT INTO orders VALUES ('a', 'AAPL')")
    conn.execute("INSERT INTO orders VALUES ('b', 'AAPL')")
    conn.commit()
    # Change PK from [id] to [symbol] — duplicate 'AAPL' values
    config = {"orders": {"columns": {"id": "TEXT", "symbol": "TEXT PRIMARY KEY"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    pk_changes = [c for c in changes if "primary key" in c.description.lower()]
    assert len(pk_changes) == 1
    assert pk_changes[0].level == "potentially_destructive"
    conn.close()


def test_change_column_type_potentially_destructive():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "TEXT"}}})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    type_changes = [c for c in changes if "type" in c.description.lower()]
    assert len(type_changes) == 1
    assert type_changes[0].level == "potentially_destructive"
    conn.close()


def test_data_preservation_on_column_removal():
    """Remove a column from a table with 100 rows, verify all rows survive."""
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER", "legacy": "TEXT"}}})
    for i in range(100):
        conn.execute("INSERT INTO orders VALUES (?, ?, ?)", (str(i), i * 10, "old"))
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    apply_changes(conn, changes)
    count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert count == 100
    # Verify data
    row = conn.execute("SELECT * FROM orders WHERE id = '50'").fetchone()
    assert row == ("50", 500)
    conn.close()


def test_pk_dedup():
    """Change PK causing duplicates, verify dedup and count."""
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "symbol": "TEXT", "qty": "INTEGER"}}})
    # Create 10 rows, 5 have symbol='AAPL' (duplicates under new PK)
    for i in range(10):
        sym = "AAPL" if i < 5 else f"SYM{i}"
        conn.execute("INSERT INTO orders VALUES (?, ?, ?)", (str(i), sym, i * 10))
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT", "symbol": "TEXT PRIMARY KEY", "qty": "INTEGER"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    before, after = apply_changes(conn, changes)
    # 10 rows before, 6 after (5 AAPL deduped to 1, plus 5 unique)
    assert after == 6
    conn.close()


def test_migrate_schema_no_changes():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, interactive=False, auto_migrate=False)
    assert result is True
    conn.close()


def test_migrate_schema_safe_auto():
    conn = _create_db({})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, interactive=False, auto_migrate=False)
    assert result is True
    # Table should exist
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    assert any(t[0] == "orders" for t in tables)
    conn.close()


def test_migrate_schema_destructive_non_interactive_no_auto():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, interactive=False, auto_migrate=False)
    assert result is False  # Should refuse without auto_migrate
    conn.close()


def test_migrate_schema_destructive_auto_migrate():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    conn.execute("INSERT INTO orders VALUES ('a', 'old')")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, interactive=False, auto_migrate=True)
    assert result is True
    row = conn.execute("SELECT * FROM orders").fetchone()
    assert row == ("a",)
    conn.close()
