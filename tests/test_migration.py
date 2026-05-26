"""Tests for schema migration."""

import sqlite3
import pytest
from mkio.migration import (
    _allowed_levels,
    check_schema,
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
    result = migrate_schema(conn, config, level="safe")
    assert result is True
    conn.close()


def test_migrate_schema_safe_auto():
    conn = _create_db({})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, level="safe")
    assert result is True
    # Table should exist
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    assert any(t[0] == "orders" for t in tables)
    conn.close()


def test_migrate_schema_destructive_blocked_at_safe():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, level="safe")
    assert result is False
    conn.close()


def test_migrate_schema_destructive_allowed():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    conn.execute("INSERT INTO orders VALUES ('a', 'old')")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, level="destructive")
    assert result is True
    row = conn.execute("SELECT * FROM orders").fetchone()
    assert row == ("a",)
    conn.close()


# --- Level-based migration tests ---


def test_allowed_levels_safe():
    assert _allowed_levels("safe") == {"safe"}


def test_allowed_levels_risky():
    assert _allowed_levels("risky") == {"safe", "potentially_destructive"}


def test_allowed_levels_destructive():
    assert _allowed_levels("destructive") == {"safe", "potentially_destructive", "destructive"}


def test_allowed_levels_unknown():
    assert _allowed_levels("bogus") == set()


def test_check_schema_no_changes():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    changes = check_schema(conn, config)
    assert changes == []
    conn.close()


def test_check_schema_detects_new_table():
    conn = _create_db({})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    changes = check_schema(conn, config)
    assert len(changes) == 1
    assert changes[0].level == "safe"
    conn.close()


def test_check_schema_detects_removed_column():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    changes = check_schema(conn, config)
    destructive = [c for c in changes if c.level == "destructive"]
    assert len(destructive) == 1
    conn.close()


def test_migrate_risky_allows_type_change():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "TEXT"}}})
    conn.execute("INSERT INTO orders VALUES ('a', '100')")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}}
    result = migrate_schema(conn, config, level="risky")
    assert result is True
    row = conn.execute("SELECT * FROM orders").fetchone()
    # SQLite type affinity may coerce the value
    assert row[0] == "a"
    assert str(row[1]) == "100"
    conn.close()


def test_migrate_risky_blocks_destructive():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    result = migrate_schema(conn, config, level="risky")
    assert result is False
    # Column still exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(orders)").fetchall()]
    assert "legacy" in cols
    conn.close()


def test_migrate_safe_applies_new_table_blocks_remove():
    """Safe level applies new tables but blocks column removal."""
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY", "legacy": "TEXT"}}})
    config = {
        "orders": {"columns": {"id": "TEXT PRIMARY KEY"}},
        "items": {"columns": {"id": "TEXT PRIMARY KEY"}},
    }
    result = migrate_schema(conn, config, level="safe")
    assert result is False  # blocked changes remain
    # But new table was created
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert "items" in tables
    # And legacy column still exists (blocked)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(orders)").fetchall()]
    assert "legacy" in cols
    conn.close()


def test_migrate_multiple_changes_mixed_levels():
    """Multiple changes at different levels, partial apply at risky."""
    conn = _create_db({
        "orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "TEXT", "legacy": "TEXT"}},
    })
    conn.execute("INSERT INTO orders VALUES ('a', '10', 'old')")
    conn.commit()
    # Change type (risky) + remove column (destructive)
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "qty": "INTEGER"}}}
    result = migrate_schema(conn, config, level="risky")
    assert result is False  # destructive blocked
    conn.close()


def test_check_schema_ignores_mkio_ref():
    """_mkio_ref column in DB is not flagged as needing removal."""
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}})
    conn.execute("ALTER TABLE orders ADD COLUMN _mkio_ref TEXT DEFAULT ''")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}}
    changes = check_schema(conn, config)
    assert changes == []
    conn.close()


def test_add_not_null_column_without_default_potentially_destructive():
    conn = _create_db({"orders": {"columns": {"id": "TEXT PRIMARY KEY"}}})
    conn.execute("INSERT INTO orders VALUES ('a')")
    conn.commit()
    config = {"orders": {"columns": {"id": "TEXT PRIMARY KEY", "name": "TEXT NOT NULL"}}}
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config)
    assert any(c.level == "potentially_destructive" for c in changes)
    conn.close()


def test_composite_pk_table_creation():
    """Tables with composite primary key create correctly."""
    conn = sqlite3.connect(":memory:")
    config = {
        "items": {
            "columns": {"category": "TEXT NOT NULL", "name": "TEXT NOT NULL", "value": "TEXT"},
            "primary_key": ["category", "name"],
        }
    }
    result = migrate_schema(conn, config, level="safe")
    assert result is True
    conn.execute("INSERT INTO items VALUES ('cat1', 'item1', 'val1')")
    conn.commit()
    # Verify PK constraint
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO items VALUES ('cat1', 'item1', 'val2')")
    conn.close()
