"""Tests for versioned tables and their history tables."""

from __future__ import annotations

import csv
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from mkio import create_app, history_table
from mkio.config import load_config
from mkio.history import (
    HISTORY_SUFFIX,
    VERSION_COLUMN,
    base_table_name,
    column_type,
    history_columns,
    history_primary_key,
    history_spec,
    history_table_name,
    is_history_table,
    primary_key_columns,
    redo_plan,
    undo_plan,
    versioned_tables,
)
from mkio.migration import (
    apply_changes,
    check_schema,
    collect_redo_garbage,
    diff_schema,
    get_existing_schema,
    orphan_history_tables,
)


VERSIONED_CONFIG = {
    "port": 0,
    "db_path": ":memory:",
    "tables": {
        "orders": {
            "columns": {
                "id": "TEXT PRIMARY KEY",
                "sym": "TEXT NOT NULL",
                "qty": "INTEGER",
            },
            "versioned": True,
        },
        "plain": {"columns": {"id": "TEXT PRIMARY KEY", "v": "TEXT"}},
    },
    "services": {
        "ord": {
            "protocol": "transaction",
            "ops": {
                "new": [{"table": "orders", "op_type": "insert",
                         "fields": ["id", "sym", "qty"]}],
                "amend": [{"table": "orders", "op_type": "update",
                           "key": ["id"], "fields": ["qty"]}],
                "bump": [{"table": "orders", "op_type": "update",
                          "key": ["sym"], "fields": ["qty"]}],
                "kill": [{"table": "orders", "op_type": "delete", "key": ["id"]}],
                "undo": [{"table": "orders", "op_type": "undo", "key": ["id"]}],
                "redo": [{"table": "orders", "op_type": "redo", "key": ["id"]}],
            },
        },
        "pl": {
            "protocol": "transaction",
            "ops": [{"table": "plain", "op_type": "insert", "fields": ["id", "v"]}],
        },
    },
}


def _config(**overrides):
    import copy
    cfg = copy.deepcopy(VERSIONED_CONFIG)
    cfg.update(overrides)
    return cfg


@pytest.fixture
async def app():
    a = create_app(_config())
    await a.start()
    yield a
    await a.stop()


# ---------------------------------------------------------------------------
# Naming convention and schema derivation
# ---------------------------------------------------------------------------


def test_history_table_name_is_derivable():
    # The suffix keeps a history table sorted next to what it records.
    assert history_table_name("orders") == "orders__history"
    assert history_table("orders") == "orders__history"
    assert is_history_table("orders__history")
    assert not is_history_table("orders")
    assert base_table_name("orders__history") == "orders"
    assert base_table_name("orders") == "orders"


@pytest.mark.parametrize("col_def,expected", [
    ("TEXT PRIMARY KEY", "TEXT"),
    ("TEXT NOT NULL", "TEXT"),
    ("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER"),
    ("REAL DEFAULT 0", "REAL"),
    ("TEXT DEFAULT 'pending'", "TEXT"),
    ("DECIMAL(10, 2) NOT NULL", "DECIMAL(10, 2)"),
    ("VARCHAR(20)", "VARCHAR(20)"),
    ("TEXT", "TEXT"),
])
def test_column_type_strips_constraints(col_def, expected):
    assert column_type(col_def) == expected


def test_history_columns_carry_types_but_no_constraints():
    cols = history_columns(VERSIONED_CONFIG["tables"]["orders"])
    assert list(cols)[:5] == [
        VERSION_COLUMN, "_mkio_op", "_mkio_ref", "_mkio_user", "_mkio_service",
    ]
    # Source columns keep their type and lose PRIMARY KEY / NOT NULL: old
    # versions and deleted rows must be storable regardless of today's schema.
    assert cols["id"] == "TEXT"
    assert cols["sym"] == "TEXT"
    assert cols["qty"] == "INTEGER"


def test_history_excludes_framework_columns():
    cfg = {"columns": {"id": "TEXT PRIMARY KEY", "_mkio_ref": "TEXT"}}
    assert "_mkio_ref" in history_columns(cfg)  # as metadata, once
    assert history_spec("t", cfg).columns == ("id",)


def test_history_is_keyed_by_row_and_version():
    cfg = VERSIONED_CONFIG["tables"]["orders"]
    assert history_primary_key(cfg) == ["id", VERSION_COLUMN]


def test_composite_key_extends_into_the_history_key():
    cfg = {"columns": {"a": "TEXT", "b": "TEXT", "v": "INTEGER"},
           "primary_key": ["a", "b"]}
    assert history_primary_key(cfg) == ["a", "b", VERSION_COLUMN]


def test_primary_key_columns():
    assert primary_key_columns({"columns": {"id": "TEXT PRIMARY KEY"}}) == ["id"]
    assert primary_key_columns(
        {"columns": {"a": "TEXT", "b": "TEXT"}, "primary_key": ["a", "b"]}
    ) == ["a", "b"]
    assert primary_key_columns({"columns": {"a": "TEXT"}}) == []


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_history_tables_derived_at_load():
    cfg = load_config(_config())
    assert list(cfg["_history_tables"]) == ["orders__history"]
    assert cfg["_history_tables"]["orders__history"]["_history_of"] == "orders"
    assert list(versioned_tables(cfg)) == ["orders"]


def test_history_tables_are_not_advertised():
    cfg = load_config(_config())
    # They live under an underscore key, so they stay out of [tables] and
    # therefore out of config_hash and the _mkio table listing.
    assert "orders__history" not in cfg["tables"]


def test_no_history_tables_without_versioned():
    cfg = load_config({"tables": {"a": {"columns": {"id": "TEXT PRIMARY KEY"}}}})
    assert cfg["_history_tables"] == {}


def test_suffix_is_reserved_for_application_tables():
    with pytest.raises(ValueError, match="reserved"):
        load_config({"tables": {"x__history": {"columns": {"a": "TEXT"}}}})


def test_a_versioned_table_may_not_be_named_like_a_history_table():
    with pytest.raises(ValueError, match="reserved"):
        load_config({"tables": {"audit__history": {
            "columns": {"id": "TEXT PRIMARY KEY"}, "versioned": True}}})


def test_names_merely_containing_the_suffix_are_fine():
    """Only a trailing '__history' is reserved."""
    cfg = load_config({"tables": {
        "history_of_orders": {"columns": {"id": "TEXT PRIMARY KEY"}},
        "__history_log": {"columns": {"id": "TEXT PRIMARY KEY"}},
    }})
    assert cfg["_history_tables"] == {}
    assert not is_history_table("history_of_orders")
    assert not is_history_table("__history_log")


def test_versioned_must_be_bool():
    with pytest.raises(ValueError, match="must be true or false"):
        load_config({"tables": {"o": {"columns": {"a": "TEXT"}, "versioned": "yes"}}})


def test_versioned_table_cannot_declare_mkio_columns():
    with pytest.raises(ValueError, match="_mkio_"):
        load_config({"tables": {"o": {"columns": {"_mkio_op": "TEXT"}, "versioned": True}}})


def test_service_may_target_a_history_table():
    cfg = load_config(_config(services={
        "audit": {"protocol": "query", "primary_table": "orders__history",
                  "filterable": ["sym", "_mkio_op"]},
    }))
    assert cfg["services"]["audit"]["watch_tables"] == ["orders__history"]


def test_history_service_on_unversioned_table_is_rejected():
    with pytest.raises(ValueError, match="is not versioned"):
        load_config({
            "tables": {"o": {"columns": {"a": "TEXT"}}},
            "services": {"h": {"protocol": "query", "primary_table": "o__history"}},
        })


def test_history_service_on_missing_table_is_rejected():
    with pytest.raises(ValueError, match="no table 'zz'"):
        load_config({
            "tables": {"o": {"columns": {"a": "TEXT"}}},
            "services": {"h": {"protocol": "query", "primary_table": "zz__history"}},
        })


def test_history_tables_are_read_only_to_clients():
    with pytest.raises(ValueError, match="read-only"):
        load_config(_config(services={
            "bad": {"protocol": "transaction", "ops": [
                {"table": "orders__history", "op_type": "insert", "fields": ["id"]},
            ]},
        }))


def test_unknown_table_error_hides_history_tables():
    with pytest.raises(ValueError) as exc:
        load_config(_config(services={
            "q": {"protocol": "query", "primary_table": "nope"},
        }))
    assert "orders__history" not in str(exc.value)


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------


async def _history(app, **kwargs):
    return await app.history("orders", **kwargs)


async def test_versions_are_numbered_from_one(app):
    await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 25}, op="amend")
    await app.execute("ord", {"id": "O1", "qty": 30}, op="amend")

    rows = await _history(app)
    assert [r[VERSION_COLUMN] for r in rows] == [1, 2, 3]
    assert [r["_mkio_op"] for r in rows] == ["insert", "update", "update"]
    assert [r["qty"] for r in rows] == [10, 25, 30]
    # The whole row is captured, not just the changed fields.
    assert all(r["sym"] == "AAPL" for r in rows)
    # The live row sits on the newest version.
    live = (await app.query("SELECT * FROM orders"))[0]
    assert live[VERSION_COLUMN] == 3
    assert live["qty"] == 30


async def test_live_row_matches_its_recorded_version(app):
    await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 25}, op="amend")

    live = (await app.query("SELECT * FROM orders"))[0]
    version = [r for r in await _history(app)
               if r[VERSION_COLUMN] == live[VERSION_COLUMN]][0]
    for col in ("id", "sym", "qty"):
        assert live[col] == version[col]


async def test_delete_removes_the_row_and_its_versions(app):
    await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 25}, op="amend")
    await app.execute("ord", {"id": "O2", "sym": "MSFT", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O1"}, op="kill")

    # A delete is a real delete: the row goes and takes its chain with it.
    assert [r["id"] for r in await app.query("SELECT * FROM orders")] == ["O2"]
    assert [r["id"] for r in await _history(app)] == ["O2"]


async def test_upsert_starts_at_one_then_increments(app):
    cfg = _config()
    cfg["services"]["ord"]["ops"]["put"] = [
        {"table": "orders", "op_type": "upsert", "key": ["id"],
         "fields": ["sym", "qty"]}
    ]
    a = create_app(cfg)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="put")
        assert (await a.query("SELECT * FROM orders"))[0][VERSION_COLUMN] == 1
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 2}, op="put")
        assert (await a.query("SELECT * FROM orders"))[0][VERSION_COLUMN] == 2
        assert [r[VERSION_COLUMN] for r in await a.history("orders")] == [1, 2]
    finally:
        await a.stop()


async def test_ref_matches_the_transaction(app):
    r1 = await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    r2 = await app.execute("ord", {"id": "O2", "sym": "B", "qty": 2}, op="new")
    rows = await _history(app)
    assert [r["_mkio_ref"] for r in rows] == [r1["ref"], r2["ref"]]
    # Each row's chain is numbered independently.
    assert [r[VERSION_COLUMN] for r in rows] == [1, 1]


async def test_attribution_records_user_and_service(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new", user="alice")
    await app.execute("ord", {"id": "O2", "sym": "B", "qty": 2}, op="new")
    rows = await _history(app)
    assert rows[0]["_mkio_user"] == "alice"
    assert rows[0]["_mkio_service"] == "ord"
    assert rows[1]["_mkio_user"] is None


async def test_multi_row_update_records_every_affected_row(app):
    await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O2", "sym": "AAPL", "qty": 2}, op="new")
    await app.execute("ord", {"sym": "AAPL", "qty": 99}, op="bump")

    updates = [r for r in await _history(app) if r["_mkio_op"] == "update"]
    assert len(updates) == 2
    assert sorted(r["id"] for r in updates) == ["O1", "O2"]
    assert all(r["qty"] == 99 for r in updates)
    # Each affected row advances its own counter.
    assert all(r[VERSION_COLUMN] == 2 for r in updates)
    live = await app.query("SELECT * FROM orders ORDER BY id")
    assert [r[VERSION_COLUMN] for r in live] == [2, 2]


async def test_unversioned_tables_have_no_history(app):
    await app.execute("pl", {"id": "P1", "v": "x"})
    tables = await app.query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
        ("%" + HISTORY_SUFFIX,),
    )
    assert [t["name"] for t in tables] == ["orders__history"]


async def test_failed_transaction_records_nothing(app):
    await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 1}, op="new")
    with pytest.raises(Exception):
        # Duplicate primary key — rolled back to the savepoint.
        await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 2}, op="new")
    rows = await _history(app)
    assert len(rows) == 1


async def test_unversioned_tables_have_no_version_column(app):
    cols = {r["name"] for r in await app.query("PRAGMA table_info(plain)")}
    assert VERSION_COLUMN not in cols
    cols = {r["name"] for r in await app.query("PRAGMA table_info(orders)")}
    assert VERSION_COLUMN in cols


async def test_delete_change_event_carries_the_full_row(app):
    """Versioned deletes RETURN the row, so subscribers see what was removed."""
    events = []

    async def on_change(event):
        events.append(event)

    unsub = await app.subscribe(["orders"], on_change)
    await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 7}, op="new")
    await app.execute("ord", {"id": "O1"}, op="kill")
    import asyncio
    await asyncio.sleep(0.05)
    unsub()

    deletes = [e for e in events if e.op == "delete"]
    assert len(deletes) == 1
    assert deletes[0].row["sym"] == "AAPL"
    assert deletes[0].row["qty"] == 7


def test_unversioned_delete_sql_is_unchanged():
    """Regression: nothing changes for tables that are not versioned."""
    from mkio.services.transaction import _compile_op
    spec = {"table": "plain", "op_type": "delete", "key": ["id"]}
    assert _compile_op(spec).sql == "DELETE FROM plain WHERE id = ?"
    assert _compile_op(spec, frozenset({"plain"})).sql == (
        "DELETE FROM plain WHERE id = ? RETURNING *"
    )


# ---------------------------------------------------------------------------
# Live history feed
# ---------------------------------------------------------------------------


async def test_history_changes_reach_subscribers():
    cfg = _config()
    cfg["services"]["audit"] = {
        "protocol": "query", "primary_table": "orders__history",
    }
    a = create_app(cfg)
    await a.start()
    try:
        events = []

        async def on_change(event):
            events.append(event)

        unsub = await a.subscribe(["orders__history"], on_change)
        await a.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 3},
                        op="new", user="alice")
        import asyncio
        await asyncio.sleep(0.05)
        unsub()

        assert len(events) == 1
        row = events[0].row
        assert row["_mkio_op"] == "insert"
        assert row[VERSION_COLUMN] == 1
        assert row["_mkio_user"] == "alice"
        assert row["sym"] == "AAPL"
    finally:
        await a.stop()


async def test_no_history_events_without_subscribers(app):
    """The feed costs nothing when nothing is listening."""
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    assert not app.change_bus.has_subscribers("orders__history")
    assert len(await _history(app)) == 1


# ---------------------------------------------------------------------------
# MkioApp.history()
# ---------------------------------------------------------------------------


async def test_history_api_filters(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O2", "sym": "B", "qty": 2}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 5}, op="amend")

    assert len(await app.history("orders")) == 3
    versions = await app.history("orders", pk={"id": "O1"})
    assert [v[VERSION_COLUMN] for v in versions] == [1, 2]
    assert len(await app.history("orders", limit=1)) == 1
    assert (await app.history("orders", newest_first=True))[0]["id"] == "O2"
    # The history table name is accepted too.
    assert len(await app.history("orders__history")) == 3


async def test_history_api_rejects_unversioned_tables(app):
    with pytest.raises(ValueError, match="not versioned"):
        await app.history("plain")


async def test_previous_version_supports_undo(app):
    """The version to restore is the preceding history row for the same key."""
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 25}, op="amend")

    versions = await app.history("orders", pk={"id": "O1"})
    latest, previous = versions[-1], versions[-2]
    assert latest["qty"] == 25
    assert previous["qty"] == 10
    assert previous["_mkio_op"] == "insert"  # undoing that one means deleting


# ---------------------------------------------------------------------------
# Migration lifecycle
# ---------------------------------------------------------------------------


def _disk_config(tmp_path, versioned=True, columns=None):
    return load_config({
        "db_path": str(tmp_path / "t.db"),
        "tables": {"orders": {
            "columns": columns or {"id": "TEXT PRIMARY KEY", "sym": "TEXT"},
            **({"versioned": True} if versioned else {}),
        }},
    })


def _migrate(conn, cfg):
    from mkio.history import effective_tables
    tables = effective_tables(cfg)
    changes = diff_schema(get_existing_schema(conn), tables)
    apply_changes(conn, changes, tables)
    return changes


def test_history_table_created_alongside_base(tmp_path):
    cfg = _disk_config(tmp_path)
    conn = sqlite3.connect(cfg["db_path"])
    changes = _migrate(conn, cfg)
    assert all(c.level == "safe" for c in changes)
    info = list(conn.execute("PRAGMA table_info(orders__history)"))
    cols = {r[1] for r in info}
    assert {VERSION_COLUMN, "_mkio_op", "_mkio_user", "id", "sym"} <= cols
    # Keyed by (row, version), so a row can hold many versions but never two
    # of the same number.
    pk = sorted((r[5], r[1]) for r in info if r[5] > 0)
    assert [name for _, name in pk] == ["id", VERSION_COLUMN]
    indexes = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
        ("orders__history",))}
    assert "idx_orders__history_ref" in indexes
    conn.close()


def test_enabling_versioning_backfills_a_baseline(tmp_path):
    unversioned = _disk_config(tmp_path, versioned=False)
    conn = sqlite3.connect(unversioned["db_path"])
    _migrate(conn, unversioned)
    conn.execute("ALTER TABLE orders ADD COLUMN _mkio_ref TEXT DEFAULT ''")
    conn.execute("INSERT INTO orders (id, sym, _mkio_ref) VALUES ('A', 'X', 'r-a')")
    conn.execute("INSERT INTO orders (id, sym) VALUES ('B', 'Y')")
    conn.commit()

    _migrate(conn, _disk_config(tmp_path))

    rows = list(conn.execute(
        f"SELECT _mkio_op, _mkio_ref, id, {VERSION_COLUMN} "
        f"FROM orders__history ORDER BY id"))
    assert [r[0] for r in rows] == ["baseline", "baseline"]
    # Existing rows become version 1, matching the counter added to the base.
    assert [r[3] for r in rows] == [1, 1]
    assert [r[0] for r in conn.execute(
        f"SELECT {VERSION_COLUMN} FROM orders")] == [1, 1]
    # An existing row keeps its own ref, so archiving by age stays honest.
    assert rows[0][1] == "r-a"
    assert rows[1][1] != ""
    conn.close()


def test_history_table_gains_columns_added_to_the_base(tmp_path):
    cfg = _disk_config(tmp_path)
    conn = sqlite3.connect(cfg["db_path"])
    _migrate(conn, cfg)
    wider = _disk_config(tmp_path, columns={
        "id": "TEXT PRIMARY KEY", "sym": "TEXT", "qty": "INTEGER NOT NULL DEFAULT 0"})
    _migrate(conn, wider)
    cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(orders__history)")}
    assert cols["qty"] == "INTEGER"
    notnull = {r[1]: r[3] for r in conn.execute("PRAGMA table_info(orders__history)")}
    assert notnull["qty"] == 0  # constraints are never copied
    conn.close()


def test_history_is_additive_only(tmp_path):
    """Dropping a base column keeps the recorded values in history."""
    wide = _disk_config(tmp_path, columns={
        "id": "TEXT PRIMARY KEY", "sym": "TEXT", "qty": "INTEGER"})
    conn = sqlite3.connect(wide["db_path"])
    _migrate(conn, wide)

    narrow = _disk_config(tmp_path, columns={"id": "TEXT PRIMARY KEY", "sym": "TEXT"})
    from mkio.history import effective_tables
    changes = diff_schema(get_existing_schema(conn), effective_tables(narrow))
    history_changes = [c for c in changes if c.table == "orders__history"]
    assert history_changes == []
    conn.close()


def test_history_table_is_never_auto_dropped(tmp_path):
    cfg = _disk_config(tmp_path)
    conn = sqlite3.connect(cfg["db_path"])
    _migrate(conn, cfg)

    unversioned = _disk_config(tmp_path, versioned=False)
    from mkio.history import effective_tables
    tables = effective_tables(unversioned)
    changes = check_schema(conn, tables)
    # No pending change, so the server still starts.
    assert changes == []
    assert orphan_history_tables(conn, tables) == ["orders__history"]
    conn.close()


def test_recreating_the_base_table_leaves_history_intact(tmp_path):
    cfg = _disk_config(tmp_path)
    conn = sqlite3.connect(cfg["db_path"])
    _migrate(conn, cfg)
    conn.execute("ALTER TABLE orders ADD COLUMN _mkio_ref TEXT DEFAULT ''")
    conn.execute(f"INSERT INTO orders__history "
                 f"(_mkio_op, _mkio_ref, id, {VERSION_COLUMN}) "
                 f"VALUES ('insert', 'r1', 'A', 1)")
    conn.execute("INSERT INTO orders (id, sym, _mkio_ref) VALUES ('A', 'X', 'r1')")
    conn.commit()

    # A primary key change forces the recreate-table path on the base table.
    retyped = _disk_config(tmp_path, columns={"id": "TEXT", "sym": "TEXT PRIMARY KEY"})
    from mkio.history import effective_tables
    tables = effective_tables(retyped)
    changes = diff_schema(get_existing_schema(conn), tables)
    apply_changes(conn, [c for c in changes if c.sql_steps], tables)

    assert list(conn.execute("SELECT id FROM orders__history")) == [("A",)]
    # The cursor survives the recreate — losing it would strand the chain.
    assert list(conn.execute(f"SELECT {VERSION_COLUMN} FROM orders")) == [(1,)]
    conn.close()


async def test_seeded_rows_get_a_baseline(tmp_path):
    seed = tmp_path / "orders.csv"
    seed.write_text("id,sym\nS1,AAA\nS2,BBB\n")
    a = create_app({
        "port": 0, "db_path": ":memory:",
        "tables": {"orders": {
            "columns": {"id": "TEXT PRIMARY KEY", "sym": "TEXT"},
            "versioned": True,
            "seed": str(seed),
        }},
    })
    await a.start()
    try:
        rows = await a.history("orders")
        assert [r["_mkio_op"] for r in rows] == ["baseline", "baseline"]
        assert sorted(r["id"] for r in rows) == ["S1", "S2"]
    finally:
        await a.stop()


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


async def _info(app, data=None):
    from tests.conftest import MockWebSocket
    ws = MockWebSocket()
    await app.services["_mkio"].on_message(
        ws, {"type": "request", "reqid": "r1", **({"data": data} if data else {})})
    return ws.get_messages()[0]


async def test_info_reports_versioned_tables_and_the_convention(app):
    row = (await _info(app))["row"]
    assert row["versioned"] == ["orders"]
    assert row["history_suffix"] == HISTORY_SUFFIX
    # History tables themselves stay out of the listing.
    assert row["tables"] == ["orders", "plain"]
    assert row["protocol"] == "1.1"


async def test_schema_query_points_at_the_history_table(app):
    row = (await _info(app, {"table": "orders"}))["row"]
    assert row["versioned"] is True
    assert row["history_table"] == "orders__history"


async def test_schema_query_works_on_a_history_table(app):
    row = (await _info(app, {"table": "orders__history"}))["row"]
    assert row["history_of"] == "orders"
    names = [c["name"] for c in row["columns"]]
    assert names[:5] == [
        VERSION_COLUMN, "_mkio_op", "_mkio_ref", "_mkio_user", "_mkio_service"]


async def test_unknown_table_error_lists_only_base_tables(app):
    msg = await _info(app, {"table": "nope"})
    assert msg["type"] == "error"
    assert "orders__history" not in msg["message"]


async def test_unversioned_table_schema_has_no_history_fields(app):
    row = (await _info(app, {"table": "plain"}))["row"]
    assert "versioned" not in row
    assert "history_table" not in row


# ---------------------------------------------------------------------------
# Archiving
# ---------------------------------------------------------------------------


OLD_REF = "20200101 00:00:00.000000000000"


async def _archive_fixture(tmp_path):
    """A DB with three orders: one still changing, one settled, one deleted."""
    cfg_path = tmp_path / "server.toml"
    cfg_path.write_text(f"""
db_path = "{tmp_path / 'a.db'}"
auto_migrate = "safe"

[tables.orders]
columns = {{ id = "TEXT PRIMARY KEY", sym = "TEXT", qty = "INTEGER" }}
versioned = true

[services.ord]
protocol = "transaction"

[services.ord.ops]
new = [{{ table = "orders", op_type = "insert", fields = ["id", "sym", "qty"] }}]
amend = [{{ table = "orders", op_type = "update", key = ["id"], fields = ["qty"] }}]
kill = [{{ table = "orders", op_type = "delete", key = ["id"] }}]
""")
    a = create_app(str(cfg_path))
    await a.start()
    await a.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 10}, op="new", user="alice")
    await a.execute("ord", {"id": "O1", "qty": 25}, op="amend", user="alice")
    await a.execute("ord", {"id": "O2", "sym": "MSFT", "qty": 5}, op="new", user="bob")
    await a.execute("ord", {"id": "O3", "sym": "TSLA", "qty": 1}, op="new")
    await a.execute("ord", {"id": "O3"}, op="kill")
    await a.stop()

    # Age everything except O1's amendment, so O1 still has recent history.
    conn = sqlite3.connect(tmp_path / "a.db")
    conn.execute(f"UPDATE orders__history SET _mkio_ref = ? "
                 f"WHERE NOT (id = 'O1' AND {VERSION_COLUMN} = 2)", (OLD_REF,))
    conn.execute("UPDATE orders SET _mkio_ref = ? WHERE id = 'O2'", (OLD_REF,))
    conn.commit()
    conn.close()
    return cfg_path


def _run_archive(cfg_path, *args):
    result = subprocess.run(
        [sys.executable, "-m", "mkio", "archive", str(cfg_path), *args],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _rows(db, sql):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql)]
    finally:
        conn.close()


async def test_archive_protects_the_current_version(tmp_path):
    """Archiving a live row's current version would strand it."""
    cfg_path = await _archive_fixture(tmp_path)
    _run_archive(cfg_path, "--older-than", "30d", "--out", str(tmp_path / "out"))

    files = list((tmp_path / "out").glob("*.csv"))
    assert len(files) == 1
    with open(files[0], newline="") as f:
        archived = list(csv.DictReader(f))
    # O1 sits on v2, so only its v1 is archivable. O2 sits on its only version
    # and O3's chain went with the delete.
    assert [(r["id"], r[VERSION_COLUMN]) for r in archived] == [("O1", "1")]
    assert archived[0]["_mkio_user"] == "alice"
    # Nothing removed without --delete.
    assert len(_rows(tmp_path / "a.db", "SELECT * FROM orders__history")) == 3


async def test_archive_delete_purges_only_archived_rows(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    _run_archive(cfg_path, "--older-than", "30d", "--out", str(tmp_path / "out"), "--delete")

    remaining = _rows(tmp_path / "a.db", "SELECT * FROM orders__history")
    # Every live row keeps the version it sits on; O1 just loses its undo depth.
    assert [(r["id"], r[VERSION_COLUMN]) for r in remaining] == [("O1", 2), ("O2", 1)]
    # Live rows are untouched without --prune-source.
    assert len(_rows(tmp_path / "a.db", "SELECT * FROM orders")) == 2


async def test_archive_prune_source_removes_only_settled_rows(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    _run_archive(cfg_path, "--older-than", "30d", "--out", str(tmp_path / "out"),
                 "--prune-source", "--yes")

    live = _rows(tmp_path / "a.db", "SELECT id FROM orders")
    # O2 is archived in full and unchanged since, so it goes.
    # O1 still has newer history; O3 was already deleted.
    assert [r["id"] for r in live] == ["O1"]


async def test_archive_keeps_a_source_row_that_changed_since(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    # O2's live row no longer matches its archived version.
    conn = sqlite3.connect(tmp_path / "a.db")
    conn.execute("UPDATE orders SET qty = 999 WHERE id = 'O2'")
    conn.commit()
    conn.close()

    _run_archive(cfg_path, "--older-than", "30d", "--out", str(tmp_path / "out"),
                 "--prune-source", "--yes")
    live = sorted(r["id"] for r in _rows(tmp_path / "a.db", "SELECT id FROM orders"))
    assert live == ["O1", "O2"]


async def test_archive_dry_run_changes_nothing(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    out = _run_archive(cfg_path, "--older-than", "30d", "--out", str(tmp_path / "out"),
                       "--prune-source", "--dry-run")

    assert "would be" in out
    assert "1 source rows would be pruned" in out
    assert not (tmp_path / "out").exists()
    assert len(_rows(tmp_path / "a.db", "SELECT * FROM orders__history")) == 3
    assert len(_rows(tmp_path / "a.db", "SELECT * FROM orders")) == 2


async def test_archive_cutoff_keeps_recent_rows(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    out = _run_archive(cfg_path, "--older-than", "20000d", "--out", str(tmp_path / "out"))
    assert "no archivable versions older than the cutoff" in out
    assert not list((tmp_path / "out").glob("*.csv"))


async def test_archive_prune_source_requires_confirmation(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "mkio", "archive", str(cfg_path),
         "--older-than", "30d", "--prune-source"],
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert "--yes" in result.stdout


async def test_archive_rejects_unversioned_table(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "mkio", "archive", str(cfg_path),
         "--older-than", "30d", "--table", "nope"],
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert "not versioned" in result.stdout


async def test_archive_accepts_the_history_table_name(tmp_path):
    cfg_path = await _archive_fixture(tmp_path)
    out = _run_archive(cfg_path, "--older-than", "30d", "--table",
                       "orders__history", "--out", str(tmp_path / "out"))
    assert "1 versions" in out


# ---------------------------------------------------------------------------
# Attribution over a real authenticated connection
# ---------------------------------------------------------------------------


AUTH_HISTORY_CONFIG = {
    "db_path": ":memory:",
    "port": 0,
    "tables": {
        "orders": {
            "columns": {"id": "TEXT PRIMARY KEY", "sym": "TEXT", "qty": "INTEGER"},
            "versioned": True,
        },
        "_mkio_users": {"columns": {
            "username": "TEXT PRIMARY KEY", "password": "TEXT NOT NULL",
            "role": "TEXT NOT NULL"}},
        "_mkio_rights": {"columns": {"role": "TEXT", "right": "TEXT"}},
    },
    "services": {
        "ord": {
            "protocol": "transaction",
            "ops": [{"table": "orders", "op_type": "insert",
                     "fields": ["id", "sym", "qty"]}],
            "access": "trade",
        },
        "order_history": {
            "protocol": "query",
            "primary_table": "orders__history",
            "access": "audit",
        },
    },
}


async def test_authenticated_writes_are_attributed():
    from mkio.auth import hash_password, load_rights_cache
    from mkio.client import MkioClient

    a = create_app(AUTH_HISTORY_CONFIG)
    await a.start()
    try:
        await a.db.write_conn.execute(
            "INSERT INTO _mkio_users (username, password, role) VALUES (?, ?, ?)",
            ("alice", hash_password("secret"), "trader"),
        )
        await a.db.write_conn.execute(
            "INSERT INTO _mkio_rights (role, right) VALUES ('trader', 'trade')")
        await a.db.write_conn.commit()
        cache = await load_rights_cache(a.db)
        a._aiohttp_app["rights_cache"]._rights = cache._rights

        for sock in a._site._server.sockets:
            port = sock.getsockname()[1]
            break
        async with MkioClient(f"ws://localhost:{port}/ws", reconnect=False) as client:
            await client.auth({"username": "alice", "password": "secret"})
            await client.send("ord", {"id": "O1", "sym": "AAPL", "qty": 3})

        rows = await a.history("orders")
        assert len(rows) == 1
        assert rows[0]["_mkio_user"] == "alice"
        assert rows[0]["_mkio_service"] == "ord"
    finally:
        await a.stop()


async def test_history_service_is_access_controlled():
    """A service on a history table is gated like any other."""
    cfg = load_config(AUTH_HISTORY_CONFIG)
    assert cfg["services"]["order_history"]["access"] == "audit"
    assert cfg["auth"] is True


# ---------------------------------------------------------------------------
# Undo and redo
# ---------------------------------------------------------------------------


async def _state(app):
    """(live version or None, [recorded versions]) for order O1."""
    live = await app.query(
        f"SELECT {VERSION_COLUMN} v FROM orders WHERE id = 'O1'")
    versions = await app.query(
        f"SELECT {VERSION_COLUMN} v FROM orders__history "
        f"WHERE id = 'O1' ORDER BY {VERSION_COLUMN}")
    return (live[0]["v"] if live else None), [r["v"] for r in versions]


async def _three_versions(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 20}, op="amend")
    await app.execute("ord", {"id": "O1", "qty": 30}, op="amend")


async def test_undo_steps_the_cursor_back_leaving_history(app):
    await _three_versions(app)
    await app.execute("ord", {"id": "O1"}, op="undo")

    live = (await app.query("SELECT * FROM orders"))[0]
    assert live[VERSION_COLUMN] == 2
    assert live["qty"] == 20
    # History is untouched, so the step is reversible.
    assert await _state(app) == (2, [1, 2, 3])


async def test_redo_steps_the_cursor_forward(app):
    await _three_versions(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    await app.execute("ord", {"id": "O1"}, op="undo")
    assert await _state(app) == (1, [1, 2, 3])

    await app.execute("ord", {"id": "O1"}, op="redo")
    live = (await app.query("SELECT * FROM orders"))[0]
    assert (live[VERSION_COLUMN], live["qty"]) == (2, 20)


async def test_undo_redo_round_trips_exactly(app):
    await _three_versions(app)
    before = (await app.query("SELECT * FROM orders"))[0]
    await app.execute("ord", {"id": "O1"}, op="undo")
    await app.execute("ord", {"id": "O1"}, op="redo")
    after = (await app.query("SELECT * FROM orders"))[0]
    for col in ("id", "sym", "qty", VERSION_COLUMN):
        assert before[col] == after[col]


async def test_undo_at_version_one_deletes_the_row(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1"}, op="undo")
    # The row goes, but its recorded version stays so redo can rebuild it.
    assert await _state(app) == (None, [1])


async def test_redo_rebuilds_a_row_undone_past_one(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1"}, op="undo")
    await app.execute("ord", {"id": "O1"}, op="redo")

    live = (await app.query("SELECT * FROM orders"))[0]
    assert (live["id"], live["sym"], live["qty"], live[VERSION_COLUMN]) == (
        "O1", "A", 10, 1)


async def test_undo_all_the_way_down_and_back_up(app):
    await _three_versions(app)
    for _ in range(3):
        await app.execute("ord", {"id": "O1"}, op="undo")
    assert await _state(app) == (None, [1, 2, 3])
    for _ in range(3):
        await app.execute("ord", {"id": "O1"}, op="redo")
    assert await _state(app) == (3, [1, 2, 3])


async def test_undo_past_the_bottom_fails(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O1"}, op="undo")
    with pytest.raises(Exception, match="nothing to undo"):
        await app.execute("ord", {"id": "O1"}, op="undo")


async def test_redo_past_the_top_fails(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    with pytest.raises(Exception, match="nothing to redo"):
        await app.execute("ord", {"id": "O1"}, op="redo")


async def test_undo_of_an_unknown_row_fails(app):
    with pytest.raises(Exception, match="nothing to undo"):
        await app.execute("ord", {"id": "nope"}, op="undo")


async def test_undo_writes_no_new_version(app):
    await _three_versions(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    await app.execute("ord", {"id": "O1"}, op="redo")
    # Moving the cursor is not itself a change worth recording.
    assert await _state(app) == (3, [1, 2, 3])


async def test_undo_and_redo_reach_subscribers(app):
    events = []

    async def on_change(event):
        events.append((event.op, event.row.get(VERSION_COLUMN)))

    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    unsub = await app.subscribe(["orders"], on_change)
    await app.execute("ord", {"id": "O1"}, op="undo")   # row removed
    await app.execute("ord", {"id": "O1"}, op="redo")   # row rebuilt
    import asyncio
    await asyncio.sleep(0.05)
    unsub()

    # The emitted op reflects what actually happened, not the op name.
    assert events == [("delete", 1), ("insert", 1)]


# ---------------------------------------------------------------------------
# Undo/redo plans: the pre-image read
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("plan_for", [undo_plan, redo_plan])
def test_plans_read_the_pre_image_by_key(plan_for):
    plan = plan_for("orders", VERSIONED_CONFIG["tables"]["orders"])
    assert plan.current_sql == "SELECT * FROM orders WHERE id = ?"
    assert plan.current_params == ("id",)


@pytest.mark.parametrize("plan_for", [undo_plan, redo_plan])
def test_plans_read_the_pre_image_by_composite_key(plan_for):
    plan = plan_for("positions", COMPOSITE_CONFIG["tables"]["positions"])
    assert plan.current_sql == (
        "SELECT * FROM positions WHERE account = ? AND symbol = ?"
    )
    assert plan.current_params == ("account", "symbol")


# ---------------------------------------------------------------------------
# Undo/redo hook: cause and the before/after shapes
# ---------------------------------------------------------------------------


async def _collect(app, tables=("orders",)):
    """Subscribe and return (events list, stop function)."""
    import asyncio

    events = []

    async def on_change(event):
        events.append(event)

    unsub = await app.subscribe(list(tables), on_change)

    async def stop():
        await asyncio.sleep(0.05)
        unsub()
        return events

    return events, stop


async def test_change_event_names_undo_as_the_cause(app):
    await _three_versions(app)
    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    (event,) = await stop()

    # op is the shape of the change; cause is why it happened.
    assert (event.op, event.cause) == ("update", "undo")


async def test_change_event_carries_both_shapes_across_an_undo(app):
    await _three_versions(app)
    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    (event,) = await stop()

    # The pair is what an application diffs to deduce the dependent action.
    assert (event.old["qty"], event.old[VERSION_COLUMN]) == (30, 3)
    assert (event.new["qty"], event.new[VERSION_COLUMN]) == (20, 2)
    assert event.new is event.row


async def test_change_event_carries_both_shapes_across_a_redo(app):
    await _three_versions(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1"}, op="redo")
    (event,) = await stop()

    assert event.cause == "redo"
    assert (event.old["qty"], event.old[VERSION_COLUMN]) == (20, 2)
    assert (event.new["qty"], event.new[VERSION_COLUMN]) == (30, 3)


async def test_undoing_an_insert_reports_no_new_shape(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    (event,) = await stop()

    assert (event.op, event.cause) == ("delete", "undo")
    assert event.old["qty"] == 10
    assert event.new is None          # the row is gone
    assert event.row["qty"] == 10     # ...but a delete still names what went


async def test_redoing_a_rebuilt_row_reports_no_old_shape(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1"}, op="undo")
    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1"}, op="redo")
    (event,) = await stop()

    assert (event.op, event.cause) == ("insert", "redo")
    assert event.old is None          # nothing was there to begin with
    assert event.new["qty"] == 10


async def test_ordinary_writes_carry_no_cause_or_old_shape(app):
    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 20}, op="amend")
    await app.execute("ord", {"id": "O1"}, op="kill")
    events = await stop()

    assert [e.op for e in events] == ["insert", "update", "delete"]
    assert all(e.cause is None and e.old is None for e in events)
    # `new` still works: it is the row for anything but a delete.
    assert [e.new is None for e in events] == [False, False, True]


async def test_envelope_carries_cause_and_old_only_for_cursor_moves(app):
    from mkio._json import loads

    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 20}, op="amend")
    await app.execute("ord", {"id": "O1"}, op="undo")
    plain, _, moved = await stop()

    assert "cause" not in loads(plain.raw_bytes)
    assert "old" not in loads(plain.raw_bytes)
    envelope = loads(moved.raw_bytes)
    assert envelope["cause"] == "undo"
    assert envelope["old"]["qty"] == 20
    assert envelope["row"]["qty"] == 10


async def test_undo_of_a_multi_row_update_reports_each_rows_own_shapes(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O2", "sym": "A", "qty": 2}, op="new")
    await app.execute("ord", {"sym": "A", "qty": 9}, op="bump")   # both rows

    _, stop = await _collect(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    await app.execute("ord", {"id": "O2"}, op="undo")
    e1, e2 = await stop()

    assert (e1.old["qty"], e1.new["qty"]) == (9, 1)
    assert (e2.old["qty"], e2.new["qty"]) == (9, 2)


# ---------------------------------------------------------------------------
# Undo/redo hook: MkioApp.on_undo_redo
# ---------------------------------------------------------------------------


async def test_on_undo_redo_fires_with_both_shapes():
    seen = []
    a = create_app(_config())

    async def hook(event):
        seen.append((event.table, event.cause, event.old, event.new))

    a.on_undo_redo(hook)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
        await a.execute("ord", {"id": "O1", "qty": 20}, op="amend")
        await a.execute("ord", {"id": "O1"}, op="undo")
        await a.execute("ord", {"id": "O1"}, op="redo")
        import asyncio
        await asyncio.sleep(0.05)
    finally:
        await a.stop()

    assert [(t, c) for t, c, _, _ in seen] == [
        ("orders", "undo"), ("orders", "redo")]
    (_, _, undo_old, undo_new), (_, _, redo_old, redo_new) = seen
    assert (undo_old["qty"], undo_new["qty"]) == (20, 10)
    assert (redo_old["qty"], redo_new["qty"]) == (10, 20)


async def test_on_undo_redo_ignores_ordinary_writes():
    seen = []
    a = create_app(_config())

    async def hook(event):
        seen.append(event)

    a.on_undo_redo(hook)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
        await a.execute("ord", {"id": "O1", "qty": 20}, op="amend")
        await a.execute("ord", {"id": "O1"}, op="kill")
        await a.execute("pl", {"id": "P1", "v": "x"})
        import asyncio
        await asyncio.sleep(0.05)
    finally:
        await a.stop()

    assert seen == []


async def test_on_undo_redo_can_make_a_dependent_write():
    """The canonical use: undo an order, unwind what the order caused."""
    a = create_app(_config())

    async def hook(event):
        if event.new is None:
            await a.execute("pl", {"id": event.old["id"], "v": "cancelled"})
        else:
            await a.execute("pl", {"id": event.new["id"], "v": "reinstated"})

    a.on_undo_redo(hook)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
        await a.execute("ord", {"id": "O1"}, op="undo")   # row withdrawn
        import asyncio
        await asyncio.sleep(0.05)
        rows = await a.query("SELECT id, v FROM plain")
    finally:
        await a.stop()

    assert rows == [{"id": "O1", "v": "cancelled"}]


async def test_a_failing_undo_redo_hook_does_not_silence_the_rest(caplog):
    seen = []
    a = create_app(_config())

    async def boom(event):
        raise RuntimeError("dependent action failed")

    async def after(event):
        seen.append(event.cause)

    a.on_undo_redo(boom)
    a.on_undo_redo(after)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
        await a.execute("ord", {"id": "O1"}, op="undo")
        await a.execute("ord", {"id": "O1"}, op="redo")
        import asyncio
        await asyncio.sleep(0.05)
    finally:
        await a.stop()

    # The listener survives a raising hook, and later hooks still run.
    assert seen == ["undo", "redo"]


async def test_on_undo_redo_is_refused_once_running(app):
    async def hook(event):
        pass

    with pytest.raises(RuntimeError, match="after the server has started"):
        app.on_undo_redo(hook)


async def test_old_shape_carries_the_ref_of_the_write_it_undoes(app):
    """`old` is a full row, so it dates the state being stepped away from."""
    first = await app.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
    second = await app.execute("ord", {"id": "O1", "qty": 20}, op="amend")
    _, stop = await _collect(app)
    moved = await app.execute("ord", {"id": "O1"}, op="undo")
    (event,) = await stop()

    assert event.old["_mkio_ref"] == second["ref"]
    assert event.new["_mkio_ref"] == moved["ref"]   # the move restamps the row
    assert event.ref == moved["ref"]
    assert first["ref"] != second["ref"]


async def test_each_step_of_a_deep_undo_reports_adjacent_versions(app):
    await _three_versions(app)
    _, stop = await _collect(app)
    for _ in range(3):
        await app.execute("ord", {"id": "O1"}, op="undo")
    events = await stop()

    steps = [
        (e.cause, e.op,
         e.old[VERSION_COLUMN],
         e.new[VERSION_COLUMN] if e.new else None)
        for e in events
    ]
    assert steps == [
        ("undo", "update", 3, 2),
        ("undo", "update", 2, 1),
        ("undo", "delete", 1, None),   # past the bottom, the row goes
    ]


async def test_a_failed_undo_emits_nothing(app):
    """A cursor move with nowhere to go rolls back, so no hook can fire."""
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O1"}, op="undo")
    _, stop = await _collect(app)
    with pytest.raises(Exception, match="nothing to undo"):
        await app.execute("ord", {"id": "O1"}, op="undo")
    assert await stop() == []


async def test_undo_composed_with_another_op_tags_only_the_cursor_move():
    """Undo composes with ordinary ops; cause marks just the one that moved."""
    cfg = _config()
    cfg["services"]["ord"]["ops"]["undo_logged"] = [
        {"table": "orders", "op_type": "undo", "key": ["id"]},
        {"table": "plain", "op_type": "insert", "fields": ["id", "v"]},
    ]
    a = create_app(cfg)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 10}, op="new")
        await a.execute("ord", {"id": "O1", "qty": 20}, op="amend")
        _, stop = await _collect(a, tables=("orders", "plain"))
        await a.execute("ord", {"id": "O1", "v": "undone"}, op="undo_logged")
        moved, logged = await stop()
    finally:
        await a.stop()

    assert (moved.table, moved.cause) == ("orders", "undo")
    assert (moved.old["qty"], moved.new["qty"]) == (20, 10)
    assert (logged.table, logged.cause, logged.old) == ("plain", None, None)


async def test_composite_key_undo_reports_both_shapes(composite_app):
    app = composite_app
    await app.execute("pos", {"account": "A1", "symbol": "X", "qty": 5}, op="open")
    await app.execute("pos", {"account": "A1", "symbol": "X", "qty": 8}, op="adjust")
    _, stop = await _collect(app, tables=("positions",))
    await app.execute("pos", {"account": "A1", "symbol": "X"}, op="undo")
    (event,) = await stop()

    assert event.cause == "undo"
    assert (event.old["qty"], event.new["qty"]) == (8, 5)
    assert (event.new["account"], event.new["symbol"]) == ("A1", "X")


async def test_on_undo_redo_spans_every_versioned_table():
    """One listener covers them all; the event says which table moved."""
    cfg = _config()
    cfg["tables"]["plain"]["versioned"] = True
    cfg["services"]["pl"]["ops"] = {
        "add": [{"table": "plain", "op_type": "insert", "fields": ["id", "v"]}],
        "undo": [{"table": "plain", "op_type": "undo", "key": ["id"]}],
    }
    seen = []
    a = create_app(cfg)

    async def hook(event):
        seen.append((event.table, event.cause))

    a.on_undo_redo(hook)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
        await a.execute("pl", {"id": "P1", "v": "x"}, op="add")
        await a.execute("ord", {"id": "O1"}, op="undo")
        await a.execute("pl", {"id": "P1"}, op="undo")
        import asyncio
        await asyncio.sleep(0.05)
    finally:
        await a.stop()

    assert seen == [("orders", "undo"), ("plain", "undo")]


async def test_on_undo_redo_hooks_run_in_registration_order():
    order = []
    a = create_app(_config())

    for tag in ("first", "second", "third"):
        async def hook(event, tag=tag):
            order.append(tag)
        a.on_undo_redo(hook)

    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
        await a.execute("ord", {"id": "O1"}, op="undo")
        import asyncio
        await asyncio.sleep(0.05)
    finally:
        await a.stop()

    assert order == ["first", "second", "third"]


async def test_a_server_with_hooks_but_no_versioned_tables_still_starts():
    cfg = _config()
    del cfg["tables"]["orders"]["versioned"]
    for name in ("undo", "redo"):
        del cfg["services"]["ord"]["ops"][name]
    seen = []

    a = create_app(cfg)

    async def hook(event):
        seen.append(event)

    a.on_undo_redo(hook)
    await a.start()
    try:
        await a.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
        import asyncio
        await asyncio.sleep(0.05)
    finally:
        await a.stop()

    assert seen == []


async def test_no_listener_is_started_when_no_hook_is_registered():
    """The bus subscription is not paid for by servers that never asked."""
    a = create_app(_config())
    await a.start()
    try:
        assert a._aiohttp_app.get("_undo_redo_listener") is None
    finally:
        await a.stop()


async def test_the_listener_is_stopped_with_the_server():
    a = create_app(_config())

    async def hook(event):
        pass

    a.on_undo_redo(hook)
    await a.start()
    task = a._aiohttp_app["_undo_redo_listener"]
    assert not task.done()
    await a.stop()
    assert task.cancelled() or task.done()


# ---------------------------------------------------------------------------
# Redo-branch truncation
# ---------------------------------------------------------------------------


async def test_editing_after_undo_drops_the_redo_branch(app):
    await _three_versions(app)
    await app.execute("ord", {"id": "O1"}, op="undo")     # cursor at 2
    await app.execute("ord", {"id": "O1", "qty": 99}, op="amend")

    live = (await app.query("SELECT * FROM orders"))[0]
    assert (live[VERSION_COLUMN], live["qty"]) == (3, 99)
    # The abandoned v3 is gone, replaced by the new one.
    versions = await app.history("orders", pk={"id": "O1"})
    assert [(v[VERSION_COLUMN], v["qty"]) for v in versions] == [
        (1, 10), (2, 20), (3, 99)]


async def test_editing_deep_below_the_top_drops_everything_above(app):
    await _three_versions(app)
    await app.execute("ord", {"id": "O1"}, op="undo")
    await app.execute("ord", {"id": "O1"}, op="undo")      # cursor at 1
    await app.execute("ord", {"id": "O1", "qty": 77}, op="amend")

    assert await _state(app) == (2, [1, 2])


async def test_inserting_over_a_fully_undone_row_wipes_its_chain(app):
    await _three_versions(app)
    for _ in range(3):
        await app.execute("ord", {"id": "O1"}, op="undo")
    assert await _state(app) == (None, [1, 2, 3])

    await app.execute("ord", {"id": "O1", "sym": "Z", "qty": 5}, op="new")
    # A fresh insert is version 1, so it discards versions 1 and above.
    assert await _state(app) == (1, [1])
    versions = await app.history("orders", pk={"id": "O1"})
    assert versions[0]["sym"] == "Z"


async def test_versions_stay_contiguous_through_any_sequence(app):
    await _three_versions(app)
    for op in ("undo", "undo", "redo", "undo"):
        await app.execute("ord", {"id": "O1"}, op=op)
    await app.execute("ord", {"id": "O1", "qty": 42}, op="amend")
    await app.execute("ord", {"id": "O1", "qty": 43}, op="amend")
    await app.execute("ord", {"id": "O1"}, op="undo")

    cursor, versions = await _state(app)
    assert versions == list(range(1, len(versions) + 1))   # 1..N, no gaps
    assert 1 <= cursor <= len(versions)


async def test_truncation_is_scoped_to_one_row(app):
    await _three_versions(app)
    await app.execute("ord", {"id": "O2", "sym": "B", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O2", "qty": 2}, op="amend")
    await app.execute("ord", {"id": "O1"}, op="undo")
    await app.execute("ord", {"id": "O1", "qty": 99}, op="amend")

    other = await app.history("orders", pk={"id": "O2"})
    assert [v[VERSION_COLUMN] for v in other] == [1, 2]


# ---------------------------------------------------------------------------
# Redo-stack garbage collection
# ---------------------------------------------------------------------------


def _gc_config(tmp_path) -> Path:
    """A config file with a single versioned table."""
    cfg_path = tmp_path / "server.toml"
    cfg_path.write_text(f"""
db_path = "{tmp_path / 'g.db'}"
auto_migrate = "safe"

[tables.orders]
columns = {{ id = "TEXT PRIMARY KEY", sym = "TEXT NOT NULL", qty = "INTEGER" }}
versioned = true

[services.ord]
protocol = "transaction"

[services.ord.ops]
new = [{{ table = "orders", op_type = "insert", fields = ["id", "sym", "qty"] }}]
amend = [{{ table = "orders", op_type = "update", key = ["id"], fields = ["qty"] }}]
undo = [{{ table = "orders", op_type = "undo", key = ["id"] }}]
""")
    return cfg_path


async def _gc_fixture(tmp_path) -> Path:
    """A DB with one dangling redo branch and one fully undone row."""
    cfg_path = _gc_config(tmp_path)
    a = create_app(str(cfg_path))
    await a.start()
    await a.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    await a.execute("ord", {"id": "O1", "qty": 2}, op="amend")
    await a.execute("ord", {"id": "O1"}, op="undo")       # v2 dangles above v1
    await a.execute("ord", {"id": "O2", "sym": "B", "qty": 9}, op="new")
    await a.execute("ord", {"id": "O2"}, op="undo")       # fully undone
    await a.execute("ord", {"id": "O3", "sym": "C", "qty": 3}, op="new")
    await a.stop()
    return cfg_path


async def test_gc_drops_orphans_and_dangling_redo(tmp_path):
    cfg = load_config(str(await _gc_fixture(tmp_path)))
    conn = sqlite3.connect(cfg["db_path"])
    from mkio.history import effective_tables

    dropped = collect_redo_garbage(conn, effective_tables(cfg))
    assert dropped == {"orders__history": (1, 1)}

    kept = sorted(conn.execute(
        f"SELECT id, {VERSION_COLUMN} FROM orders__history"))
    # O1 keeps only the version it sits on; O2's orphaned chain is gone;
    # O3 is untouched.
    assert kept == [("O1", 1), ("O3", 1)]
    conn.close()


async def test_gc_is_a_no_op_when_nothing_dangles(tmp_path):
    cfg_path = _gc_config(tmp_path)
    a = create_app(str(cfg_path))
    await a.start()
    await a.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    await a.execute("ord", {"id": "O1", "qty": 2}, op="amend")
    await a.stop()

    cfg = load_config(str(cfg_path))
    conn = sqlite3.connect(cfg["db_path"])
    from mkio.history import effective_tables
    assert collect_redo_garbage(conn, effective_tables(cfg)) == {}
    assert conn.execute("SELECT COUNT(*) FROM orders__history").fetchone()[0] == 2
    conn.close()


async def test_dbupdate_discards_redo_and_keep_redo_does_not(tmp_path):
    cfg_path = await _gc_fixture(tmp_path)
    db_path = load_config(str(cfg_path))["db_path"]

    def count():
        conn = sqlite3.connect(db_path)
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM orders__history").fetchone()[0]
        finally:
            conn.close()

    before = count()
    kept = subprocess.run(
        [sys.executable, "-m", "mkio", "dbupdate", str(cfg_path), "--keep-redo"],
        capture_output=True, text=True)
    assert kept.returncode == 0, kept.stdout + kept.stderr
    assert count() == before

    run = subprocess.run(
        [sys.executable, "-m", "mkio", "dbupdate", str(cfg_path)],
        capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert "Discarded redo history" in run.stdout
    assert "no longer redo-able" in run.stdout
    assert count() == 2


# ---------------------------------------------------------------------------
# Composite primary keys
# ---------------------------------------------------------------------------


COMPOSITE_CONFIG = {
    "port": 0,
    "db_path": ":memory:",
    "tables": {
        "positions": {
            "columns": {
                "account": "TEXT NOT NULL",
                "symbol": "TEXT NOT NULL",
                "qty": "INTEGER",
            },
            "primary_key": ["account", "symbol"],
            "versioned": True,
        },
    },
    "services": {
        "pos": {
            "protocol": "transaction",
            "ops": {
                "open": [{"table": "positions", "op_type": "insert",
                          "fields": ["account", "symbol", "qty"]}],
                "adjust": [{"table": "positions", "op_type": "update",
                            "key": ["account", "symbol"], "fields": ["qty"]}],
                "close": [{"table": "positions", "op_type": "delete",
                           "key": ["account", "symbol"]}],
                "undo": [{"table": "positions", "op_type": "undo",
                          "key": ["account", "symbol"]}],
                "redo": [{"table": "positions", "op_type": "redo",
                          "key": ["account", "symbol"]}],
            },
        },
    },
}


@pytest.fixture
async def composite_app():
    a = create_app(dict(COMPOSITE_CONFIG))
    await a.start()
    yield a
    await a.stop()


async def test_composite_key_versions_each_row_independently(composite_app):
    app = composite_app
    await app.execute("pos", {"account": "A1", "symbol": "AAPL", "qty": 10}, op="open")
    await app.execute("pos", {"account": "A1", "symbol": "MSFT", "qty": 20}, op="open")
    await app.execute("pos", {"account": "A2", "symbol": "AAPL", "qty": 30}, op="open")
    await app.execute("pos", {"account": "A1", "symbol": "AAPL", "qty": 11}, op="adjust")

    rows = await app.query(
        "SELECT account, symbol, qty, _mkio_version FROM positions "
        "ORDER BY account, symbol")
    assert [(r["account"], r["symbol"], r["_mkio_version"]) for r in rows] == [
        ("A1", "AAPL", 2), ("A1", "MSFT", 1), ("A2", "AAPL", 1)]


async def test_composite_key_undo_and_redo(composite_app):
    app = composite_app
    key = {"account": "A1", "symbol": "AAPL"}
    await app.execute("pos", {**key, "qty": 10}, op="open")
    await app.execute("pos", {**key, "qty": 20}, op="adjust")
    await app.execute("pos", {**key, "qty": 30}, op="adjust")
    # A decoy sharing half the key must not move.
    await app.execute("pos", {"account": "A1", "symbol": "MSFT", "qty": 99}, op="open")

    await app.execute("pos", key, op="undo")
    await app.execute("pos", key, op="undo")
    live = await app.query(
        "SELECT symbol, qty, _mkio_version FROM positions ORDER BY symbol")
    assert [(r["symbol"], r["qty"], r["_mkio_version"]) for r in live] == [
        ("AAPL", 10, 1), ("MSFT", 99, 1)]

    await app.execute("pos", key, op="redo")
    live = await app.query(
        "SELECT qty, _mkio_version FROM positions WHERE symbol = 'AAPL'")
    assert (live[0]["qty"], live[0][VERSION_COLUMN]) == (20, 2)


async def test_composite_key_undo_to_nothing_and_back(composite_app):
    app = composite_app
    key = {"account": "A1", "symbol": "AAPL"}
    await app.execute("pos", {**key, "qty": 10}, op="open")
    await app.execute("pos", key, op="undo")
    assert await app.query("SELECT * FROM positions") == []

    await app.execute("pos", key, op="redo")
    row = (await app.query("SELECT * FROM positions"))[0]
    assert (row["account"], row["symbol"], row["qty"], row[VERSION_COLUMN]) == (
        "A1", "AAPL", 10, 1)


async def test_composite_key_truncation_is_scoped_to_one_row(composite_app):
    app = composite_app
    key = {"account": "A1", "symbol": "AAPL"}
    await app.execute("pos", {**key, "qty": 10}, op="open")
    await app.execute("pos", {**key, "qty": 20}, op="adjust")
    await app.execute("pos", {"account": "A2", "symbol": "AAPL", "qty": 1}, op="open")
    await app.execute("pos", {"account": "A2", "symbol": "AAPL", "qty": 2}, op="adjust")

    await app.execute("pos", key, op="undo")
    await app.execute("pos", {**key, "qty": 77}, op="adjust")

    kept = await app.query(
        "SELECT account, symbol, _mkio_version, qty FROM positions__history "
        "ORDER BY account, symbol, _mkio_version")
    assert [(r["account"], r[VERSION_COLUMN], r["qty"]) for r in kept] == [
        ("A1", 1, 10), ("A1", 2, 77), ("A2", 1, 1), ("A2", 2, 2)]


async def test_composite_key_delete_truncates_only_that_chain(composite_app):
    app = composite_app
    await app.execute("pos", {"account": "A1", "symbol": "AAPL", "qty": 1}, op="open")
    await app.execute("pos", {"account": "A2", "symbol": "AAPL", "qty": 2}, op="open")
    await app.execute("pos", {"account": "A1", "symbol": "AAPL"}, op="close")

    kept = await app.query("SELECT account FROM positions__history")
    assert [r["account"] for r in kept] == ["A2"]


# ---------------------------------------------------------------------------
# Further capture edge cases
# ---------------------------------------------------------------------------


async def test_multi_row_delete_truncates_every_chain(app):
    await app.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O2", "sym": "AAPL", "qty": 2}, op="new")
    await app.execute("ord", {"id": "O3", "sym": "MSFT", "qty": 3}, op="new")

    cfg = _config()
    cfg["services"]["ord"]["ops"]["purge"] = [
        {"table": "orders", "op_type": "delete", "key": ["sym"]}]
    a = create_app(cfg)
    await a.start()
    try:
        for oid, sym in (("O1", "AAPL"), ("O2", "AAPL"), ("O3", "MSFT")):
            await a.execute("ord", {"id": oid, "sym": sym, "qty": 1}, op="new")
        await a.execute("ord", {"sym": "AAPL"}, op="purge")
        assert [r["id"] for r in await a.query("SELECT id FROM orders")] == ["O3"]
        assert [r["id"] for r in await a.history("orders")] == ["O3"]
    finally:
        await a.stop()


async def test_two_writes_to_one_row_in_a_single_transaction(app):
    """Both ops run under one ref, and the counter advances once per op."""
    cfg = _config()
    cfg["services"]["ord"]["ops"]["new_then_amend"] = [
        {"table": "orders", "op_type": "insert", "fields": ["id", "sym", "qty"]},
        {"table": "orders", "op_type": "update", "key": ["id"], "fields": ["qty"]},
    ]
    a = create_app(cfg)
    await a.start()
    try:
        result = await a.execute(
            "ord", {"id": "O1", "sym": "A", "qty": 1}, op="new_then_amend")
        live = (await a.query("SELECT * FROM orders"))[0]
        assert live[VERSION_COLUMN] == 2
        versions = await a.history("orders")
        assert [(v[VERSION_COLUMN], v["_mkio_op"]) for v in versions] == [
            (1, "insert"), (2, "update")]
        # One transaction, so both versions carry its ref.
        assert {v["_mkio_ref"] for v in versions} == {result["ref"]}
    finally:
        await a.stop()


async def test_redo_after_a_real_delete_fails(app):
    await app.execute("ord", {"id": "O1", "sym": "A", "qty": 1}, op="new")
    await app.execute("ord", {"id": "O1", "qty": 2}, op="amend")
    await app.execute("ord", {"id": "O1"}, op="kill")
    # A delete takes the chain with it, so there is nothing to come back to.
    with pytest.raises(Exception, match="nothing to redo"):
        await app.execute("ord", {"id": "O1"}, op="redo")


async def test_a_baselined_row_can_be_undone_away(tmp_path):
    """Rows that predate versioning get version 1, so undo reaches them."""
    unversioned = _disk_config(tmp_path, versioned=False)
    conn = sqlite3.connect(unversioned["db_path"])
    _migrate(conn, unversioned)
    conn.execute("ALTER TABLE orders ADD COLUMN _mkio_ref TEXT DEFAULT ''")
    conn.execute("INSERT INTO orders (id, sym) VALUES ('A', 'X')")
    conn.commit()
    conn.close()

    cfg = {
        "port": 0,
        "db_path": unversioned["db_path"],
        "auto_migrate": "safe",
        "tables": {"orders": {
            "columns": {"id": "TEXT PRIMARY KEY", "sym": "TEXT"},
            "versioned": True}},
        "services": {"ord": {"protocol": "transaction", "ops": {
            "undo": [{"table": "orders", "op_type": "undo", "key": ["id"]}],
            "redo": [{"table": "orders", "op_type": "redo", "key": ["id"]}],
        }}},
    }
    a = create_app(cfg)
    await a.start()
    try:
        assert (await a.history("orders"))[0]["_mkio_op"] == "baseline"
        await a.execute("ord", {"id": "A"}, op="undo")
        assert await a.query("SELECT * FROM orders") == []
        await a.execute("ord", {"id": "A"}, op="redo")
        row = (await a.query("SELECT * FROM orders"))[0]
        assert (row["sym"], row[VERSION_COLUMN]) == ("X", 1)
    finally:
        await a.stop()


async def test_unauthenticated_info_hides_versioned_tables():
    """The versioned list names tables, so it is gated like the table list."""
    from tests.conftest import MockWebSocket

    a = create_app(AUTH_HISTORY_CONFIG)
    await a.start()
    try:
        ws = MockWebSocket()
        await a.services["_mkio"].on_message(
            ws, {"type": "request", "reqid": "r1"})
        row = ws.get_messages()[0]["row"]
        assert "versioned" not in row
        assert "tables" not in row
        # The naming convention itself is not sensitive.
        assert row["history_suffix"] == HISTORY_SUFFIX
    finally:
        await a.stop()


async def test_dbupdate_drop_history_removes_the_orphan(tmp_path):
    cfg_path = await _gc_fixture(tmp_path)
    db_path = load_config(str(cfg_path))["db_path"]
    # Stop versioning. The undo/redo ops have to go with it — they are only
    # valid against a versioned table — but the history table is retained.
    cfg_path.write_text(f"""
db_path = "{db_path}"

[tables.orders]
columns = {{ id = "TEXT PRIMARY KEY", sym = "TEXT NOT NULL", qty = "INTEGER" }}
""")

    def tables():
        conn = sqlite3.connect(db_path)
        try:
            return {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            conn.close()

    kept = subprocess.run(
        [sys.executable, "-m", "mkio", "dbupdate", str(cfg_path)],
        capture_output=True, text=True)
    assert kept.returncode == 0, kept.stdout + kept.stderr
    assert "history retained" in kept.stdout
    assert "orders__history" in tables()

    dropped = subprocess.run(
        [sys.executable, "-m", "mkio", "dbupdate", str(cfg_path), "--drop-history"],
        capture_output=True, text=True)
    assert dropped.returncode == 0, dropped.stdout + dropped.stderr
    assert "Dropped orphaned history table" in dropped.stdout
    assert "orders__history" not in tables()
    # The base table and its rows are untouched.
    assert "orders" in tables()
