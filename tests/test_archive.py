"""Row archiving: config, cutoffs, the offline and online drivers, restore,
the CLI, and the stream buffer forgetting deleted rows."""

from __future__ import annotations

import asyncio
import csv
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mkio import ChangeEvent, create_app
from mkio.archive import (
    ArchiveError,
    archive_offline,
    archive_specs,
    cutoff_value,
    midnight_today,
    parse_cutoff,
    restore_offline,
    select_specs,
)
from mkio.config import load_config
from mkio.history import VERSION_COLUMN

OLD = "2026-01-05 09:00:00"
NEW = "2026-09-01 09:00:00"


def _config(tmp_path: Path, *, versioned_orders: bool = True) -> str:
    cfg_path = tmp_path / "server.toml"
    cfg_path.write_text(f"""
name = "arctest"
host = "127.0.0.1"
port = 0
db_path = "{tmp_path / 'a.db'}"
auto_migrate = "safe"
batch_max_size = 4

[tables.orders]
columns = {{ id = "TEXT PRIMARY KEY", sym = "TEXT", qty = "INTEGER", px = "REAL", note = "TEXT DEFAULT ''", created_at = "TEXT", live = "TEXT DEFAULT ''" }}
versioned = {"true" if versioned_orders else "false"}
{"unversioned = ['live']" if versioned_orders else ""}
archive = {{ cutoff = "created_at", group = "data" }}

[tables.messages]
columns = {{ id = "INTEGER PRIMARY KEY AUTOINCREMENT", body = "TEXT", ts = "TEXT" }}
archive = {{ cutoff = "ts", format = "%Y-%m-%d %H:%M:%S" }}

[tables.sessions]
columns = {{ session_id = "TEXT PRIMARY KEY", status = "TEXT DEFAULT 'DOWN'" }}
archive = {{ group = "config", with = ["session_state"] }}

[tables.session_state]
columns = {{ session_id = "TEXT PRIMARY KEY", seq = "INTEGER DEFAULT 1" }}

[tables.settings]
columns = {{ key = "TEXT PRIMARY KEY", value = "TEXT DEFAULT ''" }}
archive = {{ group = "config" }}

[tables.plain]
columns = {{ id = "TEXT PRIMARY KEY" }}

[services.ord]
protocol = "transaction"
[services.ord.ops]
new = [{{ table = "orders", op_type = "insert", fields = ["id", "sym", "qty", "px", "note", "created_at"] }}]
amend = [{{ table = "orders", op_type = "update", key = ["id"], fields = ["qty"] }}]
kill = [{{ table = "orders", op_type = "delete", key = ["id"] }}]

[services.msg]
protocol = "transaction"
ops = [{{ table = "messages", op_type = "insert", fields = ["body", "ts"] }}]

[services.sess]
protocol = "transaction"
[services.sess.ops]
add = [
  {{ table = "sessions", op_type = "insert", fields = ["session_id", "status"] }},
  {{ table = "session_state", op_type = "insert", fields = ["session_id", "seq"] }},
]

[services.setting]
protocol = "transaction"
ops = [{{ table = "settings", op_type = "insert", fields = ["key", "value"] }}]

[services.orders_q]
protocol = "query"
primary_table = "orders"

[services.msg_stream]
protocol = "stream"
primary_table = "messages"
buffer_size = 100
""")
    return str(cfg_path)


async def _populate(cfg_path: str):
    """Two old orders (one amended, one with a NULL price), one new; three
    messages; two sessions with state; one setting."""
    a = create_app(cfg_path)
    await a.start()
    await a.execute("ord", {"id": "O1", "sym": "AAPL", "qty": 10, "px": 1.5, "note": "a,b \"q\"", "created_at": OLD}, op="new", user="alice")
    await a.execute("ord", {"id": "O1", "qty": 25}, op="amend", user="alice")
    await a.execute("ord", {"id": "O2", "sym": "MSFT", "qty": 5, "px": None, "note": "", "created_at": OLD}, op="new")
    await a.execute("ord", {"id": "O3", "sym": "TSLA", "qty": 1, "px": 2.0, "note": "", "created_at": NEW}, op="new")
    await a.execute("msg", {"body": "old\x01one", "ts": OLD})
    await a.execute("msg", {"body": "old two", "ts": OLD})
    await a.execute("msg", {"body": "new", "ts": NEW})
    await a.execute("sess", {"session_id": "S1", "status": "DOWN", "seq": 7}, op="add")
    await a.execute("sess", {"session_id": "S2", "status": "ACTIVE", "seq": 3}, op="add")
    await a.execute("setting", {"key": "k", "value": "v"})
    await a.stop()


def _rows(db: Path, sql: str) -> list[dict]:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql)]
    finally:
        conn.close()


def _snapshot(db: Path) -> dict[str, list[dict]]:
    return {
        t: sorted(_rows(db, f"SELECT * FROM {t}"), key=lambda r: json.dumps(r, sort_keys=True, default=str))
        for t in ("orders", "orders__history", "messages", "sessions", "session_state", "settings")
    }


CUTOFF = datetime(2026, 6, 1, tzinfo=timezone.utc)


# ── Config ────────────────────────────────────────────────────────────


def test_specs_come_from_config(tmp_path):
    specs = archive_specs(load_config(_config(tmp_path)))
    assert list(specs) == ["orders", "messages", "sessions", "settings"]
    orders = specs["orders"]
    assert orders.cutoff == "created_at" and orders.versioned and orders.group == "data"
    assert orders.columns["px"] == "REAL" and orders.columns[VERSION_COLUMN] == "INTEGER"
    assert specs["sessions"].companions == ("session_state",)
    assert specs["sessions"].cutoff is None


def test_select_specs_defaults_to_the_data_group(tmp_path):
    cfg = load_config(_config(tmp_path))
    assert [s.table for s in select_specs(cfg)] == ["orders", "messages"]
    assert [s.table for s in select_specs(cfg, group="config")] == ["sessions", "settings"]
    assert [s.table for s in select_specs(cfg, group="all")] == ["orders", "messages", "sessions", "settings"]
    assert [s.table for s in select_specs(cfg, tables=["settings", "orders"])] == ["settings", "orders"]
    with pytest.raises(ArchiveError, match="not archivable"):
        select_specs(cfg, tables=["plain"])
    with pytest.raises(ArchiveError, match="Groups: config, data"):
        select_specs(cfg, group="nope")


@pytest.mark.parametrize("archive, message", [
    ('{ cutoff = "nope" }', "not a column"),
    ('{ bogus = 1 }', "unknown archive option"),
    ('{ group = "all" }', "other than 'all'"),
    ('{ format = "%Y" }', "needs a cutoff column"),
    ('{ with = ["missing"] }', "not a declared table"),
    ('{ with = ["nokey"] }', "lacks the key column"),
    ('{ with = ["orders"] }', "is versioned"),
])
def test_archive_key_is_validated(tmp_path, archive, message):
    cfg = {
        "db_path": ":memory:",
        "tables": {
            "orders": {"columns": {"id": "TEXT PRIMARY KEY"}, "versioned": True},
            "nokey": {"columns": {"other": "TEXT PRIMARY KEY"}},
            "t": {"columns": {"id": "TEXT PRIMARY KEY", "ts": "TEXT"}},
        },
    }
    import tomllib
    cfg["tables"]["t"]["archive"] = tomllib.loads(f"a = {archive}")["a"]
    with pytest.raises(ValueError, match=message):
        load_config(cfg)


def test_archivable_table_needs_a_primary_key():
    cfg = {"db_path": ":memory:", "tables": {"t": {"columns": {"x": "TEXT"}, "archive": {}}}}
    with pytest.raises(ValueError, match="need a primary key"):
        load_config(cfg)


# ── Cutoffs ───────────────────────────────────────────────────────────


def test_parse_cutoff_forms():
    now = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
    assert parse_cutoff("2d", now=now) == now - timedelta(days=2)
    assert parse_cutoff("3h", now=now) == now - timedelta(hours=3)
    assert parse_cutoff("2026-09-11T17:30:00Z") == datetime(2026, 9, 11, 17, 30, tzinfo=timezone.utc)
    local = parse_cutoff("2026-09-11 17:30")
    assert local.tzinfo == timezone.utc
    assert local == datetime(2026, 9, 11, 17, 30).astimezone(timezone.utc)
    with pytest.raises(ArchiveError, match="Cannot parse cutoff"):
        parse_cutoff("yesterday")


def test_midnight_today_is_local_midnight_in_utc():
    m = midnight_today()
    local = m.astimezone()
    assert (local.hour, local.minute, local.second) == (0, 0, 0)
    assert m.tzinfo == timezone.utc


def test_cutoff_value_renders_per_table(tmp_path):
    specs = archive_specs(load_config(_config(tmp_path)))
    assert cutoff_value(specs["orders"], CUTOFF, None) == "2026-06-01 00:00:00"
    assert cutoff_value(specs["orders"], None, "2026-07") == "2026-07"
    assert cutoff_value(specs["sessions"], None, None) is None
    with pytest.raises(ArchiveError, match="a cutoff is required"):
        cutoff_value(specs["orders"], None, None)


# ── Offline round trip ────────────────────────────────────────────────


async def test_offline_archive_and_restore_round_trip(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    db = tmp_path / "a.db"
    before = _snapshot(db)
    cfg = load_config(cfg_path)

    result = archive_offline(cfg, group="all", cutoff=CUTOFF, out_dir=tmp_path / "out", mkio_version="t")
    run_dir = Path(result["dir"])
    assert run_dir.parent == tmp_path / "out" and run_dir.name.startswith("arctest_")
    assert result["tables"]["orders"] == {
        "rows": 2, "history": 3, "companions": {}, "cutoff_column": "created_at",
        "cutoff_value": "2026-06-01 00:00:00",
    }
    assert result["tables"]["messages"]["rows"] == 2
    assert result["tables"]["sessions"] == {
        "rows": 2, "history": None, "companions": {"session_state": 2},
        "cutoff_column": None, "cutoff_value": None,
    }

    # Files: every column, the chains, the companions, and a manifest.
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["mode"] == "offline" and manifest["app"] == "arctest"
    assert manifest["cutoff"]["utc"] == "2026-06-01T00:00:00+00:00"
    orders_entry = manifest["tables"]["orders"]
    assert orders_entry["columns"]["px"] == "REAL"
    assert orders_entry["history"]["file"] == "orders__history.csv"
    with open(run_dir / "orders.csv", newline="") as f:
        archived = list(csv.DictReader(f))
    assert [r["id"] for r in archived] == ["O1", "O2"]
    assert archived[0][VERSION_COLUMN] == "2" and archived[0]["_mkio_ref"]
    assert archived[0]["note"] == 'a,b "q"'
    assert archived[1]["px"] == "\\N" and archived[1]["note"] == ""  # NULL vs empty
    with open(run_dir / "orders__history.csv", newline="") as f:
        hist = list(csv.DictReader(f))
    assert [(r["id"], r[VERSION_COLUMN]) for r in hist] == [("O1", "1"), ("O1", "2"), ("O2", "1")]
    assert "live" not in hist[0]  # unversioned column is not in the chain
    with open(run_dir / "messages.csv", newline="") as f:
        msgs = list(csv.DictReader(f))
    assert msgs[0]["body"] == "old\x01one"
    with open(run_dir / "session_state.csv", newline="") as f:
        assert [r["seq"] for r in csv.DictReader(f)] == ["7", "3"]

    # Deleted: the archived rows, their chains and companions; nothing else.
    after = _snapshot(db)
    assert [r["id"] for r in after["orders"]] == ["O3"]
    assert [r["id"] for r in after["orders__history"]] == ["O3"]
    assert [r["body"] for r in after["messages"]] == ["new"]
    assert after["sessions"] == [] and after["session_state"] == [] and after["settings"] == []

    restored = restore_offline(cfg, run_dir)
    assert restored["tables"]["orders"] == {"rows": 2, "replaced": 0, "history": 3, "companions": {}}
    assert restored["tables"]["sessions"]["companions"] == {"session_state": 2}
    assert _snapshot(db) == before


async def test_offline_dry_run_writes_and_deletes_nothing(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    db = tmp_path / "a.db"
    before = _snapshot(db)
    result = archive_offline(load_config(cfg_path), cutoff=CUTOFF, out_dir=tmp_path / "out", dry_run=True)
    assert result["dry_run"] and result["dir"] is None
    assert result["tables"]["orders"]["rows"] == 2
    assert not (tmp_path / "out").exists()
    assert _snapshot(db) == before


async def test_offline_archive_of_data_group_leaves_config_alone(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    db = tmp_path / "a.db"
    result = archive_offline(load_config(cfg_path), cutoff=CUTOFF, out_dir=tmp_path / "out")
    assert set(result["tables"]) == {"orders", "messages"}
    assert len(_rows(db, "SELECT * FROM sessions")) == 2
    assert not (Path(result["dir"]) / "sessions.csv").exists()


async def test_archive_requires_a_cutoff_for_cutoff_tables(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    with pytest.raises(ArchiveError, match="a cutoff is required"):
        archive_offline(load_config(cfg_path), out_dir=tmp_path / "out")
    # Whole tables need none.
    result = archive_offline(load_config(cfg_path), tables=["settings"], out_dir=tmp_path / "out")
    assert result["tables"]["settings"]["rows"] == 1


async def test_restore_refuses_a_cutoff_table_collision(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    cfg = load_config(cfg_path)
    db = tmp_path / "a.db"
    result = archive_offline(cfg, cutoff=CUTOFF, out_dir=tmp_path / "out")
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO orders (id, sym, created_at) VALUES ('O1', 'X', ?)", (NEW,))
    conn.commit()
    conn.close()
    with pytest.raises(ArchiveError, match=r"orders.*1 archived row.*\('O1',\)"):
        restore_offline(cfg, result["dir"])
    # Aborted as a whole: the messages were not put back either.
    assert [r["body"] for r in _rows(db, "SELECT body FROM messages")] == ["new"]


async def test_restore_replaces_rows_of_a_whole_table(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    cfg = load_config(cfg_path)
    db = tmp_path / "a.db"
    result = archive_offline(cfg, tables=["settings", "sessions"], out_dir=tmp_path / "out")
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO settings (key, value) VALUES ('k', 'changed')")
    conn.execute("INSERT INTO sessions (session_id, status) VALUES ('S1', 'ERROR')")
    conn.commit()
    conn.close()
    restored = restore_offline(cfg, result["dir"])
    assert restored["tables"]["settings"]["replaced"] == 1
    assert restored["tables"]["sessions"]["replaced"] == 1
    assert _rows(db, "SELECT value FROM settings")[0]["value"] == "v"
    assert _rows(db, "SELECT status FROM sessions WHERE session_id = 'S1'")[0]["status"] == "DOWN"
    assert _rows(db, "SELECT seq FROM session_state WHERE session_id = 'S1'")[0]["seq"] == 7


async def test_restore_checks_the_archive_and_the_schema(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    cfg = load_config(cfg_path)
    with pytest.raises(ArchiveError, match="Not an archive directory"):
        restore_offline(cfg, tmp_path)
    result = archive_offline(cfg, cutoff=CUTOFF, out_dir=tmp_path / "out")
    with pytest.raises(ArchiveError, match="Not in this archive: settings"):
        restore_offline(cfg, result["dir"], tables=["settings"])
    # A column the schema no longer has.
    manifest_path = Path(result["dir"]) / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    conn = sqlite3.connect(tmp_path / "a.db")
    conn.execute("ALTER TABLE messages DROP COLUMN body")
    conn.commit()
    conn.close()
    with pytest.raises(ArchiveError, match="archived column\\(s\\) body no longer exist"):
        restore_offline(cfg, result["dir"], tables=["messages"])


async def test_restore_dry_run_changes_nothing(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    cfg = load_config(cfg_path)
    db = tmp_path / "a.db"
    result = archive_offline(cfg, cutoff=CUTOFF, out_dir=tmp_path / "out")
    after = _snapshot(db)
    preview = restore_offline(cfg, result["dir"], dry_run=True)
    assert preview["dry_run"] and preview["tables"]["orders"]["rows"] == 2
    assert _snapshot(db) == after


async def test_restore_into_a_table_no_longer_versioned_skips_the_chain(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    result = archive_offline(load_config(cfg_path), cutoff=CUTOFF, out_dir=tmp_path / "out")
    plain = load_config(_config(tmp_path, versioned_orders=False))
    # Migrate the file to the unversioned shape (drops _mkio_version).
    a = create_app(plain)
    await a.start()
    await a.stop()
    restored = restore_offline(plain, result["dir"], tables=["orders"])
    assert restored["tables"]["orders"]["history"] == 0
    assert "no longer versioned" in restored["tables"]["orders"]["history_skipped"]
    assert [r["id"] for r in _rows(tmp_path / "a.db", "SELECT id FROM orders ORDER BY id")] == ["O1", "O2", "O3"]


# ── Online driver ─────────────────────────────────────────────────────


async def test_online_archive_deletes_through_the_writer(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    db = tmp_path / "a.db"
    before = _snapshot(db)
    a = create_app(cfg_path)
    await a.start()
    try:
        events: list[ChangeEvent] = []

        async def on_change(event: ChangeEvent):
            events.append(event)

        unsub = await a.subscribe(["orders", "messages", "sessions", "session_state"], on_change)
        stream = a.services["msg_stream"]
        assert len(stream._buffer) == 3

        result = await a.archive(group="all", cutoff="2026-06-01T00:00:00Z", out_dir=tmp_path / "out")
        await asyncio.sleep(0.05)
        unsub()

        assert result["tables"]["orders"]["rows"] == 2 and result["dir"]
        manifest = json.loads((Path(result["dir"]) / "manifest.json").read_text())
        assert manifest["mode"] == "online"

        # One delete event per row, carrying the row, companions first.
        deletes = [(e.table, e.op, e.row.get("id") or e.row.get("session_id")) for e in events]
        assert deletes == [
            ("orders", "delete", "O1"), ("orders", "delete", "O2"),
            ("messages", "delete", 1), ("messages", "delete", 2),
            ("session_state", "delete", "S1"), ("sessions", "delete", "S1"),
            ("session_state", "delete", "S2"), ("sessions", "delete", "S2"),
        ]
        assert all(e.row.get("_mkio_ref") for e in events)

        # The stream buffer no longer serves the archived messages.
        assert [r["body"] for _, r in stream._buffered()] == ["new"]
    finally:
        await a.stop()

    after = _snapshot(db)
    assert [r["id"] for r in after["orders"]] == ["O3"]
    assert [r["id"] for r in after["orders__history"]] == ["O3"]
    assert after["sessions"] == [] and after["session_state"] == []

    restore_offline(load_config(cfg_path), result["dir"])
    assert _snapshot(db) == before


async def test_online_archive_hooks_refuse_and_release(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    a = create_app(cfg_path)
    calls: list[tuple[str, list[str]]] = []

    async def guard(stage, selection):
        calls.append((stage, [r["session_id"] for r in selection.get("sessions", [])]))
        if stage == "before" and any(r["status"] == "ACTIVE" for r in selection.get("sessions", [])):
            raise RuntimeError("session S2 is running")

    a.on_archive(guard)
    await a.start()
    try:
        with pytest.raises(ArchiveError, match="session S2 is running"):
            await a.archive(tables=["sessions"], out_dir=tmp_path / "out")
        assert calls == [("before", ["S1", "S2"])]
        assert not (tmp_path / "out").exists()
        assert len(await a.query("SELECT * FROM sessions")) == 2

        await a.execute("sess", {"session_id": "S3", "status": "DOWN", "seq": 1}, op="add")
        conn = sqlite3.connect(tmp_path / "a.db")
        conn.execute("UPDATE sessions SET status = 'DOWN' WHERE session_id = 'S2'")
        conn.commit()
        conn.close()
        calls.clear()
        result = await a.archive(tables=["sessions"], out_dir=tmp_path / "out", dry_run=True)
        assert result["dry_run"] and calls == [("before", ["S1", "S2", "S3"])]
        calls.clear()
        result = await a.archive(tables=["sessions"], out_dir=tmp_path / "out")
        assert calls == [("before", ["S1", "S2", "S3"]), ("after", ["S1", "S2", "S3"])]
        assert await a.query("SELECT * FROM sessions") == []
    finally:
        await a.stop()


async def test_online_archive_over_the_wire(tmp_path):
    """The _mkio service runs a client's archive request."""
    from mkio.client import MkioClient

    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    a = create_app(cfg_path)
    await a.start()
    try:
        port = a._site._server.sockets[0].getsockname()[1]
        async with MkioClient(f"ws://127.0.0.1:{port}/ws", reconnect=False) as client:
            preview = await client.request("_mkio", {"archive": {"cutoff": "2026-06-01", "dry_run": True}})
            assert preview["row"]["dry_run"] and preview["row"]["tables"]["orders"]["rows"] == 2
            bad = await client.request("_mkio", {"archive": {"tables": ["plain"]}})
            assert bad["type"] == "error" and "not archivable" in bad["message"]
            done = await client.request("_mkio", {"archive": {"cutoff": "2026-06-01", "out": str(tmp_path / "out")}})
            assert done["row"]["dir"].startswith(str(tmp_path / "out"))
        assert [r["id"] for r in await a.query("SELECT id FROM orders")] == ["O3"]
    finally:
        await a.stop()


async def test_online_archive_refuses_in_memory(tmp_path):
    cfg = load_config(_config(tmp_path))
    cfg["db_path"] = ":memory:"
    a = create_app(cfg)
    await a.start()
    try:
        with pytest.raises(ArchiveError, match="in-memory"):
            await a.archive(cutoff="1d")
    finally:
        await a.stop()


# ── Stream buffer ─────────────────────────────────────────────────────


async def test_stream_forgets_deleted_rows_and_compacts(tmp_path):
    cfg_path = _config(tmp_path)
    a = create_app(cfg_path)
    await a.start()
    try:
        for i in range(30):
            await a.execute("msg", {"body": f"m{i}", "ts": OLD if i < 20 else NEW})
        stream = a.services["msg_stream"]
        assert len(stream._buffer) == 30
        await a.archive(tables=["messages"], cutoff="2026-06-01", out_dir=tmp_path / "out")
        await asyncio.sleep(0.05)
        assert [r["body"] for _, r in stream._buffered()] == [f"m{i}" for i in range(20, 30)]
        # 20 deletes against a buffer of 100 crossed the compaction threshold
        # part-way, so the buffer itself shrank and few refs are still pending.
        assert len(stream._buffer) < 30 and len(stream._deleted) < 10
    finally:
        await a.stop()


# ── CLI ───────────────────────────────────────────────────────────────


def _cli(*args, expect=0):
    result = subprocess.run(
        [sys.executable, "-m", "mkio", *args], capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == expect, result.stdout + result.stderr
    return result.stdout


async def test_cli_archive_and_restore(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    db = tmp_path / "a.db"
    before = _snapshot(db)

    out = _cli("archive", cfg_path, "--cutoff", "2026-06-01T00:00:00Z", "--out", str(tmp_path / "out"), "--dry-run")
    assert "orders: 2 rows, 3 history rows would be archived (created_at < 2026-06-01 00:00:00)" in out
    assert "messages: 2 rows would be archived" in out
    assert not (tmp_path / "out").exists()

    # Not a terminal, no --yes: refused before anything happens.
    out = _cli("archive", cfg_path, "--cutoff", "2026-06-01", "--out", str(tmp_path / "out"), expect=1)
    assert "pass --yes" in out
    assert _snapshot(db) == before

    out = _cli("archive", cfg_path, "--all", "--cutoff", "2026-06-01", "--out", str(tmp_path / "out"), "--yes")
    assert "sessions: 2 rows, 2 session_state rows to archive (whole table, cutoff ignored)" in out
    run_dir = next((tmp_path / "out").iterdir())
    assert f"Archived 7 rows to {run_dir}" in out
    assert _rows(db, "SELECT id FROM orders") == [{"id": "O3"}]

    out = _cli("restore", cfg_path, str(run_dir), "--dry-run")
    assert "orders: 2 rows, 3 history rows would be restored" in out
    assert _rows(db, "SELECT id FROM orders") == [{"id": "O3"}]
    out = _cli("restore", cfg_path, str(run_dir))
    assert "sessions: 2 rows, 2 session_state rows restored" in out
    assert _snapshot(db) == before

    out = _cli("restore", cfg_path, str(run_dir), expect=1)
    assert "already exist" in out


async def test_cli_archive_via_url(tmp_path):
    cfg_path = _config(tmp_path)
    await _populate(cfg_path)
    a = create_app(cfg_path)
    await a.start()
    try:
        port = a._site._server.sockets[0].getsockname()[1]
        loop = asyncio.get_running_loop()
        out = await loop.run_in_executor(
            None, lambda: _cli("archive", str(port), "--cutoff", "2026-06-01T00:00:00Z",
                               "--out", str(tmp_path / "out"), "--yes"),
        )
        assert "orders: 2 rows, 3 history rows to archive" in out
        assert "written by the server" in out and "Archived 4 rows to" in out
        assert [r["id"] for r in await a.query("SELECT id FROM orders")] == ["O3"]
    finally:
        await a.stop()


def test_cli_archive_rejects_conflicting_flags(tmp_path):
    cfg_path = _config(tmp_path)
    out = _cli("archive", cfg_path, "--all", "--group", "data", expect=1)
    assert "--all cannot be combined" in out
    out = _cli("archive", cfg_path, "--cutoff", "1d", "--cutoff-literal", "x", expect=1)
    assert "either --cutoff or --cutoff-literal" in out
    out = _cli("archive", cfg_path, "--cutoff", "someday", "--dry-run", expect=1)
    assert "Cannot parse cutoff" in out
    out = _cli("archive", cfg_path, "--tables", "plain", "--dry-run", expect=1)
    assert "not archivable" in out


def test_cli_usage_lists_restore():
    result = subprocess.run([sys.executable, "-m", "mkio"], capture_output=True, text=True)
    assert "mkio restore" in result.stdout
    assert "--older-than" in result.stdout
