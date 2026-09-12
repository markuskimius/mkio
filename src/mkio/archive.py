"""Row archiving: export table rows to CSV, delete them, and restore them later.

A table opts in with an ``archive`` key::

    [tables.orders]
    archive = { cutoff = "created_at", format = "%Y-%m-%d %H:%M:%S", group = "data" }

    [tables.sessions]
    archive = { group = "config", with = ["session_state"] }

``cutoff`` names the column a run's cutoff is compared against (``column <
cutoff``, as text); a table without one is archived whole.  ``format`` is the
strftime pattern that renders a cutoff instant in the column's own convention
(default ``%Y-%m-%d %H:%M:%S``, SQLite's ``CURRENT_TIMESTAMP`` shape) — every
comparison is against UTC.  ``group`` is a free label a run can select by
(default ``data``).  ``with`` names companion tables whose rows share the
table's primary key columns and travel with it: selected, exported, deleted
and restored alongside their parent row.

A run writes one directory: ``manifest.json``, ``<table>.csv`` per table,
``<table>__history.csv`` for versioned tables (the archived rows' whole
version chains — deleting a live row drops its chain, so the chain has to
leave with it) and ``<companion>.csv`` per companion.  Every column is
written, ``_mkio_ref`` and ``_mkio_version`` included, so a restore puts the
rows back exactly as they were.  CSV cannot tell NULL from ``''`` on its own,
so NULL is written as backslash-N (a text value starting with a backslash gets one
more in front, and loses it on the way back); the declared types the manifest
carries turn the other cells back into numbers.

Two drivers share the selection and file writing here: the offline one
(``archive_offline``) works on the database file with sqlite3 in one
transaction, and the online one (``MkioApp.archive``) reads through the
server and deletes through the writer, one op per row, so every subscriber
sees each removal.  Restore (``restore_offline``) is offline only: it must
write ``_mkio_version`` and ``_mkio_ref`` verbatim, which the writer would
not.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from mkio.history import (
    VERSION_COLUMN,
    column_type,
    history_columns,
    history_table_name,
    primary_key_columns,
)

MANIFEST = "manifest.json"
DEFAULT_GROUP = "data"
DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S"

#: Framework columns present on every base table, in the order they are
#: exported after the declared ones.
_REF_COLUMN = "_mkio_ref"


class ArchiveError(Exception):
    """A run that cannot proceed: bad selection, refused by a hook, a
    restore that would collide.  The message is meant for the user."""


@dataclass(frozen=True)
class ArchiveSpec:
    """One archivable table, resolved from config."""

    table: str
    cutoff: str | None
    format: str
    group: str
    companions: tuple[str, ...]
    versioned: bool
    pk: tuple[str, ...]
    columns: dict[str, str]  # declared name -> SQLite type, framework columns included
    history_columns: dict[str, str] = field(default_factory=dict)
    companion_columns: dict[str, dict[str, str]] = field(default_factory=dict)

    @property
    def history_table(self) -> str | None:
        return history_table_name(self.table) if self.versioned else None


# ── Config ────────────────────────────────────────────────────────────


def validate_archive_key(table: str, table_cfg: dict[str, Any], tables: dict[str, Any]) -> None:
    """Validate a table's ``archive`` key against the whole ``[tables]`` map."""
    spec = table_cfg.get("archive")
    if spec is None:
        return
    if not isinstance(spec, dict):
        raise ValueError(
            f"Table {table!r}: 'archive' must be a table of options "
            f"(cutoff, format, group, with), got {spec!r}"
        )
    unknown = set(spec) - {"cutoff", "format", "group", "with"}
    if unknown:
        raise ValueError(
            f"Table {table!r}: unknown archive option(s) {', '.join(sorted(unknown))} "
            f"(expected cutoff, format, group, with)"
        )
    columns = table_cfg.get("columns", {})
    cutoff = spec.get("cutoff")
    if cutoff is not None and (not isinstance(cutoff, str) or cutoff not in columns):
        raise ValueError(
            f"Table {table!r}: archive cutoff {cutoff!r} is not a column of the table"
        )
    fmt = spec.get("format")
    if fmt is not None and (not isinstance(fmt, str) or not fmt):
        raise ValueError(f"Table {table!r}: archive format must be a strftime pattern")
    if fmt is not None and cutoff is None:
        raise ValueError(
            f"Table {table!r}: archive format needs a cutoff column to apply to"
        )
    group = spec.get("group")
    if group is not None and (not isinstance(group, str) or not group or group == "all"):
        raise ValueError(
            f"Table {table!r}: archive group must be a non-empty name other than 'all'"
        )
    if not primary_key_columns(table_cfg):
        raise ValueError(
            f"Table {table!r}: archivable tables need a primary key — rows are "
            f"deleted and restored by key. Add PRIMARY KEY to a column, or a "
            f"primary_key = [...] entry."
        )
    companions = spec.get("with")
    if companions is None:
        return
    if not isinstance(companions, list) or not all(
        isinstance(c, str) and c for c in companions
    ):
        raise ValueError(
            f"Table {table!r}: archive 'with' must be a list of table names, "
            f"got {companions!r}"
        )
    pk = primary_key_columns(table_cfg)
    for comp in companions:
        if comp == table:
            raise ValueError(f"Table {table!r}: cannot be its own archive companion")
        comp_cfg = tables.get(comp)
        if comp_cfg is None:
            raise ValueError(
                f"Table {table!r}: archive companion {comp!r} is not a declared table"
            )
        if comp_cfg.get("versioned"):
            raise ValueError(
                f"Table {table!r}: archive companion {comp!r} is versioned — a "
                f"versioned table archives on its own, with its history"
            )
        missing = [k for k in pk if k not in comp_cfg.get("columns", {})]
        if missing:
            raise ValueError(
                f"Table {table!r}: archive companion {comp!r} lacks the key "
                f"column(s) {', '.join(missing)} rows are matched on"
            )


def _declared_columns(table_cfg: dict[str, Any], *, versioned: bool) -> dict[str, str]:
    cols = {
        name: column_type(defn).upper() or "TEXT"
        for name, defn in table_cfg.get("columns", {}).items()
    }
    cols.setdefault(_REF_COLUMN, "TEXT")
    if versioned:
        cols.setdefault(VERSION_COLUMN, "INTEGER")
    return cols


def archive_specs(config: dict[str, Any]) -> dict[str, ArchiveSpec]:
    """Every table carrying an ``archive`` key, in config order."""
    tables = config.get("tables", {})
    specs: dict[str, ArchiveSpec] = {}
    for name, cfg in tables.items():
        spec = cfg.get("archive")
        if spec is None:
            continue
        versioned = bool(cfg.get("versioned"))
        companions = tuple(spec.get("with", []) or [])
        specs[name] = ArchiveSpec(
            table=name,
            cutoff=spec.get("cutoff"),
            format=spec.get("format") or DEFAULT_FORMAT,
            group=spec.get("group") or DEFAULT_GROUP,
            companions=companions,
            versioned=versioned,
            pk=tuple(primary_key_columns(cfg)),
            columns=_declared_columns(cfg, versioned=versioned),
            history_columns=(
                {c: t.upper() or "TEXT" for c, t in history_columns(cfg).items()}
                if versioned else {}
            ),
            companion_columns={
                c: _declared_columns(tables[c], versioned=False) for c in companions
            },
        )
    return specs


def select_specs(
    config: dict[str, Any],
    tables: list[str] | None = None,
    group: str | None = None,
) -> list[ArchiveSpec]:
    """Resolve a run's tables: named ones, a group, or ``group="all"``.

    With neither, the ``data`` group.  Names and groups are checked, so a
    typo is an error rather than an empty run.
    """
    specs = archive_specs(config)
    if not specs:
        raise ArchiveError(
            "No archivable tables in config — add archive = { ... } to a table."
        )
    if tables:
        chosen: list[ArchiveSpec] = []
        for name in tables:
            if name not in specs:
                available = ", ".join(specs)
                raise ArchiveError(
                    f"Table {name!r} is not archivable. Archivable tables: {available}"
                )
            if specs[name] not in chosen:
                chosen.append(specs[name])
        return chosen
    if group == "all":
        return list(specs.values())
    group = group or DEFAULT_GROUP
    chosen = [s for s in specs.values() if s.group == group]
    if not chosen:
        groups = ", ".join(sorted({s.group for s in specs.values()}))
        raise ArchiveError(f"No archivable table is in group {group!r}. Groups: {groups}")
    return chosen


# ── Cutoff ────────────────────────────────────────────────────────────


_RELATIVE = re.compile(r"(\d+)([dhm])")


def parse_cutoff(text: str, *, now: datetime | None = None) -> datetime:
    """A cutoff instant from ``Nd``/``Nh``/``Nm`` back from now, or an ISO
    date or date-time.  Naive values are local time; the result is UTC."""
    text = text.strip()
    now = now or datetime.now(timezone.utc)
    m = _RELATIVE.fullmatch(text)
    if m:
        seconds = int(m.group(1)) * {"d": 86400, "h": 3600, "m": 60}[m.group(2)]
        return (now - timedelta(seconds=seconds)).astimezone(timezone.utc)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ArchiveError(
            f"Cannot parse cutoff {text!r}: use Nd/Nh/Nm, a date (2026-09-11) or "
            f"a date-time (2026-09-11 17:30, 2026-09-11T17:30:00Z)"
        ) from None
    if dt.tzinfo is None:
        dt = dt.astimezone()  # local
    return dt.astimezone(timezone.utc)


def midnight_today(*, now: datetime | None = None) -> datetime:
    """The start of today in local time, as UTC."""
    local = (now or datetime.now(timezone.utc)).astimezone()
    return local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)


def cutoff_value(spec: ArchiveSpec, cutoff: datetime | None, literal: str | None) -> str | None:
    """The string compared against ``spec.cutoff``: the literal as given, or
    the instant rendered in the table's format.  None for whole-table specs."""
    if spec.cutoff is None:
        return None
    if literal is not None:
        return literal
    if cutoff is None:
        raise ArchiveError(f"Table {spec.table!r} archives by {spec.cutoff!r}: a cutoff is required")
    return cutoff.astimezone(timezone.utc).strftime(spec.format)


# ── Selection SQL ─────────────────────────────────────────────────────


def _where(spec: ArchiveSpec, alias: str, value: str | None) -> tuple[str, tuple[Any, ...]]:
    if spec.cutoff is None or value is None:
        return "1", ()
    return f"{alias}.{spec.cutoff} < ?", (value,)


def live_select_sql(spec: ArchiveSpec, value: str | None) -> tuple[str, tuple[Any, ...]]:
    where, params = _where(spec, "b", value)
    order = ", ".join(f"b.{k}" for k in spec.pk)
    return f"SELECT b.* FROM {spec.table} b WHERE {where} ORDER BY {order}", params


def dependent_select_sql(
    spec: ArchiveSpec, table: str, value: str | None, *, extra_order: str | None = None
) -> tuple[str, tuple[Any, ...]]:
    """Rows of a history or companion table whose parent row is selected."""
    where, params = _where(spec, "b", value)
    match = " AND ".join(f"b.{k} = d.{k}" for k in spec.pk)
    order = ", ".join(f"d.{k}" for k in spec.pk)
    if extra_order:
        order += f", d.{extra_order}"
    return (
        f"SELECT d.* FROM {table} d WHERE EXISTS "
        f"(SELECT 1 FROM {spec.table} b WHERE {match} AND {where}) ORDER BY {order}",
        params,
    )


def key_of(spec: ArchiveSpec, row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row[k] for k in spec.pk)


def delete_sql(table: str, pk: tuple[str, ...], *, returning: bool) -> str:
    where = " AND ".join(f"{k} = ?" for k in pk)
    return f"DELETE FROM {table} WHERE {where}" + (" RETURNING *" if returning else "")


# ── Files ─────────────────────────────────────────────────────────────


@dataclass
class TableSelection:
    spec: ArchiveSpec
    cutoff_value: str | None
    live: list[dict[str, Any]]
    history: list[dict[str, Any]]
    companions: dict[str, list[dict[str, Any]]]

    @property
    def keys(self) -> list[tuple[Any, ...]]:
        return [key_of(self.spec, r) for r in self.live]


def run_dir_name(app_name: str, *, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d-%H%M%S")
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", app_name) or "mkio"
    return f"{safe}_{stamp}"


NULL = "\\N"


def _encode(value: Any) -> Any:
    if value is None:
        return NULL
    if isinstance(value, str) and value.startswith("\\"):
        return "\\" + value
    return value


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([_encode(row.get(c)) for c in columns])
        f.flush()
        os.fsync(f.fileno())


def _columns_of(rows: list[dict[str, Any]], declared: dict[str, str]) -> list[str]:
    """Declared columns first, then anything else the database returned
    (a column added by migration that config no longer declares)."""
    cols = list(declared)
    if rows:
        cols += [c for c in rows[0] if c not in declared]
    return cols


def _typed(columns: list[str], declared: dict[str, str]) -> dict[str, str]:
    return {c: declared.get(c, "TEXT") for c in columns}


def write_archive(
    out_dir: Path,
    selections: list[TableSelection],
    *,
    app_name: str,
    app_version: str,
    mkio_version: str,
    mode: str,
    cutoff: datetime | None,
    cutoff_given: str | None,
    db_path: str,
) -> dict[str, Any]:
    """Write every CSV and the manifest; returns the manifest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "mkio": mkio_version,
        "app": app_name,
        "app_version": app_version,
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": mode,
        "database": str(Path(db_path).resolve()) if db_path != ":memory:" else db_path,
        "cutoff": {
            "given": cutoff_given,
            "utc": cutoff.isoformat(timespec="seconds") if cutoff else None,
        },
        "tables": {},
    }
    for sel in selections:
        spec = sel.spec
        live_cols = _columns_of(sel.live, spec.columns)
        file = f"{spec.table}.csv"
        _write_csv(out_dir / file, live_cols, sel.live)
        entry: dict[str, Any] = {
            "file": file,
            "rows": len(sel.live),
            "primary_key": list(spec.pk),
            "cutoff_column": spec.cutoff,
            "cutoff_value": sel.cutoff_value,
            "group": spec.group,
            "columns": _typed(live_cols, spec.columns),
        }
        if spec.versioned:
            hist_cols = _columns_of(sel.history, spec.history_columns)
            hfile = f"{spec.history_table}.csv"
            _write_csv(out_dir / hfile, hist_cols, sel.history)
            entry["history"] = {
                "table": spec.history_table,
                "file": hfile,
                "rows": len(sel.history),
                "columns": _typed(hist_cols, spec.history_columns),
            }
        if spec.companions:
            entry["companions"] = {}
            for comp in spec.companions:
                rows = sel.companions.get(comp, [])
                declared = spec.companion_columns[comp]
                cols = _columns_of(rows, declared)
                cfile = f"{comp}.csv"
                _write_csv(out_dir / cfile, cols, rows)
                entry["companions"][comp] = {
                    "file": cfile, "rows": len(rows), "columns": _typed(cols, declared),
                }
        manifest["tables"][spec.table] = entry
    with open(out_dir / MANIFEST, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    return manifest



# ── Offline driver ────────────────────────────────────────────────────


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)
    ).fetchone() is not None


def select_offline(
    conn: sqlite3.Connection, spec: ArchiveSpec, value: str | None
) -> TableSelection:
    sql, params = live_select_sql(spec, value)
    live = _rows(conn, sql, params)
    history: list[dict[str, Any]] = []
    if spec.versioned and _table_exists(conn, spec.history_table or ""):
        sql, params = dependent_select_sql(
            spec, spec.history_table or "", value, extra_order=VERSION_COLUMN
        )
        history = _rows(conn, sql, params)
    companions: dict[str, list[dict[str, Any]]] = {}
    for comp in spec.companions:
        sql, params = dependent_select_sql(spec, comp, value)
        companions[comp] = _rows(conn, sql, params)
    return TableSelection(spec, value, live, history, companions)


def delete_offline(conn: sqlite3.Connection, sel: TableSelection) -> None:
    spec = sel.spec
    keys = sel.keys
    if not keys:
        return
    if spec.versioned and _table_exists(conn, spec.history_table or ""):
        conn.executemany(delete_sql(spec.history_table or "", spec.pk, returning=False), keys)
    for comp in spec.companions:
        conn.executemany(delete_sql(comp, spec.pk, returning=False), keys)
    conn.executemany(delete_sql(spec.table, spec.pk, returning=False), keys)


def archive_offline(
    config: dict[str, Any],
    *,
    tables: list[str] | None = None,
    group: str | None = None,
    cutoff: datetime | None = None,
    cutoff_literal: str | None = None,
    cutoff_given: str | None = None,
    out_dir: str | Path = ".",
    dry_run: bool = False,
    mkio_version: str = "",
) -> dict[str, Any]:
    """Archive on the database file.  Returns a summary shaped like the
    online driver's: ``{"dir", "tables": {name: {"rows", "history", ...}},
    "dry_run"}``.  Nothing is committed until every file is on disk."""
    specs = select_specs(config, tables, group)
    values = [cutoff_value(spec, cutoff, cutoff_literal) for spec in specs]
    db_path = config.get("db_path", "mkio.db")
    if db_path == ":memory:":
        raise ArchiveError("archive does not apply to in-memory databases")
    if not Path(db_path).exists():
        raise ArchiveError(f"Database not found: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN IMMEDIATE")
        selections = [
            select_offline(conn, spec, value) for spec, value in zip(specs, values)
        ]
        summary = _summary(selections, dry_run=dry_run)
        if dry_run:
            conn.rollback()
            return summary
        run_dir = Path(out_dir) / run_dir_name(config.get("name", "") or Path(db_path).stem)
        write_archive(
            run_dir, selections,
            app_name=config.get("name", ""), app_version=str(config.get("version", "")),
            mkio_version=mkio_version, mode="offline", cutoff=cutoff,
            cutoff_given=cutoff_given, db_path=db_path,
        )
        for sel in selections:
            delete_offline(conn, sel)
        conn.commit()
        summary["dir"] = str(run_dir)
        return summary
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _summary(selections: list[TableSelection], *, dry_run: bool) -> dict[str, Any]:
    return {
        "dry_run": dry_run,
        "dir": None,
        "tables": {
            sel.spec.table: {
                "rows": len(sel.live),
                "history": len(sel.history) if sel.spec.versioned else None,
                "companions": {c: len(r) for c, r in sel.companions.items()},
                "cutoff_column": sel.spec.cutoff,
                "cutoff_value": sel.cutoff_value,
            }
            for sel in selections
        },
    }


# ── Restore ───────────────────────────────────────────────────────────


def _decode(value: str, sql_type: str) -> Any:
    """A CSV cell back to a Python value: backslash-N is NULL, a leading
    backslash pair is one backslash, numbers by the column's declared type."""
    if value == NULL:
        return None
    if value.startswith("\\\\"):
        return value[1:]
    if value == "":
        return ""
    t = sql_type.upper()
    if t.startswith("INT"):
        return int(value)
    if t.startswith(("REAL", "FLOA", "DOUB")):
        return float(value)
    return value


def read_csv(path: Path, types: dict[str, str]) -> tuple[list[str], list[list[Any]]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        columns = next(reader, [])
        rows = [[_decode(v, types.get(c, "TEXT")) for c, v in zip(columns, r)] for r in reader]
    return columns, rows


def load_manifest(archive_dir: str | Path) -> dict[str, Any]:
    path = Path(archive_dir) / MANIFEST
    if not path.exists():
        raise ArchiveError(f"Not an archive directory (no {MANIFEST}): {archive_dir}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _existing_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def _check_columns(conn: sqlite3.Connection, table: str, archived: list[str]) -> None:
    if not _table_exists(conn, table):
        raise ArchiveError(f"Table {table!r} does not exist in this database")
    unknown = [c for c in archived if c not in _existing_columns(conn, table)]
    if unknown:
        raise ArchiveError(
            f"Table {table!r}: archived column(s) {', '.join(unknown)} no longer "
            f"exist — restore into a database with the schema the archive was taken from"
        )


def _insert(
    conn: sqlite3.Connection, table: str, columns: list[str], rows: list[list[Any]],
    *, replace: bool,
) -> None:
    if not rows:
        return
    verb = "INSERT OR REPLACE" if replace else "INSERT"
    placeholders = ", ".join("?" for _ in columns)
    conn.executemany(
        f"{verb} INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", rows
    )


def restore_offline(
    config: dict[str, Any],
    archive_dir: str | Path,
    *,
    tables: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Put an archive's rows back.  One transaction; a collision aborts it.

    A table archived by cutoff must not already hold any restored key.  A
    table archived whole (settings, counters) may: its rows are replaced and
    reported under ``replaced``.
    """
    db_path = config.get("db_path", "mkio.db")
    if db_path == ":memory:":
        raise ArchiveError("restore does not apply to in-memory databases")
    if not Path(db_path).exists():
        raise ArchiveError(f"Database not found: {db_path}")
    archive_dir = Path(archive_dir)
    manifest = load_manifest(archive_dir)
    entries: dict[str, Any] = manifest.get("tables", {})
    if tables:
        missing = [t for t in tables if t not in entries]
        if missing:
            raise ArchiveError(
                f"Not in this archive: {', '.join(missing)} (archived: {', '.join(entries)})"
            )
        entries = {t: entries[t] for t in tables}
    specs = archive_specs(config)

    conn = sqlite3.connect(db_path)
    result: dict[str, Any] = {"dry_run": dry_run, "tables": {}}
    try:
        conn.execute("BEGIN IMMEDIATE")
        for table, entry in entries.items():
            pk = list(entry.get("primary_key", []))
            columns, rows = read_csv(archive_dir / entry["file"], entry["columns"])
            _check_columns(conn, table, columns)
            whole = entry.get("cutoff_column") is None
            existing = _existing_keys(conn, table, pk, columns, rows)
            if existing and not whole:
                shown = ", ".join(str(k) for k in existing[:10])
                more = f" (+{len(existing) - 10} more)" if len(existing) > 10 else ""
                raise ArchiveError(
                    f"Table {table!r}: {len(existing)} archived row(s) already exist "
                    f"— {shown}{more}. Restore into a database they were archived from, "
                    f"or archive them again first."
                )
            spec = specs.get(table)
            versioned_now = spec.versioned if spec else bool(
                config.get("tables", {}).get(table, {}).get("versioned")
            )
            if VERSION_COLUMN in columns and not versioned_now:
                idx = columns.index(VERSION_COLUMN)
                columns = columns[:idx] + columns[idx + 1:]
                rows = [r[:idx] + r[idx + 1:] for r in rows]
            summary: dict[str, Any] = {
                "rows": len(rows), "replaced": len(existing), "history": 0, "companions": {},
            }
            try:
                _insert(conn, table, columns, rows, replace=whole)
                hist = entry.get("history")
                if hist and versioned_now:
                    hcols, hrows = read_csv(archive_dir / hist["file"], hist["columns"])
                    _check_columns(conn, hist["table"], hcols)
                    # A whole-table restore replaced the live rows, so their
                    # chains are replaced too rather than doubled.
                    if whole and hrows:
                        conn.executemany(
                            delete_sql(hist["table"], tuple(pk), returning=False),
                            [tuple(r[hcols.index(k)] for k in pk) for r in hrows],
                        )
                    _insert(conn, hist["table"], hcols, hrows, replace=whole)
                    summary["history"] = len(hrows)
                elif hist:
                    summary["history_skipped"] = (
                        f"{table} is no longer versioned; {hist['rows']} history rows not restored"
                    )
                for comp, centry in (entry.get("companions") or {}).items():
                    ccols, crows = read_csv(archive_dir / centry["file"], centry["columns"])
                    _check_columns(conn, comp, ccols)
                    _insert(conn, comp, ccols, crows, replace=whole)
                    summary["companions"][comp] = len(crows)
            except sqlite3.IntegrityError as exc:
                raise ArchiveError(
                    f"Table {table!r}: restore collides with rows already present ({exc})"
                ) from None
            result["tables"][table] = summary
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return result
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _existing_keys(
    conn: sqlite3.Connection, table: str, pk: list[str],
    columns: list[str], rows: list[list[Any]],
) -> list[tuple[Any, ...]]:
    if not pk or not rows or any(k not in columns for k in pk):
        return []
    idx = [columns.index(k) for k in pk]
    where = " AND ".join(f"{k} = ?" for k in pk)
    found: list[tuple[Any, ...]] = []
    for r in rows:
        key = tuple(r[i] for i in idx)
        if conn.execute(f"SELECT 1 FROM {table} WHERE {where}", key).fetchone():
            found.append(key)
    return found
