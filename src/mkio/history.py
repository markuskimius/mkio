"""Versioned tables: version numbering, history schema, and undo/redo plans.

A table marked ``versioned = true`` carries a ``_mkio_version`` counter and a
companion history table holding every version of every row.  The history table
is a *version store with a cursor*, not an append-only audit log:

    orders__history, PK "O1":   v1 ── v2 ── v3 ── v4   (contiguous, 1..N)
    base row:                          ▲
                                _mkio_version = 3     (the cursor)
                                            └── v4 is redo, still reachable

The base row's ``_mkio_version`` says which version is current; its source
columns equal that history row's.  An absent base row means the cursor is at 0.
Writing at version V discards history at V and above, so a new edit after an
undo drops the abandoned redo branch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


#: Reserved suffix for history tables.  Application tables may not use it.
#: A suffix keeps a history table sorted next to the table it records, which
#: is what you want when browsing a schema with many tables.
HISTORY_SUFFIX = "__history"

#: Version counter, carried by both the base table and its history table.
VERSION_COLUMN = "_mkio_version"

#: Column definition used when adding the counter to a versioned base table.
#: Existing rows become version 1, matching the baseline history backfill.
VERSION_COLUMN_DEF = "INTEGER NOT NULL DEFAULT 1"

#: Metadata columns prepended to every history table, in CREATE TABLE order.
#: The primary key is ``(base pk columns..., _mkio_version)``, declared
#: separately so it can be composite.
HISTORY_META_COLUMNS: dict[str, str] = {
    VERSION_COLUMN: "INTEGER NOT NULL",
    "_mkio_op": "TEXT NOT NULL",
    "_mkio_ref": "TEXT NOT NULL",
    "_mkio_user": "TEXT",
    "_mkio_service": "TEXT",
}

#: Metadata columns the writer fills on every captured version, in SQL order.
HISTORY_WRITE_META: tuple[str, ...] = (
    VERSION_COLUMN,
    "_mkio_op",
    "_mkio_ref",
    "_mkio_user",
    "_mkio_service",
)

#: ``_mkio_op`` value for rows captured when versioning is first enabled.
OP_BASELINE = "baseline"

#: Framework-managed columns on a base table — never declared in config, and
#: excluded from schema diffs.
BASE_INTERNAL_COLUMNS: frozenset[str] = frozenset({"_mkio_ref", VERSION_COLUMN})


def history_table_name(table: str) -> str:
    """Return the history table name for a base table."""
    return table + HISTORY_SUFFIX


#: Client-facing alias.  The convention is fixed, so code that knows a base
#: table can reach its history without being told the name.
history_table = history_table_name


def is_history_table(name: str) -> bool:
    """True if ``name`` follows the history table naming convention."""
    return name.endswith(HISTORY_SUFFIX)


def base_table_name(name: str) -> str:
    """Return the base table a history table records, or ``name`` unchanged."""
    return name[: -len(HISTORY_SUFFIX)] if is_history_table(name) else name


_CONSTRAINT_KEYWORDS = frozenset({
    "PRIMARY", "NOT", "NULL", "UNIQUE", "DEFAULT", "CHECK", "REFERENCES",
    "COLLATE", "AUTOINCREMENT", "GENERATED", "AS", "CONSTRAINT",
})


def column_type(col_def: str) -> str:
    """Strip constraints from a column definition, leaving the type.

    ``"TEXT PRIMARY KEY"`` -> ``"TEXT"``, ``"DECIMAL(10, 2) NOT NULL"`` ->
    ``"DECIMAL(10, 2)"``.  History tables copy types but never constraints:
    old versions must be storable even when they would violate the constraints
    the base table carries today.
    """
    kept: list[str] = []
    depth = 0
    for token in col_def.split():
        if depth == 0 and token.upper().strip(",") in _CONSTRAINT_KEYWORDS:
            break
        kept.append(token)
        depth += token.count("(") - token.count(")")
    return " ".join(kept)


def primary_key_columns(table_cfg: dict[str, Any]) -> list[str]:
    """Primary key columns of a table config (composite key or inline)."""
    pk = list(table_cfg.get("primary_key", []) or [])
    if pk:
        return pk
    result = []
    for name, col_def in table_cfg.get("columns", {}).items():
        parts = col_def.upper().split()
        if "PRIMARY" in parts and "KEY" in parts:
            result.append(name)
    return result


def source_columns(table_cfg: dict[str, Any]) -> tuple[str, ...]:
    """Base table columns carried into history, in declaration order.

    ``_mkio_``-prefixed columns are excluded: they are framework-managed and
    already present as history metadata.
    """
    return tuple(
        name for name in table_cfg.get("columns", {})
        if not name.startswith("_mkio_")
    )


def history_columns(table_cfg: dict[str, Any]) -> dict[str, str]:
    """Full column definitions for a history table: metadata then source."""
    cols = dict(HISTORY_META_COLUMNS)
    for name in source_columns(table_cfg):
        cols[name] = column_type(table_cfg["columns"][name])
    return cols


def history_primary_key(table_cfg: dict[str, Any]) -> list[str]:
    """History table primary key: the base key, then the version."""
    return primary_key_columns(table_cfg) + [VERSION_COLUMN]


def history_table_config(table: str, table_cfg: dict[str, Any]) -> dict[str, Any]:
    """Synthetic table config for a history table.

    Shaped like a ``[tables]`` entry so config validation, migration and schema
    introspection can treat it as an ordinary table.
    """
    return {
        "columns": history_columns(table_cfg),
        "primary_key": history_primary_key(table_cfg),
        "_history_of": table,
    }


def history_index_sql(table: str, table_cfg: dict[str, Any]) -> list[str]:
    """Index statements for a history table.

    The primary key already indexes ``(pk..., _mkio_version)``, which serves
    per-row lookups and undo/redo.  This adds the ref index that age-based
    archiving scans.
    """
    hist = history_table_name(table)
    return [
        f"CREATE INDEX IF NOT EXISTS idx_{hist}_ref ON {hist}(_mkio_ref)"
    ]


def history_create_sql(table: str, table_cfg: dict[str, Any]) -> list[str]:
    """CREATE TABLE plus indexes for a history table."""
    hist = history_table_name(table)
    col_defs = ", ".join(
        f"{col} {typ}" for col, typ in history_columns(table_cfg).items()
    )
    pk = ", ".join(history_primary_key(table_cfg))
    return [
        f"CREATE TABLE IF NOT EXISTS {hist} ({col_defs}, PRIMARY KEY({pk}))"
    ] + history_index_sql(table, table_cfg)


def baseline_sql(table: str, table_cfg: dict[str, Any], *, has_ref: bool) -> str:
    """INSERT ... SELECT capturing existing rows as version 1.

    Run once, when a history table is first created for a table that already
    holds data.  Without it, undoing the first change to a pre-existing row
    would have no earlier version to step back onto.

    Each baseline row carries the base row's own ``_mkio_ref`` where one is
    present, so age-based archiving reflects when the row actually last
    changed.  Takes two parameters: the op label and the fallback ref.
    """
    hist = history_table_name(table)
    cols = list(source_columns(table_cfg))
    target = [VERSION_COLUMN, "_mkio_op", "_mkio_ref"] + cols
    ref_expr = "COALESCE(NULLIF(_mkio_ref, ''), ?)" if has_ref else "?"
    select = ["1", "?", ref_expr] + cols
    return (
        f"INSERT INTO {hist} ({', '.join(target)}) "
        f"SELECT {', '.join(select)} FROM {table}"
    )


def _key_match(left: str, right: str, pk: list[str]) -> str:
    return " AND ".join(f"{left}.{k} = {right}.{k}" for k in pk)


def _key_filter(prefix: str, pk: list[str]) -> str:
    return " AND ".join(f"{prefix}{k} = ?" for k in pk)


def _current_row_sql(table: str, pk: list[str]) -> str:
    """SELECT the live row for a key — the pre-image of a cursor move.

    Read before the plan runs so the change event can carry the shape the row
    had, which is what an application needs to work out the action that a
    given undo or redo implies.
    """
    return f"SELECT * FROM {table} WHERE {_key_filter('', pk)}"


@dataclass(frozen=True, slots=True)
class VersionPlan:
    """A two-step statement pair for undo/redo.

    ``primary`` is tried first; if it affects no row, ``fallback`` runs.  The
    parameter lists name either a data field or ``_mkio_ref``, which the writer
    substitutes with the transaction's ref.

    ``current`` reads the row before either step runs, so the emitted change
    carries both the old and the new shape.
    """

    primary_sql: str
    primary_params: tuple[str, ...]
    primary_op: str
    fallback_sql: str
    fallback_params: tuple[str, ...]
    fallback_op: str
    empty_message: str
    current_sql: str
    current_params: tuple[str, ...]


def undo_plan(table: str, table_cfg: dict[str, Any]) -> VersionPlan:
    """Step the cursor back one version, deleting the row when it was at 1.

    History is left untouched, so the step is redoable.
    """
    hist = history_table_name(table)
    pk = primary_key_columns(table_cfg)
    cols = [c for c in source_columns(table_cfg) if c not in pk]
    assignments = [f"{c} = h.{c}" for c in cols]
    assignments += [f"{VERSION_COLUMN} = h.{VERSION_COLUMN}", "_mkio_ref = ?"]
    step_back = (
        f"UPDATE {table} SET {', '.join(assignments)} "
        f"FROM {hist} h "
        f"WHERE {_key_match(table, 'h', pk)} "
        f"AND h.{VERSION_COLUMN} = {table}.{VERSION_COLUMN} - 1 "
        f"AND {_key_filter(table + '.', pk)} RETURNING *"
    )
    remove = (
        f"DELETE FROM {table} WHERE {_key_filter('', pk)} "
        f"AND {VERSION_COLUMN} = 1 RETURNING *"
    )
    return VersionPlan(
        primary_sql=step_back,
        primary_params=("_mkio_ref",) + tuple(pk),
        primary_op="update",
        fallback_sql=remove,
        fallback_params=tuple(pk),
        fallback_op="delete",
        empty_message="nothing to undo",
        current_sql=_current_row_sql(table, pk),
        current_params=tuple(pk),
    )


def redo_plan(table: str, table_cfg: dict[str, Any]) -> VersionPlan:
    """Step the cursor forward one version, rebuilding a row undone past 1."""
    hist = history_table_name(table)
    pk = primary_key_columns(table_cfg)
    cols = [c for c in source_columns(table_cfg) if c not in pk]
    assignments = [f"{c} = h.{c}" for c in cols]
    assignments += [f"{VERSION_COLUMN} = h.{VERSION_COLUMN}", "_mkio_ref = ?"]
    step_forward = (
        f"UPDATE {table} SET {', '.join(assignments)} "
        f"FROM {hist} h "
        f"WHERE {_key_match(table, 'h', pk)} "
        f"AND h.{VERSION_COLUMN} = {table}.{VERSION_COLUMN} + 1 "
        f"AND {_key_filter(table + '.', pk)} RETURNING *"
    )
    all_cols = list(source_columns(table_cfg))
    target = all_cols + ["_mkio_ref", VERSION_COLUMN]
    rebuild = (
        f"INSERT INTO {table} ({', '.join(target)}) "
        f"SELECT {', '.join(all_cols)}, ?, 1 FROM {hist} "
        f"WHERE {_key_filter('', pk)} AND {VERSION_COLUMN} = 1 "
        f"AND NOT EXISTS (SELECT 1 FROM {table} WHERE {_key_filter('', pk)}) "
        f"RETURNING *"
    )
    return VersionPlan(
        primary_sql=step_forward,
        primary_params=("_mkio_ref",) + tuple(pk),
        primary_op="update",
        fallback_sql=rebuild,
        fallback_params=("_mkio_ref",) + tuple(pk) + tuple(pk),
        fallback_op="insert",
        empty_message="nothing to redo",
        current_sql=_current_row_sql(table, pk),
        current_params=tuple(pk),
    )


@dataclass(frozen=True, slots=True)
class HistorySpec:
    """Precompiled statements the writer uses to record one changed row."""

    base_table: str
    table: str
    insert_sql: str
    truncate_sql: str
    truncate_all_sql: str
    columns: tuple[str, ...]
    pk: tuple[str, ...]

    def insert_params(
        self,
        row: dict[str, Any],
        op: str,
        ref: str,
        user: str | None,
        service: str | None,
    ) -> tuple[Any, ...]:
        return (row.get(VERSION_COLUMN), op, ref, user, service) + tuple(
            row.get(c) for c in self.columns
        )

    def key_params(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row.get(k) for k in self.pk)

    def truncate_params(self, row: dict[str, Any]) -> tuple[Any, ...]:
        """Key values plus the version at which the redo branch is cut."""
        return self.key_params(row) + (row.get(VERSION_COLUMN),)


def history_spec(table: str, table_cfg: dict[str, Any]) -> HistorySpec:
    """Compile the history statements for a versioned table."""
    cols = source_columns(table_cfg)
    pk = tuple(primary_key_columns(table_cfg))
    hist = history_table_name(table)
    all_cols = HISTORY_WRITE_META + cols
    placeholders = ", ".join("?" for _ in all_cols)
    key_filter = _key_filter("", list(pk))
    return HistorySpec(
        base_table=table,
        table=hist,
        insert_sql=(
            f"INSERT INTO {hist} ({', '.join(all_cols)}) VALUES ({placeholders})"
        ),
        truncate_sql=(
            f"DELETE FROM {hist} WHERE {key_filter} AND {VERSION_COLUMN} >= ?"
        ),
        truncate_all_sql=f"DELETE FROM {hist} WHERE {key_filter}",
        columns=cols,
        pk=pk,
    )


def orphan_history_gc_sql(table: str, table_cfg: dict[str, Any]) -> list[str]:
    """Statements dropping the redo stack: orphaned chains, then dangling redo.

    An orphaned chain is history for a key with no base row — a row undone past
    version 1 and not yet redone.  A dangling redo entry is a version above a
    live row's cursor.  Both are discarded by ``mkio dbupdate``.
    """
    hist = history_table_name(table)
    pk = primary_key_columns(table_cfg)
    match = " AND ".join(f"b.{k} = {hist}.{k}" for k in pk)
    return [
        f"DELETE FROM {hist} WHERE NOT EXISTS "
        f"(SELECT 1 FROM {table} b WHERE {match})",
        f"DELETE FROM {hist} WHERE EXISTS "
        f"(SELECT 1 FROM {table} b WHERE {match} "
        f"AND b.{VERSION_COLUMN} < {hist}.{VERSION_COLUMN})",
    ]


def versioned_tables(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Base tables marked ``versioned = true``, in config order."""
    return {
        name: cfg for name, cfg in config.get("tables", {}).items()
        if cfg.get("versioned")
    }


def history_table_configs(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Synthetic table configs for every versioned table's history table."""
    return {
        history_table_name(name): history_table_config(name, cfg)
        for name, cfg in versioned_tables(config).items()
    }


def history_specs(config: dict[str, Any]) -> dict[str, HistorySpec]:
    """Writer capture specs keyed by base table name."""
    return {
        name: history_spec(name, cfg)
        for name, cfg in versioned_tables(config).items()
    }


def effective_tables(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Config tables plus the derived history tables of versioned ones."""
    return {**config.get("tables", {}), **config.get("_history_tables", {})}
