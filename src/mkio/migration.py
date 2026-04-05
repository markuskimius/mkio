"""Schema migration: diff, classify, and apply schema changes with data preservation."""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass, field


@dataclass
class SchemaChange:
    table: str
    description: str
    level: str  # "safe" | "potentially_destructive" | "destructive"
    sql_steps: list[str] = field(default_factory=list)
    data_impact: str = ""


def get_existing_schema(conn: sqlite3.Connection) -> dict[str, dict]:
    """Read existing table schemas from SQLite.

    Returns: {table_name: {"columns": {col_name: {"type": str, "notnull": bool,
              "dflt_value": str|None, "pk": int}}, "pk_columns": [str, ...]}}
    """
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = {}
    for (table_name,) in cursor.fetchall():
        cols = {}
        pk_columns = []
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall():
            # row: (cid, name, type, notnull, dflt_value, pk)
            cid, name, col_type, notnull, dflt_value, pk = row
            cols[name] = {
                "type": col_type,
                "notnull": bool(notnull),
                "dflt_value": dflt_value,
                "pk": pk,
            }
            if pk > 0:
                pk_columns.append((pk, name))
        pk_columns.sort()
        tables[table_name] = {
            "columns": cols,
            "pk_columns": [name for _, name in pk_columns],
        }
    return tables


def _parse_config_columns(config_columns: dict[str, str]) -> dict[str, dict]:
    """Parse column definitions from config format to structured format.

    Config format: {"id": "TEXT PRIMARY KEY", "name": "TEXT NOT NULL"}
    """
    parsed = {}
    for col_name, col_def in config_columns.items():
        parts = col_def.upper().split()
        col_type = parts[0] if parts else ""
        notnull = "NOT" in parts and "NULL" in parts
        pk = "PRIMARY" in parts and "KEY" in parts
        dflt_value = None
        if "DEFAULT" in parts:
            idx = parts.index("DEFAULT")
            if idx + 1 < len(parts):
                # Rejoin from original to preserve case
                orig_parts = col_def.split()
                dflt_idx = next(
                    i for i, p in enumerate(orig_parts) if p.upper() == "DEFAULT"
                )
                dflt_value = " ".join(orig_parts[dflt_idx + 1 :])
        parsed[col_name] = {
            "type": col_type,
            "notnull": notnull or pk,
            "dflt_value": dflt_value,
            "pk": pk,
        }
    return parsed


def _get_pk_columns(parsed_cols: dict[str, dict]) -> list[str]:
    """Extract primary key column names from parsed columns."""
    return [name for name, info in parsed_cols.items() if info["pk"]]


_NON_CONSTANT_DEFAULTS = {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}


def _is_non_constant_default(dflt_value: str | None) -> bool:
    """Check if a default value is non-constant (can't be used with ALTER TABLE ADD COLUMN)."""
    if dflt_value is None:
        return False
    return dflt_value.upper().strip("'\"") in _NON_CONSTANT_DEFAULTS


def diff_schema(
    existing: dict[str, dict], config_tables: dict[str, dict]
) -> list[SchemaChange]:
    """Compare existing DB schema against config and classify changes."""
    changes: list[SchemaChange] = []

    # Tables in config but not in DB → new tables (safe)
    for table_name, table_config in config_tables.items():
        if table_name not in existing:
            col_defs = ", ".join(
                f"{col} {typ}" for col, typ in table_config["columns"].items()
            )
            changes.append(SchemaChange(
                table=table_name,
                description=f"Create new table",
                level="safe",
                sql_steps=[f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})"],
                data_impact="None — new table",
            ))
            continue

        # Table exists — compare columns
        existing_table = existing[table_name]
        existing_cols = existing_table["columns"]
        existing_pk = existing_table["pk_columns"]

        config_parsed = _parse_config_columns(table_config["columns"])
        config_pk = _get_pk_columns(config_parsed)

        # New columns
        for col_name, col_info in config_parsed.items():
            if col_name not in existing_cols:
                if col_info["notnull"] and col_info["dflt_value"] is None and not col_info["pk"]:
                    changes.append(SchemaChange(
                        table=table_name,
                        description=f'Add NOT NULL column "{col_name}" without default',
                        level="potentially_destructive",
                        data_impact=f"Existing rows cannot satisfy NOT NULL constraint — requires table recreate",
                    ))
                elif _is_non_constant_default(col_info["dflt_value"]):
                    # SQLite ALTER TABLE ADD COLUMN doesn't support non-constant defaults
                    # (e.g. CURRENT_TIMESTAMP) — must use table recreation
                    changes.append(SchemaChange(
                        table=table_name,
                        description=f'Add column "{col_name}" (non-constant default)',
                        level="safe",
                        sql_steps=[],  # filled by _build_recreate_steps
                        data_impact=f"None — existing rows get default/NULL",
                    ))
                else:
                    col_def = table_config["columns"][col_name]
                    changes.append(SchemaChange(
                        table=table_name,
                        description=f'Add column "{col_name}"',
                        level="safe",
                        sql_steps=[f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}"],
                        data_impact=f"None — existing rows get default/NULL",
                    ))

        # Removed columns (skip internal _mkio_ref — managed by framework)
        for col_name in existing_cols:
            if col_name == "_mkio_ref":
                continue
            if col_name not in config_parsed:
                changes.append(SchemaChange(
                    table=table_name,
                    description=f'Remove column "{col_name}"',
                    level="destructive",
                    data_impact=f'Data in column "{col_name}" will be lost',
                ))

        # Changed column types (skip internal columns)
        for col_name, col_info in config_parsed.items():
            if col_name == "_mkio_ref":
                continue
            if col_name in existing_cols:
                existing_type = existing_cols[col_name]["type"].upper()
                config_type = col_info["type"].upper()
                if existing_type != config_type:
                    changes.append(SchemaChange(
                        table=table_name,
                        description=f'Change column "{col_name}" type from {existing_type} to {config_type}',
                        level="potentially_destructive",
                        data_impact=f"Values may not convert cleanly",
                    ))

        # Primary key changes
        if config_pk and existing_pk and config_pk != existing_pk:
            changes.append(SchemaChange(
                table=table_name,
                description=f"Change primary key {existing_pk} → {config_pk}",
                level="potentially_destructive",
                data_impact=f"Duplicate rows under new PK may be dropped",
            ))

    # Tables in DB but not in config → remove (destructive)
    for table_name in existing:
        if table_name not in config_tables:
            changes.append(SchemaChange(
                table=table_name,
                description="Remove table",
                level="destructive",
                sql_steps=[f"DROP TABLE {table_name}"],
                data_impact=f"All data in table will be lost",
            ))

    # Build SQL steps for changes that need table recreation
    for change in changes:
        if not change.sql_steps and change.table in config_tables and change.table in existing:
            _build_recreate_steps(change, existing[change.table], config_tables[change.table], changes)

    return changes


def _build_recreate_steps(
    trigger_change: SchemaChange,
    existing_table: dict,
    config_table: dict,
    all_changes: list[SchemaChange],
) -> None:
    """Build SQL steps for table recreation (used when ALTER TABLE can't handle the change).

    Only builds steps for the first change that needs recreation for a given table —
    subsequent changes for the same table will be handled in the same recreation.
    """
    table = trigger_change.table
    # Check if another change for this table already has recreation steps
    for other in all_changes:
        if other is not trigger_change and other.table == table and other.sql_steps:
            # Already handled
            trigger_change.sql_steps = ["-- handled by prior recreation"]
            return

    existing_cols = set(existing_table["columns"].keys()) - {"_mkio_ref"}
    config_parsed = _parse_config_columns(config_table["columns"])
    config_cols = set(config_parsed.keys())
    shared = existing_cols & config_cols

    col_defs = ", ".join(
        f"{col} {typ}" for col, typ in config_table["columns"].items()
    )
    shared_list = ", ".join(sorted(shared))

    config_pk = _get_pk_columns(config_parsed)
    existing_pk = existing_table["pk_columns"]
    pk_changed = config_pk and existing_pk and config_pk != existing_pk

    if pk_changed:
        insert_sql = (
            f"INSERT OR IGNORE INTO {table} ({shared_list}) "
            f"SELECT {shared_list} FROM {table}_old ORDER BY rowid"
        )
    else:
        insert_sql = (
            f"INSERT INTO {table} ({shared_list}) "
            f"SELECT {shared_list} FROM {table}_old"
        )

    trigger_change.sql_steps = [
        f"ALTER TABLE {table} RENAME TO {table}_old",
        f"CREATE TABLE {table} ({col_defs})",
        insert_sql,
        f"DROP TABLE {table}_old",
    ]


def count_table_rows(conn: sqlite3.Connection, table: str) -> int:
    """Count rows in a table."""
    cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
    return cursor.fetchone()[0]


def print_change_summary(
    changes: list[SchemaChange], db_path: str, conn: sqlite3.Connection
) -> None:
    """Print a human-readable summary of schema changes."""
    print(f'\nSchema changes detected for database "{db_path}":')
    print()
    print(f"  {'TABLE':<20} {'CHANGE':<45} {'LEVEL':<25} {'DATA IMPACT'}")
    print(f"  {'-'*20} {'-'*45} {'-'*25} {'-'*40}")
    for change in changes:
        level_display = change.level.upper().replace("_", " ")
        # Add row count for destructive table removal
        impact = change.data_impact
        if change.description == "Remove table":
            try:
                count = count_table_rows(conn, change.table)
                impact = f"All {count:,} rows will be lost"
            except Exception:
                pass
        print(f"  {change.table:<20} {change.description:<45} {level_display:<25} {impact}")
    print()


def apply_changes(
    conn: sqlite3.Connection, changes: list[SchemaChange]
) -> tuple[int, int]:
    """Apply schema changes within a transaction.

    Returns (rows_before, rows_after) for tables that were recreated,
    so callers can report deduplication.
    """
    total_before = 0
    total_after = 0

    for change in changes:
        if not change.sql_steps or change.sql_steps == ["-- handled by prior recreation"]:
            continue
        for sql in change.sql_steps:
            if sql.startswith("--"):
                continue
            # Track row counts for recreation
            if sql.startswith("ALTER TABLE") and sql.endswith("_old"):
                table = change.table
                try:
                    total_before += count_table_rows(conn, table)
                except Exception:
                    pass
            conn.execute(sql)
            if sql.startswith("DROP TABLE") and sql.endswith("_old"):
                table = change.table
                try:
                    total_after += count_table_rows(conn, table)
                except Exception:
                    pass

    conn.commit()
    return total_before, total_after


def migrate_schema(
    conn: sqlite3.Connection,
    config_tables: dict[str, dict],
    db_path: str = "",
    interactive: bool = True,
    auto_migrate: bool = False,
) -> bool:
    """Run schema migration. Returns True if migration succeeded or no changes needed.

    Args:
        conn: SQLite connection (write connection).
        config_tables: The "tables" section from config.
        db_path: Path to DB file (for display).
        interactive: Whether a TTY is available for prompting.
        auto_migrate: If True, apply all changes without prompting.
    """
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config_tables)

    if not changes:
        return True

    safe_changes = [c for c in changes if c.level == "safe"]
    risky_changes = [c for c in changes if c.level != "safe"]

    if not risky_changes:
        # All safe — apply automatically
        apply_changes(conn, safe_changes)
        for change in safe_changes:
            print(f"  Applied (safe): {change.table} — {change.description}")
        return True

    # Has risky changes — need confirmation
    print_change_summary(changes, db_path, conn)

    if auto_migrate:
        print("  auto_migrate=true, applying all changes...")
        rows_before, rows_after = apply_changes(conn, changes)
        if rows_before > rows_after:
            print(f"  Note: {rows_before - rows_after} rows were deduplicated during migration")
        return True

    if not interactive or not sys.stdin.isatty():
        print("  ERROR: Destructive/potentially destructive changes require confirmation.")
        print("  Run interactively or set auto_migrate=true in config.")
        return False

    # Interactive prompt
    response = input("  Apply these changes? [y/N] ").strip().lower()
    if response != "y":
        print("  Migration cancelled.")
        return False

    rows_before, rows_after = apply_changes(conn, changes)
    if rows_before > rows_after:
        print(f"  Note: {rows_before - rows_after} rows were deduplicated during migration")
    print("  Schema changes applied successfully.")
    return True
