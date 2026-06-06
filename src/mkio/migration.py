"""Schema migration: diff, classify, and apply schema changes with data preservation."""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path


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


def _build_col_defs(table_config: dict) -> str:
    """Build column definitions string for CREATE TABLE, including composite PK."""
    col_defs = ", ".join(
        f"{col} {typ}" for col, typ in table_config["columns"].items()
    )
    pk = table_config.get("primary_key")
    if pk:
        col_defs += f", PRIMARY KEY({', '.join(pk)})"
    return col_defs


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
            col_defs = _build_col_defs(table_config)
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

    col_defs = _build_col_defs(config_table)
    shared_list = ", ".join(sorted(shared))

    config_pk = _get_pk_columns(config_parsed)
    if not config_pk:
        config_pk = list(config_table.get("primary_key", []))
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
    conn: sqlite3.Connection,
    changes: list[SchemaChange],
    config_tables: dict[str, dict] | None = None,
) -> tuple[int, int]:
    """Apply schema changes within a transaction.

    Returns (rows_before, rows_after) for tables that were recreated,
    so callers can report deduplication.
    """
    total_before = 0
    total_after = 0
    created_tables: list[str] = []

    for change in changes:
        if not change.sql_steps or change.sql_steps == ["-- handled by prior recreation"]:
            continue
        is_create = False
        for sql in change.sql_steps:
            if sql.startswith("--"):
                continue
            if sql.startswith("CREATE TABLE"):
                is_create = True
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
        if is_create:
            created_tables.append(change.table)

    # Seed newly created tables
    if config_tables and created_tables:
        for table_name in created_tables:
            tbl_cfg = config_tables.get(table_name, {})
            seed_path = tbl_cfg.get("_seed_path")
            if seed_path:
                table_columns = set(tbl_cfg.get("columns", {}).keys())
                count = seed_table(conn, table_name, seed_path, table_columns)
                if count:
                    print(f"  Seeded {table_name}: {count} rows from {Path(seed_path).name}")

    conn.commit()
    return total_before, total_after


_MIGRATE_LEVELS = ("safe", "risky", "destructive")


def _allowed_levels(level: str) -> set[str]:
    """Return the set of change levels permitted by the given migrate level."""
    if level == "safe":
        return {"safe"}
    if level == "risky":
        return {"safe", "potentially_destructive"}
    if level == "destructive":
        return {"safe", "potentially_destructive", "destructive"}
    return set()


def check_schema(
    conn: sqlite3.Connection,
    config_tables: dict[str, dict],
) -> list[SchemaChange]:
    """Return pending schema changes (empty list if schema is up to date)."""
    existing = get_existing_schema(conn)
    return diff_schema(existing, config_tables)


def migrate_schema(
    conn: sqlite3.Connection,
    config_tables: dict[str, dict],
    db_path: str = "",
    level: str = "safe",
) -> bool:
    """Run schema migration at the given level.

    Args:
        conn: SQLite connection (write connection).
        config_tables: The "tables" section from config.
        db_path: Path to DB file (for display).
        level: "safe", "risky", or "destructive".

    Returns True if migration succeeded or no changes needed.
    Returns False if there are changes beyond the allowed level.
    """
    existing = get_existing_schema(conn)
    changes = diff_schema(existing, config_tables)

    if not changes:
        return True

    allowed = _allowed_levels(level)
    applicable = [c for c in changes if c.level in allowed]
    blocked = [c for c in changes if c.level not in allowed]

    if applicable:
        rows_before, rows_after = apply_changes(conn, applicable, config_tables)
        for change in applicable:
            print(f"  Applied ({change.level}): {change.table} — {change.description}")
        if rows_before > rows_after:
            print(f"  Note: {rows_before - rows_after} rows were deduplicated during migration")

    if blocked:
        print()
        print("  Blocked changes (require higher migration level):")
        for change in blocked:
            print(f"    [{change.level}] {change.table} — {change.description}")
            if change.data_impact:
                print(f"      Impact: {change.data_impact}")
        if level == "safe":
            print()
            print("  To apply: mkio dbupdate --allow-risky  or  mkio dbupdate --allow-destructive")
        elif level == "risky":
            print()
            print("  To apply: mkio dbupdate --allow-destructive")
        return False

    return True


# ---------------------------------------------------------------------------
# Table seeding
# ---------------------------------------------------------------------------


def _auto_convert(value: str):
    """Convert string values to int/float if possible."""
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _load_seed_rows(seed_path: str) -> list[dict]:
    """Load rows from a CSV, JSON, or JSONL file."""
    ext = Path(seed_path).suffix.lower()
    if ext == ".csv":
        with open(seed_path, newline="") as f:
            reader = csv.DictReader(f)
            return [{k: _auto_convert(v) for k, v in row.items()} for row in reader]
    elif ext == ".json":
        with open(seed_path) as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError(f"seed file {seed_path}: expected a JSON array, got {type(data).__name__}")
        return data
    elif ext == ".jsonl":
        rows = []
        with open(seed_path) as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise ValueError(f"seed file {seed_path}, line {lineno}: {exc}") from None
        return rows
    else:
        raise ValueError(f"seed file {seed_path}: unsupported extension {ext!r}")


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    for row in conn.execute(f"PRAGMA table_info({table})").fetchall():
        if row[1] == column:
            return True
    return False


def seed_table(
    conn: sqlite3.Connection,
    table_name: str,
    seed_path: str,
    table_columns: set[str],
) -> int:
    """Load seed data into a newly created table (sync, for on-disk DBs).

    Returns the number of rows inserted.
    """
    rows = _load_seed_rows(seed_path)
    if not rows:
        return 0

    for row in rows:
        unknown = set(row.keys()) - table_columns
        if unknown:
            available = ", ".join(sorted(table_columns))
            raise ValueError(
                f"seed file {seed_path}: unknown column(s) {', '.join(sorted(unknown))}. "
                f"Available columns: {available}"
            )

    has_ref = _table_has_column(conn, table_name, "_mkio_ref")
    cols = list(rows[0].keys())
    if has_ref:
        from mkio._ref import next_ref
        ref = next_ref()
        cols.append("_mkio_ref")

    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    for row in rows:
        vals = tuple(row.get(c) for c in rows[0].keys())
        if has_ref:
            vals += (ref,)
        conn.execute(sql, vals)

    return len(rows)


async def async_seed_table(
    conn,
    table_name: str,
    seed_path: str,
    table_columns: set[str],
) -> int:
    """Load seed data into a newly created table (async, for in-memory DBs).

    Returns the number of rows inserted.
    """
    rows = _load_seed_rows(seed_path)
    if not rows:
        return 0

    for row in rows:
        unknown = set(row.keys()) - table_columns
        if unknown:
            available = ", ".join(sorted(table_columns))
            raise ValueError(
                f"seed file {seed_path}: unknown column(s) {', '.join(sorted(unknown))}. "
                f"Available columns: {available}"
            )

    has_ref = False
    async with conn.execute(f"PRAGMA table_info({table_name})") as cursor:
        async for row_info in cursor:
            if row_info[1] == "_mkio_ref":
                has_ref = True
                break

    cols = list(rows[0].keys())
    if has_ref:
        from mkio._ref import next_ref
        ref = next_ref()
        cols.append("_mkio_ref")

    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    for row in rows:
        vals = tuple(row.get(c) for c in rows[0].keys())
        if has_ref:
            vals += (ref,)
        await (await conn.execute(sql, vals)).close()

    return len(rows)
