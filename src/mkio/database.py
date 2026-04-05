"""Database layer: dual-connection SQLite with WAL mode, schema migration, backup."""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import aiosqlite

from mkio.migration import migrate_schema


class Database:
    def __init__(self, path: str, tables: dict[str, dict], config: dict[str, Any] | None = None):
        self._path = path
        self._tables = tables
        self._config = config or {}
        self._write_conn: aiosqlite.Connection | None = None
        self._read_conn: aiosqlite.Connection | None = None
        self._checkpoint_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Open connections, run migration, set pragmas."""
        is_memory = self._path == ":memory:"

        # Run migration synchronously before opening async connections
        # (migration needs interactive prompting which doesn't work well with async)
        if not is_memory:
            self._run_migration()

        if is_memory:
            # Use a named shared-cache in-memory DB so both connections see the same data
            import uuid
            uri = f"file:mkio_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._write_conn = await aiosqlite.connect(uri, uri=True)
            self._read_conn = await aiosqlite.connect(uri, uri=True)
        else:
            self._write_conn = await aiosqlite.connect(self._path)
            self._read_conn = await aiosqlite.connect(self._path)

        for conn in (self._write_conn, self._read_conn):
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA synchronous=NORMAL")
            await conn.execute("PRAGMA cache_size=-64000")

        self._write_conn.row_factory = aiosqlite.Row
        self._read_conn.row_factory = aiosqlite.Row

        # Create tables for :memory: databases (migration doesn't run for them)
        if is_memory:
            for name, spec in self._tables.items():
                col_defs = ", ".join(
                    f"{col} {typ}" for col, typ in spec["columns"].items()
                )
                await self._write_conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {name} ({col_defs})"
                )
            await self._write_conn.commit()

        # Start periodic WAL checkpoint
        interval = self._config.get("wal_checkpoint_interval_s", 300)
        if interval > 0 and not is_memory:
            self._checkpoint_task = asyncio.create_task(
                self._periodic_checkpoint(interval)
            )

    def _run_migration(self) -> None:
        """Run schema migration synchronously."""
        # Backup before migration if configured
        if self._config.get("backup_on_startup") and Path(self._path).exists():
            self._create_backup_sync()

        conn = sqlite3.connect(self._path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            # Create tables that don't exist
            success = migrate_schema(
                conn=conn,
                config_tables=self._tables,
                db_path=self._path,
                interactive=True,
                auto_migrate=self._config.get("auto_migrate", False),
            )
            if not success:
                conn.close()
                raise SystemExit(1)
        finally:
            conn.close()

    def _create_backup_sync(self) -> None:
        """Create a backup of the database file."""
        backup_dir = Path(self._config.get("backup_dir", "./backups"))
        backup_dir.mkdir(parents=True, exist_ok=True)
        from mkio._version import next_version
        version = next_version().replace(" ", "_").replace(":", "").replace(".", "_")
        dest = backup_dir / f"{Path(self._path).stem}_backup_{version}.db"
        shutil.copy2(self._path, dest)
        print(f"  Backup created: {dest}")

    @property
    def write_conn(self) -> aiosqlite.Connection:
        assert self._write_conn is not None, "Database not started"
        return self._write_conn

    @property
    def read_conn(self) -> aiosqlite.Connection:
        assert self._read_conn is not None, "Database not started"
        return self._read_conn

    async def read(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Execute a read query on the read connection. Returns list of dicts."""
        async with self.read_conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            if rows and isinstance(rows[0], aiosqlite.Row):
                return [dict(r) for r in rows]
            return [dict(r) for r in rows] if rows else []

    async def checkpoint(self) -> None:
        """Force a WAL checkpoint, merging WAL into main DB."""
        if self._write_conn:
            await self._write_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    async def backup(self, dest_path: str) -> None:
        """Create a safe live backup of the database."""
        if self._write_conn:
            await self._write_conn.execute(f"VACUUM INTO '{dest_path}'")

    async def _periodic_checkpoint(self, interval_s: float) -> None:
        """Periodically checkpoint the WAL."""
        try:
            while True:
                await asyncio.sleep(interval_s)
                await self.checkpoint()
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Checkpoint WAL and close connections cleanly."""
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # Final checkpoint to merge WAL
        if self._write_conn and self._path != ":memory:":
            try:
                await self.checkpoint()
            except Exception:
                pass

        if self._read_conn:
            await self._read_conn.close()
        if self._write_conn:
            await self._write_conn.close()
