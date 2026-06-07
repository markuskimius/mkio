"""Auth: authentication and access control."""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any

from aiohttp import web

from mkio._json import dumps
from mkio.database import Database

log = logging.getLogger("mkio.auth")

_BCRYPT_AVAILABLE = False
try:
    import bcrypt
    _BCRYPT_AVAILABLE = True
except ImportError:
    pass


def hash_password(password: str) -> str:
    """Hash a password using bcrypt if available, otherwise PBKDF2."""
    if _BCRYPT_AVAILABLE:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return "pbkdf2:" + salt.hex() + ":" + dk.hex()


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against its hash."""
    if hashed.startswith("pbkdf2:"):
        try:
            parts = hashed.split(":")
            if len(parts) != 3:
                return False
            salt = bytes.fromhex(parts[1])
            dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
            return dk.hex() == parts[2]
        except (ValueError, TypeError):
            return False
    if _BCRYPT_AVAILABLE:
        try:
            return bcrypt.checkpw(password.encode(), hashed.encode())
        except (ValueError, TypeError):
            return False
    return False


class RightsCache:
    """In-memory cache of role -> set of rights from _mkio_rights table."""

    def __init__(self) -> None:
        self._rights: dict[str, set[str]] = {}

    def load(self, rows: list[dict[str, Any]]) -> None:
        rights: dict[str, set[str]] = {}
        for row in rows:
            role = str(row["role"])
            right = str(row["right"])
            rights.setdefault(role, set()).add(right)
        self._rights = rights

    def role_has_right(self, role: str, right: str) -> bool:
        role_rights = self._rights.get(role)
        if role_rights is None:
            return False
        return right in role_rights

    def get_rights(self, role: str) -> set[str]:
        return self._rights.get(role, set())


async def load_rights_cache(db: Database) -> RightsCache:
    """Load rights from _mkio_rights table into a cache."""
    cache = RightsCache()
    rows = await db.read("SELECT role, right FROM _mkio_rights")
    cache.load(rows)
    return cache


async def authenticate_builtin(
    db: Database, data: dict[str, Any]
) -> dict[str, Any]:
    """Authenticate against _mkio_users table. Returns user info dict."""
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        raise ValueError("missing username or password")

    rows = await db.read(
        "SELECT * FROM _mkio_users WHERE username = :username",
        {"username": username},
    )
    if not rows:
        raise ValueError("invalid credentials")

    user_row = rows[0]
    stored_password = user_row.get("password", "")
    if not verify_password(password, stored_password):
        raise ValueError("invalid credentials")

    result = dict(user_row)
    del result["password"]
    result.setdefault("user", result.get("username", username))
    result.setdefault("role", "")
    return result


def check_access(
    access_config: Any,
    auth_info: dict[str, Any] | None,
    rights_cache: RightsCache,
) -> tuple[bool, str | None]:
    """Check if a user has access based on the access config.

    Returns (allowed, when_sql) where when_sql is the SQL condition
    to pre-check (if any), or None if no pre-check needed.
    """
    if access_config == "open":
        return True, None

    if auth_info is None:
        return False, None

    if access_config == "auth":
        return True, None

    if isinstance(access_config, str):
        role = auth_info.get("role", "")
        if rights_cache.role_has_right(role, access_config):
            return True, None
        return False, None

    if isinstance(access_config, dict):
        role = auth_info.get("role", "")
        role_rights = rights_cache.get_rights(role)
        for right_name, when_val in access_config.items():
            if right_name in role_rights:
                if when_val is True:
                    return True, None
                return True, when_val
        return False, None

    return False, None


def build_when_params(
    auth_info: dict[str, Any],
    msg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build bind parameters for a when SQL pre-check."""
    params: dict[str, Any] = {}
    params["user"] = auth_info.get("user", "")
    params["role"] = auth_info.get("role", "")
    for k, v in auth_info.items():
        if k not in ("user", "role", "password"):
            params[k] = v
    if msg:
        data = msg.get("data", {})
        if isinstance(data, dict):
            for k, v in data.items():
                if k not in params:
                    params[k] = v
        topic = msg.get("topic")
        if topic is not None:
            params["topic"] = topic
    return params


async def execute_when_check(
    db: Database,
    when_sql: str,
    params: dict[str, Any],
    *,
    conn: Any = None,
) -> bool:
    """Execute a when SQL pre-check. Returns True if at least one row returned."""
    sql = f"SELECT 1 FROM {when_sql} LIMIT 1"
    if conn is not None:
        cursor = await conn.execute(sql, params)
        row = await cursor.fetchone()
        await cursor.close()
        return row is not None
    rows = await db.read(sql, params)
    return len(rows) > 0
