"""Built-in _mkio service: server identity and metadata."""

from __future__ import annotations

import hashlib
import importlib.metadata
import time
from typing import Any

from aiohttp.web import WebSocketResponse

from mkio.expr import LANGUAGE_VERSION
from mkio._json import dumps as json_dumps
from mkio.history import (
    HISTORY_SUFFIX,
    effective_tables,
    history_table_name,
    is_history_table,
    versioned_tables,
)
from mkio.services.base import Service
from mkio.ws_protocol import make_error, make_nack, make_reply


def _strip_internal(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_internal(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_strip_internal(item) for item in obj]
    return obj


def _config_hash(config: dict[str, Any]) -> str:
    raw = json_dumps(_strip_internal(config))
    return hashlib.sha256(raw).hexdigest()[:8]


def _parse_semver(s: str) -> tuple[int, int, int] | None:
    if not s or not isinstance(s, str):
        return None
    parts = s.split(".")
    if len(parts) < 2 or len(parts) > 3:
        return None
    try:
        major = int(parts[0])
        minor = int(parts[1])
        patch = int(parts[2]) if len(parts) == 3 else 0
    except ValueError:
        return None
    if major < 0 or minor < 0 or patch < 0:
        return None
    return (major, minor, patch)


def _semver_compatible(server_ver: str, client_ver: str) -> bool:
    sv = _parse_semver(server_ver)
    cv = _parse_semver(client_ver)
    if sv is None or cv is None:
        return False
    s_maj, s_min, s_pat = sv
    c_maj, c_min, c_pat = cv
    if c_maj > 0:
        return s_maj == c_maj and sv >= cv
    if c_min > 0:
        return s_maj == 0 and s_min == c_min and s_pat >= c_pat
    return sv == cv


class InfoService(Service):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._server_config: dict[str, Any] = {}
        self._server_services: dict[str, Service] = {}
        self._started_ref: str = ""
        self._started_monotonic: float = 0.0

    async def on_subscribe(
        self, ws: WebSocketResponse, msg: dict[str, Any]
    ) -> int:
        resp = make_nack(
            self.name,
            "_mkio does not support subscriptions — use type 'request'",
            ref=msg.get("ref"),
            txnid=msg.get("txnid"),
            subid=msg.get("subid"),
        )
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)
        return 0

    async def _handle_schema(
        self, ws: WebSocketResponse, table: str, reqid: str | None
    ) -> None:
        # History tables are not advertised, but a caller that knows the
        # naming convention can introspect one.
        all_tables = effective_tables(self._server_config)
        if table not in all_tables:
            available = [t for t in all_tables if not is_history_table(t)]
            resp = make_error(
                self.name,
                f"Unknown table: {table!r} (available: {', '.join(available)})",
                reqid=reqid,
            )
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
            return

        rows = await self.db.read(f"PRAGMA table_info({table})")
        columns = [
            {
                "name": r["name"],
                "type": r["type"],
                "notnull": bool(r["notnull"]),
                "pk": bool(r["pk"]),
                "dflt_value": r["dflt_value"],
            }
            for r in rows
        ]
        row = {"table": table, "columns": columns}
        history_of = all_tables[table].get("_history_of")
        if history_of:
            row["history_of"] = history_of
        elif all_tables[table].get("versioned"):
            row["versioned"] = True
            row["history_table"] = history_table_name(table)
        resp = make_reply(self.name, row=row, reqid=reqid)
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)

    async def on_message(
        self, ws: WebSocketResponse, msg: dict[str, Any]
    ) -> None:
        reqid = msg.get("reqid")
        try:
            data = msg.get("data")
            # With auth enabled, unauthenticated callers get identity and
            # compatibility only — enough to pick and verify a server before
            # logging in, without exposing service or table names.
            limited = bool(self._server_config.get("auth")) and getattr(ws, "_mkio_auth", None) is None

            if isinstance(data, dict) and "table" in data:
                if limited:
                    resp = make_error(None, "authentication required", reqid=reqid, service=self.name)
                    await ws.send_bytes(resp)
                    await self.notify_monitors("out", resp)
                    return
                await self._handle_schema(ws, data["table"], reqid)
                return

            services_map = {
                name: svc.config.get("protocol", "unknown")
                for name, svc in self._server_services.items()
                if not name.startswith("_")
            }
            tables = list(self._server_config.get("tables", {}).keys())
            uptime = round(time.monotonic() - self._started_monotonic, 1)

            try:
                mkio_version = importlib.metadata.version("mkio")
            except importlib.metadata.PackageNotFoundError:
                mkio_version = "dev"

            row = {
                "name": self._server_config.get("name", ""),
                "version": self._server_config.get("version", ""),
                "mkio": mkio_version,
                "protocol": "1.2",
                "expr": LANGUAGE_VERSION,
                "services": services_map,
                "tables": tables,
                "versioned": list(versioned_tables(self._server_config)),
                "history_suffix": HISTORY_SUFFIX,
                "config_hash": _config_hash(self._server_config),
                "uptime": uptime,
                "started": self._started_ref,
            }
            if limited:
                for k in ("services", "tables", "versioned", "config_hash",
                          "uptime", "started"):
                    row.pop(k, None)
            if isinstance(data, dict):
                version_keys = ("version", "protocol", "mkio", "expr")
                checks = {k: data[k] for k in version_keys if k in data}
                if checks:
                    compatibility = {
                        k: (str(row[k]) == str(v)) if k == "expr" else _semver_compatible(row[k], v)
                        for k, v in checks.items()
                    }
                    row["compatible"] = all(compatibility.values())
                    row["compatibility"] = compatibility

            resp = make_reply(self.name, row=row, reqid=reqid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
        except Exception as e:
            resp = make_error(None, str(e), reqid=reqid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
