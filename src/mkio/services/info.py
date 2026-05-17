"""Built-in _mkio service: server identity and metadata."""

from __future__ import annotations

import hashlib
import importlib.metadata
import time
from typing import Any

from aiohttp.web import WebSocketResponse

from mkio._json import dumps as json_dumps
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

    async def on_message(
        self, ws: WebSocketResponse, msg: dict[str, Any]
    ) -> None:
        reqid = msg.get("reqid")
        try:
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
                "protocol": "1.0",
                "services": services_map,
                "tables": tables,
                "config_hash": _config_hash(self._server_config),
                "uptime": uptime,
                "started": self._started_ref,
            }
            resp = make_reply(self.name, row=row, reqid=reqid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
        except Exception as e:
            resp = make_error(None, str(e), reqid=reqid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
