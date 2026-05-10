"""ReqRep service: config-driven request-reply with SQL and/or expressions."""

from __future__ import annotations

from typing import Any, Callable

from aiohttp.web import WebSocketResponse

from mkio.services.base import Service
from mkio.ws_protocol import make_error, make_nack, make_reply


class ReqRepService(Service):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sql: str | None = self.config.get("sql")
        self._reply_expr: Callable[[dict], Any] | None = self.config.get(
            "_compiled_reply_expr"
        )
        self._reply_formatter: Callable[[dict], dict] | None = self.config.get(
            "_compiled_reply_formatter"
        )
        self._params_formatter: Callable[[dict], dict] | None = self.config.get(
            "_compiled_params"
        )

    async def on_subscribe(
        self, ws: WebSocketResponse, msg: dict[str, Any]
    ) -> int:
        subid = msg.get("subid")
        resp = make_nack(
            self.name,
            "reqrep does not support subscriptions — use type 'request'",
            ref=msg.get("ref"),
            msgid=msg.get("msgid"),
            subid=subid,
        )
        await ws.send_bytes(resp)
        await self.notify_monitors("out", resp)
        return 0

    async def on_message(
        self, ws: WebSocketResponse, msg: dict[str, Any]
    ) -> None:
        reqid = msg.get("reqid")
        data = msg.get("data") or {}

        try:
            params = (
                self._params_formatter(data)
                if self._params_formatter
                else data
            )

            if self._sql is not None:
                rows = await self.db.read(self._sql, params)
                if self._reply_expr is not None:
                    value = self._reply_expr(rows[0]) if rows else None
                    resp = make_reply(self.name, value=value, reqid=reqid)
                elif self._reply_formatter is not None:
                    rows = [self._reply_formatter(row) for row in rows]
                    resp = make_reply(self.name, rows=rows, reqid=reqid)
                else:
                    resp = make_reply(self.name, rows=rows, reqid=reqid)
            else:
                if self._reply_expr is not None:
                    value = self._reply_expr(data)
                    resp = make_reply(self.name, value=value, reqid=reqid)
                else:
                    assert self._reply_formatter is not None
                    row = self._reply_formatter(data)
                    resp = make_reply(self.name, row=row, reqid=reqid)

            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)

        except Exception as e:
            resp = make_error(None, str(e), reqid=reqid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
