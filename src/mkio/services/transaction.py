"""Transaction service: config-driven SQL operations wrapping the write batcher."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

from aiohttp.web import WebSocketResponse

from mkio.services.base import Service
from mkio.writer import CompiledOp
from mkio.ws_protocol import make_result, make_error


class TransactionService(Service):
    """Executes configured SQL operations (insert/update/delete/upsert).

    Config:
        ops: list of {table, op_type, key?, fields?}

    Maintains a bounded result cache for transaction recovery after reconnect.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._compiled_ops: tuple[CompiledOp, ...] = ()
        self._result_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._cache_max_size = self.config.get("change_log_size", 10000)

    async def start(self) -> None:
        ops = self.config.get("ops", [])
        compiled = []
        for op_spec in ops:
            compiled.append(_compile_op(op_spec))
        self._compiled_ops = tuple(compiled)

    async def on_message(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        ref = msg.get("ref")
        msg_type = msg.get("type", "")

        # Handle "check" messages for transaction recovery
        if msg_type == "check":
            version = msg.get("version", "")
            cached = self._result_cache.get(version)
            if cached is not None:
                await ws.send_bytes(make_result(ref, self.name, version, cached))
            else:
                await ws.send_bytes(make_result(ref, self.name, "", {"status": "unknown"}))
            return

        # Execute transaction
        data = msg.get("data", {})
        try:
            params_list = tuple(
                _extract_params(op, data) for op in self._compiled_ops
            )
            result = await self.writer.submit(self._compiled_ops, params_list, data)
            version = result.get("version", "")
            # Cache result for recovery
            self._cache_result(version, result)
            await ws.send_bytes(make_result(ref, self.name, version, result))
        except KeyError as e:
            await ws.send_bytes(make_error(ref, f"Missing field: {e}"))
        except Exception as e:
            await ws.send_bytes(make_error(ref, str(e)))

    def _cache_result(self, version: str, result: dict[str, Any]) -> None:
        self._result_cache[version] = result
        while len(self._result_cache) > self._cache_max_size:
            self._result_cache.popitem(last=False)


def _compile_op(spec: dict[str, Any]) -> CompiledOp:
    """Compile a single operation spec into a CompiledOp with parameterized SQL."""
    op_type = spec["op_type"]
    table = spec["table"]
    fields = spec.get("fields", [])
    key = spec.get("key", [])

    if op_type == "insert":
        col_list = ", ".join(fields)
        placeholders = ", ".join("?" for _ in fields)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) RETURNING *"
        return CompiledOp(table, op_type, sql, tuple(fields))

    elif op_type == "update":
        set_clause = ", ".join(f"{f} = ?" for f in fields)
        where_clause = " AND ".join(f"{k} = ?" for k in key)
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause} RETURNING *"
        return CompiledOp(table, op_type, sql, tuple(fields) + tuple(key))

    elif op_type == "delete":
        where_clause = " AND ".join(f"{k} = ?" for k in key)
        sql = f"DELETE FROM {table} WHERE {where_clause}"
        return CompiledOp(table, op_type, sql, tuple(key))

    elif op_type == "upsert":
        all_fields = list(key) + list(fields)
        col_list = ", ".join(all_fields)
        placeholders = ", ".join("?" for _ in all_fields)
        update_clause = ", ".join(f"{f} = excluded.{f}" for f in fields)
        key_list = ", ".join(key)
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT({key_list}) DO UPDATE SET {update_clause} RETURNING *"
        )
        return CompiledOp(table, op_type, sql, tuple(all_fields))

    else:
        raise ValueError(f"Unknown operation type: {op_type}")


def _extract_params(op: CompiledOp, data: dict[str, Any]) -> tuple[Any, ...]:
    """Extract parameters from message data in the order the SQL expects."""
    return tuple(data[p] for p in op.param_names)
