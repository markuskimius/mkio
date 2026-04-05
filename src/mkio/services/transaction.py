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

    Config (single op set):
        ops: list of {table, op_type, key?, fields?}

    Config (named op sets):
        ops: dict of name -> list of {table, op_type, key?, fields?}
        Client sends "op": "<name>" to select which set to run.

    Maintains a bounded result cache for transaction recovery after reconnect.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Named op sets: name -> tuple of CompiledOp
        self._named_ops: dict[str, tuple[CompiledOp, ...]] = {}
        # Default (unnamed) op set for backwards compatibility
        self._default_ops: tuple[CompiledOp, ...] | None = None
        self._result_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._cache_max_size = self.config.get("change_log_size", 10000)

    async def start(self) -> None:
        ops = self.config.get("ops", [])
        if isinstance(ops, dict):
            # Named op sets
            for op_name, op_list in ops.items():
                compiled = tuple(_compile_op(spec) for spec in op_list)
                self._named_ops[op_name] = compiled
        else:
            # Single (unnamed) op set
            self._default_ops = tuple(_compile_op(spec) for spec in ops)

    def _resolve_ops(self, msg: dict[str, Any]) -> tuple[CompiledOp, ...]:
        """Resolve which op set to use from the message."""
        op_name = msg.get("op")
        if op_name is not None:
            ops = self._named_ops.get(op_name)
            if ops is None:
                raise ValueError(f"Unknown op: {op_name!r}")
            return ops
        if self._default_ops is not None:
            return self._default_ops
        if len(self._named_ops) == 1:
            return next(iter(self._named_ops.values()))
        raise ValueError("Multiple ops defined — specify 'op' field")

    async def on_message(self, ws: WebSocketResponse, msg: dict[str, Any]) -> None:
        ref = msg.get("ref")
        msg_type = msg.get("type", "")

        # Handle "check" messages for transaction recovery
        if msg_type == "check":
            version = msg.get("version", "")
            cached = self._result_cache.get(version)
            if cached is not None:
                resp = make_result(ref, self.name, version, cached)
            else:
                resp = make_result(ref, self.name, "", {"status": "unknown"})
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
            return

        # Execute transaction
        data = msg.get("data", {})
        try:
            compiled_ops = self._resolve_ops(msg)
            params_list = tuple(
                _extract_params(op, data) for op in compiled_ops
            )
            result = await self.writer.submit(compiled_ops, params_list, data)
            version = result.get("version", "")
            # Cache result for recovery
            self._cache_result(version, result)
            resp = make_result(ref, self.name, version, result)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
        except KeyError as e:
            resp = make_error(ref, f"Missing field: {e}")
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
        except Exception as e:
            resp = make_error(ref, str(e))
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)

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
