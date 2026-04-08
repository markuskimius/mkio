"""Transaction service: config-driven SQL operations wrapping the write batcher."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

from aiohttp.web import WebSocketResponse

from mkio._ref import next_ref
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
        if ref is None:
            ref = next_ref()
        msg_type = msg.get("type", "")
        msgid = msg.get("msgid")

        # Handle "check" messages for transaction recovery
        if msg_type == "check":
            check_ref = msg.get("ref", "")
            cached = self._result_cache.get(check_ref)
            if cached is not None:
                resp = make_result(check_ref, self.name, cached, msgid=msgid)
            else:
                resp = make_result(check_ref, self.name, {"status": "unknown"}, msgid=msgid)
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
            result = await self.writer.submit(compiled_ops, params_list, data, ref=ref)
            # Cache result for recovery
            self._cache_result(ref, result)
            resp = make_result(ref, self.name, result, msgid=msgid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
        except KeyError as e:
            resp = make_error(ref, f"Missing field: {e}", msgid=msgid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)
        except Exception as e:
            resp = make_error(ref, str(e), msgid=msgid)
            await ws.send_bytes(resp)
            await self.notify_monitors("out", resp)

    def _cache_result(self, version: str, result: dict[str, Any]) -> None:
        self._result_cache[version] = result
        while len(self._result_cache) > self._cache_max_size:
            self._result_cache.popitem(last=False)


def _compile_op(spec: dict[str, Any]) -> CompiledOp:
    """Compile a single operation spec into a CompiledOp with parameterized SQL.

    The optional ``bind`` dict maps column names to ``"$N.field"`` references,
    where N is the zero-based index of a prior op in the same transaction whose
    RETURNING row supplies the value.  Bound columns are included in the SQL
    but their parameter values are resolved at execution time by the writer.
    """
    op_type = spec["op_type"]
    table = spec["table"]
    fields = list(spec.get("fields", []))
    key = spec.get("key", [])
    raw_bind = spec.get("bind", {})
    defaults = dict(spec.get("defaults", {}))

    # Parse bind references: {"order_id": "$0.id"} -> {"order_id": (0, "id")}
    bind: dict[str, tuple[int, str]] = {}
    for col, ref in raw_bind.items():
        if not isinstance(ref, str) or not ref.startswith("$"):
            raise ValueError(f"Invalid bind reference: {ref!r} (must be '$N.field')")
        idx_str, _, field_name = ref[1:].partition(".")
        if not field_name:
            raise ValueError(f"Invalid bind reference: {ref!r} (must be '$N.field')")
        bind[col] = (int(idx_str), field_name)

    # Merge bound and defaulted column names into the field list for SQL generation
    bound_cols = list(raw_bind.keys())
    default_cols = [c for c in defaults if c not in fields and c not in bound_cols]
    all_fields = fields + [c for c in bound_cols if c not in fields] + default_cols

    # _mkio_ref is always the last parameter (filled by writer at execution time)
    REF_COL = "_mkio_ref"

    if op_type == "insert":
        cols = all_fields + [REF_COL]
        col_list = ", ".join(cols)
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) RETURNING *"
        return CompiledOp(table, op_type, sql, tuple(cols), bind, defaults)

    elif op_type == "update":
        set_fields = fields + [c for c in bound_cols if c not in fields] + default_cols + [REF_COL]
        set_clause = ", ".join(f"{f} = ?" for f in set_fields)
        where_clause = " AND ".join(f"{k} = ?" for k in key)
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause} RETURNING *"
        return CompiledOp(table, op_type, sql, tuple(set_fields) + tuple(key), bind, defaults)

    elif op_type == "delete":
        where_clause = " AND ".join(f"{k} = ?" for k in key)
        sql = f"DELETE FROM {table} WHERE {where_clause}"
        return CompiledOp(table, op_type, sql, tuple(key), bind, defaults)

    elif op_type == "upsert":
        all_upsert = list(key) + [f for f in all_fields if f not in key] + [REF_COL]
        col_list = ", ".join(all_upsert)
        placeholders = ", ".join("?" for _ in all_upsert)
        non_key = [f for f in all_upsert if f not in key]
        update_clause = ", ".join(f"{f} = excluded.{f}" for f in non_key)
        key_list = ", ".join(key)
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT({key_list}) DO UPDATE SET {update_clause} RETURNING *"
        )
        return CompiledOp(table, op_type, sql, tuple(all_upsert), bind, defaults)

    else:
        raise ValueError(f"Unknown operation type: {op_type}")


def _extract_params(op: CompiledOp, data: dict[str, Any]) -> tuple[Any, ...]:
    """Extract parameters from message data in the order the SQL expects.

    Bound params and _mkio_ref get None placeholders — resolved by the writer.
    Defaults are used when the client doesn't provide a value.
    """
    result = []
    for p in op.param_names:
        if p in op.bind or p == "_mkio_ref":
            result.append(None)
        elif p in data:
            result.append(data[p])
        elif p in op.defaults:
            result.append(op.defaults[p])
        else:
            result.append(data[p])  # raises KeyError for missing required fields
    return tuple(result)
