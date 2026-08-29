"""Value semantics shared by the evaluator and the standard library.

Kinds: null, boolean, number, string, array, map, function, or a registered
host type. These rules are pinned by the conformance fixtures and mirrored
by the JS implementation.
"""

from __future__ import annotations

import math
from typing import Any

from ._env import find_type
from ._errors import ExprError


def kind(v: Any) -> str:
    if v is None:
        return "null"
    if v is True or v is False:
        return "boolean"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, (list, tuple)):
        return "array"
    if isinstance(v, dict):
        return "map"
    if callable(v):
        return "function"
    t = find_type(v)
    if t:
        return t.name
    return type(v).__name__


def is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and v is not True and v is not False


def truthy(v: Any) -> bool:
    if v is None or v is False:
        return False
    if v is True:
        return True
    if is_number(v):
        return v != 0
    if isinstance(v, (str, list, tuple, dict)):
        return len(v) > 0
    t = find_type(v)
    if t and t.truthy:
        return bool(t.truthy(v))
    return True


def number_to_string(v: int | float) -> str:
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
        if v.is_integer() and abs(v) < 1e21:
            return str(int(v))
        r = repr(v)
        # Python repr gives '1e-07'; JS gives '1e-7'. Normalise exponent form.
        if "e" in r:
            mant, exp = r.split("e")
            sign = "-" if exp.startswith("-") else "+"
            exp = exp.lstrip("+-").lstrip("0") or "0"
            return f"{mant}e{sign}{exp}"
        return r
    return str(v)


def to_string(v: Any) -> str:
    """String coercion used by templates, STR(), and CONCAT()."""
    if v is None:
        return ""
    if v is True:
        return "true"
    if v is False:
        return "false"
    if is_number(v):
        return number_to_string(v)
    if isinstance(v, str):
        return v
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_repr(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k}: {_repr(x)}" for k, x in v.items()) + "}"
    t = find_type(v)
    if t and t.to_string:
        return t.to_string(v)
    if callable(v):
        return "<function>"
    return str(v)


def _repr(v: Any) -> str:
    if isinstance(v, str):
        return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if v is None:
        return "null"
    return to_string(v)


def equals(a: Any, b: Any) -> bool:
    """Strict equality: same kind, deep for arrays and maps, no coercion."""
    ka, kb = kind(a), kind(b)
    if ka != kb:
        return False
    if ka == "array":
        return len(a) == len(b) and all(equals(x, y) for x, y in zip(a, b))
    if ka == "map":
        return a.keys() == b.keys() and all(equals(a[k], b[k]) for k in a)
    if ka == "number":
        return a == b
    t = find_type(a)
    if t and t.compare:
        return t.compare(a, b) == 0
    return a == b


def compare(a: Any, b: Any, op: str, pos: int | None = None) -> int:
    """Ordering for < <= > >=. Numbers with numbers, strings with strings,
    host types via their compare hook. Anything else is an error."""
    if is_number(a) and is_number(b):
        return (a > b) - (a < b)
    if isinstance(a, str) and isinstance(b, str):
        return (a > b) - (a < b)
    t = find_type(a)
    if t and t.compare and t.is_instance(b):
        return t.compare(a, b)
    raise ExprError(f"Cannot compare {kind(a)} {op} {kind(b)}", pos)


def add(a: Any, b: Any, pos: int | None = None) -> Any:
    if is_number(a) and is_number(b):
        return normalize(a + b)
    if isinstance(a, str) and isinstance(b, str):
        return a + b
    ta = find_type(a)
    if ta and ta.add:
        return ta.add(a, b)
    tb = find_type(b)
    if tb and tb.add:
        return tb.add(a, b)
    raise ExprError(f"Cannot add {kind(a)} + {kind(b)}", pos)


def require_number(v: Any, what: str, pos: int | None = None) -> int | float:
    if not is_number(v):
        raise ExprError(f"{what} requires a number, got {kind(v)}", pos)
    return v


def normalize(v: Any) -> Any:
    """Collapse integral floats to int so arithmetic stays exact-looking."""
    if isinstance(v, float) and v.is_integer() and abs(v) < 2**53:
        return int(v)
    return v


def is_blocked_key(key: Any) -> bool:
    return isinstance(key, str) and (
        key.startswith("__") or key in ("constructor", "prototype")
    )
