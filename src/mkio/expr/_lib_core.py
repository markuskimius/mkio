"""core library: control flow, coercion, lookup."""

from __future__ import annotations

from typing import Any

from ._env import register_library
from ._errors import ExprError
from ._values import equals, is_number, kind, normalize, to_string, truthy


def _if(ctx, *args, **kwargs):
    if kwargs or len(args) != 3:
        raise ExprError("IF(cond, then, else) requires exactly 3 arguments", ctx.pos)
    return args[1].value() if truthy(args[0].value()) else args[2].value()


def _case(ctx, *args, **kwargs):
    if kwargs or len(args) < 2:
        raise ExprError("CASE(cond1, value1, ..., default?) requires at least 2 arguments", ctx.pos)
    pairs, default = (args[:-1], args[-1]) if len(args) % 2 == 1 else (args, None)
    for i in range(0, len(pairs), 2):
        if truthy(pairs[i].value()):
            return pairs[i + 1].value()
    return default.value() if default is not None else None


def _try(ctx, *args, **kwargs):
    if kwargs or len(args) not in (1, 2):
        raise ExprError("TRY(expr, fallback?) requires 1 or 2 arguments", ctx.pos)
    try:
        return args[0].value()
    except ExprError:
        return args[1].value() if len(args) == 2 else None


def _let(ctx, *args, **kwargs):
    if kwargs or len(args) < 3 or len(args) % 2 == 0:
        raise ExprError("LET(name, value, ..., body) requires an odd number of arguments (at least 3)", ctx.pos)
    scope = ctx.scope.child({})
    for i in range(0, len(args) - 1, 2):
        name = args[i].name
        if name is None:
            raise ExprError("LET binding names must be plain names", ctx.pos)
        scope.vars[name] = args[i + 1].eval(scope)
    return args[-1].eval(scope)


def _coalesce(*args):
    for a in args:
        if a is not None:
            return a
    return None


def _num_of(x):
    if is_number(x):
        return x
    if x is True:
        return 1
    if x is False:
        return 0
    if isinstance(x, str):
        s = x.strip().replace("_", "")
        if not s:
            return None
        try:
            return normalize(int(s))
        except ValueError:
            pass
        try:
            v = float(s)
        except ValueError:
            return None
        if v != v or v in (float("inf"), float("-inf")):
            return None
        return normalize(v)
    return None


def _int(x):
    n = _num_of(x)
    if n is None:
        return None
    return int(n)  # truncates toward zero


def _get(coll, key, default=None):
    if isinstance(coll, dict):
        return coll.get(key, default) if isinstance(key, str) else coll.get(str(key), default)
    if isinstance(coll, (list, tuple)) and is_number(key):
        i = int(key)
        return coll[i] if -len(coll) <= i < len(coll) else default
    return default


def _has(coll, key):
    if isinstance(coll, dict):
        return (key if isinstance(key, str) else str(key)) in coll
    if isinstance(coll, (list, tuple)) and is_number(key):
        return -len(coll) <= int(key) < len(coll)
    return False


register_library("core", {
    "IF":       (_if,       {"lazy": True, "params": ("cond", "then", "else"), "doc": "Return `then` if `cond` is truthy, else `else`. Only the taken branch is evaluated."}),
    "CASE":     (_case,     {"lazy": True, "doc": "CASE(c1, v1, c2, v2, ..., default?) — first truthy condition wins; NULL if none and no default."}),
    "TRY":      (_try,      {"lazy": True, "params": ("expr", "fallback"), "doc": "Evaluate `expr`; on error return `fallback` (or NULL)."}),
    "LET":      (_let,      {"lazy": True, "doc": "LET(name, value, ..., body) — bind names in order, then evaluate `body`."}),
    "COALESCE": (_coalesce, {"doc": "First non-NULL argument."}),
    "TYPE":     (kind,      {"params": ("x",), "doc": "Kind of a value: null, boolean, number, string, array, map, function, or a host type name."}),
    "STR":      (to_string, {"params": ("x",), "doc": "String form (NULL → '', TRUE → 'true', 2.0 → '2')."}),
    "NUM_OF":   (_num_of,   {"params": ("x",), "numeric": False, "doc": "Parse a number from a string or boolean; NULL if not numeric."}),
    "INT":      (_int,      {"params": ("x",), "doc": "Integer part, truncated toward zero; NULL if not numeric."}),
    "BOOL":     (truthy,    {"params": ("x",), "doc": "Truthiness: NULL, FALSE, 0, '', [], {} are false."}),
    "IS_NUM":   (is_number, {"params": ("x",), "doc": "TRUE for numbers."}),
    "IS_STR":   (lambda x: isinstance(x, str), {"params": ("x",), "doc": "TRUE for strings."}),
    "GET":      (_get,      {"params": ("coll", "key", "default"), "doc": "Lookup in a map or array, `default` (NULL) when absent — never an error."}),
    "HAS":      (_has,      {"params": ("coll", "key"), "doc": "TRUE if a map has the key or an array has the index."}),
})
