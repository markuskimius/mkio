"""collection library: arrays and maps, higher-order via lambdas."""

from __future__ import annotations

from functools import cmp_to_key
from typing import Any

from ._env import register_library
from ._errors import ExprError
from ._values import compare, is_number, kind, truthy


def _arr(xs, name):
    if xs is None:
        return []
    if not isinstance(xs, (list, tuple)):
        raise ExprError(f"{name} requires an array, got {kind(xs)}")
    return xs


def _fn(f, name):
    if not callable(f):
        raise ExprError(f"{name} requires a lambda, got {kind(f)}")
    return f


def _map(xs, f):
    return [f(x) for x in _arr(xs, "MAP")] if (f := _fn(f, "MAP")) else []


def _filter(xs, f):
    f = _fn(f, "FILTER")
    return [x for x in _arr(xs, "FILTER") if truthy(f(x))]


def _any(xs, f=None):
    xs = _arr(xs, "ANY")
    if f is None:
        return any(truthy(x) for x in xs)
    f = _fn(f, "ANY")
    return any(truthy(f(x)) for x in xs)


def _all(xs, f=None):
    xs = _arr(xs, "ALL")
    if f is None:
        return all(truthy(x) for x in xs)
    f = _fn(f, "ALL")
    return all(truthy(f(x)) for x in xs)


def _find(xs, f):
    f = _fn(f, "FIND")
    for x in _arr(xs, "FIND"):
        if truthy(f(x)):
            return x
    return None


def _first(xs):
    xs = _arr(xs, "FIRST")
    return xs[0] if xs else None


def _last(xs):
    xs = _arr(xs, "LAST")
    return xs[-1] if xs else None


def _range(a, b=None, step=1):
    if b is None:
        a, b = 0, a
    for v in (a, b, step):
        if not is_number(v) or int(v) != v:
            raise ExprError("RANGE requires integers")
    if step == 0:
        raise ExprError("RANGE step must not be zero")
    return list(range(int(a), int(b), int(step)))


def _sort_by(xs, f=None, desc=False):
    xs = list(_arr(xs, "SORT_BY"))
    key = _fn(f, "SORT_BY") if f is not None else (lambda x: x)
    keyed = [(key(x), x) for x in xs]

    def cmp(a, b):
        ka, kb = a[0], b[0]
        if ka is None and kb is None:
            return 0
        if ka is None:
            return 1      # NULLs last
        if kb is None:
            return -1
        return compare(ka, kb, "SORT_BY")

    keyed.sort(key=cmp_to_key(cmp), reverse=truthy(desc))
    if truthy(desc):
        # keep NULLs last even when descending
        nn = [p for p in keyed if p[0] is not None]
        nulls = [p for p in keyed if p[0] is None]
        keyed = nn + nulls
    return [x for _, x in keyed]


def _reduce(xs, f, init=None):
    f = _fn(f, "REDUCE")
    acc = init
    for x in _arr(xs, "REDUCE"):
        acc = f(acc, x)
    return acc


def _keys(m):
    if m is None:
        return []
    if not isinstance(m, dict):
        raise ExprError(f"KEYS requires a map, got {kind(m)}")
    return list(m.keys())


def _values(m):
    if m is None:
        return []
    if not isinstance(m, dict):
        raise ExprError(f"VALUES requires a map, got {kind(m)}")
    return list(m.values())


def _flatten(xs):
    out: list[Any] = []
    for x in _arr(xs, "FLATTEN"):
        if isinstance(x, (list, tuple)):
            out.extend(x)
        else:
            out.append(x)
    return out


def _merge(*maps):
    out: dict[str, Any] = {}
    for m in maps:
        if m is None:
            continue
        if not isinstance(m, dict):
            raise ExprError(f"MERGE requires maps, got {kind(m)}")
        out.update(m)
    return out


register_library("collection", {
    "MAP":     (_map, {"params": ("xs", "fn"), "doc": "Apply `fn` to each element."}),
    "FILTER":  (_filter, {"params": ("xs", "fn"), "doc": "Elements for which `fn` is truthy."}),
    "ANY":     (_any, {"params": ("xs", "fn"), "doc": "TRUE if `fn` (or the element) is truthy for any element."}),
    "ALL":     (_all, {"params": ("xs", "fn"), "doc": "TRUE if `fn` (or the element) is truthy for every element (TRUE for empty)."}),
    "FIND":    (_find, {"params": ("xs", "fn"), "doc": "First element for which `fn` is truthy, else NULL."}),
    "FIRST":   (_first, {"params": ("xs",), "doc": "First element, or NULL."}),
    "LAST":    (_last, {"params": ("xs",), "doc": "Last element, or NULL."}),
    "RANGE":   (_range, {"params": ("a", "b", "step"), "doc": "RANGE(n) → [0..n), RANGE(a, b) → [a..b), optional step."}),
    "SORT_BY": (_sort_by, {"params": ("xs", "fn", "desc"), "doc": "Stable sort by `fn(x)` (or the element); NULL keys last; `desc: TRUE` reverses."}),
    "REDUCE":  (_reduce, {"params": ("xs", "fn", "init"), "doc": "Fold with `fn(acc, x)` starting from `init`."}),
    "KEYS":    (_keys, {"params": ("m",), "doc": "Keys of a map."}),
    "VALUES":  (_values, {"params": ("m",), "doc": "Values of a map."}),
    "FLATTEN": (_flatten, {"params": ("xs",), "doc": "Flatten one level of nesting."}),
    "MERGE":   (_merge, {"doc": "Merge maps left to right (later keys win)."}),
})
