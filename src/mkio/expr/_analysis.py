"""Static analysis over the AST: which names, functions, numeric contexts."""

from __future__ import annotations

from ._ast import (
    Apply, Array, Binary, Call, Index, Lambda, Literal, Map, Name, Node, Unary,
)
from dataclasses import dataclass
from typing import Any, Mapping

from ._env import Env, default_env
from ._errors import ExprError

_NUMERIC_OPS = frozenset({"-", "*", "/", "//", "%", "**"})


def _let_binders(node: Call) -> tuple[set[str], list[tuple[Node, bool]]]:
    """For LET(name, value, ..., body): bound names and (child, bound-context) pairs."""
    names: set[str] = set()
    children: list[tuple[Node, bool]] = []
    args = node.args
    for i in range(0, len(args) - 1, 2):
        if isinstance(args[i], Name):
            names.add(args[i].name)
        children.append((args[i + 1], True))
    if args:
        children.append((args[-1], True))
    return names, children


def field_refs(node: Node) -> set[str]:
    """Root-scope names referenced, excluding lambda parameters and LET bindings."""
    refs: set[str] = set()
    _collect_refs(node, refs, frozenset())
    return refs


def _collect_refs(node: Node, refs: set[str], bound: frozenset[str]) -> None:
    if isinstance(node, Name):
        if node.name not in bound:
            refs.add(node.name)
    elif isinstance(node, Literal):
        return
    elif isinstance(node, Binary):
        _collect_refs(node.left, refs, bound)
        _collect_refs(node.right, refs, bound)
    elif isinstance(node, Unary):
        _collect_refs(node.operand, refs, bound)
    elif isinstance(node, Call):
        if node.name == "LET":
            names, _ = _let_binders(node)
            inner = bound
            args = node.args
            for i in range(0, len(args) - 1, 2):
                _collect_refs(args[i + 1], refs, inner)
                if isinstance(args[i], Name):
                    inner = inner | {args[i].name}
            if args:
                _collect_refs(args[-1], refs, inner)
        else:
            for a in node.args:
                _collect_refs(a, refs, bound)
        for _, v in node.kwargs:
            _collect_refs(v, refs, bound)
    elif isinstance(node, Lambda):
        _collect_refs(node.body, refs, bound | set(node.params))
    elif isinstance(node, Apply):
        _collect_refs(node.fn, refs, bound)
        _collect_refs(node.arg, refs, bound)
    elif isinstance(node, Array):
        for e in node.elements:
            _collect_refs(e, refs, bound)
    elif isinstance(node, Map):
        for v in node.values:
            _collect_refs(v, refs, bound)
    elif isinstance(node, Index):
        _collect_refs(node.target, refs, bound)
        _collect_refs(node.key, refs, bound)


def function_refs(node: Node) -> set[str]:
    """Upper-case names of every function called."""
    out: set[str] = set()
    _collect_fns(node, out)
    return out


def _collect_fns(node: Node, out: set[str]) -> None:
    if isinstance(node, Call):
        out.add(node.name)
        for a in node.args:
            _collect_fns(a, out)
        for _, v in node.kwargs:
            _collect_fns(v, out)
    elif isinstance(node, Binary):
        _collect_fns(node.left, out)
        _collect_fns(node.right, out)
    elif isinstance(node, Unary):
        _collect_fns(node.operand, out)
    elif isinstance(node, Lambda):
        _collect_fns(node.body, out)
    elif isinstance(node, Apply):
        _collect_fns(node.fn, out)
        _collect_fns(node.arg, out)
    elif isinstance(node, Array):
        for e in node.elements:
            _collect_fns(e, out)
    elif isinstance(node, Map):
        for v in node.values:
            _collect_fns(v, out)
    elif isinstance(node, Index):
        _collect_fns(node.target, out)
        _collect_fns(node.key, out)


def numeric_fields(node: Node, env: Env | None = None) -> set[str]:
    """Root-scope names that appear in numeric contexts (arithmetic other
    than +, unary minus, or arguments of functions registered ``numeric``)."""
    refs: set[str] = set()
    _collect_numeric(node, refs, False, frozenset(), env or default_env)
    return refs


def _collect_numeric(node: Node, refs: set[str], ctx: bool, bound: frozenset[str], env: Env) -> None:
    if isinstance(node, Name):
        if ctx and node.name not in bound:
            refs.add(node.name)
    elif isinstance(node, Binary):
        child = ctx or node.op in _NUMERIC_OPS
        _collect_numeric(node.left, refs, child, bound, env)
        _collect_numeric(node.right, refs, child, bound, env)
    elif isinstance(node, Unary):
        _collect_numeric(node.operand, refs, ctx or node.op == "-", bound, env)
    elif isinstance(node, Call):
        fdef = env.function(node.name)
        child = ctx or (fdef is not None and fdef.numeric)
        if node.name == "LET":
            inner = bound
            args = node.args
            for i in range(0, len(args) - 1, 2):
                _collect_numeric(args[i + 1], refs, ctx, inner, env)
                if isinstance(args[i], Name):
                    inner = inner | {args[i].name}
            if args:
                _collect_numeric(args[-1], refs, ctx, inner, env)
        else:
            for a in node.args:
                _collect_numeric(a, refs, child, bound, env)
        for _, v in node.kwargs:
            _collect_numeric(v, refs, False, bound, env)
    elif isinstance(node, Lambda):
        _collect_numeric(node.body, refs, ctx, bound | set(node.params), env)
    elif isinstance(node, Apply):
        _collect_numeric(node.fn, refs, ctx, bound, env)
        _collect_numeric(node.arg, refs, ctx, bound, env)
    elif isinstance(node, Array):
        for e in node.elements:
            _collect_numeric(e, refs, ctx, bound, env)
    elif isinstance(node, Map):
        for v in node.values:
            _collect_numeric(v, refs, ctx, bound, env)
    elif isinstance(node, Index):
        _collect_numeric(node.target, refs, ctx, bound, env)


# -- Field paths -------------------------------------------------------------

# Standard functions that hand the elements of their first argument to a
# lambda, and which of its parameters receives them.
_ELEMENT_PARAM = {
    "MAP": 0, "FILTER": 0, "ANY": 0, "ALL": 0, "FIND": 0, "COUNT": 0, "SORT_BY": 0,
    "SUM": 0, "AVG": 0, "MIN": 0, "MAX": 0, "REDUCE": 1,
}


@dataclass(frozen=True, slots=True)
class FieldPath:
    """One read of the root scope: ``order.leaves_qty`` is ``("order",
    "leaves_qty")``. ``"*"`` stands for an element — a numeric or computed
    key, or what a lambda parameter ranges over — so ``trades[0].px`` and the
    ``t.px`` of ``MAP(trades, t -> t.px)`` are both ``("trades", "*", "px")``.
    ``positions`` holds the source offset of each segment."""
    path: tuple[str, ...]
    positions: tuple[int, ...]

    @property
    def pos(self) -> int:
        return self.positions[-1]

    def __str__(self) -> str:
        return ".".join(self.path)


_Binding = tuple[tuple[str, ...], tuple[int, ...]] | None   # a path, or opaque


def field_paths(node: Node) -> list[FieldPath]:
    """Every path the expression reads from the root scope, in source order,
    each reported once at its full length (``a.b.c``, not ``a`` and ``a.b``).
    Lambda parameters and LET names resolve to what they were bound to where
    that is itself a path; a name bound to anything else is not reported."""
    out: list[FieldPath] = []
    _collect_paths(node, {}, out)
    return out


def _chain(node: Node, bound: dict[str, _Binding], out: list[FieldPath]) -> _Binding:
    """The path ``node`` denotes, or None; computed keys are walked for their own reads."""
    if isinstance(node, Name):
        if node.name in bound:
            return bound[node.name]
        return (node.name,), (node.pos,)
    if isinstance(node, Index):
        base = _chain(node.target, bound, out)
        key = node.key
        if isinstance(key, Literal) and isinstance(key.value, str):
            seg = key.value
        else:
            seg = "*"
            if not isinstance(key, Literal):
                _collect_paths(key, bound, out)
        if base is None:
            if not isinstance(node.target, (Name, Index)):
                _collect_paths(node.target, bound, out)
            return None
        return base[0] + (seg,), base[1] + (key.pos,)
    return None


def _collect_paths(node: Node, bound: dict[str, _Binding], out: list[FieldPath]) -> None:
    if isinstance(node, (Name, Index)):
        found = _chain(node, bound, out)
        if found is not None:
            out.append(FieldPath(*found))
    elif isinstance(node, Binary):
        _collect_paths(node.left, bound, out)
        _collect_paths(node.right, bound, out)
    elif isinstance(node, Unary):
        _collect_paths(node.operand, bound, out)
    elif isinstance(node, Call):
        args = node.args
        if node.name == "LET":
            inner = dict(bound)
            for i in range(0, len(args) - 1, 2):
                _collect_paths(args[i + 1], inner, out)
                if isinstance(args[i], Name):
                    inner[args[i].name] = _silent_chain(args[i + 1], inner)
            if args:
                _collect_paths(args[-1], inner, out)
        else:
            element: _Binding = None
            if args and node.name in _ELEMENT_PARAM:
                source = _silent_chain(args[0], bound)
                if source is not None:
                    element = (source[0] + ("*",), source[1] + (source[1][-1],))
            for a in args:
                if isinstance(a, Lambda) and node.name in _ELEMENT_PARAM:
                    inner = {**bound, **{p: None for p in a.params}}
                    k = _ELEMENT_PARAM[node.name]
                    if k < len(a.params):
                        inner[a.params[k]] = element
                    _collect_paths(a.body, inner, out)
                else:
                    _collect_paths(a, bound, out)
        for _, v in node.kwargs:
            _collect_paths(v, bound, out)
    elif isinstance(node, Lambda):
        _collect_paths(node.body, {**bound, **{p: None for p in node.params}}, out)
    elif isinstance(node, Apply):
        _collect_paths(node.arg, bound, out)
        fn = node.fn
        if isinstance(fn, Lambda) and len(fn.params) == 1:
            _collect_paths(fn.body, {**bound, fn.params[0]: _silent_chain(node.arg, bound)}, out)
        else:
            _collect_paths(fn, bound, out)
    elif isinstance(node, Array):
        for e in node.elements:
            _collect_paths(e, bound, out)
    elif isinstance(node, Map):
        for v in node.values:
            _collect_paths(v, bound, out)


def _silent_chain(node: Node, bound: dict[str, _Binding]) -> _Binding:
    return _chain(node, bound, []) if isinstance(node, (Name, Index)) else None


def check_fields(node: Node, schema: Mapping[str, Any]) -> list[ExprError]:
    """Problems with the paths ``node`` reads, against ``schema`` — returned,
    not raised, so a host can show them all. A missing map key is NULL at run
    time, which makes a misspelt column (``order.leave_qty``) fail silently in
    ``==`` or ``MIN(…)``; this finds it before anything runs.

    ``schema`` maps each root name to what lies under it: a mapping of known
    keys (recursively), or None for a value whose inside is not described.
    The key ``"*"`` describes elements — of an array, or of a map with
    arbitrary keys (``{"tag": {"*": None}}``).
    """
    problems: list[ExprError] = []
    for fp in field_paths(node):
        spec: Any = schema
        for i, seg in enumerate(fp.path):
            if not isinstance(spec, Mapping):
                break
            if seg in spec and seg != "*":
                spec = spec[seg]
            elif "*" in spec:
                spec = spec["*"]
            elif seg == "*":
                break
            else:
                known = ", ".join(sorted(k for k in spec)) or "(none)"
                where = "Available fields" if i == 0 else f"{'.'.join(fp.path[:i])} has"
                problems.append(ExprError(
                    f"Unknown field: {'.'.join(fp.path[:i + 1])!r}. {where}: {known}", fp.positions[i]))
                break
    return problems
