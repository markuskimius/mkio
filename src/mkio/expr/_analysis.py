"""Static analysis over the AST: which names, functions, numeric contexts."""

from __future__ import annotations

from ._ast import (
    Apply, Array, Binary, Call, Index, Lambda, Literal, Map, Name, Node, Unary,
)
from ._env import Env, default_env

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
