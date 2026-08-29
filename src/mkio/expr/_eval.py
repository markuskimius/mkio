"""Compile an AST into nested closures over a Scope.

``compile_node(node, env)`` returns ``fn(scope) -> value``. Lambdas become
:class:`Closure` values; lazy library functions receive :class:`Arg` thunks.
"""

from __future__ import annotations

import math
from typing import Any, Callable

from ._ast import (
    Apply, Array, Binary, Call, Index, Lambda, Literal, Map, Name, Node, Unary,
)
from ._env import Env, FunctionDef
from ._errors import ExprError
from ._values import (
    add, compare, equals, is_blocked_key, is_number, kind, normalize,
    require_number, truthy,
)


class Scope:
    """A frame of bindings with a parent. The root frame wraps the host mapping."""

    __slots__ = ("vars", "parent", "strict")

    def __init__(self, vars: dict[str, Any], parent: "Scope | None" = None, strict: bool = True) -> None:
        self.vars = vars
        self.parent = parent
        self.strict = strict

    def lookup(self, name: str, pos: int | None) -> Any:
        s: Scope | None = self
        while s is not None:
            if name in s.vars:
                return s.vars[name]
            s = s.parent
        if self.strict:
            names = sorted(self.all_names())
            available = ", ".join(names) if names else "(none)"
            raise ExprError(f"Unknown field: {name!r}. Available fields: {available}", pos)
        return None

    def all_names(self) -> set[str]:
        out: set[str] = set()
        s: Scope | None = self
        while s is not None:
            out.update(s.vars.keys())
            s = s.parent
        return out

    def child(self, vars: dict[str, Any]) -> "Scope":
        return Scope(vars, self, self.strict)


class Closure:
    """A lambda value. Callable from Python with positional arguments."""

    __slots__ = ("params", "body", "scope")

    def __init__(self, params: tuple[str, ...], body: Callable[[Scope], Any], scope: Scope) -> None:
        self.params = params
        self.body = body
        self.scope = scope

    def __call__(self, *args: Any) -> Any:
        if len(args) != len(self.params):
            raise ExprError(
                f"Lambda expects {len(self.params)} argument(s), got {len(args)}"
            )
        return self.body(self.scope.child(dict(zip(self.params, args))))


class Arg:
    """An unevaluated argument handed to a lazy function."""

    __slots__ = ("node", "fn", "scope")

    def __init__(self, node: Node, fn: Callable[[Scope], Any], scope: Scope) -> None:
        self.node = node
        self.fn = fn
        self.scope = scope

    def value(self) -> Any:
        return self.fn(self.scope)

    def eval(self, scope: Scope) -> Any:
        return self.fn(scope)

    @property
    def name(self) -> str | None:
        return self.node.name if isinstance(self.node, Name) else None


class Ctx:
    """Passed first to lazy functions."""

    __slots__ = ("scope", "env", "pos")

    def __init__(self, scope: Scope, env: Env, pos: int) -> None:
        self.scope = scope
        self.env = env
        self.pos = pos


def compile_node(node: Node, env: Env) -> Callable[[Scope], Any]:
    if isinstance(node, Literal):
        v = node.value
        return lambda scope: v

    if isinstance(node, Name):
        name, pos = node.name, node.pos
        return lambda scope: scope.lookup(name, pos)

    if isinstance(node, Unary):
        operand = compile_node(node.operand, env)
        pos = node.pos
        if node.op == "!":
            return lambda scope: not truthy(operand(scope))
        if node.op == "-":
            return lambda scope: -require_number(operand(scope), "Unary minus", pos)
        raise ExprError(f"Unknown unary operator: {node.op}", pos)

    if isinstance(node, Binary):
        return _compile_binary(node, env)

    if isinstance(node, Call):
        return _compile_call(node, env)

    if isinstance(node, Lambda):
        body = compile_node(node.body, env)
        params = node.params
        return lambda scope: Closure(params, body, scope)

    if isinstance(node, Apply):
        fn = compile_node(node.fn, env)
        arg = compile_node(node.arg, env)
        pos = node.pos
        def apply(scope: Scope) -> Any:
            f = fn(scope)
            if not isinstance(f, Closure):
                raise ExprError("Right side of |> must be a lambda", pos)
            return f(arg(scope))
        return apply

    if isinstance(node, Array):
        elements = [compile_node(e, env) for e in node.elements]
        return lambda scope: [e(scope) for e in elements]

    if isinstance(node, Map):
        keys = node.keys
        values = [compile_node(v, env) for v in node.values]
        return lambda scope: {k: v(scope) for k, v in zip(keys, values)}

    if isinstance(node, Index):
        return _compile_index(node, env)

    raise ExprError(f"Unknown node type: {type(node).__name__}")


def _compile_binary(node: Binary, env: Env) -> Callable[[Scope], Any]:
    op, pos = node.op, node.pos
    left = compile_node(node.left, env)
    right = compile_node(node.right, env)

    if op == "||":
        def or_(scope: Scope) -> Any:
            l = left(scope)
            return True if truthy(l) else truthy(right(scope))
        return or_
    if op == "&&":
        def and_(scope: Scope) -> Any:
            l = left(scope)
            return truthy(right(scope)) if truthy(l) else False
        return and_
    if op == "??":
        def coalesce(scope: Scope) -> Any:
            l = left(scope)
            return right(scope) if l is None else l
        return coalesce
    if op == "==":
        return lambda scope: equals(left(scope), right(scope))
    if op == "!=":
        return lambda scope: not equals(left(scope), right(scope))
    if op in ("<", "<=", ">", ">="):
        test = {
            "<": lambda c: c < 0, "<=": lambda c: c <= 0,
            ">": lambda c: c > 0, ">=": lambda c: c >= 0,
        }[op]
        return lambda scope: test(compare(left(scope), right(scope), op, pos))
    if op == "+":
        return lambda scope: add(left(scope), right(scope), pos)

    def numeric(fn: Callable[[Any, Any], Any]) -> Callable[[Scope], Any]:
        def run(scope: Scope) -> Any:
            a = require_number(left(scope), f"Operator {op}", pos)
            b = require_number(right(scope), f"Operator {op}", pos)
            return normalize(fn(a, b))
        return run

    if op == "-":
        return numeric(lambda a, b: a - b)
    if op == "*":
        return numeric(lambda a, b: a * b)
    if op == "/":
        def div(a: Any, b: Any) -> Any:
            if b == 0:
                raise ExprError("Division by zero", pos)
            return a / b
        return numeric(div)
    if op == "//":
        def floordiv(a: Any, b: Any) -> Any:
            if b == 0:
                raise ExprError("Division by zero", pos)
            return math.floor(a / b)
        return numeric(floordiv)
    if op == "%":
        def mod(a: Any, b: Any) -> Any:
            if b == 0:
                raise ExprError("Division by zero", pos)
            return a % b   # Python semantics: sign of the divisor
        return numeric(mod)
    if op == "**":
        def power(a: Any, b: Any) -> Any:
            try:
                r = a ** b
            except (OverflowError, ZeroDivisionError) as e:
                raise ExprError(f"Power error: {e}", pos) from None
            if isinstance(r, complex):
                raise ExprError("Power of a negative base with fractional exponent", pos)
            return r
        return numeric(power)
    raise ExprError(f"Unknown operator: {op}", pos)


def _compile_call(node: Call, env: Env) -> Callable[[Scope], Any]:
    fdef = env.function(node.name)
    if fdef is None:
        raise ExprError(f"Unknown function: {node.name}", node.pos)
    pos = node.pos
    arg_fns = [compile_node(a, env) for a in node.args]
    kw_fns = [(k, compile_node(v, env)) for k, v in node.kwargs]
    for k, _ in node.kwargs:
        if fdef.params and k not in fdef.params:
            raise ExprError(
                f"{fdef.name}() has no parameter {k!r}. Parameters: {', '.join(fdef.params)}", pos
            )
    fn = fdef.fn
    name = fdef.name

    if fdef.lazy:
        arg_nodes = list(node.args)
        kw_nodes = dict(node.kwargs)
        def call_lazy(scope: Scope) -> Any:
            ctx = Ctx(scope, env, pos)
            args = [Arg(n, f, scope) for n, f in zip(arg_nodes, arg_fns)]
            kwargs = {k: Arg(kw_nodes[k], f, scope) for k, f in kw_fns}
            try:
                return fn(ctx, *args, **kwargs)
            except ExprError:
                raise
            except TypeError as e:
                raise ExprError(f"{name}(): {e}", pos) from None
        return call_lazy

    def call(scope: Scope) -> Any:
        args = [f(scope) for f in arg_fns]
        kwargs = {k: f(scope) for k, f in kw_fns}
        try:
            return fn(*args, **kwargs)
        except ExprError as e:
            if e.pos is None:
                e.pos = pos
            raise
        except TypeError as e:
            raise ExprError(f"{name}(): {e}", pos) from None
        except (ValueError, ArithmeticError, IndexError, KeyError, AttributeError) as e:
            raise ExprError(f"{name}(): {e}", pos) from None
    return call


def _compile_index(node: Index, env: Env) -> Callable[[Scope], Any]:
    target = compile_node(node.target, env)
    key = compile_node(node.key, env)
    pos = node.pos

    def index(scope: Scope) -> Any:
        t = target(scope)
        k = key(scope)
        if is_blocked_key(k):
            raise ExprError(f"Access to {k!r} is not allowed", pos)
        if t is None:
            return None
        if isinstance(t, (list, tuple, str)):
            if not is_number(k) or (isinstance(k, float) and not k.is_integer()):
                raise ExprError(f"Index must be an integer, got {kind(k)}", pos)
            i = int(k)
            return t[i] if -len(t) <= i < len(t) else None
        if isinstance(t, dict):
            if is_number(k):
                k = str(normalize(k))
            return t.get(k) if isinstance(k, str) else None
        raise ExprError(f"Cannot index into {kind(t)}", pos)

    return index
