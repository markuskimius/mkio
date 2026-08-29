"""mkio expression language.

One grammar, two implementations (this module and ``client/mkio-expr.js``),
kept in lock-step by ``tests/expr_cases.json``.

    from mkio import expr
    f = expr.compile("qty * price > 1000 && status == 'open'")
    f({"qty": 5, "price": 300, "status": "open"})      # True

    t = expr.compile_template("Order #${id}: ${NUM(qty * price, digits: 2)}")
    t({"id": 7, "qty": 2, "price": 1.5})                # 'Order #7: 3.00'
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from ._ast import (
    Apply, Array, Binary, Call, Index, Lambda, Literal, Map, Name, Node, Unary,
)
from ._analysis import field_refs, function_refs, numeric_fields
from ._env import (
    LANGUAGE_VERSION, LIBRARIES, TYPES, Env, FunctionDef, TypeDef, default_env,
    find_type, register_function, register_library, register_type, unregister_library,
)
from ._errors import ExprError
from ._eval import Arg, Closure, Ctx, Scope, compile_node
from ._lexer import Token, tokenize
from ._parser import parse
from ._template import has_expressions, split_template
from ._values import equals, kind, to_string, truthy

# Standard libraries register themselves on import.
from . import _lib_core, _lib_math, _lib_string, _lib_format, _lib_time, _lib_collection  # noqa: E402,F401


class Compiled:
    """A compiled expression: call it with a mapping of root names."""

    __slots__ = ("source", "ast", "env", "_fn")

    def __init__(self, source: str, ast: Node, env: Env, fn: Callable[[Scope], Any]) -> None:
        self.source = source
        self.ast = ast
        self.env = env
        self._fn = fn

    def __call__(self, scope: Mapping[str, Any] | None = None) -> Any:
        return self._fn(Scope(dict(scope) if scope is not None and not isinstance(scope, dict) else (scope or {}), None, self.env.strict))

    def evaluate(self, scope: Scope) -> Any:
        return self._fn(scope)

    @property
    def field_refs(self) -> set[str]:
        return field_refs(self.ast)

    def __repr__(self) -> str:
        return f"<expr {self.source!r}>"


def compile(expr: str, env: Env | None = None) -> Compiled:  # noqa: A001
    """Parse and compile an expression against ``env`` (default: strict, all libraries)."""
    env = env or default_env
    ast = parse(expr)
    return Compiled(expr, ast, env, compile_node(ast, env))


compile_expression = compile


def compile_filter(expr: str, env: Env | None = None) -> Callable[[Mapping[str, Any]], bool]:
    """Compile a predicate: the result is coerced with truthiness rules."""
    c = compile(expr, env)
    def predicate(scope: Mapping[str, Any]) -> bool:
        return truthy(c(scope))
    predicate.compiled = c  # type: ignore[attr-defined]
    return predicate


def compile_formatter(fields: Mapping[str, str], env: Env | None = None) -> Callable[[Mapping[str, Any]], dict[str, Any]]:
    """Compile ``{name: expression}`` into a row transformer."""
    compiled = [(name, compile(src, env)) for name, src in fields.items()]
    def formatter(scope: Mapping[str, Any]) -> dict[str, Any]:
        s = Scope(dict(scope), None, (env or default_env).strict)
        return {name: c.evaluate(s) for name, c in compiled}
    formatter.compiled = dict(compiled)  # type: ignore[attr-defined]
    return formatter


class CompiledTemplate:
    __slots__ = ("source", "parts", "env")

    def __init__(self, source: str, parts: list[tuple[str, Any]], env: Env) -> None:
        self.source = source
        self.parts = parts   # ('text', str) | ('expr', Compiled)
        self.env = env

    def __call__(self, scope: Mapping[str, Any] | None = None) -> Any:
        return self.evaluate(Scope(dict(scope or {}), None, self.env.strict))

    def evaluate(self, s: Scope) -> Any:
        """Evaluate against an existing Scope (hosts that build scope chains).

        Mixed templates concatenate as strings — unless a part is a host
        type with a ``concat`` hook (rich text, say), in which case the
        result is built through it so the host value survives.
        """
        if len(self.parts) == 1 and self.parts[0][0] == "expr":
            return self.parts[0][1].evaluate(s)
        out: Any = ""
        joiner = None
        for kind_, p in self.parts:
            v = p if kind_ == "text" else p.evaluate(s)
            t = None if isinstance(v, str) else find_type(v)
            if t is not None and t.concat is not None:
                joiner = t.concat
                out = v if out == "" else joiner(out, v)
            elif joiner is not None:
                out = joiner(out, to_string(v))
            else:
                out += to_string(v)
        return out

    @property
    def field_refs(self) -> set[str]:
        refs: set[str] = set()
        for kind_, p in self.parts:
            if kind_ == "expr":
                refs |= p.field_refs
        return refs

    @property
    def is_pure(self) -> bool:
        return len(self.parts) == 1 and self.parts[0][0] == "expr"


def compile_template(template: str, env: Env | None = None) -> CompiledTemplate:
    """Compile ``"text ${expr}"``. A plain string (no ``${``) returns itself."""
    env = env or default_env
    parts: list[tuple[str, Any]] = []
    for kind_, src in split_template(template):
        parts.append((kind_, compile(src, env) if kind_ == "expr" else src))
    if not parts:
        parts.append(("text", ""))
    return CompiledTemplate(template, parts, env)


def evaluate(expr: str | Node, scope: Mapping[str, Any] | None = None, env: Env | None = None) -> Any:
    """One-shot: parse (if needed), compile, evaluate."""
    if isinstance(expr, str):
        return compile(expr, env)(scope)
    env = env or default_env
    return compile_node(expr, env)(Scope(dict(scope or {}), None, env.strict))


__all__ = [
    "LANGUAGE_VERSION", "ExprError", "Env", "default_env", "Compiled", "CompiledTemplate",
    "compile", "compile_expression", "compile_filter", "compile_formatter", "compile_template",
    "evaluate", "parse", "tokenize", "Token",
    "register_function", "register_library", "register_type", "unregister_library",
    "FunctionDef", "TypeDef", "LIBRARIES", "TYPES", "find_type",
    "field_refs", "function_refs", "numeric_fields",
    "Scope", "Closure", "Arg", "Ctx", "compile_node",
    "to_string", "truthy", "equals", "kind", "split_template", "has_expressions",
    "Node", "Literal", "Name", "Binary", "Unary", "Call", "Lambda", "Apply", "Array", "Map", "Index",
]
