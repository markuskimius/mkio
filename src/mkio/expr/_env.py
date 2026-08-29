"""Function, library, and type registries; evaluation environments.

Functions live in *libraries* (``core``, ``math``, ... and any registered by
an application). An ``Env`` selects which libraries are visible and whether
name lookup is strict. The default environment sees every registered library
and is strict — that is what mkio's own config expressions use.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from ._errors import ExprError
from ._lexer import KEYWORDS

LANGUAGE_VERSION = "1"


@dataclass(frozen=True, slots=True)
class FunctionDef:
    name: str                        # upper-case
    fn: Callable[..., Any]
    library: str
    lazy: bool = False               # receives Arg thunks instead of values
    numeric: bool = False            # arguments are numeric (drives numeric_fields)
    params: tuple[str, ...] = ()     # positional parameter names (for named args / docs)
    doc: str = ""


@dataclass(frozen=True, slots=True)
class TypeDef:
    """Hooks that let operators understand a host value type."""
    name: str
    is_instance: Callable[[Any], bool]
    add: Callable[[Any, Any], Any] | None = None
    to_string: Callable[[Any], str] | None = None
    truthy: Callable[[Any], bool] | None = None
    compare: Callable[[Any, Any], int] | None = None   # -1 / 0 / 1
    concat: Callable[[Any, Any], Any] | None = None    # template joining; sides may be str


LIBRARIES: dict[str, dict[str, FunctionDef]] = {}
TYPES: dict[str, TypeDef] = {}

_STDLIB_ORDER = ("core", "math", "string", "format", "time", "collection")


def register_function(
    name: str,
    fn: Callable[..., Any],
    *,
    lazy: bool = False,
    numeric: bool = False,
    params: tuple[str, ...] | list[str] = (),
    doc: str = "",
    library: str = "user",
) -> FunctionDef:
    """Register a function under ``library`` (default ``"user"``).

    The name is case-insensitive. Eager functions are called with evaluated
    positional and named arguments. Lazy functions are called as
    ``fn(ctx, *args, **kwargs)`` where each arg is an :class:`Arg` thunk.
    """
    upper = name.upper()
    if upper in KEYWORDS:
        raise ExprError(f"Cannot register function with reserved name: {name}")
    if not upper.replace("_", "").isalnum() or upper[0].isdigit():
        raise ExprError(f"Invalid function name: {name!r}")
    for lib, fns in LIBRARIES.items():
        if lib != library and upper in fns:
            raise ExprError(f"Function {upper} already registered in library {lib!r}")
    fdef = FunctionDef(upper, fn, library, lazy, numeric, tuple(params), doc)
    LIBRARIES.setdefault(library, {})[upper] = fdef
    return fdef


def register_library(name: str, functions: dict[str, Any]) -> None:
    """Register a bundle of functions.

    Each value is either a callable or ``(callable, {metadata})`` where the
    metadata keys are the keyword arguments of :func:`register_function`.
    """
    for fname, spec in functions.items():
        if isinstance(spec, tuple):
            fn, meta = spec
            register_function(fname, fn, library=name, **meta)
        else:
            register_function(fname, spec, library=name)


def unregister_library(name: str) -> None:
    LIBRARIES.pop(name, None)


def register_type(
    name: str,
    is_instance: Callable[[Any], bool],
    *,
    add: Callable[[Any, Any], Any] | None = None,
    to_string: Callable[[Any], str] | None = None,
    truthy: Callable[[Any], bool] | None = None,
    compare: Callable[[Any, Any], int] | None = None,
    concat: Callable[[Any, Any], Any] | None = None,
) -> TypeDef:
    """Teach the operators about a host value type.

    ``concat`` lets a type survive ``"text ${x}"`` template joining (either
    side may be a str); without it the type is rendered via ``to_string``.
    """
    tdef = TypeDef(name, is_instance, add, to_string, truthy, compare, concat)
    TYPES[name] = tdef
    return tdef


def find_type(value: Any) -> TypeDef | None:
    for t in TYPES.values():
        if t.is_instance(value):
            return t
    return None


class Env:
    """An evaluation environment: visible libraries + lookup mode.

    ``libraries=None`` means every library registered at the time a function
    is looked up (i.e. compile time), which is what most hosts want.
    ``strict=True`` makes an unknown root name an error; ``False`` yields NULL.
    Missing map keys and out-of-range indexes yield NULL in both modes.
    """

    __slots__ = ("libraries", "strict")

    def __init__(self, libraries: list[str] | tuple[str, ...] | None = None, *, strict: bool = True) -> None:
        self.libraries = tuple(libraries) if libraries is not None else None
        self.strict = strict

    def function(self, name: str) -> FunctionDef | None:
        upper = name.upper()
        libs = self.libraries if self.libraries is not None else LIBRARIES.keys()
        for lib in libs:
            fns = LIBRARIES.get(lib)
            if fns and upper in fns:
                return fns[upper]
        return None

    def functions(self) -> dict[str, FunctionDef]:
        out: dict[str, FunctionDef] = {}
        libs = self.libraries if self.libraries is not None else list(LIBRARIES.keys())
        for lib in libs:
            out.update(LIBRARIES.get(lib, {}))
        return out


default_env = Env()
