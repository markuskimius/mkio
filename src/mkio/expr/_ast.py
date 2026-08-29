"""AST node types. Every node carries ``pos`` — the source offset of its start."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Literal:
    value: Any
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Name:
    name: str
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Binary:
    op: str
    left: Node
    right: Node
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Unary:
    op: str
    operand: Node
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Call:
    name: str                      # upper-cased function name
    args: tuple[Node, ...]
    kwargs: tuple[tuple[str, Node], ...] = ()
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Lambda:
    params: tuple[str, ...]
    body: Node
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Apply:
    """``value |> (x -> body)`` — apply a lambda to one argument."""
    fn: Node
    arg: Node
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Array:
    elements: tuple[Node, ...]
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Map:
    keys: tuple[str, ...]
    values: tuple[Node, ...]
    pos: int = 0


@dataclass(frozen=True, slots=True)
class Index:
    target: Node
    key: Node
    pos: int = 0


Node = Literal | Name | Binary | Unary | Call | Lambda | Apply | Array | Map | Index
