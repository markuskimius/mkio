"""String templates: ``"text ${expr} text"``.

A template that is exactly one ``${...}`` yields the expression's raw value;
any other template yields a string (NULL renders as ''). ``$${`` is a
literal ``${``.
"""

from __future__ import annotations

from typing import Any, Callable

from ._ast import Node
from ._errors import ExprError


def split_template(template: str) -> list[tuple[str, str]]:
    """Split into [('text', str) | ('expr', source)] parts."""
    parts: list[tuple[str, str]] = []
    buf: list[str] = []
    i = 0
    n = len(template)
    while i < n:
        if template.startswith("$${", i):
            buf.append("${")
            i += 3
            continue
        if template.startswith("${", i):
            j = i + 2
            depth = 1
            quote: str | None = None
            while j < n:
                c = template[j]
                if quote:
                    if c == "\\":
                        j += 2
                        continue
                    if c == quote:
                        quote = None
                elif c in "'\"":
                    quote = c
                elif c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            if j >= n:
                raise ExprError("Unterminated ${...} in template", i)
            if buf:
                parts.append(("text", "".join(buf)))
                buf = []
            parts.append(("expr", template[i + 2:j]))
            i = j + 1
            continue
        buf.append(template[i])
        i += 1
    if buf:
        parts.append(("text", "".join(buf)))
    return parts


def has_expressions(template: str) -> bool:
    return "${" in template
