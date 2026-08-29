"""Expression language error type."""

from __future__ import annotations


class ExprError(Exception):
    """Raised for lexing, parsing, compilation, and evaluation failures.

    ``pos`` is the character offset into the source expression when known.
    """

    def __init__(self, message: str, pos: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.pos = pos

    def __str__(self) -> str:
        if self.pos is None:
            return self.message
        return f"{self.message} (at position {self.pos})"
