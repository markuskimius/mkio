"""Recursive-descent parser.

Precedence, lowest to highest:
  |>  (right side must be a parenthesized lambda)
  ||
  &&
  == != < <= > >=   (non-chaining)
  ??
  + -
  * / // %
  unary - !
  **  (right-associative)
  postfix  .name  [expr]  F(args)
  primary  literal  [..]  {..}  (expr)  lambda
"""

from __future__ import annotations

from ._ast import (
    Apply, Array, Binary, Call, Index, Lambda, Literal, Map, Name, Node, Unary,
)
from ._errors import ExprError
from ._lexer import Token, tokenize

_TOKEN_NAMES = {
    "RPAREN": "')'", "RBRACKET": "']'", "RBRACE": "'}'", "COLON": "':'",
    "COMMA": "','", "IDENT": "a name",
}
_COMPARE = frozenset({"==", "!=", "<", "<=", ">", ">="})


class _Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.pos = 0

    # -- helpers -------------------------------------------------------------

    def peek(self, k: int = 0) -> Token:
        i = self.pos + k
        return self.tokens[i] if i < len(self.tokens) else self.tokens[-1]

    def advance(self) -> Token:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def at(self, type_: str, value: str | None = None) -> bool:
        tok = self.peek()
        return tok.type == type_ and (value is None or tok.value == value)

    def expect(self, type_: str, value: str | None = None) -> Token:
        tok = self.peek()
        if tok.type != type_ or (value is not None and tok.value != value):
            want = value if value else _TOKEN_NAMES.get(type_, type_.lower())
            got = "end of expression" if tok.type == "EOF" else repr(tok.value)
            raise ExprError(f"Expected {want}, got {got}", tok.pos)
        return self.advance()

    # -- grammar -------------------------------------------------------------

    def parse(self) -> Node:
        node = self.parse_expr()
        if not self.at("EOF"):
            tok = self.peek()
            raise ExprError(f"Unexpected token: {tok.value!r}", tok.pos)
        return node

    def parse_expr(self) -> Node:
        """Full expression: lambda literal, or pipe chain."""
        lam = self.try_lambda()
        if lam is not None:
            return lam
        left = self.parse_or()
        while self.at("PIPE"):
            pipe = self.advance()
            if not self.at("LPAREN"):
                raise ExprError(
                    "Right side of |> must be a parenthesized lambda: (x -> ...)", self.peek().pos
                )
            start = self.peek().pos
            right = self.parse_postfix(self.parse_primary())
            if not isinstance(right, Lambda):
                raise ExprError(
                    "Right side of |> must be a parenthesized lambda: (x -> ...)", start
                )
            left = Apply(right, left, pipe.pos)
        return left

    def try_lambda(self) -> Node | None:
        """``x -> body`` or ``(a, b) -> body`` at the current position, else None."""
        tok = self.peek()
        if tok.type == "IDENT" and self.peek(1).type == "ARROW":
            self.advance()
            self.advance()
            body = self.parse_expr()
            return Lambda((tok.value,), body, tok.pos)
        if tok.type == "LPAREN":
            # Look ahead for "( IDENT [, IDENT]* ) ->"
            k = 1
            params: list[str] = []
            while True:
                t = self.peek(k)
                if t.type != "IDENT":
                    return None
                params.append(t.value)
                k += 1
                t = self.peek(k)
                if t.type == "COMMA":
                    k += 1
                    continue
                if t.type == "RPAREN":
                    k += 1
                    break
                return None
            if self.peek(k).type != "ARROW":
                return None
            self.pos += k + 1  # params, ')' and '->'
            body = self.parse_expr()
            return Lambda(tuple(params), body, tok.pos)
        return None

    def parse_or(self) -> Node:
        left = self.parse_and()
        while self.at("OP", "||"):
            pos = self.advance().pos
            left = Binary("||", left, self.parse_and(), pos)
        return left

    def parse_and(self) -> Node:
        left = self.parse_comparison()
        while self.at("OP", "&&"):
            pos = self.advance().pos
            left = Binary("&&", left, self.parse_comparison(), pos)
        return left

    def parse_comparison(self) -> Node:
        left = self.parse_coalesce()
        tok = self.peek()
        if tok.type == "OP" and tok.value in _COMPARE:
            self.advance()
            right = self.parse_coalesce()
            nxt = self.peek()
            if nxt.type == "OP" and nxt.value in _COMPARE:
                raise ExprError(
                    "Comparisons don't chain — use && to combine them", nxt.pos
                )
            return Binary(tok.value, left, right, tok.pos)
        return left

    def parse_coalesce(self) -> Node:
        left = self.parse_additive()
        while self.at("OP", "??"):
            pos = self.advance().pos
            left = Binary("??", left, self.parse_additive(), pos)
        return left

    def parse_additive(self) -> Node:
        left = self.parse_multiplicative()
        while self.peek().type == "OP" and self.peek().value in ("+", "-"):
            tok = self.advance()
            left = Binary(tok.value, left, self.parse_multiplicative(), tok.pos)
        return left

    def parse_multiplicative(self) -> Node:
        left = self.parse_unary()
        while self.peek().type == "OP" and self.peek().value in ("*", "/", "//", "%"):
            tok = self.advance()
            left = Binary(tok.value, left, self.parse_unary(), tok.pos)
        return left

    def parse_unary(self) -> Node:
        tok = self.peek()
        if tok.type == "OP" and tok.value in ("-", "!"):
            self.advance()
            return Unary(tok.value, self.parse_unary(), tok.pos)
        return self.parse_power()

    def parse_power(self) -> Node:
        base = self.parse_postfix(self.parse_primary())
        if self.at("OP", "**"):
            pos = self.advance().pos
            return Binary("**", base, self.parse_unary(), pos)
        return base

    def parse_postfix(self, node: Node) -> Node:
        while True:
            tok = self.peek()
            if tok.type == "LBRACKET":
                self.advance()
                key = self.parse_expr()
                self.expect("RBRACKET")
                node = Index(node, key, tok.pos)
            elif tok.type == "DOT":
                self.advance()
                t = self.peek()
                if t.type in ("IDENT", "KEYWORD"):
                    self.advance()
                    node = Index(node, Literal(t.value, t.pos), tok.pos)
                elif t.type == "NUMBER":
                    self.advance()
                    node = Index(node, Literal(_number(t.value), t.pos), tok.pos)
                else:
                    raise ExprError("Expected a name after '.'", t.pos)
            else:
                return node

    def parse_primary(self) -> Node:
        tok = self.peek()

        if tok.type == "LPAREN":
            lam = self.try_lambda()
            if lam is not None:
                return lam
            self.advance()
            node = self.parse_expr()
            self.expect("RPAREN")
            return node

        if tok.type == "LBRACKET":
            self.advance()
            elements: list[Node] = []
            while not self.at("RBRACKET"):
                elements.append(self.parse_expr())
                if not self.at("COMMA"):
                    break
                self.advance()
            self.expect("RBRACKET")
            return Array(tuple(elements), tok.pos)

        if tok.type == "LBRACE":
            self.advance()
            keys: list[str] = []
            values: list[Node] = []
            while not self.at("RBRACE"):
                kt = self.peek()
                if kt.type in ("IDENT", "STRING", "KEYWORD"):
                    self.advance()
                    keys.append(kt.value)
                else:
                    raise ExprError("Map key must be a name or string", kt.pos)
                self.expect("COLON")
                values.append(self.parse_expr())
                if not self.at("COMMA"):
                    break
                self.advance()
            self.expect("RBRACE")
            return Map(tuple(keys), tuple(values), tok.pos)

        if tok.type == "STRING":
            self.advance()
            return Literal(tok.value, tok.pos)

        if tok.type == "NUMBER":
            self.advance()
            return Literal(_number(tok.value), tok.pos)

        if tok.type == "KEYWORD":
            self.advance()
            return Literal({"TRUE": True, "FALSE": False, "NULL": None}[tok.value.upper()], tok.pos)

        if tok.type == "IDENT":
            self.advance()
            if self.at("LPAREN"):
                self.advance()
                args: list[Node] = []
                kwargs: list[tuple[str, Node]] = []
                while not self.at("RPAREN"):
                    if self.peek().type == "IDENT" and self.peek(1).type == "COLON":
                        kt = self.advance()
                        self.advance()
                        kwargs.append((kt.value, self.parse_expr()))
                    else:
                        if kwargs:
                            raise ExprError(
                                "Positional argument after named argument", self.peek().pos
                            )
                        args.append(self.parse_expr())
                    if not self.at("COMMA"):
                        break
                    self.advance()
                self.expect("RPAREN")
                return Call(tok.value.upper(), tuple(args), tuple(kwargs), tok.pos)
            return Name(tok.value, tok.pos)

        if tok.type == "ARROW":
            raise ExprError("Lambda parameters must be names: x -> ... or (a, b) -> ...", tok.pos)
        if tok.type == "EOF":
            raise ExprError("Unexpected end of expression", tok.pos)
        raise ExprError(f"Unexpected token: {tok.value!r}", tok.pos)


def _number(text: str) -> int | float:
    if "." in text or "e" in text or "E" in text:
        return float(text)
    return int(text)


def parse(expr: str) -> Node:
    """Parse an expression string into an AST."""
    if not isinstance(expr, str):
        raise ExprError(f"Expression must be a string, got {type(expr).__name__}")
    return _Parser(tokenize(expr)).parse()
