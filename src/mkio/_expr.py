"""Expression language: tokenizer, parser, evaluator, and function registry.

Supports: comparisons, logical ops, arithmetic, string ops, null checks,
extensible function calls, array/map literals, index/dot access, and LET
bindings. Safe for client-submitted expressions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

# ---------------------------------------------------------------------------
# AST nodes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Literal:
    value: str | int | float | bool | None

@dataclass(frozen=True, slots=True)
class FieldRef:
    name: str

@dataclass(frozen=True, slots=True)
class BinOp:
    op: str
    left: Node
    right: Node

@dataclass(frozen=True, slots=True)
class UnaryOp:
    op: str
    operand: Node

@dataclass(frozen=True, slots=True)
class FuncCall:
    name: str
    args: tuple[Node, ...]

@dataclass(frozen=True, slots=True)
class ArrayLiteral:
    elements: tuple[Node, ...]

@dataclass(frozen=True, slots=True)
class MapLiteral:
    keys: tuple[str, ...]
    values: tuple[Node, ...]

@dataclass(frozen=True, slots=True)
class Index:
    target: Node
    key: Node

@dataclass(frozen=True, slots=True)
class Let:
    bindings: tuple[tuple[str, Node], ...]
    body: Node

Node = (Literal | FieldRef | BinOp | UnaryOp | FuncCall
        | ArrayLiteral | MapLiteral | Index | Let)


class ExprError(Exception):
    pass


# ---------------------------------------------------------------------------
# Function registry
# ---------------------------------------------------------------------------

FUNCTIONS: dict[str, Callable[..., Any]] = {
    "UPPER": lambda s: s.upper() if isinstance(s, str) else s,
    "LOWER": lambda s: s.lower() if isinstance(s, str) else s,
    "ROUND": lambda x, n=0: round(x, int(n)),
    "ABS": lambda x: abs(x),
    "COALESCE": lambda *args: next((a for a in args if a is not None), None),
    "LEN": lambda x: len(x),
    "KEYS": lambda m: list(m.keys()),
    "VALUES": lambda m: list(m.values()),
    "FLATTEN": lambda arr: [x for sub in arr for x in (sub if isinstance(sub, list) else [sub])],
    "MERGE": lambda *maps: {k: v for m in maps for k, v in m.items()},
}

_KEYWORDS = frozenset({
    "AND", "OR", "NOT", "IS", "NULL", "IN", "CONTAINS", "STARTS_WITH",
    "TRUE", "FALSE", "LET",
})

_SPECIAL_FORMS = frozenset({"IF"})


def register_function(name: str, fn: Callable[..., Any]) -> None:
    """Add a custom function to the expression language."""
    upper = name.upper()
    if upper in _KEYWORDS or upper in _SPECIAL_FORMS:
        raise ExprError(f"Cannot register function with reserved keyword name: {name}")
    FUNCTIONS[upper] = fn


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Token:
    type: str
    value: str

_OPERATORS = frozenset({"==", "!=", ">=", "<=", ">", "<", "+", "-", "*", "/"})


def tokenize(expr: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(expr)
    while i < n:
        c = expr[i]

        # Whitespace
        if c in " \t\r\n":
            i += 1
            continue

        # String literal
        if c == "'":
            i += 1
            start = i
            while i < n and expr[i] != "'":
                if expr[i] == "\\" and i + 1 < n:
                    i += 1  # skip escaped char
                i += 1
            if i >= n:
                raise ExprError("Unterminated string literal")
            val = expr[start:i].replace("\\'", "'")
            tokens.append(Token("STRING", val))
            i += 1
            continue

        # DOT (context-dependent: postfix position → DOT, otherwise fall through to number)
        if c == ".":
            prev_type = tokens[-1].type if tokens else None
            if prev_type in ("IDENT", "RPAREN", "RBRACKET", "RBRACE", "NUMBER"):
                tokens.append(Token("DOT", "."))
                i += 1
                continue

        # Number
        if c.isdigit() or (c == "." and i + 1 < n and expr[i + 1].isdigit()):
            start = i
            has_dot = c == "."
            i += 1
            while i < n and (expr[i].isdigit() or (expr[i] == "." and not has_dot)):
                if expr[i] == ".":
                    has_dot = True
                i += 1
            tokens.append(Token("NUMBER", expr[start:i]))
            continue

        # Two-char operators
        if i + 1 < n and expr[i:i + 2] in _OPERATORS:
            tokens.append(Token("OP", expr[i:i + 2]))
            i += 2
            continue

        # Single-char operators
        if c in "><=+-*/":
            tokens.append(Token("OP", c))
            i += 1
            continue

        # Parens, brackets, braces, comma, colon
        if c == "(":
            tokens.append(Token("LPAREN", "("))
            i += 1
            continue
        if c == ")":
            tokens.append(Token("RPAREN", ")"))
            i += 1
            continue
        if c == "[":
            tokens.append(Token("LBRACKET", "["))
            i += 1
            continue
        if c == "]":
            tokens.append(Token("RBRACKET", "]"))
            i += 1
            continue
        if c == "{":
            tokens.append(Token("LBRACE", "{"))
            i += 1
            continue
        if c == "}":
            tokens.append(Token("RBRACE", "}"))
            i += 1
            continue
        if c == ",":
            tokens.append(Token("COMMA", ","))
            i += 1
            continue
        if c == ":":
            tokens.append(Token("COLON", ":"))
            i += 1
            continue

        # Identifiers and keywords
        if c.isalpha() or c == "_":
            start = i
            while i < n and (expr[i].isalnum() or expr[i] == "_"):
                i += 1
            tokens.append(Token("IDENT", expr[start:i]))
            continue

        raise ExprError(f"Unexpected character: {c!r} at position {i}")

    tokens.append(Token("EOF", ""))
    return tokens


# ---------------------------------------------------------------------------
# Parser (recursive descent)
# ---------------------------------------------------------------------------

class _Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

    def advance(self) -> Token:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def expect(self, type_: str, value: str | None = None) -> Token:
        tok = self.advance()
        if tok.type != type_ or (value is not None and tok.value != value):
            expected = f"{type_}({value})" if value else type_
            raise ExprError(f"Expected {expected}, got {tok.type}({tok.value!r})")
        return tok

    def match_ident(self, *values: str) -> bool:
        tok = self.peek()
        return tok.type == "IDENT" and tok.value.upper() in values

    def parse(self) -> Node:
        node = self.parse_or()
        if self.peek().type != "EOF":
            raise ExprError(f"Unexpected token: {self.peek().value!r}")
        return node

    def parse_or(self) -> Node:
        left = self.parse_and()
        while self.match_ident("OR"):
            self.advance()
            right = self.parse_and()
            left = BinOp("OR", left, right)
        return left

    def parse_and(self) -> Node:
        left = self.parse_not()
        while self.match_ident("AND"):
            self.advance()
            right = self.parse_not()
            left = BinOp("AND", left, right)
        return left

    def parse_not(self) -> Node:
        if self.match_ident("NOT"):
            self.advance()
            operand = self.parse_not()
            return UnaryOp("NOT", operand)
        return self.parse_comparison()

    def parse_comparison(self) -> Node:
        left = self.parse_addition()

        tok = self.peek()

        # IS NULL / IS NOT NULL
        if self.match_ident("IS"):
            self.advance()
            if self.match_ident("NOT"):
                self.advance()
                self.expect("IDENT", "NULL")
                return UnaryOp("IS NOT NULL", left)
            self.expect("IDENT", "NULL")
            return UnaryOp("IS NULL", left)

        # CONTAINS / STARTS_WITH / IN
        if self.match_ident("CONTAINS"):
            self.advance()
            right = self.parse_addition()
            return BinOp("CONTAINS", left, right)
        if self.match_ident("STARTS_WITH"):
            self.advance()
            right = self.parse_addition()
            return BinOp("STARTS_WITH", left, right)
        if self.match_ident("IN"):
            self.advance()
            right = self.parse_addition()
            return BinOp("IN", left, right)

        # Standard comparisons
        if tok.type == "OP" and tok.value in ("==", "!=", ">", "<", ">=", "<="):
            self.advance()
            right = self.parse_addition()
            return BinOp(tok.value, left, right)

        return left

    def parse_addition(self) -> Node:
        left = self.parse_multiplication()
        while self.peek().type == "OP" and self.peek().value in ("+", "-"):
            op = self.advance().value
            right = self.parse_multiplication()
            left = BinOp(op, left, right)
        return left

    def parse_multiplication(self) -> Node:
        left = self.parse_unary_minus()
        while self.peek().type == "OP" and self.peek().value in ("*", "/"):
            op = self.advance().value
            right = self.parse_unary_minus()
            left = BinOp(op, left, right)
        return left

    def parse_unary_minus(self) -> Node:
        if self.peek().type == "OP" and self.peek().value == "-":
            self.advance()
            operand = self.parse_unary_minus()
            return UnaryOp("-", operand)
        return self.parse_postfix(self.parse_primary())

    def parse_postfix(self, node: Node) -> Node:
        while True:
            if self.peek().type == "LBRACKET":
                self.advance()
                key = self.parse_or()
                self.expect("RBRACKET")
                node = Index(node, key)
            elif self.peek().type == "DOT":
                self.advance()
                tok = self.peek()
                if tok.type == "IDENT":
                    self.advance()
                    node = Index(node, Literal(tok.value))
                elif tok.type == "NUMBER":
                    self.advance()
                    val = float(tok.value) if "." in tok.value else int(tok.value)
                    node = Index(node, Literal(val))
                else:
                    raise ExprError(
                        f"Expected identifier or number after '.', got {tok.type}({tok.value!r})"
                    )
            else:
                break
        return node

    def parse_primary(self) -> Node:
        tok = self.peek()

        # Parenthesized expression
        if tok.type == "LPAREN":
            self.advance()
            node = self.parse_or()
            self.expect("RPAREN")
            return node

        # Array literal
        if tok.type == "LBRACKET":
            self.advance()
            elements: list[Node] = []
            if self.peek().type != "RBRACKET":
                elements.append(self.parse_or())
                while self.peek().type == "COMMA":
                    self.advance()
                    elements.append(self.parse_or())
            self.expect("RBRACKET")
            return ArrayLiteral(tuple(elements))

        # Map literal
        if tok.type == "LBRACE":
            self.advance()
            keys: list[str] = []
            values: list[Node] = []
            if self.peek().type != "RBRACE":
                k, v = self._parse_map_entry()
                keys.append(k)
                values.append(v)
                while self.peek().type == "COMMA":
                    self.advance()
                    k, v = self._parse_map_entry()
                    keys.append(k)
                    values.append(v)
            self.expect("RBRACE")
            return MapLiteral(tuple(keys), tuple(values))

        # String literal
        if tok.type == "STRING":
            self.advance()
            return Literal(tok.value)

        # Number literal
        if tok.type == "NUMBER":
            self.advance()
            if "." in tok.value:
                return Literal(float(tok.value))
            return Literal(int(tok.value))

        # Keywords: true, false, null, let
        if tok.type == "IDENT":
            upper = tok.value.upper()
            if upper == "TRUE":
                self.advance()
                return Literal(True)
            if upper == "FALSE":
                self.advance()
                return Literal(False)
            if upper == "NULL":
                self.advance()
                return Literal(None)
            if upper == "LET":
                return self._parse_let()

            # Function call or field reference
            self.advance()
            if self.peek().type == "LPAREN":
                # Function call
                fname = upper
                if fname not in FUNCTIONS and fname not in _SPECIAL_FORMS:
                    raise ExprError(f"Unknown function: {tok.value}")
                self.advance()  # consume (
                args: list[Node] = []
                if self.peek().type != "RPAREN":
                    args.append(self.parse_or())
                    while self.peek().type == "COMMA":
                        self.advance()
                        args.append(self.parse_or())
                self.expect("RPAREN")
                return FuncCall(fname, tuple(args))
            else:
                # Field reference
                return FieldRef(tok.value)

        raise ExprError(f"Unexpected token: {tok.value!r}")

    def _parse_map_entry(self) -> tuple[str, Node]:
        tok = self.peek()
        if tok.type == "IDENT":
            key = tok.value
            self.advance()
        elif tok.type == "STRING":
            key = tok.value
            self.advance()
        else:
            raise ExprError(
                f"Map key must be identifier or string, got {tok.type}({tok.value!r})"
            )
        self.expect("COLON")
        value = self.parse_or()
        return key, value

    def _parse_let(self) -> Let:
        self.advance()  # consume LET
        bindings: list[tuple[str, Node]] = []
        name_tok = self.expect("IDENT")
        self.expect("OP", "=")
        expr = self.parse_addition()
        bindings.append((name_tok.value, expr))
        while self.peek().type == "COMMA":
            self.advance()
            name_tok = self.expect("IDENT")
            self.expect("OP", "=")
            expr = self.parse_addition()
            bindings.append((name_tok.value, expr))
        if not self.match_ident("IN"):
            raise ExprError("Expected 'IN' after LET bindings")
        self.advance()  # consume IN
        body = self.parse_or()
        return Let(tuple(bindings), body)


def parse(expr: str) -> Node:
    """Parse an expression string into an AST."""
    tokens = tokenize(expr)
    return _Parser(tokens).parse()


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

def evaluate(node: Node, row: dict[str, Any]) -> Any:
    """Evaluate an AST node against a row dict."""
    if isinstance(node, Literal):
        return node.value

    if isinstance(node, FieldRef):
        try:
            return row[node.name]
        except KeyError:
            available = ", ".join(sorted(row.keys()))
            raise ExprError(
                f"Unknown field: {node.name!r}. Available fields: {available}"
            )

    if isinstance(node, UnaryOp):
        if node.op == "NOT":
            return not evaluate(node.operand, row)
        if node.op == "-":
            return -evaluate(node.operand, row)
        if node.op == "IS NULL":
            return evaluate(node.operand, row) is None
        if node.op == "IS NOT NULL":
            return evaluate(node.operand, row) is not None
        raise ExprError(f"Unknown unary op: {node.op}")

    if isinstance(node, BinOp):
        left = evaluate(node.left, row)
        right = evaluate(node.right, row)

        if node.op == "AND":
            return bool(left and right)
        if node.op == "OR":
            return bool(left or right)
        if node.op == "==":
            return left == right
        if node.op == "!=":
            return left != right
        if node.op == ">":
            return left > right
        if node.op == "<":
            return left < right
        if node.op == ">=":
            return left >= right
        if node.op == "<=":
            return left <= right
        if node.op == "+":
            return left + right
        if node.op == "-":
            return left - right
        if node.op == "*":
            return left * right
        if node.op == "/":
            if right == 0:
                raise ExprError("Division by zero")
            return left / right
        if node.op == "CONTAINS":
            return right in left if isinstance(left, str) else False
        if node.op == "STARTS_WITH":
            return left.startswith(right) if isinstance(left, str) else False
        if node.op == "IN":
            return left in right if isinstance(right, (list, tuple, set)) else False
        raise ExprError(f"Unknown binary op: {node.op}")

    if isinstance(node, FuncCall):
        if node.name == "IF":
            if len(node.args) != 3:
                raise ExprError("IF() requires exactly 3 arguments")
            cond = evaluate(node.args[0], row)
            return evaluate(node.args[1], row) if cond else evaluate(node.args[2], row)
        fn = FUNCTIONS.get(node.name)
        if fn is None:
            raise ExprError(f"Unknown function: {node.name}")
        args = [evaluate(arg, row) for arg in node.args]
        return fn(*args)

    if isinstance(node, ArrayLiteral):
        return [evaluate(el, row) for el in node.elements]

    if isinstance(node, MapLiteral):
        return {k: evaluate(v, row) for k, v in zip(node.keys, node.values)}

    if isinstance(node, Index):
        target = evaluate(node.target, row)
        key = evaluate(node.key, row)
        if isinstance(key, str) and key.startswith("__"):
            raise ExprError(f"Access to dunder attributes is not allowed: {key!r}")
        if target is None:
            raise ExprError("Cannot index into NULL")
        if isinstance(target, (list, tuple)):
            try:
                return target[int(key)]
            except (IndexError, ValueError) as e:
                raise ExprError(f"Index error: {e}") from None
        if isinstance(target, dict):
            try:
                return target[key]
            except KeyError:
                available = ", ".join(sorted(str(k) for k in target.keys()))
                raise ExprError(
                    f"Unknown key: {key!r}. Available keys: {available}"
                ) from None
        raise ExprError(f"Cannot index into {type(target).__name__}")

    if isinstance(node, Let):
        scope = dict(row)
        for name, expr in node.bindings:
            scope[name] = evaluate(expr, scope)
        return evaluate(node.body, scope)

    raise ExprError(f"Unknown node type: {type(node)}")


# ---------------------------------------------------------------------------
# Compiler API
# ---------------------------------------------------------------------------

def compile_filter(expr: str) -> Callable[[dict[str, Any]], bool]:
    """Parse an expression once, return a row predicate function."""
    ast = parse(expr)
    def predicate(row: dict[str, Any]) -> bool:
        return bool(evaluate(ast, row))
    return predicate


def compile_formatter(publish: dict[str, str]) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Parse a publish config, return a row transformer function.

    publish maps output field names to expression strings.
    Example: {"total": "qty * price", "ticker": "UPPER(symbol)"}
    """
    compiled: list[tuple[str, Node]] = []
    for field_name, expr_str in publish.items():
        ast = parse(expr_str)
        compiled.append((field_name, ast))

    def formatter(row: dict[str, Any]) -> dict[str, Any]:
        return {name: evaluate(ast, row) for name, ast in compiled}

    return formatter
