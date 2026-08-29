"""Tokenizer.

Token types: NUMBER, STRING, IDENT, KEYWORD (TRUE/FALSE/NULL), OP, LPAREN,
RPAREN, LBRACKET, RBRACKET, LBRACE, RBRACE, COMMA, COLON, DOT, ARROW (->),
PIPE (|>), EOF. Every token records its source offset.
"""

from __future__ import annotations

from dataclasses import dataclass

from ._errors import ExprError

KEYWORDS = frozenset({"TRUE", "FALSE", "NULL"})

# Longest match first.
_OPS2 = ("|>", "->", "??", "**", "//", "&&", "||", "==", "!=", "<=", ">=")
_OPS1 = "<>+-*/%!"

_STRING_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", "'": "'", '"': '"'}

# A DOT is only meaningful directly after something indexable; elsewhere a
# leading "." starts a number (".5").
_DOT_AFTER = frozenset({"IDENT", "RPAREN", "RBRACKET", "RBRACE", "NUMBER", "STRING"})


@dataclass(frozen=True, slots=True)
class Token:
    type: str
    value: str
    pos: int


def tokenize(expr: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(expr)
    while i < n:
        c = expr[i]

        if c in " \t\r\n":
            i += 1
            continue

        if c in "'\"":
            start = i
            quote = c
            i += 1
            out: list[str] = []
            while i < n and expr[i] != quote:
                ch = expr[i]
                if ch == "\\":
                    i += 1
                    if i >= n:
                        break
                    esc = expr[i]
                    if esc == "u" and i + 1 < n and expr[i + 1] == "{":
                        end = expr.find("}", i + 2)
                        if end < 0:
                            raise ExprError("Unterminated \\u{...} escape", i)
                        try:
                            out.append(chr(int(expr[i + 2:end], 16)))
                        except ValueError:
                            raise ExprError("Bad \\u{...} escape", i) from None
                        i = end + 1
                        continue
                    if esc not in _STRING_ESCAPES:
                        raise ExprError(f"Unknown escape: \\{esc}", i)
                    out.append(_STRING_ESCAPES[esc])
                    i += 1
                    continue
                out.append(ch)
                i += 1
            if i >= n:
                raise ExprError("Unterminated string literal", start)
            i += 1  # closing quote
            tokens.append(Token("STRING", "".join(out), start))
            continue

        if c == "`":
            start = i
            end = expr.find("`", i + 1)
            if end < 0:
                raise ExprError("Unterminated backtick identifier", start)
            if end == i + 1:
                raise ExprError("Empty backtick identifier", start)
            tokens.append(Token("IDENT", expr[i + 1:end], start))
            i = end + 1
            continue

        if c == "." and tokens and tokens[-1].type in _DOT_AFTER:
            tokens.append(Token("DOT", ".", i))
            i += 1
            continue

        if c.isdigit() or (c == "." and i + 1 < n and expr[i + 1].isdigit()):
            start = i
            while i < n and (expr[i].isdigit() or expr[i] == "_"):
                i += 1
            if i < n and expr[i] == "." and i + 1 < n and expr[i + 1].isdigit():
                i += 1
                while i < n and (expr[i].isdigit() or expr[i] == "_"):
                    i += 1
            if i < n and expr[i] in "eE":
                j = i + 1
                if j < n and expr[j] in "+-":
                    j += 1
                if j < n and expr[j].isdigit():
                    i = j
                    while i < n and expr[i].isdigit():
                        i += 1
            text = expr[start:i]
            if text.startswith("_") or text.endswith("_") or "__" in text:
                raise ExprError(f"Bad number literal: {text}", start)
            if i < n and (expr[i].isalpha() or expr[i] == "_"):
                raise ExprError(f"Bad number literal: {expr[start:i + 1]}", start)
            tokens.append(Token("NUMBER", text.replace("_", ""), start))
            continue

        two = expr[i:i + 2]
        if two in _OPS2:
            kind = "PIPE" if two == "|>" else "ARROW" if two == "->" else "OP"
            tokens.append(Token(kind, two, i))
            i += 2
            continue

        if c in _OPS1:
            tokens.append(Token("OP", c, i))
            i += 1
            continue

        simple = {
            "(": "LPAREN", ")": "RPAREN", "[": "LBRACKET", "]": "RBRACKET",
            "{": "LBRACE", "}": "RBRACE", ",": "COMMA", ":": "COLON",
        }
        if c in simple:
            tokens.append(Token(simple[c], c, i))
            i += 1
            continue

        if c.isalpha() or c == "_":
            start = i
            while i < n and (expr[i].isalnum() or expr[i] == "_"):
                i += 1
            word = expr[start:i]
            if word.upper() in KEYWORDS:
                tokens.append(Token("KEYWORD", word, start))
            else:
                tokens.append(Token("IDENT", word, start))
            continue

        raise ExprError(f"Unexpected character: {c!r}", i)

    tokens.append(Token("EOF", "", n))
    return tokens
