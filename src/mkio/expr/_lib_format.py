"""format library: numbers to display strings."""

from __future__ import annotations

import math
from decimal import ROUND_HALF_UP, Decimal

from ._env import register_library
from ._errors import ExprError
from ._values import is_number, kind, number_to_string, require_number, to_string


def _fixed(x, digits: int) -> str:
    """Fixed-point string, half away from zero on the decimal representation."""
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return number_to_string(x)
    d = Decimal(number_to_string(x))
    q = Decimal(1).scaleb(-digits)
    r = d.quantize(q, rounding=ROUND_HALF_UP)
    s = f"{r:f}"
    if s.startswith("-") and set(s[1:]) <= set("0."):
        s = s[1:]   # no negative zero
    return s


def _group(intpart: str) -> str:
    neg = intpart.startswith("-")
    digits = intpart[1:] if neg else intpart
    out = []
    while len(digits) > 3:
        out.insert(0, digits[-3:])
        digits = digits[:-3]
    out.insert(0, digits)
    return ("-" if neg else "") + ",".join(out)


def num(x, digits=None, group=False):
    """NUM(x, digits:, group:) — fixed digits (or shortest form when NULL),
    optional thousands grouping. NULL → ''."""
    if x is None:
        return ""
    require_number(x, "NUM")
    if digits is None:
        s = number_to_string(x)
    else:
        s = _fixed(x, int(require_number(digits, "NUM digits")))
    if group and "e" not in s:
        ip, _, fp = s.partition(".")
        s = _group(ip) + ("." + fp if fp else "")
    return s


def pct(x, digits=0):
    if x is None:
        return ""
    require_number(x, "PCT")
    return _fixed(x * 100, int(digits)) + "%"


def sci(x, digits=2):
    if x is None:
        return ""
    require_number(x, "SCI")
    if x == 0:
        return _fixed(0, int(digits)) + "e+0"
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return number_to_string(x)
    exp = math.floor(math.log10(abs(x)))
    mant = x / (10 ** exp)
    m = _fixed(mant, int(digits))
    if abs(float(m)) >= 10:   # rounding carried (9.99 → 10.0)
        exp += 1
        m = _fixed(mant / 10, int(digits))
    return f"{m}e{'+' if exp >= 0 else '-'}{abs(exp)}"


_BYTE_UNITS = ("B", "KB", "MB", "GB", "TB", "PB", "EB")


def bytes_(n, digits=1):
    if n is None:
        return ""
    require_number(n, "BYTES")
    v = abs(n)
    i = 0
    while v >= 1024 and i < len(_BYTE_UNITS) - 1:
        v /= 1024
        i += 1
    s = _fixed(v, 0 if i == 0 else int(digits))
    return ("-" if n < 0 else "") + s + " " + _BYTE_UNITS[i]


def duration(seconds, digits=0):
    """Seconds → '1d 2h 3m 4s' (leading zero units dropped)."""
    if seconds is None:
        return ""
    require_number(seconds, "DURATION")
    neg = seconds < 0
    s = abs(seconds)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    parts = []
    if d:
        parts.append(f"{int(d)}d")
    if h or parts:
        parts.append(f"{int(h)}h")
    if m or parts:
        parts.append(f"{int(m)}m")
    parts.append(_fixed(s, int(digits)) + "s")
    return ("-" if neg else "") + " ".join(parts)


def format_(pattern, *args):
    """FORMAT('{} of {}', a, b) / FORMAT('{1}-{0}', a, b); '{{' and '}}' escape."""
    pattern = to_string(pattern)
    out = []
    i = 0
    auto = 0
    n = len(pattern)
    while i < n:
        c = pattern[i]
        if c == "{":
            if pattern.startswith("{{", i):
                out.append("{")
                i += 2
                continue
            end = pattern.find("}", i)
            if end < 0:
                raise ExprError("FORMAT: unmatched '{'")
            spec = pattern[i + 1:end].strip()
            if spec == "":
                idx = auto
                auto += 1
            elif spec.isdigit():
                idx = int(spec)
            else:
                raise ExprError(f"FORMAT: bad placeholder {{{spec}}}")
            if idx >= len(args):
                raise ExprError(f"FORMAT: placeholder {{{spec}}} has no argument")
            out.append(to_string(args[idx]))
            i = end + 1
            continue
        if c == "}":
            if pattern.startswith("}}", i):
                out.append("}")
                i += 2
                continue
            raise ExprError("FORMAT: unmatched '}'")
        out.append(c)
        i += 1
    return "".join(out)


register_library("format", {
    "NUM":      (num, {"params": ("x", "digits", "group"), "doc": "Number to string with fixed `digits` (shortest form when omitted) and optional thousands `group`."}),
    "PCT":      (pct, {"params": ("x", "digits"), "doc": "Fraction to percentage string: 0.125 → '12.5%' with digits: 1."}),
    "SCI":      (sci, {"params": ("x", "digits"), "doc": "Scientific notation: 123456 → '1.23e+5'."}),
    "BYTES":    (bytes_, {"params": ("n", "digits"), "doc": "Byte count to '1.5 KB' (1024-based)."}),
    "DURATION": (duration, {"params": ("seconds", "digits"), "doc": "Seconds to '1d 2h 3m 4s'."}),
    "FORMAT":   (format_, {"doc": "Fill '{}' / '{0}' placeholders in a pattern with the remaining arguments."}),
})
