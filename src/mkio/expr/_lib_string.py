"""string library. REPLACE and SPLIT are literal; MATCHES is a regex search."""

from __future__ import annotations

import re

from ._env import register_library
from ._errors import ExprError
from ._values import equals, is_number, kind, to_string


def _s(x, name):
    if x is None:
        return None
    if not isinstance(x, str):
        raise ExprError(f"{name} requires a string, got {kind(x)}")
    return x


def _passthrough(fn, name):
    def run(x):
        s = _s(x, name)
        return None if s is None else fn(s)
    return run


def _title(s):
    return " ".join(w[:1].upper() + w[1:].lower() for w in s.split(" "))


def _pad(s, width, ch=" "):
    s = to_string(s)
    ch = _s(ch, "PAD") or " "
    width = int(width)
    if len(s) >= width:
        return s
    fill = (ch * width)[: width - len(s)]
    return fill + s


def _pad_end(s, width, ch=" "):
    s = to_string(s)
    ch = _s(ch, "PAD_END") or " "
    width = int(width)
    if len(s) >= width:
        return s
    return s + (ch * width)[: width - len(s)]


def _truncate(s, n, suffix=""):
    s = _s(s, "TRUNCATE")
    if s is None:
        return None
    n = int(n)
    if len(s) <= n:
        return s
    keep = max(n - len(suffix), 0)
    return s[:keep] + suffix


def _replace(s, old, new):
    s = _s(s, "REPLACE")
    if s is None:
        return None
    return s.replace(to_string(old), to_string(new))


def _substr(s, start, length=None):
    s = _s(s, "SUBSTR")
    if s is None:
        return None
    start = int(start)
    if start < 0:
        start = max(len(s) + start, 0)
    if length is None:
        return s[start:]
    return s[start:start + max(int(length), 0)]


def _split(s, sep):
    s = _s(s, "SPLIT")
    if s is None:
        return None
    sep = to_string(sep)
    if sep == "":
        return list(s)
    return s.split(sep)


def _join(xs, sep=""):
    if xs is None:
        return None
    if not isinstance(xs, (list, tuple)):
        raise ExprError(f"JOIN requires an array, got {kind(xs)}")
    return to_string(sep).join(to_string(x) for x in xs)


def _len(x):
    if x is None:
        return 0
    if isinstance(x, (str, list, tuple, dict)):
        return len(x)
    raise ExprError(f"LEN requires a string, array, or map, got {kind(x)}")


def _contains(hay, x):
    if hay is None:
        return False
    if isinstance(hay, str):
        return to_string(x) in hay
    if isinstance(hay, (list, tuple)):
        return any(equals(v, x) for v in hay)
    if isinstance(hay, dict):
        return (x if isinstance(x, str) else to_string(x)) in hay
    raise ExprError(f"CONTAINS requires a string, array, or map, got {kind(hay)}")


def _starts_with(s, prefix):
    s = _s(s, "STARTS_WITH")
    return False if s is None else s.startswith(to_string(prefix))


def _ends_with(s, suffix):
    s = _s(s, "ENDS_WITH")
    return False if s is None else s.endswith(to_string(suffix))


def _matches(s, pattern):
    s = _s(s, "MATCHES")
    if s is None:
        return False
    try:
        return re.search(to_string(pattern), s) is not None
    except re.error as e:
        raise ExprError(f"MATCHES: bad pattern: {e}") from None


register_library("string", {
    "UPPER":       (_passthrough(str.upper, "UPPER"), {"params": ("s",), "doc": "Upper-case."}),
    "LOWER":       (_passthrough(str.lower, "LOWER"), {"params": ("s",), "doc": "Lower-case."}),
    "TITLE":       (_passthrough(_title, "TITLE"), {"params": ("s",), "doc": "Capitalise each space-separated word."}),
    "TRIM":        (_passthrough(str.strip, "TRIM"), {"params": ("s",), "doc": "Strip surrounding whitespace."}),
    "PAD":         (_pad, {"params": ("s", "width", "ch"), "doc": "Left-pad to `width` with `ch` (default space)."}),
    "PAD_END":     (_pad_end, {"params": ("s", "width", "ch"), "doc": "Right-pad to `width` with `ch` (default space)."}),
    "TRUNCATE":    (_truncate, {"params": ("s", "n", "suffix"), "doc": "Cut to at most `n` characters, ending with `suffix` if cut."}),
    "REPLACE":     (_replace, {"params": ("s", "old", "new"), "doc": "Replace every literal occurrence of `old` with `new`."}),
    "SUBSTR":      (_substr, {"params": ("s", "start", "length"), "doc": "Substring from `start` (negative counts from the end), optionally `length` long."}),
    "SPLIT":       (_split, {"params": ("s", "sep"), "doc": "Split on a literal separator ('' splits into characters)."}),
    "JOIN":        (_join, {"params": ("xs", "sep"), "doc": "Join an array's string forms with `sep` (default '')."}),
    "CONCAT":      (lambda *a: "".join(to_string(x) for x in a), {"doc": "Concatenate the string forms of all arguments."}),
    "LEN":         (_len, {"params": ("x",), "doc": "Length of a string, array, or map (NULL → 0)."}),
    "CONTAINS":    (_contains, {"params": ("hay", "x"), "doc": "Substring of a string, member of an array, or key of a map."}),
    "STARTS_WITH": (_starts_with, {"params": ("s", "prefix"), "doc": "TRUE if `s` starts with `prefix`."}),
    "ENDS_WITH":   (_ends_with, {"params": ("s", "suffix"), "doc": "TRUE if `s` ends with `suffix`."}),
    "MATCHES":     (_matches, {"params": ("s", "pattern"), "doc": "TRUE if the regular expression matches anywhere in `s`."}),
})
