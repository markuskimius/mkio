"""math library. ROUND is half-away-from-zero on the decimal representation."""

from __future__ import annotations

import math
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from ._env import register_library
from ._errors import ExprError
from ._values import compare, is_number, kind, normalize, number_to_string, require_number


def round_decimal(x: float | int, digits: int = 0) -> float | int:
    """Round `x` to `digits` places, half away from zero, judged on the
    shortest decimal representation (so 2.675 → 2.68, not 2.67)."""
    require_number(x, "ROUND")
    digits = int(require_number(digits, "ROUND digits"))
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return x
    d = Decimal(number_to_string(x))
    q = Decimal(1).scaleb(-digits)
    r = d.quantize(q, rounding=ROUND_HALF_UP)
    if digits <= 0:
        return int(r)
    return normalize(float(r))


def _minmax(pick, name, *args):
    vals = list(args[0]) if len(args) == 1 and isinstance(args[0], (list, tuple)) else list(args)
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    best = vals[0]
    for v in vals[1:]:
        if pick(compare(v, best, name)):
            best = v
    return best


def _sum(xs):
    if not isinstance(xs, (list, tuple)):
        raise ExprError(f"SUM requires an array, got {kind(xs)}")
    total: Any = 0
    for v in xs:
        if v is None:
            continue
        total += require_number(v, "SUM element")
    return normalize(total)


def _avg(xs):
    if not isinstance(xs, (list, tuple)):
        raise ExprError(f"AVG requires an array, got {kind(xs)}")
    vals = [require_number(v, "AVG element") for v in xs if v is not None]
    if not vals:
        return None
    return normalize(sum(vals) / len(vals))


def _clamp(x, lo, hi):
    require_number(x, "CLAMP"); require_number(lo, "CLAMP"); require_number(hi, "CLAMP")
    return min(max(x, lo), hi)


def _sqrt(x):
    require_number(x, "SQRT")
    if x < 0:
        raise ExprError("SQRT of a negative number")
    return normalize(math.sqrt(x))


def _pow(a, b):
    require_number(a, "POW"); require_number(b, "POW")
    r = a ** b
    if isinstance(r, complex):
        raise ExprError("POW of a negative base with fractional exponent")
    return normalize(r)


register_library("math", {
    "ROUND": (round_decimal, {"numeric": True, "params": ("x", "digits"), "doc": "Round half away from zero to `digits` places (default 0)."}),
    "FLOOR": (lambda x: math.floor(require_number(x, "FLOOR")), {"numeric": True, "params": ("x",), "doc": "Largest integer ≤ x."}),
    "CEIL":  (lambda x: math.ceil(require_number(x, "CEIL")), {"numeric": True, "params": ("x",), "doc": "Smallest integer ≥ x."}),
    "ABS":   (lambda x: abs(require_number(x, "ABS")), {"numeric": True, "params": ("x",), "doc": "Absolute value."}),
    "SIGN":  (lambda x: (require_number(x, "SIGN") > 0) - (x < 0), {"numeric": True, "params": ("x",), "doc": "-1, 0, or 1."}),
    "MIN":   (lambda *a: _minmax(lambda c: c < 0, "MIN", *a), {"doc": "Smallest of the arguments, or of a single array; NULLs ignored."}),
    "MAX":   (lambda *a: _minmax(lambda c: c > 0, "MAX", *a), {"doc": "Largest of the arguments, or of a single array; NULLs ignored."}),
    "SUM":   (_sum, {"numeric": True, "params": ("xs",), "doc": "Sum of an array of numbers; NULLs ignored."}),
    "AVG":   (_avg, {"numeric": True, "params": ("xs",), "doc": "Mean of an array of numbers; NULL when empty."}),
    "CLAMP": (_clamp, {"numeric": True, "params": ("x", "lo", "hi"), "doc": "x limited to [lo, hi]."}),
    "POW":   (_pow, {"numeric": True, "params": ("x", "y"), "doc": "x to the power y."}),
    "SQRT":  (_sqrt, {"numeric": True, "params": ("x",), "doc": "Square root."}),
})
