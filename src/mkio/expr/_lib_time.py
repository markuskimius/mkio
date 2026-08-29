"""time library. The numeric unit is seconds since the Unix epoch (float).

Refs ("YYYYMMDD HH:MM:SS.mmmuuunnnppp", UTC) and ISO-8601 strings parse
via EPOCH. Formatting understands a fixed strftime subset:
%Y %m %d %H %M %S %f (microseconds, 6 digits) %z (+HHMM) %%.
Time zones: 'UTC' (default), 'local', or a fixed offset '+09:00' / '-0500'.
"""

from __future__ import annotations

import re
import time as _time
from datetime import datetime, timedelta, timezone

from ._env import register_library
from ._errors import ExprError
from ._values import is_number, kind, normalize, require_number

_REF_RE = re.compile(r"^(\d{4})(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,12}))?$")
_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?"
    r"(Z|[+-]\d{2}:?\d{2})?$"
)
_OFFSET_RE = re.compile(r"^([+-])(\d{2}):?(\d{2})$")


def _tz(spec):
    if spec is None or spec == "UTC" or spec == "utc":
        return timezone.utc
    if spec == "local":
        return None
    m = _OFFSET_RE.match(str(spec))
    if not m:
        raise ExprError(f"Unknown time zone {spec!r}: use 'UTC', 'local', or '+HH:MM'")
    sign = 1 if m.group(1) == "+" else -1
    return timezone(sign * timedelta(hours=int(m.group(2)), minutes=int(m.group(3))))


def epoch(x):
    """Seconds since the epoch from a number, ref string, or ISO-8601 string."""
    if x is None:
        return None
    if is_number(x):
        return x
    if not isinstance(x, str):
        raise ExprError(f"EPOCH requires a number or string, got {kind(x)}")
    s = x.strip()
    m = _REF_RE.match(s)
    if m:
        y, mo, d, h, mi, sec, frac = m.groups()
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(sec), tzinfo=timezone.utc)
        base = dt.timestamp()
        if frac:
            base += int(frac[:9].ljust(9, "0")) / 1e9
        return normalize(base)
    m = _ISO_RE.match(s)
    if m:
        y, mo, d, h, mi, sec, frac, tz = m.groups()
        if tz is None or tz == "Z":
            tzinfo = timezone.utc
        else:
            tzinfo = _tz(tz)
        dt = datetime(int(y), int(mo), int(d), int(h or 0), int(mi or 0), int(sec or 0), tzinfo=tzinfo)
        base = dt.timestamp()
        if frac:
            base += int(frac[:9].ljust(9, "0")) / 1e9
        return normalize(base)
    raise ExprError(f"EPOCH: cannot parse {x!r} as a ref or ISO-8601 time")


def _dt(ts, tz):
    secs = epoch(ts)
    if secs is None:
        return None
    require_number(secs, "time")
    tzinfo = _tz(tz)
    if tzinfo is None:
        return datetime.fromtimestamp(secs).astimezone()
    return datetime.fromtimestamp(secs, tz=tzinfo)


def _strftime(dt: datetime, fmt: str) -> str:
    out = []
    i = 0
    n = len(fmt)
    while i < n:
        c = fmt[i]
        if c != "%":
            out.append(c)
            i += 1
            continue
        if i + 1 >= n:
            raise ExprError("Bad time format: trailing '%'")
        t = fmt[i + 1]
        i += 2
        if t == "Y":
            out.append(f"{dt.year:04d}")
        elif t == "m":
            out.append(f"{dt.month:02d}")
        elif t == "d":
            out.append(f"{dt.day:02d}")
        elif t == "H":
            out.append(f"{dt.hour:02d}")
        elif t == "M":
            out.append(f"{dt.minute:02d}")
        elif t == "S":
            out.append(f"{dt.second:02d}")
        elif t == "f":
            out.append(f"{dt.microsecond:06d}")
        elif t == "z":
            off = dt.utcoffset() or timedelta(0)
            total = int(off.total_seconds())
            sign = "+" if total >= 0 else "-"
            total = abs(total)
            out.append(f"{sign}{total // 3600:02d}{(total % 3600) // 60:02d}")
        elif t == "%":
            out.append("%")
        else:
            raise ExprError(f"Bad time format token: %{t}")
    return "".join(out)


def date(ts, fmt="%Y-%m-%d", tz="UTC"):
    dt = _dt(ts, tz)
    return "" if dt is None else _strftime(dt, str(fmt))


def time_(ts, fmt="%H:%M:%S", tz="UTC"):
    dt = _dt(ts, tz)
    return "" if dt is None else _strftime(dt, str(fmt))


def ref_time(ref, fmt="%Y-%m-%d %H:%M:%S", tz="UTC"):
    dt = _dt(ref, tz)
    return "" if dt is None else _strftime(dt, str(fmt))


register_library("time", {
    "NOW":      (lambda: _time.time(), {"doc": "Current time in seconds since the epoch."}),
    "EPOCH":    (epoch, {"params": ("x",), "numeric": False, "doc": "Seconds since the epoch from a number, mkio ref string, or ISO-8601 string."}),
    "DATE":     (date, {"params": ("ts", "fmt", "tz"), "doc": "Format a time as a date (default '%Y-%m-%d', UTC)."}),
    "TIME":     (time_, {"params": ("ts", "fmt", "tz"), "doc": "Format a time as a clock time (default '%H:%M:%S', UTC)."}),
    "REF_TIME": (ref_time, {"params": ("ref", "fmt", "tz"), "doc": "Format an mkio ref (default '%Y-%m-%d %H:%M:%S', UTC)."}),
})
