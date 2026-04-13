"""Ref string generator: YYYYMMDD HH:mm:ss.mmmuuunnnppp in UTC.

Monotonically increasing, lexicographically sortable, human-readable.
Used by both server and client to generate refs.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

_last_time_ns: int = 0
_counter: int = 0


def next_ref() -> str:
    """Generate the next monotonically increasing ref string.

    Uses time.time_ns() for the clock portion. When multiple calls occur
    within the same nanosecond, a counter fills the picosecond digits to
    guarantee uniqueness.
    """
    global _last_time_ns, _counter

    now_ns = time.time_ns()
    if now_ns <= _last_time_ns:
        _counter += 1
    else:
        _counter = 0
        _last_time_ns = now_ns

    dt = datetime.fromtimestamp(now_ns / 1_000_000_000, tz=timezone.utc)

    # Extract sub-second components
    total_ns = now_ns % 1_000_000_000
    ms = total_ns // 1_000_000
    us = (total_ns // 1_000) % 1_000
    ns = total_ns % 1_000

    # Counter fills the picosecond digits (000-999)
    ps = _counter % 1000

    return dt.strftime("%Y%m%d %H:%M:%S") + f".{ms:03d}{us:03d}{ns:03d}{ps:03d}"


def local_ts() -> str:
    """Return the current local time for human-readable display.

    Format: ``YYYYMMDD HH:MM:SS.mmmuuu ±HHMM``. Not a ref — no monotonic
    counter, no uniqueness guarantee.
    """
    now_ns = time.time_ns()
    dt = datetime.fromtimestamp(now_ns / 1_000_000_000).astimezone()
    total_us = (now_ns // 1_000) % 1_000_000
    ms = total_us // 1_000
    us = total_us % 1_000
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ms:03d}{us:03d} " + dt.strftime("%z")


def compare_refs(a: str, b: str) -> int:
    """Compare two ref strings. Returns -1, 0, or 1.

    Missing trailing digits are treated as zeros.
    """
    na = normalize_ref(a)
    nb = normalize_ref(b)
    if na < nb:
        return -1
    elif na > nb:
        return 1
    return 0


def normalize_ref(v: str) -> str:
    """Normalize a ref string to full 31-character precision.

    Pads missing sub-second digits with zeros.
    Example: "20260404 15:30:45.123" -> "20260404 15:30:45.123000000000"
    """
    # Full format: "YYYYMMDD HH:MM:SS.mmmuuunnnppp" = 31 chars
    if "." not in v:
        return v + ".000000000000"
    base, frac = v.split(".", 1)
    return base + "." + frac.ljust(12, "0")
