"""Tests for ref string generator."""

import re
from mkio._ref import next_ref, compare_refs, normalize_ref, local_ts


def test_format():
    v = next_ref()
    # YYYYMMDD HH:MM:SS.mmmuuunnnppp
    assert re.match(r"\d{8} \d{2}:\d{2}:\d{2}\.\d{12}", v), f"Bad format: {v}"


def test_monotonically_increasing():
    refs = [next_ref() for _ in range(100)]
    for i in range(1, len(refs)):
        assert refs[i] > refs[i - 1], f"{refs[i]} <= {refs[i-1]}"


def test_unique():
    refs = [next_ref() for _ in range(1000)]
    assert len(set(refs)) == 1000


def test_compare_equal():
    v = next_ref()
    assert compare_refs(v, v) == 0


def test_compare_less():
    a = next_ref()
    b = next_ref()
    assert compare_refs(a, b) == -1


def test_compare_greater():
    a = next_ref()
    b = next_ref()
    assert compare_refs(b, a) == 1


def test_compare_different_precision():
    assert compare_refs("20260404 15:30:45.123", "20260404 15:30:45.123000000000") == 0
    assert compare_refs("20260404 15:30:45.123", "20260404 15:30:45.124000000000") == -1
    assert compare_refs("20260404 15:30:45.124", "20260404 15:30:45.123000000000") == 1


def test_local_ts_format():
    v = local_ts()
    # YYYY-MM-DD HH:MM:SS.mmmuuu ±HHMM
    assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6} [+-]\d{4}$", v), f"Bad format: {v}"


def test_normalize():
    assert normalize_ref("20260404 15:30:45") == "20260404 15:30:45.000000000000"
    assert normalize_ref("20260404 15:30:45.123") == "20260404 15:30:45.123000000000"
    assert normalize_ref("20260404 15:30:45.123456789012") == "20260404 15:30:45.123456789012"
