"""Tests for version string generator."""

import re
from mkio._version import next_version, compare_versions, normalize_version


def test_format():
    v = next_version()
    # YYYYMMDD HH:MM:SS.mmmuuunnnppp
    assert re.match(r"\d{8} \d{2}:\d{2}:\d{2}\.\d{12}", v), f"Bad format: {v}"


def test_monotonically_increasing():
    versions = [next_version() for _ in range(100)]
    for i in range(1, len(versions)):
        assert versions[i] > versions[i - 1], f"{versions[i]} <= {versions[i-1]}"


def test_unique():
    versions = [next_version() for _ in range(1000)]
    assert len(set(versions)) == 1000


def test_compare_equal():
    v = next_version()
    assert compare_versions(v, v) == 0


def test_compare_less():
    a = next_version()
    b = next_version()
    assert compare_versions(a, b) == -1


def test_compare_greater():
    a = next_version()
    b = next_version()
    assert compare_versions(b, a) == 1


def test_compare_different_precision():
    assert compare_versions("20260404 15:30:45.123", "20260404 15:30:45.123000000000") == 0
    assert compare_versions("20260404 15:30:45.123", "20260404 15:30:45.124000000000") == -1
    assert compare_versions("20260404 15:30:45.124", "20260404 15:30:45.123000000000") == 1


def test_normalize():
    assert normalize_version("20260404 15:30:45") == "20260404 15:30:45.000000000000"
    assert normalize_version("20260404 15:30:45.123") == "20260404 15:30:45.123000000000"
    assert normalize_version("20260404 15:30:45.123456789012") == "20260404 15:30:45.123456789012"
