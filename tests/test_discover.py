"""Tests for the stdlib discovery helper URL normalization."""

from __future__ import annotations

from mkio.skill_helpers.discover import _normalize_http_url


def test_http_url_passthrough():
    assert _normalize_http_url("http://localhost:8080") == "http://localhost:8080"


def test_https_url_passthrough():
    assert _normalize_http_url("https://example.com") == "https://example.com"


def test_ws_converted_to_http():
    assert _normalize_http_url("ws://localhost:8080") == "http://localhost:8080"


def test_wss_converted_to_https():
    assert _normalize_http_url("wss://example.com") == "https://example.com"


def test_bare_host_port_gets_http_prefix():
    assert _normalize_http_url("localhost:8080") == "http://localhost:8080"


def test_trailing_slash_stripped():
    assert _normalize_http_url("http://localhost:8080/") == "http://localhost:8080"


def test_ws_suffix_stripped():
    assert _normalize_http_url("ws://localhost:8080/ws") == "http://localhost:8080"


def test_ws_suffix_stripped_from_https():
    assert _normalize_http_url("wss://example.com/ws") == "https://example.com"


def test_trailing_slash_and_ws_suffix():
    assert _normalize_http_url("ws://localhost:8080/ws/") == "http://localhost:8080"


def test_bare_host_with_ws_suffix():
    assert _normalize_http_url("localhost:8080/ws") == "http://localhost:8080"


def test_bare_port():
    assert _normalize_http_url("8080") == "http://localhost:8080"
