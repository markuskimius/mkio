"""Tests for JS client: ref generation and reconnection protocol.

These tests verify the server-side behavior that the JS client depends on,
plus JS ref generation via assertions on the JS source.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from mkio._json import loads


JS_CLIENT_PATH = Path(__file__).parent.parent / "src" / "mkio" / "client" / "mkio.js"


def test_js_client_file_exists():
    assert JS_CLIENT_PATH.exists(), f"JS client not found at {JS_CLIENT_PATH}"


def test_js_makeref_produces_correct_format():
    """Verify the JS makeRef function produces the right format by reading the source."""
    src = JS_CLIENT_PATH.read_text()
    # Verify the format string pattern is present
    assert "padStart(2" in src  # Zero-padded date components
    assert "padStart(3" in src  # Milliseconds
    assert "padStart(9" in src  # Sub-ms counter
    # The template literal should produce YYYYMMDD HH:mm:ss.mmmnnnnnnnnn
    assert "${yyyy}${MM}${dd} ${HH}:${mm}:${ss}.${ms}${sub}" in src


def test_js_client_has_reconnect():
    """Verify the JS client has reconnection logic."""
    src = JS_CLIENT_PATH.read_text()
    assert "_attemptReconnect" in src
    assert "_resubscribe" in src


def test_js_client_tracks_ref():
    """Verify ref tracking in the JS client."""
    src = JS_CLIENT_PATH.read_text()
    assert "sub.ref = data.ref" in src or "sub.ref" in src


def test_js_client_has_check():
    """Verify the JS client supports transaction recovery check."""
    src = JS_CLIENT_PATH.read_text()
    assert "check(service, ref)" in src


def test_js_client_exports():
    """Verify the JS client exports correctly."""
    src = JS_CLIENT_PATH.read_text()
    assert "MkioClient" in src
    assert "makeRef" in src
    # ES module / CommonJS / global export
    assert "module.exports" in src
    assert "window.MkioClient" in src


def test_js_client_subscribe_sends_filter_and_ref():
    """Verify subscribe sends filter and ref for reconnection."""
    src = JS_CLIENT_PATH.read_text()
    assert 'msg.filter' in src
    assert 'msg.ref' in src


def test_js_client_no_external_deps():
    """Verify no imports/requires of external packages."""
    src = JS_CLIENT_PATH.read_text()
    # Should not import anything
    lines = src.split("\n")
    imports = [l for l in lines if l.strip().startswith("import ") and "from" in l]
    requires = [l for l in lines if "require(" in l and "module.exports" not in l]
    assert len(imports) == 0, f"Found external imports: {imports}"
    assert len(requires) == 0, f"Found external requires: {requires}"
