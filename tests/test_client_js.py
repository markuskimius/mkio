"""Tests for JS client: ref generation and reconnection protocol.

These tests verify the server-side behavior that the JS client depends on,
plus JS ref generation via assertions on the JS source.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
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
    assert "padStart(3" in src  # Milliseconds + us/ns/ps sub-ms components
    # The template literal should produce YYYYMMDD HH:mm:ss.mmmuuunnnppp
    assert "${yyyy}${MM}${dd} ${HH}:${mm}:${ss}.${ms}${us}${ns}${ps}" in src


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


def test_js_client_has_send_chokepoint():
    """All outbound sends route through _sendRaw for the monitor hook."""
    src = JS_CLIENT_PATH.read_text()
    assert "_sendRaw(msg)" in src
    # Raw ws.send should only appear inside _sendRaw itself
    raw_ws_sends = re.findall(r"this\._ws\.send\(", src)
    assert len(raw_ws_sends) == 1, f"expected one this._ws.send(, found {len(raw_ws_sends)}"


def test_js_client_tracks_seen_services():
    src = JS_CLIENT_PATH.read_text()
    assert "_seenServices" in src
    assert "this._seenServices.add" in src


def test_js_client_monitor_hook():
    src = JS_CLIENT_PATH.read_text()
    assert "_emitMonitor" in src
    assert "this._monitor === null" in src
    assert "defaultMonitorFormatter" in src


def test_js_client_pending_tracks_service():
    """_pending entries carry the service so result/error frames can be attributed."""
    src = JS_CLIENT_PATH.read_text()
    assert "resolve, reject, service" in src


def test_js_client_console_dispatcher():
    """The mkio.<verb> console object exists with CLI-matching methods."""
    src = JS_CLIENT_PATH.read_text()
    assert "const mkio = {" in src
    for method in ("services(name)", "monitor(arg)", "send(service, data, opts)", "subscribe(service, opts)"):
        assert method in src, f"missing mkio method {method}"
    assert "window.mkio" in src
    assert "MKIO_HELP" in src


def test_js_client_instance_registry():
    src = JS_CLIENT_PATH.read_text()
    assert "MkioClient._instances" in src
    assert "static instances()" in src


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_client_monitor_hook_runtime():
    """Actually run the client under Node and verify monitor/dispatcher behavior."""
    script = r"""
const { MkioClient, mkio } = require(PATH);
const c = new MkioClient("ws://localhost:8080/ws");
c._ws = { send: () => {}, readyState: 1 };

const entries = [];
c.monitor({ services: ["orders"], fn: (e) => entries.push({dir:e.direction, svc:e.service, type:e.type, ref:e.ref}) });

// outbound send path through _sendRaw
c.send("orders", {x:1}, {ref:"r1"});
// inbound result, service omitted -- must be attributed via _pending
c._dispatch({type:"result", ref:"r1", ok:true});
// filtered-out service
c.send("trades", {}, {ref:"r2"});

// _seenServices populated from both directions
const seen = Array.from(c._seenServices).sort();

// mkio.monitor("off") clears the hook on all instances
mkio.monitor("off");
const afterOffIsNull = c._monitor === null;

// dispatcher surface
const dispatcherMethods = Object.keys(mkio).sort();

console.log(JSON.stringify({entries, seen, afterOffIsNull, dispatcherMethods}));
""".replace("PATH", json.dumps(str(JS_CLIENT_PATH)))

    result = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, check=True
    )
    out = json.loads(result.stdout.strip().splitlines()[-1])

    # Two entries captured: outbound transaction on orders, inbound result on orders
    # (the trades send is filtered out by the services filter).
    assert len(out["entries"]) == 2, out["entries"]
    assert out["entries"][0]["dir"] == "out"
    assert out["entries"][0]["svc"] == "orders"
    assert out["entries"][0]["ref"] == "r1"
    assert out["entries"][1]["dir"] == "in"
    assert out["entries"][1]["svc"] == "orders"  # attributed via _pending
    assert out["entries"][1]["type"] == "result"
    assert out["entries"][1]["ref"] == "r1"

    # _seenServices captures both services (including trades that was filtered from the trace)
    assert out["seen"] == ["orders", "trades"]

    # mkio.monitor("off") clears the hook
    assert out["afterOffIsNull"] is True

    # Dispatcher exposes CLI-matching verbs as methods
    assert out["dispatcherMethods"] == [
        "help", "instances", "monitor", "send", "services", "subscribe",
    ]


def test_js_client_no_external_deps():
    """Verify no imports/requires of external packages."""
    src = JS_CLIENT_PATH.read_text()
    # Should not import anything
    lines = src.split("\n")
    imports = [l for l in lines if l.strip().startswith("import ") and "from" in l]
    requires = [l for l in lines if "require(" in l and "module.exports" not in l]
    assert len(imports) == 0, f"Found external imports: {imports}"
    assert len(requires) == 0, f"Found external requires: {requires}"
