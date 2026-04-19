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
    for method in ("services(name)", "monitor(arg)", "send(service, data, opts)",
                   "subpub(service, topic, opts)", "stream(service, opts)", "query(service, opts)"):
        assert method in src, f"missing mkio method {method}"
    assert "window.mkio" in src
    assert "MKIO_HELP" in src


def test_js_client_instance_registry():
    src = JS_CLIENT_PATH.read_text()
    assert "MkioClient._instances" in src
    assert "static instances()" in src


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_client_reconnect_race_does_not_throw():
    """A single failed connect must not stack multiple reconnects, and a stale
    socket's onopen must not fire _resubscribe against a newer CONNECTING socket.
    Regression test for the InvalidStateError race when the server is killed."""
    script = r"""
global.WebSocket = class {
  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.binaryType = null;
    FakeWS.instances.push(this);
  }
  send(data) {
    if (this.readyState !== 1) {
      throw new Error("Failed to execute 'send' on 'WebSocket': Still in CONNECTING state.");
    }
  }
  close() { this.readyState = 3; }
};
global.WebSocket.OPEN = 1;
const FakeWS = global.WebSocket;
FakeWS.instances = [];

const { MkioClient } = require(PATH);
const c = new MkioClient("ws://x/ws", { backoffBase: 1, backoffMax: 1 });

// Inject a subscription without hitting the socket, so _resubscribe has work.
c._subscriptions.set("orders", {
  service: "orders", filter: null, ref: null,
  onSnapshot: () => {}, onDelta: () => {}, onUpdate: () => {},
});

c.connect().catch(() => {});

// Fire BOTH onerror and onclose on the first socket in the order the browser
// does. Before the fix, each scheduled its own reconnect.
const ws1 = FakeWS.instances[0];
ws1.onerror(new Error("fail"));
ws1.onclose();

setTimeout(() => {
  let threw = null;
  const latest = FakeWS.instances[FakeWS.instances.length - 1];
  latest.readyState = 1;
  try {
    latest.onopen();
  } catch (e) {
    threw = e.message;
  }
  console.log(JSON.stringify({ sockets: FakeWS.instances.length, threw }));
  process.exit(0);
}, 20);
""".replace("PATH", json.dumps(str(JS_CLIENT_PATH)))

    result = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, check=True
    )
    out = json.loads(result.stdout.strip().splitlines()[-1])

    # One failed connect must produce exactly one reconnect socket, not two.
    assert out["sockets"] == 2, f"expected 2 sockets, got {out['sockets']}"
    # onopen on the live socket must not throw.
    assert out["threw"] is None, f"onopen threw: {out['threw']}"


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
        "help", "instances", "monitor", "query", "send", "services", "stream", "subpub",
    ]


def test_js_client_has_delta_dispatch():
    """Verify _dispatch routes delta messages to onDelta."""
    src = JS_CLIENT_PATH.read_text()
    assert 'type === "delta"' in src
    assert "sub.onDelta(data.changes)" in src


def test_js_client_subscribe_stores_ondelta():
    """Verify subscribe includes onDelta in the sub object."""
    src = JS_CLIENT_PATH.read_text()
    assert "onDelta: opts.onDelta" in src


def test_js_client_monitor_filter():
    """Verify monitor supports a filter function."""
    src = JS_CLIENT_PATH.read_text()
    assert "this._monitor.filter" in src


def test_js_client_subpub_requires_topic():
    """Verify mkio.subpub validates that topic is provided."""
    src = JS_CLIENT_PATH.read_text()
    assert "mkio.subpub: topic is required" in src


def test_js_client_query_mutual_exclusion():
    """Verify mkio.query rejects snapshotOnly + updateOnly together."""
    src = JS_CLIENT_PATH.read_text()
    assert "snapshotOnly and updateOnly are mutually exclusive" in src


def test_js_client_stream_auto_ref():
    """Verify mkio.stream auto-generates a ref if not provided."""
    src = JS_CLIENT_PATH.read_text()
    assert "if (!o.ref) o.ref = makeRef()" in src


def test_js_client_default_nack_handler():
    """Verify _mkioSubscribe sets a default onNack formatter."""
    src = JS_CLIENT_PATH.read_text()
    assert "nack:" in src
    assert "console.warn" in src


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_client_subpub_stream_query_runtime():
    """Run subpub/stream/query console methods under Node and verify behavior."""
    script = r"""
global.WebSocket = class {
  constructor(url) { this.url = url; this.readyState = 1; this.binaryType = null; }
  send(data) { FakeWS.sent.push(JSON.parse(data)); }
  close() {}
};
global.WebSocket.OPEN = 1;
const FakeWS = global.WebSocket;
FakeWS.sent = [];

const { MkioClient, mkio } = require(PATH);
const c = new MkioClient("ws://localhost:8080/ws");
c._ws = new WebSocket("ws://localhost:8080/ws");

const results = {};

// subpub without topic should warn and return undefined
const noTopic = mkio.subpub("prices");
results.subpubNoTopic = noTopic === undefined;

// subpub with topic
FakeWS.sent = [];
const sub1 = mkio.subpub("prices", "AAPL", { fields: ["bid", "ask"] });
results.subpubSent = FakeWS.sent[0];
results.subpubHasStop = typeof sub1.stop === "function";

// stream auto-generates ref
FakeWS.sent = [];
mkio.stream("trades");
results.streamSent = FakeWS.sent[0];
results.streamHasRef = typeof FakeWS.sent[0].ref === "string" && FakeWS.sent[0].ref.length > 0;

// stream with explicit ref
FakeWS.sent = [];
mkio.stream("trades", { ref: "custom-ref" });
results.streamExplicitRef = FakeWS.sent[0].ref;

// query basic
FakeWS.sent = [];
mkio.query("orders", { filter: "status == 'open'" });
results.querySent = FakeWS.sent[0];

// query snapshotOnly
FakeWS.sent = [];
mkio.query("orders", { snapshotOnly: true });
results.querySnapshotOnly = FakeWS.sent[0];

// query updateOnly
FakeWS.sent = [];
mkio.query("orders", { updateOnly: true });
results.queryUpdateOnly = FakeWS.sent[0];

// query mutual exclusion
const bad = mkio.query("orders", { snapshotOnly: true, updateOnly: true });
results.queryMutualExclusion = bad === undefined;

// delta dispatch
let deltaReceived = null;
c._subscriptions.set("feed", {
  service: "feed", protocol: "query",
  onSnapshot: () => {}, onDelta: (changes) => { deltaReceived = changes; },
  onUpdate: () => {}, onNack: null, ref: null,
});
c._dispatch({ type: "delta", service: "feed", changes: [{ op: "insert", row: { id: 1 } }] });
results.deltaReceived = deltaReceived;

// send auto-generates msgid
FakeWS.sent = [];
mkio.send("orders", { id: 1 }, { op: "new" });
results.sendMsgid = FakeWS.sent[0].msgid;
results.sendHasMsgid = typeof FakeWS.sent[0].msgid === "string" && FakeWS.sent[0].msgid.startsWith("_mkio_");

// send preserves explicit msgid
FakeWS.sent = [];
mkio.send("orders", { id: 2 }, { op: "new", msgid: "user-123" });
results.sendExplicitMsgid = FakeWS.sent[0].msgid;

// nack default handler (just verify it doesn't throw)
FakeWS.sent = [];
const nackSub = mkio.query("nack_test");
// Find the subscription by its generated subid (keyed by subid, not service name)
const nackSubId = nackSub.service;  // MkioSubscription.service holds the key (subid)
const nackSubObj = c._subscriptions.get(nackSubId);
if (nackSubObj && nackSubObj.onNack) {
  nackSubObj.onNack("test rejection", {});
  results.nackHandlerExists = true;
} else {
  results.nackHandlerExists = false;
}

console.log(JSON.stringify(results));
""".replace("PATH", json.dumps(str(JS_CLIENT_PATH)))

    result = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, check=True
    )
    out = json.loads(result.stdout.strip().splitlines()[-1])

    # subpub without topic returns undefined
    assert out["subpubNoTopic"] is True

    # subpub sends correct message with auto-generated subid
    assert out["subpubSent"]["protocol"] == "subpub"
    assert out["subpubSent"]["topic"] == "AAPL"
    assert out["subpubSent"]["fields"] == ["bid", "ask"]
    assert out["subpubSent"]["subid"].startswith("_mkio_")
    assert out["subpubHasStop"] is True

    # stream auto-generates ref and subid
    assert out["streamSent"]["protocol"] == "stream"
    assert out["streamHasRef"] is True
    assert out["streamSent"]["subid"].startswith("_mkio_")

    # stream with explicit ref
    assert out["streamExplicitRef"] == "custom-ref"

    # query with filter and auto-generated subid
    assert out["querySent"]["protocol"] == "query"
    assert out["querySent"]["filter"] == "status == 'open'"
    assert out["querySent"]["subid"].startswith("_mkio_")

    # query snapshotOnly sets updates=false
    assert out["querySnapshotOnly"].get("updates") is False
    assert "snapshot" not in out["querySnapshotOnly"] or out["querySnapshotOnly"].get("snapshot") is not False

    # query updateOnly sets snapshot=false
    assert out["queryUpdateOnly"].get("snapshot") is False
    assert "updates" not in out["queryUpdateOnly"] or out["queryUpdateOnly"].get("updates") is not False

    # query mutual exclusion
    assert out["queryMutualExclusion"] is True

    # delta dispatch
    assert out["deltaReceived"] == [{"op": "insert", "row": {"id": 1}}]

    # send auto-generates msgid with _mkio_ prefix
    assert out["sendHasMsgid"] is True

    # send preserves explicit msgid
    assert out["sendExplicitMsgid"] == "user-123"

    # nack default handler exists
    assert out["nackHandlerExists"] is True


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_client_monitor_filter_runtime():
    """Verify monitor filter function works at runtime."""
    script = r"""
global.WebSocket = class {
  constructor(url) { this.url = url; this.readyState = 1; this.binaryType = null; }
  send() {}
  close() {}
};
global.WebSocket.OPEN = 1;

const { MkioClient } = require(PATH);
const c = new MkioClient("ws://localhost:8080/ws");
c._ws = new WebSocket("ws://localhost:8080/ws");

const entries = [];
c.monitor({
  filter: (e) => e.direction === "in",
  fn: (e) => entries.push({ dir: e.direction, svc: e.service }),
});

// outbound should be filtered out
c.send("orders", { x: 1 }, { ref: "r1" });
// inbound should pass through
c._dispatch({ type: "result", ref: "r1", ok: true, service: "orders" });

console.log(JSON.stringify({ count: entries.length, entries }));
""".replace("PATH", json.dumps(str(JS_CLIENT_PATH)))

    result = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, check=True
    )
    out = json.loads(result.stdout.strip().splitlines()[-1])

    assert out["count"] == 1
    assert out["entries"][0]["dir"] == "in"
    assert out["entries"][0]["svc"] == "orders"


def test_js_client_no_external_deps():
    """Verify no imports/requires of external packages."""
    src = JS_CLIENT_PATH.read_text()
    # Should not import anything
    lines = src.split("\n")
    imports = [l for l in lines if l.strip().startswith("import ") and "from" in l]
    requires = [l for l in lines if "require(" in l and "module.exports" not in l]
    assert len(imports) == 0, f"Found external imports: {imports}"
    assert len(requires) == 0, f"Found external requires: {requires}"
