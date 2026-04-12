/**
 * mkio JavaScript client library.
 * Auto-served at /mkio.js by the mkio server.
 * No dependencies, no CDN, ES module compatible.
 */

// ---------------------------------------------------------------------------
// Ref generator (same YYYYMMDD HH:mm:ss.mmmuuunnnppp format as server)
// ---------------------------------------------------------------------------

let _lastNs = 0n;
let _counter = 0;

// Pick a nanosecond clock once. Adding performance.timeOrigin (~1.77e12 ms)
// to performance.now() as a single double loses ~500ns of precision, so we
// keep the wall-clock anchor and the high-res offset separate.
let _nowNs;
if (typeof process !== "undefined" && process.hrtime && process.hrtime.bigint) {
  const _originNs = BigInt(Math.trunc(performance.timeOrigin * 1e6));
  const _startHr = process.hrtime.bigint();
  _nowNs = () => _originNs + (process.hrtime.bigint() - _startHr);
} else if (
  typeof performance !== "undefined" &&
  typeof performance.now === "function" &&
  typeof performance.timeOrigin === "number"
) {
  const _originNs = BigInt(Math.trunc(performance.timeOrigin)) * 1_000_000n;
  _nowNs = () => _originNs + BigInt(Math.round(performance.now() * 1e6));
} else {
  _nowNs = () => BigInt(Date.now()) * 1_000_000n;
}

function makeRef() {
  // Tiered monotonic counter: counter fills the picosecond slot first
  // (preserving real platform precision), then spills into _lastNs on
  // overflow so refs stay strictly increasing without wrapping. Chrome
  // clamps performance.now() to 100us in non-COI contexts, which is why
  // the spill path matters there.
  let nowNs = _nowNs();
  if (nowNs > _lastNs) {
    _lastNs = nowNs;
    _counter = 0;
  } else {
    _counter++;
    if (_counter >= 1000) {
      _lastNs = _lastNs + 1n;
      _counter = 0;
    }
    nowNs = _lastNs;
  }

  const epochMs = Number(nowNs / 1_000_000n);
  const subNs = Number(nowNs % 1_000_000n);

  const d = new Date(epochMs);
  const yyyy = d.getUTCFullYear();
  const MM = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const HH = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  const ms = String(d.getUTCMilliseconds()).padStart(3, "0");
  const us = String(Math.floor(subNs / 1000)).padStart(3, "0");
  const ns = String(subNs % 1000).padStart(3, "0");
  const ps = String(_counter).padStart(3, "0");

  return `${yyyy}${MM}${dd} ${HH}:${mm}:${ss}.${ms}${us}${ns}${ps}`;
}

// ---------------------------------------------------------------------------
// Default monitor formatter (styled console output)
// ---------------------------------------------------------------------------

function defaultMonitorFormatter(entry) {
  const { direction, service, type, ref, msg } = entry;
  const arrow = direction === "out" ? "\u2192" : "\u2190";
  const arrowStyle =
    direction === "out"
      ? "color:#2a8a2a;font-weight:bold"
      : "color:#2a5fa8;font-weight:bold";
  const typeStyle = type === "error" ? "color:#c33;font-weight:bold" : "color:#888";
  const refStyle = "color:#aaa";
  const svc = service || "?";
  const typ = type || "(no type)";
  const refStr = ref ? `ref=${ref}` : "";
  // eslint-disable-next-line no-console
  console.groupCollapsed(
    `%c${arrow} ${svc}%c ${typ} %c${refStr}`,
    arrowStyle,
    typeStyle,
    refStyle
  );
  // eslint-disable-next-line no-console
  console.log(msg);
  // eslint-disable-next-line no-console
  console.groupEnd();
}

// ---------------------------------------------------------------------------
// MkioClient
// ---------------------------------------------------------------------------

class MkioClient {
  /**
   * @param {string} url - WebSocket URL (e.g., "ws://localhost:8080/ws")
   * @param {Object} [opts]
   * @param {boolean} [opts.reconnect=true]
   * @param {number} [opts.backoffBase=100] - Initial backoff in ms
   * @param {number} [opts.backoffMax=30000] - Max backoff in ms
   * @param {Function} [opts.onConnect] - Called on (re)connect
   * @param {Function} [opts.onDisconnect] - Called on disconnect
   */
  constructor(url, opts = {}) {
    this.url = url;
    this.reconnect = opts.reconnect !== false;
    this.backoffBase = opts.backoffBase || 100;
    this.backoffMax = opts.backoffMax || 30000;
    this.onConnect = opts.onConnect || (() => {});
    this.onDisconnect = opts.onDisconnect || (() => {});

    this._ws = null;
    this._pending = new Map(); // ref -> {resolve, reject, service}
    this._subscriptions = new Map(); // service -> subscription
    this._backoff = this.backoffBase;
    this._closed = false;
    this._reconnectTimer = null;

    // Debugging: monitor hook and service-usage tracking.
    this._monitor = null; // null | {services: Set|null, fn}
    this._seenServices = new Set();

    MkioClient._instances.add(this);
  }

  static makeRef = makeRef;
  static _instances = new Set();

  /** Return live client instances (for console poking). */
  static instances() {
    return Array.from(MkioClient._instances);
  }

  connect() {
    return new Promise((resolve, reject) => {
      // Bind to this specific socket: if a later reconnect replaces
      // this._ws, the handlers below must not act on the stale one.
      const ws = new WebSocket(this.url);
      ws.binaryType = "arraybuffer";
      this._ws = ws;

      ws.onopen = () => {
        if (ws !== this._ws || this._closed) return;
        this._backoff = this.backoffBase;
        this._resubscribe();
        this.onConnect();
        resolve();
      };

      ws.onmessage = (event) => {
        if (ws !== this._ws) return;
        let data;
        if (typeof event.data === "string") {
          data = JSON.parse(event.data);
        } else {
          const text = new TextDecoder().decode(event.data);
          data = JSON.parse(text);
        }
        this._dispatch(data);
      };

      ws.onclose = () => {
        if (ws !== this._ws) return;
        this.onDisconnect();
        if (!this._closed && this.reconnect) {
          this._attemptReconnect();
        }
      };

      ws.onerror = (err) => {
        if (ws !== this._ws) return;
        if (ws.readyState !== WebSocket.OPEN) {
          reject(err);
        }
      };
    });
  }

  close() {
    this._closed = true;
    if (this._reconnectTimer !== null) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._ws) {
      this._ws.close();
    }
    MkioClient._instances.delete(this);
  }

  /**
   * Send a transaction message and wait for the result.
   * @param {string} service
   * @param {Object} data
   * @param {Object} [opts] - May include ref, msgid, op, etc.
   * @returns {Promise<Object>}
   */
  send(service, data, opts = {}) {
    const ref = opts.ref || makeRef();
    const msg = { ...opts, service, data, ref };

    return new Promise((resolve, reject) => {
      this._pending.set(ref, { resolve, reject, service });
      this._sendRaw(msg);
    });
  }

  /**
   * Subscribe to a service with callbacks.
   * @param {string} service
   * @param {Object} opts
   * @param {string} [opts.filter]
   * @param {string} [opts.ref] - Ref from last received message for recovery
   * @param {Function} [opts.onSnapshot] - (rows) => void
   * @param {Function} [opts.onDelta] - (changes) => void
   * @param {Function} [opts.onUpdate] - (op, row) => void
   */
  subscribe(service, opts = {}) {
    const sub = {
      service,
      filter: opts.filter || null,
      ref: opts.ref || null,
      onSnapshot: opts.onSnapshot || (() => {}),
      onDelta: opts.onDelta || (() => {}),
      onUpdate: opts.onUpdate || (() => {}),
    };
    this._subscriptions.set(service, sub);

    const msg = { service, type: "subscribe" };
    if (sub.filter) msg.filter = sub.filter;
    if (sub.ref) msg.ref = sub.ref;

    this._sendRaw(msg);
  }

  /**
   * Unsubscribe from a service.
   * @param {string} service
   */
  unsubscribe(service) {
    this._subscriptions.delete(service);
    const msg = { service, type: "unsubscribe", ref: makeRef() };
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._sendRaw(msg);
    }
  }

  /**
   * Check if a transaction committed (for recovery after reconnect).
   * @param {string} service
   * @param {string} ref
   * @returns {Promise<Object>}
   */
  check(service, ref) {
    const msg = { service, type: "check", ref };

    return new Promise((resolve, reject) => {
      this._pending.set(ref, { resolve, reject, service });
      this._sendRaw(msg);
    });
  }

  // -- Debugging ------------------------------------------------------------

  /**
   * Enable, modify, or disable the per-instance monitor hook.
   * See the module-level `mkio("monitor", ...)` dispatcher for the usual
   * console entry point; this method is the underlying per-client control.
   */
  monitor(opts) {
    if (opts === false || opts === "off") {
      this._monitor = null;
      return null;
    }
    const fn =
      (opts && typeof opts === "object" && opts.fn) || defaultMonitorFormatter;
    let services = null;
    if (opts && typeof opts === "object" && opts.services) {
      services = new Set(opts.services);
    } else if (this._monitor) {
      services = this._monitor.services;
    }
    this._monitor = { services, fn };
    return this._monitor;
  }

  // -- Private ---------------------------------------------------------------

  _sendRaw(msg) {
    if (msg && msg.service) {
      this._seenServices.add(msg.service);
    }
    this._emitMonitor("out", msg);
    this._ws.send(JSON.stringify(msg));
  }

  _emitMonitor(direction, msg) {
    if (this._monitor === null) return;
    let service = msg && msg.service;
    if (!service && msg && msg.ref && this._pending.has(msg.ref)) {
      service = this._pending.get(msg.ref).service;
    }
    if (this._monitor.services && service && !this._monitor.services.has(service)) {
      return;
    }
    const entry = {
      direction,
      service: service || null,
      type: msg && msg.type,
      ref: msg && msg.ref,
      msg,
      ts: Date.now(),
    };
    try {
      this._monitor.fn(entry);
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error("mkio monitor formatter threw:", e);
    }
  }

  _dispatch(data) {
    this._emitMonitor("in", data);

    const { type, ref, service } = data;
    if (service) {
      this._seenServices.add(service);
    } else if (ref && this._pending.has(ref)) {
      const pendingSvc = this._pending.get(ref).service;
      if (pendingSvc) this._seenServices.add(pendingSvc);
    }

    // Route to pending promise (only result/error, not subscription updates)
    if (ref && this._pending.has(ref) && (type === "result" || type === "error")) {
      const { resolve } = this._pending.get(ref);
      this._pending.delete(ref);
      resolve(data);
      return;
    }

    // Route to subscription
    if (service && this._subscriptions.has(service)) {
      const sub = this._subscriptions.get(service);
      if (data.ref) sub.ref = data.ref;

      if (type === "snapshot") {
        sub.onSnapshot(data.rows);
      } else if (type === "delta") {
        sub.onDelta(data.changes);
      } else if (type === "update") {
        sub.onUpdate(data.op, data.row);
      }
    }
  }

  _resubscribe() {
    for (const [service, sub] of this._subscriptions) {
      const msg = { service, type: "subscribe" };
      if (sub.filter) msg.filter = sub.filter;
      if (sub.ref) msg.ref = sub.ref;
      this._sendRaw(msg);
    }
  }

  _attemptReconnect() {
    // Dedupe: one failed connect fires both onerror and onclose, each of
    // which wants to schedule a reconnect. Also blocks rescheduling while
    // a pending attempt has not yet run.
    if (this._reconnectTimer !== null || this._closed) return;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._backoff = Math.min(this._backoff * 2, this.backoffMax);
      this.connect().catch(() => {
        if (!this._closed && this.reconnect) {
          this._attemptReconnect();
        }
      });
    }, this._backoff);
  }
}

// ---------------------------------------------------------------------------
// Console dispatcher: mkio.<verb>(...args) — mirrors the mkio CLI
// ---------------------------------------------------------------------------

const MKIO_HELP = [
  'mkio.help()                               show this help',
  'mkio.services()                           list services this tab has talked to',
  'mkio.services("<name>")                   show detail for one service',
  'mkio.services("*")                        list every service on the server',
  'mkio.monitor()                            tap every service (this tab)',
  'mkio.monitor("<service>")                 tap one service (call again to add more)',
  'mkio.monitor("off")                       stop tapping',
  'mkio.send("<service>", data, {op})        send a transaction',
  'mkio.subscribe("<service>", {filter})     stream live data (returns {stop})',
].join("\n");

function _mkioHttpBase(client) {
  // Derive http(s):// base from the client's ws(s):// URL.
  const u = client.url.replace(/^ws/, "http");
  return u.replace(/\/[^/]*$/, "");
}

function _mkioPickClient() {
  const all = MkioClient.instances();
  if (all.length === 0) {
    // eslint-disable-next-line no-console
    console.warn("mkio: no live MkioClient instances");
    return null;
  }
  if (all.length > 1) {
    // eslint-disable-next-line no-console
    console.warn(`mkio: ${all.length} clients active, using the first`);
  }
  return all[0];
}

async function _mkioServices(arg) {
  const client = _mkioPickClient();
  if (!client) return null;
  if (arg === undefined) {
    const seen = Array.from(client._seenServices).sort();
    const rows = seen.map((s) => ({
      service: s,
      monitored: client._monitor
        ? !client._monitor.services || client._monitor.services.has(s)
        : false,
    }));
    // eslint-disable-next-line no-console
    console.table(rows);
    return seen;
  }
  const base = _mkioHttpBase(client);
  const url = arg === "*" ? `${base}/api/services` : `${base}/api/services/${encodeURIComponent(arg)}`;
  const resp = await fetch(url);
  const data = await resp.json();
  // eslint-disable-next-line no-console
  console.log(data);
  return data;
}

function _mkioMonitor(arg) {
  const clients = MkioClient.instances();
  if (clients.length === 0) {
    // eslint-disable-next-line no-console
    console.warn("mkio: no live MkioClient instances");
    return null;
  }
  let state;
  if (arg === undefined || arg === "*") {
    for (const c of clients) state = c.monitor({ services: null });
    // eslint-disable-next-line no-console
    console.log("mkio: monitoring *");
  } else if (arg === "off" || arg === false) {
    for (const c of clients) c.monitor(false);
    // eslint-disable-next-line no-console
    console.log("mkio: monitor off");
    return null;
  } else if (typeof arg === "string") {
    for (const c of clients) {
      const existing = c._monitor && c._monitor.services ? Array.from(c._monitor.services) : [];
      const next = new Set(existing);
      next.add(arg);
      state = c.monitor({ services: Array.from(next) });
    }
    // eslint-disable-next-line no-console
    console.log(`mkio: monitoring ${Array.from(state.services).join(", ")}`);
  } else if (arg && typeof arg === "object") {
    for (const c of clients) state = c.monitor(arg);
  }
  return state;
}

function _mkioSend(service, data, opts) {
  const client = _mkioPickClient();
  if (!client) return null;
  return client.send(service, data, opts || {});
}

function _mkioSubscribe(service, opts) {
  const client = _mkioPickClient();
  if (!client) return null;
  const o = { ...(opts || {}) };
  if (!o.onSnapshot) {
    o.onSnapshot = (rows) => console.log(`\u2190 ${service} snapshot`, rows);
  }
  if (!o.onDelta) {
    o.onDelta = (changes) => console.log(`\u2190 ${service} delta`, changes);
  }
  if (!o.onUpdate) {
    o.onUpdate = (op, row) => console.log(`\u2190 ${service} update ${op}`, row);
  }
  client.subscribe(service, o);
  return {
    stop: () => client.unsubscribe(service),
  };
}

const mkio = {
  help() {
    // eslint-disable-next-line no-console
    console.log(MKIO_HELP);
  },
  services(name) {
    return _mkioServices(name);
  },
  monitor(arg) {
    return _mkioMonitor(arg);
  },
  send(service, data, opts) {
    return _mkioSend(service, data, opts);
  },
  subscribe(service, opts) {
    return _mkioSubscribe(service, opts);
  },
  instances() {
    return MkioClient.instances();
  },
};

// Export for ES modules and global scope
if (typeof module !== "undefined" && module.exports) {
  module.exports = { MkioClient, makeRef, mkio, defaultMonitorFormatter };
}
if (typeof window !== "undefined") {
  window.MkioClient = MkioClient;
  window.mkioMakeRef = makeRef;
  if (typeof window.mkio === "undefined") {
    window.mkio = mkio;
  } else {
    window.__mkio = mkio;
    // eslint-disable-next-line no-console
    console.warn("mkio: window.mkio already set; exposed as window.__mkio instead");
  }
}
