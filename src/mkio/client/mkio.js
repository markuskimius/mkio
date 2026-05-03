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
// Local display timestamp (YYYYMMDD HH:MM:SS.mmmuuu ±HHMM)
// Not a ref — display only, no monotonic counter.
// ---------------------------------------------------------------------------

function makeLocalTs() {
  const nowNs = _nowNs();
  const epochMs = Number(nowNs / 1_000_000n);
  const subUs = Number((nowNs / 1_000n) % 1_000_000n);
  const d = new Date(epochMs);
  const yyyy = d.getFullYear();
  const MM = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms = String(Math.floor(subUs / 1000)).padStart(3, "0");
  const us = String(subUs % 1000).padStart(3, "0");
  // getTimezoneOffset() returns minutes west of UTC; ISO ±HHMM is east-positive.
  const offMin = -d.getTimezoneOffset();
  const sign = offMin >= 0 ? "+" : "-";
  const absMin = Math.abs(offMin);
  const offH = String(Math.floor(absMin / 60)).padStart(2, "0");
  const offM = String(absMin % 60).padStart(2, "0");
  return `${yyyy}-${MM}-${dd} ${HH}:${mm}:${ss}.${ms}${us} ${sign}${offH}${offM}`;
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
  const ts = makeLocalTs();
  const tsStyle = "color:#888";
  // eslint-disable-next-line no-console
  console.groupCollapsed(
    `%c[${ts}] %c${arrow} ${svc}%c ${typ} %c${refStr}`,
    tsStyle,
    arrowStyle,
    typeStyle,
    refStyle
  );
  // eslint-disable-next-line no-console
  console.log(_nullProtoClone(msg));
  // eslint-disable-next-line no-console
  console.groupEnd();
}

// Deep-clone into null-prototype objects so DevTools shows an expandable
// tree without the "[[Prototype]]: Object" entry on every node.
function _nullProtoClone(v) {
  if (v === null || typeof v !== "object") return v;
  if (Array.isArray(v)) return v.map(_nullProtoClone);
  const out = Object.create(null);
  for (const k of Object.keys(v)) out[k] = _nullProtoClone(v[k]);
  return out;
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
   * @param {number} [opts.backoffMax=1000] - Max backoff in ms
   * @param {Function} [opts.onConnect] - Called on (re)connect
   * @param {Function} [opts.onDisconnect] - Called on disconnect
   */
  constructor(url, opts = {}) {
    this.url = url;
    this.reconnect = opts.reconnect !== false;
    this.backoffBase = opts.backoffBase || 100;
    this.backoffMax = opts.backoffMax || 1000;
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
   * @param {string} [opts.topic] - Topic value (required for subpub)
   * @param {string} [opts.filter] - Expression filter (query only)
   * @param {string} [opts.ref] - Ref from last received message for recovery (stream only)
   * @param {string} [opts.subid] - Subscription ID echoed on all responses
   * @param {string} protocol - Expected protocol ("subpub", "stream", "query"); server rejects on mismatch
   * @param {boolean} [opts.snapshot=true] - Whether to receive the initial snapshot
   * @param {boolean} [opts.updates=true] - Whether to receive live updates
   * @param {string[]} [opts.fields] - Restrict rows to these fields only
   * @param {Function} [opts.onSnapshot] - (rows) => void
   * @param {Function} [opts.onUpdate] - (op, row) => void
   */
  subscribe(service, protocol, opts = {}) {
    const sub = {
      service,
      protocol,
      topic: opts.topic || null,
      filter: opts.filter || null,
      ref: opts.ref || null,
      subid: opts.subid || null,
      snapshot: opts.snapshot !== false,
      updates: opts.updates !== false,
      fields: opts.fields || null,
      maxcount: opts.maxcount || null,
      onSnapshot: opts.onSnapshot || (() => {}),
      onDelta: opts.onDelta || (() => {}),
      onUpdate: opts.onUpdate || (() => {}),
      onNack: opts.onNack || null,
      _snapshotRows: [],
    };
    const key = sub.subid && sub.topic && !Array.isArray(sub.topic)
      ? `${sub.subid}\0${sub.topic}` : (sub.subid || service);
    this._subscriptions.set(key, sub);

    const msg = { service, type: "subscribe", protocol };
    if (sub.topic) msg.topic = sub.topic;
    if (sub.filter) msg.filter = sub.filter;
    if (sub.ref) msg.ref = sub.ref;
    if (sub.subid) msg.subid = sub.subid;
    if (!sub.snapshot) msg.snapshot = false;
    if (!sub.updates) msg.updates = false;
    if (sub.fields) msg.fields = sub.fields;
    if (sub.maxcount) msg.maxcount = sub.maxcount;

    this._sendRaw(msg);
  }

  /**
   * Request the next page of a paginated query snapshot.
   * @param {string} service - Service name
   * @param {string} subid - Subscription ID for the paginating query
   */
  getmore(service, subid) {
    this._sendRaw({ service, type: "getmore", subid });
  }

  /**
   * Unsubscribe from a service.
   * @param {string} serviceOrSubid - Service name or subid used when subscribing
   */
  unsubscribe(serviceOrSubid) {
    // Collect all matching entries (composite keys share the same subid)
    let service = serviceOrSubid;
    let subid = null;
    const directSub = this._subscriptions.get(serviceOrSubid);
    if (directSub) {
      service = directSub.service;
      subid = directSub.subid;
      this._subscriptions.delete(serviceOrSubid);
    }
    // Remove any composite-keyed entries matching this subid
    if (!directSub || subid) {
      for (const [key, sub] of this._subscriptions) {
        if (sub.subid === (subid || serviceOrSubid)) {
          service = sub.service;
          subid = sub.subid;
          this._subscriptions.delete(key);
        }
      }
    }
    const msg = { service, type: "unsubscribe", ref: makeRef() };
    if (subid) msg.subid = subid;
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
    const filter =
      (opts && typeof opts === "object" && opts.filter) || null;
    this._monitor = { services, filter, fn };
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
    if (this._monitor.filter) {
      try {
        if (!this._monitor.filter(entry)) return;
      } catch (_) {
        // filter errors pass through
      }
    }
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

    // Nack: deliver to callback and remove to prevent reconnect retry
    if (type === "nack") {
      const nackKey = data.subid || service;
      for (const [key, sub] of this._subscriptions) {
        if ((sub.subid || sub.service) === nackKey) {
          this._subscriptions.delete(key);
          if (sub.onNack) sub.onNack(data.message, data);
        }
      }
      return;
    }

    // Route to subscription (composite key for subid+topic, else subid or service)
    const topic = (type === "snapshot" && data.rows && data.rows[0])
      ? data.rows[0]._mkio_topic
      : (data.row ? data.row._mkio_topic : null);
    const compositeKey = data.subid && topic != null ? `${data.subid}\0${topic}` : null;
    const fallbackKey = data.subid || service;
    let sub = (compositeKey && this._subscriptions.get(compositeKey))
      || this._subscriptions.get(fallbackKey)
      || this._subscriptions.get(service);
    if (sub) {
      // Track server-assigned subid (pagination auto-assigns one)
      if (data.subid && !sub.subid) {
        const oldKey = sub.service;
        sub.subid = data.subid;
        this._subscriptions.delete(oldKey);
        this._subscriptions.set(data.subid, sub);
      }
      if (data.ref) sub.ref = data.ref;
      if (type === "snapshot") {
        if (sub.maxcount && data.hasmore) {
          sub._snapshotRows = sub._snapshotRows.concat(data.rows);
          if (sub.protocol === "stream") {
            this._sendSubscribe(sub);
          } else {
            this.getmore(sub.service, sub.subid || data.subid);
          }
        } else if (sub.maxcount && sub.protocol === "stream" && !data.hasmore) {
          if (sub._snapshotRows.length > 0) {
            sub.onSnapshot(sub._snapshotRows.concat(data.rows));
            sub._snapshotRows = [];
          } else {
            sub.onSnapshot(data.rows);
          }
          if (sub.updates) {
            sub.maxcount = null;
            this._sendSubscribe(sub);
          }
        } else if (sub._snapshotRows.length > 0) {
          sub.onSnapshot(sub._snapshotRows.concat(data.rows));
          sub._snapshotRows = [];
        } else {
          sub.onSnapshot(data.rows);
        }
      } else if (type === "delta") {
        sub.onDelta(data.changes);
      } else if (type === "update") {
        sub.onUpdate(data.op, data.row);
      }
    }
  }

  _sendSubscribe(sub) {
    const msg = { service: sub.service, type: "subscribe", protocol: sub.protocol };
    if (sub.topic) msg.topic = sub.topic;
    if (sub.filter) msg.filter = sub.filter;
    if (sub.ref) msg.ref = sub.ref;
    if (sub.subid) msg.subid = sub.subid;
    if (!sub.snapshot) msg.snapshot = false;
    if (!sub.updates) msg.updates = false;
    if (sub.fields) msg.fields = sub.fields;
    if (sub.maxcount) msg.maxcount = sub.maxcount;
    this._sendRaw(msg);
  }

  _resubscribe() {
    for (const [, sub] of this._subscriptions) {
      sub._snapshotRows = [];
      this._sendSubscribe(sub);
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
  'mkio.help()                                          show this help',
  'mkio.services()                                      list every service on the server',
  'mkio.services("<name>")                              show detail for one service',
  'mkio.monitor()                                       tap every service (this tab)',
  'mkio.monitor("<service>")                            tap one service (call again to add more)',
  'mkio.monitor({filter: fn})                           tap with client-side filter function',
  'mkio.monitor("off")                                  stop tapping',
  'mkio.send("<service>", data, {op, ref, msgid})       send a transaction',
  'mkio.subpub("<svc>", "<topic>"|["t1","t2"], {fields, subid})  subscribe to a subpub service',
  'mkio.stream("<svc>", {ref, filter, fields, subid})   subscribe to a stream service',
  'mkio.query("<svc>", {filter, fields, subid,          subscribe to a query service',
  '                     snapshotOnly, updateOnly})',
  'mkio.instances()                                     list live MkioClient instances',
].join("\n");

function _mkioHttpBase(client) {
  // Derive http(s):// base from the client's ws(s):// URL.
  const u = client.url.replace(/^ws/, "http");
  return u.replace(/\/[^/]*$/, "");
}

function _formatServiceDetail(detail) {
  const lines = [];
  const desc = detail.description || "";
  lines.push(`Service: ${detail.name} (${detail.protocol})`);
  if (desc) lines.push(`  ${desc}`);
  lines.push("");

  if (detail.protocol === "transaction") {
    const ops = detail.ops || {};
    const opNames = Object.keys(ops);
    if (opNames.length > 0) {
      const nameW = Math.max(2, ...opNames.map((n) => n.length));
      lines.push("  Operations:");
      lines.push("");
      for (const opName of opNames) {
        const op = ops[opName];
        const steps = op.steps || [];
        const primary = steps[0] || {};
        const fields = primary.fields || {};
        const parts = [];
        for (const [f, info] of Object.entries(fields)) {
          if (info.key) parts.push(`${f}* (key)`);
          else if (info.required) parts.push(`${f}*`);
          else if (info.default) parts.push(`${f}=${info.default}`);
          else parts.push(f);
        }
        const fieldStr = parts.length > 0 ? parts.join(", ") : "(no fields)";
        const descStr = op.description ? `  — ${op.description}` : "";
        lines.push(`    ${opName.padEnd(nameW)}  ${fieldStr}${descStr}`);
      }
      lines.push("");
      for (const opName of opNames) {
        const op = ops[opName];
        const steps = op.steps || [];
        if (!steps.length) continue;
        const primary = steps[0];
        const fields = primary.fields || {};
        const auto = primary.auto || {};
        if (!Object.keys(fields).length && !Object.keys(auto).length) continue;
        lines.push(`  ${opName}:`);
        if (Object.keys(fields).length) {
          const colW = Math.max(...Object.keys(fields).map((f) => f.length));
          for (const [f, info] of Object.entries(fields)) {
            const typ = info.type || "";
            const notes = [];
            if (info.key) notes.push("required, key");
            else if (info.required) notes.push("required");
            if (info.default) notes.push(`default: ${info.default}`);
            const noteStr = notes.length ? `  ${notes.join(", ")}` : "";
            lines.push(`    ${f.padEnd(colW)}  ${typ.padEnd(10)}${noteStr}`);
          }
        }
        if (Object.keys(auto).length) {
          const autoParts = Object.entries(auto).map(([f, info]) => `${f} (${info.source || ""})`);
          lines.push(`    auto: ${autoParts.join(", ")}`);
        }
        for (const step of steps.slice(1)) {
          const bind = step.bind || {};
          if (Object.keys(bind).length) {
            const boundParts = Object.entries(bind).map(([k, v]) => `${k}=${v}`);
            lines.push(`    + ${step.table}: ${boundParts.join(", ")}`);
          }
        }
        if (op.example) lines.push(`    example: ${op.example}`);
        lines.push("");
      }
    }
    const recovery = detail.recovery;
    if (recovery) {
      lines.push("  Recovery:");
      lines.push(`    ${recovery.description}`);
      if (recovery.check_message) lines.push(`    Check: ${JSON.stringify(recovery.check_message)}`);
      lines.push("");
    }
  } else {
    if (detail.primary_table) lines.push(`  Table: ${detail.primary_table}`);
    if (detail.topic) lines.push(`  Topic: ${detail.topic}`);
    const filterable = detail.filterable || [];
    if (filterable.length) lines.push(`  Filter by: ${filterable.join(", ")}`);
    const schema = detail.schema || {};
    if (Object.keys(schema).length) {
      lines.push("");
      lines.push("  Schema:");
      const colW = Math.max(...Object.keys(schema).map((f) => f.length));
      for (const [f, info] of Object.entries(schema)) {
        const typ = info.type || "";
        const note = info.pk ? "  (primary key)" : "";
        lines.push(`    ${f.padEnd(colW)}  ${typ}${note}`);
      }
    }
    const subscribe = detail.subscribe || {};
    if (Object.keys(subscribe).length) {
      lines.push("");
      lines.push("  Subscribe protocol:");
      if (subscribe.message) lines.push(`    Message: ${JSON.stringify(subscribe.message)}`);
      if (subscribe.response_types) lines.push(`    Response types: ${subscribe.response_types.join(", ")}`);
      if (subscribe.recovery) lines.push(`    Recovery: ${subscribe.recovery}`);
      const logSize = subscribe.change_log_size || subscribe.buffer_size;
      if (logSize) {
        const label = subscribe.buffer_size ? "buffer_size" : "change_log_size";
        lines.push(`    ${label}: ${logSize.toLocaleString()}`);
      }
    }
    const example = detail.example || {};
    if (Object.keys(example).length) {
      lines.push("");
      lines.push("  Example:");
      for (const cmd of Object.values(example)) lines.push(`    ${cmd}`);
    }
  }
  return lines.join("\n");
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
  if (!client) return undefined;
  const base = _mkioHttpBase(client);
  if (arg === undefined || arg === "*") {
    const resp = await fetch(`${base}/api/services`);
    const services = await resp.json();
    if (!services || services.length === 0) {
      // eslint-disable-next-line no-console
      console.log("No services available.");
      return undefined;
    }
    const nameW = Math.max(7, ...services.map((s) => s.name.length));
    const protoW = Math.max(8, ...services.map((s) => s.protocol.length));
    const lines = [];
    lines.push(`${"SERVICE".padEnd(nameW)}  ${"PROTOCOL".padEnd(protoW)}  DETAILS`);
    lines.push(`${"-".repeat(nameW)}  ${"-".repeat(protoW)}  ${"-".repeat(30)}`);
    for (const svc of services) {
      const details = [];
      if (svc.primary_table) details.push(`table=${svc.primary_table}`);
      if (svc.tables) details.push(`tables=${svc.tables.join(",")}`);
      if (svc.watch_tables) details.push(`watch=${svc.watch_tables.join(",")}`);
      lines.push(`${svc.name.padEnd(nameW)}  ${svc.protocol.padEnd(protoW)}  ${details.join(", ")}`);
    }
    // eslint-disable-next-line no-console
    console.log(lines.join("\n"));
    return undefined;
  }
  const resp = await fetch(`${base}/api/services/${encodeURIComponent(arg)}`);
  if (resp.status === 404) {
    // eslint-disable-next-line no-console
    console.warn(`Unknown service: ${arg}`);
    return undefined;
  }
  const detail = await resp.json();
  // eslint-disable-next-line no-console
  console.log(_formatServiceDetail(detail));
  return undefined;
}

function _mkioMonitor(arg) {
  const clients = MkioClient.instances();
  if (clients.length === 0) {
    // eslint-disable-next-line no-console
    console.warn("mkio: no live MkioClient instances");
    return undefined;
  }
  let state;
  if (arg === undefined || arg === "*") {
    for (const c of clients) state = c.monitor({ services: null });
    return "mkio: monitoring *";
  }
  if (arg === "off" || arg === false) {
    for (const c of clients) c.monitor(false);
    return "mkio: monitor off";
  }
  if (typeof arg === "string") {
    for (const c of clients) {
      const existing = c._monitor && c._monitor.services ? Array.from(c._monitor.services) : [];
      const next = new Set(existing);
      next.add(arg);
      state = c.monitor({ services: Array.from(next) });
    }
    return `mkio: monitoring ${Array.from(state.services).join(", ")}`;
  }
  if (arg && typeof arg === "object") {
    for (const c of clients) state = c.monitor(arg);
    if (!state) return "mkio: monitor off";
    return state.services
      ? `mkio: monitoring ${Array.from(state.services).join(", ")}`
      : "mkio: monitoring *";
  }
  return undefined;
}

let _mkioConsoleSeq = 0;
function _mkioConsoleId() {
  const hex = Array.from(crypto.getRandomValues(new Uint8Array(4)),
    (b) => b.toString(16).padStart(2, "0")).join("");
  return `_mkio_${(++_mkioConsoleSeq).toString(36)}_${hex}`;
}

function _mkioSend(service, data, opts) {
  const client = _mkioPickClient();
  if (!client) return null;
  const o = { ...(opts || {}) };
  if (!o.msgid) o.msgid = _mkioConsoleId();
  return client.send(service, data, o);
}

class MkioSubscription {
  constructor(key, client) {
    this.service = key;
    Object.defineProperty(this, "_client", { value: client, enumerable: false });
  }
  stop() {
    this._client.unsubscribe(this.service);
  }
}

function _mkioSubscribe(service, protocol, opts) {
  const client = _mkioPickClient();
  if (!client) return undefined;
  const o = { ...(opts || {}) };
  if (!o.subid) o.subid = _mkioConsoleId();
  if (!o.onSnapshot) {
    o.onSnapshot = (rows) => console.log(`[${makeLocalTs()}] \u2190 ${service} snapshot`, rows);
  }
  if (!o.onDelta) {
    o.onDelta = (changes) => console.log(`[${makeLocalTs()}] \u2190 ${service} delta`, changes);
  }
  if (!o.onUpdate) {
    o.onUpdate = (op, row) => console.log(`[${makeLocalTs()}] \u2190 ${service} update ${op}`, row);
  }
  if (!o.onNack) {
    o.onNack = (message) => console.warn(`[${makeLocalTs()}] \u2190 ${service} nack: ${message}`);
  }
  client.subscribe(service, protocol, o);
  return new MkioSubscription(o.subid, client);
}

const mkio = {
  help() {
    // eslint-disable-next-line no-console
    console.log(MKIO_HELP);
    return undefined;
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
  subpub(service, topic, opts) {
    if (topic === undefined || topic === null || (Array.isArray(topic) && topic.length === 0)) {
      // eslint-disable-next-line no-console
      console.warn("mkio.subpub: topic is required (string or non-empty array)");
      return undefined;
    }
    return _mkioSubscribe(service, "subpub", { ...opts, topic });
  },
  stream(service, opts) {
    const o = { ...(opts || {}) };
    if (!o.ref) o.ref = makeRef();
    return _mkioSubscribe(service, "stream", o);
  },
  query(service, opts) {
    const o = { ...(opts || {}) };
    if (o.snapshotOnly && o.updateOnly) {
      // eslint-disable-next-line no-console
      console.warn("mkio.query: snapshotOnly and updateOnly are mutually exclusive");
      return undefined;
    }
    if (o.snapshotOnly) {
      o.updates = false;
      delete o.snapshotOnly;
    }
    if (o.updateOnly) {
      o.snapshot = false;
      delete o.updateOnly;
    }
    return _mkioSubscribe(service, "query", o);
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
