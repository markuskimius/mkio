/**
 * mkio JavaScript client library.
 * Auto-served at /mkio.js by the mkio server.
 * No dependencies, no CDN, ES module compatible.
 */

// ---------------------------------------------------------------------------
// Ref generator (same YYYYMMDD HH:mm:ss.mmmuuunnnppp format as server)
// ---------------------------------------------------------------------------

let _lastMs = 0;
let _counter = 0;

function makeRef() {
  const now = Date.now();
  if (now <= _lastMs) {
    _counter++;
  } else {
    _counter = 0;
    _lastMs = now;
  }

  const d = new Date(now);
  const yyyy = d.getUTCFullYear();
  const MM = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const HH = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  const ms = String(d.getUTCMilliseconds()).padStart(3, "0");
  // JS only has ms precision; fill us/ns/ps with counter
  const sub = String(_counter).padStart(9, "0");

  return `${yyyy}${MM}${dd} ${HH}:${mm}:${ss}.${ms}${sub}`;
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
    this._pending = new Map(); // ref -> {resolve, reject}
    this._subscriptions = new Map(); // service -> subscription
    this._backoff = this.backoffBase;
    this._closed = false;
  }

  static makeRef = makeRef;

  connect() {
    return new Promise((resolve, reject) => {
      this._ws = new WebSocket(this.url);
      this._ws.binaryType = "arraybuffer";

      this._ws.onopen = () => {
        this._backoff = this.backoffBase;
        this._resubscribe();
        this.onConnect();
        resolve();
      };

      this._ws.onmessage = (event) => {
        let data;
        if (typeof event.data === "string") {
          data = JSON.parse(event.data);
        } else {
          const text = new TextDecoder().decode(event.data);
          data = JSON.parse(text);
        }
        this._dispatch(data);
      };

      this._ws.onclose = () => {
        this.onDisconnect();
        if (!this._closed && this.reconnect) {
          this._attemptReconnect();
        }
      };

      this._ws.onerror = (err) => {
        if (this._ws.readyState !== WebSocket.OPEN) {
          reject(err);
        }
      };
    });
  }

  close() {
    this._closed = true;
    if (this._ws) {
      this._ws.close();
    }
  }

  /**
   * Send a transaction message and wait for the result.
   * @param {string} service
   * @param {Object} data
   * @param {string} [ref]
   * @returns {Promise<Object>}
   */
  send(service, data, opts = {}) {
    const ref = opts.ref || makeRef();
    const msg = { service, data, ref, ...opts };
    delete msg.ref;  // already set above
    msg.ref = ref;

    return new Promise((resolve, reject) => {
      this._pending.set(ref, { resolve, reject });
      this._ws.send(JSON.stringify(msg));
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

    this._ws.send(JSON.stringify(msg));
  }

  /**
   * Unsubscribe from a service.
   * @param {string} service
   */
  unsubscribe(service) {
    this._subscriptions.delete(service);
    const msg = { service, type: "unsubscribe", ref: makeRef() };
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(msg));
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
      this._pending.set(ref, { resolve, reject });
      this._ws.send(JSON.stringify(msg));
    });
  }

  // -- Private ---------------------------------------------------------------

  _dispatch(data) {
    const { type, ref, service } = data;

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
      this._ws.send(JSON.stringify(msg));
    }
  }

  _attemptReconnect() {
    setTimeout(() => {
      this._backoff = Math.min(this._backoff * 2, this.backoffMax);
      this.connect().catch(() => {
        if (!this._closed && this.reconnect) {
          this._attemptReconnect();
        }
      });
    }, this._backoff);
  }
}

// Export for ES modules and global scope
if (typeof module !== "undefined" && module.exports) {
  module.exports = { MkioClient, makeRef };
}
if (typeof window !== "undefined") {
  window.MkioClient = MkioClient;
  window.mkioMakeRef = makeRef;
}
