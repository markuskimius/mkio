# mkio

[![PyPI](https://img.shields.io/pypi/v/mkio?v=3)](https://pypi.org/project/mkio/)
[![Python](https://img.shields.io/pypi/pyversions/mkio?v=3)](https://pypi.org/project/mkio/)
[![License](https://img.shields.io/pypi/l/mkio?v=3)](https://github.com/markuskimius/mkio/blob/main/LICENSE)

Config-driven microservice framework for Python. Define your schema, services, and data flows in a TOML file — zero coding required for standard configurations.

A single TCP port serves HTTP and WebSocket, backed by an embedded SQLite database. Designed for restricted environments where runtime downloads aren't possible — everything installs via `pip`.

## Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Service Types](#service-types)
- [WebSocket Protocol](#websocket-protocol)
- [Client Libraries](#client-libraries)
- [Expression Language](#expression-language)
- [Performance](#performance)
- [CLI Tools](#cli-tools)
- [Using mkio from a Claude-Based Project](#using-mkio-from-a-claude-based-project)
- [Schema Migration](#schema-migration)
- [License](#license)

## Quick Start

```bash
pip install mkio
```

Create `mkio.toml`:

```toml
port = 8080

[tables.orders]
columns = { id = "TEXT PRIMARY KEY", symbol = "TEXT NOT NULL", qty = "INTEGER", status = "TEXT DEFAULT 'pending'" }

[services.add_order]
protocol = "transaction"
table = "orders"
op_type = "insert"
fields = ["id", "symbol", "qty"]

[services.all_orders]
protocol = "query"
primary_table = "orders"
filterable = ["status", "symbol"]

[static]
"/" = "./static"
```

Run:

```bash
mkio serve
```

Or programmatically:

```python
from mkio import serve
serve("mkio.toml")
serve({...})  # or pass a dict
```

## Features

- **Single port** — HTTP pages and WebSocket messages on one port
- **Config-driven** — define tables, transactions, and live data services in TOML
- **Transaction services** — insert, update, delete, upsert across multiple tables atomically
- **SubPub** — topic-based single-row subscription with live push, server-side `where` filtering and `publish` formatting, expression-based defaults for missing topics
- **Stream** — append-only ring buffer with cursor-based reconnection
- **Query** — snapshot + change feed from SQLite
- **ReqRep** — one-shot request-reply with parameterized SQL and/or expression evaluation, returning scalar values, single records, or result sets
- **Expression language** — safe, extensible filter and formatter expressions (`qty > 100 AND status == 'pending'`)
- **Schema migration** — automatic detection of safe/destructive changes with interactive confirmation
- **Write batching** — hundreds of writes committed in a single SQLite transaction for high throughput
- **Reconnection recovery** — stream services use ref-based cursor reconnection persisted across server restarts via `_mkio_ref` column; subpub and query always replay a full snapshot
- **Field projection** — subscribers can request specific fields per subscription, reducing payload size. Framework fields (`_mkio_ref`, `_mkio_row`, `_mkio_topic`, `_mkio_exists`) are always preserved through projection
- **Client libraries** — Python and JavaScript clients with auto-reconnect and ref tracking
- **Graceful shutdown** — drains pending writes, checkpoints WAL, clean close
- **Service monitoring** — tap into any service's inbound/outbound message flow via CLI or WebSocket
- **Service discovery** — `GET /api/services` list and `GET /api/services/<name>` detail endpoints, `mkio services` CLI
- **Connection identity** — built-in `_mkio` reqrep service reports server name, version, framework version, protocol version, services, tables, config hash, and uptime — lets clients verify they're connected to the correct session
- **Config endpoint** — `/config` path serves TOML files as JSON (request `foo.json`, server reads `foo.toml` and returns JSON); falls back to literal `.json` files; other extensions served as-is
- **CLI tools** — send transactions, subscribe to live data, monitor traffic, inspect services

## Service Types

### Transaction

Execute INSERT, UPDATE, DELETE, or UPSERT operations. Supports multi-table atomic transactions with named ops and cross-op bind references.

```toml
[services.orders]
protocol = "transaction"

[services.orders.ops]
new = [
    { table = "orders", op_type = "insert", fields = ["side", "symbol", "qty", "price"] },
    { table = "audit_log", op_type = "insert", defaults = { event = "new" }, bind = { order_id = "$0.id", status = "$0.status" } },
]
accept = [
    { table = "orders", op_type = "update", key = ["id"], fields = ["status"], defaults = { status = "accepted" } },
    { table = "audit_log", op_type = "insert", defaults = { event = "accepted" }, bind = { order_id = "$0.id", status = "$0.status" } },
]
```

Clients select a named set by sending `"op": "new"` (or `"accept"`, etc.) in the transaction message. For a service with only one workflow, `ops` may instead be a plain list — clients then omit the `op` field.

Bind references (`$N.field`) pull values from a prior op's `RETURNING` row, where `N` is the zero-based index of an earlier op in the same op set. Only `insert`, `update`, and `upsert` ops produce `RETURNING` rows that can be bound against. Op-level `defaults` provide static values the client doesn't need to send — here, `event` and `status` are set automatically per operation.

### SubPub

Subscribe by topic (the `topic` column value) to get a single-row snapshot, then receive live updates as data changes. Every published row includes three framework fields: `_mkio_exists` (whether the topic was found), `_mkio_topic` (the subscribed topic value), and `_mkio_ref` (the timestamp ref of the last write, or `null` for not-found topics). Supports server-side `where` filtering (rows that don't match are never cached or published; once cached, a row that stops matching is frozen at its last matching state — no eviction, no notification), `publish` formatting with expressions, configurable `defaults` (expression strings) for topics that don't exist yet, and custom `sql` for computed topics or JOINs.

```toml
[services.last_trade]
protocol = "subpub"
primary_table = "orders"
topic = "symbol"
where = "status == 'filled'"
change_log_size = 10000

[services.last_trade.defaults]
price = "0"
time = "''"

[services.last_trade.publish]
symbol = "symbol"
price = "IF(side == 'Buy', price, -price)"
```

Use `sql` with a computed column when the topic doesn't map 1:1 to an existing column:

```toml
[services.last_trade_by_side]
protocol = "subpub"
primary_table = "orders"
topic = "topic_key"
sql = "SELECT *, symbol || ':' || side AS topic_key FROM orders"
where = "status == 'filled'"
```

Clients subscribe with `topic: "AAPL:Buy"`. The `topic` must name a column in the `sql` result set.

### Stream

Append-only data with ring buffer and ref-based cursor reconnection.

```toml
[services.audit_feed]
protocol = "stream"
primary_table = "audit_log"
buffer_size = 10000
```

### Query

Snapshot from SQLite with change feed. Every published row includes `_mkio_row` (primary key identifier) and `_mkio_ref` (timestamp ref of the last write).

```toml
[services.all_orders]
protocol = "query"
primary_table = "orders"
filterable = ["status"]
```

### ReqRep

One-shot request-reply: the client sends a request with data, the server evaluates configured SQL and/or expressions, and returns a reply. No subscriptions or change feeds — pure request-reply. Supports three reply shapes determined by config:

| `sql` | `reply` config | Reply field | Description |
|-------|---------------|-------------|-------------|
| no | `"expr"` (string) | `"value": ...` | Scalar computed from request data |
| no | `{ ... }` (dict) | `"row": {...}` | Single record computed from request data |
| yes | (none) | `"rows": [...]` | Raw SQL result set |
| yes | `{ ... }` (dict) | `"rows": [...]` | SQL rows, each transformed |
| yes | `"expr"` (string) | `"value": ...` | Scalar from first SQL row |

```toml
# Scalar from expression
[services.tax]
protocol = "reqrep"
reply = "ROUND(qty * price * rate, 2)"

# Single record from expressions
[services.invoice]
protocol = "reqrep"
reply = { subtotal = "qty * price", tax = "ROUND(qty * price * 0.08, 2)" }

# SQL result set with params manipulation
[services.search]
protocol = "reqrep"
params = { symbol = "UPPER(symbol)" }
sql = "SELECT * FROM prices WHERE symbol = :symbol"

# SQL rows with per-row transform
[services.holdings]
protocol = "reqrep"
sql = "SELECT p.*, pr.price FROM positions p JOIN prices pr ON p.symbol = pr.symbol WHERE p.account = :account"
reply = { symbol = "symbol", qty = "qty", market_value = "ROUND(qty * price, 2)" }
```

### Connection Identity (`_mkio`)

Every mkio server automatically registers a built-in `_mkio` reqrep service (no config required). Clients can verify they're connected to the correct server by sending a request:

```json
{"type": "request", "service": "_mkio", "reqid": "hello"}
```

Reply:

```json
{
  "type": "reply", "service": "_mkio", "reqid": "hello",
  "row": {
    "name": "order-book-dev",
    "version": "2.1.0",
    "mkio": "0.1.46",
    "protocol": "1.0",
    "services": {"orders": "transaction", "last_trade": "subpub", "all_orders": "query"},
    "tables": ["orders", "audit_log"],
    "config_hash": "a3f7c2b1",
    "uptime": 3621.4,
    "started": "20260517 08:12:03.000000000000"
  }
}
```

| Field | Description |
|-------|-------------|
| `name` | Application name from config `name` key (default `""`) |
| `version` | Application version from config `version` key (default `""`) |
| `mkio` | Framework version |
| `protocol` | Protocol version (semver — bump minor for compatible additions, major for breaking changes) |
| `services` | Map of service name → protocol type |
| `tables` | List of configured table names |
| `config_hash` | Short hex hash of the running config (detects config drift) |
| `uptime` | Seconds since server startup |
| `started` | Server startup time as a ref string |

Set `name` and `version` in your config to identify the application:

```toml
name = "order-book-dev"
version = "2.1.0"
port = 8080
```

From the CLI: `mkio reqrep 8080 _mkio`. From the browser console: `mkio.reqrep("_mkio")`.

#### Version Compatibility

Clients can check whether they're compatible with the server by sending expected version(s) in the request `data`. The server replies with a `compatible` boolean (AND of all checks) and a `compatibility` dict with per-version results. All versions use semantic versioning (caret `^` convention).

```json
{"type": "request", "service": "_mkio", "reqid": "v1",
 "data": {"version": "2.0.0", "protocol": "1.0", "mkio": "0.1.40"}}
```

Reply:

```json
{
  "type": "reply", "service": "_mkio", "reqid": "v1",
  "row": {
    "name": "order-book-dev", "version": "2.3.0", "mkio": "0.1.46", "protocol": "1.0",
    "compatible": true,
    "compatibility": {"version": true, "protocol": true, "mkio": true},
    ...
  }
}
```

If any version is incompatible, `compatible` is `false` and the failing key(s) show `false` in `compatibility`. When no version expectations are sent, neither field appears (backward compatible).

| Field | Type | Description |
|-------|------|-------------|
| `compatible` | bool | `true` if all requested versions are compatible |
| `compatibility` | dict | Per-version result: `{key: true/false}` for each key sent in `data` |

From the CLI: `mkio check 8080 version=2.0.0 protocol=1.0`. From the browser console: `mkio.check({version: "2.0.0", protocol: "1.0"})`. The CLI exits with code 0 if compatible, 1 if not.

The `_mkio` service is hidden from `/api/services` and error hints. Using the wrong protocol (e.g., `mkio subpub 8080 _mkio`) returns a nack with a hint suggesting the correct command.

## WebSocket Protocol

Connect to `/ws` (general) or `/ws/{service_name}` (per-service).

```json
// Transaction
{"service": "add_order", "ref": "...", "data": {"id": "1", "symbol": "AAPL", "qty": 100}}

// Named op transaction
{"service": "orders", "ref": "...", "op": "new", "data": {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150}}

// Transaction with txnid (echoed back on result/error for async correlation)
{"service": "orders", "ref": "...", "op": "new", "txnid": "req-42", "data": {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150}}

// Subscribe (subpub — topic required, protocol required; string or array)
{"service": "last_trade", "type": "subscribe", "protocol": "subpub", "topic": "AAPL"}
{"service": "last_trade", "type": "subscribe", "protocol": "subpub", "topic": ["AAPL", "MSFT", "GOOG"]}

// Subscribe (query — with filter)
{"service": "all_orders", "type": "subscribe", "protocol": "query", "filter": "status == 'pending'"}

// Subscribe with subid (echoed on every snapshot and update for this subscription)
{"service": "all_orders", "type": "subscribe", "protocol": "query", "subid": "my-sub-1"}

// Subscribe with field projection (receive only specified columns)
{"service": "all_orders", "type": "subscribe", "protocol": "query", "fields": ["symbol", "qty"]}

// Subscribe with pagination (server sends at most N rows per snapshot message)
{"service": "all_orders", "type": "subscribe", "protocol": "query", "maxcount": 50, "subid": "q1"}
// → {"type": "snapshot", "service": "all_orders", "subid": "q1", "rows": [...], "hasmore": true}

// Request next page (subid required to identify the subscription)
{"service": "all_orders", "type": "getmore", "subid": "q1"}
// → {"type": "snapshot", "service": "all_orders", "subid": "q1", "rows": [...], "hasmore": false}
// Once hasmore is false, live updates begin flowing

// Stream (ref resumes from that point; omit ref to start from beginning of buffer)
{"service": "audit_feed", "type": "subscribe", "protocol": "stream", "ref": "20260404 15:30:45.123456000000"}

// Stream with pagination (stateless — no getmore, just re-subscribe with returned ref)
{"service": "audit_feed", "type": "subscribe", "protocol": "stream", "maxcount": 100}
// → {"type": "snapshot", "service": "audit_feed", "ref": "<last-row-ref>", "rows": [...], "hasmore": true}
// Next page: subscribe again with ref from previous response
{"service": "audit_feed", "type": "subscribe", "protocol": "stream", "ref": "<last-row-ref>", "maxcount": 100}
// Once hasmore is false, subscribe without maxcount to go live

// ReqRep — one-shot request-reply (reqid echoed on reply for correlation)
{"service": "tax", "type": "request", "reqid": "r1", "data": {"qty": 10, "price": 99.95, "rate": 0.08}}
// → {"type": "reply", "service": "tax", "reqid": "r1", "value": 79.96}

{"service": "search", "type": "request", "reqid": "r2", "data": {"symbol": "AAPL"}}
// → {"type": "reply", "service": "search", "reqid": "r2", "rows": [{"symbol": "AAPL", ...}]}
```

## Client Libraries

### Python

```python
from mkio.client import MkioClient

async with MkioClient("ws://localhost:8080/ws") as client:
    result = await client.send("add_order", {"id": "1", "symbol": "AAPL", "qty": 100})

    async for msg in client.subscribe("last_trade", "subpub", topic="AAPL"):
        print(msg)  # single row with _mkio_exists, _mkio_topic, _mkio_ref

    async for msg in client.subscribe("last_trade", "subpub", topic=["AAPL", "MSFT"]):
        print(msg)  # snapshot with one row per topic, then individual updates

    async for msg in client.subscribe("all_orders", "query", filter="status == 'pending'"):
        print(msg)

    # Paginated query (client auto-sends getmore until snapshot complete)
    async for msg in client.subscribe("all_orders", "query", maxcount=50):
        print(msg)

    # ReqRep — one-shot request-reply (auto-generates reqid)
    result = await client.request("tax", {"qty": 10, "price": 99.95, "rate": 0.08})
    print(result)  # {"type": "reply", "value": 79.96, ...}
```

### JavaScript

Auto-served at `/mkio.js` — no CDN or bundler needed.

```html
<script src="/mkio.js"></script>
<script>
const client = new MkioClient("ws://localhost:8080/ws");
await client.connect();

client.subscribe("last_trade", "subpub", {
    topic: "AAPL",
    onSnapshot: (rows) => renderTrade(rows[0]),
    onUpdate: (op, row) => renderTrade(row),
    onNack: (message) => console.error("Subscription rejected:", message),
});

client.subscribe("all_orders", "query", {
    filter: "status == 'pending'",
    onSnapshot: (rows) => renderTable(rows),
    onUpdate: (op, row) => updateRow(op, row),
});

// Paginated query (client auto-sends getmore; onSnapshot fires once with all rows)
client.subscribe("all_orders", "query", {
    maxcount: 50,
    onSnapshot: (rows) => renderTable(rows),
    onUpdate: (op, row) => updateRow(op, row),
});
</script>
```

**Compatibility:** Runs in all evergreen browsers (Chrome, Edge, Firefox, Safari) with no polyfills. Also works in Node.js ≥22, where `WebSocket`, `TextDecoder`, and `performance` are available as globals. On Node 18–21, assign a `WebSocket` polyfill to `globalThis` before importing:

```js
globalThis.WebSocket = require("ws");
const { MkioClient } = require("./mkio.js");
```

The file uses CommonJS `module.exports`; load it via `require(...)` in Node, or `<script src="/mkio.js">` in the browser.

#### Debugging from the browser console

Once `/mkio.js` is loaded, a `mkio` object is available in DevTools with methods that mirror the `mkio` CLI (the `<url>` argument is dropped since the page already holds the connection):

```js
mkio.help()                                // show help
mkio.services()                            // list every service on the server
mkio.services("orders")                    // detail for one service
mkio.monitor()                             // log every frame to/from any service
mkio.monitor("orders")                     // filter to one service (call again to add more)
mkio.monitor({filter: e => e.direction === "in"})  // filter with a function
mkio.monitor("off")                        // stop
mkio.send("orders", {side:"Buy",...}, {op:"new"})
mkio.subpub("last_trade", "AAPL")
mkio.subpub("last_trade", ["AAPL","MSFT","GOOG"])
mkio.subpub("last_trade", "AAPL", {fields:["bid","ask"], subid:"p1"})
mkio.stream("audit_feed")                 // ref auto-generated
mkio.stream("audit_feed", {ref:"...", filter:"qty > 100"})
mkio.query("all_orders", {filter:"status == 'pending'"})
mkio.query("all_orders", {maxcount: 50})     // paginated snapshot
mkio.query("all_orders", {snapshotOnly: true})
mkio.query("all_orders", {updateOnly: true, fields:["id","status"]})
mkio.reqrep("tax", {qty: 10, price: 99.95, rate: 0.08})
mkio.reqrep("search", {symbol: "AAPL"})
```

All subscribe methods return a `MkioSubscription` with `.stop()`. Nack responses are logged to the console by default. Console commands auto-generate `subid` (subscriptions) and `txnid` (sends) with a `_mkio_` prefix so they never intercept messages meant for the application.

`mkio.monitor(...)` only taps **this tab's** traffic. For traffic across all connected clients use the CLI's server-side `mkio monitor` instead.

## Expression Language

Used for client filters, server-side `where` filters, and `publish` formatters.

| Category | Syntax |
|----------|--------|
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical | `AND`, `OR`, `NOT` |
| Arithmetic | `+`, `-`, `*`, `/` |
| String | `CONTAINS`, `STARTS_WITH` |
| Null | `IS NULL`, `IS NOT NULL` |
| Functions | `UPPER()`, `LOWER()`, `ROUND()`, `ABS()`, `COALESCE()`, `IF()` |
| Membership | `IN` (right side is a list/tuple/set supplied by host code) |
| Grouping | `(` ... `)` |

**Data types:** string (single-quoted, e.g. `'pending'`), integer, float, boolean (`TRUE`/`FALSE`), and `NULL`.

**Operator precedence** (lowest to highest):

1. `OR`
2. `AND`
3. `NOT`
4. Comparisons: `==` `!=` `<` `>` `<=` `>=`, `IS NULL` / `IS NOT NULL`, `IN`, `CONTAINS`, `STARTS_WITH`
5. Additive: `+` `-`
6. Multiplicative: `*` `/`
7. Unary minus: `-x`
8. Primary: literals, field references, function calls, parenthesized expressions

Use parentheses to override precedence, e.g. `(status == 'new' OR status == 'pending') AND qty > 100`.

### Built-in Functions

| Function | Signature | Description |
|---|---|---|
| `UPPER` | `UPPER(s)` | Uppercase a string. Non-string values pass through unchanged. |
| `LOWER` | `LOWER(s)` | Lowercase a string. Non-string values pass through unchanged. |
| `ROUND` | `ROUND(x, n=0)` | Round numeric `x` to `n` decimal places. `n` defaults to 0. |
| `ABS` | `ABS(x)` | Absolute value of a numeric. |
| `COALESCE` | `COALESCE(a, b, ...)` | Returns the first non-`NULL` argument, or `NULL` if all are `NULL`. Variadic (1+ args). |
| `IF` | `IF(cond, then, else)` | Returns `then` if `cond` is truthy, else `else`. Short-circuits — only the taken branch is evaluated. |

Notes:

- `IF` is a special form, not a regular function: the non-taken branch is never evaluated, so it's safe to guard against nulls or division-by-zero, e.g. `IF(qty > 0, price / qty, 0)`.
- `UPPER` / `LOWER` are null-safe via passthrough: `UPPER(NULL)` returns `NULL`.
- Function names are case-insensitive at parse time but conventionally written uppercase.
- Custom functions registered via `register_function` appear alongside these built-ins.

Worked example combining several functions:

```
IF(status == 'filled', UPPER(symbol), COALESCE(note, '-'))
```

Extend with custom functions:

```python
from mkio import register_function

register_function("MASK_PAN", lambda s: "****" + s[-4:])
```

## Performance

- **Write batching** — collects writes over a 2ms window, commits as single SQLite transaction with per-request SAVEPOINTs
- **WAL mode** — dual connections (write + read) for concurrent reads during writes
- **Zero-copy fan-out** — change events serialized once, same bytes sent to all subscribers
- **Optional acceleration** — `pip install mkio[fast]` for orjson (5-10x JSON) and uvloop (2-4x I/O)

## CLI Tools

The URL argument defaults to `localhost`, `http://`, and port 80, so you can use shorthand:

```bash
mkio services 8080                      # same as http://localhost:8080
mkio services localhost:8080            # same as http://localhost:8080
mkio services myhost                    # same as http://myhost:80
mkio services https://prod.example.com  # uses port 443, wss for WebSocket
```

### List and inspect services

```bash
mkio services localhost:8080                # List all services
mkio services localhost:8080 orders         # Show detail for one service
```

Detail view shows fields, types, required/optional, auto-generated columns, and example commands.

### Send transactions

```bash
mkio send localhost:8080 orders --op new '{"side":"Buy","symbol":"AAPL","qty":100,"price":150}'
mkio send localhost:8080 orders --op new orders.json    # From JSON file
mkio send localhost:8080 orders --op new orders.csv     # From CSV file
mkio send localhost:8080 orders mixed.csv               # CSV with per-row op column
```

### Subscribe to live data

Each listener service type has its own command with only the relevant options:

```bash
# SubPub — topic-based snapshot + live updates
mkio subpub localhost:8080 last_trade AAPL
mkio subpub localhost:8080 last_trade AAPL MSFT GOOG
mkio subpub localhost:8080 last_trade AAPL --fields symbol,price

# Stream — ring buffer with cursor reconnect (ref defaults to now)
mkio stream localhost:8080 audit_feed
mkio stream localhost:8080 audit_feed --ref "20260404 15:30:45.123456000000"
mkio stream localhost:8080 audit_feed --fields event,order_id
mkio stream localhost:8080 audit_feed --maxcount 100    # page from beginning of buffer

# Query — snapshot + live updates
mkio query localhost:8080 all_orders
mkio query localhost:8080 all_orders --filter "status == 'pending'"
mkio query localhost:8080 all_orders --fields symbol,qty --snapshotOnly

# ReqRep — one-shot request-reply
mkio reqrep localhost:8080 tax '{"qty": 10, "price": 99.95, "rate": 0.08}'
mkio reqrep localhost:8080 search symbol=AAPL
```

### Monitor traffic

Tap into inbound and outbound message flow in real time. Monitor a single service or all services at once:

```bash
mkio monitor localhost:8080                 # Monitor all services
mkio monitor localhost:8080 orders          # Monitor one service
mkio monitor localhost:8080 --filter "direction == 'in'"    # Inbound only
mkio monitor localhost:8080 --filter "service == 'orders'"  # Filter by service
```

```
[2026-04-04 15:30:45.123456 -0400] >> IN  subscribe
{ "type": "subscribe", "service": "last_trade", "protocol": "subpub" }

[2026-04-04 15:30:45.125789 -0400] << OUT snapshot
{ "type": "snapshot", "rows": [...] }
```

The `--filter` flag accepts any expression from the [expression language](#expression-language), evaluated against each monitor envelope (`direction`, `service`, `message`).

The monitor protocol is a native framework feature — any mkio application supports it.

## Config Endpoint

The `[config]` section maps routes to directories, with automatic TOML-to-JSON conversion. This keeps `[static]` strictly for static assets.

```toml
[config]
"/config" = "./configs"
"/settings" = "./settings"
```

**Behavior:**
- `GET /config/app.json` — reads `./configs/app.toml`, parses it, and serves as `application/json`
- If no `.toml` file exists, falls back to serving `./configs/app.json` directly
- `GET /config/style.css` — serves the file as-is (no conversion for non-`.json` extensions)
- Subdirectories are supported: `GET /config/sub/db.json` reads `./configs/sub/db.toml`
- Multiple routes map to independent directories
- Path traversal is blocked

## Config Validation

mkio validates your TOML config at load time and fails fast with clear error messages:

- **Table references** — `primary_table`, `watch_tables`, and op `table` fields must reference tables defined in `[tables]`
- **Column references** — op `fields`, `key`, `defaults`, `bind` columns, `filterable`, and subpub `topic` are checked against table schemas
- **Protocol validation** — service `protocol` must be a known type (`transaction`, `subpub`, `stream`, `query`, `reqrep`)
- **Required fields** — missing `protocol`, `primary_table`, `topic`, `ops`, or `key` (for update/delete/upsert) are caught immediately
- **Bind references** — forward references and out-of-bounds op indices in `$N.field` binds are rejected
- **Typo detection** — unknown config keys produce warnings with "did you mean?" suggestions

Runtime error messages include context to help debugging:

- Unknown service/op errors list available options
- Missing transaction fields show the op name and list provided fields
- Expression errors list available fields
- Requests to unknown services return `nack` (not generic errors), with the service name echoed back

## Schema Migration

When the config schema changes between restarts, mkio detects and classifies each difference:

- **Safe** (new table, nullable column) — applied automatically
- **Potentially destructive** (type change, PK change) — requires confirmation
- **Destructive** (remove column/table) — requires confirmation

Set `auto_migrate = true` in config for non-interactive environments.

## License

Apache-2.0
