# mkio

[![PyPI](https://img.shields.io/pypi/v/mkio?v=3)](https://pypi.org/project/mkio/)
[![Python](https://img.shields.io/pypi/pyversions/mkio?v=3)](https://pypi.org/project/mkio/)
[![License](https://img.shields.io/pypi/l/mkio?v=3)](https://github.com/markuskimius/mkio/blob/main/LICENSE)

Config-driven microservice framework for Python. Define your schema, services, and data flows in a TOML file — zero coding required for standard configurations.

A single TCP port serves HTTP and WebSocket, backed by an embedded SQLite database. Designed for restricted environments where runtime downloads aren't possible — everything installs via `pip`.

## Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Authentication & Access Control](#authentication--access-control)
- [Programmatic API](#programmatic-api)
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

Create `server.toml`:

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
serve("server.toml")
serve({...})  # or pass a dict
```

For programmatic control (custom routes, non-blocking lifecycle), see [Programmatic API](#programmatic-api).

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
- **Connection identity** — built-in `_mkio` reqrep service reports server name, version, framework version, protocol version, services, tables, config hash, and uptime — lets clients verify they're connected to the correct session. Also supports table schema introspection (columns, types, primary keys, defaults)
- **Config endpoint** — `/config` path serves TOML files as JSON (request `foo.json`, server reads `foo.toml` and returns JSON); falls back to literal `.json` files; other extensions served as-is
- **CLI tools** — send transactions, subscribe to live data, monitor traffic, inspect services
- **Authentication & access control** — table-driven roles (`_mkio_rights`) with config-driven per-service/per-op access. Supports SQL pre-check conditions (`when`), built-in username/password auth (`_mkio_users` with bcrypt), or custom auth via `app.on_auth()` (JWT, LDAP, etc.)
- **Programmatic API** — `create_app()` returns a controllable server handle with async `start()`/`stop()` lifecycle, custom HTTP routes, custom service registration, lifecycle hooks, and a data facade (`execute`/`query`/`subscribe`) for server-side interaction without WebSocket

## Authentication & Access Control

Enable auth by adding the `_mkio_rights` table to your config. When it exists, all services are locked by default — each service must declare its `access`.

### Setup

```toml
# Auth tables
[tables._mkio_users]
columns = { username = "TEXT PRIMARY KEY", password = "TEXT NOT NULL", role = "TEXT NOT NULL" }

[tables._mkio_rights]
columns = { role = "TEXT", right = "TEXT" }

# Service access control
[services.prices]
protocol = "subpub"
primary_table = "prices"
topic = "symbol"
access = "open"               # anyone, no auth needed

[services.positions]
protocol = "query"
primary_table = "positions"
access = "market"             # role must have "market" right

[services.orders]
protocol = "transaction"
access = "auth"               # any logged-in user (service-level default)

[services.orders.ops.place]
table = "orders"
op_type = "insert"
fields = ["symbol", "qty"]

[services.orders.ops.place.access]
trade_limited = "accounts WHERE username = :user AND balance >= :qty * :price"
trade_full = true
```

`access` values: `"open"` (no auth), `"auth"` (any logged-in user), `"right_name"` (role must have this right), or a dict mapping rights to SQL pre-check conditions.

### Rights table

The `_mkio_rights` table maps roles to rights. Adding a new role is a table insert — no config change:

```
role       right
trader     market
trader     trade_limited
senior     market
senior     trade_full
admin      admin
```

### Auth protocol

Clients authenticate after connecting:

```json
{"type": "auth", "data": {"username": "alice", "password": "secret"}}
→ {"type": "auth", "ok": true, "user": "alice", "role": "trader"}
```

### SQL pre-checks (`when`)

Dict-form `access` maps rights to SQL conditions. The condition is executed as `SELECT 1 FROM <condition> LIMIT 1` — if it returns a row, access is granted. Bind parameters available: `:user`, `:role`, `:topic` (subpub), submitted data fields (`:symbol`, `:qty`, etc.), and any extra columns from the user's `_mkio_users` row.

For transaction ops, pre-checks run inside the same SAVEPOINT as the write — ACID-safe.

### Custom auth

For JWT, LDAP, or other auth backends, use `on_auth()` instead of `_mkio_users`:

```python
app = create_app(config)

async def my_auth(data):
    user = verify_jwt(data["token"])
    return {"user": user.name, "role": user.role}

app.on_auth(my_auth)
app.run()
```

Rights enforcement still uses the `_mkio_rights` table regardless of auth method.

### Monitor access

When auth is enabled, monitoring is **disabled by default**. Set the top-level `monitor_access` key to control who can use `mkio monitor`:

```toml
monitor_access = "admin"    # role must have "admin" right
```

`monitor_access` accepts the same values as service `access`: `"open"` (no auth needed), `"auth"` (any logged-in user), a right name, or a dict with SQL pre-checks. Without this key, all monitor requests return `"monitoring disabled"`.

### CLI authentication

All WebSocket-based CLI commands accept `--username` for authentication. The password is read from the `MKIO_PASSWORD` environment variable, or prompted interactively if not set:

```bash
mkio monitor 8080 --username alice
mkio send 8080 orders --op new '{"symbol":"AAPL"}' --username alice
mkio subpub 8080 prices AAPL --username bob
mkio query 8080 all_orders --username bob
mkio stream 8080 audit_feed --username bob
mkio reqrep 8080 lookup name=test --username bob
mkio schema 8080 orders --username bob
mkio check 8080 --username bob
```

For scripted/CI use, set the `MKIO_PASSWORD` environment variable:

```bash
MKIO_PASSWORD=secret mkio send 8080 orders --op new '{"symbol":"AAPL"}' --username alice
```

### Bootstrap

```bash
mkio adduser alice trader              # prompts for password (or reads MKIO_PASSWORD)
mkio adduser admin1 admin my.toml      # use a specific config file
mkio hashpass                          # generate a hashed password for seed files
```

Install bcrypt for strong password hashing: `pip install mkio[auth]`. Without it, PBKDF2 is used as a fallback.

## Programmatic API

For applications that embed mkio (e.g., adding custom HTTP routes, registering custom services, or controlling server lifecycle), use `create_app()` instead of `serve()`.

### `create_app(config, *, routes=None) -> MkioApp`

Creates a server instance from a TOML file path or config dict.

```python
import asyncio
from aiohttp import web
from mkio import create_app

async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})

app = create_app("server.toml", routes=[
    ("GET", "/health", health),
    ("POST", "/api/custom", my_handler),
])

# Blocking (like serve())
app.run()

# Or async for non-blocking control
async def main():
    await app.start()       # binds port, begins serving
    # ... do other async work ...
    await app.wait()        # blocks until stop() or signal

asyncio.run(main())
```

### `MkioApp` methods

| Method | Description |
|--------|-------------|
| `add_routes(routes)` | Add `(method, path, handler)` tuples before starting. Raises `RuntimeError` if already running. |
| `add_service(name, cls, config=None)` | Register a custom `Service` subclass before starting. Same lifecycle as config-driven services. |
| `on_startup(callback)` | Register an async callback invoked after all services start. |
| `on_shutdown(callback)` | Register an async callback invoked before services stop. |
| `on_connect(callback)` | Register an async `(ws) -> None` callback invoked when a WebSocket client connects. |
| `on_disconnect(callback)` | Register an async `(ws) -> None` callback invoked when a WebSocket client disconnects. |
| `on_auth(callback)` | Register a custom async auth handler `(data) -> {"user", "role", ...}`. Overrides table-backed auth. Raise to reject. |
| `async execute(service, data, *, op=None)` | Submit a transaction through the write path. Returns `{"ok": True, "ref": "..."}`. |
| `async query(sql, params=())` | Read query on the read connection. Returns `list[dict]`. |
| `async subscribe(tables, callback)` | Subscribe to `ChangeEvent`s. Returns an unsubscribe function. |
| `async start()` | Non-blocking start — runs migration, preflight, binds the port. |
| `async stop()` | Graceful shutdown — drains writes, closes WebSockets, checkpoints DB. Idempotent. |
| `async wait()` | Blocks until `stop()` is called or a signal fires. |
| `run()` | Blocking convenience: starts, installs signal handlers, waits. Tries uvloop if available. |
| `.config` | The resolved config dict (read-only property). |
| `.db` | `Database` instance (after start, `None` otherwise). **Unstable.** |
| `.writer` | `WriteBatcher` instance (after start, `None` otherwise). **Unstable.** |
| `.change_bus` | `ChangeBus` instance (after start, `None` otherwise). **Unstable.** |
| `.services` | `dict[str, Service]` map (after start, empty otherwise). **Unstable.** |

Supported HTTP methods for routes: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`.

Properties marked **Unstable** expose internal types that may change across versions. The facade methods (`execute`, `query`, `subscribe`) are the stable interface for the same operations.

### Custom services

Register a `Service` subclass to add custom behavior reachable over WebSocket:

```python
from mkio import create_app, Service

class AuditService(Service):
    async def start(self):
        self._q = self.bus.subscribe(["orders"])
        import asyncio
        asyncio.create_task(self._watch())

    async def _watch(self):
        while True:
            event = await self._q.get()
            print(f"order changed: {event.row}")

app = create_app("server.toml")
app.add_service("audit", AuditService)
app.run()
```

Custom services go through the same lifecycle as config-driven services — `start()` is called at server startup, monitors work, and WS dispatch routes messages to `on_subscribe()` / `on_message()`.

### Data facade

Write data, read data, and react to changes without going through WebSocket:

```python
app = create_app(config)
await app.start()

# Write through the normal write path (WriteBatcher → ChangeBus → subscribers)
result = await app.execute("orders", {"symbol": "AAPL", "qty": 100}, op="new")

# Read
rows = await app.query("SELECT * FROM orders WHERE symbol = ?", ("AAPL",))

# React to changes
from mkio import ChangeEvent

async def on_change(event: ChangeEvent):
    print(f"{event.table} {event.op}: {event.row}")

unsub = await app.subscribe(["orders"], on_change)
# later: unsub()
```

### Lifecycle hooks

Register callbacks for server and connection events:

```python
app = create_app("server.toml")

async def setup():
    print("Server ready")

async def teardown():
    print("Server stopping")

async def on_connect(ws):
    print(f"Client connected")

async def on_disconnect(ws):
    print(f"Client disconnected")

app.on_startup(setup)
app.on_shutdown(teardown)
app.on_connect(on_connect)
app.on_disconnect(on_disconnect)
app.run()
```

Multiple callbacks can be registered for each hook — they are called in registration order.

### `get_default_config() -> dict`

Returns the default scaffold config (what `mkio init` writes) as a Python dict. Modify it before passing to `create_app()`:

```python
from mkio import create_app, get_default_config

cfg = get_default_config()
cfg["port"] = 9090
cfg["db_path"] = ":memory:"
cfg["tables"]["widgets"] = {
    "columns": {"id": "INTEGER PRIMARY KEY", "name": "TEXT"},
}
cfg["services"]["widgets"] = {
    "protocol": "subpub",
    "primary_table": "widgets",
    "topic": "id",
}
app = create_app(cfg)
app.run()
```

### `init(directory, *, no_static=False) -> list[Path]`

Programmatic equivalent of `mkio init`. Creates project files and returns the list of paths created.

```python
from mkio import init

created = init("./my-project")
# [Path('my-project/server.toml'), Path('my-project/static/index.html')]

created = init("./api-only", no_static=True)
# [Path('api-only/server.toml')]
```

Raises `FileExistsError` if `server.toml` already exists in the target directory.

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

Append-only data with ring buffer and ref-based cursor reconnection. Supports forward and backward pagination through the buffer.

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
    "mkio": "0.1.47",
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
    "name": "order-book-dev", "version": "2.3.0", "mkio": "0.1.47", "protocol": "1.0",
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

#### Table Schema

Clients can query the schema of any table by sending `data` with a `"table"` key. The server returns column definitions from the live database:

```json
{"type": "request", "service": "_mkio", "reqid": "s1", "data": {"table": "orders"}}
```

Reply:

```json
{
  "type": "reply", "service": "_mkio", "reqid": "s1",
  "row": {
    "table": "orders",
    "columns": [
      {"name": "id", "type": "TEXT", "notnull": false, "pk": true, "dflt_value": null},
      {"name": "symbol", "type": "TEXT", "notnull": true, "pk": false, "dflt_value": null},
      {"name": "qty", "type": "INTEGER", "notnull": false, "pk": false, "dflt_value": null},
      {"name": "status", "type": "TEXT", "notnull": false, "pk": false, "dflt_value": "'pending'"},
      {"name": "_mkio_ref", "type": "TEXT", "notnull": false, "pk": false, "dflt_value": "''"}
    ]
  }
}
```

Unknown tables return an error listing available tables. From the CLI: `mkio schema 8080 orders`. From the browser console: `mkio.schema("orders")`.

## WebSocket Protocol

Connect to `/ws` (general) or `/ws/{service_name}` (per-service).

```json
// Authenticate (before sending other messages to auth-protected services)
{"type": "auth", "data": {"username": "alice", "password": "secret"}}
// → {"type": "auth", "ok": true, "user": "alice", "role": "trader"}

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

// Stream backward pagination (rows before a ref, newest N closest to the ref)
{"service": "audit_feed", "type": "subscribe", "protocol": "stream", "ref": "<ref>", "before": true, "maxcount": 20}
// → {"type": "snapshot", "ref": "<earliest-row-ref>", "rows": [...], "hasmore": true}
// Next page backward: subscribe again with returned ref and before: true
{"service": "audit_feed", "type": "subscribe", "protocol": "stream", "ref": "<earliest-row-ref>", "before": true, "maxcount": 20}

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
    # Authenticate (credentials stored for auto-re-auth on reconnect)
    await client.auth({"username": "alice", "password": "secret"})

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

    # Stream — backward pagination (last 20 rows before a ref)
    async for msg in client.subscribe("audit_feed", "stream", before=True, ref=some_ref, maxcount=20):
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

// Authenticate (credentials stored for auto-re-auth on reconnect)
await client.auth({username: "alice", password: "secret"});

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
mkio.auth({username: "alice", password: "secret"})  // authenticate
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
mkio.stream("audit_feed", {before: true, ref:"...", maxcount: 20})  // backward
mkio.query("all_orders", {filter:"status == 'pending'"})
mkio.query("all_orders", {maxcount: 50})     // paginated snapshot
mkio.query("all_orders", {snapshotOnly: true})
mkio.query("all_orders", {updateOnly: true, fields:["id","status"]})
mkio.reqrep("tax", {qty: 10, price: 99.95, rate: 0.08})
mkio.reqrep("search", {symbol: "AAPL"})
mkio.schema("orders")                   // table schema (columns, types, keys)
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
mkio stream localhost:8080 audit_feed --maxcount 100    # page forward from beginning of buffer
mkio stream localhost:8080 audit_feed --before --ref "<ref>" --maxcount 20  # page backward

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

### Inspect table schema

```bash
mkio schema localhost:8080 orders          # Show columns, types, keys, defaults
```

### Schema management

```bash
mkio dbupdate                       # Apply safe schema changes
mkio dbupdate --allow-risky         # Include potentially destructive changes
mkio dbupdate --allow-destructive   # Include all changes
mkio dbupdate custom.toml           # Use a specific config file
```

### Manage users

```bash
mkio adduser alice trader               # Add user, prompts for password
mkio adduser admin1 admin custom.toml   # Use a specific config file
mkio hashpass                           # Generate a hashed password for seed files
MKIO_PASSWORD=secret mkio adduser bob viewer  # Non-interactive (CI/scripts)
```

### Initialize a project

```bash
mkio init                           # Create server.toml + static/ in current directory
mkio init ./my-project              # Create in a specific directory
mkio init --no-static               # Config only, no static/index.html
```

### Error handling

All CLI commands show clean error messages instead of Python tracebacks. Common scenarios:

- **Server not running** — `Error: could not connect to ... Is the mkio server running?`
- **Port already in use** — `Error: address already in use ... Stop the other process or change the port in server.toml`
- **Invalid TOML** — `Error: invalid TOML in server.toml ...`
- **Invalid config** — `Error: invalid config: ...`

Use `--traceback` (or `MKIO_TRACEBACK=1`) to show the full Python traceback for debugging.

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

## Table Seeding

Tables can be populated with initial data from a file when first created. Add `seed` to any table config:

```toml
[tables.products]
columns = { id = "TEXT PRIMARY KEY", name = "TEXT NOT NULL", price = "REAL" }
seed = "data/products.csv"
```

Supported formats: `.csv`, `.json` (array of objects), `.jsonl` (one JSON object per line).

Seed data is loaded **only when the table is first created** — not on every restart. If a user deletes rows, they stay deleted.

**Path resolution:**

| Path starts with | Resolved relative to |
|------------------|---------------------|
| `/` | Absolute path |
| `./` | Current working directory |
| anything else | Directory containing the config file |

**Error handling:** Seed errors are fatal. Missing files, bad format, or unknown column names prevent the server from starting — same behavior as other config errors.

## Schema Migration

When the config schema changes, mkio detects and classifies each difference:

| Level | Examples | Risk |
|-------|----------|------|
| **Safe** | New table, nullable column, column with default | None |
| **Potentially destructive** | Type change, PK change | Values may not convert; duplicates may be dropped |
| **Destructive** | Remove column/table | Data loss |

By default, `mkio serve` refuses to start if the database schema differs from config. Use `mkio dbupdate` to apply changes explicitly:

```bash
mkio dbupdate                       # Apply safe changes only
mkio dbupdate --allow-risky         # Also apply potentially destructive changes
mkio dbupdate --allow-destructive   # Apply all changes (including data loss)
```

For automatic migration on startup, set `auto_migrate` in config:

```toml
auto_migrate = "safe"          # Apply safe changes on startup (same as true)
auto_migrate = "risky"         # Also apply potentially destructive
auto_migrate = "destructive"   # Apply all changes on startup
```

## License

Apache-2.0
