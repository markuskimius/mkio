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
- [Versioned Tables](#versioned-tables)
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
- **Expression language** — one safe, extensible language for filters and formatters (`qty > 100 && status == 'pending'`), implemented identically in Python and JavaScript
- **Versioned tables** — set `versioned = true` on a table and every row gets a `_mkio_version` counter and a full history of its versions in `<table>__history`, stamped with the ref, the user and the service. Built-in `undo`/`redo` ops step a row along its own history; `mkio archive` moves old versions to CSV
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

Install bcrypt for strong password hashing: `pip install mkio[auth]`. Without it, PBKDF2 with per-password random salt is used as a fallback.

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
| `async execute(service, data, *, op=None, user=None)` | Submit a transaction through the write path. Returns `{"ok": True, "ref": "..."}`. `user` is recorded on history rows of [versioned tables](#versioned-tables). |
| `async query(sql, params=())` | Read query on the read connection. Returns `list[dict]`. |
| `async history(table, *, pk=None, since=None, until=None, limit=1000, newest_first=False)` | Recorded versions of a [versioned table](#versioned-tables). Returns `list[dict]`. |
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

# Recorded versions, if the table is versioned
versions = await app.history("orders", pk={"id": "O1"})

# Step the row back and forward along them (ops declared in config)
await app.execute("orders", {"id": "O1"}, op="undo")
await app.execute("orders", {"id": "O1"}, op="redo")
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

## Versioned Tables

Set `versioned = true` on a table and mkio numbers every version of every row, keeping them all in a companion **history table**. Rows can then be stepped backwards and forwards along their own history — undo and redo — and the trail is there for audit.

```toml
[tables.orders]
columns = { id = "TEXT PRIMARY KEY", symbol = "TEXT", qty = "INTEGER", status = "TEXT" }
versioned = true
```

Then run `mkio dbupdate` to create the history table (or set `auto_migrate`). A versioned table must have a primary key, since the history table is keyed by it.

### The model: a cursor over recorded versions

```
history for "O1":   v1 ── v2 ── v3 ── v4      (contiguous, 1..N)
base row:                        ▲
                          _mkio_version = 3    (the cursor)
                                      └── v4 is redo, still reachable
```

Every versioned row carries a `_mkio_version` counter: **1** when inserted, **+1** on every edit. The base row's source columns always equal the history row with the same key and that version number — normally the highest one. After an undo the cursor sits lower, and the versions above it remain as redo.

An absent base row means the cursor is at 0: the row was undone past version 1, and redo can rebuild it.

### What a history row holds

| Column | Description |
|--------|-------------|
| `_mkio_version` | Version number; part of the primary key with the base table's own key |
| `_mkio_op` | `insert`, `update`, or `baseline` |
| `_mkio_ref` | Ref of the transaction that recorded this version |
| `_mkio_user` | Authenticated user who made it (`NULL` when auth is disabled) |
| `_mkio_service` | Service the change came through |
| *(source columns)* | The row as it stood at that version |

Source columns keep their types but drop every constraint — an old version must be storable even when it would violate the constraints the base table carries today.

`baseline` rows are written once, when versioning is switched on for a table that already holds data: existing rows become version 1 so they have something to step back onto. The history table is otherwise **additive-only** across schema changes: a column added to the base table is added here too, but one dropped or retyped keeps its recorded values.

### Undo and redo

Two op types move the cursor. They take only the key — the values come from the history table — and record no new version, so a step is always reversible:

```toml
[services.orders.ops]
undo = [{ table = "orders", op_type = "undo", key = ["id"] }]
redo = [{ table = "orders", op_type = "redo", key = ["id"] }]
```

```python
await app.execute("orders", {"id": "O1"}, op="undo")
await app.execute("orders", {"id": "O1"}, op="redo")
```

| Situation | `undo` | `redo` |
|---|---|---|
| Cursor at V > 1 | step back to V−1 | — |
| Cursor at V, version V+1 recorded | — | step forward to V+1 |
| Cursor at 1 | **delete the row**, keeping its history | — |
| Row absent, version 1 recorded | — | **rebuild the row** at version 1 |
| Nothing left to step onto | error: `nothing to undo` | error: `nothing to redo` |

Subscribers see ordinary row changes: an undo that removes a row emits a `delete`, a redo that rebuilds one emits an `insert`. Undo and redo compose with the rest of a transaction, so a bound `audit_log` entry works the way it does for any other op.

### Editing after an undo discards the redo branch

Writing at version V removes the recorded versions at V and above. So an edit made while the cursor sits below the top abandons everything above it — the same as typing after undoing in an editor:

```
v1 ── v2 ── v3        undo, undo        v1 ── v2 ── v3        edit        v1 ── v2 ── v3'
            ▲                            ▲                                      ▲
```

A fresh `insert` is version 1, so it discards the whole prior chain — which is what makes "undo to nothing, then insert again" behave sensibly. A `delete` is a real delete: the row goes and its history goes with it.

**This means truncation destroys audit history.** If you need the abandoned versions kept for audit, record application events separately — the `order_book` example's `audit_log` pattern does exactly that, and survives truncation because it is an ordinary table.

### Redo state is transient

`mkio dbupdate` discards every redo entry: versions above a live row's cursor, and the history of rows undone past version 1. It reports what went, and `--keep-redo` skips it:

```
  Discarded redo history from orders__history: 7 rows of fully undone records,
  12 redo rows above the current version
  These are no longer redo-able. Use --keep-redo to retain them.
```

Restarting a server never does this, even with `auto_migrate` — only the explicit command.

### Naming convention

The history table is always `<table>__history` — `orders` becomes `orders__history`, which sorts right next to it when you browse the schema. The `__history` suffix is reserved: mkio refuses to start if an application table ends in it, so it can never clash with one of yours. Only a *trailing* `__history` is reserved, so `history_of_orders` and `__history_log` are ordinary names.

Because the convention is fixed, a client that knows the base table can reach its history without being told the name:

```python
from mkio import history_table
history_table("orders")     # "orders__history"
```

```javascript
mkio.historyTable("orders") // "orders__history"
```

The server reports the convention at runtime too — the `_mkio` reply carries `versioned` (which tables are recorded) and `history_suffix`.

### Visibility

mkio never advertises a history table on its own. It is absent from the `_mkio` service's `tables` list and from the available-tables list in error messages, and no service exists for it unless you write one. It *is* reachable by anyone who knows the name:

```bash
mkio schema localhost:8080 orders__history
```

To expose history to your users, configure an ordinary service on it — with its own `access` rule, so audit visibility is a permission like any other. Such a service appears in `GET /api/services` and `mkio services` like any other, naming the history table as its `primary_table`; that is deliberate, since you wrote it:

```toml
[services.order_history]
protocol = "query"
primary_table = "orders__history"
filterable = ["id", "_mkio_user", "_mkio_op"]
access = "audit"
```

Subscribers to such a service receive new versions live as they are recorded. Transaction ops targeting a history table are rejected at config load — history is written only by the framework.

### Reading history

```python
# Every recorded version, oldest first
await app.history("orders")

# One row's versions — compare _mkio_version against the live row's to see
# where the cursor sits and what is still redoable
await app.history("orders", pk={"id": "O1"})

# A window, newest first
await app.history("orders", since=start_ref, until=end_ref, limit=50, newest_first=True)
```

### Archiving

`mkio archive` writes versions older than a cutoff to CSV, and optionally purges them. Refs sort lexicographically, so the age cutoff is an indexed range scan.

```bash
# See what would be archived
mkio archive server.toml --older-than 90d --out ./archive --dry-run

# Write the CSV and keep the history
mkio archive server.toml --older-than 90d --out ./archive

# Write the CSV, then purge the archived versions
mkio archive server.toml --older-than 90d --out ./archive --delete

# Also delete live rows the archive fully captured
mkio archive server.toml --older-than 90d --out ./archive --prune-source --yes
```

**Archiving a version removes that much undo depth** — the archived versions are no longer there to step back onto. By default the version a live row currently sits on is never archived; archiving it would leave the row pointing at a version that no longer exists.

The CSV is written and fsynced *before* anything is deleted, and the whole run is one transaction — a failed write archives nothing and deletes nothing. Files are named `orders__history_<first-ref>_<last-ref>.csv` and include `_mkio_version`, so an archive can be re-imported.

`--prune-source` lifts the cursor guard for rows whose whole chain predates the cutoff, and deletes the live row when all three hold:

1. its entire chain was just archived,
2. the newest archived version is the one the row sits on, and
3. every column of that version — `_mkio_version` included — matches the live row exactly.

It implies `--delete` and requires `--yes`. Run it against a stopped server: pruning bypasses the change bus, so live subscribers would not see the removals.

| Flag | Effect |
|------|--------|
| `--older-than <N>d\|<ref>` | Cutoff. `90d` counts back from now (`h`/`m` also work); anything else is a literal ref, so a bare `20260101` means "before that date" |
| `--table <name>` | Archive one table (base or history name); defaults to every versioned table |
| `--out <dir>` | Where to write CSVs (default: current directory) |
| `--delete` | Purge the archived versions |
| `--prune-source` | Also delete fully-archived, unchanged live rows |
| `--dry-run` | Report what would happen, change nothing |
| `--yes` | Confirm `--prune-source` |

### Turning versioning off

Removing `versioned = true` stops recording. The history table is **never** dropped automatically, so no destructive change is queued and the server keeps starting. `mkio dbupdate` notes the retained table; remove it deliberately with `mkio dbupdate --drop-history`.

### Scope

Versioning covers every write through mkio's write path — the transaction services and `MkioApp.execute()`. Writes made to the database file by another process are not recorded, and would leave the counter and the history out of step. Capture happens inside the same SAVEPOINT as the change itself, so a rolled-back transaction records nothing.

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
    "mkio": "0.3.0",
    "protocol": "1.1",
    "expr": "1",
    "services": {"orders": "transaction", "last_trade": "subpub", "all_orders": "query"},
    "tables": ["orders", "audit_log"],
    "versioned": ["orders"],
    "history_suffix": "__history",
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
| `expr` | Expression language version (exact match required) |
| `services` | Map of service name → protocol type |
| `tables` | List of configured table names |
| `versioned` | Base tables whose changes are recorded — see [Versioned Tables](#versioned-tables) |
| `history_suffix` | Suffix for deriving a versioned table's history table name |
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

Clients can check whether they're compatible with the server by sending expected version(s) in the request `data`. The server replies with a `compatible` boolean (AND of all checks) and a `compatibility` dict with per-version results. `version`, `protocol`, and `mkio` use semantic versioning (caret `^` convention); `expr` must match exactly.

```json
{"type": "request", "service": "_mkio", "reqid": "v1",
 "data": {"version": "2.0.0", "protocol": "1.0", "mkio": "0.3.0", "expr": "1"}}
```

Reply:

```json
{
  "type": "reply", "service": "_mkio", "reqid": "v1",
  "row": {
    "name": "order-book-dev", "version": "2.3.0", "mkio": "0.3.0", "protocol": "1.1", "expr": "1",
    "compatible": true,
    "compatibility": {"version": true, "protocol": true, "mkio": true, "expr": true},
    ...
  }
}
```

If any version is incompatible, `compatible` is `false` and the failing key(s) show `false` in `compatibility`. When no version expectations are sent, neither field appears (backward compatible).

| Field | Type | Description |
|-------|------|-------------|
| `compatible` | bool | `true` if all requested versions are compatible |
| `compatibility` | dict | Per-version result: `{key: true/false}` for each key sent in `data` |

From the CLI: `mkio check 8080 version=2.0.0 protocol=1.0 expr=1`. From the browser console: `mkio.check({version: "2.0.0", protocol: "1.0", expr: "1"})`. The CLI exits with code 0 if compatible, 1 if not.

When authentication is enabled, `_mkio` still answers **before** login — with identity and compatibility fields only (`name`, `version`, `mkio`, `protocol`, `expr`, `compatible`, `compatibility`), so `mkio check` and client verification can pick a server without credentials and without learning its service or table names. Any authenticated user, whatever their rights, gets the full reply and the schema query.

Error replies to requests always echo `reqid` (and `service`), including access denials, so clients can correlate them; both clients also settle their oldest pending request if an older server sends an error without one.

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

A [versioned table's](#versioned-tables) reply also carries `"versioned": true` and `"history_table": "orders__history"`; querying the history table by name works too and replies with `"history_of": "orders"`. History tables are not listed among the available tables, so they stay out of listings while remaining reachable by anyone who knows the convention.

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

    # A versioned table's history table, by convention
    from mkio.client import history_table
    async for msg in client.subscribe("order_history", "query",
                                      filter="_mkio_user == 'alice'"):
        print(msg)  # versions from history_table("orders")
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

#### Expressions in the browser

The [expression language](#expression-language) is also served, as an ES module, at `/mkio-expr.js`:

```html
<script src="/mkio.js"></script>
<script type="module" src="/mkio-expr.js"></script>
<script>
// after the module has loaded: globalThis.mkioExpr, also reachable as mkio.expr
const pred = mkioExpr.compileFilter("status == 'pending' && qty > 100");
const label = mkioExpr.compileTemplate("${symbol}: ${NUM(qty * price, digits: 2, group: TRUE)}");
rows.filter((r) => pred(r)).map((r) => label.call(r));
</script>
```

or `import { compile, compileTemplate } from "/mkio-expr.js"` from your own module. With it loaded, `mkio.monitor({filter: "direction == 'in'"})` accepts expression strings.

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
mkio.monitor({filter: "direction == 'in'"})       // ...or an expression (needs /mkio-expr.js)
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
mkio.historyTable("orders")             // "orders__history"
```

All subscribe methods return a `MkioSubscription` with `.stop()`. Nack responses are logged to the console by default. Console commands auto-generate `subid` (subscriptions) and `txnid` (sends) with a `_mkio_` prefix so they never intercept messages meant for the application.

`mkio.monitor(...)` only taps **this tab's** traffic. For traffic across all connected clients use the CLI's server-side `mkio monitor` instead.

## Expression Language

One small, safe expression language is used everywhere mkio evaluates something per row or per request: client `filter`s, server-side `where`, `publish`, `defaults`, reqrep `params` and `reply`, and the CLI's `--filter`. The same language ships as a JavaScript module (`/mkio-expr.js`), so a browser UI evaluates the identical grammar; both implementations run the shared conformance fixtures in `tests/expr_cases.json`, and the `_mkio` identity reply carries `expr` (the language version, currently `"1"`).

```
qty * price > 1000 && status == 'open'
IF(side == 'Buy', price, -price)
qty * price |> (sub -> NUM(sub * 0.001, digits: 2))
items |> (xs -> SUM(MAP(xs, i -> i.qty * i.price)))
meta.region ?? 'n/a'
```

### Syntax

| | Form |
|---|---|
| Numbers | `42` `3.5` `1e6` `1_000_000` — one number type; integral values print without a fraction (`2`, not `2.0`) |
| Strings | `'text'` or `"text"`, escapes `\n \t \\ \' \" \u{1F600}` |
| Literals | `TRUE` `FALSE` `NULL` (case-insensitive — the only reserved words) |
| Names | `qty`, `_mkio_ref` — case-sensitive; `` `order id` `` in backticks for any other name |
| Arrays / maps | `[1, 2, 3]`, `{symbol: 'AAPL', qty: 100}` (trailing commas allowed) |
| Access | `meta.region`, `tags[0]`, `tags[-1]`, `data.items[0].name` — a missing key or index yields `NULL` |
| Calls | `ROUND(x, 2)`, `NUM(x, digits: 2, group: TRUE)` — names are case-insensitive; named arguments follow positional ones |
| Lambdas | `x -> x * 2`, `(a, b) -> a + b` — values you pass to `MAP`, `FILTER`, `SORT_BY`, … |
| Pipes | `value \|> (x -> body)` — the right side must be a parenthesized lambda; chains read left to right |

**Operators, lowest to highest precedence:**

| Level | Operators | Notes |
|---|---|---|
| 1 | `\|>` | pipe into a lambda |
| 2 | `\|\|` | short-circuit |
| 3 | `&&` | short-circuit |
| 4 | `== != < <= > >=` | `==` is strict (no coercion: `'1' == 1` is `FALSE`); comparisons don't chain |
| 5 | `??` | null-coalescing, short-circuit |
| 6 | `+ -` | `+` adds two numbers or concatenates two strings |
| 7 | `* / // %` | `/` true division, `//` floor division, `%` takes the divisor's sign |
| 8 | `- !` (unary) | `!a == b` is `(!a) == b` |
| 9 | `**` | right-associative |
| 10 | `.name` `[i]` `F()` | postfix |

**Semantics:** falsy values are `NULL`, `FALSE`, `0`, `''`, `[]`, `{}`; everything else is truthy. `&&`, `||`, `??` and the `IF`/`CASE`/`TRY`/`LET` functions evaluate only what they need, so `qty > 0 && price / qty > 10` never divides by zero. Errors (unknown field, division by zero, type mismatch, bad index type) abort the expression with a message and position; `TRY(expr, fallback)` catches them. There are no statements, loops, assignments, or side effects — name intermediate results with `LET(name, value, ..., body)` or a pipe.

**Scope and strictness.** Bare names resolve in the scope the host supplies — the row, for server filters and formatters. The server compiles in *strict* mode: an unknown root name is an error listing the available fields. Hosts may choose a *lenient* environment where unknown names are `NULL` (a UI over heterogeneous rows, say). Missing map keys, out-of-range indexes, and indexing into `NULL` yield `NULL` in both modes, so `meta.region ?? 'n/a'` always works. Keys beginning with `__` (and `constructor` / `prototype`) are never accessible.

### Templates

`compile_template` handles strings with embedded expressions: `"Order #${id} for ${UPPER(symbol)}"`. A template that is exactly one `${...}` returns the expression's raw value (type preserved); any other template returns a string, with `NULL` rendering as empty. Write `$${` for a literal `${`.

### Standard library

All libraries are enabled by default. Function names are shown upper-case by convention; `upper(s)` is the same function.

<!-- expr-functions:start -->

#### `core`

Control flow, coercion, and safe lookup. `IF`, `CASE`, `TRY`, and `LET` are lazy — only the branches they take are evaluated.

| Function | Description |
|---|---|
| `BOOL(x)` | Truthiness: NULL, FALSE, 0, '', [], {} are false. |
| `CASE(cond, value, ..., default?)` *(lazy)* | CASE(c1, v1, c2, v2, ..., default?) — first truthy condition wins; NULL if none and no default. |
| `COALESCE(...)` | First non-NULL argument. |
| `GET(coll, key, default)` | Lookup in a map or array, `default` (NULL) when absent — never an error. |
| `HAS(coll, key)` | TRUE if a map has the key or an array has the index. |
| `IF(cond, then, else)` *(lazy)* | Return `then` if `cond` is truthy, else `else`. Only the taken branch is evaluated. |
| `INT(x)` | Integer part, truncated toward zero; NULL if not numeric. |
| `IS_NUM(x)` | TRUE for numbers. |
| `IS_STR(x)` | TRUE for strings. |
| `LET(name, value, ..., body)` *(lazy)* | LET(name, value, ..., body) — bind names in order, then evaluate `body`. |
| `NUM_OF(x)` | Parse a number from a string or boolean; NULL if not numeric. |
| `STR(x)` | String form (NULL → '', TRUE → 'true', 2.0 → '2'). |
| `TRY(expr, fallback)` *(lazy)* | Evaluate `expr`; on error return `fallback` (or NULL). |
| `TYPE(x)` | Kind of a value: null, boolean, number, string, array, map, function, or a host type name. |

#### `math`

Numeric helpers. `ROUND` rounds half away from zero on the decimal representation (`ROUND(2.675, 2)` is `2.68`).

| Function | Description |
|---|---|
| `ABS(x)` | Absolute value. |
| `AVG(xs)` | Mean of an array of numbers; NULL when empty. |
| `CEIL(x)` | Smallest integer ≥ x. |
| `CLAMP(x, lo, hi)` | x limited to [lo, hi]. |
| `FLOOR(x)` | Largest integer ≤ x. |
| `MAX(...)` | Largest of the arguments, or of a single array; NULLs ignored. |
| `MIN(...)` | Smallest of the arguments, or of a single array; NULLs ignored. |
| `POW(x, y)` | x to the power y. |
| `ROUND(x, digits)` | Round half away from zero to `digits` places (default 0). |
| `SIGN(x)` | -1, 0, or 1. |
| `SQRT(x)` | Square root. |
| `SUM(xs)` | Sum of an array of numbers; NULLs ignored. |

#### `string`

`REPLACE` and `SPLIT` take literal strings; only `MATCHES` takes a regular expression. Lengths and positions count characters (code points).

| Function | Description |
|---|---|
| `CONCAT(...)` | Concatenate the string forms of all arguments. |
| `CONTAINS(hay, x)` | Substring of a string, member of an array, or key of a map. |
| `ENDS_WITH(s, suffix)` | TRUE if `s` ends with `suffix`. |
| `JOIN(xs, sep)` | Join an array's string forms with `sep` (default ''). |
| `LEN(x)` | Length of a string, array, or map (NULL → 0). |
| `LOWER(s)` | Lower-case. |
| `MATCHES(s, pattern)` | TRUE if the regular expression matches anywhere in `s`. |
| `PAD(s, width, ch)` | Left-pad to `width` with `ch` (default space). |
| `PAD_END(s, width, ch)` | Right-pad to `width` with `ch` (default space). |
| `REPLACE(s, old, new)` | Replace every literal occurrence of `old` with `new`. |
| `SPLIT(s, sep)` | Split on a literal separator ('' splits into characters). |
| `STARTS_WITH(s, prefix)` | TRUE if `s` starts with `prefix`. |
| `SUBSTR(s, start, length)` | Substring from `start` (negative counts from the end), optionally `length` long. |
| `TITLE(s)` | Capitalise each space-separated word. |
| `TRIM(s)` | Strip surrounding whitespace. |
| `TRUNCATE(s, n, suffix)` | Cut to at most `n` characters, ending with `suffix` if cut. |
| `UPPER(s)` | Upper-case. |

#### `format`

Numbers to display strings. Rounding follows `ROUND`; no locale is involved.

| Function | Description |
|---|---|
| `BYTES(n, digits)` | Byte count to '1.5 KB' (1024-based). |
| `DURATION(seconds, digits)` | Seconds to '1d 2h 3m 4s'. |
| `FORMAT(pattern, ...)` | Fill '{}' / '{0}' placeholders in a pattern with the remaining arguments. |
| `NUM(x, digits, group)` | Number to string with fixed `digits` (shortest form when omitted) and optional thousands `group`. |
| `PCT(x, digits)` | Fraction to percentage string: 0.125 → '12.5%' with digits: 1. |
| `SCI(x, digits)` | Scientific notation: 123456 → '1.23e+5'. |

#### `time`

The numeric unit is **seconds since the Unix epoch**. `EPOCH` accepts numbers, mkio ref strings (`"20260828 12:34:56.123456789000"`), and ISO-8601. Formats understand `%Y %m %d %H %M %S %f %z %%` (`%f` = microseconds); time zones are `'UTC'` (default), `'local'`, or a fixed offset such as `'+09:00'`.

| Function | Description |
|---|---|
| `DATE(ts, fmt, tz)` | Format a time as a date (default '%Y-%m-%d', UTC). |
| `EPOCH(x)` | Seconds since the epoch from a number, mkio ref string, or ISO-8601 string. |
| `NOW()` | Current time in seconds since the epoch. |
| `REF_TIME(ref, fmt, tz)` | Format an mkio ref (default '%Y-%m-%d %H:%M:%S', UTC). |
| `TIME(ts, fmt, tz)` | Format a time as a clock time (default '%H:%M:%S', UTC). |

#### `collection`

Arrays and maps; the higher-order functions take lambdas.

| Function | Description |
|---|---|
| `ALL(xs, fn)` | TRUE if `fn` (or the element) is truthy for every element (TRUE for empty). |
| `ANY(xs, fn)` | TRUE if `fn` (or the element) is truthy for any element. |
| `FILTER(xs, fn)` | Elements for which `fn` is truthy. |
| `FIND(xs, fn)` | First element for which `fn` is truthy, else NULL. |
| `FIRST(xs)` | First element, or NULL. |
| `FLATTEN(xs)` | Flatten one level of nesting. |
| `KEYS(m)` | Keys of a map. |
| `LAST(xs)` | Last element, or NULL. |
| `MAP(xs, fn)` | Apply `fn` to each element. |
| `MERGE(...)` | Merge maps left to right (later keys win). |
| `RANGE(a, b, step)` | RANGE(n) → [0..n), RANGE(a, b) → [a..b), optional step. |
| `REDUCE(xs, fn, init)` | Fold with `fn(acc, x)` starting from `init`. |
| `SORT_BY(xs, fn, desc)` | Stable sort by `fn(x)` (or the element); NULL keys last; `desc: TRUE` reverses. |
| `VALUES(m)` | Values of a map. |

<!-- expr-functions:end -->

### Extending the language

Applications register functions, bundles of functions, and value types. Registered names are callable from every expression the server evaluates, and appear in the `functions` list of the service detail API.

```python
from mkio import expr

# A plain function — arguments arrive evaluated
expr.register_function("MASK_PAN", lambda s: "****" + s[-4:], doc="Mask all but the last 4 digits")

# A library: metadata drives docs, named arguments, and the numeric-field analysis
expr.register_library("risk", {
    "VAR":    (lambda pos, conf=0.99: value_at_risk(pos, conf), {"numeric": True, "params": ("pos", "conf")}),
    "BUCKET": (lambda x, edges: sum(1 for e in edges if x >= e), {"numeric": True}),
})

# A lazy function receives unevaluated arguments (Arg thunks with .value(), .eval(scope), .name)
def when(ctx, cond, then):
    return then.value() if expr.truthy(cond.value()) else None
expr.register_function("WHEN", when, lazy=True)

# A host value type: teach the operators about it
from decimal import Decimal
expr.register_type("decimal", is_instance=lambda v: isinstance(v, Decimal),
                   add=lambda a, b: a + b, to_string=str,
                   compare=lambda a, b: (a > b) - (a < b))
# A `concat=` hook lets a type survive "text ${x}" template joining (mkui's
# rich text uses it); without one the type renders through to_string.

# Use the engine directly, with a custom environment
env = expr.Env(libraries=["core", "math", "risk"], strict=True)
alert = expr.compile("VAR(positions, conf: 0.95) > limit", env)
alert({"positions": pos, "limit": 1e6})
msg = expr.compile_template("VaR ${NUM(VAR(positions), digits: 0, group: TRUE)}", env)
```

Static analysis is available for validation and dependency tracking: `expr.field_refs(ast)`, `expr.function_refs(ast)`, `expr.numeric_fields(ast)` (fields used in numeric contexts — driven by each function's `numeric` flag). `expr.parse(src)` returns the AST; `expr.compile(src, env)` validates function names against the environment, so a typo fails at config load, not at request time.

The JavaScript module exposes the same API with camelCase names — `compile`, `compileTemplate`, `compileFilter`, `registerFunction`, `registerLibrary`, `registerType`, `Env`, `fieldRefs`, … — as ES-module exports and as `globalThis.mkioExpr`. Lazy JS functions are called as `fn(ctx, args, kwargs)`.

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

### Archive change history

Write a [versioned table's](#versioned-tables) old versions to CSV, optionally purging them:

```bash
mkio archive server.toml --older-than 90d --out ./archive --dry-run
mkio archive server.toml --older-than 90d --out ./archive --delete
mkio archive server.toml --older-than 90d --out ./archive --prune-source --yes
```

### Monitor traffic

Tap into inbound and outbound message flow in real time. Monitor a single service or all services at once:

```bash
mkio monitor localhost:8080                 # Monitor all services
mkio monitor localhost:8080 orders          # Monitor one service
mkio monitor localhost:8080 --filter "direction == 'in'"    # Inbound only
mkio monitor localhost:8080 --filter "service == 'orders'"  # Filter by service
mkio monitor localhost:8080 --filter "direction == 'out' && CONTAINS(['snapshot', 'update'], message.type)"
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
mkio schema localhost:8080 orders                 # Show columns, types, keys, defaults
mkio schema localhost:8080 orders__history   # A versioned table's history schema
```

### Schema management

```bash
mkio dbupdate                       # Apply safe schema changes (discards redo state)
mkio dbupdate --allow-risky         # Include potentially destructive changes
mkio dbupdate --allow-destructive   # Include all changes
mkio dbupdate --drop-history        # Drop history tables no longer versioned
mkio dbupdate --keep-redo           # Keep pending undo/redo state
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

Seed data is loaded **only when the table is first created** — not on every restart. If a user deletes rows, they stay deleted. Each seeded row gets its own unique `_mkio_ref` timestamp.

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
mkio dbupdate --drop-history        # Also drop history tables no longer versioned
mkio dbupdate --keep-redo           # Do not discard pending redo state
```

History tables of [versioned tables](#versioned-tables) migrate alongside their base table. Creating one, adding the `_mkio_version` counter and backfilling the baseline is a safe change, as is adding a column that was added to the base. A column dropped or retyped on the base table produces no change at all — history is additive-only. A history table is never proposed for removal, so turning `versioned` off does not queue a destructive change or block startup; use `--drop-history` when you actually want it gone.

`mkio dbupdate` also discards pending redo state — versions above a live row's cursor, and the history of rows undone past version 1 — reporting what it dropped. Pass `--keep-redo` to leave it in place.

For automatic migration on startup, set `auto_migrate` in config:

```toml
auto_migrate = "safe"          # Apply safe changes on startup (same as true)
auto_migrate = "risky"         # Also apply potentially destructive
auto_migrate = "destructive"   # Apply all changes on startup
```

## License

Apache-2.0
