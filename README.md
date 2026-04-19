# mkio

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
type = "transaction"
table = "orders"
op_type = "insert"
fields = ["id", "symbol", "qty"]

[services.all_orders]
type = "query"
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
- **SubPub** — topic-based single-row subscription with live push, server-side `where` filtering and `publish` formatting, configurable defaults for missing topics
- **Stream** — append-only ring buffer with cursor-based reconnection
- **Query** — snapshot + change feed from SQLite
- **Expression language** — safe, extensible filter and formatter expressions (`qty > 100 AND status == 'pending'`)
- **Schema migration** — automatic detection of safe/destructive changes with interactive confirmation
- **Write batching** — hundreds of writes committed in a single SQLite transaction for high throughput
- **Reconnection recovery** — stream services use ref-based cursor reconnection persisted across server restarts via `_mkio_ref` column; subpub and query always replay a full snapshot
- **Field projection** — subscribers can request specific fields per subscription, reducing payload size
- **Client libraries** — Python and JavaScript clients with auto-reconnect and ref tracking
- **Graceful shutdown** — drains pending writes, checkpoints WAL, clean close
- **Service monitoring** — tap into any service's inbound/outbound message flow via CLI or WebSocket
- **Service discovery** — `GET /api/services` list and `GET /api/services/<name>` detail endpoints, `mkio services` CLI
- **CLI tools** — send transactions, subscribe to live data, monitor traffic, inspect services

## Service Types

### Transaction

Execute INSERT, UPDATE, DELETE, or UPSERT operations. Supports multi-table atomic transactions with named ops and cross-op bind references.

```toml
[services.orders]
type = "transaction"

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

Subscribe by topic (the `key` column value) to get a single-row snapshot, then receive live updates as data changes. Always returns one row with `_mkio_exists` indicating whether the topic was found. Supports server-side `where` filtering (rows that don't match are never cached or published), `publish` formatting with expressions, and configurable `defaults` for topics that don't exist yet.

```toml
[services.last_trade]
type = "subpub"
primary_table = "orders"
key = "symbol"
where = "status == 'filled'"
change_log_size = 10000

[services.last_trade.defaults]
price = 0
time = ""

[services.last_trade.publish]
symbol = "symbol"
price = "IF(side == 'Buy', price, -price)"
```

### Stream

Append-only data with ring buffer and ref-based cursor reconnection.

```toml
[services.audit_feed]
type = "stream"
primary_table = "audit_log"
buffer_size = 10000
```

### Query

Snapshot from SQLite with change feed. Supports delta reconnection.

```toml
[services.all_orders]
type = "query"
primary_table = "orders"
filterable = ["status"]
```

## WebSocket Protocol

Connect to `/ws` (general) or `/ws/{service_name}` (per-service).

```json
// Transaction
{"service": "add_order", "ref": "...", "data": {"id": "1", "symbol": "AAPL", "qty": 100}}

// Named op transaction
{"service": "orders", "ref": "...", "op": "new", "data": {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150}}

// Transaction with msgid (echoed back on result/error for async correlation)
{"service": "orders", "ref": "...", "op": "new", "msgid": "req-42", "data": {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150}}

// Subscribe (subpub — topic required)
{"service": "last_trade", "type": "subscribe", "topic": "AAPL"}

// Subscribe (query — with filter)
{"service": "all_orders", "type": "subscribe", "filter": "status == 'pending'"}

// Subscribe with subid (echoed on every snapshot and update for this subscription)
{"service": "all_orders", "type": "subscribe", "subid": "my-sub-1"}

// Subscribe with field projection (receive only specified columns)
{"service": "all_orders", "type": "subscribe", "fields": ["symbol", "qty"]}

// Stream reconnect with ref (required for streams)
{"service": "audit_feed", "type": "subscribe", "ref": "20260404 15:30:45.123456000000"}
```

## Client Libraries

### Python

```python
from mkio.client import MkioClient

async with MkioClient("ws://localhost:8080/ws") as client:
    result = await client.send("add_order", {"id": "1", "symbol": "AAPL", "qty": 100})

    async for msg in client.subscribe("last_trade", topic="AAPL"):
        print(msg)  # single row with _mkio_exists

    async for msg in client.subscribe("all_orders", filter="status == 'pending'"):
        print(msg)
```

### JavaScript

Auto-served at `/mkio.js` — no CDN or bundler needed.

```html
<script src="/mkio.js"></script>
<script>
const client = new MkioClient("ws://localhost:8080/ws");
await client.connect();

client.subscribe("last_trade", {
    topic: "AAPL",
    onSnapshot: (rows) => renderTrade(rows[0]),
    onUpdate: (op, row) => renderTrade(row),
});

client.subscribe("all_orders", {
    filter: "status == 'pending'",
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
mkio.services()                            // list services this tab has talked to
mkio.services("orders")                    // detail for one service (via /api/services)
mkio.monitor()                             // log every frame to/from any service
mkio.monitor("orders")                     // filter to one service (call again to add more)
mkio.monitor("off")                        // stop
mkio.send("orders", {side:"Buy",...}, {op:"new"})
mkio.subscribe("last_trade", {topic:"AAPL"})
mkio.subscribe("all_orders", {filter:"status == 'pending'"})
```

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

### List and inspect services

```bash
mkio services http://localhost:8080                # List all services
mkio services http://localhost:8080 orders         # Show detail for one service
```

Detail view shows fields, types, required/optional, auto-generated columns, and example commands.

### Send transactions

```bash
mkio send http://localhost:8080 orders --op new '{"side":"Buy","symbol":"AAPL","qty":100,"price":150}'
mkio send http://localhost:8080 orders --op new orders.json    # From JSON file
mkio send http://localhost:8080 orders --op new orders.csv     # From CSV file
mkio send http://localhost:8080 orders mixed.csv                 # CSV with per-row op column
```

### Subscribe to live data

Each listener service type has its own command with only the relevant options:

```bash
# SubPub — topic-based snapshot + live updates
mkio subpub http://localhost:8080 last_trade --topic AAPL
mkio subpub http://localhost:8080 last_trade --topic AAPL --fields symbol,price

# Stream — ring buffer with cursor reconnect (ref defaults to now)
mkio stream http://localhost:8080 audit_feed
mkio stream http://localhost:8080 audit_feed --ref "20260404 15:30:45.123456000000"
mkio stream http://localhost:8080 audit_feed --fields event,order_id

# Query — snapshot + live updates
mkio query http://localhost:8080 all_orders
mkio query http://localhost:8080 all_orders --filter "status == 'pending'"
mkio query http://localhost:8080 all_orders --fields symbol,qty --snapshotOnly
```

### Monitor a service

Tap into a service's inbound and outbound message flow in real time:

```bash
mkio monitor http://localhost:8080 last_trade
```

```
[2026-04-04 15:30:45.123456 -0400] >> IN  subscribe
{ "type": "subscribe", "service": "last_trade" }

[2026-04-04 15:30:45.125789 -0400] << OUT snapshot
{ "type": "snapshot", "rows": [...] }
```

The monitor protocol is a native framework feature — any mkio application supports it.

## Using mkio from a Claude-Based Project

mkio ships agent-facing docs inside the package for AI-assisted integration. Three files in `src/mkio/agents/`:

- `AGENTS.md` — protocol, refs, discovery, service types (always needed)
- `AGENTS.python.md` — Python client API + worked example
- `AGENTS.js.md` — JS client API + worked example

### Option A: Reference in your project's CLAUDE.md

```markdown
# Python-only consumer
@/path/to/mkio/src/mkio/agents/AGENTS.md
@/path/to/mkio/src/mkio/agents/AGENTS.python.md

# JS-only consumer
@/path/to/mkio/src/mkio/agents/AGENTS.md
@/path/to/mkio/src/mkio/agents/AGENTS.js.md

# Both
@/path/to/mkio/src/mkio/agents/AGENTS.md
@/path/to/mkio/src/mkio/agents/AGENTS.python.md
@/path/to/mkio/src/mkio/agents/AGENTS.js.md
```

To find the installed path: `python -c "import mkio, os; print(os.path.join(os.path.dirname(mkio.__file__), 'agents'))"`

### Option B: Install the Claude Code skill

```bash
cp -r <mkio-checkout>/skills/mkio ~/.claude/skills/
```

The skill auto-triggers on mkio-related work and reads the agent docs from the installed package.

### Runtime service discovery

A stdlib-only helper fetches service descriptors as LLM-friendly JSON:

```bash
python -m mkio.skill_helpers.discover http://localhost:8080           # list services
python -m mkio.skill_helpers.discover http://localhost:8080 orders    # full descriptor
```

## Schema Migration

When the config schema changes between restarts, mkio detects and classifies each difference:

- **Safe** (new table, nullable column) — applied automatically
- **Potentially destructive** (type change, PK change) — requires confirmation
- **Destructive** (remove column/table) — requires confirmation

Set `auto_migrate = true` in config for non-interactive environments.

## License

Apache-2.0
