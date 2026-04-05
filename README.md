# mkio

Config-driven microservice framework for Python. Define your schema, services, and data flows in a TOML file — zero coding required for standard configurations.

A single TCP port serves HTTP and WebSocket, backed by an embedded SQLite database. Designed for restricted environments where runtime downloads aren't possible — everything installs via `pip`.

## Quick Start

```bash
pip install mkio
```

Create `config.toml`:

```toml
port = 8080

[tables.orders]
columns = { id = "TEXT PRIMARY KEY", symbol = "TEXT NOT NULL", qty = "INTEGER", status = "TEXT DEFAULT 'pending'" }

[services.add_order]
type = "transaction"
table = "orders"
op_type = "insert"
fields = ["id", "symbol", "qty"]

[services.live_orders]
type = "subpub"
primary_table = "orders"
key = "id"
filterable = ["status", "symbol"]

[static]
"/" = "./static"
```

Run:

```bash
mkio serve config.toml
```

Or programmatically:

```python
from mkio import serve
serve("config.toml")
serve({...})  # or pass a dict
```

## Features

- **Single port** — HTTP pages and WebSocket messages on one port
- **Config-driven** — define tables, transactions, and live data services in TOML
- **Transaction services** — insert, update, delete, upsert across multiple tables atomically
- **SubPub** — in-memory cache with live push to subscribers, client-side filtering
- **Stream** — append-only ring buffer with cursor-based reconnection
- **Query** — snapshot + change feed from SQLite
- **Expression language** — safe, extensible filter and formatter expressions (`qty > 100 AND status == 'pending'`)
- **Schema migration** — automatic detection of safe/destructive changes with interactive confirmation
- **Write batching** — hundreds of writes committed in a single SQLite transaction for high throughput
- **Reconnection recovery** — version-based delta sync across all service types
- **Client libraries** — Python and JavaScript clients with auto-reconnect and version tracking
- **Graceful shutdown** — drains pending writes, checkpoints WAL, clean close
- **Service monitoring** — tap into any service's inbound/outbound message flow via CLI or WebSocket
- **Service discovery** — `GET /api/services` endpoint and `mkio services` CLI command

## Service Types

### Transaction

Execute INSERT, UPDATE, DELETE, or UPSERT operations. Supports multi-table atomic transactions.

```toml
[services.place_order]
type = "transaction"
ops = [
    { table = "orders", op_type = "insert", fields = ["id", "symbol", "qty"] },
    { table = "audit_log", op_type = "insert", fields = ["event", "order_id"] },
]
```

### SubPub

Subscribe to get a snapshot from an in-memory cache, then receive live updates as data changes. Supports client filters and server-side formatting.

```toml
[services.live_orders]
type = "subpub"
primary_table = "orders"
key = "id"
filterable = ["status", "symbol"]

[services.live_orders.publish]
ticker = "UPPER(symbol)"
total = "qty * price"
```

### Stream

Append-only data with ring buffer and version-based cursor reconnection.

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

// Subscribe
{"service": "live_orders", "type": "subscribe", "filter": "status == 'pending'"}

// Reconnect with version (gets delta instead of full snapshot)
{"service": "live_orders", "type": "subscribe", "version": "20260404 15:30:45.123456000000"}
```

## Client Libraries

### Python

```python
from mkio.client import MkioClient

async with MkioClient("ws://localhost:8080/ws") as client:
    result = await client.send("add_order", {"id": "1", "symbol": "AAPL", "qty": 100})

    async for msg in client.subscribe("live_orders", filter="status == 'pending'"):
        print(msg)
```

### JavaScript

Auto-served at `/mkio.js` — no CDN or bundler needed.

```html
<script src="/mkio.js"></script>
<script>
const client = new MkioClient("ws://localhost:8080/ws");
await client.connect();

client.subscribe("live_orders", {
    filter: "status == 'pending'",
    onSnapshot: (rows) => renderTable(rows),
    onUpdate: (op, row) => updateRow(op, row),
});
</script>
```

## Expression Language

Used for client filters and server-side publish formatters.

| Category | Syntax |
|----------|--------|
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical | `AND`, `OR`, `NOT` |
| Arithmetic | `+`, `-`, `*`, `/` |
| String | `CONTAINS`, `STARTS_WITH` |
| Null | `IS NULL`, `IS NOT NULL` |
| Functions | `UPPER()`, `LOWER()`, `ROUND()`, `ABS()`, `COALESCE()` |

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

### List services

```bash
mkio services http://localhost:8080
```

```
SERVICE      TYPE         DETAILS
add_order    transaction  tables=orders
live_orders  subpub       table=orders, watch=orders
```

### Monitor a service

Tap into a service's inbound and outbound message flow in real time:

```bash
mkio monitor ws://localhost:8080 live_orders
```

```
[15:30:45.123] >> IN  subscribe
{ "type": "subscribe", "service": "live_orders" }

[15:30:45.125] << OUT snapshot
{ "type": "snapshot", "rows": [...] }
```

The monitor protocol is a native framework feature — any mkio application supports it.

## Schema Migration

When the config schema changes between restarts, mkio detects and classifies each difference:

- **Safe** (new table, nullable column) — applied automatically
- **Potentially destructive** (type change, PK change) — requires confirmation
- **Destructive** (remove column/table) — requires confirmation

Set `auto_migrate = true` in config for non-interactive environments.

## License

Apache-2.0
