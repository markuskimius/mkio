# mkio

Config-driven microservice framework for Python. Define your schema, services, and data flows in a TOML file — zero coding required for standard configurations.

A single TCP port serves HTTP and WebSocket, backed by an embedded SQLite database. Designed for restricted environments where runtime downloads aren't possible — everything installs via `pip`.

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
- **SubPub** — in-memory cache with live push to subscribers, client-side filtering
- **Stream** — append-only ring buffer with cursor-based reconnection
- **Query** — snapshot + change feed from SQLite
- **Expression language** — safe, extensible filter and formatter expressions (`qty > 100 AND status == 'pending'`)
- **Schema migration** — automatic detection of safe/destructive changes with interactive confirmation
- **Write batching** — hundreds of writes committed in a single SQLite transaction for high throughput
- **Reconnection recovery** — ref-based delta sync across all service types, persisted across server restarts via `_mkio_ref` column
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

Bind references (`$0.id`) pull values from a prior op's RETURNING row. Op-level `defaults` provide static values the client doesn't need to send — here, `event` and `status` are set automatically per operation.

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

// Subscribe
{"service": "live_orders", "type": "subscribe", "filter": "status == 'pending'"}

// Reconnect with ref (gets delta instead of full snapshot)
{"service": "live_orders", "type": "subscribe", "ref": "20260404 15:30:45.123456000000"}
```

## Client Libraries

### Python

```python
from mkio.client import MkioClient

async with MkioClient("ws://localhost:8080/ws") as client:
    result = await client.send("add_order", {"id": "1", "symbol": "AAPL", "qty": 100})

    async for msg in client.subscribe("live_orders", filter="status == 'pending'"):
        print(msg)
        # msg["ref"] tracks position for recovery on reconnect
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

```bash
mkio subscribe http://localhost:8080 live_orders
mkio subscribe http://localhost:8080 live_orders --filter "status == 'pending'"
mkio subscribe http://localhost:8080 live_orders --ref "20260404 15:30:45.123456000000"  # Recovery mode
```

### Monitor a service

Tap into a service's inbound and outbound message flow in real time:

```bash
mkio monitor http://localhost:8080 live_orders
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
