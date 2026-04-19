# mkio — Agent Integration Guide

mkio is a config-driven Python microservice framework. A single TCP port serves HTTP and WebSocket, backed by embedded SQLite. You interact with it via WebSocket messages (or the Python/JS client libraries documented separately).

## Rule: Always Discover First

Before calling any service, fetch the service descriptor. Never guess field names, types, or op names.

```bash
# List all services
curl http://<host>:<port>/api/services

# Get full descriptor for one service (fields, types, required/optional, defaults, examples)
curl http://<host>:<port>/api/services/<service_name>
```

Or use the discovery helper (ships with mkio):

```bash
python -m mkio.skill_helpers.discover <url>              # list all services
python -m mkio.skill_helpers.discover <url> <service>    # full descriptor as JSON
```

### Example discovery response (transaction service)

```json
{
  "name": "orders",
  "type": "transaction",
  "ops": {
    "new": {
      "steps": [
        {
          "table": "orders",
          "op_type": "insert",
          "fields": {
            "side": {"type": "TEXT", "required": true},
            "symbol": {"type": "TEXT", "required": true},
            "qty": {"type": "INTEGER", "required": false},
            "price": {"type": "REAL", "required": false}
          },
          "auto": {
            "id": {"type": "INTEGER", "source": "autoincrement"},
            "status": {"type": "TEXT", "source": "op_default", "default": "pending"}
          }
        }
      ],
      "example": "mkio send <url> orders --op new '{\"side\": \"Buy\", \"symbol\": \"AAPL\", \"qty\": 0, \"price\": 0.0}'"
    }
  },
  "recovery": {
    "description": "Each result includes a ref string. To check if a transaction committed after a disconnect, send a check message with that ref.",
    "check_message": {"service": "orders", "type": "check", "ref": "<ref>"}
  }
}
```

### Example discovery response (query service)

```json
{
  "name": "all_orders",
  "type": "query",
  "primary_table": "orders",
  "filterable": ["status", "symbol"],
  "schema": {
    "id": {"type": "INTEGER", "pk": true},
    "symbol": {"type": "TEXT"},
    "qty": {"type": "INTEGER"},
    "status": {"type": "TEXT"}
  },
  "subscribe": {
    "message": {"service": "all_orders", "type": "subscribe", "filter": "<expr>"},
    "response_types": ["snapshot", "update"]
  }
}
```

### Example discovery response (subpub service)

```json
{
  "name": "last_trade",
  "type": "subpub",
  "primary_table": "orders",
  "topic_key": "symbol",
  "subscribe": {
    "message": {"service": "last_trade", "type": "subscribe", "topic": "<key value>"},
    "response_types": ["snapshot", "update"]
  }
}
```

## WebSocket Envelope

Connect to `ws://<host>:<port>/ws` (general) or `ws://<host>:<port>/ws/<service>` (per-service, no `service` field needed in messages).

### Sending a transaction

```json
{"service": "orders", "op": "new", "ref": "<optional client ref>", "data": {"side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0}}
```

- `service` — target service name (omit if connected to `/ws/<service>`)
- `op` — operation name (omit if the service has a single unnamed op)
- `ref` — optional client-supplied ref string; if omitted, the server generates one
- `msgid` — optional string echoed on the result/error response (for async correlation)
- `data` — field values matching the descriptor's `fields`

### Transaction result

```json
{"type": "result", "ok": true, "ref": "20260409 14:30:45.123456000000", "rows": [{"id": 1, "side": "Buy", "symbol": "AAPL", "qty": 100, "price": 150.0, "status": "pending"}]}
```

### Error response

```json
{"type": "error", "message": "Missing required field: symbol", "ref": "...", "msgid": "..."}
```

### Subscribing

```json
// SubPub — topic required (the key column value)
{"service": "last_trade", "type": "subscribe", "topic": "AAPL"}

// Query — with optional filter
{"service": "all_orders", "type": "subscribe", "filter": "status == 'pending'"}
```

- `topic` — (subpub only, required) the key column value to subscribe to. Returns a single row with `_mkio_exists: true/false`. If the topic doesn't exist yet, all fields are returned with null values (or configured defaults — expression strings evaluated at startup). When the topic later appears, it's published as an update.
- `filter` — (query only) expression string, only valid if the service descriptor lists `filterable` fields
- `fields` — optional list of field names to include in each row (e.g., `["symbol", "qty"]`). Omit to receive all fields. Filtering still operates on the full row before projection.
- `ref` — (stream only, required) ref from the last received message for cursor-based reconnection
- `subid` — optional string echoed on every response (snapshot, update) for this subscription, useful for correlating when multiplexing subscriptions on one WebSocket
- `snapshot` — (query only) boolean, default true. Set to false to skip the initial snapshot.
- `updates` — (query only) boolean, default true. Set to false to receive only the snapshot.

### Subscription messages

| Type | Shape | When |
|------|-------|------|
| `snapshot` | `{"type": "snapshot", "rows": [...]}` | Initial state on subscribe |
| `update` | `{"type": "update", "op": "insert\|update\|delete", "row": {...}}` | Live change (subpub always uses `op: "update"`) |

Stream service messages include `ref` for cursor-based reconnection. **Query service rows** include a `_mkio_row` field — a collision-free string identifying the row by its primary key(s) across all watched tables. Single PK: `"42"`. Multiple PKs: `["P1",10]`.

### Check (post-disconnect recovery)

```json
{"service": "orders", "type": "check", "ref": "<ref from prior result>"}
```

Returns the original result if committed, or an error if not found.

## Ref Strings

Format: `YYYYMMDD HH:mm:ss.mmmuuunnnppp` (UTC, lexicographically sortable).

- Every transaction result includes a `ref`
- Stream service messages include `ref` for cursor-based reconnection
- Save the last `ref` you receive from a stream. Pass it back on reconnect to resume from that point.
- The same ref is stamped into the `_mkio_ref` column in the database

## Service Types

| Type | Purpose | Write? | Subscribe? |
|------|---------|--------|------------|
| `transaction` | Insert/update/delete/upsert across one or more tables | Yes | No (use a listener service) |
| `subpub` | In-memory cache + live push. Snapshot on subscribe, then updates. | No | Yes |
| `stream` | Append-only ring buffer. Snapshot + cursor-based reconnection. | No | Yes |
| `query` | SQLite snapshot + change feed. Full DB query on subscribe, then live changes. | No | Yes |

Typical pattern: a `transaction` service writes data, and one or more `subpub`/`stream`/`query` services expose it to subscribers in real time.

## Fields You Don't Need to Send

The service descriptor separates `fields` (client-provided) from `auto` (server-generated). Auto fields include:

- **autoincrement** — integer primary keys
- **default** — column-level SQL defaults
- **op_default** — operation-level defaults (e.g., `status = "pending"` for a "new order" op)

If a field in `fields` has `"required": false` with a `"default"`, you can omit it and the default applies.

### Bind references

Multi-step operations use `bind` to pass values between steps. For example:

```json
{"table": "audit_log", "op_type": "insert", "bind": {"order_id": "$0.id", "status": "$0.status"}}
```

`$0.id` means "the `id` column from step 0's RETURNING row." You don't send bound fields — they're filled automatically.

## Expression Language

Used in `filter` (query subscribe) and `where`/`publish` (server config). Supports:

- Comparison: `==`, `!=`, `>`, `<`, `>=`, `<=`
- Logical: `AND`, `OR`, `NOT`
- Arithmetic: `+`, `-`, `*`, `/`
- String: `CONTAINS`, `STARTS_WITH`
- Null: `IS NULL`, `IS NOT NULL`
- Functions: `UPPER()`, `LOWER()`, `ROUND()`, `ABS()`, `COALESCE()`, `IF(cond, then, else)`
