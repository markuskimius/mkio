# mkio

Config-driven Python microservice framework. Single TCP port serves HTTP + WebSocket, backed by embedded SQLite.

## Build & Test

```bash
pip install -e ".[dev]"        # Install with dev dependencies
pip install -e ".[fast,dev]"   # With orjson + uvloop acceleration
pytest tests/                  # Run all tests (224 tests)
pytest tests/ -x -v            # Stop on first failure, verbose
```

## Architecture

```
src/mkio/
├── _json.py          # orjson-with-fallback (dumps -> bytes, loads)
├── _ref.py           # "YYYYMMDD HH:mm:ss.mmmuuunnnppp" ref strings
├── _expr.py          # Expression language: tokenizer, parser, evaluator
├── config.py         # TOML/dict loader, normalization, validation
├── migration.py      # Schema diff, change classification, data preservation
├── database.py       # Dual aiosqlite connections (write + read), WAL mode
├── change_bus.py     # Async broadcast with pre-serialized bytes
├── writer.py         # Write batcher: SAVEPOINTs, single commit per batch
├── ws_protocol.py    # JSON envelope helpers
├── server.py         # aiohttp wiring, WS dispatch, static serving, monitor protocol
├── services/
│   ├── base.py       # Service base class with monitor notification
│   ├── transaction.py # Config-driven SQL ops + result cache
│   ├── subpub.py     # In-memory cache + topic-based single-row push
│   ├── stream.py     # Ring buffer + ref-based cursor
│   └── query.py      # SQLite snapshot + change feed
└── client/
    ├── __init__.py   # Python client with auto-reconnect
    └── mkio.js       # JS client, auto-served at /mkio.js
```

## Key Patterns

- **Write path**: TransactionService -> WriteBatcher queue -> batch with SAVEPOINTs -> single COMMIT -> ChangeBus publish -> fan out to subscribers
- **In-memory databases** use shared-cache URI (`file:mkio_{uuid}?mode=memory&cache=shared`) so write and read connections see the same data
- **Expression language** is parsed once (at subscribe/startup), evaluated per row via AST walk. Built-in functions: `UPPER`, `LOWER`, `ROUND`, `ABS`, `COALESCE`, `IF(cond, then, else)`. `IF` short-circuits (only the chosen branch is evaluated).
- **Ref strings** are lexicographically sortable UTC timestamps with sub-nanosecond counter for uniqueness
- **Schema migration** uses recreate-table strategy for changes SQLite's ALTER TABLE can't handle
- **`_mkio_ref` column** is automatically added to all tables by the framework. The writer stamps each row with the transaction's `ref` on INSERT/UPDATE/UPSERT. If the client supplies a `ref`, it is used directly; otherwise the server generates one. Stream services seed their buffer from the DB using this column on startup, enabling cursor-based reconnection across server restarts. Migration system excludes `_mkio_ref` from schema diffs.
- **Op-level `defaults`** in transaction op specs provide static values the client doesn't send (e.g., `defaults = { status = "accepted" }`). Stored in `CompiledOp.defaults`, used by `_extract_params` as fallback when the field isn't in client data.
- **`msgid` echo** — Transaction messages may include an optional `"msgid"` string. The server echoes it back on both result and error responses, letting clients correlate async responses. Not stored in the DB or propagated to subscribers. Supported in CLI CSV/JSON via `_ENVELOPE_KEYS`.
- **`subid` echo** — Subscribe messages may include an optional `"subid"` string. The server echoes it on every outbound message (snapshot, update) for that subscription, letting clients correlate responses when multiplexing subscriptions on a single WebSocket. Stored on the subscriber dataclass, passed through `make_snapshot`/`make_update` via keyword arg.
- **`_mkio_row` field** — QueryService adds a `_mkio_row` string to every published row (snapshot, update) identifying the row by its primary key(s). PK columns are discovered at startup via `PRAGMA table_info` for all tables in `watch_tables` (primary table first, then secondaries, deduplicating by column name). For single-column PKs the value is `str(pk_value)`; for multiple PK columns it's a compact JSON array (e.g., `["P1",10]`). Collision-free since PK structure is fixed per table set. Always included even when `fields` projection is active.
- **SubPub topic protocol** — SubPub subscribe requires a `"topic"` field (the primary key value). The server always returns exactly one row in the snapshot with `_mkio_exists: true/false` indicating whether the topic was found. Not-found rows include all output fields with null values (or configured defaults via `defaults = { field = "expression" }` in service config — values are expression strings compiled at startup, e.g., `defaults = { price = "0", label = "'n/a'" }`). All live changes are sent as `op: "update"` (never "insert" or "delete") — if a topic transitions from not-found to found, it's an update with `_mkio_exists: true`; if deleted, it's an update with `_mkio_exists: false`. Cache keys are stringified for consistent topic matching across integer/text primary keys.
- **SubPub `sql` option** — By default SubPub uses `SELECT * FROM primary_table`. Set `sql` to a custom query to compute topics, JOIN tables, or reshape data. The `topic` field must name a column in the query result. When `sql` contains a JOIN, SubPub uses re-query on change events instead of direct cache update.
- **Field projection** — Subscribe messages may include `"fields": ["col1", "col2"]` to receive only the specified columns in each row. Filtering still operates on the full row before projection. Query service always preserves `_mkio_` prefixed fields (`_mkio_row`). SubPub always includes `_mkio_exists` after projection. Stored on the subscriber dataclass and applied at output time.

## Conventions

- Python 3.11+ required (for `asyncio.TaskGroup`, `tomllib`)
- All async tests use `pytest-asyncio` with `asyncio_mode = "auto"`
- `from mkio._json import dumps, loads` everywhere (never raw json/orjson)
- Services communicate changes via `ChangeBus` (never direct DB polling)
- **Subscribe protocol** — SubPub requires `topic` (primary key value) and always sends a single-row snapshot + updates. Query supports `snapshot`/`updates` booleans and optional `filter` expressions with `filterable` config. Stream requires `ref` for cursor-based reconnection. All subscribe types support optional `fields` list and `subid`.
- **Monitor protocol**: WS clients send `{"type": "monitor", "service": "..."}` to tap into a service's inbound/outbound message flow.
- **Service discovery**: `GET /api/services` lists services, `GET /api/services/<name>` returns detailed usage info (fields, types, examples).
- **CLI tools**: `mkio services <url> [service]` lists/inspects services, `mkio send` sends transactions, `mkio subpub`/`mkio stream`/`mkio query` subscribe to live data (each with protocol-specific options), `mkio monitor` taps traffic
