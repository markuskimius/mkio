# mkio

Config-driven Python microservice framework. Single TCP port serves HTTP + WebSocket, backed by embedded SQLite.

## Build & Test

```bash
pip install -e ".[dev]"        # Install with dev dependencies
pip install -e ".[fast,dev]"   # With orjson + uvloop acceleration
pytest tests/                  # Run all tests (174 tests)
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
│   ├── subpub.py     # In-memory cache + live push + delta reconnect + where filter
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
- **`_mkio_ref` column** is automatically added to all tables by the framework. The writer stamps each row with the transaction's `ref` on INSERT/UPDATE/UPSERT. If the client supplies a `ref`, it is used directly; otherwise the server generates one. On startup, services seed their change logs from the DB using this column, enabling delta reconnection across server restarts. Migration system excludes `_mkio_ref` from schema diffs.
- **Op-level `defaults`** in transaction op specs provide static values the client doesn't send (e.g., `defaults = { status = "accepted" }`). Stored in `CompiledOp.defaults`, used by `_extract_params` as fallback when the field isn't in client data.
- **`msgid` echo** — Transaction messages may include an optional `"msgid"` string. The server echoes it back on both result and error responses, letting clients correlate async responses. Not stored in the DB or propagated to subscribers. Supported in CLI CSV/JSON via `_ENVELOPE_KEYS`.

## Conventions

- Python 3.11+ required (for `asyncio.TaskGroup`, `tomllib`)
- All async tests use `pytest-asyncio` with `asyncio_mode = "auto"`
- `from mkio._json import dumps, loads` everywhere (never raw json/orjson)
- Services communicate changes via `ChangeBus` (never direct DB polling)
- **Subscribe protocol** uses `ref` as the recovery cursor. Clients send `"ref": "<last ref>"` to resume from that point. Transaction results include `ref` which is the same value stamped into `_mkio_ref`.
- **Monitor protocol**: WS clients send `{"type": "monitor", "service": "..."}` to tap into a service's inbound/outbound message flow.
- **Service discovery**: `GET /api/services` lists services, `GET /api/services/<name>` returns detailed usage info (fields, types, examples).
- **CLI tools**: `mkio services <url> [service]` lists/inspects services, `mkio send` sends transactions, `mkio subscribe` streams live data, `mkio monitor` taps traffic
